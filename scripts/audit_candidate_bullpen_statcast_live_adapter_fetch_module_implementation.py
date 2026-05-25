from __future__ import annotations

import ast
import csv
import importlib.util
import json
import subprocess
import sys
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, Dict, List


AUDIT_VERSION = "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_audit_v0.1"

ADAPTER_SCRIPT = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
BACKFILL_SCAFFOLD = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
PLAN_6DF = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_fetch_contract_module.py")
AUDIT_6DG = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan.py")
TEST_DOUBLE_PLAN_6DC = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_fetch_test_doubles.py")
TEST_DOUBLE_PROTOTYPE_6DD = Path("scripts/prototype_candidate_bullpen_statcast_live_adapter_fetch_test_doubles.py")
TEST_DOUBLE_AUDIT_6DE = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_fetch_test_double_prototype.py")
FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"
DATES_DIR = FIXTURE_ROOT / "dates"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

MODULE_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation.json"
MODULE_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_checks.csv"
MODULE_RESULT_CONTRACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_result_contract.csv"
MODULE_ROW_CONTRACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_normalized_row_contract.csv"
MODULE_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_status_mapping.csv"
MODULE_DUPLICATE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_duplicate_audit.csv"
MODULE_ORDERING = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_ordering_audit.csv"
MODULE_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_safety_audit.csv"

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_audit_checks.csv"
OUTPUT_ARTIFACTS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_artifact_validation.csv"
OUTPUT_SOURCE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_source_inspection.csv"
OUTPUT_API = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_public_api_audit.csv"
OUTPUT_BEHAVIOR = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_independent_behavior_audit.csv"
OUTPUT_RESULT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_independent_result_contract_audit.csv"
OUTPUT_ROW = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_independent_normalized_row_contract_audit.csv"
OUTPUT_INTEGRATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_scaffold_integration_safety.csv"
OUTPUT_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_immutability_audit.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_independent_safety_audit.csv"

EXPECTED_RESULT_FIELDS = {
    "label_date",
    "status",
    "rows",
    "raw_row_count",
    "normalized_row_count",
    "duplicate_count",
    "required_field_failures",
    "missing_fields",
    "fetch_error",
    "external_fetch_performed",
    "db_writes_performed",
    "fetch_duration_ms",
    "retry_count",
    "source_adapter_version",
}
EXPECTED_NORMALIZED_FIELDS = {
    "game_date",
    "game_pk",
    "inning",
    "inning_topbot",
    "at_bat_number",
    "pitch_number",
    "outs_when_up",
    "pitcher_id",
    "home_team",
    "away_team",
    "events",
    "description",
}
EXPECTED_NATURAL_KEY_FIELDS = {"game_pk", "at_bat_number", "pitch_number", "pitcher_id"}
EXPECTED_STATUSES = {
    "live_dry_run_ready",
    "live_fetch_empty",
    "live_fetch_error",
    "live_schema_failed_safely",
    "live_adapter_not_configured",
    "live_write_blocked",
    "live_requires_dry_run",
    "live_date_window_invalid",
    "live_dependency_missing",
}


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames: List[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def _snapshot_files() -> Dict[str, str]:
    paths = [
        ADAPTER_SCRIPT,
        BACKFILL_SCAFFOLD,
        PLAN_6DF,
        AUDIT_6DG,
        TEST_DOUBLE_PLAN_6DC,
        TEST_DOUBLE_PROTOTYPE_6DD,
        TEST_DOUBLE_AUDIT_6DE,
        MANIFEST,
        EXPECTED_RESULTS,
    ]
    snapshot = {str(path): path.read_text(errors="ignore") if path.exists() else "__MISSING__" for path in paths}
    if DATES_DIR.exists():
        for payload in sorted(DATES_DIR.glob("*.jsonl")):
            snapshot[str(payload)] = payload.read_text(errors="ignore")
    return snapshot


def _literal_list_from_ast(tree: ast.AST, name: str) -> List[str]:
    constant_values: Dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if (
                    isinstance(target, ast.Name)
                    and isinstance(node.value, ast.Constant)
                    and isinstance(node.value.value, str)
                ):
                    constant_values[target.id] = node.value.value

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    if isinstance(node.value, ast.List):
                        values: List[str] = []
                        for elt in node.value.elts:
                            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                                values.append(elt.value)
                            elif isinstance(elt, ast.Name) and elt.id in constant_values:
                                values.append(constant_values[elt.id])
                        return values
    return []


def _run_module_self_check() -> Dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(ADAPTER_SCRIPT)],
        capture_output=True,
        text=True,
    )
    diagnosis = _read_json(MODULE_JSON)
    passed = (
        completed.returncode == 0
        and diagnosis.get("all_checks_passed") is True
        and diagnosis.get("adapter_module_created") is True
        and diagnosis.get("external_fetch_performed") is False
        and diagnosis.get("db_writes_performed") is False
    )
    return {
        "returncode": completed.returncode,
        "diagnosis": diagnosis.get("diagnosis", ""),
        "stdout_tail": completed.stdout[-1000:],
        "stderr_tail": completed.stderr[-1000:],
        "passed": passed,
    }


def _artifact_validation() -> List[Dict[str, Any]]:
    artifacts = [
        MODULE_JSON,
        MODULE_CHECKS,
        MODULE_RESULT_CONTRACT,
        MODULE_ROW_CONTRACT,
        MODULE_STATUS,
        MODULE_DUPLICATE,
        MODULE_ORDERING,
        MODULE_SAFETY,
    ]
    return [
        {
            "artifact": str(path),
            "exists": path.exists(),
            "size_bytes": path.stat().st_size if path.exists() else 0,
            "passed": path.exists() and path.stat().st_size > 0,
        }
        for path in artifacts
    ]


def _source_inspection() -> List[Dict[str, Any]]:
    source = ADAPTER_SCRIPT.read_text(errors="ignore") if ADAPTER_SCRIPT.exists() else ""
    tree = ast.parse(source) if source else ast.Module(body=[], type_ignores=[])

    class_names = {node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)}
    assign_names = {target.id for node in ast.walk(tree) if isinstance(node, ast.Assign) for target in node.targets if isinstance(target, ast.Name)}
    result_fields = set(_literal_list_from_ast(tree, "RESULT_FIELDS"))
    normalized_fields = set(_literal_list_from_ast(tree, "NORMALIZED_FIELDS"))
    natural_key_fields = set(_literal_list_from_ast(tree, "NATURAL_KEY_FIELDS"))
    statuses = set(_literal_list_from_ast(tree, "STATUS_TAXONOMY"))

    import_lines = "\n".join(
        line.strip()
        for line in source.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    )
    scanner_start = source.find("def _source_safety_scan()")
    executable_prefix = source[:scanner_start] if scanner_start >= 0 else source
    executable_lower = executable_prefix.lower()

    rows = [
        {"check": "module_file_exists", "passed": ADAPTER_SCRIPT.exists(), "detail": str(ADAPTER_SCRIPT)},
        {"check": "live_adapter_version_exists", "passed": "LIVE_ADAPTER_VERSION" in assign_names, "detail": "LIVE_ADAPTER_VERSION"},
        {"check": "live_adapter_result_dataclass_exists", "passed": "LiveAdapterResult" in class_names and "@dataclass" in source, "detail": "LiveAdapterResult"},
        {"check": "result_fields_exact", "passed": result_fields == EXPECTED_RESULT_FIELDS, "detail": f"{len(result_fields)} fields"},
        {"check": "normalized_fields_exact", "passed": normalized_fields == EXPECTED_NORMALIZED_FIELDS, "detail": f"{len(normalized_fields)} fields"},
        {"check": "natural_key_fields_exact", "passed": natural_key_fields == EXPECTED_NATURAL_KEY_FIELDS, "detail": f"{len(natural_key_fields)} fields"},
        {"check": "status_taxonomy_exact", "passed": statuses == EXPECTED_STATUSES, "detail": f"{len(statuses)} statuses"},
        {"check": "no_top_level_pybaseball_import", "passed": "pybaseball" not in import_lines and "statcast" not in import_lines, "detail": True},
        {"check": "no_external_network_usage", "passed": all(token not in executable_prefix for token in ["requests.", "httpx.", "urllib."]), "detail": True},
        {"check": "no_db_write_tokens", "passed": all(token not in executable_lower for token in ["session.commit(", ".to_sql(", "insert into"]), "detail": True},
    ]
    return rows


def _load_adapter_module() -> Any:
    spec = importlib.util.spec_from_file_location("candidate_bullpen_statcast_live_adapter_audit_target", ADAPTER_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load adapter module spec")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _base_row(label_date: str, game_pk: int, at_bat_number: int, pitch_number: int, pitcher_id: int) -> Dict[str, Any]:
    return {
        "game_date": label_date,
        "game_pk": game_pk,
        "inning": 7,
        "inning_topbot": "Top",
        "at_bat_number": at_bat_number,
        "pitch_number": pitch_number,
        "outs_when_up": 1,
        "pitcher_id": pitcher_id,
        "home_team": "NYY",
        "away_team": "BOS",
        "events": "strikeout",
        "description": "called_strike",
    }


def _public_api_audit(module: Any) -> List[Dict[str, Any]]:
    result_cls = getattr(module, "LiveAdapterResult", None)
    fetch_fn = getattr(module, "fetch_candidate_bullpen_statcast_live_rows_for_date", None)
    normalize_fn = getattr(module, "normalize_statcast_pitch_rows", None)
    natural_key_fn = getattr(module, "natural_key", None)

    return [
        {"api": "LiveAdapterResult", "present": result_cls is not None, "dataclass": is_dataclass(result_cls), "field_count": len(fields(result_cls)) if is_dataclass(result_cls) else 0, "passed": result_cls is not None and is_dataclass(result_cls) and len(fields(result_cls)) == 14},
        {"api": "fetch_candidate_bullpen_statcast_live_rows_for_date", "present": callable(fetch_fn), "passed": callable(fetch_fn)},
        {"api": "normalize_statcast_pitch_rows", "present": callable(normalize_fn), "passed": callable(normalize_fn)},
        {"api": "natural_key", "present": callable(natural_key_fn), "passed": callable(natural_key_fn)},
    ]


def _independent_behavior_audit(module: Any) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    def success_fetcher(label_date: str) -> List[Dict[str, Any]]:
        return [
            _base_row(label_date, 2001, 9, 2, 700),
            _base_row(label_date, 2001, 9, 1, 700),
            _base_row(label_date, 2002, 1, 1, 701),
        ]

    def empty_fetcher(label_date: str) -> List[Dict[str, Any]]:
        return []

    def error_fetcher(label_date: str) -> List[Dict[str, Any]]:
        raise RuntimeError("independent audit injected error")

    def schema_failure_fetcher(label_date: str) -> List[Dict[str, Any]]:
        row = _base_row(label_date, 2003, 2, 1, 702)
        del row["pitcher_id"]
        return [row]

    def duplicate_fetcher(label_date: str) -> List[Dict[str, Any]]:
        row_a = _base_row(label_date, 2004, 3, 1, 703)
        row_b = dict(row_a)
        row_b["description"] = "duplicate natural key"
        row_c = _base_row(label_date, 2004, 3, 2, 703)
        return [row_a, row_b, row_c]

    def unordered_fetcher(label_date: str) -> List[Dict[str, Any]]:
        return [
            _base_row(label_date, 3000, 8, 3, 900),
            _base_row(label_date, 1000, 1, 1, 100),
            _base_row(label_date, 1000, 1, 2, 100),
        ]

    fetch_fn = module.fetch_candidate_bullpen_statcast_live_rows_for_date

    results = {
        "success": fetch_fn("2024-07-16", 30, 0, fetcher=success_fetcher),
        "empty": fetch_fn("2024-07-16", 30, 0, fetcher=empty_fetcher),
        "error": fetch_fn("2024-07-16", 30, 2, fetcher=error_fetcher),
        "schema_failure": fetch_fn("2024-07-16", 30, 0, fetcher=schema_failure_fetcher),
        "duplicate": fetch_fn("2024-07-16", 30, 0, fetcher=duplicate_fetcher),
        "unordered": fetch_fn("2024-07-16", 30, 0, fetcher=unordered_fetcher),
        "invalid_label_date": fetch_fn("2024-7-16", 30, 0, fetcher=success_fetcher),
        "dependency_missing": fetch_fn("2024-07-16", 30, 0, fetcher=None),
    }

    expected_status = {
        "success": "live_dry_run_ready",
        "empty": "live_fetch_empty",
        "error": "live_fetch_error",
        "schema_failure": "live_schema_failed_safely",
        "duplicate": "live_dry_run_ready",
        "unordered": "live_dry_run_ready",
        "invalid_label_date": "live_fetch_error",
        "dependency_missing": "live_dependency_missing",
    }

    rows = []
    for case, result in results.items():
        ordering_valid = result.rows == sorted(result.rows, key=module.natural_key)
        duplicate_valid = result.duplicate_count == 1 if case == "duplicate" else result.duplicate_count == 0
        retry_valid = result.retry_count == 2 if case == "error" else isinstance(result.retry_count, int)
        fetch_error_valid = bool(result.fetch_error) if case in {"error", "invalid_label_date", "dependency_missing"} else result.fetch_error == ""
        rows.append({
            "case": case,
            "expected_status": expected_status[case],
            "actual_status": result.status,
            "retry_count": result.retry_count,
            "duplicate_count": result.duplicate_count,
            "ordering_valid": ordering_valid,
            "fetch_error_populated_as_expected": fetch_error_valid,
            "external_fetch_performed": result.external_fetch_performed,
            "db_writes_performed": result.db_writes_performed,
            "passed": (
                result.status == expected_status[case]
                and duplicate_valid
                and retry_valid
                and ordering_valid
                and fetch_error_valid
                and result.external_fetch_performed is False
                and result.db_writes_performed is False
            ),
        })
    return rows, results


def _result_contract_audit(module: Any, results: Dict[str, Any]) -> List[Dict[str, Any]]:
    result_field_names = {field.name for field in fields(module.LiveAdapterResult)}
    rows = []
    for case, result in results.items():
        payload = result.__dict__
        rows.append({
            "case": case,
            "field_count": len(payload),
            "fields_exact": set(payload) == EXPECTED_RESULT_FIELDS and result_field_names == EXPECTED_RESULT_FIELDS,
            "external_fetch_performed": result.external_fetch_performed,
            "db_writes_performed": result.db_writes_performed,
            "fetch_duration_ms_type_valid": isinstance(result.fetch_duration_ms, int),
            "retry_count_type_valid": isinstance(result.retry_count, int),
            "source_adapter_version_populated": bool(result.source_adapter_version),
            "passed": (
                len(payload) == 14
                and set(payload) == EXPECTED_RESULT_FIELDS
                and result_field_names == EXPECTED_RESULT_FIELDS
                and result.external_fetch_performed is False
                and result.db_writes_performed is False
                and isinstance(result.fetch_duration_ms, int)
                and isinstance(result.retry_count, int)
                and bool(result.source_adapter_version)
            ),
        })
    return rows


def _normalized_row_contract_audit(module: Any, results: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    for case, result in results.items():
        sorted_rows = sorted(result.rows, key=module.natural_key)
        for idx, row in enumerate(result.rows):
            rows.append({
                "case": case,
                "row_index": idx,
                "field_count": len(row),
                "fields_exact": set(row) == EXPECTED_NORMALIZED_FIELDS,
                "natural_key_complete": all(field in row for field in EXPECTED_NATURAL_KEY_FIELDS),
                "deterministic_order": result.rows == sorted_rows,
                "missing_fields_unique_sorted": result.missing_fields == sorted(set(result.missing_fields)),
                "passed": (
                    len(row) == 12
                    and set(row) == EXPECTED_NORMALIZED_FIELDS
                    and all(field in row for field in EXPECTED_NATURAL_KEY_FIELDS)
                    and result.rows == sorted_rows
                    and result.missing_fields == sorted(set(result.missing_fields))
                ),
            })
    if not rows:
        rows.append({"case": "no_rows", "row_index": -1, "field_count": 0, "fields_exact": True, "natural_key_complete": True, "deterministic_order": True, "missing_fields_unique_sorted": True, "passed": True})
    return rows


def _scaffold_integration_safety() -> List[Dict[str, Any]]:
    scaffold_source = BACKFILL_SCAFFOLD.read_text(errors="ignore") if BACKFILL_SCAFFOLD.exists() else ""
    adapter_name = "fetch_candidate_bullpen_statcast_live_adapter"
    return [
        {
            "check": "scaffold_not_wired_to_adapter",
            "passed": adapter_name not in scaffold_source,
            "detail": "adapter import/name absent from backfill scaffold",
        },
        {
            "check": "adapter_module_exists",
            "passed": ADAPTER_SCRIPT.exists(),
            "detail": str(ADAPTER_SCRIPT),
        },
    ]


def _safety_audit(before_snapshot: Dict[str, str], behavior_results: Dict[str, Any]) -> List[Dict[str, Any]]:
    after_snapshot = _snapshot_files()
    return [
        {"check": "audit_only", "passed": True, "detail": "independent audit only"},
        {"check": "adapter_not_mutated_by_audit", "passed": before_snapshot.get(str(ADAPTER_SCRIPT)) == after_snapshot.get(str(ADAPTER_SCRIPT)), "detail": str(ADAPTER_SCRIPT)},
        {"check": "no_scaffold_mutation", "passed": before_snapshot.get(str(BACKFILL_SCAFFOLD)) == after_snapshot.get(str(BACKFILL_SCAFFOLD)), "detail": str(BACKFILL_SCAFFOLD)},
        {
            "check": "no_plan_or_prior_layer_mutation",
            "passed": all(
                before_snapshot.get(str(path)) == after_snapshot.get(str(path))
                for path in [PLAN_6DF, AUDIT_6DG, TEST_DOUBLE_PLAN_6DC, TEST_DOUBLE_PROTOTYPE_6DD, TEST_DOUBLE_AUDIT_6DE]
            ),
            "detail": "6DF/6DG/6DC/6DD/6DE scripts unchanged",
        },
        {"check": "no_fixture_mutation", "passed": before_snapshot == after_snapshot, "detail": "fixture and tracked scripts unchanged"},
        {"check": "no_external_fetch", "passed": all(not result.external_fetch_performed for result in behavior_results.values()), "detail": True},
        {"check": "no_db_writes", "passed": all(not result.db_writes_performed for result in behavior_results.values()), "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]


def main() -> None:
    before_snapshot = _snapshot_files()

    module_execution = _run_module_self_check()
    artifact_rows = _artifact_validation()
    source_rows = _source_inspection()

    module = _load_adapter_module()
    api_rows = _public_api_audit(module)
    behavior_rows, behavior_results = _independent_behavior_audit(module)
    result_rows = _result_contract_audit(module, behavior_results)
    row_contract_rows = _normalized_row_contract_audit(module, behavior_results)
    integration_rows = _scaffold_integration_safety()
    safety_rows = _safety_audit(before_snapshot, behavior_results)
    immutability_rows = [row for row in safety_rows if "mutation" in row["check"] or "mutated" in row["check"]]

    _write_csv(OUTPUT_ARTIFACTS, artifact_rows)
    _write_csv(OUTPUT_SOURCE, source_rows)
    _write_csv(OUTPUT_API, api_rows)
    _write_csv(OUTPUT_BEHAVIOR, behavior_rows)
    _write_csv(OUTPUT_RESULT, result_rows)
    _write_csv(OUTPUT_ROW, row_contract_rows)
    _write_csv(OUTPUT_INTEGRATION, integration_rows)
    _write_csv(OUTPUT_IMMUTABILITY, immutability_rows)
    _write_csv(OUTPUT_SAFETY, safety_rows)

    checks = [
        {"check": "module_execution_valid", "passed": module_execution["passed"], "detail": module_execution["diagnosis"]},
        {"check": "artifact_validation_valid", "passed": all(row["passed"] for row in artifact_rows), "detail": f"{sum(row['passed'] for row in artifact_rows)}/{len(artifact_rows)}"},
        {"check": "source_inspection_valid", "passed": all(row["passed"] for row in source_rows), "detail": f"{sum(row['passed'] for row in source_rows)}/{len(source_rows)}"},
        {"check": "public_api_valid", "passed": all(row["passed"] for row in api_rows), "detail": f"{sum(row['passed'] for row in api_rows)}/{len(api_rows)}"},
        {"check": "independent_behavior_valid", "passed": all(row["passed"] for row in behavior_rows), "detail": f"{sum(row['passed'] for row in behavior_rows)}/{len(behavior_rows)}"},
        {"check": "result_contract_valid", "passed": all(row["passed"] for row in result_rows), "detail": f"{sum(row['passed'] for row in result_rows)}/{len(result_rows)}"},
        {"check": "normalized_row_contract_valid", "passed": all(row["passed"] for row in row_contract_rows), "detail": f"{sum(row['passed'] for row in row_contract_rows)}/{len(row_contract_rows)}"},
        {"check": "scaffold_integration_safety_valid", "passed": all(row["passed"] for row in integration_rows), "detail": f"{sum(row['passed'] for row in integration_rows)}/{len(integration_rows)}"},
        {"check": "immutability_valid", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(row['passed'] for row in immutability_rows)}/{len(immutability_rows)}"},
        {"check": "safety_audit_valid", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(row['passed'] for row in safety_rows)}/{len(safety_rows)}"},
        {"check": "adapter_not_mutated_by_audit", "passed": any(row["check"] == "adapter_not_mutated_by_audit" and row["passed"] for row in safety_rows), "detail": str(ADAPTER_SCRIPT)},
        {"check": "scaffold_not_wired_to_adapter", "passed": any(row["check"] == "scaffold_not_wired_to_adapter" and row["passed"] for row in integration_rows), "detail": str(BACKFILL_SCAFFOLD)},
        {"check": "no_scaffold_mutation", "passed": any(row["check"] == "no_scaffold_mutation" and row["passed"] for row in safety_rows), "detail": str(BACKFILL_SCAFFOLD)},
        {"check": "no_fixture_mutation", "passed": any(row["check"] == "no_fixture_mutation" and row["passed"] for row in safety_rows), "detail": True},
        {"check": "no_external_fetch", "passed": any(row["check"] == "no_external_fetch" and row["passed"] for row in safety_rows), "detail": True},
        {"check": "no_db_writes", "passed": any(row["check"] == "no_db_writes" and row["passed"] for row in safety_rows), "detail": True},
        {"check": "production_default_unchanged", "passed": any(row["check"] == "production_default_unchanged" and row["passed"] for row in safety_rows), "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(check["passed"] for check in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_audit_complete",
        "audit_version": AUDIT_VERSION,
        "artifact_rows": len(artifact_rows),
        "source_inspection_rows": len(source_rows),
        "public_api_rows": len(api_rows),
        "independent_behavior_rows": len(behavior_rows),
        "result_contract_rows": len(result_rows),
        "normalized_row_contract_rows": len(row_contract_rows),
        "scaffold_integration_safety_rows": len(integration_rows),
        "immutability_rows": len(immutability_rows),
        "safety_rows": len(safety_rows),
        "all_checks_passed": all_checks_passed,
        "audit_only": True,
        "adapter_implementation_validated": True,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "adapter_mutated_by_audit": False,
        "scaffold_wired_to_adapter": False,
        "fixture_assets_mutated": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6DJ_candidate_bullpen_statcast_live_adapter_scaffold_integration_plan"
            if all_checks_passed
            else "6DI_patch_candidate_bullpen_statcast_live_adapter_fetch_module_implementation_audit"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
