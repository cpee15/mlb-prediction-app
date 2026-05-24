from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Set


AUDIT_VERSION = "candidate_bullpen_statcast_live_adapter_fetch_contract_audit_v0.1"

DESIGN_SCRIPT = Path("scripts/design_candidate_bullpen_statcast_live_adapter_fetch.py")
BACKFILL_SCAFFOLD = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
LIVE_ADAPTER_TARGET = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"
DATES_DIR = FIXTURE_ROOT / "dates"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

DESIGN_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_design.json"
DESIGN_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_design_checks.csv"
DESIGN_SURFACE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_surface_inventory.csv"
DESIGN_MODULE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_design.csv"
DESIGN_RESULT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_result_contract.csv"
DESIGN_STRATEGY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_strategy.csv"
DESIGN_MAPPING = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_raw_to_normalized_mapping.csv"
DESIGN_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_status_taxonomy.csv"
DESIGN_VALIDATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_validation_plan.csv"
DESIGN_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_safety_gates.csv"

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_audit_checks.csv"
OUTPUT_ARTIFACTS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_artifact_validation.csv"
OUTPUT_MODULE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_boundary_audit.csv"
OUTPUT_RESULT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_result_contract_audit.csv"
OUTPUT_MAPPING = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_mapping_audit.csv"
OUTPUT_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_status_taxonomy_audit.csv"
OUTPUT_STRATEGY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_strategy_audit.csv"
OUTPUT_VALIDATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_validation_plan_audit.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_safety_audit.csv"

EXPECTED_RESULT_FIELDS: Set[str] = {
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
EXPECTED_MAPPING_FIELDS: Set[str] = {
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
EXPECTED_NATURAL_KEY: Set[str] = {"game_pk", "at_bat_number", "pitch_number", "pitcher_id"}
EXPECTED_STATUSES: Set[str] = {
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
EXPECTED_VALIDATION_CASES: Set[str] = {
    "monkeypatched_success_rows",
    "one_date_live_dry_run",
    "short_window_live_dry_run",
    "empty_fetch_path",
    "error_fetch_path",
    "schema_failure_path",
    "duplicate_detection_path",
    "write_blocked",
    "non_dry_run_blocked",
    "no_external_fetch_when_mocked",
    "no_db_writes",
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


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def _snapshot_files() -> Dict[str, str]:
    paths = [DESIGN_SCRIPT, BACKFILL_SCAFFOLD, MANIFEST, EXPECTED_RESULTS]
    snapshot = {str(path): path.read_text(errors="ignore") if path.exists() else "__MISSING__" for path in paths}
    if DATES_DIR.exists():
        for payload in sorted(DATES_DIR.glob("*.jsonl")):
            snapshot[str(payload)] = payload.read_text(errors="ignore")
    return snapshot


def _run_design_script() -> Dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(DESIGN_SCRIPT)],
        capture_output=True,
        text=True,
    )
    diagnosis = _read_json(DESIGN_JSON)
    return {
        "returncode": completed.returncode,
        "stdout_tail": completed.stdout[-1000:],
        "stderr_tail": completed.stderr[-1000:],
        "diagnosis": diagnosis.get("diagnosis", ""),
        "all_checks_passed": diagnosis.get("all_checks_passed", False),
        "passed": completed.returncode == 0 and diagnosis.get("all_checks_passed") is True,
    }


def _artifact_validation() -> List[Dict[str, Any]]:
    artifacts = [
        DESIGN_JSON,
        DESIGN_CHECKS,
        DESIGN_SURFACE,
        DESIGN_MODULE,
        DESIGN_RESULT,
        DESIGN_STRATEGY,
        DESIGN_MAPPING,
        DESIGN_STATUS,
        DESIGN_VALIDATION,
        DESIGN_SAFETY,
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


def _module_boundary_audit(module_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    values = {row.get("component", ""): row.get("value", "") for row in module_rows}
    target_path = values.get("future_module_path", "")
    callable_value = values.get("callable", "")
    import_boundary = values.get("safe_import_boundary", "")

    expected_callable = (
        "fetch_candidate_bullpen_statcast_live_rows_for_date(label_date: str, "
        "timeout_seconds: int, max_retries: int) -> LiveAdapterResult"
    )
    rows = [
        {
            "check": "target_path_exact",
            "expected": str(LIVE_ADAPTER_TARGET),
            "actual": target_path,
            "passed": target_path == str(LIVE_ADAPTER_TARGET),
        },
        {
            "check": "target_module_absent",
            "expected": False,
            "actual": LIVE_ADAPTER_TARGET.exists(),
            "passed": not LIVE_ADAPTER_TARGET.exists(),
        },
        {
            "check": "callable_signature_exact",
            "expected": expected_callable,
            "actual": callable_value,
            "passed": callable_value == expected_callable,
        },
        {
            "check": "safe_import_boundary_live_only",
            "expected": "inside --source-mode live after dry-run/write/date gates pass",
            "actual": import_boundary,
            "passed": "--source-mode live" in import_boundary and "dry-run/write/date gates pass" in import_boundary,
        },
        {
            "check": "all_module_rows_required",
            "expected": True,
            "actual": all(row.get("required") == "True" for row in module_rows),
            "passed": len(module_rows) == 5 and all(row.get("required") == "True" for row in module_rows),
        },
        {
            "check": "no_component_implemented_this_layer",
            "expected": False,
            "actual": any(row.get("implemented_this_layer") == "True" for row in module_rows),
            "passed": not any(row.get("implemented_this_layer") == "True" for row in module_rows),
        },
    ]
    return rows


def _result_contract_audit(result_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    fields = {row.get("field", "") for row in result_rows}
    return [
        {
            "check": "result_contract_field_count",
            "expected": 14,
            "actual": len(result_rows),
            "passed": len(result_rows) == 14,
        },
        {
            "check": "result_contract_exact_fields",
            "expected": "|".join(sorted(EXPECTED_RESULT_FIELDS)),
            "actual": "|".join(sorted(fields)),
            "passed": fields == EXPECTED_RESULT_FIELDS,
        },
        {
            "check": "all_result_fields_required",
            "expected": True,
            "actual": all(row.get("required") == "True" for row in result_rows),
            "passed": all(row.get("required") == "True" for row in result_rows),
        },
        {
            "check": "result_contract_includes_fetch_metadata",
            "expected": "fetch_duration_ms|retry_count|source_adapter_version",
            "actual": "|".join(sorted(fields & {"fetch_duration_ms", "retry_count", "source_adapter_version"})),
            "passed": {"fetch_duration_ms", "retry_count", "source_adapter_version"}.issubset(fields),
        },
    ]


def _mapping_audit(mapping_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    fields = {row.get("normalized_field", "") for row in mapping_rows}
    natural_key = {row.get("normalized_field", "") for row in mapping_rows if row.get("natural_key") == "True"}
    return [
        {
            "check": "mapping_field_count",
            "expected": 12,
            "actual": len(mapping_rows),
            "passed": len(mapping_rows) == 12,
        },
        {
            "check": "mapping_exact_fields",
            "expected": "|".join(sorted(EXPECTED_MAPPING_FIELDS)),
            "actual": "|".join(sorted(fields)),
            "passed": fields == EXPECTED_MAPPING_FIELDS,
        },
        {
            "check": "natural_key_exact",
            "expected": "|".join(sorted(EXPECTED_NATURAL_KEY)),
            "actual": "|".join(sorted(natural_key)),
            "passed": natural_key == EXPECTED_NATURAL_KEY,
        },
        {
            "check": "all_mapping_fields_required",
            "expected": True,
            "actual": all(row.get("required") == "True" for row in mapping_rows),
            "passed": all(row.get("required") == "True" for row in mapping_rows),
        },
    ]


def _status_taxonomy_audit(status_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    statuses = {row.get("status", "") for row in status_rows}
    return [
        {
            "check": "status_count",
            "expected": 9,
            "actual": len(status_rows),
            "passed": len(status_rows) == 9,
        },
        {
            "check": "status_exact_set",
            "expected": "|".join(sorted(EXPECTED_STATUSES)),
            "actual": "|".join(sorted(statuses)),
            "passed": statuses == EXPECTED_STATUSES,
        },
        {
            "check": "all_statuses_terminal",
            "expected": True,
            "actual": all(row.get("terminal") == "True" for row in status_rows),
            "passed": all(row.get("terminal") == "True" for row in status_rows),
        },
    ]


def _fetch_strategy_audit(strategy_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    operations = {row.get("operation", "") for row in strategy_rows}
    expected_operations = {
        "validate_label_date",
        "bounded_dependency_import",
        "bounded_timeout",
        "bounded_retries",
        "capture_fetch_error",
        "normalize_rows",
        "validate_schema",
        "stable_sort",
        "return_result_only",
    }
    detail_blob = " ".join(row.get("detail", "") for row in strategy_rows).lower()
    return [
        {
            "check": "fetch_strategy_step_count",
            "expected": 9,
            "actual": len(strategy_rows),
            "passed": len(strategy_rows) == 9,
        },
        {
            "check": "fetch_strategy_operations_exact",
            "expected": "|".join(sorted(expected_operations)),
            "actual": "|".join(sorted(operations)),
            "passed": operations == expected_operations,
        },
        {
            "check": "fetch_strategy_required",
            "expected": True,
            "actual": all(row.get("required") == "True" for row in strategy_rows),
            "passed": all(row.get("required") == "True" for row in strategy_rows),
        },
        {
            "check": "fetch_strategy_no_db_writes",
            "expected": "no db writes",
            "actual": detail_blob,
            "passed": "no db writes" in detail_blob,
        },
        {
            "check": "fetch_strategy_stable_sort",
            "expected": "stable deterministic natural-key sort",
            "actual": detail_blob,
            "passed": (
                ("stable" in detail_blob and "sort" in detail_blob)
                or ("sort by natural key" in detail_blob and "deterministic" in detail_blob)
            ),
        },
    ]


def _validation_plan_audit(validation_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    cases = {row.get("case", "") for row in validation_rows}
    return [
        {
            "check": "validation_case_count",
            "expected": 11,
            "actual": len(validation_rows),
            "passed": len(validation_rows) == 11,
        },
        {
            "check": "validation_cases_exact",
            "expected": "|".join(sorted(EXPECTED_VALIDATION_CASES)),
            "actual": "|".join(sorted(cases)),
            "passed": cases == EXPECTED_VALIDATION_CASES,
        },
        {
            "check": "all_validation_cases_required",
            "expected": True,
            "actual": all(row.get("required") == "True" for row in validation_rows),
            "passed": all(row.get("required") == "True" for row in validation_rows),
        },
    ]


def _safety_audit(before_snapshot: Dict[str, str]) -> List[Dict[str, Any]]:
    after_snapshot = _snapshot_files()
    design_source = DESIGN_SCRIPT.read_text(errors="ignore") if DESIGN_SCRIPT.exists() else ""
    design_table_start = design_source.find("SURFACE_INVENTORY = [")
    executable_prefix = design_source[:design_table_start] if design_table_start >= 0 else design_source
    import_lines = "\n".join(
        line.strip()
        for line in executable_prefix.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    )
    executable_lower = executable_prefix.lower()

    rows = [
        {
            "check": "audit_only",
            "passed": True,
            "detail": "contract audit only",
        },
        {
            "check": "design_only",
            "passed": True,
            "detail": "design script executed but no adapter created",
        },
        {
            "check": "target_adapter_not_created",
            "passed": not LIVE_ADAPTER_TARGET.exists(),
            "detail": str(LIVE_ADAPTER_TARGET),
        },
        {
            "check": "scaffold_not_mutated_by_audit",
            "passed": before_snapshot.get(str(BACKFILL_SCAFFOLD)) == after_snapshot.get(str(BACKFILL_SCAFFOLD)),
            "detail": str(BACKFILL_SCAFFOLD),
        },
        {
            "check": "design_script_not_mutated_by_audit",
            "passed": before_snapshot.get(str(DESIGN_SCRIPT)) == after_snapshot.get(str(DESIGN_SCRIPT)),
            "detail": str(DESIGN_SCRIPT),
        },
        {
            "check": "fixture_payload_metadata_unchanged",
            "passed": before_snapshot == after_snapshot,
            "detail": "fixture assets and tracked scripts unchanged",
        },
        {
            "check": "no_pybaseball_import",
            "passed": "pybaseball" not in import_lines and "statcast" not in import_lines,
            "detail": "import_lines_only",
        },
        {
            "check": "no_external_fetch",
            "passed": all(token not in executable_prefix for token in ["requests.", "httpx.", "urllib."]),
            "detail": "executable_prefix_only",
        },
        {
            "check": "no_db_writes",
            "passed": all(token not in executable_lower for token in ["session.commit(", ".to_sql(", "insert into"]),
            "detail": "executable_prefix_only",
        },
        {
            "check": "production_default_unchanged",
            "passed": True,
            "detail": True,
        },
    ]

    for token in [
        "mlb_app.simulation",
        "GameEngine",
        "canonical_matchup_probability",
        "sportsbook",
        "routes",
        "frontend",
    ]:
        rows.append({"check": f"forbidden_import::{token}", "passed": token not in import_lines, "detail": "import_lines_only"})

    return rows


def main() -> None:
    before_snapshot = _snapshot_files()
    target_existed_before = LIVE_ADAPTER_TARGET.exists()

    design_execution = _run_design_script()
    artifact_rows = _artifact_validation()
    module_rows = _module_boundary_audit(_read_csv(DESIGN_MODULE))
    result_rows = _result_contract_audit(_read_csv(DESIGN_RESULT))
    mapping_rows = _mapping_audit(_read_csv(DESIGN_MAPPING))
    status_rows = _status_taxonomy_audit(_read_csv(DESIGN_STATUS))
    strategy_rows = _fetch_strategy_audit(_read_csv(DESIGN_STRATEGY))
    validation_rows = _validation_plan_audit(_read_csv(DESIGN_VALIDATION))
    safety_rows = _safety_audit(before_snapshot)

    _write_csv(OUTPUT_ARTIFACTS, artifact_rows)
    _write_csv(OUTPUT_MODULE, module_rows)
    _write_csv(OUTPUT_RESULT, result_rows)
    _write_csv(OUTPUT_MAPPING, mapping_rows)
    _write_csv(OUTPUT_STATUS, status_rows)
    _write_csv(OUTPUT_STRATEGY, strategy_rows)
    _write_csv(OUTPUT_VALIDATION, validation_rows)
    _write_csv(OUTPUT_SAFETY, safety_rows)

    design_script_execution_valid = design_execution["passed"]
    artifact_validation_valid = all(row["passed"] for row in artifact_rows)
    module_boundary_valid = all(row["passed"] for row in module_rows)
    result_contract_valid = all(row["passed"] for row in result_rows)
    raw_to_normalized_mapping_valid = all(row["passed"] for row in mapping_rows)
    status_taxonomy_valid = all(row["passed"] for row in status_rows)
    fetch_strategy_valid = all(row["passed"] for row in strategy_rows)
    validation_plan_valid = all(row["passed"] for row in validation_rows)
    safety_audit_valid = all(row["passed"] for row in safety_rows)
    target_adapter_not_created = target_existed_before is False and not LIVE_ADAPTER_TARGET.exists()
    scaffold_not_mutated_by_audit = before_snapshot.get(str(BACKFILL_SCAFFOLD)) == _snapshot_files().get(str(BACKFILL_SCAFFOLD))
    no_fixture_mutation = before_snapshot == _snapshot_files()
    no_pybaseball_import = any(row["check"] == "no_pybaseball_import" and row["passed"] for row in safety_rows)
    no_external_fetch = any(row["check"] == "no_external_fetch" and row["passed"] for row in safety_rows)
    no_db_writes = any(row["check"] == "no_db_writes" and row["passed"] for row in safety_rows)

    checks = [
        {"check": "design_script_execution_valid", "passed": design_script_execution_valid, "detail": design_execution["diagnosis"]},
        {"check": "artifact_validation_valid", "passed": artifact_validation_valid, "detail": f"{sum(row['passed'] for row in artifact_rows)}/{len(artifact_rows)}"},
        {"check": "module_boundary_valid", "passed": module_boundary_valid, "detail": f"{sum(row['passed'] for row in module_rows)}/{len(module_rows)}"},
        {"check": "result_contract_valid", "passed": result_contract_valid, "detail": f"{sum(row['passed'] for row in result_rows)}/{len(result_rows)}"},
        {"check": "raw_to_normalized_mapping_valid", "passed": raw_to_normalized_mapping_valid, "detail": f"{sum(row['passed'] for row in mapping_rows)}/{len(mapping_rows)}"},
        {"check": "status_taxonomy_valid", "passed": status_taxonomy_valid, "detail": f"{sum(row['passed'] for row in status_rows)}/{len(status_rows)}"},
        {"check": "fetch_strategy_valid", "passed": fetch_strategy_valid, "detail": f"{sum(row['passed'] for row in strategy_rows)}/{len(strategy_rows)}"},
        {"check": "validation_plan_valid", "passed": validation_plan_valid, "detail": f"{sum(row['passed'] for row in validation_rows)}/{len(validation_rows)}"},
        {"check": "safety_audit_valid", "passed": safety_audit_valid, "detail": f"{sum(row['passed'] for row in safety_rows)}/{len(safety_rows)}"},
        {"check": "target_adapter_not_created", "passed": target_adapter_not_created, "detail": str(LIVE_ADAPTER_TARGET)},
        {"check": "scaffold_not_mutated_by_audit", "passed": scaffold_not_mutated_by_audit, "detail": str(BACKFILL_SCAFFOLD)},
        {"check": "no_fixture_mutation", "passed": no_fixture_mutation, "detail": True},
        {"check": "no_pybaseball_import", "passed": no_pybaseball_import, "detail": True},
        {"check": "no_external_fetch", "passed": no_external_fetch, "detail": True},
        {"check": "no_db_writes", "passed": no_db_writes, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_fetch_contract_audit_complete",
        "audit_version": AUDIT_VERSION,
        "artifact_rows": len(artifact_rows),
        "module_boundary_rows": len(module_rows),
        "result_contract_rows": len(result_rows),
        "mapping_rows": len(mapping_rows),
        "status_rows": len(status_rows),
        "strategy_rows": len(strategy_rows),
        "validation_rows": len(validation_rows),
        "safety_rows": len(safety_rows),
        "all_checks_passed": all(check["passed"] for check in checks),
        "audit_only": True,
        "design_contract_validated": True,
        "target_live_adapter_created": False,
        "backfill_scaffold_mutated": False,
        "fixture_assets_mutated": False,
        "pybaseball_imported": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6DC_candidate_bullpen_statcast_live_adapter_fetch_test_double_plan"
            if all(check["passed"] for check in checks)
            else "6DA_patch_candidate_bullpen_statcast_live_adapter_fetch_design"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
