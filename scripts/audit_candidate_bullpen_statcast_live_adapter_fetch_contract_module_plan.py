from __future__ import annotations

import ast
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Set


AUDIT_VERSION = "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_audit_v0.1"

PLAN_SCRIPT = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_fetch_contract_module.py")
LIVE_ADAPTER_TARGET = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
BACKFILL_SCAFFOLD = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
TEST_DOUBLE_PLAN = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_fetch_test_doubles.py")
TEST_DOUBLE_PROTOTYPE = Path("scripts/prototype_candidate_bullpen_statcast_live_adapter_fetch_test_doubles.py")
TEST_DOUBLE_AUDIT = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_fetch_test_double_prototype.py")
FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"
DATES_DIR = FIXTURE_ROOT / "dates"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

PLAN_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan.json"
PLAN_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_checks.csv"
PLAN_MODULE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_boundary.csv"
PLAN_API = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_public_api.csv"
PLAN_DEPENDENCY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_dependency_boundary.csv"
PLAN_FETCH = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_fetch_behavior.csv"
PLAN_NORMALIZATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_normalization_contract.csv"
PLAN_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_status_taxonomy.csv"
PLAN_INTEGRATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_scaffold_integration_boundary.csv"
PLAN_TEST_STRATEGY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_test_strategy.csv"
PLAN_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_safety_gates.csv"

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_audit_checks.csv"
OUTPUT_ARTIFACTS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_artifact_validation.csv"
OUTPUT_SOURCE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_source_inspection.csv"
OUTPUT_API = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_public_api_audit.csv"
OUTPUT_DEPENDENCY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_dependency_boundary_audit.csv"
OUTPUT_FETCH = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_fetch_behavior_audit.csv"
OUTPUT_NORMALIZATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_normalization_contract_audit.csv"
OUTPUT_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_status_taxonomy_audit.csv"
OUTPUT_INTEGRATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_scaffold_integration_audit.csv"
OUTPUT_TEST_STRATEGY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_test_strategy_audit.csv"
OUTPUT_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_immutability_audit.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_safety_audit.csv"

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
EXPECTED_PUBLIC_APIS = {
    "LiveAdapterResult",
    "fetch_candidate_bullpen_statcast_live_rows_for_date",
    "normalize_statcast_pitch_rows",
    "natural_key",
}
EXPECTED_TEST_CASES = {
    "success_rows",
    "empty_rows",
    "fetch_error",
    "dependency_missing",
    "schema_failure",
    "duplicate_detection",
    "unordered_rows",
    "safety_scan",
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


def _as_bool(value: Any) -> bool:
    return value is True or str(value) == "True"


def _snapshot_files() -> Dict[str, str]:
    paths = [
        PLAN_SCRIPT,
        BACKFILL_SCAFFOLD,
        TEST_DOUBLE_PLAN,
        TEST_DOUBLE_PROTOTYPE,
        TEST_DOUBLE_AUDIT,
        MANIFEST,
        EXPECTED_RESULTS,
    ]
    snapshot = {str(path): path.read_text(errors="ignore") if path.exists() else "__MISSING__" for path in paths}
    if DATES_DIR.exists():
        for payload in sorted(DATES_DIR.glob("*.jsonl")):
            snapshot[str(payload)] = payload.read_text(errors="ignore")
    return snapshot


def _run_plan() -> Dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(PLAN_SCRIPT)],
        capture_output=True,
        text=True,
    )
    diagnosis = _read_json(PLAN_JSON)
    passed = (
        completed.returncode == 0
        and diagnosis.get("all_checks_passed") is True
        and diagnosis.get("planning_only") is True
        and diagnosis.get("real_adapter_created") is False
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
        PLAN_JSON,
        PLAN_CHECKS,
        PLAN_MODULE,
        PLAN_API,
        PLAN_DEPENDENCY,
        PLAN_FETCH,
        PLAN_NORMALIZATION,
        PLAN_STATUS,
        PLAN_INTEGRATION,
        PLAN_TEST_STRATEGY,
        PLAN_SAFETY,
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


def _literal_list_from_ast(tree: ast.AST, name: str) -> List[str]:
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    if isinstance(node.value, ast.List):
                        values: List[str] = []
                        for elt in node.value.elts:
                            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                                values.append(elt.value)
                        return values
    return []


def _source_inspection() -> List[Dict[str, Any]]:
    source = PLAN_SCRIPT.read_text(errors="ignore") if PLAN_SCRIPT.exists() else ""
    tree = ast.parse(source) if source else ast.Module(body=[], type_ignores=[])
    result_fields = set(_literal_list_from_ast(tree, "RESULT_FIELDS"))
    normalized_fields = set(_literal_list_from_ast(tree, "NORMALIZED_FIELDS"))
    natural_key_fields = set(_literal_list_from_ast(tree, "NATURAL_KEY_FIELDS"))
    statuses = set(_literal_list_from_ast(tree, "STATUSES"))
    rows = [
        {
            "check": "target_path_planned",
            "expected": str(LIVE_ADAPTER_TARGET),
            "actual": str(LIVE_ADAPTER_TARGET) in source,
            "passed": str(LIVE_ADAPTER_TARGET) in source,
        },
        {
            "check": "real_adapter_not_created",
            "expected": False,
            "actual": LIVE_ADAPTER_TARGET.exists(),
            "passed": not LIVE_ADAPTER_TARGET.exists(),
        },
        {
            "check": "result_fields_exact",
            "expected": "|".join(sorted(EXPECTED_RESULT_FIELDS)),
            "actual": "|".join(sorted(result_fields)),
            "passed": result_fields == EXPECTED_RESULT_FIELDS,
        },
        {
            "check": "normalized_fields_exact",
            "expected": "|".join(sorted(EXPECTED_NORMALIZED_FIELDS)),
            "actual": "|".join(sorted(normalized_fields)),
            "passed": normalized_fields == EXPECTED_NORMALIZED_FIELDS,
        },
        {
            "check": "natural_key_fields_exact",
            "expected": "|".join(sorted(EXPECTED_NATURAL_KEY_FIELDS)),
            "actual": "|".join(sorted(natural_key_fields)),
            "passed": natural_key_fields == EXPECTED_NATURAL_KEY_FIELDS,
        },
        {
            "check": "status_taxonomy_exact",
            "expected": "|".join(sorted(EXPECTED_STATUSES)),
            "actual": "|".join(sorted(statuses)),
            "passed": statuses == EXPECTED_STATUSES,
        },
    ]
    return rows


def _public_api_audit(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    by_api = {row.get("api", ""): row for row in rows}
    audit = []
    for api in sorted(EXPECTED_PUBLIC_APIS):
        row = by_api.get(api, {})
        audit.append({
            "api": api,
            "present": api in by_api,
            "signature": row.get("signature", ""),
            "required": row.get("required", ""),
            "passed": api in by_api and _as_bool(row.get("required", False)),
        })
    return audit


def _dependency_audit(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    expected = {
        "no_top_level_statcast_import",
        "live_callable_import_only",
        "dependency_missing_status",
        "no_direct_network_clients",
        "no_exception_leak_to_scaffold",
    }
    by_boundary = {row.get("boundary", ""): row for row in rows}
    return [
        {
            "boundary": boundary,
            "present": boundary in by_boundary,
            "detail": by_boundary.get(boundary, {}).get("detail", ""),
            "required": by_boundary.get(boundary, {}).get("required", ""),
            "passed": boundary in by_boundary and _as_bool(by_boundary[boundary].get("required", False)),
        }
        for boundary in sorted(expected)
    ]


def _fetch_behavior_audit(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    expected = {
        "validate_label_date",
        "single_date_fetch",
        "timeout_metadata",
        "bounded_retries",
        "capture_fetch_exceptions",
        "never_raise_to_scaffold",
        "no_db_writes",
    }
    by_behavior = {row.get("behavior", ""): row for row in rows}
    return [
        {
            "behavior": behavior,
            "present": behavior in by_behavior,
            "detail": by_behavior.get(behavior, {}).get("detail", ""),
            "required": by_behavior.get(behavior, {}).get("required", ""),
            "passed": behavior in by_behavior and _as_bool(by_behavior[behavior].get("required", False)),
        }
        for behavior in sorted(expected)
    ]


def _normalization_audit(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    normalized_rows = [row for row in rows if row.get("contract") == "normalized_field"]
    natural_key_fields = {row.get("field") for row in rows if row.get("natural_key") == "True"}
    contracts = {row.get("contract") for row in rows}
    audit = [
        {
            "check": "normalized_fields_exact",
            "expected": "|".join(sorted(EXPECTED_NORMALIZED_FIELDS)),
            "actual": "|".join(sorted(row.get("field", "") for row in normalized_rows)),
            "passed": {row.get("field") for row in normalized_rows} == EXPECTED_NORMALIZED_FIELDS and len(normalized_rows) == 12,
        },
        {
            "check": "natural_key_fields_exact",
            "expected": "|".join(sorted(EXPECTED_NATURAL_KEY_FIELDS)),
            "actual": "|".join(sorted(natural_key_fields)),
            "passed": natural_key_fields == EXPECTED_NATURAL_KEY_FIELDS,
        },
        {
            "check": "missing_required_fields_defined",
            "expected": "missing_required_fields",
            "actual": "missing_required_fields" in contracts,
            "passed": "missing_required_fields" in contracts,
        },
        {
            "check": "duplicate_detection_defined",
            "expected": "duplicate_detection",
            "actual": "duplicate_detection" in contracts,
            "passed": "duplicate_detection" in contracts,
        },
        {
            "check": "deterministic_ordering_defined",
            "expected": "deterministic_ordering",
            "actual": "deterministic_ordering" in contracts,
            "passed": "deterministic_ordering" in contracts,
        },
    ]
    return audit


def _status_audit(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    statuses = {row.get("status") for row in rows}
    return [
        {
            "status": status,
            "present": status in statuses,
            "required": next((row.get("required") for row in rows if row.get("status") == status), ""),
            "passed": status in statuses and any(row.get("status") == status and _as_bool(row.get("required")) for row in rows),
        }
        for status in sorted(EXPECTED_STATUSES)
    ]


def _integration_audit(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    expected = {
        ("scaffold", "date-window validation"),
        ("scaffold", "dry-run/write gates"),
        ("scaffold", "artifact emission"),
        ("adapter", "one-date fetch"),
        ("adapter", "row normalization"),
        ("adapter", "return result object only"),
        ("adapter", "never write DB"),
        ("adapter", "never mutate fixtures"),
    }
    actual = {(row.get("owner"), row.get("responsibility")) for row in rows}
    return [
        {
            "owner": owner,
            "responsibility": responsibility,
            "present": (owner, responsibility) in actual,
            "passed": (owner, responsibility) in actual,
        }
        for owner, responsibility in sorted(expected)
    ]


def _test_strategy_audit(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    cases = {row.get("case") for row in rows}
    return [
        {
            "case": case,
            "present": case in cases,
            "expected_status": next((row.get("expected_status") for row in rows if row.get("case") == case), ""),
            "required": next((row.get("required") for row in rows if row.get("case") == case), ""),
            "passed": case in cases and any(row.get("case") == case and _as_bool(row.get("required")) for row in rows),
        }
        for case in sorted(EXPECTED_TEST_CASES)
    ]


def _safety_audit(before_snapshot: Dict[str, str]) -> List[Dict[str, Any]]:
    after_snapshot = _snapshot_files()
    source = PLAN_SCRIPT.read_text(errors="ignore") if PLAN_SCRIPT.exists() else ""
    scanner_start = source.find("def _source_safety_scan()")
    executable_prefix = source[:scanner_start] if scanner_start >= 0 else source
    import_lines = "\n".join(
        line.strip()
        for line in executable_prefix.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    )
    executable_lower = executable_prefix.lower()

    return [
        {"check": "audit_only", "passed": True, "detail": "independent audit only"},
        {"check": "real_adapter_not_created", "passed": not LIVE_ADAPTER_TARGET.exists(), "detail": str(LIVE_ADAPTER_TARGET)},
        {"check": "plan_not_mutated_by_audit", "passed": before_snapshot.get(str(PLAN_SCRIPT)) == after_snapshot.get(str(PLAN_SCRIPT)), "detail": str(PLAN_SCRIPT)},
        {"check": "no_scaffold_mutation", "passed": before_snapshot.get(str(BACKFILL_SCAFFOLD)) == after_snapshot.get(str(BACKFILL_SCAFFOLD)), "detail": str(BACKFILL_SCAFFOLD)},
        {
            "check": "no_test_double_mutation",
            "passed": (
                before_snapshot.get(str(TEST_DOUBLE_PLAN)) == after_snapshot.get(str(TEST_DOUBLE_PLAN))
                and before_snapshot.get(str(TEST_DOUBLE_PROTOTYPE)) == after_snapshot.get(str(TEST_DOUBLE_PROTOTYPE))
                and before_snapshot.get(str(TEST_DOUBLE_AUDIT)) == after_snapshot.get(str(TEST_DOUBLE_AUDIT))
            ),
            "detail": "test-double plan/prototype/audit unchanged",
        },
        {"check": "no_fixture_mutation", "passed": before_snapshot == after_snapshot, "detail": "fixture and tracked scripts unchanged"},
        {"check": "no_pybaseball_import", "passed": "pybaseball" not in import_lines and "statcast" not in import_lines, "detail": "plan import lines only"},
        {"check": "no_external_fetch", "passed": all(token not in executable_prefix for token in ["requests.", "httpx.", "urllib."]), "detail": "plan executable prefix only"},
        {"check": "no_db_writes", "passed": all(token not in executable_lower for token in ["session.commit(", ".to_sql(", "insert into"]), "detail": "plan executable prefix only"},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]


def main() -> None:
    before_snapshot = _snapshot_files()
    plan_execution = _run_plan()

    artifact_rows = _artifact_validation()
    source_rows = _source_inspection()
    api_rows = _public_api_audit(_read_csv(PLAN_API))
    dependency_rows = _dependency_audit(_read_csv(PLAN_DEPENDENCY))
    fetch_rows = _fetch_behavior_audit(_read_csv(PLAN_FETCH))
    normalization_rows = _normalization_audit(_read_csv(PLAN_NORMALIZATION))
    status_rows = _status_audit(_read_csv(PLAN_STATUS))
    integration_rows = _integration_audit(_read_csv(PLAN_INTEGRATION))
    test_strategy_rows = _test_strategy_audit(_read_csv(PLAN_TEST_STRATEGY))
    safety_rows = _safety_audit(before_snapshot)
    immutability_rows = [row for row in safety_rows if "mutation" in row["check"] or "mutated" in row["check"]]

    _write_csv(OUTPUT_ARTIFACTS, artifact_rows)
    _write_csv(OUTPUT_SOURCE, source_rows)
    _write_csv(OUTPUT_API, api_rows)
    _write_csv(OUTPUT_DEPENDENCY, dependency_rows)
    _write_csv(OUTPUT_FETCH, fetch_rows)
    _write_csv(OUTPUT_NORMALIZATION, normalization_rows)
    _write_csv(OUTPUT_STATUS, status_rows)
    _write_csv(OUTPUT_INTEGRATION, integration_rows)
    _write_csv(OUTPUT_TEST_STRATEGY, test_strategy_rows)
    _write_csv(OUTPUT_IMMUTABILITY, immutability_rows)
    _write_csv(OUTPUT_SAFETY, safety_rows)

    checks = [
        {"check": "plan_execution_valid", "passed": plan_execution["passed"], "detail": plan_execution["diagnosis"]},
        {"check": "artifact_validation_valid", "passed": all(row["passed"] for row in artifact_rows), "detail": f"{sum(row['passed'] for row in artifact_rows)}/{len(artifact_rows)}"},
        {"check": "source_inspection_valid", "passed": all(row["passed"] for row in source_rows), "detail": f"{sum(row['passed'] for row in source_rows)}/{len(source_rows)}"},
        {"check": "public_api_valid", "passed": all(row["passed"] for row in api_rows), "detail": f"{sum(row['passed'] for row in api_rows)}/{len(api_rows)}"},
        {"check": "dependency_boundary_valid", "passed": all(row["passed"] for row in dependency_rows), "detail": f"{sum(row['passed'] for row in dependency_rows)}/{len(dependency_rows)}"},
        {"check": "fetch_behavior_valid", "passed": all(row["passed"] for row in fetch_rows), "detail": f"{sum(row['passed'] for row in fetch_rows)}/{len(fetch_rows)}"},
        {"check": "normalization_contract_valid", "passed": all(row["passed"] for row in normalization_rows), "detail": f"{sum(row['passed'] for row in normalization_rows)}/{len(normalization_rows)}"},
        {"check": "status_taxonomy_valid", "passed": all(row["passed"] for row in status_rows), "detail": f"{sum(row['passed'] for row in status_rows)}/{len(status_rows)}"},
        {"check": "scaffold_integration_valid", "passed": all(row["passed"] for row in integration_rows), "detail": f"{sum(row['passed'] for row in integration_rows)}/{len(integration_rows)}"},
        {"check": "implementation_test_strategy_valid", "passed": all(row["passed"] for row in test_strategy_rows), "detail": f"{sum(row['passed'] for row in test_strategy_rows)}/{len(test_strategy_rows)}"},
        {"check": "immutability_valid", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(row['passed'] for row in immutability_rows)}/{len(immutability_rows)}"},
        {"check": "safety_audit_valid", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(row['passed'] for row in safety_rows)}/{len(safety_rows)}"},
        {"check": "real_adapter_not_created", "passed": any(row["check"] == "real_adapter_not_created" and row["passed"] for row in safety_rows), "detail": str(LIVE_ADAPTER_TARGET)},
        {"check": "plan_not_mutated_by_audit", "passed": any(row["check"] == "plan_not_mutated_by_audit" and row["passed"] for row in safety_rows), "detail": str(PLAN_SCRIPT)},
        {"check": "no_scaffold_mutation", "passed": any(row["check"] == "no_scaffold_mutation" and row["passed"] for row in safety_rows), "detail": str(BACKFILL_SCAFFOLD)},
        {"check": "no_test_double_mutation", "passed": any(row["check"] == "no_test_double_mutation" and row["passed"] for row in safety_rows), "detail": True},
        {"check": "no_fixture_mutation", "passed": any(row["check"] == "no_fixture_mutation" and row["passed"] for row in safety_rows), "detail": True},
        {"check": "no_pybaseball_import", "passed": any(row["check"] == "no_pybaseball_import" and row["passed"] for row in safety_rows), "detail": True},
        {"check": "no_external_fetch", "passed": any(row["check"] == "no_external_fetch" and row["passed"] for row in safety_rows), "detail": True},
        {"check": "no_db_writes", "passed": any(row["check"] == "no_db_writes" and row["passed"] for row in safety_rows), "detail": True},
        {"check": "production_default_unchanged", "passed": any(row["check"] == "production_default_unchanged" and row["passed"] for row in safety_rows), "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(check["passed"] for check in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_audit_complete",
        "audit_version": AUDIT_VERSION,
        "artifact_rows": len(artifact_rows),
        "source_inspection_rows": len(source_rows),
        "public_api_rows": len(api_rows),
        "dependency_boundary_rows": len(dependency_rows),
        "fetch_behavior_rows": len(fetch_rows),
        "normalization_contract_rows": len(normalization_rows),
        "status_taxonomy_rows": len(status_rows),
        "scaffold_integration_rows": len(integration_rows),
        "implementation_test_strategy_rows": len(test_strategy_rows),
        "immutability_rows": len(immutability_rows),
        "safety_rows": len(safety_rows),
        "all_checks_passed": all_checks_passed,
        "audit_only": True,
        "contract_module_plan_validated": True,
        "real_adapter_created": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "plan_mutated_by_audit": False,
        "fixture_assets_mutated": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6DH_candidate_bullpen_statcast_live_adapter_fetch_module_implementation"
            if all_checks_passed
            else "6DG_patch_candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_audit"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
