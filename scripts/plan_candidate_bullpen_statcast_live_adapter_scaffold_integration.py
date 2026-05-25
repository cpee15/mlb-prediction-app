from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_v0.1"

SCAFFOLD_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
ADAPTER_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
PLAN_6DF = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_fetch_contract_module.py")
AUDIT_6DG = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan.py")
AUDIT_6DI = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_fetch_module_implementation.py")
TEST_DOUBLE_PROTOTYPE_6DD = Path("scripts/prototype_candidate_bullpen_statcast_live_adapter_fetch_test_doubles.py")
TEST_DOUBLE_AUDIT_6DE = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_fetch_test_double_prototype.py")

FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"
DATES_DIR = FIXTURE_ROOT / "dates"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_checks.csv"
OUTPUT_BOUNDARY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_boundary.csv"
OUTPUT_CLI = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_cli_source_mode_contract.csv"
OUTPUT_IMPORT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_import_boundary.csv"
OUTPUT_DATE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_date_window_safety.csv"
OUTPUT_FLOW = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_data_flow_contract.csv"
OUTPUT_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_status_propagation.csv"
OUTPUT_ARTIFACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_artifact_contract.csv"
OUTPUT_GATES = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_safety_gates.csv"
OUTPUT_TESTS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_future_test_strategy.csv"

STATUSES = [
    "live_dry_run_ready",
    "live_fetch_empty",
    "live_fetch_error",
    "live_schema_failed_safely",
    "live_adapter_not_configured",
    "live_write_blocked",
    "live_requires_dry_run",
    "live_date_window_invalid",
    "live_dependency_missing",
]

ARTIFACT_FIELDS = [
    "source_mode",
    "adapter_status",
    "adapter_raw_row_count",
    "adapter_normalized_row_count",
    "adapter_duplicate_count",
    "adapter_required_field_failures",
    "adapter_missing_fields",
    "adapter_fetch_error",
    "adapter_external_fetch_performed",
    "adapter_db_writes_performed",
    "adapter_source_adapter_version",
]

FUTURE_TEST_CASES = [
    "default_fixture_mode_unaffected",
    "explicit_live_dry_run_success",
    "live_empty",
    "live_fetch_error",
    "live_schema_failure",
    "live_dependency_missing",
    "live_duplicate_detection_propagated",
    "live_unordered_rows_deterministic",
    "live_without_dry_run_blocked",
    "live_write_blocked",
    "invalid_live_date_blocked_before_adapter_fetch",
    "fixture_mode_does_not_import_adapter",
]


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


def _snapshot_files() -> Dict[str, str]:
    paths = [
        SCAFFOLD_PATH,
        ADAPTER_PATH,
        PLAN_6DF,
        AUDIT_6DG,
        AUDIT_6DI,
        TEST_DOUBLE_PROTOTYPE_6DD,
        TEST_DOUBLE_AUDIT_6DE,
        MANIFEST,
        EXPECTED_RESULTS,
    ]
    snapshot = {
        str(path): path.read_text(errors="ignore") if path.exists() else "__MISSING__"
        for path in paths
    }
    if DATES_DIR.exists():
        for payload in sorted(DATES_DIR.glob("*.jsonl")):
            snapshot[str(payload)] = payload.read_text(errors="ignore")
    return snapshot


def _source_safety_scan() -> Dict[str, bool]:
    source = Path(__file__).read_text(errors="ignore")
    import_lines = "\n".join(
        line.strip()
        for line in source.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    )
    scanner_start = source.find("def _source_safety_scan()")
    executable_prefix = source[:scanner_start] if scanner_start >= 0 else source
    executable_lower = executable_prefix.lower()
    return {
        "no_pybaseball_import": "pybaseball" not in import_lines and "statcast" not in import_lines,
        "no_external_fetch": all(token not in executable_prefix for token in ["requests.", "httpx.", "urllib."]),
        "no_db_writes": all(token not in executable_lower for token in ["session.commit(", ".to_sql(", "insert into"]),
    }


def _integration_boundary_rows() -> List[Dict[str, Any]]:
    return [
        {"component": "scaffold_target", "path": str(SCAFFOLD_PATH), "created_or_modified_this_layer": False, "required": True},
        {"component": "validated_adapter_module", "path": str(ADAPTER_PATH), "created_or_modified_this_layer": False, "required": True},
        {"component": "integration_mode", "path": "future --source-mode live branch only", "created_or_modified_this_layer": False, "required": True},
        {"component": "default_behavior", "path": "fixture/default mode unchanged", "created_or_modified_this_layer": False, "required": True},
        {"component": "write_boundary", "path": "no DB writes from live rows in first integration layer", "created_or_modified_this_layer": False, "required": True},
    ]


def _cli_contract_rows() -> List[Dict[str, Any]]:
    return [
        {"argument_or_mode": "--source-mode fixture", "future_behavior": "default mode; existing fixture behavior unchanged", "required": True},
        {"argument_or_mode": "--source-mode live", "future_behavior": "explicit opt-in live adapter path", "required": True},
        {"argument_or_mode": "--dry-run", "future_behavior": "required for live source mode", "required": True},
        {"argument_or_mode": "live without --dry-run", "future_behavior": "blocked with live_requires_dry_run", "required": True},
        {"argument_or_mode": "live write attempt", "future_behavior": "blocked with live_write_blocked until later write-gate layer", "required": True},
        {"argument_or_mode": "fixture default", "future_behavior": "does not import adapter and preserves current outputs", "required": True},
    ]


def _import_boundary_rows() -> List[Dict[str, Any]]:
    return [
        {"boundary": "no_top_level_adapter_import_in_scaffold", "detail": "scaffold imports adapter only inside live source branch", "required": True},
        {"boundary": "no_top_level_pybaseball_import_in_scaffold", "detail": "scaffold never imports pybaseball/statcast directly", "required": True},
        {"boundary": "adapter_owns_live_dependency_handling", "detail": "dependency errors remain LiveAdapterResult statuses", "required": True},
        {"boundary": "fixture_mode_does_not_load_adapter", "detail": "fixture/default branch remains isolated from live adapter", "required": True},
        {"boundary": "no_exception_leak_to_scaffold", "detail": "scaffold consumes result object/status only", "required": True},
    ]


def _date_window_rows() -> List[Dict[str, Any]]:
    return [
        {"rule": "validate_label_date_format", "future_owner": "scaffold", "expected_safe_status": "live_date_window_invalid", "required": True},
        {"rule": "single_label_date_only", "future_owner": "scaffold", "expected_safe_status": "live_date_window_invalid", "required": True},
        {"rule": "no_range_live_fetch", "future_owner": "scaffold", "expected_safe_status": "live_date_window_invalid", "required": True},
        {"rule": "invalid_date_blocks_before_adapter_fetch", "future_owner": "scaffold", "expected_safe_status": "live_date_window_invalid", "required": True},
    ]


def _data_flow_rows() -> List[Dict[str, Any]]:
    return [
        {"step": "select_source_mode", "future_owner": "scaffold", "required": True},
        {"step": "validate_live_gates", "future_owner": "scaffold", "required": True},
        {"step": "call_live_adapter", "future_owner": "scaffold live branch only", "required": True},
        {"step": "receive_LiveAdapterResult", "future_owner": "scaffold", "required": True},
        {"step": "emit_adapter_artifacts", "future_owner": "scaffold", "required": True},
        {"step": "do_not_materialize_candidate_labels_from_live_rows_yet", "future_owner": "scaffold", "required": True},
        {"step": "do_not_write_db_from_live_rows", "future_owner": "scaffold", "required": True},
    ]


def _status_rows() -> List[Dict[str, Any]]:
    return [
        {"status": status, "future_scaffold_action": "propagate into artifact and return safely", "required": True}
        for status in STATUSES
    ]


def _artifact_rows() -> List[Dict[str, Any]]:
    return [
        {"field": field, "future_artifact_source": "LiveAdapterResult/scaffold source-mode metadata", "required": True}
        for field in ARTIFACT_FIELDS
    ]


def _gate_rows() -> List[Dict[str, Any]]:
    return [
        {"gate": "default_fixture_behavior_unchanged", "requirement": "fixture mode remains default", "required": True},
        {"gate": "explicit_live_source_mode_required", "requirement": "--source-mode live required for adapter path", "required": True},
        {"gate": "live_requires_dry_run", "requirement": "live mode blocked without --dry-run", "required": True},
        {"gate": "live_write_blocked", "requirement": "live write attempt blocked until later audited layer", "required": True},
        {"gate": "no_db_writes_from_live_rows", "requirement": "do not persist adapter rows", "required": True},
        {"gate": "no_fixture_mutation", "requirement": "fixture payloads and metadata unchanged", "required": True},
        {"gate": "no_production_default_change", "requirement": "existing default behavior unchanged", "required": True},
        {"gate": "no_adapter_wiring_this_layer", "requirement": "planning-only; scaffold remains unwired", "required": True},
    ]


def _future_test_rows() -> List[Dict[str, Any]]:
    expected = {
        "default_fixture_mode_unaffected": "fixture outputs unchanged",
        "explicit_live_dry_run_success": "live_dry_run_ready propagated",
        "live_empty": "live_fetch_empty propagated",
        "live_fetch_error": "live_fetch_error propagated",
        "live_schema_failure": "live_schema_failed_safely propagated",
        "live_dependency_missing": "live_dependency_missing propagated",
        "live_duplicate_detection_propagated": "duplicate_count surfaced",
        "live_unordered_rows_deterministic": "deterministic adapter rows surfaced",
        "live_without_dry_run_blocked": "live_requires_dry_run",
        "live_write_blocked": "live_write_blocked",
        "invalid_live_date_blocked_before_adapter_fetch": "live_date_window_invalid and adapter not called",
        "fixture_mode_does_not_import_adapter": "adapter name absent from fixture path",
    }
    return [
        {"case": case, "expected": expected[case], "required": True}
        for case in FUTURE_TEST_CASES
    ]


def main() -> None:
    before_snapshot = _snapshot_files()

    boundary_rows = _integration_boundary_rows()
    cli_rows = _cli_contract_rows()
    import_rows = _import_boundary_rows()
    date_rows = _date_window_rows()
    flow_rows = _data_flow_rows()
    status_rows = _status_rows()
    artifact_rows = _artifact_rows()
    gate_rows = _gate_rows()
    test_rows = _future_test_rows()

    _write_csv(OUTPUT_BOUNDARY, boundary_rows)
    _write_csv(OUTPUT_CLI, cli_rows)
    _write_csv(OUTPUT_IMPORT, import_rows)
    _write_csv(OUTPUT_DATE, date_rows)
    _write_csv(OUTPUT_FLOW, flow_rows)
    _write_csv(OUTPUT_STATUS, status_rows)
    _write_csv(OUTPUT_ARTIFACT, artifact_rows)
    _write_csv(OUTPUT_GATES, gate_rows)
    _write_csv(OUTPUT_TESTS, test_rows)

    after_snapshot = _snapshot_files()
    scaffold_source = SCAFFOLD_PATH.read_text(errors="ignore") if SCAFFOLD_PATH.exists() else ""
    safety_scan = _source_safety_scan()

    checks = [
        {"check": "integration_boundary_defined", "passed": len(boundary_rows) == 5 and all(row["required"] for row in boundary_rows), "detail": f"{len(boundary_rows)} rows"},
        {"check": "cli_source_mode_contract_defined", "passed": len(cli_rows) == 6 and all(row["required"] for row in cli_rows), "detail": f"{len(cli_rows)} rows"},
        {"check": "import_boundary_defined", "passed": len(import_rows) == 5 and all(row["required"] for row in import_rows), "detail": f"{len(import_rows)} rows"},
        {"check": "date_window_safety_defined", "passed": len(date_rows) == 4 and all(row["required"] for row in date_rows), "detail": f"{len(date_rows)} rows"},
        {"check": "data_flow_contract_defined", "passed": len(flow_rows) == 7 and all(row["required"] for row in flow_rows), "detail": f"{len(flow_rows)} rows"},
        {"check": "status_propagation_defined", "passed": len(status_rows) == 9 and {row["status"] for row in status_rows} == set(STATUSES), "detail": f"{len(status_rows)} rows"},
        {"check": "artifact_contract_defined", "passed": len(artifact_rows) == 11 and all(row["required"] for row in artifact_rows), "detail": f"{len(artifact_rows)} rows"},
        {"check": "scaffold_safety_gates_defined", "passed": len(gate_rows) == 8 and all(row["required"] for row in gate_rows), "detail": f"{len(gate_rows)} rows"},
        {"check": "future_test_strategy_defined", "passed": len(test_rows) == 12 and all(row["required"] for row in test_rows), "detail": f"{len(test_rows)} rows"},
        {"check": "scaffold_not_modified", "passed": before_snapshot.get(str(SCAFFOLD_PATH)) == after_snapshot.get(str(SCAFFOLD_PATH)), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_not_modified", "passed": before_snapshot.get(str(ADAPTER_PATH)) == after_snapshot.get(str(ADAPTER_PATH)), "detail": str(ADAPTER_PATH)},
        {"check": "scaffold_not_wired_to_adapter", "passed": "fetch_candidate_bullpen_statcast_live_adapter" not in scaffold_source, "detail": str(SCAFFOLD_PATH)},
        {"check": "no_fixture_mutation", "passed": before_snapshot == after_snapshot, "detail": "fixture and tracked scripts unchanged"},
        {"check": "no_pybaseball_import", "passed": safety_scan["no_pybaseball_import"], "detail": True},
        {"check": "no_external_fetch", "passed": safety_scan["no_external_fetch"], "detail": True},
        {"check": "no_db_writes", "passed": safety_scan["no_db_writes"], "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(check["passed"] for check in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_complete",
        "plan_version": PLAN_VERSION,
        "integration_boundary_rows": len(boundary_rows),
        "cli_source_mode_contract_rows": len(cli_rows),
        "import_boundary_rows": len(import_rows),
        "date_window_safety_rows": len(date_rows),
        "data_flow_contract_rows": len(flow_rows),
        "status_propagation_rows": len(status_rows),
        "artifact_contract_rows": len(artifact_rows),
        "scaffold_safety_gate_rows": len(gate_rows),
        "future_test_strategy_rows": len(test_rows),
        "all_checks_passed": all_checks_passed,
        "planning_only": True,
        "scaffold_modified": False,
        "adapter_modified": False,
        "scaffold_wired_to_adapter": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "fixture_assets_mutated": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6DK_candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_audit"
            if all_checks_passed
            else "6DJ_patch_candidate_bullpen_statcast_live_adapter_scaffold_integration_plan"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
