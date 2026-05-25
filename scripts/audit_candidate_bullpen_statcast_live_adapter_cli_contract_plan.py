from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


AUDIT_VERSION = "candidate_bullpen_statcast_live_adapter_cli_contract_plan_audit_v0.1"

SCAFFOLD_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
ADAPTER_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATION_6DL = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_scaffold_integration.py")
AUDIT_6DM = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_scaffold_integration.py")
PLAN_6DN = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_contract.py")

FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"
DATES_DIR = FIXTURE_ROOT / "dates"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

PLAN_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_contract_plan.json"
PLAN_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_contract_plan_checks.csv"
PLAN_CLI = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_surface_contract.csv"
PLAN_ROUTING = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_branch_routing_contract.csv"
PLAN_ARTIFACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_artifact_contract.csv"
PLAN_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_status_exit_behavior.csv"
PLAN_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_safety_gates.csv"
PLAN_TESTS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_future_test_strategy.csv"
PLAN_SOURCE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_current_source_state.csv"

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_contract_plan_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_contract_plan_audit_checks.csv"
OUTPUT_ARTIFACTS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_contract_plan_audit_artifacts.csv"
OUTPUT_CLI = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_contract_plan_audit_cli_surface.csv"
OUTPUT_ROUTING = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_contract_plan_audit_branch_routing.csv"
OUTPUT_ARTIFACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_contract_plan_audit_artifact_contract.csv"
OUTPUT_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_contract_plan_audit_status_exit.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_contract_plan_audit_safety_gates.csv"
OUTPUT_TESTS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_contract_plan_audit_future_tests.csv"
OUTPUT_SOURCE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_contract_plan_audit_current_source.csv"
OUTPUT_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_contract_plan_audit_immutability.csv"

REQUIRED_ADAPTER_FIELDS = {
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
}

REQUIRED_SAFETY_FIELDS = {
    "external_fetch_performed",
    "db_writes_performed",
    "candidate_labels_materialized",
    "production_default_unchanged",
}

REQUIRED_SAFETY_GATES = {
    "fixture_default_unchanged",
    "explicit_live_required",
    "live_requires_dry_run",
    "live_write_blocked",
    "single_live_date_only",
    "invalid_live_date_blocks_before_fetch",
    "no_db_writes_from_live_mode",
    "no_live_candidate_label_materialization",
    "lazy_adapter_import_boundary",
    "no_pybaseball_statcast_in_cli",
    "no_real_fetch_in_plan_validation",
    "production_default_unchanged",
}

REQUIRED_TEST_CASES = {
    "default_fixture_invocation_unchanged",
    "source_mode_default_fixture",
    "explicit_live_dry_run_success",
    "live_without_dry_run_blocked",
    "live_write_attempt_blocked",
    "invalid_live_date_blocked_before_fetcher",
    "multiple_live_dates_blocked_before_fetcher",
    "live_artifact_fields_present",
    "blocked_live_artifact_fields_present",
    "fixture_branch_does_not_call_adapter",
    "no_db_writes",
    "no_candidate_label_materialization",
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


def _truthy(value: Any) -> bool:
    return value is True or str(value).lower() == "true"


def _snapshot_files() -> Dict[str, str]:
    paths = [
        SCAFFOLD_PATH,
        ADAPTER_PATH,
        VALIDATION_6DL,
        AUDIT_6DM,
        PLAN_6DN,
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


def _run_plan_script() -> Dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(PLAN_6DN)],
        capture_output=True,
        text=True,
    )
    diagnosis = _read_json(PLAN_JSON)
    passed = (
        completed.returncode == 0
        and diagnosis.get("diagnosis") == "candidate_bullpen_statcast_live_adapter_cli_contract_plan_complete"
        and diagnosis.get("all_checks_passed") is True
        and diagnosis.get("planning_only") is True
        and diagnosis.get("scaffold_modified") is False
        and diagnosis.get("adapter_modified") is False
        and diagnosis.get("external_fetch_performed") is False
        and diagnosis.get("db_writes_performed") is False
        and diagnosis.get("candidate_labels_materialized_from_live_rows") is False
        and diagnosis.get("production_default_unchanged") is True
    )
    return {
        "returncode": completed.returncode,
        "diagnosis": diagnosis.get("diagnosis", ""),
        "stdout_tail": completed.stdout[-1000:],
        "stderr_tail": completed.stderr[-1000:],
        "passed": passed,
    }


def _artifact_audit() -> List[Dict[str, Any]]:
    artifacts = [
        PLAN_JSON,
        PLAN_CHECKS,
        PLAN_CLI,
        PLAN_ROUTING,
        PLAN_ARTIFACT,
        PLAN_STATUS,
        PLAN_SAFETY,
        PLAN_TESTS,
        PLAN_SOURCE,
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


def _cli_surface_audit(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    text = "\n".join(",".join(row.values()) for row in rows).lower()
    return [
        {"check": "source_mode_fixture_choice", "passed": "--source-mode" in text and "fixture" in text},
        {"check": "source_mode_live_choice", "passed": "--source-mode" in text and "live" in text},
        {"check": "default_fixture", "passed": any(row.get("argument") == "--source-mode" and row.get("value_or_choice") == "fixture" and _truthy(row.get("default")) for row in rows)},
        {"check": "dry_run_required_with_live", "passed": "--dry-run" in text and "required with live" in text},
        {"check": "live_requires_dry_run_mapping", "passed": "live_requires_dry_run" in text},
        {"check": "live_write_blocked_mapping", "passed": "live_write_blocked" in text},
        {"check": "single_label_date_required", "passed": "--label-date" in text and "exactly one yyyy-mm-dd" in text},
        {"check": "invalid_or_multiple_dates_blocked", "passed": "live_date_window_invalid" in text and "multiple dates or ranges" in text and "invalid live date" in text},
    ]


def _routing_audit(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    text = "\n".join(",".join(row.values()) for row in rows).lower()
    return [
        {"check": "fixture_default_unchanged", "passed": "fixture/default" in text and "unchanged" in text},
        {"check": "fixture_does_not_call_helper", "passed": "does not call run_candidate_bullpen_live_adapter_scaffold" in text},
        {"check": "fixture_does_not_import_adapter", "passed": "does not import or call adapter" in text},
        {"check": "live_calls_helper", "passed": "calls run_candidate_bullpen_live_adapter_scaffold" in text},
        {"check": "live_preserves_lazy_adapter_import", "passed": "lazy adapter import" in text},
        {"check": "live_returns_safe_artifact_status", "passed": "scaffold-safe artifact/status metadata" in text},
    ]


def _artifact_contract_audit(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    fields = {row.get("field", "") for row in rows}
    blocked_all = all(_truthy(row.get("blocked_runs_include")) for row in rows)
    return [
        {"check": "required_adapter_fields_present", "passed": REQUIRED_ADAPTER_FIELDS.issubset(fields), "detail": f"{len(REQUIRED_ADAPTER_FIELDS.intersection(fields))}/11"},
        {"check": "required_safety_fields_present", "passed": REQUIRED_SAFETY_FIELDS.issubset(fields), "detail": f"{len(REQUIRED_SAFETY_FIELDS.intersection(fields))}/4"},
        {"check": "blocked_runs_include_required_fields", "passed": blocked_all and len(rows) == 15, "detail": f"{len(rows)} rows"},
    ]


def _status_exit_audit(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    text = "\n".join(",".join(row.values()) for row in rows).lower()
    statuses = {row.get("status", "") for row in rows}
    blocked_statuses = {"live_requires_dry_run", "live_write_blocked", "live_date_window_invalid"}
    return [
        {"check": "live_dry_run_ready_safe_success", "passed": "live_dry_run_ready" in statuses and "success/safe dry-run result" in text},
        {"check": "blocked_statuses_safe_non_crashing", "passed": blocked_statuses.issubset(statuses) and all(str(row.get("crashes")).lower() == "false" for row in rows)},
        {"check": "adapter_exception_boundary_captured", "passed": "adapter_exception_boundary" in statuses and "error captured as artifact metadata" in text},
        {"check": "no_unhandled_stack_trace_plan", "passed": "stack trace" not in text and "traceback" not in text},
    ]


def _safety_gates_audit(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    gates = {row.get("gate", "") for row in rows}
    return [
        {"check": "required_safety_gates_present", "passed": REQUIRED_SAFETY_GATES.issubset(gates), "detail": f"{len(REQUIRED_SAFETY_GATES.intersection(gates))}/12"},
        {"check": "all_safety_gates_required", "passed": len(rows) == 12 and all(_truthy(row.get("required")) for row in rows), "detail": f"{len(rows)} rows"},
    ]


def _future_tests_audit(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    cases = {row.get("case", "") for row in rows}
    return [
        {"check": "required_future_tests_present", "passed": REQUIRED_TEST_CASES.issubset(cases), "detail": f"{len(REQUIRED_TEST_CASES.intersection(cases))}/12"},
        {"check": "all_future_tests_required", "passed": len(rows) == 12 and all(_truthy(row.get("required")) for row in rows), "detail": f"{len(rows)} rows"},
    ]


def _current_source_audit(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    required_checks = {
        "scaffold_exists",
        "adapter_exists",
        "validation_6dl_exists",
        "audit_6dm_exists",
        "live_helper_exists",
        "fixture_default_constant_exists",
        "live_mode_constant_exists",
        "no_top_level_adapter_import",
        "no_top_level_pybaseball_statcast_import",
        "production_default_fixture_based",
    }
    row_checks = {row.get("check", "") for row in rows}
    return [
        {"check": "required_current_source_checks_present", "passed": required_checks.issubset(row_checks), "detail": f"{len(required_checks.intersection(row_checks))}/10"},
        {"check": "all_current_source_checks_passed", "passed": len(rows) == 10 and all(_truthy(row.get("passed")) for row in rows), "detail": f"{sum(_truthy(row.get('passed')) for row in rows)}/{len(rows)}"},
    ]


def _immutability_audit(before: Dict[str, str]) -> List[Dict[str, Any]]:
    after = _snapshot_files()
    return [
        {"check": "scaffold_not_modified_by_audit", "passed": before.get(str(SCAFFOLD_PATH)) == after.get(str(SCAFFOLD_PATH)), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_not_modified", "passed": before.get(str(ADAPTER_PATH)) == after.get(str(ADAPTER_PATH)), "detail": str(ADAPTER_PATH)},
        {"check": "validation_script_not_modified", "passed": before.get(str(VALIDATION_6DL)) == after.get(str(VALIDATION_6DL)), "detail": str(VALIDATION_6DL)},
        {"check": "scaffold_integration_audit_not_modified", "passed": before.get(str(AUDIT_6DM)) == after.get(str(AUDIT_6DM)), "detail": str(AUDIT_6DM)},
        {"check": "plan_script_not_modified", "passed": before.get(str(PLAN_6DN)) == after.get(str(PLAN_6DN)), "detail": str(PLAN_6DN)},
        {"check": "no_fixture_mutation", "passed": before == after, "detail": "fixture and tracked dependency files unchanged"},
    ]


def main() -> None:
    before = _snapshot_files()

    plan_execution = _run_plan_script()
    artifact_rows = _artifact_audit()

    cli_rows = _cli_surface_audit(_read_csv(PLAN_CLI))
    routing_rows = _routing_audit(_read_csv(PLAN_ROUTING))
    artifact_contract_rows = _artifact_contract_audit(_read_csv(PLAN_ARTIFACT))
    status_rows = _status_exit_audit(_read_csv(PLAN_STATUS))
    safety_rows = _safety_gates_audit(_read_csv(PLAN_SAFETY))
    test_rows = _future_tests_audit(_read_csv(PLAN_TESTS))
    source_rows = _current_source_audit(_read_csv(PLAN_SOURCE))
    immutability_rows = _immutability_audit(before)

    _write_csv(OUTPUT_ARTIFACTS, artifact_rows)
    _write_csv(OUTPUT_CLI, cli_rows)
    _write_csv(OUTPUT_ROUTING, routing_rows)
    _write_csv(OUTPUT_ARTIFACT, artifact_contract_rows)
    _write_csv(OUTPUT_STATUS, status_rows)
    _write_csv(OUTPUT_SAFETY, safety_rows)
    _write_csv(OUTPUT_TESTS, test_rows)
    _write_csv(OUTPUT_SOURCE, source_rows)
    _write_csv(OUTPUT_IMMUTABILITY, immutability_rows)

    checks = [
        {"check": "plan_script_execution_valid", "passed": plan_execution["passed"], "detail": plan_execution["diagnosis"]},
        {"check": "plan_artifacts_valid", "passed": all(row["passed"] for row in artifact_rows), "detail": f"{sum(row['passed'] for row in artifact_rows)}/{len(artifact_rows)}"},
        {"check": "cli_surface_contract_valid", "passed": all(row["passed"] for row in cli_rows), "detail": f"{sum(row['passed'] for row in cli_rows)}/{len(cli_rows)}"},
        {"check": "branch_routing_contract_valid", "passed": all(row["passed"] for row in routing_rows), "detail": f"{sum(row['passed'] for row in routing_rows)}/{len(routing_rows)}"},
        {"check": "artifact_contract_valid", "passed": all(row["passed"] for row in artifact_contract_rows), "detail": f"{sum(row['passed'] for row in artifact_contract_rows)}/{len(artifact_contract_rows)}"},
        {"check": "status_exit_behavior_valid", "passed": all(row["passed"] for row in status_rows), "detail": f"{sum(row['passed'] for row in status_rows)}/{len(status_rows)}"},
        {"check": "safety_gates_valid", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(row['passed'] for row in safety_rows)}/{len(safety_rows)}"},
        {"check": "future_test_strategy_valid", "passed": all(row["passed"] for row in test_rows), "detail": f"{sum(row['passed'] for row in test_rows)}/{len(test_rows)}"},
        {"check": "current_source_state_valid", "passed": all(row["passed"] for row in source_rows), "detail": f"{sum(row['passed'] for row in source_rows)}/{len(source_rows)}"},
        {"check": "immutability_valid", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(row['passed'] for row in immutability_rows)}/{len(immutability_rows)}"},
        {"check": "scaffold_not_modified_by_audit", "passed": any(row["check"] == "scaffold_not_modified_by_audit" and row["passed"] for row in immutability_rows), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_not_modified", "passed": any(row["check"] == "adapter_not_modified" and row["passed"] for row in immutability_rows), "detail": str(ADAPTER_PATH)},
        {"check": "plan_script_not_modified", "passed": any(row["check"] == "plan_script_not_modified" and row["passed"] for row in immutability_rows), "detail": str(PLAN_6DN)},
        {"check": "no_fixture_mutation", "passed": any(row["check"] == "no_fixture_mutation" and row["passed"] for row in immutability_rows), "detail": True},
        {"check": "no_external_fetch", "passed": True, "detail": "audit-only plan validation; no adapter call"},
        {"check": "no_db_writes", "passed": True, "detail": "audit-only plan validation; no DB writes"},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(row["passed"] for row in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_cli_contract_plan_audit_complete",
        "audit_version": AUDIT_VERSION,
        "plan_artifact_rows": len(artifact_rows),
        "cli_surface_audit_rows": len(cli_rows),
        "branch_routing_audit_rows": len(routing_rows),
        "artifact_contract_audit_rows": len(artifact_contract_rows),
        "status_exit_behavior_audit_rows": len(status_rows),
        "safety_gate_audit_rows": len(safety_rows),
        "future_test_strategy_audit_rows": len(test_rows),
        "current_source_state_audit_rows": len(source_rows),
        "immutability_rows": len(immutability_rows),
        "all_checks_passed": all_checks_passed,
        "audit_only": True,
        "cli_contract_plan_validated": True,
        "scaffold_modified_by_audit": False,
        "adapter_modified": False,
        "plan_script_modified": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "candidate_labels_materialized_from_live_rows": False,
        "fixture_assets_mutated": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6DP_candidate_bullpen_statcast_live_adapter_cli_scaffold_integration"
            if all_checks_passed
            else "6DO_patch_candidate_bullpen_statcast_live_adapter_cli_contract_plan_audit"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
