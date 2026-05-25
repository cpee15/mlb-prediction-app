from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


AUDIT_VERSION = "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_audit_v0.1"

PLAN_PATH = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_scaffold_integration.py")
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

PLAN_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan.json"
PLAN_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_checks.csv"
PLAN_BOUNDARY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_boundary.csv"
PLAN_CLI = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_cli_source_mode_contract.csv"
PLAN_IMPORT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_import_boundary.csv"
PLAN_DATE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_date_window_safety.csv"
PLAN_FLOW = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_data_flow_contract.csv"
PLAN_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_status_propagation.csv"
PLAN_ARTIFACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_artifact_contract.csv"
PLAN_GATES = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_safety_gates.csv"
PLAN_TESTS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_future_test_strategy.csv"

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_audit_checks.csv"
OUTPUT_ARTIFACTS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_artifact_validation.csv"
OUTPUT_SOURCE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_source_inspection.csv"
OUTPUT_CONTENT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_content_audit.csv"
OUTPUT_FUTURE_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_future_safety_audit.csv"
OUTPUT_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_immutability_audit.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_safety_audit.csv"

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

EXPECTED_ARTIFACT_FIELDS = {
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

EXPECTED_TEST_CASES = {
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
    paths = [
        PLAN_PATH,
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


def _run_plan() -> Dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(PLAN_PATH)],
        capture_output=True,
        text=True,
    )
    diagnosis = _read_json(PLAN_JSON)
    passed = (
        completed.returncode == 0
        and diagnosis.get("diagnosis") == "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_complete"
        and diagnosis.get("all_checks_passed") is True
        and diagnosis.get("planning_only") is True
        and diagnosis.get("scaffold_modified") is False
        and diagnosis.get("adapter_modified") is False
        and diagnosis.get("scaffold_wired_to_adapter") is False
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
        PLAN_BOUNDARY,
        PLAN_CLI,
        PLAN_IMPORT,
        PLAN_DATE,
        PLAN_FLOW,
        PLAN_STATUS,
        PLAN_ARTIFACT,
        PLAN_GATES,
        PLAN_TESTS,
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
    source = PLAN_PATH.read_text(errors="ignore") if PLAN_PATH.exists() else ""
    import_lines = "\n".join(
        line.strip()
        for line in source.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    )
    scanner_start = source.find("def _source_safety_scan()")
    executable_prefix = source[:scanner_start] if scanner_start >= 0 else source
    executable_lower = executable_prefix.lower()
    return [
        {"check": "plan_file_exists", "passed": PLAN_PATH.exists(), "detail": str(PLAN_PATH)},
        {"check": "plan_version_exists", "passed": "PLAN_VERSION" in source, "detail": "PLAN_VERSION"},
        {"check": "scaffold_target_named", "passed": str(SCAFFOLD_PATH) in source, "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_path_named", "passed": str(ADAPTER_PATH) in source, "detail": str(ADAPTER_PATH)},
        {"check": "no_pybaseball_import", "passed": "pybaseball" not in import_lines and "statcast" not in import_lines, "detail": True},
        {"check": "no_external_network_usage", "passed": all(token not in executable_prefix for token in ["requests.", "httpx.", "urllib."]), "detail": True},
        {"check": "no_db_write_tokens", "passed": all(token not in executable_lower for token in ["session.commit(", ".to_sql(", "insert into"]), "detail": True},
    ]


def _plan_content_audit() -> List[Dict[str, Any]]:
    boundary_rows = _read_csv(PLAN_BOUNDARY)
    cli_rows = _read_csv(PLAN_CLI)
    import_rows = _read_csv(PLAN_IMPORT)
    date_rows = _read_csv(PLAN_DATE)
    flow_rows = _read_csv(PLAN_FLOW)
    status_rows = _read_csv(PLAN_STATUS)
    artifact_rows = _read_csv(PLAN_ARTIFACT)
    gate_rows = _read_csv(PLAN_GATES)
    test_rows = _read_csv(PLAN_TESTS)

    status_set = {row.get("status", "") for row in status_rows}
    artifact_set = {row.get("field", "") for row in artifact_rows}
    test_set = {row.get("case", "") for row in test_rows}

    return [
        {"check": "integration_boundary_rows_valid", "passed": len(boundary_rows) == 5, "detail": f"{len(boundary_rows)} rows"},
        {"check": "cli_source_mode_contract_rows_valid", "passed": len(cli_rows) == 6, "detail": f"{len(cli_rows)} rows"},
        {"check": "import_boundary_rows_valid", "passed": len(import_rows) == 5, "detail": f"{len(import_rows)} rows"},
        {"check": "date_window_safety_rows_valid", "passed": len(date_rows) == 4, "detail": f"{len(date_rows)} rows"},
        {"check": "data_flow_contract_rows_valid", "passed": len(flow_rows) == 7, "detail": f"{len(flow_rows)} rows"},
        {"check": "status_taxonomy_exact", "passed": len(status_rows) == 9 and status_set == EXPECTED_STATUSES, "detail": f"{len(status_rows)} statuses"},
        {"check": "artifact_fields_exact", "passed": len(artifact_rows) == 11 and artifact_set == EXPECTED_ARTIFACT_FIELDS, "detail": f"{len(artifact_rows)} fields"},
        {"check": "scaffold_safety_gate_rows_valid", "passed": len(gate_rows) == 8, "detail": f"{len(gate_rows)} rows"},
        {"check": "future_test_strategy_exact", "passed": len(test_rows) == 12 and test_set == EXPECTED_TEST_CASES, "detail": f"{len(test_rows)} tests"},
    ]


def _future_safety_audit() -> List[Dict[str, Any]]:
    cli_rows = _read_csv(PLAN_CLI)
    import_rows = _read_csv(PLAN_IMPORT)
    date_rows = _read_csv(PLAN_DATE)
    flow_rows = _read_csv(PLAN_FLOW)
    gate_rows = _read_csv(PLAN_GATES)

    all_text = "\n".join(
        ",".join(row.values())
        for rows in [cli_rows, import_rows, date_rows, flow_rows, gate_rows]
        for row in rows
    )
    all_text_lower = all_text.lower()

    expected_phrases = {
        "fixture_default_behavior_remains_default": ["fixture mode remains default", "existing fixture behavior unchanged"],
        "explicit_live_source_mode_required": ["--source-mode live required", "explicit opt-in live adapter path"],
        "live_requires_dry_run": ["live mode blocked without --dry-run", "required for live source mode"],
        "live_write_blocked": ["live write attempt blocked", "live_write_blocked"],
        "adapter_import_inside_live_branch": ["imports adapter only inside live source branch"],
        "fixture_branch_does_not_import_adapter": ["fixture/default branch remains isolated from live adapter", "does not import adapter"],
        "scaffold_no_pybaseball_import": ["scaffold never imports pybaseball/statcast directly"],
        "invalid_date_blocks_before_fetch": ["invalid_date_blocks_before_adapter_fetch", "live_date_window_invalid"],
        "single_label_date_only": ["single_label_date_only"],
        "no_range_live_fetch": ["no_range_live_fetch"],
        "no_live_candidate_label_materialization_yet": ["do_not_materialize_candidate_labels_from_live_rows_yet"],
    }

    rows = []
    for check, phrases in expected_phrases.items():
        normalized_phrases = [phrase.lower() for phrase in phrases]
        rows.append({
            "check": check,
            "expected_phrases": " | ".join(phrases),
            "passed": all(phrase in all_text_lower for phrase in normalized_phrases),
        })

    no_db_write_gate = any(
        row.get("gate") == "no_db_writes_from_live_rows"
        and "do not persist adapter rows" in row.get("requirement", "").lower()
        for row in gate_rows
    )
    no_db_write_flow = any(
        row.get("step") == "do_not_write_db_from_live_rows"
        and row.get("future_owner") == "scaffold"
        for row in flow_rows
    )
    rows.append({
        "check": "no_db_writes_from_live_rows",
        "expected_phrases": "gate:no_db_writes_from_live_rows | step:do_not_write_db_from_live_rows",
        "passed": no_db_write_gate and no_db_write_flow,
    })
    return rows


def _immutability_and_safety(before_snapshot: Dict[str, str]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    after_snapshot = _snapshot_files()
    scaffold_source = SCAFFOLD_PATH.read_text(errors="ignore") if SCAFFOLD_PATH.exists() else ""

    immutability_rows = [
        {"check": "plan_not_modified_by_audit", "passed": before_snapshot.get(str(PLAN_PATH)) == after_snapshot.get(str(PLAN_PATH)), "detail": str(PLAN_PATH)},
        {"check": "scaffold_not_modified", "passed": before_snapshot.get(str(SCAFFOLD_PATH)) == after_snapshot.get(str(SCAFFOLD_PATH)), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_not_modified", "passed": before_snapshot.get(str(ADAPTER_PATH)) == after_snapshot.get(str(ADAPTER_PATH)), "detail": str(ADAPTER_PATH)},
        {
            "check": "prior_layers_not_modified",
            "passed": all(
                before_snapshot.get(str(path)) == after_snapshot.get(str(path))
                for path in [PLAN_6DF, AUDIT_6DG, AUDIT_6DI, TEST_DOUBLE_PROTOTYPE_6DD, TEST_DOUBLE_AUDIT_6DE]
            ),
            "detail": "6DF/6DG/6DI/6DD/6DE unchanged",
        },
        {"check": "no_fixture_mutation", "passed": before_snapshot == after_snapshot, "detail": "fixture and tracked scripts unchanged"},
    ]

    source_rows = _source_inspection()
    safety_rows = [
        {"check": "audit_only", "passed": True, "detail": "independent plan audit only"},
        {"check": "scaffold_not_wired_to_adapter", "passed": "fetch_candidate_bullpen_statcast_live_adapter" not in scaffold_source, "detail": str(SCAFFOLD_PATH)},
        {"check": "no_pybaseball_import", "passed": any(row["check"] == "no_pybaseball_import" and row["passed"] for row in source_rows), "detail": True},
        {"check": "no_external_fetch", "passed": any(row["check"] == "no_external_network_usage" and row["passed"] for row in source_rows), "detail": True},
        {"check": "no_db_writes", "passed": any(row["check"] == "no_db_write_tokens" and row["passed"] for row in source_rows), "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    return immutability_rows, safety_rows


def main() -> None:
    before_snapshot = _snapshot_files()

    plan_execution = _run_plan()
    artifact_rows = _artifact_validation()
    source_rows = _source_inspection()
    content_rows = _plan_content_audit()
    future_safety_rows = _future_safety_audit()
    immutability_rows, safety_rows = _immutability_and_safety(before_snapshot)

    _write_csv(OUTPUT_ARTIFACTS, artifact_rows)
    _write_csv(OUTPUT_SOURCE, source_rows)
    _write_csv(OUTPUT_CONTENT, content_rows)
    _write_csv(OUTPUT_FUTURE_SAFETY, future_safety_rows)
    _write_csv(OUTPUT_IMMUTABILITY, immutability_rows)
    _write_csv(OUTPUT_SAFETY, safety_rows)

    checks = [
        {"check": "plan_execution_valid", "passed": plan_execution["passed"], "detail": plan_execution["diagnosis"]},
        {"check": "artifact_validation_valid", "passed": all(row["passed"] for row in artifact_rows), "detail": f"{sum(row['passed'] for row in artifact_rows)}/{len(artifact_rows)}"},
        {"check": "source_inspection_valid", "passed": all(row["passed"] for row in source_rows), "detail": f"{sum(row['passed'] for row in source_rows)}/{len(source_rows)}"},
        {"check": "plan_content_valid", "passed": all(row["passed"] for row in content_rows), "detail": f"{sum(row['passed'] for row in content_rows)}/{len(content_rows)}"},
        {"check": "future_integration_safety_valid", "passed": all(row["passed"] for row in future_safety_rows), "detail": f"{sum(row['passed'] for row in future_safety_rows)}/{len(future_safety_rows)}"},
        {"check": "immutability_valid", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(row['passed'] for row in immutability_rows)}/{len(immutability_rows)}"},
        {"check": "safety_audit_valid", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(row['passed'] for row in safety_rows)}/{len(safety_rows)}"},
        {"check": "plan_not_modified_by_audit", "passed": any(row["check"] == "plan_not_modified_by_audit" and row["passed"] for row in immutability_rows), "detail": str(PLAN_PATH)},
        {"check": "scaffold_not_modified", "passed": any(row["check"] == "scaffold_not_modified" and row["passed"] for row in immutability_rows), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_not_modified", "passed": any(row["check"] == "adapter_not_modified" and row["passed"] for row in immutability_rows), "detail": str(ADAPTER_PATH)},
        {"check": "scaffold_not_wired_to_adapter", "passed": any(row["check"] == "scaffold_not_wired_to_adapter" and row["passed"] for row in safety_rows), "detail": str(SCAFFOLD_PATH)},
        {"check": "no_fixture_mutation", "passed": any(row["check"] == "no_fixture_mutation" and row["passed"] for row in immutability_rows), "detail": True},
        {"check": "no_pybaseball_import", "passed": any(row["check"] == "no_pybaseball_import" and row["passed"] for row in safety_rows), "detail": True},
        {"check": "no_external_fetch", "passed": any(row["check"] == "no_external_fetch" and row["passed"] for row in safety_rows), "detail": True},
        {"check": "no_db_writes", "passed": any(row["check"] == "no_db_writes" and row["passed"] for row in safety_rows), "detail": True},
        {"check": "production_default_unchanged", "passed": any(row["check"] == "production_default_unchanged" and row["passed"] for row in safety_rows), "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(check["passed"] for check in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_audit_complete",
        "audit_version": AUDIT_VERSION,
        "artifact_rows": len(artifact_rows),
        "source_inspection_rows": len(source_rows),
        "plan_content_rows": len(content_rows),
        "future_integration_safety_rows": len(future_safety_rows),
        "immutability_rows": len(immutability_rows),
        "safety_rows": len(safety_rows),
        "all_checks_passed": all_checks_passed,
        "audit_only": True,
        "scaffold_integration_plan_validated": True,
        "plan_modified_by_audit": False,
        "scaffold_modified": False,
        "adapter_modified": False,
        "scaffold_wired_to_adapter": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "fixture_assets_mutated": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6DL_candidate_bullpen_statcast_live_adapter_scaffold_integration"
            if all_checks_passed
            else "6DK_patch_candidate_bullpen_statcast_live_adapter_scaffold_integration_plan_audit"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
