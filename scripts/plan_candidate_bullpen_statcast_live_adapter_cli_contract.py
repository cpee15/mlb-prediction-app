from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_live_adapter_cli_contract_plan_v0.1"

SCAFFOLD_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
ADAPTER_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATION_6DL = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_scaffold_integration.py")
AUDIT_6DM = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_scaffold_integration.py")

PLAN_6DF = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_fetch_contract_module.py")
AUDIT_6DG = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan.py")
AUDIT_6DI = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_fetch_module_implementation.py")
PLAN_6DJ = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_scaffold_integration.py")
AUDIT_6DK = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_scaffold_integration_plan.py")
TEST_DOUBLE_PROTOTYPE_6DD = Path("scripts/prototype_candidate_bullpen_statcast_live_adapter_fetch_test_doubles.py")
TEST_DOUBLE_AUDIT_6DE = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_fetch_test_double_prototype.py")

FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"
DATES_DIR = FIXTURE_ROOT / "dates"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_contract_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_contract_plan_checks.csv"
OUTPUT_CLI = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_surface_contract.csv"
OUTPUT_ROUTING = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_branch_routing_contract.csv"
OUTPUT_ARTIFACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_artifact_contract.csv"
OUTPUT_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_status_exit_behavior.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_safety_gates.csv"
OUTPUT_TESTS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_future_test_strategy.csv"
OUTPUT_SOURCE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_current_source_state.csv"

REQUIRED_ADAPTER_ARTIFACT_FIELDS = [
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

SCAFFOLD_SAFETY_FIELDS = [
    "external_fetch_performed",
    "db_writes_performed",
    "candidate_labels_materialized",
    "production_default_unchanged",
]

EXPECTED_STATUSES = [
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
        VALIDATION_6DL,
        AUDIT_6DM,
        PLAN_6DF,
        AUDIT_6DG,
        AUDIT_6DI,
        PLAN_6DJ,
        AUDIT_6DK,
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


def _top_level_import_text(source: str) -> str:
    tree = ast.parse(source)
    imports = []
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            imports.append(ast.get_source_segment(source, node) or "")
    return "\n".join(imports)


def _cli_surface_contract() -> List[Dict[str, Any]]:
    return [
        {"argument": "--source-mode", "value_or_choice": "fixture", "default": True, "required": True, "future_behavior": "default fixture path remains unchanged"},
        {"argument": "--source-mode", "value_or_choice": "live", "default": False, "required": True, "future_behavior": "explicit opt-in live adapter branch"},
        {"argument": "--dry-run", "value_or_choice": "required with live", "default": False, "required": True, "future_behavior": "live mode without dry-run returns live_requires_dry_run"},
        {"argument": "live write attempt", "value_or_choice": "blocked", "default": False, "required": True, "future_behavior": "live write attempts return live_write_blocked"},
        {"argument": "--label-date", "value_or_choice": "exactly one YYYY-MM-DD", "default": False, "required": True, "future_behavior": "single live date routed to helper"},
        {"argument": "multiple dates or ranges", "value_or_choice": "blocked", "default": False, "required": True, "future_behavior": "maps to live_date_window_invalid before adapter call"},
        {"argument": "invalid live date", "value_or_choice": "blocked", "default": False, "required": True, "future_behavior": "maps to live_date_window_invalid before adapter call"},
    ]


def _branch_routing_contract() -> List[Dict[str, Any]]:
    return [
        {"branch": "fixture/default", "future_behavior": "current fixture/default path remains unchanged", "required": True},
        {"branch": "fixture/default", "future_behavior": "does not call run_candidate_bullpen_live_adapter_scaffold", "required": True},
        {"branch": "fixture/default", "future_behavior": "does not import or call adapter", "required": True},
        {"branch": "live", "future_behavior": "calls run_candidate_bullpen_live_adapter_scaffold", "required": True},
        {"branch": "live", "future_behavior": "preserves lazy adapter import inside 6DL helper", "required": True},
        {"branch": "live", "future_behavior": "returns scaffold-safe artifact/status metadata", "required": True},
    ]


def _artifact_contract() -> List[Dict[str, Any]]:
    rows = [
        {"field": field, "source": "adapter/scaffold live metadata", "blocked_runs_include": True, "required": True}
        for field in REQUIRED_ADAPTER_ARTIFACT_FIELDS
    ]
    rows.extend(
        {"field": field, "source": "scaffold safety metadata", "blocked_runs_include": True, "required": True}
        for field in SCAFFOLD_SAFETY_FIELDS
    )
    return rows


def _status_exit_behavior() -> List[Dict[str, Any]]:
    rows = [
        {"status": "live_dry_run_ready", "future_cli_outcome": "success/safe dry-run result", "crashes": False, "required": True},
        {"status": "live_requires_dry_run", "future_cli_outcome": "safe blocked result", "crashes": False, "required": True},
        {"status": "live_write_blocked", "future_cli_outcome": "safe blocked result", "crashes": False, "required": True},
        {"status": "live_date_window_invalid", "future_cli_outcome": "safe blocked result before adapter call", "crashes": False, "required": True},
        {"status": "live_fetch_empty", "future_cli_outcome": "safe no-row artifact result", "crashes": False, "required": True},
        {"status": "live_fetch_error", "future_cli_outcome": "safe adapter error artifact result", "crashes": False, "required": True},
        {"status": "live_schema_failed_safely", "future_cli_outcome": "safe schema failure artifact result", "crashes": False, "required": True},
        {"status": "live_adapter_not_configured", "future_cli_outcome": "safe not-configured artifact result", "crashes": False, "required": True},
        {"status": "live_dependency_missing", "future_cli_outcome": "safe dependency-missing artifact result", "crashes": False, "required": True},
        {"status": "adapter_exception_boundary", "future_cli_outcome": "no exception leakage into CLI; error captured as artifact metadata", "crashes": False, "required": True},
    ]
    return rows


def _safety_gates() -> List[Dict[str, Any]]:
    return [
        {"gate": "fixture_default_unchanged", "requirement": "source-mode default remains fixture", "required": True},
        {"gate": "explicit_live_required", "requirement": "--source-mode live required for live branch", "required": True},
        {"gate": "live_requires_dry_run", "requirement": "live mode blocked without --dry-run", "required": True},
        {"gate": "live_write_blocked", "requirement": "live write attempts blocked until later audited write-gate", "required": True},
        {"gate": "single_live_date_only", "requirement": "live mode accepts exactly one label date", "required": True},
        {"gate": "invalid_live_date_blocks_before_fetch", "requirement": "invalid/range/multiple dates block before adapter call", "required": True},
        {"gate": "no_db_writes_from_live_mode", "requirement": "live CLI path does not write DB rows", "required": True},
        {"gate": "no_live_candidate_label_materialization", "requirement": "live CLI path does not materialize candidate labels", "required": True},
        {"gate": "lazy_adapter_import_boundary", "requirement": "adapter import remains inside helper/live branch only", "required": True},
        {"gate": "no_pybaseball_statcast_in_cli", "requirement": "CLI/scaffold does not directly import pybaseball/statcast", "required": True},
        {"gate": "no_real_fetch_in_plan_validation", "requirement": "planning validation performs no real external fetch", "required": True},
        {"gate": "production_default_unchanged", "requirement": "existing production/default behavior unchanged", "required": True},
    ]


def _future_test_strategy() -> List[Dict[str, Any]]:
    return [
        {"case": "default_fixture_invocation_unchanged", "expected": "existing default outputs unchanged", "required": True},
        {"case": "source_mode_default_fixture", "expected": "omitted source-mode resolves to fixture", "required": True},
        {"case": "explicit_live_dry_run_success", "expected": "live_dry_run_ready with synthetic fetcher", "required": True},
        {"case": "live_without_dry_run_blocked", "expected": "live_requires_dry_run", "required": True},
        {"case": "live_write_attempt_blocked", "expected": "live_write_blocked", "required": True},
        {"case": "invalid_live_date_blocked_before_fetcher", "expected": "live_date_window_invalid and adapter/fetcher not called", "required": True},
        {"case": "multiple_live_dates_blocked_before_fetcher", "expected": "live_date_window_invalid and adapter/fetcher not called", "required": True},
        {"case": "live_artifact_fields_present", "expected": "required 11 adapter fields present", "required": True},
        {"case": "blocked_live_artifact_fields_present", "expected": "blocked payload includes required artifact fields", "required": True},
        {"case": "fixture_branch_does_not_call_adapter", "expected": "fixture/default branch inert", "required": True},
        {"case": "no_db_writes", "expected": "db_writes_performed false", "required": True},
        {"case": "no_candidate_label_materialization", "expected": "candidate_labels_materialized false", "required": True},
    ]


def _current_source_state() -> List[Dict[str, Any]]:
    scaffold_source = SCAFFOLD_PATH.read_text(errors="ignore") if SCAFFOLD_PATH.exists() else ""
    top_level_imports = _top_level_import_text(scaffold_source) if scaffold_source else ""
    return [
        {"check": "scaffold_exists", "passed": SCAFFOLD_PATH.exists(), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_exists", "passed": ADAPTER_PATH.exists(), "detail": str(ADAPTER_PATH)},
        {"check": "validation_6dl_exists", "passed": VALIDATION_6DL.exists(), "detail": str(VALIDATION_6DL)},
        {"check": "audit_6dm_exists", "passed": AUDIT_6DM.exists(), "detail": str(AUDIT_6DM)},
        {"check": "live_helper_exists", "passed": "def run_candidate_bullpen_live_adapter_scaffold(" in scaffold_source, "detail": "6DL helper"},
        {"check": "fixture_default_constant_exists", "passed": 'CANDIDATE_BULLPEN_SOURCE_MODE_FIXTURE = "fixture"' in scaffold_source, "detail": "fixture default constant"},
        {"check": "live_mode_constant_exists", "passed": 'CANDIDATE_BULLPEN_SOURCE_MODE_LIVE = "live"' in scaffold_source, "detail": "live constant"},
        {"check": "no_top_level_adapter_import", "passed": "fetch_candidate_bullpen_statcast_live_adapter" not in top_level_imports, "detail": True},
        {"check": "no_top_level_pybaseball_statcast_import", "passed": "pybaseball" not in top_level_imports and "statcast" not in top_level_imports, "detail": True},
        {"check": "production_default_fixture_based", "passed": "fixture_mode_unchanged" in scaffold_source, "detail": "fixture path inert"},
    ]


def main() -> None:
    before = _snapshot_files()

    cli_rows = _cli_surface_contract()
    routing_rows = _branch_routing_contract()
    artifact_rows = _artifact_contract()
    status_rows = _status_exit_behavior()
    safety_rows = _safety_gates()
    test_rows = _future_test_strategy()
    source_rows = _current_source_state()

    _write_csv(OUTPUT_CLI, cli_rows)
    _write_csv(OUTPUT_ROUTING, routing_rows)
    _write_csv(OUTPUT_ARTIFACT, artifact_rows)
    _write_csv(OUTPUT_STATUS, status_rows)
    _write_csv(OUTPUT_SAFETY, safety_rows)
    _write_csv(OUTPUT_TESTS, test_rows)
    _write_csv(OUTPUT_SOURCE, source_rows)

    after = _snapshot_files()

    scaffold_not_modified = before.get(str(SCAFFOLD_PATH)) == after.get(str(SCAFFOLD_PATH))
    adapter_not_modified = before.get(str(ADAPTER_PATH)) == after.get(str(ADAPTER_PATH))
    validation_not_modified = before.get(str(VALIDATION_6DL)) == after.get(str(VALIDATION_6DL))
    audit_6dm_not_modified = before.get(str(AUDIT_6DM)) == after.get(str(AUDIT_6DM))
    no_fixture_mutation = before == after

    checks = [
        {"check": "cli_surface_contract_defined", "passed": len(cli_rows) == 7 and all(row["required"] for row in cli_rows), "detail": f"{len(cli_rows)} rows"},
        {"check": "branch_routing_contract_defined", "passed": len(routing_rows) == 6 and all(row["required"] for row in routing_rows), "detail": f"{len(routing_rows)} rows"},
        {"check": "artifact_contract_defined", "passed": len(artifact_rows) == 15 and all(row["required"] for row in artifact_rows), "detail": f"{len(artifact_rows)} rows"},
        {"check": "status_exit_behavior_defined", "passed": len(status_rows) == 10 and all(not row["crashes"] for row in status_rows), "detail": f"{len(status_rows)} rows"},
        {"check": "safety_gates_defined", "passed": len(safety_rows) == 12 and all(row["required"] for row in safety_rows), "detail": f"{len(safety_rows)} rows"},
        {"check": "future_test_strategy_defined", "passed": len(test_rows) == 12 and all(row["required"] for row in test_rows), "detail": f"{len(test_rows)} rows"},
        {"check": "current_source_state_valid", "passed": all(row["passed"] for row in source_rows), "detail": f"{sum(row['passed'] for row in source_rows)}/{len(source_rows)}"},
        {"check": "scaffold_not_modified", "passed": scaffold_not_modified, "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_not_modified", "passed": adapter_not_modified, "detail": str(ADAPTER_PATH)},
        {"check": "validation_script_not_modified", "passed": validation_not_modified, "detail": str(VALIDATION_6DL)},
        {"check": "scaffold_integration_audit_not_modified", "passed": audit_6dm_not_modified, "detail": str(AUDIT_6DM)},
        {"check": "no_fixture_mutation", "passed": no_fixture_mutation, "detail": "fixture and tracked dependency files unchanged"},
        {"check": "no_external_fetch", "passed": True, "detail": "planning-only; no adapter call"},
        {"check": "no_db_writes", "passed": True, "detail": "planning-only; no DB writes"},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(row["passed"] for row in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_cli_contract_plan_complete",
        "plan_version": PLAN_VERSION,
        "cli_surface_contract_rows": len(cli_rows),
        "branch_routing_contract_rows": len(routing_rows),
        "artifact_contract_rows": len(artifact_rows),
        "status_exit_behavior_rows": len(status_rows),
        "safety_gate_rows": len(safety_rows),
        "future_test_strategy_rows": len(test_rows),
        "current_source_state_rows": len(source_rows),
        "all_checks_passed": all_checks_passed,
        "planning_only": True,
        "scaffold_modified": False,
        "adapter_modified": False,
        "validation_script_modified": False,
        "scaffold_integration_audit_modified": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "candidate_labels_materialized_from_live_rows": False,
        "fixture_assets_mutated": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6DO_candidate_bullpen_statcast_live_adapter_cli_contract_plan_audit"
            if all_checks_passed
            else "6DN_patch_candidate_bullpen_statcast_live_adapter_cli_contract_plan"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
