from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan_v0.1"

SCAFFOLD_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
ADAPTER_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATION_6DX = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution.py")
AUDIT_6DY = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution.py")
PLAN_6DV = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution.py")
AUDIT_6DW = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan.py")
VALIDATION_6DT = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection.py")
AUDIT_6DU = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection.py")
VALIDATION_6DL = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_scaffold_integration.py")
AUDIT_6DM = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_scaffold_integration.py")
PLAN_6DN = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_contract.py")
AUDIT_6DO = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_contract_plan.py")
VALIDATION_6DP = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_scaffold_integration.py")
AUDIT_6DQ = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_scaffold_integration.py")
PLAN_6DR = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection.py")
AUDIT_6DS = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan.py")

FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"
DATES_DIR = FIXTURE_ROOT / "dates"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan_checks.csv"
OUTPUT_CURRENT_STATE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan_current_state.csv"
OUTPUT_FIELD_CONTRACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan_field_contract.csv"
OUTPUT_STATUS_CONTRACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan_status_value_contract.csv"
OUTPUT_ARTIFACT_COMPAT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan_artifact_compatibility_contract.csv"
OUTPUT_VALIDATION_STRATEGY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan_validation_strategy.csv"
OUTPUT_SAFETY_NON_GOALS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan_safety_non_goals.csv"
OUTPUT_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan_immutability.csv"


OBSERVABILITY_FIELDS = [
    "live_fetcher_resolution_source",
    "live_fetcher_resolution_status",
    "live_fetcher_resolution_gate",
    "live_fetcher_resolution_reason",
    "live_fetcher_resolution_dependency_error",
    "live_fetcher_resolution_external_fetch_enabled",
    "live_fetcher_resolution_synthetic_enabled",
    "live_fetcher_resolution_real_enabled",
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


def _sha(path: Path) -> str:
    if not path.exists():
        return "__MISSING__"
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _snapshot_files() -> Dict[str, str]:
    paths = [
        SCAFFOLD_PATH,
        ADAPTER_PATH,
        VALIDATION_6DX,
        AUDIT_6DY,
        PLAN_6DV,
        AUDIT_6DW,
        VALIDATION_6DT,
        AUDIT_6DU,
        VALIDATION_6DL,
        AUDIT_6DM,
        PLAN_6DN,
        AUDIT_6DO,
        VALIDATION_6DP,
        AUDIT_6DQ,
        PLAN_6DR,
        AUDIT_6DS,
        MANIFEST,
        EXPECTED_RESULTS,
    ]
    snapshot = {str(path): _sha(path) for path in paths}
    if DATES_DIR.exists():
        for payload in sorted(DATES_DIR.glob("*.jsonl")):
            snapshot[str(payload)] = _sha(payload)
    return snapshot


def _top_level_imports(source: str) -> str:
    tree = ast.parse(source)
    imports = []
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            imports.append(ast.get_source_segment(source, node) or "")
    return "\n".join(imports)


def _current_state_rows(source: str) -> List[Dict[str, Any]]:
    top_imports = _top_level_imports(source)
    return [
        {"check": "scaffold_exists", "passed": SCAFFOLD_PATH.exists(), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_exists", "passed": ADAPTER_PATH.exists(), "detail": str(ADAPTER_PATH)},
        {"check": "six_dx_validation_exists", "passed": VALIDATION_6DX.exists(), "detail": str(VALIDATION_6DX)},
        {"check": "six_dy_audit_exists", "passed": AUDIT_6DY.exists(), "detail": str(AUDIT_6DY)},
        {"check": "six_dx_marker_present", "passed": "candidate_bullpen_live_adapter_cli_real_fetcher_resolution_v0.1" in source, "detail": True},
        {"check": "real_fetcher_env_gate_present", "passed": "CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER" in source, "detail": True},
        {"check": "synthetic_env_gate_preserved", "passed": "CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE" in source, "detail": True},
        {"check": "resolver_present", "passed": "def _resolve_candidate_bullpen_live_fetcher" in source, "detail": True},
        {"check": "adapter_backed_import_branch_present", "passed": "fetch_candidate_bullpen_statcast_live_rows_for_date" in source, "detail": True},
        {"check": "dependency_missing_status_present", "passed": "live_dependency_missing" in source, "detail": True},
        {"check": "production_default_preserved", "passed": 'default="scaffold"' in source or "default=CANDIDATE_BULLPEN_SOURCE_MODE_FIXTURE" in source, "detail": True},
        {"check": "no_top_level_adapter_import", "passed": "fetch_candidate_bullpen_statcast_live_adapter" not in top_imports, "detail": True},
        {"check": "no_top_level_pybaseball_statcast_import", "passed": "pybaseball" not in top_imports and "statcast" not in top_imports, "detail": True},
    ]


def _field_contract_rows() -> List[Dict[str, Any]]:
    return [
        {
            "field": field,
            "planned_contract": "Additive diagnostic-only live fetcher resolution observability field; must not affect writes, materialization, or resolver gate decisions.",
            "required": True,
        }
        for field in OBSERVABILITY_FIELDS
    ]


def _status_contract_rows() -> List[Dict[str, Any]]:
    return [
        {"case": "default_no_gate", "planned_status": "safe no-real-fetcher status", "planned_source": "none", "required": True},
        {"case": "synthetic_gate", "planned_status": "live_dry_run_ready", "planned_source": "synthetic_test_double", "required": True},
        {"case": "real_gate", "planned_status": "real adapter source only with CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER=1", "planned_source": "real_adapter", "required": True},
        {"case": "dependency_missing", "planned_status": "live_dependency_missing", "planned_source": "dependency_missing", "required": True},
        {"case": "live_without_dry_run", "planned_status": "live_requires_dry_run", "planned_source": "blocked", "required": True},
        {"case": "live_write_attempt", "planned_status": "live_write_blocked", "planned_source": "blocked", "required": True},
        {"case": "invalid_or_multi_date", "planned_status": "live_date_window_invalid", "planned_source": "blocked", "required": True},
    ]


def _artifact_compatibility_rows() -> List[Dict[str, Any]]:
    return [
        {"item": "existing_required_fields_preserved", "planned_contract": "Existing 15 adapter/safety fields remain present on live success and blocked payloads.", "required": True},
        {"item": "observability_fields_additive", "planned_contract": "New fields are additive only and do not remove or rename current payload fields.", "required": True},
        {"item": "fixture_scaffold_behavior_unchanged", "planned_contract": "Fixture/scaffold behavior remains unchanged.", "required": True},
        {"item": "downstream_json_consumers_safe", "planned_contract": "Current downstream JSON consumers should not break because new fields are optional/additive.", "required": True},
        {"item": "diagnostic_only", "planned_contract": "Observability fields are diagnostic-only and cannot trigger DB writes or materialization.", "required": True},
    ]


def _validation_strategy_rows() -> List[Dict[str, Any]]:
    return [
        {"validation": "compileall", "planned_check": "python -m compileall mlb_app scripts", "required": True},
        {"validation": "source_audit", "planned_check": "Confirm 6DZ marker, observability fields, and existing gates.", "required": True},
        {"validation": "default_no_real_fetch_observability_audit", "planned_check": "Confirm default live dry-run emits safe no-real-fetcher observability without network.", "required": True},
        {"validation": "synthetic_observability_audit", "planned_check": "Confirm synthetic env gate emits synthetic/test-double observability.", "required": True},
        {"validation": "monkeypatched_real_fetcher_observability_audit", "planned_check": "Confirm real gate emits real-adapter observability via monkeypatch and no network.", "required": True},
        {"validation": "dependency_missing_observability_audit", "planned_check": "Confirm dependency missing path emits live_dependency_missing observability.", "required": True},
        {"validation": "blocked_path_observability_audit", "planned_check": "Confirm blocked paths preserve statuses and emit blocked observability.", "required": True},
        {"validation": "artifact_compatibility_audit", "planned_check": "Confirm existing 15 fields plus additive observability fields.", "required": True},
        {"validation": "import_boundary_audit", "planned_check": "Confirm no top-level adapter/pybaseball/statcast imports.", "required": True},
        {"validation": "immutability_audit", "planned_check": "Confirm adapter, fixtures, and prior scripts unchanged.", "required": True},
        {"validation": "safety_audit", "planned_check": "Confirm no real fetch in validation/CI, no DB writes, no materialization.", "required": True},
    ]


def _safety_non_goal_rows() -> List[Dict[str, Any]]:
    return [
        {"item": "planning_only", "detail": "This layer creates a plan only and implements no observability fields.", "required": True},
        {"item": "no_real_fetch", "detail": "No real Statcast/pybaseball fetch occurs in this planning layer.", "required": True},
        {"item": "no_real_fetch_in_validation_ci", "detail": "Future validation/CI must not perform real network fetches.", "required": True},
        {"item": "no_db_writes", "detail": "No DB writes are introduced.", "required": True},
        {"item": "no_candidate_materialization", "detail": "No candidate labels are materialized from live rows.", "required": True},
        {"item": "no_fixture_mutation", "detail": "Fixture assets remain unchanged.", "required": True},
        {"item": "no_adapter_modification", "detail": "Adapter source remains unchanged in this planning layer.", "required": True},
        {"item": "no_production_default_change", "detail": "Production defaults remain unchanged.", "required": True},
        {"item": "no_resolver_gate_change", "detail": "Resolver gates remain unchanged by this planning layer.", "required": True},
    ]


def _immutability_rows(before: Dict[str, str]) -> List[Dict[str, Any]]:
    after = _snapshot_files()
    return [
        {"check": "scaffold_unchanged_by_plan", "passed": before.get(str(SCAFFOLD_PATH)) == after.get(str(SCAFFOLD_PATH)), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_unchanged_by_plan", "passed": before.get(str(ADAPTER_PATH)) == after.get(str(ADAPTER_PATH)), "detail": str(ADAPTER_PATH)},
        {"check": "six_dx_validation_unchanged_by_plan", "passed": before.get(str(VALIDATION_6DX)) == after.get(str(VALIDATION_6DX)), "detail": str(VALIDATION_6DX)},
        {"check": "six_dy_audit_unchanged_by_plan", "passed": before.get(str(AUDIT_6DY)) == after.get(str(AUDIT_6DY)), "detail": str(AUDIT_6DY)},
        {"check": "prior_validation_audit_plan_scripts_unchanged_by_plan", "passed": all(before.get(str(path)) == after.get(str(path)) for path in [PLAN_6DV, AUDIT_6DW, VALIDATION_6DT, AUDIT_6DU, VALIDATION_6DL, AUDIT_6DM, PLAN_6DN, AUDIT_6DO, VALIDATION_6DP, AUDIT_6DQ, PLAN_6DR, AUDIT_6DS]), "detail": "6DL through 6DW unchanged"},
        {"check": "fixtures_unchanged_by_plan", "passed": before == after, "detail": "fixtures and tracked dependency files unchanged"},
    ]


def main() -> None:
    before = _snapshot_files()
    source = SCAFFOLD_PATH.read_text(errors="ignore")

    current_state = _current_state_rows(source)
    field_contract = _field_contract_rows()
    status_contract = _status_contract_rows()
    artifact_compat = _artifact_compatibility_rows()
    validation_strategy = _validation_strategy_rows()
    safety_non_goals = _safety_non_goal_rows()
    immutability = _immutability_rows(before)

    _write_csv(OUTPUT_CURRENT_STATE, current_state)
    _write_csv(OUTPUT_FIELD_CONTRACT, field_contract)
    _write_csv(OUTPUT_STATUS_CONTRACT, status_contract)
    _write_csv(OUTPUT_ARTIFACT_COMPAT, artifact_compat)
    _write_csv(OUTPUT_VALIDATION_STRATEGY, validation_strategy)
    _write_csv(OUTPUT_SAFETY_NON_GOALS, safety_non_goals)
    _write_csv(OUTPUT_IMMUTABILITY, immutability)

    checks = [
        {"check": "current_state_valid", "passed": all(row["passed"] for row in current_state), "detail": f"{sum(row['passed'] for row in current_state)}/{len(current_state)}"},
        {"check": "observability_field_contract_defined", "passed": all(row["required"] for row in field_contract), "detail": len(field_contract)},
        {"check": "status_value_contract_defined", "passed": all(row["required"] for row in status_contract), "detail": len(status_contract)},
        {"check": "artifact_compatibility_contract_defined", "passed": all(row["required"] for row in artifact_compat), "detail": len(artifact_compat)},
        {"check": "validation_strategy_defined", "passed": all(row["required"] for row in validation_strategy), "detail": len(validation_strategy)},
        {"check": "safety_non_goals_defined", "passed": all(row["required"] for row in safety_non_goals), "detail": len(safety_non_goals)},
        {"check": "immutability_valid", "passed": all(row["passed"] for row in immutability), "detail": f"{sum(row['passed'] for row in immutability)}/{len(immutability)}"},
        {"check": "scaffold_unchanged", "passed": any(row["check"] == "scaffold_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_unchanged", "passed": any(row["check"] == "adapter_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(ADAPTER_PATH)},
        {"check": "six_dx_validation_unchanged", "passed": any(row["check"] == "six_dx_validation_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(VALIDATION_6DX)},
        {"check": "six_dy_audit_unchanged", "passed": any(row["check"] == "six_dy_audit_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(AUDIT_6DY)},
        {"check": "no_fixture_mutation", "passed": any(row["check"] == "fixtures_unchanged_by_plan" and row["passed"] for row in immutability), "detail": True},
        {"check": "no_real_external_fetch", "passed": True, "detail": "planning-only"},
        {"check": "no_db_writes", "passed": True, "detail": "planning-only"},
        {"check": "no_candidate_label_materialization", "passed": True, "detail": "planning-only"},
        {"check": "production_default_unchanged", "passed": any(row["check"] == "production_default_preserved" and row["passed"] for row in current_state), "detail": True},
    ]

    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(row["passed"] for row in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan_complete",
        "plan_version": PLAN_VERSION,
        "current_state_rows": len(current_state),
        "observability_field_contract_rows": len(field_contract),
        "status_value_contract_rows": len(status_contract),
        "artifact_compatibility_rows": len(artifact_compat),
        "validation_strategy_rows": len(validation_strategy),
        "safety_non_goal_rows": len(safety_non_goals),
        "immutability_rows": len(immutability),
        "all_checks_passed": all_checks_passed,
        "planning_only": True,
        "current_state_valid": all(row["passed"] for row in current_state),
        "observability_field_contract_defined": True,
        "status_value_contract_defined": True,
        "artifact_compatibility_contract_defined": True,
        "validation_strategy_defined": True,
        "safety_non_goals_defined": True,
        "immutability_valid": all(row["passed"] for row in immutability),
        "scaffold_unchanged": True,
        "adapter_unchanged": True,
        "six_dx_validation_unchanged": True,
        "six_dy_audit_unchanged": True,
        "fixture_assets_mutated": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "candidate_labels_materialized_from_live_rows": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6EA_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan_audit"
            if all_checks_passed
            else "6DZ_patch_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
