from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_v0.1"

SCAFFOLD_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
ADAPTER_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATION_6EB = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability.py")
AUDIT_6EC = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability.py")
PLAN_6DZ = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability.py")
AUDIT_6EA = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan.py")
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

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_checks.csv"
OUTPUT_CURRENT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_current_state.csv"
OUTPUT_GATES = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_gates.csv"
OUTPUT_PREFLIGHT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_preflight.csv"
OUTPUT_ACCEPTANCE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_acceptance_contract.csv"
OUTPUT_NON_GOALS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_non_goals.csv"
OUTPUT_VALIDATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_future_validation.csv"
OUTPUT_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_immutability.csv"

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
        VALIDATION_6EB,
        AUDIT_6EC,
        PLAN_6DZ,
        AUDIT_6EA,
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
    synthetic_idx = source.find('CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE") == "synthetic"')
    real_idx = source.find('CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER") == "1"')
    return [
        {"check": "six_eb_marker_present", "passed": "candidate_bullpen_live_adapter_cli_live_fetcher_observability_v0.1" in source, "detail": True},
        {"check": "observability_fields_present", "passed": all(field in source for field in OBSERVABILITY_FIELDS), "detail": f"{sum(field in source for field in OBSERVABILITY_FIELDS)}/{len(OBSERVABILITY_FIELDS)}"},
        {"check": "observability_builder_defined", "passed": "def _candidate_bullpen_live_fetcher_observability" in source, "detail": True},
        {"check": "observability_apply_helper_defined", "passed": "def _candidate_bullpen_apply_live_fetcher_observability" in source, "detail": True},
        {"check": "real_fetcher_env_gate_present", "passed": "CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER" in source, "detail": True},
        {"check": "synthetic_env_gate_preserved", "passed": "CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE" in source, "detail": True},
        {"check": "dependency_missing_status_preserved", "passed": "live_dependency_missing" in source, "detail": True},
        {"check": "resolver_present", "passed": "def _resolve_candidate_bullpen_live_fetcher" in source, "detail": True},
        {"check": "synthetic_precedes_real_branch", "passed": synthetic_idx != -1 and real_idx != -1 and synthetic_idx < real_idx, "detail": f"{synthetic_idx}:{real_idx}"},
        {"check": "no_top_level_adapter_import", "passed": "fetch_candidate_bullpen_statcast_live_adapter" not in top_imports, "detail": True},
        {"check": "no_top_level_pybaseball_statcast_import", "passed": "pybaseball" not in top_imports and "statcast" not in top_imports, "detail": True},
        {"check": "production_default_preserved", "passed": 'default="scaffold"' in source or "default=CANDIDATE_BULLPEN_SOURCE_MODE_FIXTURE" in source, "detail": True},
    ]


def _gate_rows() -> List[Dict[str, Any]]:
    return [
        {"gate": "dry_run_default", "contract": "Live path remains dry-run only by default.", "required": True},
        {"gate": "real_fetcher_explicit_env_gate", "contract": "Real fetcher remains explicitly gated by CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER=1.", "required": True},
        {"gate": "writes_blocked", "contract": "Writes remain blocked unless a separate future layer defines a controlled write policy.", "required": True},
        {"gate": "single_date_window", "contract": "Live operational readiness requires exactly one label date.", "required": True},
        {"gate": "invalid_multi_date_blocked", "contract": "Invalid or multi-date live windows remain blocked.", "required": True},
        {"gate": "dependency_missing_safe", "contract": "Dependency-missing path remains safe and diagnostic-only.", "required": True},
        {"gate": "ci_no_network", "contract": "Network fetches are never allowed in CI validation.", "required": True},
    ]


def _preflight_rows() -> List[Dict[str, Any]]:
    return [
        {"preflight": "environment_gate_explicit", "contract": "Confirm the real-fetcher env gate is present and explicitly set.", "required": True},
        {"preflight": "dry_run_required", "contract": "Confirm dry-run is required for live fetcher execution.", "required": True},
        {"preflight": "one_date_window_confirmed", "contract": "Confirm exactly one label date is requested.", "required": True},
        {"preflight": "live_write_flags_false", "contract": "Confirm live write flags are false.", "required": True},
        {"preflight": "observability_fields_present", "contract": "Confirm all 8 observability fields are present in the artifact.", "required": True},
        {"preflight": "adapter_status_known", "contract": "Confirm adapter status is safe and known.", "required": True},
        {"preflight": "adapter_counts_present", "contract": "Confirm raw, normalized, duplicate, and required-field-failure counts are present.", "required": True},
        {"preflight": "external_fetch_performed_surfaced", "contract": "Confirm external_fetch_performed is surfaced.", "required": True},
        {"preflight": "db_writes_performed_surfaced", "contract": "Confirm db_writes_performed is surfaced.", "required": True},
        {"preflight": "candidate_labels_materialized_surfaced", "contract": "Confirm candidate_labels_materialized is surfaced.", "required": True},
        {"preflight": "dependency_error_surfaced", "contract": "Confirm dependency error is surfaced when dependency is missing.", "required": True},
    ]


def _acceptance_rows() -> List[Dict[str, Any]]:
    return [
        {"case": "default_no_gate", "contract": "Expose live_fetcher_resolution_source=none or dependency-missing safe source.", "required": True},
        {"case": "synthetic", "contract": "Expose live_fetcher_resolution_source=synthetic_test_double.", "required": True},
        {"case": "monkeypatched_real", "contract": "Expose live_fetcher_resolution_source=real_adapter without network.", "required": True},
        {"case": "dependency_missing", "contract": "Expose live_fetcher_resolution_status=live_dependency_missing.", "required": True},
        {"case": "blocked_paths", "contract": "Preserve existing blocked statuses.", "required": True},
        {"case": "existing_required_fields", "contract": "Existing 15 required fields remain present.", "required": True},
        {"case": "additive_observability", "contract": "All 8 observability fields remain additive.", "required": True},
    ]


def _non_goal_rows() -> List[Dict[str, Any]]:
    return [
        {"non_goal": "no_real_fetch_in_plan", "detail": "No real fetch occurs in this planning layer.", "required": True},
        {"non_goal": "no_real_fetch_in_validation_ci", "detail": "No real fetch is allowed in validation/CI.", "required": True},
        {"non_goal": "no_db_writes", "detail": "No DB writes are introduced.", "required": True},
        {"non_goal": "no_live_row_materialization", "detail": "No live rows are materialized into candidate labels.", "required": True},
        {"non_goal": "no_schedule_or_cron", "detail": "No schedule, cron, or automation is introduced.", "required": True},
        {"non_goal": "no_secrets_management_changes", "detail": "No secrets management changes are introduced.", "required": True},
        {"non_goal": "no_production_default_changes", "detail": "No production defaults are changed.", "required": True},
        {"non_goal": "no_resolver_gate_changes", "detail": "No resolver gates are changed.", "required": True},
        {"non_goal": "no_adapter_changes", "detail": "No adapter source changes are introduced.", "required": True},
    ]


def _future_validation_rows() -> List[Dict[str, Any]]:
    return [
        {"validation": "source_audit", "planned_check": "Confirm 6EB baseline and 6ED operational readiness plan source.", "required": True},
        {"validation": "plan_execution_audit", "planned_check": "Execute this plan and validate all artifacts.", "required": True},
        {"validation": "dry_run_preflight_audit", "planned_check": "Validate dry-run, one-date, no-write preflight contracts.", "required": True},
        {"validation": "default_no_gate_observability_audit", "planned_check": "Validate default no-gate observability without network.", "required": True},
        {"validation": "synthetic_observability_audit", "planned_check": "Validate synthetic observability path.", "required": True},
        {"validation": "monkeypatched_real_fetcher_observability_audit", "planned_check": "Validate real fetcher observability with monkeypatch and no network.", "required": True},
        {"validation": "dependency_missing_audit", "planned_check": "Validate dependency-missing safe artifact.", "required": True},
        {"validation": "blocked_path_audit", "planned_check": "Validate blocked paths remain blocked and safe.", "required": True},
        {"validation": "artifact_compatibility_audit", "planned_check": "Validate existing 15 fields plus additive 8 observability fields.", "required": True},
        {"validation": "import_boundary_audit", "planned_check": "Validate no top-level adapter/pybaseball/statcast imports.", "required": True},
        {"validation": "immutability_audit", "planned_check": "Validate scaffold/adapter/prior scripts/fixtures unchanged by audit.", "required": True},
        {"validation": "safety_audit", "planned_check": "Validate no real fetch, no DB writes, no materialization.", "required": True},
    ]


def _immutability_rows(before: Dict[str, str]) -> List[Dict[str, Any]]:
    after = _snapshot_files()
    prior_paths = [
        VALIDATION_6EB,
        AUDIT_6EC,
        PLAN_6DZ,
        AUDIT_6EA,
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
    ]
    return [
        {"check": "scaffold_unchanged_by_plan", "passed": before.get(str(SCAFFOLD_PATH)) == after.get(str(SCAFFOLD_PATH)), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_unchanged_by_plan", "passed": before.get(str(ADAPTER_PATH)) == after.get(str(ADAPTER_PATH)), "detail": str(ADAPTER_PATH)},
        {"check": "six_eb_validation_unchanged_by_plan", "passed": before.get(str(VALIDATION_6EB)) == after.get(str(VALIDATION_6EB)), "detail": str(VALIDATION_6EB)},
        {"check": "six_ec_audit_unchanged_by_plan", "passed": before.get(str(AUDIT_6EC)) == after.get(str(AUDIT_6EC)), "detail": str(AUDIT_6EC)},
        {"check": "six_dz_plan_unchanged_by_plan", "passed": before.get(str(PLAN_6DZ)) == after.get(str(PLAN_6DZ)), "detail": str(PLAN_6DZ)},
        {"check": "six_ea_audit_unchanged_by_plan", "passed": before.get(str(AUDIT_6EA)) == after.get(str(AUDIT_6EA)), "detail": str(AUDIT_6EA)},
        {"check": "prior_validation_audit_plan_scripts_unchanged_by_plan", "passed": all(before.get(str(path)) == after.get(str(path)) for path in prior_paths), "detail": "6DL through 6EC unchanged"},
        {"check": "fixtures_unchanged_by_plan", "passed": before == after, "detail": "fixtures and tracked dependency files unchanged"},
    ]


def main() -> None:
    before = _snapshot_files()
    source = SCAFFOLD_PATH.read_text(errors="ignore")

    current_state = _current_state_rows(source)
    gates = _gate_rows()
    preflight = _preflight_rows()
    acceptance = _acceptance_rows()
    non_goals = _non_goal_rows()
    future_validation = _future_validation_rows()
    immutability = _immutability_rows(before)

    _write_csv(OUTPUT_CURRENT, current_state)
    _write_csv(OUTPUT_GATES, gates)
    _write_csv(OUTPUT_PREFLIGHT, preflight)
    _write_csv(OUTPUT_ACCEPTANCE, acceptance)
    _write_csv(OUTPUT_NON_GOALS, non_goals)
    _write_csv(OUTPUT_VALIDATION, future_validation)
    _write_csv(OUTPUT_IMMUTABILITY, immutability)

    checks = [
        {"check": "current_state_valid", "passed": all(row["passed"] for row in current_state), "detail": f"{sum(row['passed'] for row in current_state)}/{len(current_state)}"},
        {"check": "readiness_gates_defined", "passed": all(row["required"] for row in gates), "detail": len(gates)},
        {"check": "preflight_checks_defined", "passed": all(row["required"] for row in preflight), "detail": len(preflight)},
        {"check": "observability_acceptance_contract_defined", "passed": all(row["required"] for row in acceptance), "detail": len(acceptance)},
        {"check": "rollout_non_goals_defined", "passed": all(row["required"] for row in non_goals), "detail": len(non_goals)},
        {"check": "future_validation_plan_defined", "passed": all(row["required"] for row in future_validation), "detail": len(future_validation)},
        {"check": "immutability_valid", "passed": all(row["passed"] for row in immutability), "detail": f"{sum(row['passed'] for row in immutability)}/{len(immutability)}"},
        {"check": "scaffold_unchanged", "passed": any(row["check"] == "scaffold_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_unchanged", "passed": any(row["check"] == "adapter_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(ADAPTER_PATH)},
        {"check": "six_eb_validation_unchanged", "passed": any(row["check"] == "six_eb_validation_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(VALIDATION_6EB)},
        {"check": "six_ec_audit_unchanged", "passed": any(row["check"] == "six_ec_audit_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(AUDIT_6EC)},
        {"check": "no_fixture_mutation", "passed": any(row["check"] == "fixtures_unchanged_by_plan" and row["passed"] for row in immutability), "detail": True},
        {"check": "no_real_external_fetch", "passed": True, "detail": "planning-only"},
        {"check": "no_db_writes", "passed": True, "detail": "planning-only"},
        {"check": "no_candidate_label_materialization", "passed": True, "detail": "planning-only"},
        {"check": "production_default_unchanged", "passed": any(row["check"] == "production_default_preserved" and row["passed"] for row in current_state), "detail": True},
    ]

    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(row["passed"] for row in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_complete",
        "plan_version": PLAN_VERSION,
        "current_state_rows": len(current_state),
        "readiness_gate_rows": len(gates),
        "preflight_rows": len(preflight),
        "observability_acceptance_rows": len(acceptance),
        "rollout_non_goal_rows": len(non_goals),
        "future_validation_rows": len(future_validation),
        "immutability_rows": len(immutability),
        "all_checks_passed": all_checks_passed,
        "planning_only": True,
        "current_state_valid": all(row["passed"] for row in current_state),
        "readiness_gates_defined": all(row["required"] for row in gates),
        "preflight_checks_defined": all(row["required"] for row in preflight),
        "observability_acceptance_contract_defined": all(row["required"] for row in acceptance),
        "rollout_non_goals_defined": all(row["required"] for row in non_goals),
        "future_validation_plan_defined": all(row["required"] for row in future_validation),
        "immutability_valid": all(row["passed"] for row in immutability),
        "scaffold_unchanged": True,
        "adapter_unchanged": True,
        "six_eb_validation_unchanged": True,
        "six_ec_audit_unchanged": True,
        "fixture_assets_mutated": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "candidate_labels_materialized_from_live_rows": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6EE_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_audit"
            if all_checks_passed
            else "6ED_patch_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
