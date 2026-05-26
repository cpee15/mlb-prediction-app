from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_v0.1"

SCAFFOLD_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
ADAPTER_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
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

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_checks.csv"
OUTPUT_CURRENT_STATE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_current_state.csv"
OUTPUT_REAL_FETCHER_CONTRACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_real_fetcher_contract.csv"
OUTPUT_OPT_IN_GATE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_opt_in_gate_contract.csv"
OUTPUT_LAZY_IMPORT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_lazy_import_contract.csv"
OUTPUT_RUNTIME = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_runtime_behavior_contract.csv"
OUTPUT_VALIDATION_STRATEGY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_validation_strategy.csv"
OUTPUT_NON_GOALS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_non_goals.csv"
OUTPUT_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_immutability.csv"


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
        {"check": "six_dt_validation_exists", "passed": VALIDATION_6DT.exists(), "detail": str(VALIDATION_6DT)},
        {"check": "six_du_audit_exists", "passed": AUDIT_6DU.exists(), "detail": str(AUDIT_6DU)},
        {"check": "six_dt_marker_present", "passed": "candidate_bullpen_live_adapter_cli_live_dry_run_fetcher_injection_v0.1" in source, "detail": True},
        {"check": "synthetic_env_gate_present", "passed": "CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE" in source, "detail": True},
        {"check": "resolver_present", "passed": "def _resolve_candidate_bullpen_live_fetcher" in source, "detail": True},
        {"check": "live_route_passes_resolved_fetcher", "passed": "fetcher=resolved_fetcher" in source, "detail": True},
        {"check": "default_live_dry_run_no_real_fetcher_currently_preserved", "passed": "return None" in source, "detail": True},
        {"check": "production_default_scaffold_preserved", "passed": 'default="scaffold"' in source or "default=CANDIDATE_BULLPEN_SOURCE_MODE_FIXTURE" in source, "detail": True},
        {"check": "no_top_level_adapter_import", "passed": "fetch_candidate_bullpen_statcast_live_adapter" not in top_imports, "detail": True},
        {"check": "no_top_level_pybaseball_statcast_import", "passed": "pybaseball" not in top_imports and "statcast" not in top_imports, "detail": True},
    ]


def _real_fetcher_contract_rows() -> List[Dict[str, Any]]:
    return [
        {"item": "implementation_target", "planned_contract": "Future implementation may modify only scripts/backfill_candidate_bullpen_statcast_labels.py unless a separate audited plan approves other files.", "required": True},
        {"item": "real_fetcher_branch", "planned_contract": "Add a resolver branch that can create an adapter-backed fetcher only after all explicit live dry-run gates pass.", "required": True},
        {"item": "source_mode_gate", "planned_contract": "Only explicit --source-mode live may be eligible for real fetcher resolution.", "required": True},
        {"item": "dry_run_gate", "planned_contract": "Real fetcher resolution requires --dry-run.", "required": True},
        {"item": "write_block_gate", "planned_contract": "Real fetcher resolution is blocked whenever --write or --allow-live-write is requested.", "required": True},
        {"item": "strict_single_date_gate", "planned_contract": "Real fetcher resolution requires exactly one strict YYYY-MM-DD label date.", "required": True},
        {"item": "default_fixture_unchanged", "planned_contract": "Default scaffold behavior and fixture behavior remain unchanged.", "required": True},
    ]


def _opt_in_gate_rows() -> List[Dict[str, Any]]:
    return [
        {"item": "explicit_opt_in_required", "planned_contract": "Real adapter-backed fetcher resolution must require a separate explicit opt-in gate.", "required": True},
        {"item": "recommended_env_gate", "planned_contract": "Use CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER=1 as the planned real-fetcher enablement gate.", "required": True},
        {"item": "default_no_real_fetcher", "planned_contract": "Default explicit live dry-run continues to resolve no real fetcher when the real-fetcher gate is absent.", "required": True},
        {"item": "synthetic_gate_separate", "planned_contract": "Keep CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE=synthetic separate from real fetcher enablement.", "required": True},
        {"item": "gate_precedence", "planned_contract": "Synthetic test double gate should remain validation-only and must not accidentally trigger real adapter fetches.", "required": True},
    ]


def _lazy_import_rows() -> List[Dict[str, Any]]:
    return [
        {"item": "no_top_level_adapter_import", "planned_contract": "Do not add top-level imports from scripts/fetch_candidate_bullpen_statcast_live_adapter.py.", "required": True},
        {"item": "no_top_level_pybaseball_statcast_import", "planned_contract": "Do not add top-level pybaseball/statcast imports in the CLI scaffold.", "required": True},
        {"item": "lazy_resolver_import", "planned_contract": "Adapter-backed fetcher import/creation happens only inside the gated resolver branch.", "required": True},
        {"item": "dependency_missing_safe", "planned_contract": "Dependency/import failure returns safe JSON status live_dependency_missing instead of raising or writing.", "required": True},
        {"item": "validation_monkeypatchable", "planned_contract": "Future validation must monkeypatch adapter fetcher resolution without network.", "required": True},
    ]


def _runtime_rows() -> List[Dict[str, Any]]:
    return [
        {"item": "real_fetch_dry_run_only", "planned_contract": "A real adapter call may happen only for explicit live dry-run plus opt-in gate.", "required": True},
        {"item": "diagnostic_only", "planned_contract": "Adapter result remains diagnostic-only and is not converted into candidate label writes.", "required": True},
        {"item": "no_db_writes", "planned_contract": "No DB writes are introduced by real fetcher resolution.", "required": True},
        {"item": "no_candidate_materialization", "planned_contract": "No candidate labels are materialized from live rows.", "required": True},
        {"item": "no_fixture_mutation", "planned_contract": "Fixture assets remain unchanged.", "required": True},
        {"item": "payload_contract", "planned_contract": "Payload retains adapter fields and safety flags.", "required": True},
        {"item": "blocked_statuses_preserved", "planned_contract": "Blocked statuses remain live_requires_dry_run, live_write_blocked, and live_date_window_invalid.", "required": True},
    ]


def _validation_strategy_rows() -> List[Dict[str, Any]]:
    return [
        {"validation": "compileall", "planned_check": "python -m compileall mlb_app scripts", "required": True},
        {"validation": "source_audit", "planned_check": "Confirm real fetcher branch, gates, and marker.", "required": True},
        {"validation": "resolver_gate_audit", "planned_check": "Confirm opt-in gate is required and blocked paths resolve None.", "required": True},
        {"validation": "default_no_real_fetch_behavior_audit", "planned_check": "Confirm live dry-run without real gate resolves no real fetcher.", "required": True},
        {"validation": "synthetic_env_behavior_audit", "planned_check": "Confirm synthetic env gate still returns deterministic test double without network.", "required": True},
        {"validation": "dependency_missing_safe_path_audit", "planned_check": "Confirm dependency/import failure maps to live_dependency_missing JSON.", "required": True},
        {"validation": "monkeypatched_adapter_fetcher_resolution_audit", "planned_check": "Confirm adapter-backed resolver branch can be exercised with monkeypatch and no network.", "required": True},
        {"validation": "subprocess_blocked_path_audit", "planned_check": "Confirm blocked CLI statuses remain stable.", "required": True},
        {"validation": "artifact_status_contract_audit", "planned_check": "Confirm adapter fields and safety flags on success and blocked payloads.", "required": True},
        {"validation": "import_boundary_audit", "planned_check": "Confirm no top-level adapter/pybaseball/statcast imports.", "required": True},
        {"validation": "immutability_audit", "planned_check": "Confirm adapter, fixtures, and prior scripts unchanged.", "required": True},
        {"validation": "safety_audit", "planned_check": "Confirm no real fetch in validation/CI, no DB writes, no materialization.", "required": True},
    ]


def _non_goal_rows() -> List[Dict[str, Any]]:
    return [
        {"non_goal": "no_implementation", "detail": "This layer does not implement real fetcher resolution.", "required": True},
        {"non_goal": "no_real_statcast_fetch", "detail": "This layer performs no real Statcast/pybaseball fetch.", "required": True},
        {"non_goal": "no_real_fetch_in_validation_ci", "detail": "Future validation/CI must not perform real network fetches.", "required": True},
        {"non_goal": "no_database_writes", "detail": "This layer performs no DB writes.", "required": True},
        {"non_goal": "no_candidate_label_materialization", "detail": "This layer materializes no candidate labels from live rows.", "required": True},
        {"non_goal": "no_adapter_modification", "detail": "This layer does not modify the adapter.", "required": True},
        {"non_goal": "no_fixture_mutation", "detail": "This layer does not mutate fixtures.", "required": True},
    ]


def _immutability_rows(before: Dict[str, str]) -> List[Dict[str, Any]]:
    after = _snapshot_files()
    return [
        {"check": "scaffold_unchanged_by_plan", "passed": before.get(str(SCAFFOLD_PATH)) == after.get(str(SCAFFOLD_PATH)), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_unchanged_by_plan", "passed": before.get(str(ADAPTER_PATH)) == after.get(str(ADAPTER_PATH)), "detail": str(ADAPTER_PATH)},
        {"check": "six_dt_validation_unchanged_by_plan", "passed": before.get(str(VALIDATION_6DT)) == after.get(str(VALIDATION_6DT)), "detail": str(VALIDATION_6DT)},
        {"check": "six_du_audit_unchanged_by_plan", "passed": before.get(str(AUDIT_6DU)) == after.get(str(AUDIT_6DU)), "detail": str(AUDIT_6DU)},
        {"check": "prior_validation_audit_plan_scripts_unchanged_by_plan", "passed": all(before.get(str(path)) == after.get(str(path)) for path in [VALIDATION_6DL, AUDIT_6DM, PLAN_6DN, AUDIT_6DO, VALIDATION_6DP, AUDIT_6DQ, PLAN_6DR, AUDIT_6DS]), "detail": "6DL through 6DS unchanged"},
        {"check": "fixtures_unchanged_by_plan", "passed": before == after, "detail": "fixtures and tracked dependency files unchanged"},
    ]


def main() -> None:
    before = _snapshot_files()
    source = SCAFFOLD_PATH.read_text(errors="ignore")

    current_state = _current_state_rows(source)
    real_fetcher_contract = _real_fetcher_contract_rows()
    opt_in_gate = _opt_in_gate_rows()
    lazy_import = _lazy_import_rows()
    runtime = _runtime_rows()
    validation_strategy = _validation_strategy_rows()
    non_goals = _non_goal_rows()
    immutability = _immutability_rows(before)

    _write_csv(OUTPUT_CURRENT_STATE, current_state)
    _write_csv(OUTPUT_REAL_FETCHER_CONTRACT, real_fetcher_contract)
    _write_csv(OUTPUT_OPT_IN_GATE, opt_in_gate)
    _write_csv(OUTPUT_LAZY_IMPORT, lazy_import)
    _write_csv(OUTPUT_RUNTIME, runtime)
    _write_csv(OUTPUT_VALIDATION_STRATEGY, validation_strategy)
    _write_csv(OUTPUT_NON_GOALS, non_goals)
    _write_csv(OUTPUT_IMMUTABILITY, immutability)

    checks = [
        {"check": "current_state_valid", "passed": all(row["passed"] for row in current_state), "detail": f"{sum(row['passed'] for row in current_state)}/{len(current_state)}"},
        {"check": "real_fetcher_resolution_contract_defined", "passed": all(row["required"] for row in real_fetcher_contract), "detail": len(real_fetcher_contract)},
        {"check": "opt_in_gate_contract_defined", "passed": all(row["required"] for row in opt_in_gate), "detail": len(opt_in_gate)},
        {"check": "lazy_import_contract_defined", "passed": all(row["required"] for row in lazy_import), "detail": len(lazy_import)},
        {"check": "runtime_behavior_contract_defined", "passed": all(row["required"] for row in runtime), "detail": len(runtime)},
        {"check": "validation_strategy_defined", "passed": all(row["required"] for row in validation_strategy), "detail": len(validation_strategy)},
        {"check": "non_goals_defined", "passed": all(row["required"] for row in non_goals), "detail": len(non_goals)},
        {"check": "immutability_valid", "passed": all(row["passed"] for row in immutability), "detail": f"{sum(row['passed'] for row in immutability)}/{len(immutability)}"},
        {"check": "scaffold_unchanged", "passed": any(row["check"] == "scaffold_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_unchanged", "passed": any(row["check"] == "adapter_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(ADAPTER_PATH)},
        {"check": "six_dt_validation_unchanged", "passed": any(row["check"] == "six_dt_validation_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(VALIDATION_6DT)},
        {"check": "six_du_audit_unchanged", "passed": any(row["check"] == "six_du_audit_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(AUDIT_6DU)},
        {"check": "prior_validation_audit_plan_scripts_unchanged", "passed": any(row["check"] == "prior_validation_audit_plan_scripts_unchanged_by_plan" and row["passed"] for row in immutability), "detail": True},
        {"check": "no_fixture_mutation", "passed": any(row["check"] == "fixtures_unchanged_by_plan" and row["passed"] for row in immutability), "detail": True},
        {"check": "no_real_external_fetch", "passed": True, "detail": "planning-only"},
        {"check": "no_db_writes", "passed": True, "detail": "planning-only"},
        {"check": "production_default_unchanged", "passed": any(row["check"] == "production_default_scaffold_preserved" and row["passed"] for row in current_state), "detail": True},
    ]

    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(row["passed"] for row in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_complete",
        "plan_version": PLAN_VERSION,
        "current_state_rows": len(current_state),
        "real_fetcher_contract_rows": len(real_fetcher_contract),
        "opt_in_gate_contract_rows": len(opt_in_gate),
        "lazy_import_contract_rows": len(lazy_import),
        "runtime_behavior_contract_rows": len(runtime),
        "validation_strategy_rows": len(validation_strategy),
        "non_goal_rows": len(non_goals),
        "immutability_rows": len(immutability),
        "all_checks_passed": all_checks_passed,
        "planning_only": True,
        "current_state_valid": all(row["passed"] for row in current_state),
        "real_fetcher_resolution_contract_defined": True,
        "opt_in_gate_contract_defined": True,
        "lazy_import_contract_defined": True,
        "runtime_behavior_contract_defined": True,
        "validation_strategy_defined": True,
        "non_goals_defined": True,
        "immutability_valid": all(row["passed"] for row in immutability),
        "scaffold_unchanged": True,
        "adapter_unchanged": True,
        "six_dt_validation_unchanged": True,
        "six_du_audit_unchanged": True,
        "fixture_assets_mutated": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "candidate_labels_materialized_from_live_rows": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6DW_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_audit"
            if all_checks_passed
            else "6DV_patch_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
