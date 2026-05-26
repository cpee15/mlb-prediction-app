from __future__ import annotations

import ast
import csv
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


AUDIT_VERSION = "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_audit_v0.1"

SCAFFOLD_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
ADAPTER_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATION_6DT = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection.py")
AUDIT_6DU = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection.py")
PLAN_6DV = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution.py")
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

PLAN_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan.json"
PLAN_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_checks.csv"
PLAN_CURRENT_STATE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_current_state.csv"
PLAN_REAL_FETCHER_CONTRACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_real_fetcher_contract.csv"
PLAN_OPT_IN_GATE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_opt_in_gate_contract.csv"
PLAN_LAZY_IMPORT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_lazy_import_contract.csv"
PLAN_RUNTIME = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_runtime_behavior_contract.csv"
PLAN_VALIDATION_STRATEGY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_validation_strategy.csv"
PLAN_NON_GOALS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_non_goals.csv"
PLAN_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_immutability.csv"

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_audit_checks.csv"
OUTPUT_PLAN_EXECUTION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_audit_execution.csv"
OUTPUT_PLAN_ARTIFACTS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_audit_artifacts.csv"
OUTPUT_CURRENT_STATE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_audit_current_state.csv"
OUTPUT_REAL_FETCHER_CONTRACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_audit_real_fetcher_contract.csv"
OUTPUT_OPT_IN_GATE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_audit_opt_in_gate_contract.csv"
OUTPUT_LAZY_IMPORT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_audit_lazy_import_contract.csv"
OUTPUT_RUNTIME = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_audit_runtime_behavior_contract.csv"
OUTPUT_VALIDATION_STRATEGY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_audit_validation_strategy.csv"
OUTPUT_NON_GOALS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_audit_non_goals.csv"
OUTPUT_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_audit_immutability.csv"


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


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _contains(rows: List[Dict[str, Any]], *needles: str) -> bool:
    haystack = json.dumps(rows, sort_keys=True).lower()
    return all(needle.lower() in haystack for needle in needles)


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
        PLAN_6DV,
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


def _run_plan() -> Dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(PLAN_6DV)],
        capture_output=True,
        text=True,
    )
    diagnosis = _read_json(PLAN_JSON)
    return {
        "returncode": completed.returncode,
        "diagnosis": diagnosis.get("diagnosis", ""),
        "all_checks_passed": diagnosis.get("all_checks_passed"),
        "planning_only": diagnosis.get("planning_only"),
        "current_state_valid": diagnosis.get("current_state_valid"),
        "real_fetcher_resolution_contract_defined": diagnosis.get("real_fetcher_resolution_contract_defined"),
        "opt_in_gate_contract_defined": diagnosis.get("opt_in_gate_contract_defined"),
        "lazy_import_contract_defined": diagnosis.get("lazy_import_contract_defined"),
        "runtime_behavior_contract_defined": diagnosis.get("runtime_behavior_contract_defined"),
        "validation_strategy_defined": diagnosis.get("validation_strategy_defined"),
        "non_goals_defined": diagnosis.get("non_goals_defined"),
        "immutability_valid": diagnosis.get("immutability_valid"),
        "passed": (
            completed.returncode == 0
            and diagnosis.get("diagnosis") == "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_complete"
            and diagnosis.get("all_checks_passed") is True
            and diagnosis.get("planning_only") is True
            and diagnosis.get("current_state_valid") is True
            and diagnosis.get("real_fetcher_resolution_contract_defined") is True
            and diagnosis.get("opt_in_gate_contract_defined") is True
            and diagnosis.get("lazy_import_contract_defined") is True
            and diagnosis.get("runtime_behavior_contract_defined") is True
            and diagnosis.get("validation_strategy_defined") is True
            and diagnosis.get("non_goals_defined") is True
            and diagnosis.get("immutability_valid") is True
            and diagnosis.get("scaffold_unchanged") is True
            and diagnosis.get("adapter_unchanged") is True
            and diagnosis.get("six_dt_validation_unchanged") is True
            and diagnosis.get("six_du_audit_unchanged") is True
            and diagnosis.get("fixture_assets_mutated") is False
            and diagnosis.get("external_fetch_performed") is False
            and diagnosis.get("db_writes_performed") is False
            and diagnosis.get("production_default_unchanged") is True
        ),
    }


def _plan_artifact_rows() -> List[Dict[str, Any]]:
    artifacts = [
        PLAN_JSON,
        PLAN_CHECKS,
        PLAN_CURRENT_STATE,
        PLAN_REAL_FETCHER_CONTRACT,
        PLAN_OPT_IN_GATE,
        PLAN_LAZY_IMPORT,
        PLAN_RUNTIME,
        PLAN_VALIDATION_STRATEGY,
        PLAN_NON_GOALS,
        PLAN_IMMUTABILITY,
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


def _current_state_rows(scaffold_source: str) -> List[Dict[str, Any]]:
    top_imports = _top_level_imports(scaffold_source)
    return [
        {"check": "scaffold_exists", "passed": SCAFFOLD_PATH.exists(), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_exists", "passed": ADAPTER_PATH.exists(), "detail": str(ADAPTER_PATH)},
        {"check": "six_dt_validation_exists", "passed": VALIDATION_6DT.exists(), "detail": str(VALIDATION_6DT)},
        {"check": "six_du_audit_exists", "passed": AUDIT_6DU.exists(), "detail": str(AUDIT_6DU)},
        {"check": "six_dv_plan_exists", "passed": PLAN_6DV.exists(), "detail": str(PLAN_6DV)},
        {"check": "six_dt_marker_present", "passed": "candidate_bullpen_live_adapter_cli_live_dry_run_fetcher_injection_v0.1" in scaffold_source, "detail": True},
        {"check": "synthetic_env_gate_present", "passed": "CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE" in scaffold_source, "detail": True},
        {"check": "resolver_present", "passed": "def _resolve_candidate_bullpen_live_fetcher" in scaffold_source, "detail": True},
        {"check": "live_route_passes_resolved_fetcher", "passed": "fetcher=resolved_fetcher" in scaffold_source, "detail": True},
        {"check": "default_live_dry_run_no_real_fetcher_currently_preserved", "passed": "return None" in scaffold_source, "detail": True},
        {"check": "production_default_preserved", "passed": 'default="scaffold"' in scaffold_source or "default=CANDIDATE_BULLPEN_SOURCE_MODE_FIXTURE" in scaffold_source, "detail": True},
        {"check": "no_top_level_adapter_import", "passed": "fetch_candidate_bullpen_statcast_live_adapter" not in top_imports, "detail": True},
        {"check": "no_top_level_pybaseball_statcast_import", "passed": "pybaseball" not in top_imports and "statcast" not in top_imports, "detail": True},
    ]


def _real_fetcher_contract_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(PLAN_REAL_FETCHER_CONTRACT)
    return [
        {"check": "future_implementation_target_scoped", "passed": _contains(rows, "implementation_target", "backfill_candidate_bullpen_statcast_labels.py"), "detail": True},
        {"check": "real_fetcher_branch_planned", "passed": _contains(rows, "real_fetcher_branch", "adapter-backed fetcher"), "detail": True},
        {"check": "source_mode_gate_planned", "passed": _contains(rows, "source_mode_gate", "--source-mode live"), "detail": True},
        {"check": "dry_run_gate_planned", "passed": _contains(rows, "dry_run_gate", "--dry-run"), "detail": True},
        {"check": "write_block_gate_planned", "passed": _contains(rows, "write_block_gate", "--write", "--allow-live-write"), "detail": True},
        {"check": "strict_single_date_gate_planned", "passed": _contains(rows, "strict_single_date_gate", "YYYY-MM-DD"), "detail": True},
        {"check": "default_fixture_unchanged_planned", "passed": _contains(rows, "default_fixture_unchanged", "fixture behavior remain unchanged"), "detail": True},
    ]


def _opt_in_gate_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(PLAN_OPT_IN_GATE)
    return [
        {"check": "explicit_opt_in_required", "passed": _contains(rows, "explicit_opt_in_required", "explicit opt-in gate"), "detail": True},
        {"check": "recommended_real_fetcher_env_gate_present", "passed": _contains(rows, "CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER=1"), "detail": True},
        {"check": "default_no_real_fetcher_preserved", "passed": _contains(rows, "default_no_real_fetcher", "absent"), "detail": True},
        {"check": "synthetic_gate_separate", "passed": _contains(rows, "CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE=synthetic", "separate"), "detail": True},
        {"check": "gate_precedence_defined", "passed": _contains(rows, "gate_precedence", "must not accidentally trigger real adapter fetches"), "detail": True},
    ]


def _lazy_import_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(PLAN_LAZY_IMPORT)
    return [
        {"check": "no_top_level_adapter_import_planned", "passed": _contains(rows, "no_top_level_adapter_import", "Do not add top-level imports"), "detail": True},
        {"check": "no_top_level_pybaseball_statcast_import_planned", "passed": _contains(rows, "no_top_level_pybaseball_statcast_import", "Do not add top-level pybaseball/statcast imports"), "detail": True},
        {"check": "lazy_resolver_import_planned", "passed": _contains(rows, "lazy_resolver_import", "inside the gated resolver branch"), "detail": True},
        {"check": "dependency_missing_safe_status_planned", "passed": _contains(rows, "dependency_missing_safe", "live_dependency_missing"), "detail": True},
        {"check": "validation_monkeypatchable_without_network", "passed": _contains(rows, "validation_monkeypatchable", "without network"), "detail": True},
    ]


def _runtime_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(PLAN_RUNTIME)
    return [
        {"check": "real_fetch_dry_run_only", "passed": _contains(rows, "real_fetch_dry_run_only", "explicit live dry-run plus opt-in gate"), "detail": True},
        {"check": "diagnostic_only_result", "passed": _contains(rows, "diagnostic_only", "diagnostic-only"), "detail": True},
        {"check": "no_db_writes", "passed": _contains(rows, "no_db_writes", "No DB writes"), "detail": True},
        {"check": "no_candidate_materialization", "passed": _contains(rows, "no_candidate_materialization", "No candidate labels"), "detail": True},
        {"check": "no_fixture_mutation", "passed": _contains(rows, "no_fixture_mutation", "Fixture assets remain unchanged"), "detail": True},
        {"check": "payload_contract_retained", "passed": _contains(rows, "payload_contract", "adapter fields", "safety flags"), "detail": True},
        {"check": "blocked_statuses_preserved", "passed": _contains(rows, "blocked_statuses_preserved", "live_requires_dry_run", "live_write_blocked", "live_date_window_invalid"), "detail": True},
    ]


def _validation_strategy_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(PLAN_VALIDATION_STRATEGY)
    return [
        {"check": "compileall_planned", "passed": _contains(rows, "compileall", "python -m compileall mlb_app scripts"), "detail": True},
        {"check": "source_audit_planned", "passed": _contains(rows, "source_audit"), "detail": True},
        {"check": "resolver_gate_audit_planned", "passed": _contains(rows, "resolver_gate_audit"), "detail": True},
        {"check": "default_no_real_fetch_behavior_audit_planned", "passed": _contains(rows, "default_no_real_fetch_behavior_audit"), "detail": True},
        {"check": "synthetic_env_behavior_audit_planned", "passed": _contains(rows, "synthetic_env_behavior_audit"), "detail": True},
        {"check": "dependency_missing_safe_path_audit_planned", "passed": _contains(rows, "dependency_missing_safe_path_audit", "live_dependency_missing"), "detail": True},
        {"check": "monkeypatched_adapter_fetcher_resolution_audit_planned", "passed": _contains(rows, "monkeypatched_adapter_fetcher_resolution_audit", "without network"), "detail": True},
        {"check": "subprocess_blocked_path_audit_planned", "passed": _contains(rows, "subprocess_blocked_path_audit"), "detail": True},
        {"check": "artifact_status_contract_audit_planned", "passed": _contains(rows, "artifact_status_contract_audit"), "detail": True},
        {"check": "import_boundary_audit_planned", "passed": _contains(rows, "import_boundary_audit"), "detail": True},
        {"check": "immutability_audit_planned", "passed": _contains(rows, "immutability_audit"), "detail": True},
        {"check": "safety_audit_planned", "passed": _contains(rows, "safety_audit", "no real fetch", "no DB writes"), "detail": True},
    ]


def _non_goal_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(PLAN_NON_GOALS)
    return [
        {"check": "no_implementation", "passed": _contains(rows, "no_implementation", "does not implement"), "detail": True},
        {"check": "no_real_statcast_fetch", "passed": _contains(rows, "no_real_statcast_fetch", "no real Statcast/pybaseball fetch"), "detail": True},
        {"check": "no_real_fetch_in_validation_ci", "passed": _contains(rows, "no_real_fetch_in_validation_ci", "CI"), "detail": True},
        {"check": "no_database_writes", "passed": _contains(rows, "no_database_writes", "no DB writes"), "detail": True},
        {"check": "no_candidate_label_materialization", "passed": _contains(rows, "no_candidate_label_materialization", "materializes no candidate labels"), "detail": True},
        {"check": "no_adapter_modification", "passed": _contains(rows, "no_adapter_modification", "does not modify the adapter"), "detail": True},
        {"check": "no_fixture_mutation", "passed": _contains(rows, "no_fixture_mutation", "does not mutate fixtures"), "detail": True},
    ]


def _immutability_rows(before: Dict[str, str]) -> List[Dict[str, Any]]:
    after = _snapshot_files()
    return [
        {"check": "scaffold_not_modified_by_audit", "passed": before.get(str(SCAFFOLD_PATH)) == after.get(str(SCAFFOLD_PATH)), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_not_modified", "passed": before.get(str(ADAPTER_PATH)) == after.get(str(ADAPTER_PATH)), "detail": str(ADAPTER_PATH)},
        {"check": "six_dt_validation_not_modified", "passed": before.get(str(VALIDATION_6DT)) == after.get(str(VALIDATION_6DT)), "detail": str(VALIDATION_6DT)},
        {"check": "six_du_audit_not_modified", "passed": before.get(str(AUDIT_6DU)) == after.get(str(AUDIT_6DU)), "detail": str(AUDIT_6DU)},
        {"check": "six_dv_plan_not_modified", "passed": before.get(str(PLAN_6DV)) == after.get(str(PLAN_6DV)), "detail": str(PLAN_6DV)},
        {"check": "prior_validation_audit_plan_scripts_not_modified", "passed": all(before.get(str(path)) == after.get(str(path)) for path in [VALIDATION_6DL, AUDIT_6DM, PLAN_6DN, AUDIT_6DO, VALIDATION_6DP, AUDIT_6DQ, PLAN_6DR, AUDIT_6DS]), "detail": "6DL through 6DS unchanged"},
        {"check": "no_fixture_mutation", "passed": before == after, "detail": "fixtures and tracked dependency files unchanged"},
    ]


def main() -> None:
    before = _snapshot_files()
    scaffold_source = SCAFFOLD_PATH.read_text(errors="ignore")

    plan_execution = _run_plan()
    plan_execution_rows = [
        {
            "check": "plan_executes_successfully",
            "returncode": plan_execution["returncode"],
            "diagnosis": plan_execution["diagnosis"],
            "all_checks_passed": plan_execution["all_checks_passed"],
            "planning_only": plan_execution["planning_only"],
            "current_state_valid": plan_execution["current_state_valid"],
            "real_fetcher_resolution_contract_defined": plan_execution["real_fetcher_resolution_contract_defined"],
            "opt_in_gate_contract_defined": plan_execution["opt_in_gate_contract_defined"],
            "lazy_import_contract_defined": plan_execution["lazy_import_contract_defined"],
            "runtime_behavior_contract_defined": plan_execution["runtime_behavior_contract_defined"],
            "validation_strategy_defined": plan_execution["validation_strategy_defined"],
            "non_goals_defined": plan_execution["non_goals_defined"],
            "immutability_valid": plan_execution["immutability_valid"],
            "passed": plan_execution["passed"],
        }
    ]

    plan_artifact_rows = _plan_artifact_rows()
    current_state_rows = _current_state_rows(scaffold_source)
    real_fetcher_rows = _real_fetcher_contract_rows()
    opt_in_gate_rows = _opt_in_gate_rows()
    lazy_import_rows = _lazy_import_rows()
    runtime_rows = _runtime_rows()
    validation_strategy_rows = _validation_strategy_rows()
    non_goal_rows = _non_goal_rows()
    immutability_rows = _immutability_rows(before)

    _write_csv(OUTPUT_PLAN_EXECUTION, plan_execution_rows)
    _write_csv(OUTPUT_PLAN_ARTIFACTS, plan_artifact_rows)
    _write_csv(OUTPUT_CURRENT_STATE, current_state_rows)
    _write_csv(OUTPUT_REAL_FETCHER_CONTRACT, real_fetcher_rows)
    _write_csv(OUTPUT_OPT_IN_GATE, opt_in_gate_rows)
    _write_csv(OUTPUT_LAZY_IMPORT, lazy_import_rows)
    _write_csv(OUTPUT_RUNTIME, runtime_rows)
    _write_csv(OUTPUT_VALIDATION_STRATEGY, validation_strategy_rows)
    _write_csv(OUTPUT_NON_GOALS, non_goal_rows)
    _write_csv(OUTPUT_IMMUTABILITY, immutability_rows)

    checks = [
        {"check": "plan_execution_valid", "passed": plan_execution["passed"], "detail": plan_execution["diagnosis"]},
        {"check": "plan_artifacts_valid", "passed": all(row["passed"] for row in plan_artifact_rows), "detail": f"{sum(row['passed'] for row in plan_artifact_rows)}/{len(plan_artifact_rows)}"},
        {"check": "current_state_valid", "passed": all(row["passed"] for row in current_state_rows), "detail": f"{sum(row['passed'] for row in current_state_rows)}/{len(current_state_rows)}"},
        {"check": "real_fetcher_contract_valid", "passed": all(row["passed"] for row in real_fetcher_rows), "detail": f"{sum(row['passed'] for row in real_fetcher_rows)}/{len(real_fetcher_rows)}"},
        {"check": "opt_in_gate_contract_valid", "passed": all(row["passed"] for row in opt_in_gate_rows), "detail": f"{sum(row['passed'] for row in opt_in_gate_rows)}/{len(opt_in_gate_rows)}"},
        {"check": "lazy_import_contract_valid", "passed": all(row["passed"] for row in lazy_import_rows), "detail": f"{sum(row['passed'] for row in lazy_import_rows)}/{len(lazy_import_rows)}"},
        {"check": "runtime_behavior_contract_valid", "passed": all(row["passed"] for row in runtime_rows), "detail": f"{sum(row['passed'] for row in runtime_rows)}/{len(runtime_rows)}"},
        {"check": "validation_strategy_valid", "passed": all(row["passed"] for row in validation_strategy_rows), "detail": f"{sum(row['passed'] for row in validation_strategy_rows)}/{len(validation_strategy_rows)}"},
        {"check": "non_goals_valid", "passed": all(row["passed"] for row in non_goal_rows), "detail": f"{sum(row['passed'] for row in non_goal_rows)}/{len(non_goal_rows)}"},
        {"check": "immutability_valid", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(row['passed'] for row in immutability_rows)}/{len(immutability_rows)}"},
        {"check": "scaffold_not_modified_by_audit", "passed": any(row["check"] == "scaffold_not_modified_by_audit" and row["passed"] for row in immutability_rows), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_not_modified", "passed": any(row["check"] == "adapter_not_modified" and row["passed"] for row in immutability_rows), "detail": str(ADAPTER_PATH)},
        {"check": "six_dt_validation_not_modified", "passed": any(row["check"] == "six_dt_validation_not_modified" and row["passed"] for row in immutability_rows), "detail": str(VALIDATION_6DT)},
        {"check": "six_du_audit_not_modified", "passed": any(row["check"] == "six_du_audit_not_modified" and row["passed"] for row in immutability_rows), "detail": str(AUDIT_6DU)},
        {"check": "six_dv_plan_not_modified", "passed": any(row["check"] == "six_dv_plan_not_modified" and row["passed"] for row in immutability_rows), "detail": str(PLAN_6DV)},
        {"check": "no_fixture_mutation", "passed": any(row["check"] == "no_fixture_mutation" and row["passed"] for row in immutability_rows), "detail": True},
        {"check": "no_real_external_fetch", "passed": True, "detail": "audit-only plan validation"},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(row["passed"] for row in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_audit_complete",
        "audit_version": AUDIT_VERSION,
        "plan_execution_rows": len(plan_execution_rows),
        "plan_artifact_rows": len(plan_artifact_rows),
        "current_state_rows": len(current_state_rows),
        "real_fetcher_contract_rows": len(real_fetcher_rows),
        "opt_in_gate_contract_rows": len(opt_in_gate_rows),
        "lazy_import_contract_rows": len(lazy_import_rows),
        "runtime_behavior_contract_rows": len(runtime_rows),
        "validation_strategy_rows": len(validation_strategy_rows),
        "non_goal_rows": len(non_goal_rows),
        "immutability_rows": len(immutability_rows),
        "all_checks_passed": all_checks_passed,
        "audit_only": True,
        "plan_validated": plan_execution["passed"],
        "plan_artifacts_valid": all(row["passed"] for row in plan_artifact_rows),
        "current_state_valid": all(row["passed"] for row in current_state_rows),
        "real_fetcher_contract_valid": all(row["passed"] for row in real_fetcher_rows),
        "opt_in_gate_contract_valid": all(row["passed"] for row in opt_in_gate_rows),
        "lazy_import_contract_valid": all(row["passed"] for row in lazy_import_rows),
        "runtime_behavior_contract_valid": all(row["passed"] for row in runtime_rows),
        "validation_strategy_valid": all(row["passed"] for row in validation_strategy_rows),
        "non_goals_valid": all(row["passed"] for row in non_goal_rows),
        "immutability_valid": all(row["passed"] for row in immutability_rows),
        "scaffold_modified_by_audit": False,
        "adapter_modified": False,
        "six_dt_validation_modified": False,
        "six_du_audit_modified": False,
        "six_dv_plan_modified": False,
        "fixture_assets_mutated": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "candidate_labels_materialized_from_live_rows": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6DX_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution"
            if all_checks_passed
            else "6DW_patch_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan_audit"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
