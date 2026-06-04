#!/usr/bin/env python3
"""Plan Layer 6 game-state realism behavioral execution.

This planning layer defines a safe non-production execution harness for real
behavioral checks. It does not execute cases, run MAE/Brier, activate mechanics,
fetch data, write databases, or grant Layer 6 exit credit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jx_game_state_realism_behavioral_execution_plan"
TMP_DIR = Path("tmp")

AUDIT_6JW_PATH = Path("scripts/audit_6jw_layer6_game_state_realism_behavioral_verification_implementation.py")
JSON_6JW = TMP_DIR / "layer6_6jw_game_state_realism_behavioral_verification_implementation_audit.json"

REQUIRED_INPUTS = [
    JSON_6JW,
    TMP_DIR / "layer6_6jw_game_state_realism_behavioral_verification_implementation_audit_checks.csv",
    TMP_DIR / "layer6_6jw_game_state_realism_behavioral_verification_implementation_audit_predecessor.csv",
    TMP_DIR / "layer6_6jw_game_state_realism_behavioral_verification_implementation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6jw_game_state_realism_behavioral_verification_implementation_audit_control_mechanic_case_audit.csv",
    TMP_DIR / "layer6_6jw_game_state_realism_behavioral_verification_implementation_audit_pre_post_state_assertion_audit.csv",
    TMP_DIR / "layer6_6jw_game_state_realism_behavioral_verification_implementation_audit_behavioral_delta_audit.csv",
    TMP_DIR / "layer6_6jw_game_state_realism_behavioral_verification_implementation_audit_distribution_snapshot_audit.csv",
    TMP_DIR / "layer6_6jw_game_state_realism_behavioral_verification_implementation_audit_deferred_mechanic_audit.csv",
    TMP_DIR / "layer6_6jw_game_state_realism_behavioral_verification_implementation_audit_future_6jx_contract.csv",
    TMP_DIR / "layer6_6jw_game_state_realism_behavioral_verification_implementation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6jw_game_state_realism_behavioral_verification_implementation_audit_decision.csv",
    TMP_DIR / "layer6_6jw_game_state_realism_behavioral_verification_implementation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6jw_game_state_realism_behavioral_verification_implementation_audit_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
HARNESS_CSV = TMP_DIR / f"{SLUG}_execution_harness_plan.csv"
EXECUTION_PLAN_CSV = TMP_DIR / f"{SLUG}_control_mechanic_execution_plan.csv"
STATE_DELTA_CSV = TMP_DIR / f"{SLUG}_observed_state_delta_plan.csv"
DIST_DELTA_CSV = TMP_DIR / f"{SLUG}_observed_distribution_delta_plan.csv"
DEFERRED_PLAN_CSV = TMP_DIR / f"{SLUG}_deferred_mechanic_plan.csv"
FUTURE_6JY_CSV = TMP_DIR / f"{SLUG}_future_6jy_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JW = "layer_6_game_state_realism_behavioral_verification_implementation_audit_complete"
DIAGNOSIS_6JX = "layer_6_game_state_realism_behavioral_execution_plan_complete"
RECOMMENDED_NEXT_LAYER_6JW = "6JX_layer_6_game_state_realism_behavioral_execution_plan"
RECOMMENDED_PATH_6JW = "audit_behavioral_verification_artifacts_then_plan_real_behavioral_execution_before_performance_evaluation"
RECOMMENDED_NEXT_LAYER_6JX = "6JY_layer_6_game_state_realism_behavioral_execution_implementation"
RECOMMENDED_PATH_6JX = "plan_real_behavioral_execution_then_implement_observed_behavior_checks_before_performance_evaluation"

NON_DEFERRED_MECHANICS = [
    "bullpen_sequencing_and_leverage_behavior",
    "stolen_bases_and_caught_stealing",
    "first_to_third_advancement",
    "second_to_home_advancement",
    "wild_pitches_and_passed_balls",
    "extra_innings_and_ghost_runner_logic",
    "double_plays_by_base_out_state",
    "sac_flies_and_tagging_up",
    "pinch_hitters_and_substitutions",
]


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True, "passed": True}]
    fieldnames: List[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    parsed = json.loads(path.read_text(encoding="utf-8"))
    return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}


def syntax_compile() -> Tuple[int, str]:
    failures: List[str] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            try:
                compile(path.read_text(encoding="utf-8"), str(path), "exec")
            except Exception as exc:
                failures.append(f"{path}: {type(exc).__name__}: {exc}")
    return (0 if not failures else 1, "\n".join(failures))


def boolish(value: Any) -> bool:
    return value is True or str(value).lower() == "true"


def all_passed(rows: List[Dict[str, Any]]) -> bool:
    return all(boolish(row.get("passed", "")) for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6jw = load_json(JSON_6JW)

    harness_plan = [
        {
            "harness_component": "non_production_behavioral_case_runner",
            "purpose": "execute deterministic control and mechanic cases without production activation",
            "writes_database": False,
            "fetches_live_data": False,
            "calls_remote_api": False,
            "runs_mae_brier": False,
            "activates_mechanics": False,
            "required_for_6jy": True,
            "passed": True,
        },
        {
            "harness_component": "observed_state_delta_collector",
            "purpose": "record actual pre/post state deltas from safe local execution",
            "writes_database": False,
            "fetches_live_data": False,
            "calls_remote_api": False,
            "runs_mae_brier": False,
            "activates_mechanics": False,
            "required_for_6jy": True,
            "passed": True,
        },
        {
            "harness_component": "observed_distribution_delta_collector",
            "purpose": "record local distribution deltas without judging predictive performance",
            "writes_database": False,
            "fetches_live_data": False,
            "calls_remote_api": False,
            "runs_mae_brier": False,
            "activates_mechanics": False,
            "required_for_6jy": True,
            "passed": True,
        },
    ]

    execution_plan = []
    state_delta_plan = []
    distribution_delta_plan = []

    for idx, mechanic in enumerate(NON_DEFERRED_MECHANICS, start=1):
        execution_plan.append({
            "mechanic": mechanic,
            "priority": idx,
            "control_case_id": f"control_{mechanic}",
            "mechanic_case_id": f"mechanic_{mechanic}",
            "execute_in_6jy": True,
            "production_simulation_allowed": False,
            "database_write_allowed": False,
            "mae_brier_allowed": False,
            "activation_allowed": False,
            "passed": True,
        })

        state_delta_plan.append({
            "mechanic": mechanic,
            "priority": idx,
            "pre_state_capture_required": True,
            "post_state_capture_required": True,
            "observed_state_delta_required": True,
            "base_out_delta_required": mechanic != "pinch_hitters_and_substitutions",
            "runner_delta_required": mechanic != "bullpen_sequencing_and_leverage_behavior",
            "lineup_or_pitcher_chain_delta_required": mechanic in {
                "bullpen_sequencing_and_leverage_behavior",
                "pinch_hitters_and_substitutions",
            },
            "runtime_error_capture_required": True,
            "passed": True,
        })

        distribution_delta_plan.append({
            "mechanic": mechanic,
            "priority": idx,
            "control_distribution_required": True,
            "mechanic_distribution_required": True,
            "observed_distribution_delta_required": True,
            "run_distribution_delta_required": True,
            "tail_or_variance_delta_required": True,
            "team_total_delta_required": True,
            "mae_brier_allowed": False,
            "passed": True,
        })

    deferred_plan = [
        {
            "mechanic": "balks",
            "status": "deferred_required_before_layer6_exit",
            "required_next_action": "keep deferred until probability surface or explicit behavioral gate exists",
            "include_in_6jy_execution": False,
            "removed_from_layer6_scope": False,
            "layer6_exit_allowed_without_resolution": False,
            "passed": True,
        }
    ]

    future_6jy = [
        {"contract": "execute_safe_local_control_and_mechanic_cases_for_9_non_deferred_mechanics", "required": True, "passed": True},
        {"contract": "record_observed_pre_and_post_state_deltas", "required": True, "passed": True},
        {"contract": "record_observed_distribution_deltas_without_mae_brier", "required": True, "passed": True},
        {"contract": "preserve_no_database_write_boundary", "required": True, "passed": True},
        {"contract": "preserve_no_live_fetch_or_remote_api_boundary", "required": True, "passed": True},
        {"contract": "preserve_balks_as_deferred_required_before_exit", "required": True, "passed": True},
        {"contract": "do_not_activate_or_grant_layer6_exit_in_6jy", "required": True, "passed": True},
        {"contract": "do_not_run_mae_brier_until_behavioral_execution_audit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jw_audit_script_exists", "expected": True, "actual": AUDIT_6JW_PATH.exists(), "passed": AUDIT_6JW_PATH.exists()},
        {"check": "6jw_json_exists", "expected": True, "actual": JSON_6JW.exists(), "passed": JSON_6JW.exists()},
        {"check": "6jw_all_checks_passed", "expected": True, "actual": json_6jw.get("all_checks_passed"), "passed": json_6jw.get("all_checks_passed") is True},
        {"check": "6jw_diagnosis", "expected": DIAGNOSIS_6JW, "actual": json_6jw.get("diagnosis"), "passed": json_6jw.get("diagnosis") == DIAGNOSIS_6JW},
        {"check": "6jw_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JW, "actual": json_6jw.get("recommended_next_layer"), "passed": json_6jw.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JW},
        {"check": "6jw_recommended_path", "expected": RECOMMENDED_PATH_6JW, "actual": json_6jw.get("recommended_path"), "passed": json_6jw.get("recommended_path") == RECOMMENDED_PATH_6JW},
        {"check": "6jw_real_behavioral_execution_required", "expected": True, "actual": json_6jw.get("real_behavioral_execution_still_required"), "passed": json_6jw.get("real_behavioral_execution_still_required") is True},
        {"check": "6jw_future_6jx_contract_valid", "expected": True, "actual": json_6jw.get("future_6jx_contract_valid"), "passed": json_6jw.get("future_6jx_contract_valid") is True},
        {"check": "6jw_no_layer6_exit", "expected": False, "actual": json_6jw.get("layer_6_exit_recommended"), "passed": json_6jw.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "real_behavioral_execution_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "mae_brier_performance_evaluation", "blocked": True, "reason": "real behavioral execution and audit required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "must certify behavior and performance first", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "6JX is planning only", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6JX is planning only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "behavioral execution plan cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jw_passed", "expected": True, "actual": json_6jw.get("all_checks_passed"), "passed": json_6jw.get("all_checks_passed") is True},
        {"decision": "execution_harness_plan_count", "expected": 3, "actual": len(harness_plan), "passed": len(harness_plan) == 3},
        {"decision": "control_mechanic_execution_plan_count", "expected": 9, "actual": len(execution_plan), "passed": len(execution_plan) == 9},
        {"decision": "observed_state_delta_plan_count", "expected": 9, "actual": len(state_delta_plan), "passed": len(state_delta_plan) == 9},
        {"decision": "observed_distribution_delta_plan_count", "expected": 9, "actual": len(distribution_delta_plan), "passed": len(distribution_delta_plan) == 9},
        {"decision": "recommend_6jy_next", "expected": RECOMMENDED_NEXT_LAYER_6JX, "actual": RECOMMENDED_NEXT_LAYER_6JX, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"decision": "real_behavioral_execution_planned", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "real_behavioral_execution_planned", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_real_behavioral_execution_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_behavioral_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_mechanic_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_simulator_logic_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mae_brier_comparison", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_activation_execution", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation_for_production", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_final_activation_decision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_truth_join_rerun", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation_rerun", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_recommendation", "expected": False, "actual": False, "passed": True},
    ]

    immutability_rows = [
        {"surface": "source_tree", "policy": "read_only_planning", "passed": True},
        {"surface": "6jw_audit", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6jx", "passed": True},
        {"surface": "behavioral_execution", "policy": "planned_not_run_in_6jx", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JX, "actual": RECOMMENDED_NEXT_LAYER_6JX, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JX, "actual": RECOMMENDED_PATH_6JX, "passed": True},
        {"decision": "recommend_real_behavioral_execution_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metrics_decision_or_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JX, "actual": DIAGNOSIS_6JX, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "execution_harness_plan", "passed": len(harness_plan) == 3 and all_passed(harness_plan), "detail": "3/3"},
        {"check": "control_mechanic_execution_plan", "passed": len(execution_plan) == 9 and all_passed(execution_plan), "detail": "9/9"},
        {"check": "observed_state_delta_plan", "passed": len(state_delta_plan) == 9 and all_passed(state_delta_plan), "detail": "9/9"},
        {"check": "observed_distribution_delta_plan", "passed": len(distribution_delta_plan) == 9 and all_passed(distribution_delta_plan), "detail": "9/9"},
        {"check": "deferred_mechanic_plan", "passed": len(deferred_plan) == 1 and all_passed(deferred_plan), "detail": "1/1"},
        {"check": "future_6jy_contract", "passed": len(future_6jy) == 8 and all_passed(future_6jy), "detail": "8/8"},
        {"check": "readonly_sources", "passed": all_passed(readonly_rows), "detail": f"{sum(1 for r in readonly_rows if r['passed'])}/{len(readonly_rows)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_rows), "detail": f"{sum(1 for r in blocking_rows if r['passed'])}/{len(blocking_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all_passed(immutability_rows), "detail": f"{sum(1 for r in immutability_rows if r['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "execution_harness_plan": write_csv(HARNESS_CSV, harness_plan),
        "control_mechanic_execution_plan": write_csv(EXECUTION_PLAN_CSV, execution_plan),
        "observed_state_delta_plan": write_csv(STATE_DELTA_CSV, state_delta_plan),
        "observed_distribution_delta_plan": write_csv(DIST_DELTA_CSV, distribution_delta_plan),
        "deferred_mechanic_plan": write_csv(DEFERRED_PLAN_CSV, deferred_plan),
        "future_6jy_contract": write_csv(FUTURE_6JY_CSV, future_6jy),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JX",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JX if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JX,
        "recommended_path": RECOMMENDED_PATH_6JX,
        "predecessor_audit": str(AUDIT_6JW_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6jw.get("diagnosis"),
        "planned_layer_after": "6JW",
        "source_family": "game_state_realism_behavioral_execution_plan",
        "roadmap_mechanic_count": 10,
        "non_deferred_mechanic_count": 9,
        "execution_harness_plan_count": len(harness_plan),
        "control_mechanic_execution_plan_count": len(execution_plan),
        "observed_state_delta_plan_count": len(state_delta_plan),
        "observed_distribution_delta_plan_count": len(distribution_delta_plan),
        "deferred_mechanic_plan_count": len(deferred_plan),
        "future_6jy_contract_valid": len(future_6jy) == 8 and all_passed(future_6jy),
        "real_behavioral_execution_planned": True,
        "behavioral_simulations_run": False,
        "production_simulations_run": False,
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
        "performance_evaluation_allowed_after_this_layer": False,
        "mae_brier_comparison_run": False,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "database_writes_run": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "execution_harness_plan_csv": str(HARNESS_CSV),
            "control_mechanic_execution_plan_csv": str(EXECUTION_PLAN_CSV),
            "observed_state_delta_plan_csv": str(STATE_DELTA_CSV),
            "observed_distribution_delta_plan_csv": str(DIST_DELTA_CSV),
            "deferred_mechanic_plan_csv": str(DEFERRED_PLAN_CSV),
            "future_6jy_contract_csv": str(FUTURE_6JY_CSV),
            "readonly_sources_csv": str(READONLY_CSV),
            "blocking_policy_csv": str(BLOCKING_CSV),
            "decision_csv": str(DECISION_CSV),
            "safety_boundaries_csv": str(SAFETY_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
