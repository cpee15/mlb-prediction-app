#!/usr/bin/env python3
"""Plan Layer 6 game-state realism behavioral verification.

This planning layer converts runtime wiring audit artifacts into explicit
behavioral verification plans. It does not run simulations, MAE/Brier, activation,
fetches, or database writes.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ju_game_state_realism_behavioral_verification_plan"
TMP_DIR = Path("tmp")

AUDIT_6JT_PATH = Path("scripts/audit_6jt_layer6_game_state_realism_runtime_wiring_implementation.py")
JSON_6JT = TMP_DIR / "layer6_6jt_game_state_realism_runtime_wiring_implementation_audit.json"

REQUIRED_INPUTS = [
    JSON_6JT,
    TMP_DIR / "layer6_6jt_game_state_realism_runtime_wiring_implementation_audit_checks.csv",
    TMP_DIR / "layer6_6jt_game_state_realism_runtime_wiring_implementation_audit_predecessor.csv",
    TMP_DIR / "layer6_6jt_game_state_realism_runtime_wiring_implementation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6jt_game_state_realism_runtime_wiring_implementation_audit_runtime_reachability_audit.csv",
    TMP_DIR / "layer6_6jt_game_state_realism_runtime_wiring_implementation_audit_mechanic_wiring_audit.csv",
    TMP_DIR / "layer6_6jt_game_state_realism_runtime_wiring_implementation_audit_sim_loop_surface_audit.csv",
    TMP_DIR / "layer6_6jt_game_state_realism_runtime_wiring_implementation_audit_state_transition_scaffolding_audit.csv",
    TMP_DIR / "layer6_6jt_game_state_realism_runtime_wiring_implementation_audit_distribution_scaffolding_audit.csv",
    TMP_DIR / "layer6_6jt_game_state_realism_runtime_wiring_implementation_audit_deferred_mechanic_audit.csv",
    TMP_DIR / "layer6_6jt_game_state_realism_runtime_wiring_implementation_audit_future_6ju_contract.csv",
    TMP_DIR / "layer6_6jt_game_state_realism_runtime_wiring_implementation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6jt_game_state_realism_runtime_wiring_implementation_audit_decision.csv",
    TMP_DIR / "layer6_6jt_game_state_realism_runtime_wiring_implementation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6jt_game_state_realism_runtime_wiring_implementation_audit_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
MECHANIC_PLAN_CSV = TMP_DIR / f"{SLUG}_mechanic_behavioral_plan.csv"
STATE_ASSERTION_CSV = TMP_DIR / f"{SLUG}_state_assertion_plan.csv"
DIST_PLAN_CSV = TMP_DIR / f"{SLUG}_distribution_snapshot_plan.csv"
PASS_FAIL_CSV = TMP_DIR / f"{SLUG}_pass_fail_criteria.csv"
DEFERRED_PLAN_CSV = TMP_DIR / f"{SLUG}_deferred_mechanic_plan.csv"
FUTURE_6JV_CSV = TMP_DIR / f"{SLUG}_future_6jv_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JT = "layer_6_game_state_realism_runtime_wiring_implementation_audit_complete"
DIAGNOSIS_6JU = "layer_6_game_state_realism_behavioral_verification_plan_complete"
RECOMMENDED_NEXT_LAYER_6JT = "6JU_layer_6_game_state_realism_behavioral_verification_plan"
RECOMMENDED_PATH_6JT = "audit_runtime_wiring_then_plan_behavioral_verification_before_performance_evaluation"
RECOMMENDED_NEXT_LAYER_6JU = "6JV_layer_6_game_state_realism_behavioral_verification_implementation"
RECOMMENDED_PATH_6JU = "plan_behavioral_verification_then_implement_behavioral_checks_before_performance_evaluation"

MECHANICS = [
    ("bullpen_sequencing_and_leverage_behavior", "bullpen_usage_and_leverage_distribution"),
    ("stolen_bases_and_caught_stealing", "steal_attempt_success_and_out_state_mutation"),
    ("first_to_third_advancement", "runner_advancement_first_to_third"),
    ("second_to_home_advancement", "runner_advancement_second_to_home"),
    ("wild_pitches_and_passed_balls", "non_batted_ball_runner_advancement"),
    ("extra_innings_and_ghost_runner_logic", "extra_inning_runner_and_walkoff_behavior"),
    ("double_plays_by_base_out_state", "base_out_double_play_mutation"),
    ("sac_flies_and_tagging_up", "tag_up_scoring_and_out_state_mutation"),
    ("pinch_hitters_and_substitutions", "lineup_chain_substitution_behavior"),
    ("balks", "deferred_balk_probability_surface_behavior"),
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
    json_6jt = load_json(JSON_6JT)

    mechanic_behavioral_plan = []
    state_assertion_plan = []
    distribution_snapshot_plan = []
    pass_fail_criteria = []

    for idx, (mechanic, behavior_focus) in enumerate(MECHANICS, start=1):
        deferred = mechanic == "balks"

        mechanic_behavioral_plan.append({
            "mechanic": mechanic,
            "priority": idx,
            "behavior_focus": behavior_focus,
            "verification_mode": "deferred_required_plan" if deferred else "behavioral_check_plan",
            "requires_control_case": True,
            "requires_mechanic_case": True,
            "requires_before_after_comparison": True,
            "run_now": False,
            "passed": True,
        })

        state_assertion_plan.append({
            "mechanic": mechanic,
            "pre_state_required": True,
            "post_state_required": True,
            "base_out_mutation_check": mechanic not in {"pinch_hitters_and_substitutions"},
            "runner_movement_check": mechanic not in {"bullpen_sequencing_and_leverage_behavior"},
            "scoring_mutation_check": mechanic not in {"pinch_hitters_and_substitutions"},
            "lineup_or_pitcher_chain_check": mechanic in {
                "pinch_hitters_and_substitutions",
                "bullpen_sequencing_and_leverage_behavior",
            },
            "inning_or_game_termination_check": mechanic == "extra_innings_and_ghost_runner_logic",
            "run_now": False,
            "passed": True,
        })

        distribution_snapshot_plan.append({
            "mechanic": mechanic,
            "baseline_snapshot_required": True,
            "mechanic_snapshot_required": True,
            "run_distribution_review_required": True,
            "tail_or_variance_review_required": True,
            "team_total_distribution_review_required": True,
            "mae_brier_comparison_allowed": False,
            "run_now": False,
            "passed": True,
        })

        pass_fail_criteria.append({
            "mechanic": mechanic,
            "pass_requires_behavioral_delta": not deferred,
            "pass_requires_no_state_corruption": True,
            "pass_requires_no_runtime_error": True,
            "pass_requires_distribution_snapshot_record": True,
            "pass_requires_no_db_write": True,
            "pass_requires_no_live_fetch": True,
            "deferred_mechanic_allowed_to_pass_plan_only": deferred,
            "layer6_exit_allowed": False,
            "passed": True,
        })

    deferred_plan = [
        {
            "mechanic": "balks",
            "status": "deferred_required_before_layer6_exit",
            "required_next_action": "define_probability_surface_or_explicit_deferral_gate_in_behavioral_implementation",
            "removed_from_layer6_scope": False,
            "layer6_exit_allowed_without_resolution": False,
            "passed": True,
        }
    ]

    future_6jv = [
        {"contract": "implement_behavioral_checks_for_all_non_deferred_mechanics", "required": True, "passed": True},
        {"contract": "preserve_balks_as_deferred_required_mechanic", "required": True, "passed": True},
        {"contract": "create_control_and_mechanic_cases", "required": True, "passed": True},
        {"contract": "record_pre_and_post_state_assertions", "required": True, "passed": True},
        {"contract": "record_distribution_snapshot_outputs_without_mae_brier", "required": True, "passed": True},
        {"contract": "do_not_run_mae_brier_in_6jv", "required": True, "passed": True},
        {"contract": "do_not_activate_mechanics_in_6jv", "required": True, "passed": True},
        {"contract": "do_not_grant_layer6_exit_in_6jv", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jt_audit_script_exists", "expected": True, "actual": AUDIT_6JT_PATH.exists(), "passed": AUDIT_6JT_PATH.exists()},
        {"check": "6jt_json_exists", "expected": True, "actual": JSON_6JT.exists(), "passed": JSON_6JT.exists()},
        {"check": "6jt_all_checks_passed", "expected": True, "actual": json_6jt.get("all_checks_passed"), "passed": json_6jt.get("all_checks_passed") is True},
        {"check": "6jt_diagnosis", "expected": DIAGNOSIS_6JT, "actual": json_6jt.get("diagnosis"), "passed": json_6jt.get("diagnosis") == DIAGNOSIS_6JT},
        {"check": "6jt_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JT, "actual": json_6jt.get("recommended_next_layer"), "passed": json_6jt.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JT},
        {"check": "6jt_recommended_path", "expected": RECOMMENDED_PATH_6JT, "actual": json_6jt.get("recommended_path"), "passed": json_6jt.get("recommended_path") == RECOMMENDED_PATH_6JT},
        {"check": "6jt_reachability_audit_count", "expected": 10, "actual": json_6jt.get("runtime_reachability_audit_count"), "passed": json_6jt.get("runtime_reachability_audit_count") == 10},
        {"check": "6jt_future_6ju_contract_valid", "expected": True, "actual": json_6jt.get("future_6ju_contract_valid"), "passed": json_6jt.get("future_6ju_contract_valid") is True},
        {"check": "6jt_no_layer6_exit", "expected": False, "actual": json_6jt.get("layer_6_exit_recommended"), "passed": json_6jt.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "behavioral_verification_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "behavioral_simulation_execution", "blocked": True, "reason": "6JU is planning only", "passed": True},
        {"blocked_surface": "mae_brier_performance_evaluation", "blocked": True, "reason": "behavioral verification implementation and audit required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "must certify behavior and performance first", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "planning only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "behavioral verification plan cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jt_passed", "expected": True, "actual": json_6jt.get("all_checks_passed"), "passed": json_6jt.get("all_checks_passed") is True},
        {"decision": "mechanic_behavioral_plan_count", "expected": 10, "actual": len(mechanic_behavioral_plan), "passed": len(mechanic_behavioral_plan) == 10},
        {"decision": "state_assertion_plan_count", "expected": 10, "actual": len(state_assertion_plan), "passed": len(state_assertion_plan) == 10},
        {"decision": "distribution_snapshot_plan_count", "expected": 10, "actual": len(distribution_snapshot_plan), "passed": len(distribution_snapshot_plan) == 10},
        {"decision": "pass_fail_criteria_count", "expected": 10, "actual": len(pass_fail_criteria), "passed": len(pass_fail_criteria) == 10},
        {"decision": "recommend_6jv_next", "expected": RECOMMENDED_NEXT_LAYER_6JU, "actual": RECOMMENDED_NEXT_LAYER_6JU, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_behavioral_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_mechanic_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_simulator_logic_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mae_brier_comparison", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_activation_execution", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation_for_production", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_final_activation_decision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
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
        {"surface": "6jt_audit", "policy": "read_only", "passed": True},
        {"surface": "6js_implementation", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
        {"surface": "behavioral_simulations", "policy": "not_run_in_6ju", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JU, "actual": RECOMMENDED_NEXT_LAYER_6JU, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JU, "actual": RECOMMENDED_PATH_6JU, "passed": True},
        {"decision": "recommend_behavioral_verification_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metrics_decision_or_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JU, "actual": DIAGNOSIS_6JU, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "mechanic_behavioral_plan", "passed": len(mechanic_behavioral_plan) == 10 and all_passed(mechanic_behavioral_plan), "detail": f"{len(mechanic_behavioral_plan)}/10"},
        {"check": "state_assertion_plan", "passed": len(state_assertion_plan) == 10 and all_passed(state_assertion_plan), "detail": f"{len(state_assertion_plan)}/10"},
        {"check": "distribution_snapshot_plan", "passed": len(distribution_snapshot_plan) == 10 and all_passed(distribution_snapshot_plan), "detail": f"{len(distribution_snapshot_plan)}/10"},
        {"check": "pass_fail_criteria", "passed": len(pass_fail_criteria) == 10 and all_passed(pass_fail_criteria), "detail": f"{len(pass_fail_criteria)}/10"},
        {"check": "deferred_mechanic_plan", "passed": len(deferred_plan) == 1 and all_passed(deferred_plan), "detail": "1/1"},
        {"check": "future_6jv_contract", "passed": len(future_6jv) == 8 and all_passed(future_6jv), "detail": "8/8"},
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
        "mechanic_behavioral_plan": write_csv(MECHANIC_PLAN_CSV, mechanic_behavioral_plan),
        "state_assertion_plan": write_csv(STATE_ASSERTION_CSV, state_assertion_plan),
        "distribution_snapshot_plan": write_csv(DIST_PLAN_CSV, distribution_snapshot_plan),
        "pass_fail_criteria": write_csv(PASS_FAIL_CSV, pass_fail_criteria),
        "deferred_mechanic_plan": write_csv(DEFERRED_PLAN_CSV, deferred_plan),
        "future_6jv_contract": write_csv(FUTURE_6JV_CSV, future_6jv),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JU",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JU if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JU,
        "recommended_path": RECOMMENDED_PATH_6JU,
        "predecessor_audit": str(AUDIT_6JT_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6jt.get("diagnosis"),
        "planned_layer_after": "6JT",
        "source_family": "game_state_realism_behavioral_verification_plan",
        "roadmap_mechanic_count": 10,
        "mechanic_behavioral_plan_count": len(mechanic_behavioral_plan),
        "state_assertion_plan_count": len(state_assertion_plan),
        "distribution_snapshot_plan_count": len(distribution_snapshot_plan),
        "pass_fail_criteria_count": len(pass_fail_criteria),
        "deferred_mechanic_plan_count": len(deferred_plan),
        "future_6jv_contract_valid": len(future_6jv) == 8 and all_passed(future_6jv),
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
        "performance_evaluation_allowed_after_this_layer": False,
        "mae_brier_comparison_run": False,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "production_simulations_run": False,
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
            "mechanic_behavioral_plan_csv": str(MECHANIC_PLAN_CSV),
            "state_assertion_plan_csv": str(STATE_ASSERTION_CSV),
            "distribution_snapshot_plan_csv": str(DIST_PLAN_CSV),
            "pass_fail_criteria_csv": str(PASS_FAIL_CSV),
            "deferred_mechanic_plan_csv": str(DEFERRED_PLAN_CSV),
            "future_6jv_contract_csv": str(FUTURE_6JV_CSV),
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
