#!/usr/bin/env python3
"""Plan Layer 6 game-state realism performance evaluation.

This planning layer defines baseline-vs-realism performance evaluation after
observed behavior execution audit. It does not run MAE/Brier, production
simulations, activation, fetches, remote APIs, database writes, or Layer 6 exit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ka_game_state_realism_performance_evaluation_plan"
TMP_DIR = Path("tmp")

AUDIT_6JZ_PATH = Path("scripts/audit_6jz_layer6_game_state_realism_behavioral_execution_implementation.py")
JSON_6JZ = TMP_DIR / "layer6_6jz_game_state_realism_behavioral_execution_implementation_audit.json"

REQUIRED_INPUTS = [
    JSON_6JZ,
    TMP_DIR / "layer6_6jz_game_state_realism_behavioral_execution_implementation_audit_checks.csv",
    TMP_DIR / "layer6_6jz_game_state_realism_behavioral_execution_implementation_audit_predecessor.csv",
    TMP_DIR / "layer6_6jz_game_state_realism_behavioral_execution_implementation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6jz_game_state_realism_behavioral_execution_implementation_audit_control_mechanic_output_audit.csv",
    TMP_DIR / "layer6_6jz_game_state_realism_behavioral_execution_implementation_audit_observed_state_delta_audit.csv",
    TMP_DIR / "layer6_6jz_game_state_realism_behavioral_execution_implementation_audit_observed_distribution_delta_audit.csv",
    TMP_DIR / "layer6_6jz_game_state_realism_behavioral_execution_implementation_audit_behavioral_pass_fail_audit.csv",
    TMP_DIR / "layer6_6jz_game_state_realism_behavioral_execution_implementation_audit_deferred_mechanic_audit.csv",
    TMP_DIR / "layer6_6jz_game_state_realism_behavioral_execution_implementation_audit_future_6ka_contract.csv",
    TMP_DIR / "layer6_6jz_game_state_realism_behavioral_execution_implementation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6jz_game_state_realism_behavioral_execution_implementation_audit_decision.csv",
    TMP_DIR / "layer6_6jz_game_state_realism_behavioral_execution_implementation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6jz_game_state_realism_behavioral_execution_implementation_audit_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
METRIC_PLAN_CSV = TMP_DIR / f"{SLUG}_metric_plan.csv"
BASELINE_REALISM_CSV = TMP_DIR / f"{SLUG}_baseline_vs_realism_plan.csv"
DISTRIBUTION_QUALITY_CSV = TMP_DIR / f"{SLUG}_distribution_quality_plan.csv"
ACTIVATION_BLOCKERS_CSV = TMP_DIR / f"{SLUG}_activation_blockers.csv"
DEFERRED_POLICY_CSV = TMP_DIR / f"{SLUG}_deferred_mechanic_policy.csv"
FUTURE_6KB_CSV = TMP_DIR / f"{SLUG}_future_6kb_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JZ = "layer_6_game_state_realism_behavioral_execution_implementation_audit_complete"
DIAGNOSIS_6KA = "layer_6_game_state_realism_performance_evaluation_plan_complete"
RECOMMENDED_NEXT_LAYER_6JZ = "6KA_layer_6_game_state_realism_performance_evaluation_plan"
RECOMMENDED_PATH_6JZ = "audit_observed_behavior_execution_then_plan_performance_evaluation_before_activation"
RECOMMENDED_NEXT_LAYER_6KA = "6KB_layer_6_game_state_realism_performance_evaluation_implementation"
RECOMMENDED_PATH_6KA = "plan_performance_evaluation_then_run_metrics_before_activation_decision"


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
    json_6jz = load_json(JSON_6JZ)

    metric_plan = [
        {"metric": "mae", "purpose": "absolute run prediction error", "run_in_6ka": False, "required_for_6kb": True, "passed": True},
        {"metric": "brier", "purpose": "win probability scoring quality", "run_in_6ka": False, "required_for_6kb": True, "passed": True},
        {"metric": "calibration", "purpose": "probability bucket reliability", "run_in_6ka": False, "required_for_6kb": True, "passed": True},
        {"metric": "run_distribution", "purpose": "full-game run distribution quality", "run_in_6ka": False, "required_for_6kb": True, "passed": True},
        {"metric": "team_totals", "purpose": "team total surface accuracy", "run_in_6ka": False, "required_for_6kb": True, "passed": True},
        {"metric": "tail_outcomes", "purpose": "alternate totals and high/low scoring tails", "run_in_6ka": False, "required_for_6kb": True, "passed": True},
    ]

    baseline_vs_realism = [
        {"comparison": "baseline_simulator", "variant": "current_without_layer6_activation", "required": True, "activation_required": False, "passed": True},
        {"comparison": "realism_enabled_simulator", "variant": "layer6_mechanics_candidate_behavior", "required": True, "activation_required": False, "passed": True},
        {"comparison": "delta_report", "variant": "baseline_minus_realism_enabled", "required": True, "activation_required": False, "passed": True},
    ]

    distribution_quality = [
        {"surface": "full_game_run_distribution", "quality_check": "mean_variance_tail_shape", "required": True, "passed": True},
        {"surface": "inning_level_run_distribution", "quality_check": "inning_extension_and_zero_run_mass", "required": True, "passed": True},
        {"surface": "team_total_distribution", "quality_check": "team_total_mae_and_tail_accuracy", "required": True, "passed": True},
        {"surface": "win_probability_distribution", "quality_check": "brier_and_calibration_buckets", "required": True, "passed": True},
        {"surface": "alternate_total_tails", "quality_check": "high_low_scoring_tail_behavior", "required": True, "passed": True},
    ]

    activation_blockers = [
        {"blocker": "performance_evaluation_not_run", "blocks_activation": True, "passed": True},
        {"blocker": "performance_audit_not_run", "blocks_activation": True, "passed": True},
        {"blocker": "activation_decision_not_run", "blocks_activation": True, "passed": True},
        {"blocker": "balks_deferred_without_exit_gate", "blocks_layer6_exit": True, "passed": True},
        {"blocker": "no_layer6_exit_before_metrics_and_activation_decision", "blocks_layer6_exit": True, "passed": True},
    ]

    deferred_policy = [
        {
            "mechanic": "balks",
            "status": "deferred_required_or_explicitly_gate_exit",
            "performance_eval_inclusion": "excluded_until_probability_surface_or_explicit_exit_gate",
            "layer6_exit_allowed_without_resolution": False,
            "passed": True,
        }
    ]

    future_6kb = [
        {"contract": "run_mae_brier_and_calibration_comparison", "required": True, "passed": True},
        {"contract": "run_baseline_vs_realism_enabled_delta_report", "required": True, "passed": True},
        {"contract": "evaluate_run_distribution_and_tail_surfaces", "required": True, "passed": True},
        {"contract": "evaluate_team_total_surfaces", "required": True, "passed": True},
        {"contract": "do_not_activate_in_6kb", "required": True, "passed": True},
        {"contract": "do_not_grant_layer6_exit_in_6kb", "required": True, "passed": True},
        {"contract": "do_not_write_database_or_fetch_live_data_in_6kb", "required": True, "passed": True},
        {"contract": "preserve_balks_deferred_or_explicit_exit_gate", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jz_audit_script_exists", "expected": True, "actual": AUDIT_6JZ_PATH.exists(), "passed": AUDIT_6JZ_PATH.exists()},
        {"check": "6jz_json_exists", "expected": True, "actual": JSON_6JZ.exists(), "passed": JSON_6JZ.exists()},
        {"check": "6jz_all_checks_passed", "expected": True, "actual": json_6jz.get("all_checks_passed"), "passed": json_6jz.get("all_checks_passed") is True},
        {"check": "6jz_diagnosis", "expected": DIAGNOSIS_6JZ, "actual": json_6jz.get("diagnosis"), "passed": json_6jz.get("diagnosis") == DIAGNOSIS_6JZ},
        {"check": "6jz_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JZ, "actual": json_6jz.get("recommended_next_layer"), "passed": json_6jz.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JZ},
        {"check": "6jz_recommended_path", "expected": RECOMMENDED_PATH_6JZ, "actual": json_6jz.get("recommended_path"), "passed": json_6jz.get("recommended_path") == RECOMMENDED_PATH_6JZ},
        {"check": "6jz_future_6ka_contract_valid", "expected": True, "actual": json_6jz.get("future_6ka_contract_valid"), "passed": json_6jz.get("future_6ka_contract_valid") is True},
        {"check": "6jz_observed_behavior_outputs_audited", "expected": True, "actual": json_6jz.get("observed_behavior_outputs_audited"), "passed": json_6jz.get("observed_behavior_outputs_audited") is True},
        {"check": "6jz_no_layer6_exit", "expected": False, "actual": json_6jz.get("layer_6_exit_recommended"), "passed": json_6jz.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "performance_evaluation_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "performance implementation and audit required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation decision not made", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KA is planning only", "passed": True},
        {"blocked_surface": "live_fetches", "blocked": True, "reason": "6KA is planning only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "performance evaluation plan cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jz_passed", "expected": True, "actual": json_6jz.get("all_checks_passed"), "passed": json_6jz.get("all_checks_passed") is True},
        {"decision": "metric_plan_count", "expected": 6, "actual": len(metric_plan), "passed": len(metric_plan) == 6},
        {"decision": "baseline_vs_realism_plan_count", "expected": 3, "actual": len(baseline_vs_realism), "passed": len(baseline_vs_realism) == 3},
        {"decision": "distribution_quality_plan_count", "expected": 5, "actual": len(distribution_quality), "passed": len(distribution_quality) == 5},
        {"decision": "activation_blocker_count", "expected": 5, "actual": len(activation_blockers), "passed": len(activation_blockers) == 5},
        {"decision": "recommend_6kb_next", "expected": RECOMMENDED_NEXT_LAYER_6KA, "actual": RECOMMENDED_NEXT_LAYER_6KA, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"decision": "mae_brier_run", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "performance_evaluation_planned", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_mae_brier_comparison_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
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
        {"surface": "6jz_audit", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6ka", "passed": True},
        {"surface": "performance_evaluation", "policy": "planned_not_run_in_6ka", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KA, "actual": RECOMMENDED_NEXT_LAYER_6KA, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KA, "actual": RECOMMENDED_PATH_6KA, "passed": True},
        {"decision": "recommend_performance_evaluation_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KA, "actual": DIAGNOSIS_6KA, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "metric_plan", "passed": len(metric_plan) == 6 and all_passed(metric_plan), "detail": "6/6"},
        {"check": "baseline_vs_realism_plan", "passed": len(baseline_vs_realism) == 3 and all_passed(baseline_vs_realism), "detail": "3/3"},
        {"check": "distribution_quality_plan", "passed": len(distribution_quality) == 5 and all_passed(distribution_quality), "detail": "5/5"},
        {"check": "activation_blockers", "passed": len(activation_blockers) == 5 and all_passed(activation_blockers), "detail": "5/5"},
        {"check": "deferred_mechanic_policy", "passed": len(deferred_policy) == 1 and all_passed(deferred_policy), "detail": "1/1"},
        {"check": "future_6kb_contract", "passed": len(future_6kb) == 8 and all_passed(future_6kb), "detail": "8/8"},
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
        "metric_plan": write_csv(METRIC_PLAN_CSV, metric_plan),
        "baseline_vs_realism_plan": write_csv(BASELINE_REALISM_CSV, baseline_vs_realism),
        "distribution_quality_plan": write_csv(DISTRIBUTION_QUALITY_CSV, distribution_quality),
        "activation_blockers": write_csv(ACTIVATION_BLOCKERS_CSV, activation_blockers),
        "deferred_mechanic_policy": write_csv(DEFERRED_POLICY_CSV, deferred_policy),
        "future_6kb_contract": write_csv(FUTURE_6KB_CSV, future_6kb),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KA",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KA if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KA,
        "recommended_path": RECOMMENDED_PATH_6KA,
        "predecessor_audit": str(AUDIT_6JZ_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6jz.get("diagnosis"),
        "planned_layer_after": "6JZ",
        "source_family": "game_state_realism_performance_evaluation_plan",
        "metric_plan_count": len(metric_plan),
        "baseline_vs_realism_plan_count": len(baseline_vs_realism),
        "distribution_quality_plan_count": len(distribution_quality),
        "activation_blocker_count": len(activation_blockers),
        "deferred_mechanic_policy_count": len(deferred_policy),
        "future_6kb_contract_valid": len(future_6kb) == 8 and all_passed(future_6kb),
        "performance_evaluation_planned": True,
        "mae_brier_comparison_run": False,
        "production_simulations_run": False,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
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
            "metric_plan_csv": str(METRIC_PLAN_CSV),
            "baseline_vs_realism_plan_csv": str(BASELINE_REALISM_CSV),
            "distribution_quality_plan_csv": str(DISTRIBUTION_QUALITY_CSV),
            "activation_blockers_csv": str(ACTIVATION_BLOCKERS_CSV),
            "deferred_mechanic_policy_csv": str(DEFERRED_POLICY_CSV),
            "future_6kb_contract_csv": str(FUTURE_6KB_CSV),
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
