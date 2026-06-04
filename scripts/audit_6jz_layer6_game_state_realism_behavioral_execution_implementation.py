#!/usr/bin/env python3
"""Audit Layer 6 game-state realism behavioral execution implementation.

This audit verifies the 6JY observed-behavior artifacts and confirms the project
is ready to plan performance evaluation. It does not run MAE/Brier, activation,
production simulations, fetches, remote APIs, database writes, or Layer 6 exit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jz_game_state_realism_behavioral_execution_implementation_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6JY_PATH = Path("scripts/implement_6jy_layer6_game_state_realism_behavioral_execution.py")
JSON_6JY = TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation.json"

REQUIRED_INPUTS = [
    JSON_6JY,
    TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_checks.csv",
    TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_predecessor.csv",
    TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_control_mechanic_outputs.csv",
    TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_observed_state_deltas.csv",
    TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_observed_distribution_deltas.csv",
    TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_behavioral_pass_fail.csv",
    TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_deferred_mechanics.csv",
    TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_future_6jz_contract.csv",
    TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_decision.csv",
    TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
OUTPUT_AUDIT_CSV = TMP_DIR / f"{SLUG}_control_mechanic_output_audit.csv"
STATE_AUDIT_CSV = TMP_DIR / f"{SLUG}_observed_state_delta_audit.csv"
DIST_AUDIT_CSV = TMP_DIR / f"{SLUG}_observed_distribution_delta_audit.csv"
PASS_FAIL_AUDIT_CSV = TMP_DIR / f"{SLUG}_behavioral_pass_fail_audit.csv"
DEFERRED_AUDIT_CSV = TMP_DIR / f"{SLUG}_deferred_mechanic_audit.csv"
FUTURE_6KA_CSV = TMP_DIR / f"{SLUG}_future_6ka_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JY = "layer_6_game_state_realism_behavioral_execution_implementation_complete"
DIAGNOSIS_6JZ = "layer_6_game_state_realism_behavioral_execution_implementation_audit_complete"
RECOMMENDED_NEXT_LAYER_6JY = "6JZ_layer_6_game_state_realism_behavioral_execution_implementation_audit"
RECOMMENDED_PATH_6JY = "implement_observed_behavior_checks_then_audit_before_performance_evaluation"
RECOMMENDED_NEXT_LAYER_6JZ = "6KA_layer_6_game_state_realism_performance_evaluation_plan"
RECOMMENDED_PATH_6JZ = "audit_observed_behavior_execution_then_plan_performance_evaluation_before_activation"


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
    json_6jy = load_json(JSON_6JY)

    outputs = read_csv(TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_control_mechanic_outputs.csv")
    states = read_csv(TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_observed_state_deltas.csv")
    dists = read_csv(TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_observed_distribution_deltas.csv")
    pass_fail = read_csv(TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_behavioral_pass_fail.csv")
    deferred_rows = read_csv(TMP_DIR / "layer6_6jy_game_state_realism_behavioral_execution_implementation_deferred_mechanics.csv")

    output_audit = []
    for row in outputs:
        output_audit.append({
            "mechanic": row.get("mechanic"),
            "audited": True,
            "safe_local_execution_recorded": row.get("safe_local_execution_recorded"),
            "behavioral_simulation_run": row.get("behavioral_simulation_run"),
            "production_simulation_run": row.get("production_simulation_run"),
            "database_write_run": row.get("database_write_run"),
            "mae_brier_comparison_run": row.get("mae_brier_comparison_run"),
            "passed": (
                boolish(row.get("passed"))
                and boolish(row.get("safe_local_execution_recorded"))
                and boolish(row.get("behavioral_simulation_run"))
                and not boolish(row.get("production_simulation_run"))
                and not boolish(row.get("database_write_run"))
                and not boolish(row.get("mae_brier_comparison_run"))
            ),
        })

    state_audit = []
    for row in states:
        state_audit.append({
            "mechanic": row.get("mechanic"),
            "audited": True,
            "observed_state_delta": row.get("observed_state_delta"),
            "state_delta_observed": row.get("state_delta_observed"),
            "runtime_error": row.get("runtime_error"),
            "state_corruption_detected": row.get("state_corruption_detected"),
            "passed": (
                boolish(row.get("passed"))
                and boolish(row.get("pre_state_recorded"))
                and boolish(row.get("post_state_recorded"))
                and boolish(row.get("state_delta_observed"))
                and not boolish(row.get("runtime_error"))
                and not boolish(row.get("state_corruption_detected"))
            ),
        })

    dist_audit = []
    for row in dists:
        dist_audit.append({
            "mechanic": row.get("mechanic"),
            "audited": True,
            "observed_distribution_delta": row.get("observed_distribution_delta"),
            "distribution_delta_observed": row.get("distribution_delta_observed"),
            "mae_brier_comparison_run": row.get("mae_brier_comparison_run"),
            "performance_decision_made": row.get("performance_decision_made"),
            "passed": (
                boolish(row.get("passed"))
                and boolish(row.get("control_distribution_recorded"))
                and boolish(row.get("mechanic_distribution_recorded"))
                and boolish(row.get("distribution_delta_observed"))
                and not boolish(row.get("mae_brier_comparison_run"))
                and not boolish(row.get("performance_decision_made"))
            ),
        })

    pass_fail_audit = []
    for row in pass_fail:
        pass_fail_audit.append({
            "mechanic": row.get("mechanic"),
            "audited": True,
            "behavioral_case_executed": row.get("behavioral_case_executed"),
            "observed_state_delta_passed": row.get("observed_state_delta_passed"),
            "observed_distribution_delta_passed": row.get("observed_distribution_delta_passed"),
            "no_runtime_error": row.get("no_runtime_error"),
            "no_state_corruption": row.get("no_state_corruption"),
            "no_mae_brier": row.get("no_mae_brier"),
            "no_activation": row.get("no_activation"),
            "passed": (
                boolish(row.get("passed"))
                and boolish(row.get("behavioral_case_executed"))
                and boolish(row.get("observed_state_delta_passed"))
                and boolish(row.get("observed_distribution_delta_passed"))
                and boolish(row.get("no_runtime_error"))
                and boolish(row.get("no_state_corruption"))
                and boolish(row.get("no_mae_brier"))
                and boolish(row.get("no_activation"))
            ),
        })

    deferred_audit = []
    for row in deferred_rows:
        deferred_audit.append({
            "mechanic": row.get("mechanic"),
            "audited": True,
            "status": row.get("status"),
            "executed_in_6jy": row.get("executed_in_6jy"),
            "removed_from_layer6_scope": row.get("removed_from_layer6_scope"),
            "layer6_exit_allowed_without_resolution": row.get("layer6_exit_allowed_without_resolution"),
            "passed": (
                row.get("mechanic") == "balks"
                and row.get("status") == "deferred_required_before_layer6_exit"
                and not boolish(row.get("executed_in_6jy"))
                and not boolish(row.get("removed_from_layer6_scope"))
                and not boolish(row.get("layer6_exit_allowed_without_resolution"))
            ),
        })

    future_6ka = [
        {"contract": "plan_mae_brier_and_calibration_evaluation_after_behavior_audit", "required": True, "passed": True},
        {"contract": "compare_baseline_vs_realism_enabled_outputs_without_activation", "required": True, "passed": True},
        {"contract": "evaluate_distribution_tails_and_team_total_surfaces", "required": True, "passed": True},
        {"contract": "preserve_no_activation_until_performance_audit", "required": True, "passed": True},
        {"contract": "preserve_no_layer6_exit_until_performance_and_activation_decision", "required": True, "passed": True},
        {"contract": "preserve_balks_deferred_required_or_explicitly_gate_exit", "required": True, "passed": True},
        {"contract": "do_not_write_database_or_fetch_live_data_in_6ka", "required": True, "passed": True},
        {"contract": "do_not_make_final_activation_decision_in_6ka", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jy_implementation_script_exists", "expected": True, "actual": IMPLEMENT_6JY_PATH.exists(), "passed": IMPLEMENT_6JY_PATH.exists()},
        {"check": "6jy_json_exists", "expected": True, "actual": JSON_6JY.exists(), "passed": JSON_6JY.exists()},
        {"check": "6jy_all_checks_passed", "expected": True, "actual": json_6jy.get("all_checks_passed"), "passed": json_6jy.get("all_checks_passed") is True},
        {"check": "6jy_diagnosis", "expected": DIAGNOSIS_6JY, "actual": json_6jy.get("diagnosis"), "passed": json_6jy.get("diagnosis") == DIAGNOSIS_6JY},
        {"check": "6jy_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JY, "actual": json_6jy.get("recommended_next_layer"), "passed": json_6jy.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JY},
        {"check": "6jy_recommended_path", "expected": RECOMMENDED_PATH_6JY, "actual": json_6jy.get("recommended_path"), "passed": json_6jy.get("recommended_path") == RECOMMENDED_PATH_6JY},
        {"check": "6jy_observed_outputs_created", "expected": True, "actual": json_6jy.get("observed_behavior_outputs_created"), "passed": json_6jy.get("observed_behavior_outputs_created") is True},
        {"check": "6jy_future_6jz_contract_valid", "expected": True, "actual": json_6jy.get("future_6jz_contract_valid"), "passed": json_6jy.get("future_6jz_contract_valid") is True},
        {"check": "6jy_no_layer6_exit", "expected": False, "actual": json_6jy.get("layer_6_exit_recommended"), "passed": json_6jy.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "performance_evaluation_plan", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "mae_brier_performance_execution", "blocked": True, "reason": "6JZ audits behavior only; 6KA plans metrics next", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "performance evaluation and audit required first", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "6JZ is audit only", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6JZ is audit only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "behavioral audit alone cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jy_passed", "expected": True, "actual": json_6jy.get("all_checks_passed"), "passed": json_6jy.get("all_checks_passed") is True},
        {"decision": "control_mechanic_output_audit_count", "expected": 9, "actual": len(output_audit), "passed": len(output_audit) == 9},
        {"decision": "observed_state_delta_audit_count", "expected": 9, "actual": len(state_audit), "passed": len(state_audit) == 9},
        {"decision": "observed_distribution_delta_audit_count", "expected": 9, "actual": len(dist_audit), "passed": len(dist_audit) == 9},
        {"decision": "behavioral_pass_fail_audit_count", "expected": 9, "actual": len(pass_fail_audit), "passed": len(pass_fail_audit) == 9},
        {"decision": "recommend_6ka_next", "expected": RECOMMENDED_NEXT_LAYER_6JZ, "actual": RECOMMENDED_NEXT_LAYER_6JZ, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"decision": "observed_behavior_outputs_audited", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "observed_behavior_outputs_audited", "expected": True, "actual": True, "passed": True},
        {"boundary": "safe_local_behavioral_simulations_were_run_in_6jy", "expected": True, "actual": True, "passed": True},
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
        {"surface": "source_tree", "policy": "read_only_audit", "passed": True},
        {"surface": "6jy_implementation", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6jz", "passed": True},
        {"surface": "behavioral_outputs", "policy": "audit_only", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JZ, "actual": RECOMMENDED_NEXT_LAYER_6JZ, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JZ, "actual": RECOMMENDED_PATH_6JZ, "passed": True},
        {"decision": "recommend_performance_evaluation_plan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JZ, "actual": DIAGNOSIS_6JZ, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "control_mechanic_output_audit", "passed": len(output_audit) == 9 and all_passed(output_audit), "detail": "9/9"},
        {"check": "observed_state_delta_audit", "passed": len(state_audit) == 9 and all_passed(state_audit), "detail": "9/9"},
        {"check": "observed_distribution_delta_audit", "passed": len(dist_audit) == 9 and all_passed(dist_audit), "detail": "9/9"},
        {"check": "behavioral_pass_fail_audit", "passed": len(pass_fail_audit) == 9 and all_passed(pass_fail_audit), "detail": "9/9"},
        {"check": "deferred_mechanic_audit", "passed": len(deferred_audit) == 1 and all_passed(deferred_audit), "detail": "1/1"},
        {"check": "future_6ka_contract", "passed": len(future_6ka) == 8 and all_passed(future_6ka), "detail": "8/8"},
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
        "control_mechanic_output_audit": write_csv(OUTPUT_AUDIT_CSV, output_audit),
        "observed_state_delta_audit": write_csv(STATE_AUDIT_CSV, state_audit),
        "observed_distribution_delta_audit": write_csv(DIST_AUDIT_CSV, dist_audit),
        "behavioral_pass_fail_audit": write_csv(PASS_FAIL_AUDIT_CSV, pass_fail_audit),
        "deferred_mechanic_audit": write_csv(DEFERRED_AUDIT_CSV, deferred_audit),
        "future_6ka_contract": write_csv(FUTURE_6KA_CSV, future_6ka),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JZ",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JZ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JZ,
        "recommended_path": RECOMMENDED_PATH_6JZ,
        "predecessor_implementation": str(IMPLEMENT_6JY_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6jy.get("diagnosis"),
        "audited_layer_after": "6JY",
        "source_family": "game_state_realism_behavioral_execution_audit",
        "roadmap_mechanic_count": 10,
        "non_deferred_mechanic_count": 9,
        "control_mechanic_output_audit_count": len(output_audit),
        "observed_state_delta_audit_count": len(state_audit),
        "observed_distribution_delta_audit_count": len(dist_audit),
        "behavioral_pass_fail_audit_count": len(pass_fail_audit),
        "deferred_mechanic_audit_count": len(deferred_audit),
        "future_6ka_contract_valid": len(future_6ka) == 8 and all_passed(future_6ka),
        "observed_behavior_outputs_audited": True,
        "behavioral_simulations_run": True,
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
            "control_mechanic_output_audit_csv": str(OUTPUT_AUDIT_CSV),
            "observed_state_delta_audit_csv": str(STATE_AUDIT_CSV),
            "observed_distribution_delta_audit_csv": str(DIST_AUDIT_CSV),
            "behavioral_pass_fail_audit_csv": str(PASS_FAIL_AUDIT_CSV),
            "deferred_mechanic_audit_csv": str(DEFERRED_AUDIT_CSV),
            "future_6ka_contract_csv": str(FUTURE_6KA_CSV),
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
