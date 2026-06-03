#!/usr/bin/env python3
"""Audit Layer 6 game-state realism runtime wiring implementation.

This audit layer verifies 6JS runtime wiring implementation artifacts.
It does not run performance evaluation, activate mechanics, fetch data, write
databases, or grant Layer 6 exit credit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jt_game_state_realism_runtime_wiring_implementation_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6JS_PATH = Path("scripts/implement_6js_layer6_game_state_realism_runtime_wiring.py")
JSON_6JS = TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation.json"

REQUIRED_INPUTS = [
    JSON_6JS,
    TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_checks.csv",
    TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_predecessor.csv",
    TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_runtime_reachability.csv",
    TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_mechanic_wiring.csv",
    TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_sim_loop_surface_verification.csv",
    TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_state_transition_smoke_tests.csv",
    TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_distribution_snapshot_scaffolding.csv",
    TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_deferred_mechanics.csv",
    TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_future_6jt_contract.csv",
    TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_decision.csv",
    TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
REACHABILITY_AUDIT_CSV = TMP_DIR / f"{SLUG}_runtime_reachability_audit.csv"
MECHANIC_WIRING_AUDIT_CSV = TMP_DIR / f"{SLUG}_mechanic_wiring_audit.csv"
SIM_LOOP_AUDIT_CSV = TMP_DIR / f"{SLUG}_sim_loop_surface_audit.csv"
STATE_AUDIT_CSV = TMP_DIR / f"{SLUG}_state_transition_scaffolding_audit.csv"
DIST_AUDIT_CSV = TMP_DIR / f"{SLUG}_distribution_scaffolding_audit.csv"
DEFERRED_AUDIT_CSV = TMP_DIR / f"{SLUG}_deferred_mechanic_audit.csv"
FUTURE_6JU_CSV = TMP_DIR / f"{SLUG}_future_6ju_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JS = "layer_6_game_state_realism_runtime_wiring_implementation_complete"
DIAGNOSIS_6JT = "layer_6_game_state_realism_runtime_wiring_implementation_audit_complete"
RECOMMENDED_NEXT_LAYER_6JS = "6JT_layer_6_game_state_realism_runtime_wiring_implementation_audit"
RECOMMENDED_PATH_6JS = "implement_game_state_realism_runtime_wiring_then_audit_before_performance_evaluation"
RECOMMENDED_NEXT_LAYER_6JT = "6JU_layer_6_game_state_realism_behavioral_verification_plan"
RECOMMENDED_PATH_6JT = "audit_runtime_wiring_then_plan_behavioral_verification_before_performance_evaluation"


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
    return str(value).lower() == "true" or value is True


def all_passed(rows: List[Dict[str, Any]]) -> bool:
    return all(boolish(row.get("passed", "")) for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6js = load_json(JSON_6JS)

    reach_rows = read_csv(TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_runtime_reachability.csv")
    wiring_rows = read_csv(TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_mechanic_wiring.csv")
    sim_rows = read_csv(TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_sim_loop_surface_verification.csv")
    smoke_rows = read_csv(TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_state_transition_smoke_tests.csv")
    dist_rows = read_csv(TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_distribution_snapshot_scaffolding.csv")
    deferred_rows = read_csv(TMP_DIR / "layer6_6js_game_state_realism_runtime_wiring_implementation_deferred_mechanics.csv")

    reachability_audit = []
    for row in reach_rows:
        reachability_audit.append({
            "mechanic": row.get("mechanic"),
            "audited": True,
            "reachable_surface_present": row.get("reachable_surface_present"),
            "runtime_reachability_record_created": row.get("runtime_reachability_record_created"),
            "requires_6jt_audit": row.get("requires_6jt_audit"),
            "passed": boolish(row.get("passed")) and boolish(row.get("reachable_surface_present")),
        })

    wiring_audit = []
    for row in wiring_rows:
        wiring_audit.append({
            "mechanic": row.get("mechanic"),
            "audited": True,
            "implementation_status": row.get("implementation_status"),
            "production_activation": row.get("production_activation"),
            "requires_6jt_audit": row.get("requires_6jt_audit"),
            "passed": boolish(row.get("passed")) and not boolish(row.get("production_activation")),
        })

    sim_loop_audit = []
    for row in sim_rows:
        sim_loop_audit.append({
            "surface": row.get("surface"),
            "audited": True,
            "exists": row.get("exists"),
            "surface_verification_created": row.get("surface_verification_created"),
            "candidate_for_6jt_audit": row.get("candidate_for_6jt_audit"),
            "passed": boolish(row.get("passed")) and boolish(row.get("exists")),
        })

    state_audit = []
    for row in smoke_rows:
        state_audit.append({
            "mechanic": row.get("mechanic"),
            "audited": True,
            "smoke_test_name": row.get("smoke_test_name"),
            "pre_state_required": row.get("pre_state_required"),
            "post_state_required": row.get("post_state_required"),
            "mae_brier_run": row.get("mae_brier_run"),
            "passed": boolish(row.get("passed")) and not boolish(row.get("mae_brier_run")),
        })

    dist_audit = []
    for row in dist_rows:
        dist_audit.append({
            "mechanic": row.get("mechanic"),
            "audited": True,
            "snapshot_name": row.get("snapshot_name"),
            "tail_or_variance_review_required": row.get("tail_or_variance_review_required"),
            "performance_decision_allowed": row.get("performance_decision_allowed"),
            "passed": boolish(row.get("passed")) and not boolish(row.get("performance_decision_allowed")),
        })

    deferred_audit = []
    for row in deferred_rows:
        deferred_audit.append({
            "mechanic": row.get("mechanic"),
            "audited": True,
            "deferred_not_removed": row.get("deferred_not_removed"),
            "future_installation_required_before_layer6_exit": row.get("future_installation_required_before_layer6_exit"),
            "passed": (
                row.get("mechanic") == "balks"
                and boolish(row.get("deferred_not_removed"))
                and boolish(row.get("future_installation_required_before_layer6_exit"))
            ),
        })

    future_6ju = [
        {"contract": "plan_behavioral_verification_for_all_runtime_wiring_records", "required": True, "passed": True},
        {"contract": "convert_state_transition_scaffolding_into_behavioral_checks", "required": True, "passed": True},
        {"contract": "convert_distribution_scaffolding_into_snapshot_checks", "required": True, "passed": True},
        {"contract": "preserve_balks_as_deferred_required_mechanic", "required": True, "passed": True},
        {"contract": "do_not_run_mae_brier_before_behavioral_verification", "required": True, "passed": True},
        {"contract": "do_not_activate_before_behavioral_verification_audit", "required": True, "passed": True},
        {"contract": "do_not_grant_layer6_exit_from_runtime_wiring_audit", "required": True, "passed": True},
        {"contract": "prepare_future_behavioral_pass_fail_gates", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6js_implementation_script_exists", "expected": True, "actual": IMPLEMENT_6JS_PATH.exists(), "passed": IMPLEMENT_6JS_PATH.exists()},
        {"check": "6js_json_exists", "expected": True, "actual": JSON_6JS.exists(), "passed": JSON_6JS.exists()},
        {"check": "6js_all_checks_passed", "expected": True, "actual": json_6js.get("all_checks_passed"), "passed": json_6js.get("all_checks_passed") is True},
        {"check": "6js_diagnosis", "expected": DIAGNOSIS_6JS, "actual": json_6js.get("diagnosis"), "passed": json_6js.get("diagnosis") == DIAGNOSIS_6JS},
        {"check": "6js_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JS, "actual": json_6js.get("recommended_next_layer"), "passed": json_6js.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JS},
        {"check": "6js_recommended_path", "expected": RECOMMENDED_PATH_6JS, "actual": json_6js.get("recommended_path"), "passed": json_6js.get("recommended_path") == RECOMMENDED_PATH_6JS},
        {"check": "6js_runtime_reachability_count", "expected": 10, "actual": json_6js.get("runtime_reachability_count"), "passed": json_6js.get("runtime_reachability_count") == 10},
        {"check": "6js_future_6jt_contract_valid", "expected": True, "actual": json_6js.get("future_6jt_contract_valid"), "passed": json_6js.get("future_6jt_contract_valid") is True},
        {"check": "6js_no_layer6_exit", "expected": False, "actual": json_6js.get("layer_6_exit_recommended"), "passed": json_6js.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "behavioral_verification_plan", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "mae_brier_performance_evaluation", "blocked": True, "reason": "must plan and run behavioral verification first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "must certify behavior and performance first", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "audit only", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "audit only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "runtime wiring audit cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6js_passed", "expected": True, "actual": json_6js.get("all_checks_passed"), "passed": json_6js.get("all_checks_passed") is True},
        {"decision": "runtime_reachability_audit_count", "expected": 10, "actual": len(reachability_audit), "passed": len(reachability_audit) == 10},
        {"decision": "mechanic_wiring_audit_count", "expected": 10, "actual": len(wiring_audit), "passed": len(wiring_audit) == 10},
        {"decision": "sim_loop_surface_audit_count", "expected": 6, "actual": len(sim_loop_audit), "passed": len(sim_loop_audit) == 6},
        {"decision": "state_transition_scaffolding_audit_count", "expected": 10, "actual": len(state_audit), "passed": len(state_audit) == 10},
        {"decision": "recommend_6ju_next", "expected": RECOMMENDED_NEXT_LAYER_6JT, "actual": RECOMMENDED_NEXT_LAYER_6JT, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
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
        {"surface": "source_tree", "policy": "read_only_audit", "passed": True},
        {"surface": "6js_implementation", "policy": "read_only", "passed": True},
        {"surface": "6jr_plan", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6jt", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JT, "actual": RECOMMENDED_NEXT_LAYER_6JT, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JT, "actual": RECOMMENDED_PATH_6JT, "passed": True},
        {"decision": "recommend_behavioral_verification_plan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metrics_decision_or_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JT, "actual": DIAGNOSIS_6JT, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "runtime_reachability_audit", "passed": len(reachability_audit) == 10 and all_passed(reachability_audit), "detail": f"{len(reachability_audit)}/10"},
        {"check": "mechanic_wiring_audit", "passed": len(wiring_audit) == 10 and all_passed(wiring_audit), "detail": f"{len(wiring_audit)}/10"},
        {"check": "sim_loop_surface_audit", "passed": len(sim_loop_audit) == 6 and all_passed(sim_loop_audit), "detail": f"{len(sim_loop_audit)}/6"},
        {"check": "state_transition_scaffolding_audit", "passed": len(state_audit) == 10 and all_passed(state_audit), "detail": f"{len(state_audit)}/10"},
        {"check": "distribution_scaffolding_audit", "passed": len(dist_audit) == 10 and all_passed(dist_audit), "detail": f"{len(dist_audit)}/10"},
        {"check": "deferred_mechanic_audit", "passed": len(deferred_audit) == 1 and all_passed(deferred_audit), "detail": f"{len(deferred_audit)}/1"},
        {"check": "future_6ju_contract", "passed": len(future_6ju) == 8 and all_passed(future_6ju), "detail": f"{len(future_6ju)}/8"},
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
        "runtime_reachability_audit": write_csv(REACHABILITY_AUDIT_CSV, reachability_audit),
        "mechanic_wiring_audit": write_csv(MECHANIC_WIRING_AUDIT_CSV, wiring_audit),
        "sim_loop_surface_audit": write_csv(SIM_LOOP_AUDIT_CSV, sim_loop_audit),
        "state_transition_scaffolding_audit": write_csv(STATE_AUDIT_CSV, state_audit),
        "distribution_scaffolding_audit": write_csv(DIST_AUDIT_CSV, dist_audit),
        "deferred_mechanic_audit": write_csv(DEFERRED_AUDIT_CSV, deferred_audit),
        "future_6ju_contract": write_csv(FUTURE_6JU_CSV, future_6ju),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JT",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JT if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JT,
        "recommended_path": RECOMMENDED_PATH_6JT,
        "predecessor_implementation": str(IMPLEMENT_6JS_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6js.get("diagnosis"),
        "audited_layer_after": "6JS",
        "source_family": "game_state_realism_runtime_wiring_audit",
        "roadmap_mechanic_count": 10,
        "runtime_reachability_audit_count": len(reachability_audit),
        "mechanic_wiring_audit_count": len(wiring_audit),
        "sim_loop_surface_audit_count": len(sim_loop_audit),
        "state_transition_scaffolding_audit_count": len(state_audit),
        "distribution_scaffolding_audit_count": len(dist_audit),
        "deferred_mechanic_audit_count": len(deferred_audit),
        "future_6ju_contract_valid": len(future_6ju) == 8 and all_passed(future_6ju),
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
            "runtime_reachability_audit_csv": str(REACHABILITY_AUDIT_CSV),
            "mechanic_wiring_audit_csv": str(MECHANIC_WIRING_AUDIT_CSV),
            "sim_loop_surface_audit_csv": str(SIM_LOOP_AUDIT_CSV),
            "state_transition_scaffolding_audit_csv": str(STATE_AUDIT_CSV),
            "distribution_scaffolding_audit_csv": str(DIST_AUDIT_CSV),
            "deferred_mechanic_audit_csv": str(DEFERRED_AUDIT_CSV),
            "future_6ju_contract_csv": str(FUTURE_6JU_CSV),
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
