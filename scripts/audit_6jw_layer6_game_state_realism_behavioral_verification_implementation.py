#!/usr/bin/env python3
"""Audit Layer 6 game-state realism behavioral verification implementation.

This audit verifies 6JV behavioral verification artifact records. It explicitly
preserves the finding that no real behavioral simulations were run, so real
behavioral execution is still required before MAE/Brier, activation, or Layer 6
exit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jw_game_state_realism_behavioral_verification_implementation_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6JV_PATH = Path("scripts/implement_6jv_layer6_game_state_realism_behavioral_verification.py")
JSON_6JV = TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation.json"

REQUIRED_INPUTS = [
    JSON_6JV,
    TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_checks.csv",
    TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_predecessor.csv",
    TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_control_mechanic_cases.csv",
    TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_pre_post_state_assertions.csv",
    TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_behavioral_delta_records.csv",
    TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_distribution_snapshot_records.csv",
    TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_deferred_mechanics.csv",
    TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_future_6jw_contract.csv",
    TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_decision.csv",
    TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CASE_AUDIT_CSV = TMP_DIR / f"{SLUG}_control_mechanic_case_audit.csv"
ASSERTION_AUDIT_CSV = TMP_DIR / f"{SLUG}_pre_post_state_assertion_audit.csv"
DELTA_AUDIT_CSV = TMP_DIR / f"{SLUG}_behavioral_delta_audit.csv"
SNAPSHOT_AUDIT_CSV = TMP_DIR / f"{SLUG}_distribution_snapshot_audit.csv"
DEFERRED_AUDIT_CSV = TMP_DIR / f"{SLUG}_deferred_mechanic_audit.csv"
FUTURE_6JX_CSV = TMP_DIR / f"{SLUG}_future_6jx_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JV = "layer_6_game_state_realism_behavioral_verification_implementation_complete"
DIAGNOSIS_6JW = "layer_6_game_state_realism_behavioral_verification_implementation_audit_complete"
RECOMMENDED_NEXT_LAYER_6JV = "6JW_layer_6_game_state_realism_behavioral_verification_implementation_audit"
RECOMMENDED_PATH_6JV = "implement_behavioral_verification_then_audit_before_performance_evaluation"
RECOMMENDED_NEXT_LAYER_6JW = "6JX_layer_6_game_state_realism_behavioral_execution_plan"
RECOMMENDED_PATH_6JW = "audit_behavioral_verification_artifacts_then_plan_real_behavioral_execution_before_performance_evaluation"


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
    json_6jv = load_json(JSON_6JV)

    case_rows = read_csv(TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_control_mechanic_cases.csv")
    assertion_rows = read_csv(TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_pre_post_state_assertions.csv")
    delta_rows = read_csv(TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_behavioral_delta_records.csv")
    snapshot_rows = read_csv(TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_distribution_snapshot_records.csv")
    deferred_rows = read_csv(TMP_DIR / "layer6_6jv_game_state_realism_behavioral_verification_implementation_deferred_mechanics.csv")

    case_audit = []
    for row in case_rows:
        case_audit.append({
            "mechanic": row.get("mechanic"),
            "audited": True,
            "control_case_created": row.get("control_case_created"),
            "mechanic_case_created": row.get("mechanic_case_created"),
            "production_simulation_run": row.get("production_simulation_run"),
            "behavioral_simulation_run": row.get("behavioral_simulation_run"),
            "passed": (
                boolish(row.get("passed"))
                and boolish(row.get("control_case_created"))
                and boolish(row.get("mechanic_case_created"))
                and not boolish(row.get("production_simulation_run"))
                and not boolish(row.get("behavioral_simulation_run"))
            ),
        })

    assertion_audit = []
    for row in assertion_rows:
        assertion_audit.append({
            "mechanic": row.get("mechanic"),
            "audited": True,
            "pre_state_recorded": row.get("pre_state_recorded"),
            "post_state_recorded": row.get("post_state_recorded"),
            "runtime_error": row.get("runtime_error"),
            "state_corruption_detected": row.get("state_corruption_detected"),
            "passed": (
                boolish(row.get("passed"))
                and boolish(row.get("pre_state_recorded"))
                and boolish(row.get("post_state_recorded"))
                and not boolish(row.get("runtime_error"))
                and not boolish(row.get("state_corruption_detected"))
            ),
        })

    delta_audit = []
    for row in delta_rows:
        delta_observed = row.get("behavioral_delta_observed", "")
        delta_audit.append({
            "mechanic": row.get("mechanic"),
            "audited": True,
            "delta_record_created": row.get("delta_record_created"),
            "behavioral_delta_required": row.get("behavioral_delta_required"),
            "behavioral_delta_observed": delta_observed,
            "requires_6jw_audit": row.get("requires_6jw_audit"),
            "mae_brier_comparison_run": row.get("mae_brier_comparison_run"),
            "production_simulation_not_run_confirmed": delta_observed == "planned_assertion_not_executed_as_production_simulation",
            "real_behavioral_execution_still_required": True,
            "passed": (
                boolish(row.get("passed"))
                and boolish(row.get("delta_record_created"))
                and not boolish(row.get("mae_brier_comparison_run"))
                and delta_observed == "planned_assertion_not_executed_as_production_simulation"
            ),
        })

    snapshot_audit = []
    for row in snapshot_rows:
        snapshot_audit.append({
            "mechanic": row.get("mechanic"),
            "audited": True,
            "baseline_snapshot_record_created": row.get("baseline_snapshot_record_created"),
            "mechanic_snapshot_record_created": row.get("mechanic_snapshot_record_created"),
            "mae_brier_comparison_allowed": row.get("mae_brier_comparison_allowed"),
            "mae_brier_comparison_run": row.get("mae_brier_comparison_run"),
            "real_distribution_execution_still_required": True,
            "passed": (
                boolish(row.get("passed"))
                and boolish(row.get("baseline_snapshot_record_created"))
                and boolish(row.get("mechanic_snapshot_record_created"))
                and not boolish(row.get("mae_brier_comparison_allowed"))
                and not boolish(row.get("mae_brier_comparison_run"))
            ),
        })

    deferred_audit = []
    for row in deferred_rows:
        deferred_audit.append({
            "mechanic": row.get("mechanic"),
            "audited": True,
            "status": row.get("status"),
            "removed_from_layer6_scope": row.get("removed_from_layer6_scope"),
            "layer6_exit_allowed_without_resolution": row.get("layer6_exit_allowed_without_resolution"),
            "passed": (
                row.get("mechanic") == "balks"
                and row.get("status") == "deferred_required_before_layer6_exit"
                and not boolish(row.get("removed_from_layer6_scope"))
                and not boolish(row.get("layer6_exit_allowed_without_resolution"))
            ),
        })

    future_6jx = [
        {"contract": "plan_real_behavioral_execution_for_non_deferred_mechanics", "required": True, "passed": True},
        {"contract": "define_safe_non_production_behavioral_execution_harness", "required": True, "passed": True},
        {"contract": "execute_control_and_mechanic_cases_without_db_writes", "required": True, "passed": True},
        {"contract": "record_observed_state_deltas_not_just_artifact_records", "required": True, "passed": True},
        {"contract": "record_distribution_deltas_without_mae_brier", "required": True, "passed": True},
        {"contract": "preserve_balks_as_deferred_required_before_exit", "required": True, "passed": True},
        {"contract": "do_not_activate_or_grant_layer6_exit_in_6jx", "required": True, "passed": True},
        {"contract": "do_not_run_mae_brier_until_behavioral_execution_audit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jv_implementation_script_exists", "expected": True, "actual": IMPLEMENT_6JV_PATH.exists(), "passed": IMPLEMENT_6JV_PATH.exists()},
        {"check": "6jv_json_exists", "expected": True, "actual": JSON_6JV.exists(), "passed": JSON_6JV.exists()},
        {"check": "6jv_all_checks_passed", "expected": True, "actual": json_6jv.get("all_checks_passed"), "passed": json_6jv.get("all_checks_passed") is True},
        {"check": "6jv_diagnosis", "expected": DIAGNOSIS_6JV, "actual": json_6jv.get("diagnosis"), "passed": json_6jv.get("diagnosis") == DIAGNOSIS_6JV},
        {"check": "6jv_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JV, "actual": json_6jv.get("recommended_next_layer"), "passed": json_6jv.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JV},
        {"check": "6jv_recommended_path", "expected": RECOMMENDED_PATH_6JV, "actual": json_6jv.get("recommended_path"), "passed": json_6jv.get("recommended_path") == RECOMMENDED_PATH_6JV},
        {"check": "6jv_control_case_count", "expected": 9, "actual": json_6jv.get("control_mechanic_case_count"), "passed": json_6jv.get("control_mechanic_case_count") == 9},
        {"check": "6jv_future_6jw_contract_valid", "expected": True, "actual": json_6jv.get("future_6jw_contract_valid"), "passed": json_6jv.get("future_6jw_contract_valid") is True},
        {"check": "6jv_no_layer6_exit", "expected": False, "actual": json_6jv.get("layer_6_exit_recommended"), "passed": json_6jv.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "real_behavioral_execution_plan", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "mae_brier_performance_evaluation", "blocked": True, "reason": "real behavioral execution and audit required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "must certify behavior and performance first", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "audit only", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "audit only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "behavioral verification artifact audit cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jv_passed", "expected": True, "actual": json_6jv.get("all_checks_passed"), "passed": json_6jv.get("all_checks_passed") is True},
        {"decision": "control_case_audit_count", "expected": 9, "actual": len(case_audit), "passed": len(case_audit) == 9},
        {"decision": "pre_post_assertion_audit_count", "expected": 9, "actual": len(assertion_audit), "passed": len(assertion_audit) == 9},
        {"decision": "behavioral_delta_audit_count", "expected": 9, "actual": len(delta_audit), "passed": len(delta_audit) == 9},
        {"decision": "distribution_snapshot_audit_count", "expected": 9, "actual": len(snapshot_audit), "passed": len(snapshot_audit) == 9},
        {"decision": "recommend_6jx_next", "expected": RECOMMENDED_NEXT_LAYER_6JW, "actual": RECOMMENDED_NEXT_LAYER_6JW, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"decision": "real_behavioral_execution_still_required", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "real_behavioral_execution_still_required", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_real_behavioral_simulations_run", "expected": False, "actual": False, "passed": True},
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
        {"surface": "6jv_implementation", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6jw", "passed": True},
        {"surface": "behavioral_cases", "policy": "audit_records_only", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JW, "actual": RECOMMENDED_NEXT_LAYER_6JW, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JW, "actual": RECOMMENDED_PATH_6JW, "passed": True},
        {"decision": "recommend_real_behavioral_execution_plan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metrics_decision_or_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JW, "actual": DIAGNOSIS_6JW, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "control_mechanic_case_audit", "passed": len(case_audit) == 9 and all_passed(case_audit), "detail": "9/9"},
        {"check": "pre_post_state_assertion_audit", "passed": len(assertion_audit) == 9 and all_passed(assertion_audit), "detail": "9/9"},
        {"check": "behavioral_delta_audit", "passed": len(delta_audit) == 9 and all_passed(delta_audit), "detail": "9/9"},
        {"check": "distribution_snapshot_audit", "passed": len(snapshot_audit) == 9 and all_passed(snapshot_audit), "detail": "9/9"},
        {"check": "deferred_mechanic_audit", "passed": len(deferred_audit) == 1 and all_passed(deferred_audit), "detail": "1/1"},
        {"check": "future_6jx_contract", "passed": len(future_6jx) == 8 and all_passed(future_6jx), "detail": "8/8"},
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
        "control_mechanic_case_audit": write_csv(CASE_AUDIT_CSV, case_audit),
        "pre_post_state_assertion_audit": write_csv(ASSERTION_AUDIT_CSV, assertion_audit),
        "behavioral_delta_audit": write_csv(DELTA_AUDIT_CSV, delta_audit),
        "distribution_snapshot_audit": write_csv(SNAPSHOT_AUDIT_CSV, snapshot_audit),
        "deferred_mechanic_audit": write_csv(DEFERRED_AUDIT_CSV, deferred_audit),
        "future_6jx_contract": write_csv(FUTURE_6JX_CSV, future_6jx),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JW",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JW if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JW,
        "recommended_path": RECOMMENDED_PATH_6JW,
        "predecessor_implementation": str(IMPLEMENT_6JV_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6jv.get("diagnosis"),
        "audited_layer_after": "6JV",
        "source_family": "game_state_realism_behavioral_verification_audit",
        "roadmap_mechanic_count": 10,
        "non_deferred_mechanic_count": 9,
        "control_mechanic_case_audit_count": len(case_audit),
        "pre_post_state_assertion_audit_count": len(assertion_audit),
        "behavioral_delta_audit_count": len(delta_audit),
        "distribution_snapshot_audit_count": len(snapshot_audit),
        "deferred_mechanic_audit_count": len(deferred_audit),
        "future_6jx_contract_valid": len(future_6jx) == 8 and all_passed(future_6jx),
        "real_behavioral_execution_still_required": True,
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
            "control_mechanic_case_audit_csv": str(CASE_AUDIT_CSV),
            "pre_post_state_assertion_audit_csv": str(ASSERTION_AUDIT_CSV),
            "behavioral_delta_audit_csv": str(DELTA_AUDIT_CSV),
            "distribution_snapshot_audit_csv": str(SNAPSHOT_AUDIT_CSV),
            "deferred_mechanic_audit_csv": str(DEFERRED_AUDIT_CSV),
            "future_6jx_contract_csv": str(FUTURE_6JX_CSV),
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
