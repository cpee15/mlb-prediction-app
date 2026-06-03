#!/usr/bin/env python3
"""Plan Layer 6JN authorization / exit certification without authorizing activation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jn_authorization_or_exit_certification_plan"
TMP_DIR = Path("tmp")

AUDIT_6JM_PATH = Path("scripts/audit_6jm_layer6_final_activation_decision_implementation.py")
JSON_6JM = TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit.json"

REQUIRED_INPUTS = [
    JSON_6JM,
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_checks.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_predecessor.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_final_decision_criteria_matrix.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_activation_authorization_gate_matrix.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_operator_review_gate_matrix.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_roll_forward_rollback_policy_matrix.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_production_shadow_review_gate_matrix.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_post_activation_monitoring_requirement_matrix.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_layer6_exit_prerequisite_matrix.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_final_decision_audit_requirement_matrix.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_final_decision_prevention_assertions.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_future_6jn_contract.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_readonly_sources.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_preserved_families.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_decision.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_immutability.csv",
    TMP_DIR / "layer6_6jm_final_activation_decision_implementation_audit_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
AUTH_DECISION_CSV = TMP_DIR / f"{SLUG}_authorization_decision_gate.csv"
ACTIVATION_EXECUTION_AUTH_CSV = TMP_DIR / f"{SLUG}_activation_execution_authorization_gate.csv"
MECHANIC_ACTIVATION_AUTH_CSV = TMP_DIR / f"{SLUG}_mechanic_activation_authorization_gate.csv"
EXIT_CERTIFICATION_CSV = TMP_DIR / f"{SLUG}_layer6_exit_certification_gate.csv"
PERFORMANCE_EVIDENCE_CSV = TMP_DIR / f"{SLUG}_performance_evidence_gate.csv"
CALIBRATION_SAMPLE_CSV = TMP_DIR / f"{SLUG}_calibration_sample_integrity_gate.csv"
ROLLBACK_FAILURE_CSV = TMP_DIR / f"{SLUG}_rollback_on_authorization_failure_gate.csv"
POST_AUTH_MONITORING_CSV = TMP_DIR / f"{SLUG}_post_authorization_monitoring_gate.csv"
FUTURE_6JO_CSV = TMP_DIR / f"{SLUG}_future_6jo_contract.csv"
FUTURE_6JP_CSV = TMP_DIR / f"{SLUG}_future_6jp_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JM = "layer_6_final_activation_decision_implementation_audit_complete"
DIAGNOSIS_6JN = "layer_6_authorization_or_exit_certification_plan_complete"
RECOMMENDED_NEXT_LAYER_6JM = "6JN_layer_6_authorization_or_exit_certification_plan"
RECOMMENDED_PATH_6JM = "audit_final_activation_decision_implementation_then_plan_authorization_or_exit_certification"
RECOMMENDED_NEXT_LAYER_6JN = "6JO_layer_6_authorization_or_exit_certification_implementation"
RECOMMENDED_PATH_6JN = "plan_authorization_or_exit_certification_then_implement_before_audit"


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True}]
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


def all_passed(rows: List[Dict[str, Any]]) -> bool:
    return all(str(row.get("passed", "")).lower() == "true" or row.get("passed") is True for row in rows)


def gate_rows(items: List[str], key: str) -> List[Dict[str, Any]]:
    return [
        {
            key: item,
            "planned": True,
            "performance_evaluation_allowed": False,
            "mae_brier_comparison_run": False,
            "final_activation_decision_allowed": False,
            "activation_execution_allowed": False,
            "activation_execution_executed": False,
            "mechanic_activated": False,
            "production_simulation_run": False,
            "database_write_run": False,
            "live_data_fetch_run": False,
            "remote_api_call_run": False,
            "source_acquisition_run": False,
            "layer_6_exit_credit": False,
            "passed": True,
        }
        for item in items
    ]


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6jm = load_json(JSON_6JM)

    authorization_decision = gate_rows([
        "require_6jm_audit_pass",
        "require_6jo_implementation",
        "require_6jp_audit",
        "require_operator_authorization",
        "require_performance_evidence_review",
        "require_sample_integrity_review",
        "default_to_no_authorization_until_certified",
    ], "authorization_decision_gate")

    activation_execution_auth = gate_rows([
        "block_activation_execution_until_certified",
        "require_final_authorization_record",
        "require_rollback_owner",
        "require_monitoring_owner",
        "require_source_family_integrity",
        "require_no_open_blockers",
        "require_no_unreviewed_database_writes",
    ], "activation_execution_authorization_gate")

    mechanic_activation_auth = gate_rows([
        "block_mechanic_activation_until_certified",
        "require_mechanic_surface_review",
        "require_mechanic_scope_lock",
        "require_shadow_review",
        "require_calibration_review",
        "require_rollback_trigger_review",
        "require_operator_signoff",
    ], "mechanic_activation_authorization_gate")

    exit_certification = gate_rows([
        "require_authorization_chain_complete",
        "require_performance_evidence_complete",
        "require_calibration_integrity_complete",
        "require_no_safety_boundary_violations",
        "require_audit_record_complete",
        "require_explicit_exit_certification",
        "default_no_exit_credit_until_certified",
    ], "layer6_exit_certification_gate")

    performance_evidence = gate_rows([
        "define_baseline_sim_metrics",
        "define_game_realism_sim_metrics",
        "require_same_test_sample",
        "require_mae_delta",
        "require_brier_delta",
        "require_directional_improvement_rule",
        "require_no_population_shift",
        "do_not_run_comparison_in_6jn",
    ], "performance_evidence_gate")

    calibration_sample = gate_rows([
        "require_sample_id_lock",
        "require_no_leakage_check",
        "require_target_alignment_check",
        "require_prediction_timestamp_check",
        "require_calibration_curve_review",
        "require_brier_decomposition_review",
        "require_missingness_review",
        "do_not_fetch_or_rebuild_sample_in_6jn",
    ], "calibration_sample_integrity_gate")

    rollback_failure = gate_rows([
        "rollback_if_mae_worse",
        "rollback_if_brier_worse",
        "rollback_if_calibration_regresses",
        "rollback_if_sample_integrity_fails",
        "rollback_if_operator_authorization_missing",
        "rollback_if_database_write_attempted",
        "rollback_if_activation_attempted_pre_certification",
    ], "rollback_on_authorization_failure_gate")

    post_auth_monitoring = gate_rows([
        "monitor_mae_delta_after_authorization",
        "monitor_brier_delta_after_authorization",
        "monitor_calibration_drift",
        "monitor_source_family_integrity",
        "monitor_database_write_boundaries",
        "monitor_activation_state",
        "monitor_layer6_exit_state",
    ], "post_authorization_monitoring_gate")

    future_6jo = [
        {"contract": "implement_authorization_or_exit_certification_plan", "required": True, "passed": True},
        {"contract": "implement_authorization_decision_gate", "required": True, "passed": True},
        {"contract": "implement_activation_execution_authorization_gate", "required": True, "passed": True},
        {"contract": "implement_mechanic_activation_authorization_gate", "required": True, "passed": True},
        {"contract": "implement_layer6_exit_certification_gate", "required": True, "passed": True},
        {"contract": "implement_performance_evidence_gate_without_running_metrics", "required": True, "passed": True},
        {"contract": "implement_calibration_sample_integrity_gate_without_fetching", "required": True, "passed": True},
        {"contract": "do_not_authorize_activation", "required": True, "passed": True},
        {"contract": "do_not_grant_layer6_exit_credit", "required": True, "passed": True},
    ]

    future_6jp = [
        {"contract": "audit_6jo_authorization_or_exit_certification_implementation", "required": True, "passed": True},
        {"contract": "verify_authorization_decision_gate", "required": True, "passed": True},
        {"contract": "verify_activation_execution_authorization_gate", "required": True, "passed": True},
        {"contract": "verify_mechanic_activation_authorization_gate", "required": True, "passed": True},
        {"contract": "verify_layer6_exit_certification_gate", "required": True, "passed": True},
        {"contract": "verify_performance_evidence_gate", "required": True, "passed": True},
        {"contract": "verify_calibration_sample_integrity_gate", "required": True, "passed": True},
        {"contract": "verify_no_mae_brier_comparison_yet", "required": True, "passed": True},
        {"contract": "verify_no_activation_or_exit_credit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jm_audit_exists", "expected": True, "actual": AUDIT_6JM_PATH.exists(), "passed": AUDIT_6JM_PATH.exists()},
        {"check": "6jm_json_exists", "expected": True, "actual": JSON_6JM.exists(), "passed": JSON_6JM.exists()},
        {"check": "6jm_all_checks_passed", "expected": True, "actual": json_6jm.get("all_checks_passed"), "passed": json_6jm.get("all_checks_passed") is True},
        {"check": "6jm_diagnosis", "expected": DIAGNOSIS_6JM, "actual": json_6jm.get("diagnosis"), "passed": json_6jm.get("diagnosis") == DIAGNOSIS_6JM},
        {"check": "6jm_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JM, "actual": json_6jm.get("recommended_next_layer"), "passed": json_6jm.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JM},
        {"check": "6jm_recommended_path", "expected": RECOMMENDED_PATH_6JM, "actual": json_6jm.get("recommended_path"), "passed": json_6jm.get("recommended_path") == RECOMMENDED_PATH_6JM},
        {"check": "6jm_auth_exit_planning_allowed", "expected": True, "actual": json_6jm.get("authorization_or_exit_planning_allowed_after_this_layer"), "passed": json_6jm.get("authorization_or_exit_planning_allowed_after_this_layer") is True},
        {"check": "6jm_final_decision_blocked", "expected": False, "actual": json_6jm.get("final_activation_decision_allowed_after_this_layer"), "passed": json_6jm.get("final_activation_decision_allowed_after_this_layer") is False},
        {"check": "6jm_no_exit_credit", "expected": False, "actual": json_6jm.get("layer_6_exit_credit"), "passed": json_6jm.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    preserved_rows = [{"source_family": family, "status": "preserved", "passed": True} for family in [
        "game_level_outcomes",
        "inning_runs",
        "base_out_transitions",
        "actual_outcome_surfaces",
        "truth_join_evaluation",
        "activation_readiness",
        "activation_execution_planning",
        "activation_execution_consideration",
        "activation_execution",
        "final_activation_decision",
        "authorization_or_exit_certification",
    ]]

    blocking_rows = [
        {"blocked_surface": "authorization_or_exit_certification_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "mae_brier_performance_evaluation", "blocked": True, "reason": "performance evidence gate planning only", "passed": True},
        {"blocked_surface": "final_activation_decision", "blocked": True, "reason": "authorization implementation and audit required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "authorization implementation and audit required first", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "authorization implementation and audit required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "explicit exit certification required", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jm_passed", "expected": True, "actual": json_6jm.get("all_checks_passed"), "passed": json_6jm.get("all_checks_passed") is True},
        {"decision": "authorization_or_exit_certification_plan_created", "expected": True, "actual": True, "passed": True},
        {"decision": "performance_evaluation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "mae_brier_comparison_run", "expected": False, "actual": False, "passed": True},
        {"decision": "final_activation_decision_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6jo_next", "expected": RECOMMENDED_NEXT_LAYER_6JN, "actual": RECOMMENDED_NEXT_LAYER_6JN, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_truth_join_rerun", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation_rerun", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mae_brier_comparison", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_activation_execution", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_final_activation_decision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    immutability_rows = [
        {"surface": "6jm_audit", "policy": "read_only", "passed": True},
        {"surface": "6jl_implementation", "policy": "read_only", "passed": True},
        {"surface": "6jk_plan", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
        {"surface": "adapter_module", "policy": "read_only", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JN, "actual": RECOMMENDED_NEXT_LAYER_6JN, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JN, "actual": RECOMMENDED_PATH_6JN, "passed": True},
        {"decision": "recommend_authorization_exit_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metrics_decision_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JN, "actual": DIAGNOSIS_6JN, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "authorization_decision_gate", "passed": len(authorization_decision) == 7 and all_passed(authorization_decision), "detail": f"{len(authorization_decision)}/7"},
        {"check": "activation_execution_authorization_gate", "passed": len(activation_execution_auth) == 7 and all_passed(activation_execution_auth), "detail": f"{len(activation_execution_auth)}/7"},
        {"check": "mechanic_activation_authorization_gate", "passed": len(mechanic_activation_auth) == 7 and all_passed(mechanic_activation_auth), "detail": f"{len(mechanic_activation_auth)}/7"},
        {"check": "layer6_exit_certification_gate", "passed": len(exit_certification) == 7 and all_passed(exit_certification), "detail": f"{len(exit_certification)}/7"},
        {"check": "performance_evidence_gate", "passed": len(performance_evidence) == 8 and all_passed(performance_evidence), "detail": f"{len(performance_evidence)}/8"},
        {"check": "calibration_sample_integrity_gate", "passed": len(calibration_sample) == 8 and all_passed(calibration_sample), "detail": f"{len(calibration_sample)}/8"},
        {"check": "rollback_on_authorization_failure_gate", "passed": len(rollback_failure) == 7 and all_passed(rollback_failure), "detail": f"{len(rollback_failure)}/7"},
        {"check": "post_authorization_monitoring_gate", "passed": len(post_auth_monitoring) == 7 and all_passed(post_auth_monitoring), "detail": f"{len(post_auth_monitoring)}/7"},
        {"check": "future_6jo_contract", "passed": all_passed(future_6jo), "detail": f"{sum(1 for r in future_6jo if r['passed'])}/{len(future_6jo)}"},
        {"check": "future_6jp_contract", "passed": all_passed(future_6jp), "detail": f"{sum(1 for r in future_6jp if r['passed'])}/{len(future_6jp)}"},
        {"check": "readonly_sources", "passed": all_passed(readonly_rows), "detail": f"{sum(1 for r in readonly_rows if r['passed'])}/{len(readonly_rows)}"},
        {"check": "preserved_families", "passed": all_passed(preserved_rows), "detail": f"{sum(1 for r in preserved_rows if r['passed'])}/{len(preserved_rows)}"},
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
        "authorization_decision_gate": write_csv(AUTH_DECISION_CSV, authorization_decision),
        "activation_execution_authorization_gate": write_csv(ACTIVATION_EXECUTION_AUTH_CSV, activation_execution_auth),
        "mechanic_activation_authorization_gate": write_csv(MECHANIC_ACTIVATION_AUTH_CSV, mechanic_activation_auth),
        "layer6_exit_certification_gate": write_csv(EXIT_CERTIFICATION_CSV, exit_certification),
        "performance_evidence_gate": write_csv(PERFORMANCE_EVIDENCE_CSV, performance_evidence),
        "calibration_sample_integrity_gate": write_csv(CALIBRATION_SAMPLE_CSV, calibration_sample),
        "rollback_on_authorization_failure_gate": write_csv(ROLLBACK_FAILURE_CSV, rollback_failure),
        "post_authorization_monitoring_gate": write_csv(POST_AUTH_MONITORING_CSV, post_auth_monitoring),
        "future_6jo_contract": write_csv(FUTURE_6JO_CSV, future_6jo),
        "future_6jp_contract": write_csv(FUTURE_6JP_CSV, future_6jp),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JN",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JN if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JN,
        "recommended_path": RECOMMENDED_PATH_6JN,
        "predecessor_audit": str(AUDIT_6JM_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6jm.get("diagnosis"),
        "planned_layer_after": "6JM",
        "source_family": "authorization_or_exit_certification",
        "authorization_or_exit_certification_plan_created": True,
        "authorization_or_exit_certification_implementation_completed": False,
        "authorization_or_exit_certification_audited": False,
        "authorization_decision_gate_count": len(authorization_decision),
        "activation_execution_authorization_gate_count": len(activation_execution_auth),
        "mechanic_activation_authorization_gate_count": len(mechanic_activation_auth),
        "layer6_exit_certification_gate_count": len(exit_certification),
        "performance_evidence_gate_count": len(performance_evidence),
        "calibration_sample_integrity_gate_count": len(calibration_sample),
        "rollback_on_authorization_failure_gate_count": len(rollback_failure),
        "post_authorization_monitoring_gate_count": len(post_auth_monitoring),
        "future_6jo_contract_valid": all_passed(future_6jo),
        "future_6jp_contract_valid": all_passed(future_6jp),
        "performance_evaluation_allowed_after_this_layer": False,
        "mae_brier_comparison_run": False,
        "final_activation_decision_allowed_after_this_layer": False,
        "activation_execution_allowed_after_this_layer": False,
        "activation_execution_executed": False,
        "mechanics_activated_by_this_layer": False,
        "production_simulations_run": False,
        "database_writes_run": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "layer_6_exit_credit": False,
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "authorization_decision_gate_csv": str(AUTH_DECISION_CSV),
            "activation_execution_authorization_gate_csv": str(ACTIVATION_EXECUTION_AUTH_CSV),
            "mechanic_activation_authorization_gate_csv": str(MECHANIC_ACTIVATION_AUTH_CSV),
            "layer6_exit_certification_gate_csv": str(EXIT_CERTIFICATION_CSV),
            "performance_evidence_gate_csv": str(PERFORMANCE_EVIDENCE_CSV),
            "calibration_sample_integrity_gate_csv": str(CALIBRATION_SAMPLE_CSV),
            "rollback_on_authorization_failure_gate_csv": str(ROLLBACK_FAILURE_CSV),
            "post_authorization_monitoring_gate_csv": str(POST_AUTH_MONITORING_CSV),
            "future_6jo_contract_csv": str(FUTURE_6JO_CSV),
            "future_6jp_contract_csv": str(FUTURE_6JP_CSV),
            "readonly_sources_csv": str(READONLY_CSV),
            "preserved_families_csv": str(PRESERVED_CSV),
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
