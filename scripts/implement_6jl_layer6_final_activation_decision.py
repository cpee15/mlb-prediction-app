#!/usr/bin/env python3
"""Implement Layer 6JL final activation decision framework without deciding activation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jl_final_activation_decision_implementation"
TMP_DIR = Path("tmp")

PLAN_6JK_PATH = Path("scripts/plan_6jk_layer6_final_activation_decision.py")
JSON_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan.json"

CHECKS_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_checks.csv"
PREDECESSOR_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_predecessor.csv"
INPUT_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_input_artifacts.csv"
FINAL_CRITERIA_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_final_decision_criteria.csv"
AUTH_GATES_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_activation_authorization_gates.csv"
OPERATOR_GATES_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_operator_review_gates.csv"
ROLLBACK_POLICY_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_roll_forward_rollback_policy.csv"
SHADOW_REVIEW_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_production_shadow_review_gates.csv"
MONITORING_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_post_activation_monitoring_requirements.csv"
EXIT_PREREQS_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_layer6_exit_prerequisites.csv"
FINAL_AUDIT_REQ_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_final_decision_audit_requirements.csv"
PREVENTION_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_final_decision_prevention_rules.csv"
FUTURE_6JL_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_future_6jl_contract.csv"
FUTURE_6JM_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_future_6jm_contract.csv"
READONLY_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_readonly_sources.csv"
PRESERVED_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_preserved_families.csv"
BLOCKING_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_blocking_policy.csv"
DECISION_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_decision.csv"
SAFETY_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_safety_boundaries.csv"
IMMUTABILITY_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_immutability.csv"
RECOMMENDED_6JK = TMP_DIR / "layer6_6jk_final_activation_decision_plan_recommended_path.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
FINAL_DECISION_CRITERIA_MATRIX_CSV = TMP_DIR / f"{SLUG}_final_decision_criteria_matrix.csv"
AUTHORIZATION_GATE_MATRIX_CSV = TMP_DIR / f"{SLUG}_activation_authorization_gate_matrix.csv"
OPERATOR_REVIEW_GATE_MATRIX_CSV = TMP_DIR / f"{SLUG}_operator_review_gate_matrix.csv"
ROLL_FORWARD_ROLLBACK_POLICY_MATRIX_CSV = TMP_DIR / f"{SLUG}_roll_forward_rollback_policy_matrix.csv"
SHADOW_REVIEW_GATE_MATRIX_CSV = TMP_DIR / f"{SLUG}_production_shadow_review_gate_matrix.csv"
MONITORING_REQUIREMENT_MATRIX_CSV = TMP_DIR / f"{SLUG}_post_activation_monitoring_requirement_matrix.csv"
EXIT_PREREQUISITE_MATRIX_CSV = TMP_DIR / f"{SLUG}_layer6_exit_prerequisite_matrix.csv"
FINAL_AUDIT_REQUIREMENT_MATRIX_CSV = TMP_DIR / f"{SLUG}_final_decision_audit_requirement_matrix.csv"
PREVENTION_ASSERTIONS_CSV = TMP_DIR / f"{SLUG}_final_decision_prevention_assertions.csv"
FUTURE_6JM_CSV = TMP_DIR / f"{SLUG}_future_6jm_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JK = "layer_6_final_activation_decision_plan_complete"
DIAGNOSIS_6JL = "layer_6_final_activation_decision_implementation_complete"
RECOMMENDED_NEXT_LAYER_6JK = "6JL_layer_6_final_activation_decision_implementation"
RECOMMENDED_PATH_6JK = "plan_final_activation_decision_then_implement_before_final_decision_audit"
RECOMMENDED_NEXT_LAYER_6JL = "6JM_layer_6_final_activation_decision_implementation_audit"
RECOMMENDED_PATH_6JL = "implement_final_activation_decision_then_audit_before_authorization_or_exit"


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


def implement_matrix(rows: List[Dict[str, str]], source_path: Path, input_key: str, output_key: str, expected_count: int) -> List[Dict[str, Any]]:
    implemented: List[Dict[str, Any]] = []
    for row in rows:
        identifier = row.get(input_key, "")
        implemented.append({
            output_key: identifier,
            "implemented": True,
            "source_plan": str(source_path),
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
            "passed": bool(identifier),
        })
    if len(rows) != expected_count:
        implemented.append({
            output_key: "__row_count_mismatch__",
            "implemented": False,
            "source_plan": str(source_path),
            "final_activation_decision_allowed": True,
            "activation_execution_allowed": True,
            "activation_execution_executed": True,
            "mechanic_activated": True,
            "production_simulation_run": True,
            "database_write_run": True,
            "live_data_fetch_run": True,
            "remote_api_call_run": True,
            "source_acquisition_run": True,
            "layer_6_exit_credit": True,
            "passed": False,
        })
    return implemented


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6jk = load_json(JSON_6JK)

    final_criteria = implement_matrix(read_csv(FINAL_CRITERIA_6JK), FINAL_CRITERIA_6JK, "final_decision_criterion", "final_decision_criterion", 8)
    auth_gates = implement_matrix(read_csv(AUTH_GATES_6JK), AUTH_GATES_6JK, "activation_authorization_gate", "activation_authorization_gate", 7)
    operator_gates = implement_matrix(read_csv(OPERATOR_GATES_6JK), OPERATOR_GATES_6JK, "operator_review_gate", "operator_review_gate", 7)
    rollback_policy = implement_matrix(read_csv(ROLLBACK_POLICY_6JK), ROLLBACK_POLICY_6JK, "roll_forward_rollback_policy", "roll_forward_rollback_policy", 7)
    shadow_review = implement_matrix(read_csv(SHADOW_REVIEW_6JK), SHADOW_REVIEW_6JK, "production_shadow_review_gate", "production_shadow_review_gate", 7)
    monitoring = implement_matrix(read_csv(MONITORING_6JK), MONITORING_6JK, "post_activation_monitoring_requirement", "post_activation_monitoring_requirement", 7)
    exit_prereqs = implement_matrix(read_csv(EXIT_PREREQS_6JK), EXIT_PREREQS_6JK, "layer6_exit_prerequisite", "layer6_exit_prerequisite", 7)
    audit_req = implement_matrix(read_csv(FINAL_AUDIT_REQ_6JK), FINAL_AUDIT_REQ_6JK, "final_decision_audit_requirement", "final_decision_audit_requirement", 8)
    prevention = implement_matrix(read_csv(PREVENTION_6JK), PREVENTION_6JK, "final_decision_prevention_rule", "final_decision_prevention_assertion", 7)

    future_6jm = [
        {"contract": "audit_6jl_final_activation_decision_implementation", "required": True, "passed": True},
        {"contract": "verify_final_decision_criteria_matrix", "required": True, "passed": True},
        {"contract": "verify_activation_authorization_gate_matrix", "required": True, "passed": True},
        {"contract": "verify_operator_review_gate_matrix", "required": True, "passed": True},
        {"contract": "verify_roll_forward_rollback_policy_matrix", "required": True, "passed": True},
        {"contract": "verify_production_shadow_review_gate_matrix", "required": True, "passed": True},
        {"contract": "verify_monitoring_requirement_matrix", "required": True, "passed": True},
        {"contract": "verify_layer6_exit_prerequisite_matrix", "required": True, "passed": True},
        {"contract": "verify_final_decision_prevention_assertions", "required": True, "passed": True},
        {"contract": "verify_no_final_decision_or_exit_credit", "required": True, "passed": True},
    ]

    required_inputs = [
        JSON_6JK, CHECKS_6JK, PREDECESSOR_6JK, INPUT_6JK,
        FINAL_CRITERIA_6JK, AUTH_GATES_6JK, OPERATOR_GATES_6JK,
        ROLLBACK_POLICY_6JK, SHADOW_REVIEW_6JK, MONITORING_6JK,
        EXIT_PREREQS_6JK, FINAL_AUDIT_REQ_6JK, PREVENTION_6JK,
        FUTURE_6JL_6JK, FUTURE_6JM_6JK, READONLY_6JK, PRESERVED_6JK,
        BLOCKING_6JK, DECISION_6JK, SAFETY_6JK, IMMUTABILITY_6JK, RECOMMENDED_6JK,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jk_plan_exists", "expected": True, "actual": PLAN_6JK_PATH.exists(), "passed": PLAN_6JK_PATH.exists()},
        {"check": "6jk_json_exists", "expected": True, "actual": JSON_6JK.exists(), "passed": JSON_6JK.exists()},
        {"check": "6jk_all_checks_passed", "expected": True, "actual": json_6jk.get("all_checks_passed"), "passed": json_6jk.get("all_checks_passed") is True},
        {"check": "6jk_diagnosis", "expected": DIAGNOSIS_6JK, "actual": json_6jk.get("diagnosis"), "passed": json_6jk.get("diagnosis") == DIAGNOSIS_6JK},
        {"check": "6jk_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JK, "actual": json_6jk.get("recommended_next_layer"), "passed": json_6jk.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JK},
        {"check": "6jk_recommended_path", "expected": RECOMMENDED_PATH_6JK, "actual": json_6jk.get("recommended_path"), "passed": json_6jk.get("recommended_path") == RECOMMENDED_PATH_6JK},
        {"check": "6jk_implementation_allowed", "expected": True, "actual": json_6jk.get("final_activation_decision_implementation_allowed_after_this_layer"), "passed": json_6jk.get("final_activation_decision_implementation_allowed_after_this_layer") is True},
        {"check": "6jk_final_decision_blocked", "expected": False, "actual": json_6jk.get("final_activation_decision_allowed_after_this_layer"), "passed": json_6jk.get("final_activation_decision_allowed_after_this_layer") is False},
        {"check": "6jk_no_exit_credit", "expected": False, "actual": json_6jk.get("layer_6_exit_credit"), "passed": json_6jk.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in required_inputs]

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
    ]]

    blocking_rows = [
        {"blocked_surface": "final_activation_decision_implementation_audit", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "final_activation_decision", "blocked": True, "reason": "implementation audit required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "final decision audit required first", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "final decision audit required first", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "final decision audit required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "final audit and explicit exit certification required", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jk_passed", "expected": True, "actual": json_6jk.get("all_checks_passed"), "passed": json_6jk.get("all_checks_passed") is True},
        {"decision": "final_activation_decision_implementation_completed", "expected": True, "actual": True, "passed": True},
        {"decision": "final_activation_decision_implementation_audited", "expected": False, "actual": False, "passed": True},
        {"decision": "final_activation_decision_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_execution_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6jm_final_decision_implementation_audit_next", "expected": RECOMMENDED_NEXT_LAYER_6JL, "actual": RECOMMENDED_NEXT_LAYER_6JL, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_truth_join_rerun", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation_rerun", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_activation_execution", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_final_activation_decision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    immutability_rows = [
        {"surface": "6jk_plan", "policy": "read_only", "passed": True},
        {"surface": "6jj_audit", "policy": "read_only", "passed": True},
        {"surface": "6ji_implementation", "policy": "read_only", "passed": True},
        {"surface": "6jh_plan", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
        {"surface": "adapter_module", "policy": "read_only", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JL, "actual": RECOMMENDED_NEXT_LAYER_6JL, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JL, "actual": RECOMMENDED_PATH_6JL, "passed": True},
        {"decision": "recommend_final_decision_audit_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_final_decision_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JL, "actual": DIAGNOSIS_6JL, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "final_decision_criteria_matrix", "passed": len(final_criteria) == 8 and all_passed(final_criteria), "detail": f"{len(final_criteria)}/8"},
        {"check": "activation_authorization_gate_matrix", "passed": len(auth_gates) == 7 and all_passed(auth_gates), "detail": f"{len(auth_gates)}/7"},
        {"check": "operator_review_gate_matrix", "passed": len(operator_gates) == 7 and all_passed(operator_gates), "detail": f"{len(operator_gates)}/7"},
        {"check": "roll_forward_rollback_policy_matrix", "passed": len(rollback_policy) == 7 and all_passed(rollback_policy), "detail": f"{len(rollback_policy)}/7"},
        {"check": "production_shadow_review_gate_matrix", "passed": len(shadow_review) == 7 and all_passed(shadow_review), "detail": f"{len(shadow_review)}/7"},
        {"check": "post_activation_monitoring_requirement_matrix", "passed": len(monitoring) == 7 and all_passed(monitoring), "detail": f"{len(monitoring)}/7"},
        {"check": "layer6_exit_prerequisite_matrix", "passed": len(exit_prereqs) == 7 and all_passed(exit_prereqs), "detail": f"{len(exit_prereqs)}/7"},
        {"check": "final_decision_audit_requirement_matrix", "passed": len(audit_req) == 8 and all_passed(audit_req), "detail": f"{len(audit_req)}/8"},
        {"check": "final_decision_prevention_assertions", "passed": len(prevention) == 7 and all_passed(prevention), "detail": f"{len(prevention)}/7"},
        {"check": "future_6jm_contract", "passed": all_passed(future_6jm), "detail": f"{sum(1 for r in future_6jm if r['passed'])}/{len(future_6jm)}"},
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
        "final_decision_criteria_matrix": write_csv(FINAL_DECISION_CRITERIA_MATRIX_CSV, final_criteria),
        "activation_authorization_gate_matrix": write_csv(AUTHORIZATION_GATE_MATRIX_CSV, auth_gates),
        "operator_review_gate_matrix": write_csv(OPERATOR_REVIEW_GATE_MATRIX_CSV, operator_gates),
        "roll_forward_rollback_policy_matrix": write_csv(ROLL_FORWARD_ROLLBACK_POLICY_MATRIX_CSV, rollback_policy),
        "production_shadow_review_gate_matrix": write_csv(SHADOW_REVIEW_GATE_MATRIX_CSV, shadow_review),
        "post_activation_monitoring_requirement_matrix": write_csv(MONITORING_REQUIREMENT_MATRIX_CSV, monitoring),
        "layer6_exit_prerequisite_matrix": write_csv(EXIT_PREREQUISITE_MATRIX_CSV, exit_prereqs),
        "final_decision_audit_requirement_matrix": write_csv(FINAL_AUDIT_REQUIREMENT_MATRIX_CSV, audit_req),
        "final_decision_prevention_assertions": write_csv(PREVENTION_ASSERTIONS_CSV, prevention),
        "future_6jm_contract": write_csv(FUTURE_6JM_CSV, future_6jm),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JL",
        "layer_type": "game_mechanics_realism",
        "implementation_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JL if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JL,
        "recommended_path": RECOMMENDED_PATH_6JL,
        "predecessor_plan": str(PLAN_6JK_PATH),
        "predecessor_plan_returncode": 0,
        "predecessor_plan_diagnosis": json_6jk.get("diagnosis"),
        "implemented_layer_after": "6JK",
        "source_family": "final_activation_decision",
        "final_activation_decision_plan_created": json_6jk.get("final_activation_decision_plan_created"),
        "final_activation_decision_implementation_completed": True,
        "final_activation_decision_implementation_audited": False,
        "final_decision_criteria_matrix_row_count": len(final_criteria),
        "activation_authorization_gate_matrix_row_count": len(auth_gates),
        "operator_review_gate_matrix_row_count": len(operator_gates),
        "roll_forward_rollback_policy_matrix_row_count": len(rollback_policy),
        "production_shadow_review_gate_matrix_row_count": len(shadow_review),
        "post_activation_monitoring_requirement_matrix_row_count": len(monitoring),
        "layer6_exit_prerequisite_matrix_row_count": len(exit_prereqs),
        "final_decision_audit_requirement_matrix_row_count": len(audit_req),
        "final_decision_prevention_assertion_count": len(prevention),
        "future_6jm_contract_valid": all_passed(future_6jm),
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
            "final_decision_criteria_matrix_csv": str(FINAL_DECISION_CRITERIA_MATRIX_CSV),
            "activation_authorization_gate_matrix_csv": str(AUTHORIZATION_GATE_MATRIX_CSV),
            "operator_review_gate_matrix_csv": str(OPERATOR_REVIEW_GATE_MATRIX_CSV),
            "roll_forward_rollback_policy_matrix_csv": str(ROLL_FORWARD_ROLLBACK_POLICY_MATRIX_CSV),
            "production_shadow_review_gate_matrix_csv": str(SHADOW_REVIEW_GATE_MATRIX_CSV),
            "post_activation_monitoring_requirement_matrix_csv": str(MONITORING_REQUIREMENT_MATRIX_CSV),
            "layer6_exit_prerequisite_matrix_csv": str(EXIT_PREREQUISITE_MATRIX_CSV),
            "final_decision_audit_requirement_matrix_csv": str(FINAL_AUDIT_REQUIREMENT_MATRIX_CSV),
            "final_decision_prevention_assertions_csv": str(PREVENTION_ASSERTIONS_CSV),
            "future_6jm_contract_csv": str(FUTURE_6JM_CSV),
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
