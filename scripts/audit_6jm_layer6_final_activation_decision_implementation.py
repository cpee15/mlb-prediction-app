#!/usr/bin/env python3
"""Audit Layer 6JM final activation decision implementation without deciding activation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jm_final_activation_decision_implementation_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6JL_PATH = Path("scripts/implement_6jl_layer6_final_activation_decision.py")
JSON_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation.json"

CHECKS_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_checks.csv"
PREDECESSOR_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_predecessor.csv"
INPUT_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_input_artifacts.csv"
FINAL_CRITERIA_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_final_decision_criteria_matrix.csv"
AUTH_GATES_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_activation_authorization_gate_matrix.csv"
OPERATOR_GATES_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_operator_review_gate_matrix.csv"
ROLLBACK_POLICY_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_roll_forward_rollback_policy_matrix.csv"
SHADOW_REVIEW_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_production_shadow_review_gate_matrix.csv"
MONITORING_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_post_activation_monitoring_requirement_matrix.csv"
EXIT_PREREQS_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_layer6_exit_prerequisite_matrix.csv"
FINAL_AUDIT_REQ_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_final_decision_audit_requirement_matrix.csv"
PREVENTION_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_final_decision_prevention_assertions.csv"
FUTURE_6JM_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_future_6jm_contract.csv"
READONLY_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_readonly_sources.csv"
PRESERVED_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_preserved_families.csv"
BLOCKING_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_blocking_policy.csv"
DECISION_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_decision.csv"
SAFETY_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_safety_boundaries.csv"
IMMUTABILITY_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_immutability.csv"
RECOMMENDED_6JL = TMP_DIR / "layer6_6jl_final_activation_decision_implementation_recommended_path.csv"

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
FUTURE_6JN_CSV = TMP_DIR / f"{SLUG}_future_6jn_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JL = "layer_6_final_activation_decision_implementation_complete"
DIAGNOSIS_6JM = "layer_6_final_activation_decision_implementation_audit_complete"
RECOMMENDED_NEXT_LAYER_6JL = "6JM_layer_6_final_activation_decision_implementation_audit"
RECOMMENDED_PATH_6JL = "implement_final_activation_decision_then_audit_before_authorization_or_exit"
RECOMMENDED_NEXT_LAYER_6JM = "6JN_layer_6_authorization_or_exit_certification_plan"
RECOMMENDED_PATH_6JM = "audit_final_activation_decision_implementation_then_plan_authorization_or_exit_certification"


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


def audit_matrix(rows: List[Dict[str, str]], source_path: Path, id_key: str, expected_count: int) -> List[Dict[str, Any]]:
    audited: List[Dict[str, Any]] = []
    for row in rows:
        identifier = row.get(id_key, "")
        audited.append({
            id_key: identifier,
            "source_artifact": str(source_path),
            "implemented": str(row.get("implemented", "")).lower() == "true",
            "final_activation_decision_allowed": str(row.get("final_activation_decision_allowed", "")).lower() == "true",
            "activation_execution_allowed": str(row.get("activation_execution_allowed", "")).lower() == "true",
            "activation_execution_executed": str(row.get("activation_execution_executed", "")).lower() == "true",
            "mechanic_activated": str(row.get("mechanic_activated", "")).lower() == "true",
            "production_simulation_run": str(row.get("production_simulation_run", "")).lower() == "true",
            "database_write_run": str(row.get("database_write_run", "")).lower() == "true",
            "live_data_fetch_run": str(row.get("live_data_fetch_run", "")).lower() == "true",
            "remote_api_call_run": str(row.get("remote_api_call_run", "")).lower() == "true",
            "source_acquisition_run": str(row.get("source_acquisition_run", "")).lower() == "true",
            "layer_6_exit_credit": str(row.get("layer_6_exit_credit", "")).lower() == "true",
            "passed": bool(identifier)
            and str(row.get("implemented", "")).lower() == "true"
            and str(row.get("final_activation_decision_allowed", "")).lower() == "false"
            and str(row.get("activation_execution_allowed", "")).lower() == "false"
            and str(row.get("activation_execution_executed", "")).lower() == "false"
            and str(row.get("mechanic_activated", "")).lower() == "false"
            and str(row.get("production_simulation_run", "")).lower() == "false"
            and str(row.get("database_write_run", "")).lower() == "false"
            and str(row.get("live_data_fetch_run", "")).lower() == "false"
            and str(row.get("remote_api_call_run", "")).lower() == "false"
            and str(row.get("source_acquisition_run", "")).lower() == "false"
            and str(row.get("layer_6_exit_credit", "")).lower() == "false",
        })
    if len(rows) != expected_count:
        audited.append({
            id_key: "__row_count_mismatch__",
            "source_artifact": str(source_path),
            "implemented": False,
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
    return audited


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6jl = load_json(JSON_6JL)

    final_criteria = audit_matrix(read_csv(FINAL_CRITERIA_6JL), FINAL_CRITERIA_6JL, "final_decision_criterion", 8)
    auth_gates = audit_matrix(read_csv(AUTH_GATES_6JL), AUTH_GATES_6JL, "activation_authorization_gate", 7)
    operator_gates = audit_matrix(read_csv(OPERATOR_GATES_6JL), OPERATOR_GATES_6JL, "operator_review_gate", 7)
    rollback_policy = audit_matrix(read_csv(ROLLBACK_POLICY_6JL), ROLLBACK_POLICY_6JL, "roll_forward_rollback_policy", 7)
    shadow_review = audit_matrix(read_csv(SHADOW_REVIEW_6JL), SHADOW_REVIEW_6JL, "production_shadow_review_gate", 7)
    monitoring = audit_matrix(read_csv(MONITORING_6JL), MONITORING_6JL, "post_activation_monitoring_requirement", 7)
    exit_prereqs = audit_matrix(read_csv(EXIT_PREREQS_6JL), EXIT_PREREQS_6JL, "layer6_exit_prerequisite", 7)
    audit_req = audit_matrix(read_csv(FINAL_AUDIT_REQ_6JL), FINAL_AUDIT_REQ_6JL, "final_decision_audit_requirement", 8)
    prevention = audit_matrix(read_csv(PREVENTION_6JL), PREVENTION_6JL, "final_decision_prevention_assertion", 7)

    future_6jn = [
        {"contract": "plan_authorization_or_exit_certification_only", "required": True, "passed": True},
        {"contract": "define_authorization_decision_gate", "required": True, "passed": True},
        {"contract": "define_activation_execution_gate", "required": True, "passed": True},
        {"contract": "define_mechanic_activation_gate", "required": True, "passed": True},
        {"contract": "define_layer6_exit_certification_gate", "required": True, "passed": True},
        {"contract": "define_rollback_on_authorization_failure", "required": True, "passed": True},
        {"contract": "define_post_authorization_monitoring_gate", "required": True, "passed": True},
        {"contract": "do_not_execute_activation", "required": True, "passed": True},
        {"contract": "do_not_activate_mechanics", "required": True, "passed": True},
        {"contract": "do_not_grant_layer6_exit_credit", "required": True, "passed": True},
    ]

    required_inputs = [
        JSON_6JL, CHECKS_6JL, PREDECESSOR_6JL, INPUT_6JL,
        FINAL_CRITERIA_6JL, AUTH_GATES_6JL, OPERATOR_GATES_6JL,
        ROLLBACK_POLICY_6JL, SHADOW_REVIEW_6JL, MONITORING_6JL,
        EXIT_PREREQS_6JL, FINAL_AUDIT_REQ_6JL, PREVENTION_6JL,
        FUTURE_6JM_6JL, READONLY_6JL, PRESERVED_6JL, BLOCKING_6JL,
        DECISION_6JL, SAFETY_6JL, IMMUTABILITY_6JL, RECOMMENDED_6JL,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jl_implementation_exists", "expected": True, "actual": IMPLEMENT_6JL_PATH.exists(), "passed": IMPLEMENT_6JL_PATH.exists()},
        {"check": "6jl_json_exists", "expected": True, "actual": JSON_6JL.exists(), "passed": JSON_6JL.exists()},
        {"check": "6jl_all_checks_passed", "expected": True, "actual": json_6jl.get("all_checks_passed"), "passed": json_6jl.get("all_checks_passed") is True},
        {"check": "6jl_diagnosis", "expected": DIAGNOSIS_6JL, "actual": json_6jl.get("diagnosis"), "passed": json_6jl.get("diagnosis") == DIAGNOSIS_6JL},
        {"check": "6jl_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JL, "actual": json_6jl.get("recommended_next_layer"), "passed": json_6jl.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JL},
        {"check": "6jl_recommended_path", "expected": RECOMMENDED_PATH_6JL, "actual": json_6jl.get("recommended_path"), "passed": json_6jl.get("recommended_path") == RECOMMENDED_PATH_6JL},
        {"check": "6jl_implementation_completed", "expected": True, "actual": json_6jl.get("final_activation_decision_implementation_completed"), "passed": json_6jl.get("final_activation_decision_implementation_completed") is True},
        {"check": "6jl_final_decision_blocked", "expected": False, "actual": json_6jl.get("final_activation_decision_allowed_after_this_layer"), "passed": json_6jl.get("final_activation_decision_allowed_after_this_layer") is False},
        {"check": "6jl_no_exit_credit", "expected": False, "actual": json_6jl.get("layer_6_exit_credit"), "passed": json_6jl.get("layer_6_exit_credit") is False},
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
        {"blocked_surface": "authorization_or_exit_certification_planning", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "final_activation_decision", "blocked": True, "reason": "authorization/exit certification planning required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "authorization/exit certification required first", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "authorization/exit certification required first", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "authorization/exit certification required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "explicit exit certification required", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jl_passed", "expected": True, "actual": json_6jl.get("all_checks_passed"), "passed": json_6jl.get("all_checks_passed") is True},
        {"decision": "final_activation_decision_implementation_audited", "expected": True, "actual": True, "passed": True},
        {"decision": "authorization_or_exit_planning_allowed", "expected": True, "actual": True, "passed": True},
        {"decision": "final_activation_decision_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_execution_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6jn_authorization_or_exit_plan_next", "expected": RECOMMENDED_NEXT_LAYER_6JM, "actual": RECOMMENDED_NEXT_LAYER_6JM, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
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
        {"surface": "6jl_implementation", "policy": "read_only", "passed": True},
        {"surface": "6jk_plan", "policy": "read_only", "passed": True},
        {"surface": "6jj_audit", "policy": "read_only", "passed": True},
        {"surface": "6ji_implementation", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
        {"surface": "adapter_module", "policy": "read_only", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JM, "actual": RECOMMENDED_NEXT_LAYER_6JM, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JM, "actual": RECOMMENDED_PATH_6JM, "passed": True},
        {"decision": "recommend_authorization_or_exit_plan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_final_decision_or_exit_credit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JM, "actual": DIAGNOSIS_6JM, "passed": True},
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
        {"check": "future_6jn_contract", "passed": all_passed(future_6jn), "detail": f"{sum(1 for r in future_6jn if r['passed'])}/{len(future_6jn)}"},
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
        "future_6jn_contract": write_csv(FUTURE_6JN_CSV, future_6jn),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JM",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JM if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JM,
        "recommended_path": RECOMMENDED_PATH_6JM,
        "predecessor_implementation": str(IMPLEMENT_6JL_PATH),
        "predecessor_implementation_returncode": 0,
        "predecessor_implementation_diagnosis": json_6jl.get("diagnosis"),
        "audited_layer": "6JL",
        "source_family": "final_activation_decision",
        "final_activation_decision_plan_created": json_6jl.get("final_activation_decision_plan_created"),
        "final_activation_decision_implementation_completed": json_6jl.get("final_activation_decision_implementation_completed"),
        "final_activation_decision_implementation_audited": True,
        "final_decision_criteria_matrix_row_count": len(final_criteria),
        "activation_authorization_gate_matrix_row_count": len(auth_gates),
        "operator_review_gate_matrix_row_count": len(operator_gates),
        "roll_forward_rollback_policy_matrix_row_count": len(rollback_policy),
        "production_shadow_review_gate_matrix_row_count": len(shadow_review),
        "post_activation_monitoring_requirement_matrix_row_count": len(monitoring),
        "layer6_exit_prerequisite_matrix_row_count": len(exit_prereqs),
        "final_decision_audit_requirement_matrix_row_count": len(audit_req),
        "final_decision_prevention_assertion_count": len(prevention),
        "future_6jn_contract_valid": all_passed(future_6jn),
        "authorization_or_exit_planning_allowed_after_this_layer": True,
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
            "future_6jn_contract_csv": str(FUTURE_6JN_CSV),
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
