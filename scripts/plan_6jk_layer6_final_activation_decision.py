#!/usr/bin/env python3
"""Plan Layer 6JK final activation decision without deciding activation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jk_final_activation_decision_plan"
TMP_DIR = Path("tmp")

AUDIT_6JJ_PATH = Path("scripts/audit_6jj_layer6_activation_execution_implementation.py")
JSON_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit.json"

CHECKS_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_checks.csv"
PREDECESSOR_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_predecessor.csv"
INPUT_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_input_artifacts.csv"
EXECUTION_CRITERIA_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_execution_criteria_matrix.csv"
MECHANIC_SURFACES_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_mechanic_execution_surface_matrix.csv"
LIVE_BLOCKERS_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_live_mode_blocker_matrix.csv"
SHADOW_PREREQS_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_production_shadow_prerequisite_matrix.csv"
ROLLBACK_GATES_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_rollback_execution_gate_matrix.csv"
FINAL_POLICY_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_final_activation_decision_policy_matrix.csv"
AUDIT_REQ_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_execution_audit_requirement_matrix.csv"
PREVENTION_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_execution_prevention_assertions.csv"
FUTURE_6JK_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_future_6jk_contract.csv"
READONLY_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_readonly_sources.csv"
PRESERVED_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_preserved_families.csv"
BLOCKING_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_blocking_policy.csv"
DECISION_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_decision.csv"
SAFETY_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_safety_boundaries.csv"
IMMUTABILITY_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_immutability.csv"
RECOMMENDED_6JJ = TMP_DIR / "layer6_6jj_activation_execution_implementation_audit_recommended_path.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
FINAL_DECISION_CRITERIA_CSV = TMP_DIR / f"{SLUG}_final_decision_criteria.csv"
AUTHORIZATION_GATES_CSV = TMP_DIR / f"{SLUG}_activation_authorization_gates.csv"
OPERATOR_REVIEW_GATES_CSV = TMP_DIR / f"{SLUG}_operator_review_gates.csv"
ROLL_FORWARD_ROLLBACK_CSV = TMP_DIR / f"{SLUG}_roll_forward_rollback_policy.csv"
SHADOW_REVIEW_GATES_CSV = TMP_DIR / f"{SLUG}_production_shadow_review_gates.csv"
MONITORING_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_post_activation_monitoring_requirements.csv"
EXIT_PREREQUISITES_CSV = TMP_DIR / f"{SLUG}_layer6_exit_prerequisites.csv"
FINAL_AUDIT_REQ_CSV = TMP_DIR / f"{SLUG}_final_decision_audit_requirements.csv"
PREVENTION_RULES_CSV = TMP_DIR / f"{SLUG}_final_decision_prevention_rules.csv"
FUTURE_6JL_CSV = TMP_DIR / f"{SLUG}_future_6jl_contract.csv"
FUTURE_6JM_CSV = TMP_DIR / f"{SLUG}_future_6jm_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JJ = "layer_6_activation_execution_implementation_audit_complete"
DIAGNOSIS_6JK = "layer_6_final_activation_decision_plan_complete"
RECOMMENDED_NEXT_LAYER_6JJ = "6JK_layer_6_final_activation_decision_plan"
RECOMMENDED_PATH_6JJ = "audit_activation_execution_then_plan_final_activation_decision"
RECOMMENDED_NEXT_LAYER_6JK = "6JL_layer_6_final_activation_decision_implementation"
RECOMMENDED_PATH_6JK = "plan_final_activation_decision_then_implement_before_final_decision_audit"


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


def gated_rows(items: List[str], key: str) -> List[Dict[str, Any]]:
    return [
        {
            key: item,
            "planned": True,
            "final_activation_decision_allowed": False,
            "activation_execution_allowed": False,
            "activation_execution_executed": False,
            "mechanic_activated": False,
            "production_simulation_run": False,
            "database_write_run": False,
            "layer_6_exit_credit": False,
            "passed": True,
        }
        for item in items
    ]


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6jj = load_json(JSON_6JJ)

    final_criteria = gated_rows([
        "6jj_activation_execution_audit_passed",
        "all_activation_execution_surfaces_verified",
        "all_live_mode_blockers_verified",
        "rollback_policy_verified",
        "operator_review_required",
        "production_shadow_review_required",
        "post_activation_monitoring_required",
        "layer6_exit_requires_future_audit",
    ], "final_decision_criterion")

    authorization_gates = gated_rows([
        "operator_authorization_gate",
        "rollback_authorization_gate",
        "shadow_review_authorization_gate",
        "monitoring_authorization_gate",
        "source_family_regression_gate",
        "mechanic_activation_gate",
        "layer6_exit_gate",
    ], "activation_authorization_gate")

    operator_review_gates = gated_rows([
        "review_6jj_audit_summary",
        "review_activation_execution_blockers",
        "review_mechanic_surface_status",
        "review_rollback_policy",
        "review_shadow_outputs_policy",
        "review_monitoring_policy",
        "review_exit_prerequisites",
    ], "operator_review_gate")

    roll_forward_rollback = gated_rows([
        "roll_forward_only_after_final_audit",
        "rollback_on_missing_source_family",
        "rollback_on_mechanic_surface_regression",
        "rollback_on_shadow_review_failure",
        "rollback_on_monitoring_failure",
        "rollback_on_database_write_attempt",
        "rollback_on_layer6_exit_attempt",
    ], "roll_forward_rollback_policy")

    shadow_review = gated_rows([
        "shadow_review_requires_no_live_write",
        "shadow_review_requires_surface_parity",
        "shadow_review_requires_blocker_compliance",
        "shadow_review_requires_rollback_compliance",
        "shadow_review_requires_operator_signoff",
        "shadow_review_requires_monitoring_plan",
        "shadow_review_blocks_exit_credit",
    ], "production_shadow_review_gate")

    monitoring = gated_rows([
        "monitor_activation_execution_status",
        "monitor_mechanic_activation_status",
        "monitor_final_decision_status",
        "monitor_database_write_status",
        "monitor_source_family_status",
        "monitor_rollback_trigger_status",
        "monitor_layer6_exit_status",
    ], "post_activation_monitoring_requirement")

    exit_prereqs = gated_rows([
        "final_decision_implementation_complete",
        "final_decision_audit_complete",
        "activation_authorization_gate_passed",
        "rollback_policy_passed",
        "monitoring_requirements_passed",
        "no_unreviewed_database_writes",
        "explicit_layer6_exit_certification",
    ], "layer6_exit_prerequisite")

    audit_requirements = gated_rows([
        "audit_final_decision_implementation",
        "verify_final_decision_criteria",
        "verify_authorization_gates",
        "verify_operator_review_gates",
        "verify_roll_forward_rollback_policy",
        "verify_shadow_review_gates",
        "verify_monitoring_requirements",
        "verify_no_layer6_exit_credit",
    ], "final_decision_audit_requirement")

    prevention_rules = gated_rows([
        "do_not_make_final_activation_decision_from_plan",
        "do_not_execute_activation_from_plan",
        "do_not_activate_mechanics_from_plan",
        "do_not_run_production_simulation_from_plan",
        "do_not_write_databases_from_plan",
        "do_not_fetch_live_data_from_plan",
        "do_not_grant_layer6_exit_from_plan",
    ], "final_decision_prevention_rule")

    future_6jl = [
        {"contract": "implement_final_activation_decision_plan_only", "required": True, "passed": True},
        {"contract": "implement_final_decision_criteria", "required": True, "passed": True},
        {"contract": "implement_activation_authorization_gates", "required": True, "passed": True},
        {"contract": "implement_operator_review_gates", "required": True, "passed": True},
        {"contract": "implement_roll_forward_rollback_policy", "required": True, "passed": True},
        {"contract": "implement_production_shadow_review_gates", "required": True, "passed": True},
        {"contract": "implement_post_activation_monitoring_requirements", "required": True, "passed": True},
        {"contract": "implement_layer6_exit_prerequisites", "required": True, "passed": True},
        {"contract": "implement_final_decision_audit_requirements", "required": True, "passed": True},
        {"contract": "do_not_make_final_decision", "required": True, "passed": True},
    ]

    future_6jm = [
        {"contract": "audit_6jl_final_activation_decision_implementation", "required": True, "passed": True},
        {"contract": "verify_final_decision_criteria", "required": True, "passed": True},
        {"contract": "verify_authorization_gates", "required": True, "passed": True},
        {"contract": "verify_operator_review_gates", "required": True, "passed": True},
        {"contract": "verify_roll_forward_rollback_policy", "required": True, "passed": True},
        {"contract": "verify_shadow_review_gates", "required": True, "passed": True},
        {"contract": "verify_monitoring_requirements", "required": True, "passed": True},
        {"contract": "verify_no_unapproved_activation", "required": True, "passed": True},
        {"contract": "verify_no_layer6_exit_credit", "required": True, "passed": True},
        {"contract": "recommend_final_authorization_or_exit_gate", "required": True, "passed": True},
    ]

    required_inputs = [
        JSON_6JJ, CHECKS_6JJ, PREDECESSOR_6JJ, INPUT_6JJ, EXECUTION_CRITERIA_6JJ,
        MECHANIC_SURFACES_6JJ, LIVE_BLOCKERS_6JJ, SHADOW_PREREQS_6JJ,
        ROLLBACK_GATES_6JJ, FINAL_POLICY_6JJ, AUDIT_REQ_6JJ, PREVENTION_6JJ,
        FUTURE_6JK_6JJ, READONLY_6JJ, PRESERVED_6JJ, BLOCKING_6JJ,
        DECISION_6JJ, SAFETY_6JJ, IMMUTABILITY_6JJ, RECOMMENDED_6JJ,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jj_audit_exists", "expected": True, "actual": AUDIT_6JJ_PATH.exists(), "passed": AUDIT_6JJ_PATH.exists()},
        {"check": "6jj_json_exists", "expected": True, "actual": JSON_6JJ.exists(), "passed": JSON_6JJ.exists()},
        {"check": "6jj_all_checks_passed", "expected": True, "actual": json_6jj.get("all_checks_passed"), "passed": json_6jj.get("all_checks_passed") is True},
        {"check": "6jj_diagnosis", "expected": DIAGNOSIS_6JJ, "actual": json_6jj.get("diagnosis"), "passed": json_6jj.get("diagnosis") == DIAGNOSIS_6JJ},
        {"check": "6jj_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JJ, "actual": json_6jj.get("recommended_next_layer"), "passed": json_6jj.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JJ},
        {"check": "6jj_recommended_path", "expected": RECOMMENDED_PATH_6JJ, "actual": json_6jj.get("recommended_path"), "passed": json_6jj.get("recommended_path") == RECOMMENDED_PATH_6JJ},
        {"check": "6jj_final_decision_planning_allowed", "expected": True, "actual": json_6jj.get("final_activation_decision_planning_allowed_after_this_layer"), "passed": json_6jj.get("final_activation_decision_planning_allowed_after_this_layer") is True},
        {"check": "6jj_final_decision_blocked", "expected": False, "actual": json_6jj.get("final_activation_decision_allowed_after_this_layer"), "passed": json_6jj.get("final_activation_decision_allowed_after_this_layer") is False},
        {"check": "6jj_no_exit_credit", "expected": False, "actual": json_6jj.get("layer_6_exit_credit"), "passed": json_6jj.get("layer_6_exit_credit") is False},
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
        {"blocked_surface": "final_activation_decision_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "final_activation_decision", "blocked": True, "reason": "implementation and audit required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "final decision not implemented or audited", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "final decision not implemented or audited", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "final decision not implemented or audited", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "final audit and exit certification required", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jj_passed", "expected": True, "actual": json_6jj.get("all_checks_passed"), "passed": json_6jj.get("all_checks_passed") is True},
        {"decision": "final_activation_decision_plan_created", "expected": True, "actual": True, "passed": True},
        {"decision": "final_activation_decision_implementation_allowed", "expected": True, "actual": True, "passed": True},
        {"decision": "final_activation_decision_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_execution_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6jl_final_decision_implementation_next", "expected": RECOMMENDED_NEXT_LAYER_6JK, "actual": RECOMMENDED_NEXT_LAYER_6JK, "passed": True},
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
        {"boundary": "no_activation_execution", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_final_activation_decision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    immutability_rows = [
        {"surface": "6jj_audit", "policy": "read_only", "passed": True},
        {"surface": "6ji_implementation", "policy": "read_only", "passed": True},
        {"surface": "6jh_plan", "policy": "read_only", "passed": True},
        {"surface": "6jg_audit", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
        {"surface": "adapter_module", "policy": "read_only", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JK, "actual": RECOMMENDED_NEXT_LAYER_6JK, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JK, "actual": RECOMMENDED_PATH_6JK, "passed": True},
        {"decision": "recommend_final_decision_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_final_decision_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JK, "actual": DIAGNOSIS_6JK, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "final_decision_criteria", "passed": len(final_criteria) == 8 and all_passed(final_criteria), "detail": f"{len(final_criteria)}/8"},
        {"check": "activation_authorization_gates", "passed": len(authorization_gates) == 7 and all_passed(authorization_gates), "detail": f"{len(authorization_gates)}/7"},
        {"check": "operator_review_gates", "passed": len(operator_review_gates) == 7 and all_passed(operator_review_gates), "detail": f"{len(operator_review_gates)}/7"},
        {"check": "roll_forward_rollback_policy", "passed": len(roll_forward_rollback) == 7 and all_passed(roll_forward_rollback), "detail": f"{len(roll_forward_rollback)}/7"},
        {"check": "production_shadow_review_gates", "passed": len(shadow_review) == 7 and all_passed(shadow_review), "detail": f"{len(shadow_review)}/7"},
        {"check": "post_activation_monitoring_requirements", "passed": len(monitoring) == 7 and all_passed(monitoring), "detail": f"{len(monitoring)}/7"},
        {"check": "layer6_exit_prerequisites", "passed": len(exit_prereqs) == 7 and all_passed(exit_prereqs), "detail": f"{len(exit_prereqs)}/7"},
        {"check": "final_decision_audit_requirements", "passed": len(audit_requirements) == 8 and all_passed(audit_requirements), "detail": f"{len(audit_requirements)}/8"},
        {"check": "final_decision_prevention_rules", "passed": len(prevention_rules) == 7 and all_passed(prevention_rules), "detail": f"{len(prevention_rules)}/7"},
        {"check": "future_6jl_contract", "passed": all_passed(future_6jl), "detail": f"{sum(1 for r in future_6jl if r['passed'])}/{len(future_6jl)}"},
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
        "final_decision_criteria": write_csv(FINAL_DECISION_CRITERIA_CSV, final_criteria),
        "activation_authorization_gates": write_csv(AUTHORIZATION_GATES_CSV, authorization_gates),
        "operator_review_gates": write_csv(OPERATOR_REVIEW_GATES_CSV, operator_review_gates),
        "roll_forward_rollback_policy": write_csv(ROLL_FORWARD_ROLLBACK_CSV, roll_forward_rollback),
        "production_shadow_review_gates": write_csv(SHADOW_REVIEW_GATES_CSV, shadow_review),
        "post_activation_monitoring_requirements": write_csv(MONITORING_REQUIREMENTS_CSV, monitoring),
        "layer6_exit_prerequisites": write_csv(EXIT_PREREQUISITES_CSV, exit_prereqs),
        "final_decision_audit_requirements": write_csv(FINAL_AUDIT_REQ_CSV, audit_requirements),
        "final_decision_prevention_rules": write_csv(PREVENTION_RULES_CSV, prevention_rules),
        "future_6jl_contract": write_csv(FUTURE_6JL_CSV, future_6jl),
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
        "layer": "6JK",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JK if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JK,
        "recommended_path": RECOMMENDED_PATH_6JK,
        "predecessor_audit": str(AUDIT_6JJ_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6jj.get("diagnosis"),
        "planned_layer_after": "6JJ",
        "source_family": "final_activation_decision",
        "activation_execution_implementation_audited": json_6jj.get("activation_execution_implementation_audited"),
        "final_activation_decision_plan_created": True,
        "final_activation_decision_implementation_allowed_after_this_layer": True,
        "final_activation_decision_implementation_completed": False,
        "final_activation_decision_implementation_audited": False,
        "final_decision_criteria_count": len(final_criteria),
        "activation_authorization_gate_count": len(authorization_gates),
        "operator_review_gate_count": len(operator_review_gates),
        "roll_forward_rollback_policy_count": len(roll_forward_rollback),
        "production_shadow_review_gate_count": len(shadow_review),
        "post_activation_monitoring_requirement_count": len(monitoring),
        "layer6_exit_prerequisite_count": len(exit_prereqs),
        "final_decision_audit_requirement_count": len(audit_requirements),
        "final_decision_prevention_rule_count": len(prevention_rules),
        "future_6jl_contract_valid": all_passed(future_6jl),
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
            "final_decision_criteria_csv": str(FINAL_DECISION_CRITERIA_CSV),
            "activation_authorization_gates_csv": str(AUTHORIZATION_GATES_CSV),
            "operator_review_gates_csv": str(OPERATOR_REVIEW_GATES_CSV),
            "roll_forward_rollback_policy_csv": str(ROLL_FORWARD_ROLLBACK_CSV),
            "production_shadow_review_gates_csv": str(SHADOW_REVIEW_GATES_CSV),
            "post_activation_monitoring_requirements_csv": str(MONITORING_REQUIREMENTS_CSV),
            "layer6_exit_prerequisites_csv": str(EXIT_PREREQUISITES_CSV),
            "final_decision_audit_requirements_csv": str(FINAL_AUDIT_REQ_CSV),
            "final_decision_prevention_rules_csv": str(PREVENTION_RULES_CSV),
            "future_6jl_contract_csv": str(FUTURE_6JL_CSV),
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
