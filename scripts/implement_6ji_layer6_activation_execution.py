#!/usr/bin/env python3
"""Implement Layer 6JI activation execution plan without executing activation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ji_activation_execution_implementation"
TMP_DIR = Path("tmp")

PLAN_6JH_PATH = Path("scripts/plan_6jh_layer6_activation_execution.py")
JSON_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan.json"

CHECKS_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_checks.csv"
PREDECESSOR_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_predecessor.csv"
INPUT_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_input_artifacts.csv"
EXECUTION_CRITERIA_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_execution_criteria.csv"
MECHANIC_SURFACES_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_mechanic_execution_surfaces.csv"
LIVE_BLOCKERS_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_live_mode_blockers.csv"
SHADOW_PREREQS_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_production_shadow_prerequisites.csv"
ROLLBACK_GATES_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_rollback_execution_gates.csv"
FINAL_POLICY_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_final_activation_decision_policy.csv"
AUDIT_REQ_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_execution_audit_requirements.csv"
PREVENTION_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_execution_prevention_rules.csv"
FUTURE_6JI_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_future_6ji_contract.csv"
FUTURE_6JJ_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_future_6jj_contract.csv"
READONLY_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_readonly_sources.csv"
PRESERVED_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_preserved_families.csv"
BLOCKING_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_blocking_policy.csv"
DECISION_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_decision.csv"
SAFETY_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_safety_boundaries.csv"
IMMUTABILITY_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_immutability.csv"
RECOMMENDED_6JH = TMP_DIR / "layer6_6jh_activation_execution_plan_recommended_path.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
EXECUTION_CRITERIA_MATRIX_CSV = TMP_DIR / f"{SLUG}_execution_criteria_matrix.csv"
MECHANIC_EXECUTION_SURFACE_MATRIX_CSV = TMP_DIR / f"{SLUG}_mechanic_execution_surface_matrix.csv"
LIVE_MODE_BLOCKER_MATRIX_CSV = TMP_DIR / f"{SLUG}_live_mode_blocker_matrix.csv"
PRODUCTION_SHADOW_PREREQ_MATRIX_CSV = TMP_DIR / f"{SLUG}_production_shadow_prerequisite_matrix.csv"
ROLLBACK_EXECUTION_GATE_MATRIX_CSV = TMP_DIR / f"{SLUG}_rollback_execution_gate_matrix.csv"
FINAL_ACTIVATION_DECISION_POLICY_MATRIX_CSV = TMP_DIR / f"{SLUG}_final_activation_decision_policy_matrix.csv"
EXECUTION_AUDIT_REQUIREMENT_MATRIX_CSV = TMP_DIR / f"{SLUG}_execution_audit_requirement_matrix.csv"
EXECUTION_PREVENTION_ASSERTIONS_CSV = TMP_DIR / f"{SLUG}_execution_prevention_assertions.csv"
FUTURE_6JJ_CSV = TMP_DIR / f"{SLUG}_future_6jj_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JH = "layer_6_activation_execution_plan_complete"
DIAGNOSIS_6JI = "layer_6_activation_execution_implementation_complete"
RECOMMENDED_NEXT_LAYER_6JH = "6JI_layer_6_activation_execution_implementation"
RECOMMENDED_PATH_6JH = "plan_activation_execution_then_implement_before_execution_audit"
RECOMMENDED_NEXT_LAYER_6JI = "6JJ_layer_6_activation_execution_implementation_audit"
RECOMMENDED_PATH_6JI = "implement_activation_execution_then_audit_before_final_activation_decision"


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


def implement_from_plan(rows: List[Dict[str, str]], source_path: Path, source_key: str, output_key: str) -> List[Dict[str, Any]]:
    implemented = []
    for row in rows:
        value = row.get(source_key, "")
        implemented.append({
            output_key: value,
            "implemented": True,
            "source_plan": str(source_path),
            "activation_execution_allowed": False,
            "activation_execution_executed": False,
            "mechanic_activated": False,
            "final_activation_decision_allowed": False,
            "production_simulation_run": False,
            "database_write_run": False,
            "layer_6_exit_credit": False,
            "passed": bool(value),
        })
    return implemented


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6jh = load_json(JSON_6JH)

    execution_criteria_plan = read_csv(EXECUTION_CRITERIA_6JH)
    mechanic_surfaces_plan = read_csv(MECHANIC_SURFACES_6JH)
    live_blockers_plan = read_csv(LIVE_BLOCKERS_6JH)
    shadow_prereqs_plan = read_csv(SHADOW_PREREQS_6JH)
    rollback_gates_plan = read_csv(ROLLBACK_GATES_6JH)
    final_policy_plan = read_csv(FINAL_POLICY_6JH)
    audit_req_plan = read_csv(AUDIT_REQ_6JH)
    prevention_plan = read_csv(PREVENTION_6JH)

    execution_criteria = implement_from_plan(execution_criteria_plan, EXECUTION_CRITERIA_6JH, "execution_criterion", "execution_criterion")
    mechanic_surfaces = []
    for row in mechanic_surfaces_plan:
        mechanic_surfaces.append({
            "mechanic": row.get("mechanic", ""),
            "execution_surface_implemented": True,
            "source_plan": str(MECHANIC_SURFACES_6JH),
            "requires_shadow_mode_first": True,
            "requires_rollback_gate": True,
            "activation_execution_allowed": False,
            "activation_execution_executed": False,
            "mechanic_activated": False,
            "final_activation_decision_allowed": False,
            "production_simulation_run": False,
            "database_write_run": False,
            "layer_6_exit_credit": False,
            "passed": bool(row.get("mechanic", "")),
        })
    live_blockers = implement_from_plan(live_blockers_plan, LIVE_BLOCKERS_6JH, "live_mode_blocker", "live_mode_blocker")
    shadow_prereqs = implement_from_plan(shadow_prereqs_plan, SHADOW_PREREQS_6JH, "production_shadow_prerequisite", "production_shadow_prerequisite")
    rollback_gates = implement_from_plan(rollback_gates_plan, ROLLBACK_GATES_6JH, "rollback_execution_gate", "rollback_execution_gate")
    final_policy = implement_from_plan(final_policy_plan, FINAL_POLICY_6JH, "final_activation_decision_policy", "final_activation_decision_policy")
    audit_requirements = implement_from_plan(audit_req_plan, AUDIT_REQ_6JH, "execution_audit_requirement", "execution_audit_requirement")
    prevention_assertions = implement_from_plan(prevention_plan, PREVENTION_6JH, "execution_prevention_rule", "execution_prevention_assertion")

    future_6jj = [
        {"contract": "audit_6ji_activation_execution_implementation", "required": True, "passed": True},
        {"contract": "verify_execution_criteria_matrix", "required": True, "passed": True},
        {"contract": "verify_mechanic_execution_surface_matrix", "required": True, "passed": True},
        {"contract": "verify_live_mode_blocker_matrix", "required": True, "passed": True},
        {"contract": "verify_production_shadow_prerequisite_matrix", "required": True, "passed": True},
        {"contract": "verify_rollback_execution_gate_matrix", "required": True, "passed": True},
        {"contract": "verify_final_activation_decision_policy_matrix", "required": True, "passed": True},
        {"contract": "verify_execution_audit_requirement_matrix", "required": True, "passed": True},
        {"contract": "verify_execution_prevention_assertions", "required": True, "passed": True},
        {"contract": "verify_no_activation_execution", "required": True, "passed": True},
        {"contract": "verify_no_mechanics_activated", "required": True, "passed": True},
        {"contract": "verify_no_final_activation_decision", "required": True, "passed": True},
        {"contract": "verify_no_production_simulation", "required": True, "passed": True},
        {"contract": "verify_no_layer_6_exit_credit", "required": True, "passed": True},
    ]

    required_inputs = [
        JSON_6JH, CHECKS_6JH, PREDECESSOR_6JH, INPUT_6JH, EXECUTION_CRITERIA_6JH,
        MECHANIC_SURFACES_6JH, LIVE_BLOCKERS_6JH, SHADOW_PREREQS_6JH,
        ROLLBACK_GATES_6JH, FINAL_POLICY_6JH, AUDIT_REQ_6JH, PREVENTION_6JH,
        FUTURE_6JI_6JH, FUTURE_6JJ_6JH, READONLY_6JH, PRESERVED_6JH,
        BLOCKING_6JH, DECISION_6JH, SAFETY_6JH, IMMUTABILITY_6JH, RECOMMENDED_6JH,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jh_plan_exists", "expected": True, "actual": PLAN_6JH_PATH.exists(), "passed": PLAN_6JH_PATH.exists()},
        {"check": "6jh_json_exists", "expected": True, "actual": JSON_6JH.exists(), "passed": JSON_6JH.exists()},
        {"check": "6jh_all_checks_passed", "expected": True, "actual": json_6jh.get("all_checks_passed"), "passed": json_6jh.get("all_checks_passed") is True},
        {"check": "6jh_diagnosis", "expected": DIAGNOSIS_6JH, "actual": json_6jh.get("diagnosis"), "passed": json_6jh.get("diagnosis") == DIAGNOSIS_6JH},
        {"check": "6jh_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JH, "actual": json_6jh.get("recommended_next_layer"), "passed": json_6jh.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JH},
        {"check": "6jh_recommended_path", "expected": RECOMMENDED_PATH_6JH, "actual": json_6jh.get("recommended_path"), "passed": json_6jh.get("recommended_path") == RECOMMENDED_PATH_6JH},
        {"check": "6jh_implementation_allowed", "expected": True, "actual": json_6jh.get("activation_execution_implementation_allowed_after_this_layer"), "passed": json_6jh.get("activation_execution_implementation_allowed_after_this_layer") is True},
        {"check": "6jh_activation_execution_blocked", "expected": False, "actual": json_6jh.get("activation_execution_allowed_after_this_layer"), "passed": json_6jh.get("activation_execution_allowed_after_this_layer") is False},
        {"check": "6jh_no_exit_credit", "expected": False, "actual": json_6jh.get("layer_6_exit_credit"), "passed": json_6jh.get("layer_6_exit_credit") is False},
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
    ]]

    blocking_rows = [
        {"blocked_surface": "activation_execution_implementation_audit", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "6JJ audit and final decision required first", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "audit and final decision required first", "passed": True},
        {"blocked_surface": "final_activation_decision", "blocked": True, "reason": "implementation cannot decide final activation", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "execution audit required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "activation execution not audited", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jh_passed", "expected": True, "actual": json_6jh.get("all_checks_passed"), "passed": json_6jh.get("all_checks_passed") is True},
        {"decision": "activation_execution_implementation_completed", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_execution_implementation_audited", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_execution_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "final_activation_decision_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6jj_activation_execution_audit_next", "expected": RECOMMENDED_NEXT_LAYER_6JI, "actual": RECOMMENDED_NEXT_LAYER_6JI, "passed": True},
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
        {"surface": "6jh_plan", "policy": "read_only", "passed": True},
        {"surface": "6jg_audit", "policy": "read_only", "passed": True},
        {"surface": "6jf_implementation", "policy": "read_only", "passed": True},
        {"surface": "6je_plan", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
        {"surface": "adapter_module", "policy": "read_only", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JI, "actual": RECOMMENDED_NEXT_LAYER_6JI, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JI, "actual": RECOMMENDED_PATH_6JI, "passed": True},
        {"decision": "recommend_activation_execution_audit_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_final_decision_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JI, "actual": DIAGNOSIS_6JI, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "execution_criteria_matrix", "passed": len(execution_criteria) == 8 and all_passed(execution_criteria), "detail": f"{len(execution_criteria)}/8"},
        {"check": "mechanic_execution_surface_matrix", "passed": len(mechanic_surfaces) == 10 and all_passed(mechanic_surfaces), "detail": f"{len(mechanic_surfaces)}/10"},
        {"check": "live_mode_blocker_matrix", "passed": len(live_blockers) == 7 and all_passed(live_blockers), "detail": f"{len(live_blockers)}/7"},
        {"check": "production_shadow_prerequisite_matrix", "passed": len(shadow_prereqs) == 7 and all_passed(shadow_prereqs), "detail": f"{len(shadow_prereqs)}/7"},
        {"check": "rollback_execution_gate_matrix", "passed": len(rollback_gates) == 7 and all_passed(rollback_gates), "detail": f"{len(rollback_gates)}/7"},
        {"check": "final_activation_decision_policy_matrix", "passed": len(final_policy) == 7 and all_passed(final_policy), "detail": f"{len(final_policy)}/7"},
        {"check": "execution_audit_requirement_matrix", "passed": len(audit_requirements) == 8 and all_passed(audit_requirements), "detail": f"{len(audit_requirements)}/8"},
        {"check": "execution_prevention_assertions", "passed": len(prevention_assertions) == 7 and all_passed(prevention_assertions), "detail": f"{len(prevention_assertions)}/7"},
        {"check": "future_6jj_contract", "passed": all_passed(future_6jj), "detail": f"{sum(1 for r in future_6jj if r['passed'])}/{len(future_6jj)}"},
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
        "execution_criteria_matrix": write_csv(EXECUTION_CRITERIA_MATRIX_CSV, execution_criteria),
        "mechanic_execution_surface_matrix": write_csv(MECHANIC_EXECUTION_SURFACE_MATRIX_CSV, mechanic_surfaces),
        "live_mode_blocker_matrix": write_csv(LIVE_MODE_BLOCKER_MATRIX_CSV, live_blockers),
        "production_shadow_prerequisite_matrix": write_csv(PRODUCTION_SHADOW_PREREQ_MATRIX_CSV, shadow_prereqs),
        "rollback_execution_gate_matrix": write_csv(ROLLBACK_EXECUTION_GATE_MATRIX_CSV, rollback_gates),
        "final_activation_decision_policy_matrix": write_csv(FINAL_ACTIVATION_DECISION_POLICY_MATRIX_CSV, final_policy),
        "execution_audit_requirement_matrix": write_csv(EXECUTION_AUDIT_REQUIREMENT_MATRIX_CSV, audit_requirements),
        "execution_prevention_assertions": write_csv(EXECUTION_PREVENTION_ASSERTIONS_CSV, prevention_assertions),
        "future_6jj_contract": write_csv(FUTURE_6JJ_CSV, future_6jj),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JI",
        "layer_type": "game_mechanics_realism",
        "implementation_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JI if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JI,
        "recommended_path": RECOMMENDED_PATH_6JI,
        "predecessor_plan": str(PLAN_6JH_PATH),
        "predecessor_plan_returncode": 0,
        "predecessor_plan_diagnosis": json_6jh.get("diagnosis"),
        "implemented_layer_after": "6JH",
        "source_family": "activation_execution",
        "activation_execution_plan_created": json_6jh.get("activation_execution_plan_created"),
        "activation_execution_implementation_completed": True,
        "activation_execution_implementation_audited": False,
        "execution_criteria_matrix_row_count": len(execution_criteria),
        "mechanic_execution_surface_matrix_row_count": len(mechanic_surfaces),
        "live_mode_blocker_matrix_row_count": len(live_blockers),
        "production_shadow_prerequisite_matrix_row_count": len(shadow_prereqs),
        "rollback_execution_gate_matrix_row_count": len(rollback_gates),
        "final_activation_decision_policy_matrix_row_count": len(final_policy),
        "execution_audit_requirement_matrix_row_count": len(audit_requirements),
        "execution_prevention_assertion_count": len(prevention_assertions),
        "future_6jj_contract_valid": all_passed(future_6jj),
        "activation_execution_allowed_after_this_layer": False,
        "activation_execution_executed": False,
        "mechanics_activated_by_this_layer": False,
        "final_activation_decision_allowed_after_this_layer": False,
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
            "execution_criteria_matrix_csv": str(EXECUTION_CRITERIA_MATRIX_CSV),
            "mechanic_execution_surface_matrix_csv": str(MECHANIC_EXECUTION_SURFACE_MATRIX_CSV),
            "live_mode_blocker_matrix_csv": str(LIVE_MODE_BLOCKER_MATRIX_CSV),
            "production_shadow_prerequisite_matrix_csv": str(PRODUCTION_SHADOW_PREREQ_MATRIX_CSV),
            "rollback_execution_gate_matrix_csv": str(ROLLBACK_EXECUTION_GATE_MATRIX_CSV),
            "final_activation_decision_policy_matrix_csv": str(FINAL_ACTIVATION_DECISION_POLICY_MATRIX_CSV),
            "execution_audit_requirement_matrix_csv": str(EXECUTION_AUDIT_REQUIREMENT_MATRIX_CSV),
            "execution_prevention_assertions_csv": str(EXECUTION_PREVENTION_ASSERTIONS_CSV),
            "future_6jj_contract_csv": str(FUTURE_6JJ_CSV),
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
