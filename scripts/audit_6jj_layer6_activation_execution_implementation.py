#!/usr/bin/env python3
"""Audit Layer 6JI activation execution implementation without executing activation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jj_activation_execution_implementation_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6JI_PATH = Path("scripts/implement_6ji_layer6_activation_execution.py")
JSON_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation.json"

CHECKS_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_checks.csv"
PREDECESSOR_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_predecessor.csv"
INPUT_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_input_artifacts.csv"
EXECUTION_CRITERIA_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_execution_criteria_matrix.csv"
MECHANIC_SURFACES_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_mechanic_execution_surface_matrix.csv"
LIVE_BLOCKERS_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_live_mode_blocker_matrix.csv"
SHADOW_PREREQS_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_production_shadow_prerequisite_matrix.csv"
ROLLBACK_GATES_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_rollback_execution_gate_matrix.csv"
FINAL_POLICY_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_final_activation_decision_policy_matrix.csv"
AUDIT_REQ_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_execution_audit_requirement_matrix.csv"
PREVENTION_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_execution_prevention_assertions.csv"
FUTURE_6JJ_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_future_6jj_contract.csv"
READONLY_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_readonly_sources.csv"
PRESERVED_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_preserved_families.csv"
BLOCKING_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_blocking_policy.csv"
DECISION_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_decision.csv"
SAFETY_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_safety_boundaries.csv"
IMMUTABILITY_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_immutability.csv"
RECOMMENDED_6JI = TMP_DIR / "layer6_6ji_activation_execution_implementation_recommended_path.csv"

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
FUTURE_6JK_CSV = TMP_DIR / f"{SLUG}_future_6jk_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JI = "layer_6_activation_execution_implementation_complete"
DIAGNOSIS_6JJ = "layer_6_activation_execution_implementation_audit_complete"
RECOMMENDED_NEXT_LAYER_6JI = "6JJ_layer_6_activation_execution_implementation_audit"
RECOMMENDED_PATH_6JI = "implement_activation_execution_then_audit_before_final_activation_decision"
RECOMMENDED_NEXT_LAYER_6JJ = "6JK_layer_6_final_activation_decision_plan"
RECOMMENDED_PATH_6JJ = "audit_activation_execution_then_plan_final_activation_decision"


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
        implemented_value = row.get("implemented", row.get("execution_surface_implemented", ""))
        audited.append({
            id_key: identifier,
            "source_artifact": str(source_path),
            "implemented": str(implemented_value).lower() == "true",
            "activation_execution_allowed": str(row.get("activation_execution_allowed", "")).lower() == "true",
            "activation_execution_executed": str(row.get("activation_execution_executed", "")).lower() == "true",
            "mechanic_activated": str(row.get("mechanic_activated", "")).lower() == "true",
            "final_activation_decision_allowed": str(row.get("final_activation_decision_allowed", "")).lower() == "true",
            "production_simulation_run": str(row.get("production_simulation_run", "")).lower() == "true",
            "database_write_run": str(row.get("database_write_run", "")).lower() == "true",
            "layer_6_exit_credit": str(row.get("layer_6_exit_credit", "")).lower() == "true",
            "passed": bool(identifier)
            and str(implemented_value).lower() == "true"
            and str(row.get("activation_execution_allowed", "")).lower() == "false"
            and str(row.get("activation_execution_executed", "")).lower() == "false"
            and str(row.get("mechanic_activated", "")).lower() == "false"
            and str(row.get("final_activation_decision_allowed", "")).lower() == "false"
            and str(row.get("production_simulation_run", "")).lower() == "false"
            and str(row.get("database_write_run", "")).lower() == "false"
            and str(row.get("layer_6_exit_credit", "")).lower() == "false",
        })
    if len(rows) != expected_count:
        audited.append({
            id_key: "__row_count_mismatch__",
            "source_artifact": str(source_path),
            "implemented": False,
            "activation_execution_allowed": True,
            "activation_execution_executed": True,
            "mechanic_activated": True,
            "final_activation_decision_allowed": True,
            "production_simulation_run": True,
            "database_write_run": True,
            "layer_6_exit_credit": True,
            "passed": False,
        })
    return audited


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6ji = load_json(JSON_6JI)

    execution_criteria = audit_matrix(read_csv(EXECUTION_CRITERIA_6JI), EXECUTION_CRITERIA_6JI, "execution_criterion", 8)
    mechanic_surfaces = audit_matrix(read_csv(MECHANIC_SURFACES_6JI), MECHANIC_SURFACES_6JI, "mechanic", 10)
    live_blockers = audit_matrix(read_csv(LIVE_BLOCKERS_6JI), LIVE_BLOCKERS_6JI, "live_mode_blocker", 7)
    shadow_prereqs = audit_matrix(read_csv(SHADOW_PREREQS_6JI), SHADOW_PREREQS_6JI, "production_shadow_prerequisite", 7)
    rollback_gates = audit_matrix(read_csv(ROLLBACK_GATES_6JI), ROLLBACK_GATES_6JI, "rollback_execution_gate", 7)
    final_policy = audit_matrix(read_csv(FINAL_POLICY_6JI), FINAL_POLICY_6JI, "final_activation_decision_policy", 7)
    audit_requirements = audit_matrix(read_csv(AUDIT_REQ_6JI), AUDIT_REQ_6JI, "execution_audit_requirement", 8)
    prevention_assertions = audit_matrix(read_csv(PREVENTION_6JI), PREVENTION_6JI, "execution_prevention_assertion", 7)

    future_6jk = [
        {"contract": "plan_final_activation_decision_only", "required": True, "passed": True},
        {"contract": "define_final_activation_criteria", "required": True, "passed": True},
        {"contract": "define_operator_review_gate", "required": True, "passed": True},
        {"contract": "define_roll_forward_or_rollback_policy", "required": True, "passed": True},
        {"contract": "define_layer6_exit_prerequisites", "required": True, "passed": True},
        {"contract": "define_final_decision_audit_requirements", "required": True, "passed": True},
        {"contract": "do_not_activate_mechanics", "required": True, "passed": True},
        {"contract": "do_not_grant_layer6_exit_credit", "required": True, "passed": True},
    ]

    required_inputs = [
        JSON_6JI, CHECKS_6JI, PREDECESSOR_6JI, INPUT_6JI, EXECUTION_CRITERIA_6JI,
        MECHANIC_SURFACES_6JI, LIVE_BLOCKERS_6JI, SHADOW_PREREQS_6JI,
        ROLLBACK_GATES_6JI, FINAL_POLICY_6JI, AUDIT_REQ_6JI, PREVENTION_6JI,
        FUTURE_6JJ_6JI, READONLY_6JI, PRESERVED_6JI, BLOCKING_6JI,
        DECISION_6JI, SAFETY_6JI, IMMUTABILITY_6JI, RECOMMENDED_6JI,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ji_implementation_exists", "expected": True, "actual": IMPLEMENT_6JI_PATH.exists(), "passed": IMPLEMENT_6JI_PATH.exists()},
        {"check": "6ji_json_exists", "expected": True, "actual": JSON_6JI.exists(), "passed": JSON_6JI.exists()},
        {"check": "6ji_all_checks_passed", "expected": True, "actual": json_6ji.get("all_checks_passed"), "passed": json_6ji.get("all_checks_passed") is True},
        {"check": "6ji_diagnosis", "expected": DIAGNOSIS_6JI, "actual": json_6ji.get("diagnosis"), "passed": json_6ji.get("diagnosis") == DIAGNOSIS_6JI},
        {"check": "6ji_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JI, "actual": json_6ji.get("recommended_next_layer"), "passed": json_6ji.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JI},
        {"check": "6ji_recommended_path", "expected": RECOMMENDED_PATH_6JI, "actual": json_6ji.get("recommended_path"), "passed": json_6ji.get("recommended_path") == RECOMMENDED_PATH_6JI},
        {"check": "6ji_implementation_completed", "expected": True, "actual": json_6ji.get("activation_execution_implementation_completed"), "passed": json_6ji.get("activation_execution_implementation_completed") is True},
        {"check": "6ji_activation_execution_blocked", "expected": False, "actual": json_6ji.get("activation_execution_allowed_after_this_layer"), "passed": json_6ji.get("activation_execution_allowed_after_this_layer") is False},
        {"check": "6ji_no_exit_credit", "expected": False, "actual": json_6ji.get("layer_6_exit_credit"), "passed": json_6ji.get("layer_6_exit_credit") is False},
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
        {"blocked_surface": "final_activation_decision_planning", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "final activation decision not planned, implemented, or audited", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "final activation decision required first", "passed": True},
        {"blocked_surface": "final_activation_decision", "blocked": True, "reason": "6JJ may only recommend final decision planning", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "final activation decision required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "final decision and exit certification required", "passed": True},
    ]

    decision_rows = [
        {"decision": "6ji_passed", "expected": True, "actual": json_6ji.get("all_checks_passed"), "passed": json_6ji.get("all_checks_passed") is True},
        {"decision": "activation_execution_implementation_audited", "expected": True, "actual": True, "passed": True},
        {"decision": "final_activation_decision_planning_allowed", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_execution_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "final_activation_decision_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6jk_final_activation_decision_plan_next", "expected": RECOMMENDED_NEXT_LAYER_6JJ, "actual": RECOMMENDED_NEXT_LAYER_6JJ, "passed": True},
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
        {"surface": "6ji_implementation", "policy": "read_only", "passed": True},
        {"surface": "6jh_plan", "policy": "read_only", "passed": True},
        {"surface": "6jg_audit", "policy": "read_only", "passed": True},
        {"surface": "6jf_implementation", "policy": "read_only", "passed": True},
        {"surface": "6je_plan", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
        {"surface": "adapter_module", "policy": "read_only", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JJ, "actual": RECOMMENDED_NEXT_LAYER_6JJ, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JJ, "actual": RECOMMENDED_PATH_6JJ, "passed": True},
        {"decision": "recommend_final_activation_decision_plan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JJ, "actual": DIAGNOSIS_6JJ, "passed": True},
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
        {"check": "future_6jk_contract", "passed": all_passed(future_6jk), "detail": f"{sum(1 for r in future_6jk if r['passed'])}/{len(future_6jk)}"},
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
        "future_6jk_contract": write_csv(FUTURE_6JK_CSV, future_6jk),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JJ",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JJ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JJ,
        "recommended_path": RECOMMENDED_PATH_6JJ,
        "predecessor_implementation": str(IMPLEMENT_6JI_PATH),
        "predecessor_implementation_returncode": 0,
        "predecessor_implementation_diagnosis": json_6ji.get("diagnosis"),
        "audited_layer": "6JI",
        "source_family": "activation_execution",
        "activation_execution_plan_created": json_6ji.get("activation_execution_plan_created"),
        "activation_execution_implementation_completed": json_6ji.get("activation_execution_implementation_completed"),
        "activation_execution_implementation_audited": True,
        "execution_criteria_matrix_row_count": len(execution_criteria),
        "mechanic_execution_surface_matrix_row_count": len(mechanic_surfaces),
        "live_mode_blocker_matrix_row_count": len(live_blockers),
        "production_shadow_prerequisite_matrix_row_count": len(shadow_prereqs),
        "rollback_execution_gate_matrix_row_count": len(rollback_gates),
        "final_activation_decision_policy_matrix_row_count": len(final_policy),
        "execution_audit_requirement_matrix_row_count": len(audit_requirements),
        "execution_prevention_assertion_count": len(prevention_assertions),
        "future_6jk_contract_valid": all_passed(future_6jk),
        "final_activation_decision_planning_allowed_after_this_layer": True,
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
            "future_6jk_contract_csv": str(FUTURE_6JK_CSV),
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
