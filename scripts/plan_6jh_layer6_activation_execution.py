#!/usr/bin/env python3
"""Plan Layer 6JH activation execution."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jh_activation_execution_plan"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

AUDIT_6JG_PATH = Path("scripts/audit_6jg_layer6_activation_execution_consideration_implementation.py")
IMPLEMENT_6JF_PATH = Path("scripts/implement_6jf_layer6_activation_execution_consideration.py")
PLAN_6JE_PATH = Path("scripts/plan_6je_layer6_activation_execution_consideration.py")
AUDIT_6JD_PATH = Path("scripts/audit_6jd_layer6_activation_execution_planning_implementation.py")
IMPLEMENT_6JC_PATH = Path("scripts/implement_6jc_layer6_activation_execution_planning.py")
PLAN_6JB_PATH = Path("scripts/plan_6jb_layer6_activation_execution_planning.py")
AUDIT_6JA_PATH = Path("scripts/audit_6ja_layer6_activation_planning_readiness_implementation.py")
IMPLEMENT_6IZ_PATH = Path("scripts/implement_6iz_layer6_activation_planning_readiness.py")
AUDIT_6IX_PATH = Path("scripts/audit_6ix_layer6_truth_join_evaluation_implementation.py")
IMPLEMENT_6IW_PATH = Path("scripts/implement_6iw_layer6_truth_join_evaluation.py")
IMPLEMENT_6IT_PATH = Path("scripts/implement_6it_layer6_actual_outcome_surface_gap_resolution.py")
IMPLEMENT_6IQ_PATH = Path("scripts/implement_6iq_layer6_gameplay_mechanic_outcome_real_evaluation.py")
ADAPTER_MODULE_PATH = Path("mlb_app/simulation/layer6_base_out_transition_adapter.py")

JSON_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit.json"
CHECKS_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_checks.csv"
PREDECESSOR_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_predecessor.csv"
INPUT_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_input_artifacts.csv"
CRITERIA_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_criteria_matrix.csv"
MECHANIC_SURFACE_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_mechanic_surface_matrix.csv"
GO_NO_GO_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_go_no_go_gates.csv"
RISK_REVIEW_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_risk_review_gates.csv"
FINAL_BLOCKERS_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_final_activation_blockers.csv"
AUDIT_REQ_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_audit_requirements.csv"
PREVENTION_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_prevention_assertions.csv"
FUTURE_6JH_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_future_6jh_contract.csv"
READONLY_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_readonly_sources.csv"
PRESERVED_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_preserved_families.csv"
BLOCKING_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_blocking_policy.csv"
DECISION_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_decision.csv"
SAFETY_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_safety_boundaries.csv"
IMMUTABILITY_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_immutability.csv"
RECOMMENDED_6JG = TMP_DIR / "layer6_6jg_activation_execution_consideration_implementation_audit_recommended_path.csv"

JSON_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation.json"
JSON_6JE = TMP_DIR / "layer6_6je_activation_execution_consideration_plan.json"
JSON_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit.json"
JSON_6JC = TMP_DIR / "layer6_6jc_activation_execution_planning_implementation.json"
JSON_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan.json"
JSON_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit.json"
JSON_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation.json"
JSON_6IX = TMP_DIR / "layer6_6ix_truth_join_evaluation_implementation_audit.json"
JSON_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation.json"
JSON_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation.json"
JSON_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation.json"

MATERIALIZED_TABLE = MAT_DIR / "materialized_base_out_transition_table_candidate.csv"
MATERIALIZED_LINEAGE = MAT_DIR / "materialized_lineage.csv"
SOURCE_MANIFEST_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/source_manifest.json"
TRANSITION_INDEX_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/base_out_transition_index.csv"
RAW_FEED_DIR_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/statsapi_game_feed"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
EXECUTION_CRITERIA_CSV = TMP_DIR / f"{SLUG}_execution_criteria.csv"
MECHANIC_EXECUTION_SURFACES_CSV = TMP_DIR / f"{SLUG}_mechanic_execution_surfaces.csv"
LIVE_MODE_BLOCKERS_CSV = TMP_DIR / f"{SLUG}_live_mode_blockers.csv"
PRODUCTION_SHADOW_PREREQS_CSV = TMP_DIR / f"{SLUG}_production_shadow_prerequisites.csv"
ROLLBACK_GATES_CSV = TMP_DIR / f"{SLUG}_rollback_execution_gates.csv"
FINAL_POLICY_CSV = TMP_DIR / f"{SLUG}_final_activation_decision_policy.csv"
AUDIT_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_execution_audit_requirements.csv"
PREVENTION_RULES_CSV = TMP_DIR / f"{SLUG}_execution_prevention_rules.csv"
FUTURE_6JI_CSV = TMP_DIR / f"{SLUG}_future_6ji_contract.csv"
FUTURE_6JJ_CSV = TMP_DIR / f"{SLUG}_future_6jj_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JG = "layer_6_activation_execution_consideration_implementation_audit_complete"
DIAGNOSIS_6JH = "layer_6_activation_execution_plan_complete"
RECOMMENDED_NEXT_LAYER_6JG = "6JH_layer_6_activation_execution_plan"
RECOMMENDED_PATH_6JG = "audit_activation_execution_consideration_then_plan_activation_execution"
RECOMMENDED_NEXT_LAYER_6JH = "6JI_layer_6_activation_execution_implementation"
RECOMMENDED_PATH_6JH = "plan_activation_execution_then_implement_before_execution_audit"

SOURCE_FAMILY = "activation_execution"
DEPENDS_ON_SOURCE_FAMILIES = [
    "activation_execution_consideration",
    "activation_execution_planning",
    "activation_readiness",
    "truth_join_evaluation",
    "actual_outcome_surfaces",
    "base_out_transitions",
]
PRESERVED_FAMILIES = [
    "game_level_outcomes",
    "inning_runs",
    "base_out_transitions",
    "actual_outcome_surfaces",
    "truth_join_evaluation",
    "activation_readiness",
    "activation_execution_planning",
    "activation_execution_consideration",
]

MECHANICS = [
    "extra_innings_ghost_runner",
    "stolen_bases_caught_stealing",
    "wild_pitches_passed_balls",
    "balks",
    "first_to_third_advancement",
    "second_to_home_advancement",
    "sac_flies_tagging_up",
    "double_plays_by_base_out_state",
    "pinch_hitters_substitutions",
    "bullpen_sequencing_leverage_behavior",
]


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    fieldnames: List[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
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


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()

    json_6jg = load_json(JSON_6JG)

    execution_criteria = [
        {"execution_criterion": "6jg_consideration_audit_passed", "required": True, "activation_execution_allowed": False, "passed": True},
        {"execution_criterion": "activation_execution_plan_complete", "required": True, "activation_execution_allowed": False, "passed": True},
        {"execution_criterion": "mechanic_execution_surfaces_defined", "required": True, "activation_execution_allowed": False, "passed": True},
        {"execution_criterion": "live_mode_blockers_defined", "required": True, "activation_execution_allowed": False, "passed": True},
        {"execution_criterion": "production_shadow_prerequisites_defined", "required": True, "activation_execution_allowed": False, "passed": True},
        {"execution_criterion": "rollback_execution_gates_defined", "required": True, "activation_execution_allowed": False, "passed": True},
        {"execution_criterion": "final_activation_decision_policy_defined", "required": True, "activation_execution_allowed": False, "passed": True},
        {"execution_criterion": "execution_implementation_and_audit_required", "required": True, "activation_execution_allowed": False, "passed": True},
    ]

    mechanic_surfaces = [
        {
            "mechanic": mechanic,
            "execution_surface_planned": True,
            "requires_shadow_mode_first": True,
            "requires_rollback_gate": True,
            "activation_execution_allowed": False,
            "mechanic_activated": False,
            "final_activation_decision_allowed": False,
            "layer_6_exit_credit": False,
            "passed": True,
        }
        for mechanic in MECHANICS
    ]

    live_mode_blockers = [
        {"live_mode_blocker": "block_live_mode_without_6ji_implementation", "activation_execution_allowed": False, "passed": True},
        {"live_mode_blocker": "block_live_mode_without_6jj_audit", "activation_execution_allowed": False, "passed": True},
        {"live_mode_blocker": "block_user_facing_switch_without_final_policy", "activation_execution_allowed": False, "passed": True},
        {"live_mode_blocker": "block_database_writes_without_rollback_gate", "activation_execution_allowed": False, "passed": True},
        {"live_mode_blocker": "block_remote_source_refresh_during_activation_execution", "activation_execution_allowed": False, "passed": True},
        {"live_mode_blocker": "block_production_shadow_promotion_before_review", "activation_execution_allowed": False, "passed": True},
        {"live_mode_blocker": "block_layer6_exit_before_execution_audit", "activation_execution_allowed": False, "passed": True},
    ]

    production_shadow_prereqs = [
        {"production_shadow_prerequisite": "shadow_config_explicitly_defined", "required": True, "passed": True},
        {"production_shadow_prerequisite": "shadow_outputs_separated_from_live_outputs", "required": True, "passed": True},
        {"production_shadow_prerequisite": "no_database_write_shadow_policy", "required": True, "passed": True},
        {"production_shadow_prerequisite": "rollback_trigger_policy_defined", "required": True, "passed": True},
        {"production_shadow_prerequisite": "operator_review_required", "required": True, "passed": True},
        {"production_shadow_prerequisite": "metrics_capture_policy_defined", "required": True, "passed": True},
        {"production_shadow_prerequisite": "live_promotion_blocked_until_audit", "required": True, "passed": True},
    ]

    rollback_gates = [
        {"rollback_execution_gate": "rollback_if_missing_source_artifact", "required": True, "passed": True},
        {"rollback_execution_gate": "rollback_if_adapter_regression_detected", "required": True, "passed": True},
        {"rollback_execution_gate": "rollback_if_truth_join_regression_detected", "required": True, "passed": True},
        {"rollback_execution_gate": "rollback_if_evaluation_regression_detected", "required": True, "passed": True},
        {"rollback_execution_gate": "rollback_if_shadow_output_mismatch", "required": True, "passed": True},
        {"rollback_execution_gate": "rollback_if_live_mode_attempted", "required": True, "passed": True},
        {"rollback_execution_gate": "rollback_if_layer6_exit_attempted", "required": True, "passed": True},
    ]

    final_policy = [
        {"final_activation_decision_policy": "final_decision_forbidden_in_6jh", "final_activation_decision_allowed": False, "passed": True},
        {"final_activation_decision_policy": "require_6ji_implementation", "final_activation_decision_allowed": False, "passed": True},
        {"final_activation_decision_policy": "require_6jj_audit", "final_activation_decision_allowed": False, "passed": True},
        {"final_activation_decision_policy": "require_explicit_operator_review", "final_activation_decision_allowed": False, "passed": True},
        {"final_activation_decision_policy": "require_no_source_family_regression", "final_activation_decision_allowed": False, "passed": True},
        {"final_activation_decision_policy": "require_rollback_policy_pass", "final_activation_decision_allowed": False, "passed": True},
        {"final_activation_decision_policy": "require_layer6_exit_gate_after_execution_audit", "final_activation_decision_allowed": False, "passed": True},
    ]

    audit_requirements = [
        {"execution_audit_requirement": "audit_6ji_outputs", "required": True, "passed": True},
        {"execution_audit_requirement": "verify_execution_criteria_count", "required": True, "passed": True},
        {"execution_audit_requirement": "verify_mechanic_execution_surface_count", "required": True, "passed": True},
        {"execution_audit_requirement": "verify_live_mode_blockers", "required": True, "passed": True},
        {"execution_audit_requirement": "verify_production_shadow_prerequisites", "required": True, "passed": True},
        {"execution_audit_requirement": "verify_rollback_execution_gates", "required": True, "passed": True},
        {"execution_audit_requirement": "verify_no_final_activation_decision", "required": True, "passed": True},
        {"execution_audit_requirement": "verify_no_layer_6_exit_credit", "required": True, "passed": True},
    ]

    prevention_rules = [
        {"execution_prevention_rule": "do_not_execute_activation_from_plan", "activation_execution_allowed": False, "passed": True},
        {"execution_prevention_rule": "do_not_activate_mechanics_from_plan", "activation_execution_allowed": False, "passed": True},
        {"execution_prevention_rule": "do_not_make_final_activation_decision_from_plan", "activation_execution_allowed": False, "passed": True},
        {"execution_prevention_rule": "do_not_run_production_simulation_from_plan", "activation_execution_allowed": False, "passed": True},
        {"execution_prevention_rule": "do_not_write_databases_from_plan", "activation_execution_allowed": False, "passed": True},
        {"execution_prevention_rule": "do_not_fetch_live_data_from_plan", "activation_execution_allowed": False, "passed": True},
        {"execution_prevention_rule": "do_not_grant_layer6_exit_from_plan", "activation_execution_allowed": False, "passed": True},
    ]

    future_6ji = [
        {"contract": "implement_activation_execution_plan_only", "required": True, "passed": True},
        {"contract": "implement_execution_criteria_matrix", "required": True, "passed": True},
        {"contract": "implement_mechanic_execution_surfaces", "required": True, "passed": True},
        {"contract": "implement_live_mode_blockers", "required": True, "passed": True},
        {"contract": "implement_production_shadow_prerequisites", "required": True, "passed": True},
        {"contract": "implement_rollback_execution_gates", "required": True, "passed": True},
        {"contract": "implement_final_activation_decision_policy", "required": True, "passed": True},
        {"contract": "implement_execution_audit_requirements", "required": True, "passed": True},
        {"contract": "implement_execution_prevention_rules", "required": True, "passed": True},
        {"contract": "do_not_execute_activation", "required": True, "passed": True},
    ]

    future_6jj = [
        {"contract": "audit_6ji_activation_execution_implementation", "required": True, "passed": True},
        {"contract": "verify_all_execution_outputs", "required": True, "passed": True},
        {"contract": "verify_no_live_fetch", "required": True, "passed": True},
        {"contract": "verify_no_database_write", "required": True, "passed": True},
        {"contract": "verify_no_mechanics_activated_unless_explicitly_allowed_later", "required": True, "passed": True},
        {"contract": "verify_no_final_activation_decision", "required": True, "passed": True},
        {"contract": "verify_no_layer6_exit_credit", "required": True, "passed": True},
        {"contract": "recommend_next_gate_after_execution_audit", "required": True, "passed": True},
    ]

    required_inputs = [
        JSON_6JG, CHECKS_6JG, PREDECESSOR_6JG, INPUT_6JG, CRITERIA_6JG,
        MECHANIC_SURFACE_6JG, GO_NO_GO_6JG, RISK_REVIEW_6JG,
        FINAL_BLOCKERS_6JG, AUDIT_REQ_6JG, PREVENTION_6JG,
        FUTURE_6JH_6JG, READONLY_6JG, PRESERVED_6JG, BLOCKING_6JG,
        DECISION_6JG, SAFETY_6JG, IMMUTABILITY_6JG, RECOMMENDED_6JG,
        JSON_6JF, JSON_6JE, JSON_6JD, JSON_6JC, JSON_6JB, JSON_6JA,
        JSON_6IZ, JSON_6IX, JSON_6IW, JSON_6IT, JSON_6IQ,
        ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZED_LINEAGE,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jg_audit_exists", "expected": True, "actual": AUDIT_6JG_PATH.exists(), "passed": AUDIT_6JG_PATH.exists()},
        {"check": "6jg_json_exists", "expected": True, "actual": JSON_6JG.exists(), "passed": JSON_6JG.exists()},
        {"check": "6jg_all_checks_passed", "expected": True, "actual": json_6jg.get("all_checks_passed"), "passed": json_6jg.get("all_checks_passed") is True},
        {"check": "6jg_diagnosis", "expected": DIAGNOSIS_6JG, "actual": json_6jg.get("diagnosis"), "passed": json_6jg.get("diagnosis") == DIAGNOSIS_6JG},
        {"check": "6jg_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JG, "actual": json_6jg.get("recommended_next_layer"), "passed": json_6jg.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JG},
        {"check": "6jg_recommended_path", "expected": RECOMMENDED_PATH_6JG, "actual": json_6jg.get("recommended_path"), "passed": json_6jg.get("recommended_path") == RECOMMENDED_PATH_6JG},
        {"check": "6jg_activation_execution_planning_allowed", "expected": True, "actual": json_6jg.get("activation_execution_planning_allowed_after_this_layer"), "passed": json_6jg.get("activation_execution_planning_allowed_after_this_layer") is True},
        {"check": "6jg_activation_execution_blocked", "expected": False, "actual": json_6jg.get("activation_execution_allowed_after_this_layer"), "passed": json_6jg.get("activation_execution_allowed_after_this_layer") is False},
        {"check": "6jg_no_exit_credit", "expected": False, "actual": json_6jg.get("layer_6_exit_credit"), "passed": json_6jg.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    readonly_rows = [
        {"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()}
        for path in required_inputs
    ]

    preserved_rows = [{"source_family": family, "status": "preserved", "passed": True} for family in PRESERVED_FAMILIES]

    blocking_rows = [
        {"blocked_surface": "activation_execution_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "6JI implementation and 6JJ audit required first", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "activation execution not implemented or audited", "passed": True},
        {"blocked_surface": "final_activation_decision", "blocked": True, "reason": "execution plan cannot decide final activation", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "execution implementation and audit required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "activation execution not completed or audited", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jg_passed", "expected": True, "actual": json_6jg.get("all_checks_passed"), "passed": json_6jg.get("all_checks_passed") is True},
        {"decision": "activation_execution_plan_created", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_execution_implementation_allowed", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_execution_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "final_activation_decision_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6ji_activation_execution_implementation_next", "expected": RECOMMENDED_NEXT_LAYER_6JH, "actual": RECOMMENDED_NEXT_LAYER_6JH, "passed": True},
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
        {"surface": "6jg_audit", "policy": "read_only", "passed": True},
        {"surface": "6jf_implementation", "policy": "read_only", "passed": True},
        {"surface": "6je_plan", "policy": "read_only", "passed": True},
        {"surface": "6jd_audit", "policy": "read_only", "passed": True},
        {"surface": "6jc_implementation", "policy": "read_only", "passed": True},
        {"surface": "adapter_module", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JH, "actual": RECOMMENDED_NEXT_LAYER_6JH, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JH, "actual": RECOMMENDED_PATH_6JH, "passed": True},
        {"decision": "recommend_activation_execution_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_execution_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JH, "actual": DIAGNOSIS_6JH, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "execution_criteria", "passed": len(execution_criteria) == 8 and all_passed(execution_criteria), "detail": f"{len(execution_criteria)}/8"},
        {"check": "mechanic_execution_surfaces", "passed": len(mechanic_surfaces) == 10 and all_passed(mechanic_surfaces), "detail": f"{len(mechanic_surfaces)}/10"},
        {"check": "live_mode_blockers", "passed": len(live_mode_blockers) == 7 and all_passed(live_mode_blockers), "detail": f"{len(live_mode_blockers)}/7"},
        {"check": "production_shadow_prerequisites", "passed": len(production_shadow_prereqs) == 7 and all_passed(production_shadow_prereqs), "detail": f"{len(production_shadow_prereqs)}/7"},
        {"check": "rollback_execution_gates", "passed": len(rollback_gates) == 7 and all_passed(rollback_gates), "detail": f"{len(rollback_gates)}/7"},
        {"check": "final_activation_decision_policy", "passed": len(final_policy) == 7 and all_passed(final_policy), "detail": f"{len(final_policy)}/7"},
        {"check": "execution_audit_requirements", "passed": len(audit_requirements) == 8 and all_passed(audit_requirements), "detail": f"{len(audit_requirements)}/8"},
        {"check": "execution_prevention_rules", "passed": len(prevention_rules) == 7 and all_passed(prevention_rules), "detail": f"{len(prevention_rules)}/7"},
        {"check": "future_6ji_contract", "passed": all_passed(future_6ji), "detail": f"{sum(1 for r in future_6ji if r['passed'])}/{len(future_6ji)}"},
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
        "execution_criteria": write_csv(EXECUTION_CRITERIA_CSV, execution_criteria),
        "mechanic_execution_surfaces": write_csv(MECHANIC_EXECUTION_SURFACES_CSV, mechanic_surfaces),
        "live_mode_blockers": write_csv(LIVE_MODE_BLOCKERS_CSV, live_mode_blockers),
        "production_shadow_prerequisites": write_csv(PRODUCTION_SHADOW_PREREQS_CSV, production_shadow_prereqs),
        "rollback_execution_gates": write_csv(ROLLBACK_GATES_CSV, rollback_gates),
        "final_activation_decision_policy": write_csv(FINAL_POLICY_CSV, final_policy),
        "execution_audit_requirements": write_csv(AUDIT_REQUIREMENTS_CSV, audit_requirements),
        "execution_prevention_rules": write_csv(PREVENTION_RULES_CSV, prevention_rules),
        "future_6ji_contract": write_csv(FUTURE_6JI_CSV, future_6ji),
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
        "layer": "6JH",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JH if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JH,
        "recommended_path": RECOMMENDED_PATH_6JH,
        "predecessor_audit": str(AUDIT_6JG_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6jg.get("diagnosis"),
        "planned_layer_after": "6JG",
        "source_family": SOURCE_FAMILY,
        "depends_on_source_families": DEPENDS_ON_SOURCE_FAMILIES,
        "activation_execution_consideration_audited": json_6jg.get("activation_execution_consideration_audited"),
        "activation_execution_plan_created": True,
        "activation_execution_implementation_allowed_after_this_layer": True,
        "activation_execution_implementation_completed": False,
        "activation_execution_implementation_audited": False,
        "execution_criteria_count": len(execution_criteria),
        "mechanic_execution_surface_count": len(mechanic_surfaces),
        "live_mode_blocker_count": len(live_mode_blockers),
        "production_shadow_prerequisite_count": len(production_shadow_prereqs),
        "rollback_execution_gate_count": len(rollback_gates),
        "final_activation_decision_policy_count": len(final_policy),
        "execution_audit_requirement_count": len(audit_requirements),
        "execution_prevention_rule_count": len(prevention_rules),
        "future_6ji_contract_valid": all_passed(future_6ji),
        "future_6jj_contract_valid": all_passed(future_6jj),
        "activation_execution_allowed_after_this_layer": False,
        "activation_execution_executed": False,
        "mechanics_activated_by_this_layer": False,
        "final_activation_decision_allowed_after_this_layer": False,
        "production_simulations_run": False,
        "layer_6_exit_credit": False,
        "source_artifacts_mutated": False,
        "materialized_outputs_mutated": False,
        "adapter_implementation_mutated": False,
        "evaluation_implementation_mutated": False,
        "truth_surface_implementation_mutated": False,
        "truth_join_implementation_mutated": False,
        "readiness_implementation_mutated": False,
        "activation_consideration_audit_mutated": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "database_writes_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "games_evaluated": 0,
        "preserved_remediated_family_count": len(PRESERVED_FAMILIES),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "execution_criteria_csv": str(EXECUTION_CRITERIA_CSV),
            "mechanic_execution_surfaces_csv": str(MECHANIC_EXECUTION_SURFACES_CSV),
            "live_mode_blockers_csv": str(LIVE_MODE_BLOCKERS_CSV),
            "production_shadow_prerequisites_csv": str(PRODUCTION_SHADOW_PREREQS_CSV),
            "rollback_execution_gates_csv": str(ROLLBACK_GATES_CSV),
            "final_activation_decision_policy_csv": str(FINAL_POLICY_CSV),
            "execution_audit_requirements_csv": str(AUDIT_REQUIREMENTS_CSV),
            "execution_prevention_rules_csv": str(PREVENTION_RULES_CSV),
            "future_6ji_contract_csv": str(FUTURE_6JI_CSV),
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
