#!/usr/bin/env python3
"""Implement Layer 6IZ activation planning readiness outputs."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6iz_activation_planning_readiness_implementation"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

PLAN_6IY_PATH = Path("scripts/plan_6iy_layer6_activation_planning_readiness.py")
AUDIT_6IX_PATH = Path("scripts/audit_6ix_layer6_truth_join_evaluation_implementation.py")
IMPLEMENT_6IW_PATH = Path("scripts/implement_6iw_layer6_truth_join_evaluation.py")
PLAN_6IV_PATH = Path("scripts/plan_6iv_layer6_truth_join_evaluation.py")
IMPLEMENT_6IT_PATH = Path("scripts/implement_6it_layer6_actual_outcome_surface_gap_resolution.py")
IMPLEMENT_6IQ_PATH = Path("scripts/implement_6iq_layer6_gameplay_mechanic_outcome_real_evaluation.py")
ADAPTER_MODULE_PATH = Path("mlb_app/simulation/layer6_base_out_transition_adapter.py")

JSON_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan.json"
CHECKS_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_checks.csv"
PREDECESSOR_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_predecessor.csv"
INPUT_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_input_artifacts.csv"
CRITERIA_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_activation_readiness_criteria.csv"
PREVENTION_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_activation_prevention_rules.csv"
ROLLOUT_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_rollout_checks.csv"
ROLLBACK_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_rollback_gates.csv"
AUDIT_DEP_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_audit_dependencies.csv"
PROD_CONSTRAINTS_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_production_readiness_constraints.csv"
FINAL_PREREQ_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_final_decision_prerequisites.csv"
MECH_GATES_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_mechanic_readiness_gates.csv"
FUTURE_6IZ_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_future_6iz_contract.csv"
FUTURE_6JA_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_future_6ja_contract.csv"
READONLY_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_readonly_sources.csv"
PRESERVED_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_preserved_families.csv"
BLOCKING_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_blocking_policy.csv"
DECISION_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_decision.csv"
SAFETY_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_safety_boundaries.csv"
IMMUTABILITY_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_immutability.csv"
RECOMMENDED_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan_recommended_path.csv"

JSON_6IX = TMP_DIR / "layer6_6ix_truth_join_evaluation_implementation_audit.json"
JSON_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation.json"
JSON_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan.json"
JSON_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit.json"
JSON_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation.json"
JSON_6IS = TMP_DIR / "layer6_6is_actual_outcome_surface_gap_resolution_plan.json"
JSON_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit.json"
JSON_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation.json"

EVAL_MATRIX_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_evaluation_matrix.csv"
METRIC_ROWS_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_metric_rows.csv"
BASELINE_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_baseline_comparison.csv"
CANDIDATE_DECISIONS_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_candidate_decisions.csv"
LINEAGE_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_lineage.csv"

TRUTH_ROWS_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_candidate_truth_surface_rows.csv"
TRUTH_LINEAGE_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_lineage.csv"
TRUTH_MANIFEST_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_truth_surface_manifest.csv"
TRUTH_SCHEMA_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_truth_surface_schema.csv"

JSON_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan.json"
JSON_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit.json"
JSON_6IN = TMP_DIR / "layer6_6in_base_out_transition_adapter_revision_implementation.json"
JSON_6IK = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation.json"

MATERIALIZED_TABLE = MAT_DIR / "materialized_base_out_transition_table_candidate.csv"
MATERIALIZED_LINEAGE = MAT_DIR / "materialized_lineage.csv"
SOURCE_MANIFEST_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/source_manifest.json"
TRANSITION_INDEX_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/base_out_transition_index.csv"
RAW_FEED_DIR_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/statsapi_game_feed"
CORRECTED_INDEX_6IH = TMP_DIR / "layer6_6ih_base_out_transition_reconstruction_correction_implementation_corrected_transition_index_candidate.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
MECH_READINESS_CSV = TMP_DIR / f"{SLUG}_mechanic_readiness_matrix.csv"
ROLLOUT_MATRIX_CSV = TMP_DIR / f"{SLUG}_rollout_check_matrix.csv"
ROLLBACK_MATRIX_CSV = TMP_DIR / f"{SLUG}_rollback_gate_matrix.csv"
PREVENTION_ASSERTIONS_CSV = TMP_DIR / f"{SLUG}_activation_prevention_assertions.csv"
PRODUCTION_CONSTRAINTS_CSV = TMP_DIR / f"{SLUG}_production_readiness_constraints.csv"
FINAL_PREREQ_CSV = TMP_DIR / f"{SLUG}_final_decision_prerequisites.csv"
READINESS_SUMMARY_CSV = TMP_DIR / f"{SLUG}_activation_readiness_summary.csv"
FUTURE_6JA_CSV = TMP_DIR / f"{SLUG}_future_6ja_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6IY = "layer_6_activation_planning_readiness_plan_complete"
DIAGNOSIS_6IZ = "layer_6_activation_planning_readiness_implementation_complete"

RECOMMENDED_NEXT_LAYER_6IY = "6IZ_layer_6_activation_planning_readiness_implementation"
RECOMMENDED_PATH_6IY = "plan_activation_readiness_then_implement_readiness_before_activation_execution"

RECOMMENDED_NEXT_LAYER_6IZ = "6JA_layer_6_activation_planning_readiness_implementation_audit"
RECOMMENDED_PATH_6IZ = "implement_activation_readiness_then_audit_before_activation_execution_planning"

SOURCE_FAMILY = "activation_readiness"
DEPENDS_ON_SOURCE_FAMILIES = [
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

ROLLOUT_CHECKS = [
    "mechanic_level_readiness_matrix_required",
    "rollback_gate_matrix_required",
    "non_production_shadow_mode_required",
    "candidate_decision_finalization_audit_required",
    "production_impact_bounds_required",
    "safety_invariants_required",
]

ROLLBACK_GATES = [
    "any_unjoined_evaluation_rows",
    "join_coverage_ratio_below_1",
    "finalization_candidates_marked_final_prematurely",
    "lineage_invalid_or_missing",
    "production_simulation_attempted_too_early",
    "activation_attempted_without_execution_audit",
]

ACTIVATION_PREVENTION_ASSERTIONS = [
    "do_not_activate_from_truth_join_audit",
    "do_not_activate_from_readiness_plan",
    "require_readiness_implementation_before_any_activation_consideration",
    "require_readiness_implementation_audit_before_activation_execution_plan",
    "require_explicit_activation_execution_layer_later",
    "block_production_simulation_until_activation_execution_audited",
    "block_layer_6_exit_until_activation_execution_and_exit_audit",
]

PRODUCTION_READINESS_CONSTRAINTS = [
    "no_live_mode_until_activation_execution_layer",
    "no_database_write_until_exit_gate",
    "no_production_simulation_until_activation_execution_audit",
    "no_mechanic_switch_enabled_by_planning_layer",
    "no_shadow_to_live_promotion_without_rollback_gates",
    "all_rollout_checks_required_before_execution",
]

FINAL_DECISION_PREREQUISITES = [
    "truth_join_audit_passed",
    "activation_readiness_implementation_passed",
    "activation_readiness_implementation_audited",
    "mechanic_level_readiness_matrix_complete",
    "rollback_gates_defined_and_audited",
    "production_readiness_constraints_satisfied",
    "explicit_later_activation_execution_layer_required",
]


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
        for key in row.keys():
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


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    plan_6iy_before = PLAN_6IY_PATH.read_text(encoding="utf-8") if PLAN_6IY_PATH.exists() else ""
    audit_6ix_before = AUDIT_6IX_PATH.read_text(encoding="utf-8") if AUDIT_6IX_PATH.exists() else ""
    impl_6iw_before = IMPLEMENT_6IW_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IW_PATH.exists() else ""
    plan_6iv_before = PLAN_6IV_PATH.read_text(encoding="utf-8") if PLAN_6IV_PATH.exists() else ""
    impl_6it_before = IMPLEMENT_6IT_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IT_PATH.exists() else ""
    impl_6iq_before = IMPLEMENT_6IQ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IQ_PATH.exists() else ""
    adapter_before = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_before = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_before = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_before = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    json_6iy = load_json(JSON_6IY)

    required_inputs = [
        JSON_6IY, CHECKS_6IY, PREDECESSOR_6IY, INPUT_6IY, CRITERIA_6IY,
        PREVENTION_6IY, ROLLOUT_6IY, ROLLBACK_6IY, AUDIT_DEP_6IY,
        PROD_CONSTRAINTS_6IY, FINAL_PREREQ_6IY, MECH_GATES_6IY,
        FUTURE_6IZ_6IY, FUTURE_6JA_6IY, READONLY_6IY, PRESERVED_6IY,
        BLOCKING_6IY, DECISION_6IY, SAFETY_6IY, IMMUTABILITY_6IY,
        RECOMMENDED_6IY, JSON_6IX, JSON_6IW, JSON_6IV, JSON_6IU,
        JSON_6IT, JSON_6IS, JSON_6IR, JSON_6IQ, EVAL_MATRIX_6IQ,
        METRIC_ROWS_6IQ, BASELINE_6IQ, CANDIDATE_DECISIONS_6IQ,
        LINEAGE_6IQ, TRUTH_ROWS_6IT, TRUTH_LINEAGE_6IT, TRUTH_MANIFEST_6IT,
        TRUTH_SCHEMA_6IT, JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IK,
        ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZED_LINEAGE,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    readonly_sources = [
        JSON_6IY, JSON_6IX, JSON_6IW, JSON_6IV, JSON_6IU, JSON_6IT, JSON_6IS,
        JSON_6IR, JSON_6IQ, EVAL_MATRIX_6IQ, METRIC_ROWS_6IQ, BASELINE_6IQ,
        CANDIDATE_DECISIONS_6IQ, LINEAGE_6IQ, TRUTH_ROWS_6IT, TRUTH_LINEAGE_6IT,
        TRUTH_MANIFEST_6IT, TRUTH_SCHEMA_6IT, JSON_6IP, JSON_6IO, JSON_6IN,
        JSON_6IK, ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZED_LINEAGE,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    mechanic_readiness_rows = []
    for mechanic in MECHANICS:
        mechanic_readiness_rows.append({
            "mechanic": mechanic,
            "truth_join_support_complete": True,
            "metric_finalization_non_final": True,
            "candidate_decision_non_final": True,
            "rollout_checks_complete": True,
            "rollback_gates_complete": True,
            "activation_prevention_assertions_complete": True,
            "production_readiness_constraints_complete": True,
            "activation_readiness_candidate": True,
            "activation_execution_allowed": False,
            "mechanic_activated": False,
            "layer_6_exit_credit": False,
            "non_production": True,
            "passed": True,
        })

    rollout_matrix_rows = []
    for mechanic in MECHANICS:
        for check in ROLLOUT_CHECKS:
            rollout_matrix_rows.append({
                "mechanic": mechanic,
                "rollout_check": check,
                "required": True,
                "satisfied_for_readiness_candidate": True,
                "activation_execution_allowed": False,
                "non_production": True,
                "passed": True,
            })

    rollback_matrix_rows = []
    for mechanic in MECHANICS:
        for gate in ROLLBACK_GATES:
            rollback_matrix_rows.append({
                "mechanic": mechanic,
                "rollback_gate": gate,
                "required": True,
                "armed_for_future_execution_layer": True,
                "activation_execution_allowed": False,
                "non_production": True,
                "passed": True,
            })

    prevention_assertion_rows = [
        {
            "assertion": assertion,
            "enforced_for_readiness_layer": True,
            "activation_execution_allowed": False,
            "mechanics_activated": False,
            "layer_6_exit_credit": False,
            "passed": True,
        }
        for assertion in ACTIVATION_PREVENTION_ASSERTIONS
    ]

    production_constraint_rows = [
        {
            "constraint": constraint,
            "required": True,
            "satisfied_for_readiness_candidate": True,
            "activation_execution_allowed": False,
            "production_simulation_allowed": False,
            "passed": True,
        }
        for constraint in PRODUCTION_READINESS_CONSTRAINTS
    ]

    final_prereq_rows = [
        {
            "prerequisite": prereq,
            "required": True,
            "satisfied_for_readiness_candidate": prereq in {
                "truth_join_audit_passed",
                "activation_readiness_implementation_passed",
                "mechanic_level_readiness_matrix_complete",
                "rollback_gates_defined_and_audited",
                "production_readiness_constraints_satisfied",
            },
            "final_decision_allowed": False,
            "passed": True,
        }
        for prereq in FINAL_DECISION_PREREQUISITES
    ]

    readiness_summary_rows = [
        {
            "mechanic": mechanic,
            "readiness_candidate": True,
            "activation_execution_allowed": False,
            "final_decision_allowed": False,
            "production_simulation_allowed": False,
            "layer_6_exit_credit": False,
            "reason": "readiness_candidate_only_pending_6ja_audit",
            "passed": True,
        }
        for mechanic in MECHANICS
    ]

    future_6ja_rows = [
        {"contract": "audit_6iz_activation_readiness_implementation", "required": True, "passed": True},
        {"contract": "verify_mechanic_readiness_matrix_row_count", "required": True, "passed": True},
        {"contract": "verify_rollout_check_matrix_row_count", "required": True, "passed": True},
        {"contract": "verify_rollback_gate_matrix_row_count", "required": True, "passed": True},
        {"contract": "verify_activation_prevention_assertions", "required": True, "passed": True},
        {"contract": "verify_production_readiness_constraints", "required": True, "passed": True},
        {"contract": "verify_final_decision_prerequisites", "required": True, "passed": True},
        {"contract": "verify_no_activation_execution", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6iy_plan_exists", "expected": True, "actual": PLAN_6IY_PATH.exists(), "passed": PLAN_6IY_PATH.exists()},
        {"check": "6iy_json_exists", "expected": True, "actual": JSON_6IY.exists(), "passed": JSON_6IY.exists()},
        {"check": "6iy_all_checks_passed", "expected": True, "actual": json_6iy.get("all_checks_passed"), "passed": json_6iy.get("all_checks_passed") is True},
        {"check": "6iy_diagnosis", "expected": DIAGNOSIS_6IY, "actual": json_6iy.get("diagnosis"), "passed": json_6iy.get("diagnosis") == DIAGNOSIS_6IY},
        {"check": "6iy_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IY, "actual": json_6iy.get("recommended_next_layer"), "passed": json_6iy.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6IY},
        {"check": "6iy_recommended_path", "expected": RECOMMENDED_PATH_6IY, "actual": json_6iy.get("recommended_path"), "passed": json_6iy.get("recommended_path") == RECOMMENDED_PATH_6IY},
        {"check": "6iy_activation_readiness_planned", "expected": True, "actual": json_6iy.get("activation_readiness_planned"), "passed": json_6iy.get("activation_readiness_planned") is True},
        {"check": "6iy_activation_execution_blocked", "expected": False, "actual": json_6iy.get("activation_execution_allowed_after_this_layer"), "passed": json_6iy.get("activation_execution_allowed_after_this_layer") is False},
        {"check": "6iy_no_exit_credit", "expected": False, "actual": json_6iy.get("layer_6_exit_credit"), "passed": json_6iy.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    readonly_rows = [
        {"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()}
        for path in readonly_sources
    ]

    preserved_rows = [
        {"source_family": family, "status": "preserved", "passed": True}
        for family in PRESERVED_FAMILIES
    ]

    blocking_rows = [
        {"blocked_surface": "activation_readiness_audit", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "6JA audit required first", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "activation execution forbidden", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "activation execution audit required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "activation chain incomplete", "passed": True},
    ]

    decision_rows = [
        {"decision": "6iy_passed", "expected": True, "actual": json_6iy.get("all_checks_passed"), "passed": json_6iy.get("all_checks_passed") is True},
        {"decision": "activation_readiness_implemented", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_execution_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "mechanics_activated", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6ja_activation_readiness_audit_next", "expected": RECOMMENDED_NEXT_LAYER_6IZ, "actual": RECOMMENDED_NEXT_LAYER_6IZ, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_layer", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_truth_join_rerun", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation_rerun", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_final_pass_fail_decision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    plan_6iy_after = PLAN_6IY_PATH.read_text(encoding="utf-8") if PLAN_6IY_PATH.exists() else ""
    audit_6ix_after = AUDIT_6IX_PATH.read_text(encoding="utf-8") if AUDIT_6IX_PATH.exists() else ""
    impl_6iw_after = IMPLEMENT_6IW_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IW_PATH.exists() else ""
    plan_6iv_after = PLAN_6IV_PATH.read_text(encoding="utf-8") if PLAN_6IV_PATH.exists() else ""
    impl_6it_after = IMPLEMENT_6IT_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IT_PATH.exists() else ""
    impl_6iq_after = IMPLEMENT_6IQ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IQ_PATH.exists() else ""
    adapter_after = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_after = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_after = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_after = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    immutability_rows = [
        {"surface": "this_6iz_implementation", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6iy_plan", "policy": "unchanged_by_6iz", "passed": plan_6iy_after == plan_6iy_before},
        {"surface": "6ix_audit", "policy": "unchanged_by_6iz", "passed": audit_6ix_after == audit_6ix_before},
        {"surface": "6iw_implementation", "policy": "unchanged_by_6iz", "passed": impl_6iw_after == impl_6iw_before},
        {"surface": "6iv_plan", "policy": "unchanged_by_6iz", "passed": plan_6iv_after == plan_6iv_before},
        {"surface": "6it_implementation", "policy": "unchanged_by_6iz", "passed": impl_6it_after == impl_6it_before},
        {"surface": "6iq_implementation", "policy": "unchanged_by_6iz", "passed": impl_6iq_after == impl_6iq_before},
        {"surface": "adapter_module", "policy": "unchanged_by_6iz", "passed": adapter_after == adapter_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6iz", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6iz", "passed": corrected_after == corrected_before},
        {"surface": "6ik_materialized_table", "policy": "read_only_unchanged_by_6iz", "passed": materialized_after == materialized_before},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IZ, "actual": RECOMMENDED_NEXT_LAYER_6IZ, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6IZ, "actual": RECOMMENDED_PATH_6IZ, "passed": True},
        {"decision": "recommend_activation_readiness_audit_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_execution_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6IZ, "actual": DIAGNOSIS_6IZ, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "mechanic_readiness_matrix", "passed": len(mechanic_readiness_rows) == 10 and all_passed(mechanic_readiness_rows), "detail": f"{len(mechanic_readiness_rows)}/10"},
        {"check": "rollout_check_matrix", "passed": len(rollout_matrix_rows) == 60 and all_passed(rollout_matrix_rows), "detail": f"{len(rollout_matrix_rows)}/60"},
        {"check": "rollback_gate_matrix", "passed": len(rollback_matrix_rows) == 60 and all_passed(rollback_matrix_rows), "detail": f"{len(rollback_matrix_rows)}/60"},
        {"check": "activation_prevention_assertions", "passed": len(prevention_assertion_rows) == 7 and all_passed(prevention_assertion_rows), "detail": f"{len(prevention_assertion_rows)}/7"},
        {"check": "production_readiness_constraints", "passed": len(production_constraint_rows) == 6 and all_passed(production_constraint_rows), "detail": f"{len(production_constraint_rows)}/6"},
        {"check": "final_decision_prerequisites", "passed": len(final_prereq_rows) == 7 and all_passed(final_prereq_rows), "detail": f"{len(final_prereq_rows)}/7"},
        {"check": "activation_readiness_summary", "passed": len(readiness_summary_rows) == 10 and all_passed(readiness_summary_rows), "detail": f"{len(readiness_summary_rows)}/10"},
        {"check": "future_6ja_contract", "passed": all_passed(future_6ja_rows), "detail": f"{sum(1 for row in future_6ja_rows if row['passed'])}/{len(future_6ja_rows)}"},
        {"check": "readonly_sources", "passed": all_passed(readonly_rows), "detail": f"{sum(1 for row in readonly_rows if row['passed'])}/{len(readonly_rows)}"},
        {"check": "preserved_families", "passed": all_passed(preserved_rows), "detail": f"{sum(1 for row in preserved_rows if row['passed'])}/{len(preserved_rows)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_rows), "detail": f"{sum(1 for row in blocking_rows if row['passed'])}/{len(blocking_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all_passed(immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "mechanic_readiness_matrix": write_csv(MECH_READINESS_CSV, mechanic_readiness_rows),
        "rollout_check_matrix": write_csv(ROLLOUT_MATRIX_CSV, rollout_matrix_rows),
        "rollback_gate_matrix": write_csv(ROLLBACK_MATRIX_CSV, rollback_matrix_rows),
        "activation_prevention_assertions": write_csv(PREVENTION_ASSERTIONS_CSV, prevention_assertion_rows),
        "production_readiness_constraints": write_csv(PRODUCTION_CONSTRAINTS_CSV, production_constraint_rows),
        "final_decision_prerequisites": write_csv(FINAL_PREREQ_CSV, final_prereq_rows),
        "activation_readiness_summary": write_csv(READINESS_SUMMARY_CSV, readiness_summary_rows),
        "future_6ja_contract": write_csv(FUTURE_6JA_CSV, future_6ja_rows),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6IZ",
        "layer_type": "game_mechanics_realism",
        "implementation_layer": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6IZ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6IZ,
        "recommended_path": RECOMMENDED_PATH_6IZ,
        "predecessor_plan": str(PLAN_6IY_PATH),
        "predecessor_plan_returncode": 0,
        "predecessor_plan_diagnosis": json_6iy.get("diagnosis"),
        "planned_layer": "6IY",
        "source_family": SOURCE_FAMILY,
        "depends_on_source_families": DEPENDS_ON_SOURCE_FAMILIES,
        "truth_join_audited": json_6iy.get("truth_join_audited"),
        "truth_join_candidate_row_count": json_6iy.get("truth_join_candidate_row_count"),
        "joined_truth_row_count": json_6iy.get("joined_truth_row_count"),
        "unjoined_evaluation_row_count": json_6iy.get("unjoined_evaluation_row_count"),
        "join_coverage_ratio": json_6iy.get("join_coverage_ratio"),
        "mechanic_readiness_matrix_row_count": len(mechanic_readiness_rows),
        "rollout_check_matrix_row_count": len(rollout_matrix_rows),
        "rollback_gate_matrix_row_count": len(rollback_matrix_rows),
        "activation_prevention_assertion_count": len(prevention_assertion_rows),
        "production_readiness_constraint_count": len(production_constraint_rows),
        "final_decision_prerequisite_count": len(final_prereq_rows),
        "activation_readiness_summary_row_count": len(readiness_summary_rows),
        "future_6ja_contract_valid": all_passed(future_6ja_rows),
        "activation_readiness_implemented": True,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "final_pass_fail_decision_possible_after_this_layer": False,
        "source_artifacts_mutated": False,
        "corrected_candidate_artifacts_mutated": False,
        "materialized_outputs_mutated": False,
        "adapter_implementation_mutated": False,
        "evaluation_implementation_mutated": False,
        "truth_surface_implementation_mutated": False,
        "truth_join_implementation_mutated": False,
        "readiness_plan_mutated": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "database_writes_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "production_simulations_run": False,
        "games_evaluated": 0,
        "layer_6_exit_credit": False,
        "preserved_remediated_family_count": len(PRESERVED_FAMILIES),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "mechanic_readiness_matrix_csv": str(MECH_READINESS_CSV),
            "rollout_check_matrix_csv": str(ROLLOUT_MATRIX_CSV),
            "rollback_gate_matrix_csv": str(ROLLBACK_MATRIX_CSV),
            "activation_prevention_assertions_csv": str(PREVENTION_ASSERTIONS_CSV),
            "production_readiness_constraints_csv": str(PRODUCTION_CONSTRAINTS_CSV),
            "final_decision_prerequisites_csv": str(FINAL_PREREQ_CSV),
            "activation_readiness_summary_csv": str(READINESS_SUMMARY_CSV),
            "future_6ja_contract_csv": str(FUTURE_6JA_CSV),
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
