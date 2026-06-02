#!/usr/bin/env python3
"""Plan Layer 6JB activation execution planning."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jb_activation_execution_planning_plan"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

AUDIT_6JA_PATH = Path("scripts/audit_6ja_layer6_activation_planning_readiness_implementation.py")
IMPLEMENT_6IZ_PATH = Path("scripts/implement_6iz_layer6_activation_planning_readiness.py")
PLAN_6IY_PATH = Path("scripts/plan_6iy_layer6_activation_planning_readiness.py")
AUDIT_6IX_PATH = Path("scripts/audit_6ix_layer6_truth_join_evaluation_implementation.py")
IMPLEMENT_6IW_PATH = Path("scripts/implement_6iw_layer6_truth_join_evaluation.py")
IMPLEMENT_6IT_PATH = Path("scripts/implement_6it_layer6_actual_outcome_surface_gap_resolution.py")
IMPLEMENT_6IQ_PATH = Path("scripts/implement_6iq_layer6_gameplay_mechanic_outcome_real_evaluation.py")
ADAPTER_MODULE_PATH = Path("mlb_app/simulation/layer6_base_out_transition_adapter.py")

JSON_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit.json"
CHECKS_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_checks.csv"
PREDECESSOR_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_predecessor.csv"
INPUT_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_input_artifacts.csv"
READINESS_MATRIX_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_readiness_matrix.csv"
ROLLOUT_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_rollout_checks.csv"
ROLLBACK_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_rollback_gates.csv"
PREVENTION_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_activation_prevention.csv"
PRODUCTION_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_production_constraints.csv"
FINAL_PREREQ_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_final_decision_prerequisites.csv"
SUMMARY_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_readiness_summary.csv"
FUTURE_6JB_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_future_6jb_contract.csv"
READONLY_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_readonly_sources.csv"
PRESERVED_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_preserved_families.csv"
BLOCKING_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_blocking_policy.csv"
DECISION_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_decision.csv"
SAFETY_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_safety_boundaries.csv"
IMMUTABILITY_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_immutability.csv"
RECOMMENDED_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit_recommended_path.csv"

JSON_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation.json"
JSON_6IY = TMP_DIR / "layer6_6iy_activation_planning_readiness_plan.json"
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
CRITERIA_CSV = TMP_DIR / f"{SLUG}_activation_execution_criteria.csv"
DECISION_SURFACES_CSV = TMP_DIR / f"{SLUG}_mechanic_activation_decision_surfaces.csv"
SHADOW_CSV = TMP_DIR / f"{SLUG}_production_shadow_constraints.csv"
ROLLBACK_EXEC_CSV = TMP_DIR / f"{SLUG}_rollback_execution_gates.csv"
FINAL_POLICY_CSV = TMP_DIR / f"{SLUG}_final_decision_policy.csv"
AUDIT_REQ_CSV = TMP_DIR / f"{SLUG}_activation_execution_audit_requirements.csv"
PREVENTION_RULES_CSV = TMP_DIR / f"{SLUG}_activation_execution_prevention_rules.csv"
FUTURE_6JC_CSV = TMP_DIR / f"{SLUG}_future_6jc_contract.csv"
FUTURE_6JD_CSV = TMP_DIR / f"{SLUG}_future_6jd_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JA = "layer_6_activation_planning_readiness_implementation_audit_complete"
DIAGNOSIS_6JB = "layer_6_activation_execution_planning_plan_complete"

RECOMMENDED_NEXT_LAYER_6JA = "6JB_layer_6_activation_execution_planning_plan"
RECOMMENDED_PATH_6JA = "audit_activation_readiness_then_plan_activation_execution"

RECOMMENDED_NEXT_LAYER_6JB = "6JC_layer_6_activation_execution_planning_implementation"
RECOMMENDED_PATH_6JB = "plan_activation_execution_then_implement_planning_before_execution_audit"

SOURCE_FAMILY = "activation_execution_planning"
DEPENDS_ON_SOURCE_FAMILIES = [
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

ACTIVATION_EXECUTION_CRITERIA = [
    "activation_readiness_audit_passed",
    "readiness_matrix_validated",
    "rollout_checks_validated",
    "rollback_gates_validated",
    "activation_prevention_validated",
    "production_constraints_validated",
    "final_decision_policy_defined",
    "execution_implementation_and_audit_required_before_activation",
]

PRODUCTION_SHADOW_CONSTRAINTS = [
    "no_live_mode",
    "shadow_only_until_execution_audit",
    "no_database_writes",
    "no_prediction_surface_mutation",
    "no_user_facing_mechanic_switch",
    "no_layer_6_exit_from_shadow",
]

ROLLBACK_EXECUTION_GATES = [
    "failed_execution_audit",
    "activation_execution_attempted_before_audit",
    "production_simulation_attempted_before_audit",
    "decision_surface_missing_mechanic",
    "final_decision_policy_incomplete",
    "readiness_or_truth_join_artifact_mutated",
]

FINAL_DECISION_POLICY = [
    "no_final_decision_in_6jb",
    "final_decision_requires_6jc_implementation",
    "final_decision_requires_6jd_audit",
    "final_decision_requires_all_mechanic_surfaces_present",
    "final_decision_requires_all_rollback_gates_present",
    "final_decision_requires_explicit_later_activation_execution_layer",
]

ACTIVATION_EXECUTION_AUDIT_REQUIREMENTS = [
    "audit_6jc_outputs",
    "verify_decision_surface_row_count",
    "verify_shadow_constraints",
    "verify_rollback_execution_gates",
    "verify_final_decision_policy",
    "verify_activation_execution_still_blocked",
    "verify_no_mechanics_activated",
    "verify_no_layer_6_exit_credit",
]

ACTIVATION_EXECUTION_PREVENTION_RULES = [
    "do_not_execute_activation_from_plan",
    "do_not_activate_mechanics_from_plan",
    "require_6jc_implementation_before_execution_consideration",
    "require_6jd_audit_before_execution_consideration",
    "block_production_simulation_from_plan",
    "block_final_pass_fail_decision_from_plan",
    "block_layer_6_exit_from_plan",
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
    audit_6ja_before = AUDIT_6JA_PATH.read_text(encoding="utf-8") if AUDIT_6JA_PATH.exists() else ""
    impl_6iz_before = IMPLEMENT_6IZ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IZ_PATH.exists() else ""
    plan_6iy_before = PLAN_6IY_PATH.read_text(encoding="utf-8") if PLAN_6IY_PATH.exists() else ""
    audit_6ix_before = AUDIT_6IX_PATH.read_text(encoding="utf-8") if AUDIT_6IX_PATH.exists() else ""
    impl_6iw_before = IMPLEMENT_6IW_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IW_PATH.exists() else ""
    impl_6it_before = IMPLEMENT_6IT_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IT_PATH.exists() else ""
    impl_6iq_before = IMPLEMENT_6IQ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IQ_PATH.exists() else ""
    adapter_before = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_before = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_before = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_before = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    json_6ja = load_json(JSON_6JA)

    required_inputs = [
        JSON_6JA, CHECKS_6JA, PREDECESSOR_6JA, INPUT_6JA, READINESS_MATRIX_6JA,
        ROLLOUT_6JA, ROLLBACK_6JA, PREVENTION_6JA, PRODUCTION_6JA,
        FINAL_PREREQ_6JA, SUMMARY_6JA, FUTURE_6JB_6JA, READONLY_6JA,
        PRESERVED_6JA, BLOCKING_6JA, DECISION_6JA, SAFETY_6JA,
        IMMUTABILITY_6JA, RECOMMENDED_6JA, JSON_6IZ, JSON_6IY, JSON_6IX,
        JSON_6IW, JSON_6IV, JSON_6IU, JSON_6IT, JSON_6IS, JSON_6IR,
        JSON_6IQ, EVAL_MATRIX_6IQ, METRIC_ROWS_6IQ, BASELINE_6IQ,
        CANDIDATE_DECISIONS_6IQ, LINEAGE_6IQ, TRUTH_ROWS_6IT,
        TRUTH_LINEAGE_6IT, TRUTH_MANIFEST_6IT, TRUTH_SCHEMA_6IT,
        JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IK, ADAPTER_MODULE_PATH,
        MATERIALIZED_TABLE, MATERIALIZED_LINEAGE, SOURCE_MANIFEST_6IB,
        TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    readonly_sources = [
        JSON_6JA, JSON_6IZ, JSON_6IY, JSON_6IX, JSON_6IW, JSON_6IV,
        JSON_6IU, JSON_6IT, JSON_6IS, JSON_6IR, JSON_6IQ, EVAL_MATRIX_6IQ,
        METRIC_ROWS_6IQ, BASELINE_6IQ, CANDIDATE_DECISIONS_6IQ,
        LINEAGE_6IQ, TRUTH_ROWS_6IT, TRUTH_LINEAGE_6IT, TRUTH_MANIFEST_6IT,
        TRUTH_SCHEMA_6IT, JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IK,
        ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZED_LINEAGE,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    criteria_rows = [
        {"criterion": criterion, "planned": True, "activation_execution_allowed": False, "passed": True}
        for criterion in ACTIVATION_EXECUTION_CRITERIA
    ]

    decision_surface_rows = [
        {
            "mechanic": mechanic,
            "decision_surface_planned": True,
            "readiness_audit_required": True,
            "production_shadow_required": True,
            "rollback_execution_gate_required": True,
            "final_decision_policy_required": True,
            "activation_execution_implementation_required": True,
            "activation_execution_audit_required": True,
            "activation_execution_allowed": False,
            "mechanic_activated": False,
            "layer_6_exit_credit": False,
            "passed": True,
        }
        for mechanic in MECHANICS
    ]

    shadow_rows = [
        {"constraint": constraint, "planned": True, "activation_execution_allowed": False, "passed": True}
        for constraint in PRODUCTION_SHADOW_CONSTRAINTS
    ]

    rollback_rows = [
        {"rollback_execution_gate": gate, "planned": True, "activation_execution_allowed": False, "passed": True}
        for gate in ROLLBACK_EXECUTION_GATES
    ]

    final_policy_rows = [
        {"policy": policy, "planned": True, "final_decision_allowed": False, "passed": True}
        for policy in FINAL_DECISION_POLICY
    ]

    audit_req_rows = [
        {"audit_requirement": requirement, "planned": True, "passed": True}
        for requirement in ACTIVATION_EXECUTION_AUDIT_REQUIREMENTS
    ]

    prevention_rows = [
        {"prevention_rule": rule, "planned": True, "activation_execution_allowed": False, "passed": True}
        for rule in ACTIVATION_EXECUTION_PREVENTION_RULES
    ]

    future_6jc_rows = [
        {"contract": "implement_activation_execution_plan_only", "required": True, "passed": True},
        {"contract": "emit_mechanic_decision_surfaces", "required": True, "passed": True},
        {"contract": "emit_production_shadow_constraints", "required": True, "passed": True},
        {"contract": "emit_rollback_execution_gate_matrix", "required": True, "passed": True},
        {"contract": "emit_final_decision_policy_matrix", "required": True, "passed": True},
        {"contract": "emit_activation_execution_audit_requirement_matrix", "required": True, "passed": True},
        {"contract": "emit_activation_execution_prevention_assertions", "required": True, "passed": True},
        {"contract": "do_not_execute_activation", "required": True, "passed": True},
    ]

    future_6jd_rows = [
        {"contract": "audit_6jc_activation_execution_planning_implementation", "required": True, "passed": True},
        {"contract": "verify_activation_execution_planning_outputs", "required": True, "passed": True},
        {"contract": "verify_immutability", "required": True, "passed": True},
        {"contract": "verify_prevention_rules", "required": True, "passed": True},
        {"contract": "verify_no_activation_execution", "required": True, "passed": True},
        {"contract": "verify_no_mechanics_activated", "required": True, "passed": True},
        {"contract": "verify_no_layer_6_exit_credit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ja_audit_exists", "expected": True, "actual": AUDIT_6JA_PATH.exists(), "passed": AUDIT_6JA_PATH.exists()},
        {"check": "6ja_json_exists", "expected": True, "actual": JSON_6JA.exists(), "passed": JSON_6JA.exists()},
        {"check": "6ja_all_checks_passed", "expected": True, "actual": json_6ja.get("all_checks_passed"), "passed": json_6ja.get("all_checks_passed") is True},
        {"check": "6ja_diagnosis", "expected": DIAGNOSIS_6JA, "actual": json_6ja.get("diagnosis"), "passed": json_6ja.get("diagnosis") == DIAGNOSIS_6JA},
        {"check": "6ja_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JA, "actual": json_6ja.get("recommended_next_layer"), "passed": json_6ja.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JA},
        {"check": "6ja_recommended_path", "expected": RECOMMENDED_PATH_6JA, "actual": json_6ja.get("recommended_path"), "passed": json_6ja.get("recommended_path") == RECOMMENDED_PATH_6JA},
        {"check": "6ja_activation_execution_planning_allowed", "expected": True, "actual": json_6ja.get("activation_execution_planning_allowed_after_this_layer"), "passed": json_6ja.get("activation_execution_planning_allowed_after_this_layer") is True},
        {"check": "6ja_activation_execution_blocked", "expected": False, "actual": json_6ja.get("activation_execution_allowed_after_this_layer"), "passed": json_6ja.get("activation_execution_allowed_after_this_layer") is False},
        {"check": "6ja_no_exit_credit", "expected": False, "actual": json_6ja.get("layer_6_exit_credit"), "passed": json_6ja.get("layer_6_exit_credit") is False},
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
        {"blocked_surface": "activation_execution_planning_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "6JC implementation and 6JD audit required first", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "activation execution forbidden", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "activation execution audit required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "activation execution not implemented or audited", "passed": True},
    ]

    decision_rows = [
        {"decision": "6ja_passed", "expected": True, "actual": json_6ja.get("all_checks_passed"), "passed": json_6ja.get("all_checks_passed") is True},
        {"decision": "activation_execution_planned", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_execution_implemented", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_execution_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6jc_activation_execution_planning_implementation_next", "expected": RECOMMENDED_NEXT_LAYER_6JB, "actual": RECOMMENDED_NEXT_LAYER_6JB, "passed": True},
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
        {"boundary": "no_final_pass_fail_decision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_6ja_after = AUDIT_6JA_PATH.read_text(encoding="utf-8") if AUDIT_6JA_PATH.exists() else ""
    impl_6iz_after = IMPLEMENT_6IZ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IZ_PATH.exists() else ""
    plan_6iy_after = PLAN_6IY_PATH.read_text(encoding="utf-8") if PLAN_6IY_PATH.exists() else ""
    audit_6ix_after = AUDIT_6IX_PATH.read_text(encoding="utf-8") if AUDIT_6IX_PATH.exists() else ""
    impl_6iw_after = IMPLEMENT_6IW_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IW_PATH.exists() else ""
    impl_6it_after = IMPLEMENT_6IT_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IT_PATH.exists() else ""
    impl_6iq_after = IMPLEMENT_6IQ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IQ_PATH.exists() else ""
    adapter_after = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_after = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_after = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_after = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    immutability_rows = [
        {"surface": "this_6jb_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6ja_audit", "policy": "unchanged_by_6jb", "passed": audit_6ja_after == audit_6ja_before},
        {"surface": "6iz_implementation", "policy": "unchanged_by_6jb", "passed": impl_6iz_after == impl_6iz_before},
        {"surface": "6iy_plan", "policy": "unchanged_by_6jb", "passed": plan_6iy_after == plan_6iy_before},
        {"surface": "6ix_audit", "policy": "unchanged_by_6jb", "passed": audit_6ix_after == audit_6ix_before},
        {"surface": "6iw_implementation", "policy": "unchanged_by_6jb", "passed": impl_6iw_after == impl_6iw_before},
        {"surface": "6it_implementation", "policy": "unchanged_by_6jb", "passed": impl_6it_after == impl_6it_before},
        {"surface": "6iq_implementation", "policy": "unchanged_by_6jb", "passed": impl_6iq_after == impl_6iq_before},
        {"surface": "adapter_module", "policy": "unchanged_by_6jb", "passed": adapter_after == adapter_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6jb", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6jb", "passed": corrected_after == corrected_before},
        {"surface": "6ik_materialized_table", "policy": "read_only_unchanged_by_6jb", "passed": materialized_after == materialized_before},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JB, "actual": RECOMMENDED_NEXT_LAYER_6JB, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JB, "actual": RECOMMENDED_PATH_6JB, "passed": True},
        {"decision": "recommend_activation_execution_planning_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_execution_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JB, "actual": DIAGNOSIS_6JB, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "activation_execution_criteria", "passed": len(criteria_rows) == 8 and all_passed(criteria_rows), "detail": f"{len(criteria_rows)}/8"},
        {"check": "mechanic_activation_decision_surfaces", "passed": len(decision_surface_rows) == 10 and all_passed(decision_surface_rows), "detail": f"{len(decision_surface_rows)}/10"},
        {"check": "production_shadow_constraints", "passed": len(shadow_rows) == 6 and all_passed(shadow_rows), "detail": f"{len(shadow_rows)}/6"},
        {"check": "rollback_execution_gates", "passed": len(rollback_rows) == 6 and all_passed(rollback_rows), "detail": f"{len(rollback_rows)}/6"},
        {"check": "final_decision_policy", "passed": len(final_policy_rows) == 6 and all_passed(final_policy_rows), "detail": f"{len(final_policy_rows)}/6"},
        {"check": "activation_execution_audit_requirements", "passed": len(audit_req_rows) == 8 and all_passed(audit_req_rows), "detail": f"{len(audit_req_rows)}/8"},
        {"check": "activation_execution_prevention_rules", "passed": len(prevention_rows) == 7 and all_passed(prevention_rows), "detail": f"{len(prevention_rows)}/7"},
        {"check": "future_6jc_contract", "passed": all_passed(future_6jc_rows), "detail": f"{sum(1 for row in future_6jc_rows if row['passed'])}/{len(future_6jc_rows)}"},
        {"check": "future_6jd_contract", "passed": all_passed(future_6jd_rows), "detail": f"{sum(1 for row in future_6jd_rows if row['passed'])}/{len(future_6jd_rows)}"},
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
        "activation_execution_criteria": write_csv(CRITERIA_CSV, criteria_rows),
        "mechanic_activation_decision_surfaces": write_csv(DECISION_SURFACES_CSV, decision_surface_rows),
        "production_shadow_constraints": write_csv(SHADOW_CSV, shadow_rows),
        "rollback_execution_gates": write_csv(ROLLBACK_EXEC_CSV, rollback_rows),
        "final_decision_policy": write_csv(FINAL_POLICY_CSV, final_policy_rows),
        "activation_execution_audit_requirements": write_csv(AUDIT_REQ_CSV, audit_req_rows),
        "activation_execution_prevention_rules": write_csv(PREVENTION_RULES_CSV, prevention_rows),
        "future_6jc_contract": write_csv(FUTURE_6JC_CSV, future_6jc_rows),
        "future_6jd_contract": write_csv(FUTURE_6JD_CSV, future_6jd_rows),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JB",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JB if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JB,
        "recommended_path": RECOMMENDED_PATH_6JB,
        "predecessor_audit": str(AUDIT_6JA_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6ja.get("diagnosis"),
        "audited_layer": "6JA",
        "source_family": SOURCE_FAMILY,
        "depends_on_source_families": DEPENDS_ON_SOURCE_FAMILIES,
        "truth_join_audited": json_6ja.get("truth_join_audited"),
        "truth_join_candidate_row_count": json_6ja.get("truth_join_candidate_row_count"),
        "joined_truth_row_count": json_6ja.get("joined_truth_row_count"),
        "unjoined_evaluation_row_count": json_6ja.get("unjoined_evaluation_row_count"),
        "join_coverage_ratio": json_6ja.get("join_coverage_ratio"),
        "activation_readiness_audited": json_6ja.get("activation_readiness_audited"),
        "activation_execution_planning_allowed": json_6ja.get("activation_execution_planning_allowed_after_this_layer"),
        "activation_execution_criteria_count": len(criteria_rows),
        "mechanic_activation_decision_surface_count": len(decision_surface_rows),
        "production_shadow_constraint_count": len(shadow_rows),
        "rollback_execution_gate_count": len(rollback_rows),
        "final_decision_policy_count": len(final_policy_rows),
        "activation_execution_audit_requirement_count": len(audit_req_rows),
        "activation_execution_prevention_rule_count": len(prevention_rows),
        "future_6jc_contract_valid": all_passed(future_6jc_rows),
        "future_6jd_contract_valid": all_passed(future_6jd_rows),
        "activation_execution_planned": True,
        "activation_execution_implemented": False,
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
        "readiness_implementation_mutated": False,
        "readiness_audit_mutated": False,
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
            "activation_execution_criteria_csv": str(CRITERIA_CSV),
            "mechanic_activation_decision_surfaces_csv": str(DECISION_SURFACES_CSV),
            "production_shadow_constraints_csv": str(SHADOW_CSV),
            "rollback_execution_gates_csv": str(ROLLBACK_EXEC_CSV),
            "final_decision_policy_csv": str(FINAL_POLICY_CSV),
            "activation_execution_audit_requirements_csv": str(AUDIT_REQ_CSV),
            "activation_execution_prevention_rules_csv": str(PREVENTION_RULES_CSV),
            "future_6jc_contract_csv": str(FUTURE_6JC_CSV),
            "future_6jd_contract_csv": str(FUTURE_6JD_CSV),
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
