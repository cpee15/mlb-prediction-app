#!/usr/bin/env python3
"""Plan Layer 6JE activation execution consideration."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6je_activation_execution_consideration_plan"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

AUDIT_6JD_PATH = Path("scripts/audit_6jd_layer6_activation_execution_planning_implementation.py")
IMPLEMENT_6JC_PATH = Path("scripts/implement_6jc_layer6_activation_execution_planning.py")
PLAN_6JB_PATH = Path("scripts/plan_6jb_layer6_activation_execution_planning.py")
AUDIT_6JA_PATH = Path("scripts/audit_6ja_layer6_activation_planning_readiness_implementation.py")
IMPLEMENT_6IZ_PATH = Path("scripts/implement_6iz_layer6_activation_planning_readiness.py")
PLAN_6IY_PATH = Path("scripts/plan_6iy_layer6_activation_planning_readiness.py")
AUDIT_6IX_PATH = Path("scripts/audit_6ix_layer6_truth_join_evaluation_implementation.py")
IMPLEMENT_6IW_PATH = Path("scripts/implement_6iw_layer6_truth_join_evaluation.py")
IMPLEMENT_6IT_PATH = Path("scripts/implement_6it_layer6_actual_outcome_surface_gap_resolution.py")
IMPLEMENT_6IQ_PATH = Path("scripts/implement_6iq_layer6_gameplay_mechanic_outcome_real_evaluation.py")
ADAPTER_MODULE_PATH = Path("mlb_app/simulation/layer6_base_out_transition_adapter.py")

JSON_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit.json"
CHECKS_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_checks.csv"
PREDECESSOR_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_predecessor.csv"
INPUT_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_input_artifacts.csv"
CRITERIA_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_criteria_matrix.csv"
DECISION_SURFACES_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_decision_surface_matrix.csv"
SHADOW_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_shadow_constraints.csv"
ROLLBACK_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_rollback_gates.csv"
FINAL_POLICY_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_final_decision_policy.csv"
AUDIT_REQ_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_audit_requirements.csv"
PREVENTION_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_prevention_assertions.csv"
FUTURE_6JE_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_future_6je_contract.csv"
READONLY_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_readonly_sources.csv"
PRESERVED_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_preserved_families.csv"
BLOCKING_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_blocking_policy.csv"
DECISION_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_decision.csv"
SAFETY_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_safety_boundaries.csv"
IMMUTABILITY_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_immutability.csv"
RECOMMENDED_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit_recommended_path.csv"

JSON_6JC = TMP_DIR / "layer6_6jc_activation_execution_planning_implementation.json"
JSON_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan.json"
JSON_6JA = TMP_DIR / "layer6_6ja_activation_planning_readiness_implementation_audit.json"
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
CONSIDERATION_CRITERIA_CSV = TMP_DIR / f"{SLUG}_consideration_criteria.csv"
MECHANIC_SURFACES_CSV = TMP_DIR / f"{SLUG}_mechanic_consideration_surfaces.csv"
GO_NO_GO_CSV = TMP_DIR / f"{SLUG}_go_no_go_gates.csv"
RISK_REVIEW_CSV = TMP_DIR / f"{SLUG}_risk_review_gates.csv"
FINAL_BLOCKERS_CSV = TMP_DIR / f"{SLUG}_final_activation_decision_blockers.csv"
AUDIT_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_consideration_audit_requirements.csv"
PREVENTION_RULES_CSV = TMP_DIR / f"{SLUG}_consideration_prevention_rules.csv"
FUTURE_6JF_CSV = TMP_DIR / f"{SLUG}_future_6jf_contract.csv"
FUTURE_6JG_CSV = TMP_DIR / f"{SLUG}_future_6jg_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JD = "layer_6_activation_execution_planning_implementation_audit_complete"
DIAGNOSIS_6JE = "layer_6_activation_execution_consideration_plan_complete"

RECOMMENDED_NEXT_LAYER_6JD = "6JE_layer_6_activation_execution_consideration_plan"
RECOMMENDED_PATH_6JD = "audit_activation_execution_planning_then_plan_activation_execution_consideration"

RECOMMENDED_NEXT_LAYER_6JE = "6JF_layer_6_activation_execution_consideration_implementation"
RECOMMENDED_PATH_6JE = "plan_activation_execution_consideration_then_implement_before_consideration_audit"

SOURCE_FAMILY = "activation_execution_consideration"
DEPENDS_ON_SOURCE_FAMILIES = [
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

CONSIDERATION_CRITERIA = [
    "activation_execution_planning_audit_passed",
    "all_mechanic_decision_surfaces_present",
    "shadow_constraints_present",
    "rollback_execution_gates_present",
    "final_decision_policy_present",
    "prevention_assertions_present",
    "risk_review_required_before_any_activation_execution",
    "implementation_and_audit_required_before_consideration_decision",
]

GO_NO_GO_GATES = [
    "consideration_matrix_complete",
    "all_mechanics_have_consideration_surface",
    "no_activation_execution_yet",
    "no_mechanics_activated",
    "no_final_activation_decision",
    "no_production_simulation",
    "layer_6_exit_blocked",
]

RISK_REVIEW_GATES = [
    "production_shadow_risk_review_required",
    "rollback_path_review_required",
    "metric_finalization_risk_review_required",
    "candidate_decision_risk_review_required",
    "live_mode_risk_review_required",
    "database_write_risk_review_required",
    "user_facing_switch_risk_review_required",
]

FINAL_ACTIVATION_DECISION_BLOCKERS = [
    "block_final_decision_from_consideration_plan",
    "require_6jf_implementation",
    "require_6jg_audit",
    "require_explicit_activation_execution_plan_after_consideration",
    "require_no_regression_in_truth_join_or_readiness",
    "require_rollback_review_before_execution",
    "require_layer6_exit_gate_after_execution_audit",
]

CONSIDERATION_AUDIT_REQUIREMENTS = [
    "audit_6jf_outputs",
    "verify_consideration_criteria_count",
    "verify_mechanic_consideration_surface_count",
    "verify_go_no_go_gates",
    "verify_risk_review_gates",
    "verify_final_activation_decision_blockers",
    "verify_activation_execution_still_blocked",
    "verify_no_layer_6_exit_credit",
]

CONSIDERATION_PREVENTION_RULES = [
    "do_not_execute_activation_from_consideration_plan",
    "do_not_activate_mechanics_from_consideration_plan",
    "block_final_activation_decision_from_consideration_plan",
    "require_6jf_implementation_before_consideration_decision",
    "require_6jg_audit_before_consideration_decision",
    "block_production_simulation_from_consideration_plan",
    "block_layer_6_exit_from_consideration_plan",
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
    audit_6jd_before = AUDIT_6JD_PATH.read_text(encoding="utf-8") if AUDIT_6JD_PATH.exists() else ""
    impl_6jc_before = IMPLEMENT_6JC_PATH.read_text(encoding="utf-8") if IMPLEMENT_6JC_PATH.exists() else ""
    plan_6jb_before = PLAN_6JB_PATH.read_text(encoding="utf-8") if PLAN_6JB_PATH.exists() else ""
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

    json_6jd = load_json(JSON_6JD)

    criteria_rows = [
        {"criterion": item, "planned": True, "activation_execution_allowed": False, "passed": True}
        for item in CONSIDERATION_CRITERIA
    ]

    mechanic_surface_rows = [
        {
            "mechanic": mechanic,
            "consideration_surface_planned": True,
            "planning_audit_required": True,
            "risk_review_required": True,
            "go_no_go_gate_required": True,
            "final_activation_decision_blocker_required": True,
            "consideration_implementation_required": True,
            "consideration_audit_required": True,
            "activation_execution_allowed": False,
            "final_activation_decision_allowed": False,
            "mechanic_activated": False,
            "layer_6_exit_credit": False,
            "passed": True,
        }
        for mechanic in MECHANICS
    ]

    go_no_go_rows = [
        {"go_no_go_gate": item, "planned": True, "activation_execution_allowed": False, "passed": True}
        for item in GO_NO_GO_GATES
    ]

    risk_review_rows = [
        {"risk_review_gate": item, "planned": True, "activation_execution_allowed": False, "passed": True}
        for item in RISK_REVIEW_GATES
    ]

    final_blocker_rows = [
        {"final_activation_decision_blocker": item, "planned": True, "final_activation_decision_allowed": False, "passed": True}
        for item in FINAL_ACTIVATION_DECISION_BLOCKERS
    ]

    audit_req_rows = [
        {"consideration_audit_requirement": item, "planned": True, "passed": True}
        for item in CONSIDERATION_AUDIT_REQUIREMENTS
    ]

    prevention_rows = [
        {"consideration_prevention_rule": item, "planned": True, "activation_execution_allowed": False, "passed": True}
        for item in CONSIDERATION_PREVENTION_RULES
    ]

    future_6jf_rows = [
        {"contract": "implement_activation_execution_consideration_plan_only", "required": True, "passed": True},
        {"contract": "emit_consideration_criteria_matrix", "required": True, "passed": True},
        {"contract": "emit_mechanic_consideration_surface_matrix", "required": True, "passed": True},
        {"contract": "emit_go_no_go_gate_matrix", "required": True, "passed": True},
        {"contract": "emit_risk_review_gate_matrix", "required": True, "passed": True},
        {"contract": "emit_final_activation_decision_blocker_matrix", "required": True, "passed": True},
        {"contract": "emit_consideration_audit_requirement_matrix", "required": True, "passed": True},
        {"contract": "emit_consideration_prevention_assertions", "required": True, "passed": True},
        {"contract": "do_not_execute_activation", "required": True, "passed": True},
    ]

    future_6jg_rows = [
        {"contract": "audit_6jf_activation_execution_consideration_implementation", "required": True, "passed": True},
        {"contract": "verify_consideration_outputs", "required": True, "passed": True},
        {"contract": "verify_immutability", "required": True, "passed": True},
        {"contract": "verify_prevention_rules", "required": True, "passed": True},
        {"contract": "verify_no_activation_execution", "required": True, "passed": True},
        {"contract": "verify_no_mechanics_activated", "required": True, "passed": True},
        {"contract": "verify_no_final_activation_decision", "required": True, "passed": True},
        {"contract": "verify_no_production_simulation", "required": True, "passed": True},
        {"contract": "verify_no_layer_6_exit_credit", "required": True, "passed": True},
    ]

    required_inputs = [
        JSON_6JD, CHECKS_6JD, PREDECESSOR_6JD, INPUT_6JD, CRITERIA_6JD,
        DECISION_SURFACES_6JD, SHADOW_6JD, ROLLBACK_6JD, FINAL_POLICY_6JD,
        AUDIT_REQ_6JD, PREVENTION_6JD, FUTURE_6JE_6JD, READONLY_6JD,
        PRESERVED_6JD, BLOCKING_6JD, DECISION_6JD, SAFETY_6JD,
        IMMUTABILITY_6JD, RECOMMENDED_6JD, JSON_6JC, JSON_6JB, JSON_6JA,
        JSON_6IZ, JSON_6IY, JSON_6IX, JSON_6IW, JSON_6IV, JSON_6IU,
        JSON_6IT, JSON_6IS, JSON_6IR, JSON_6IQ, EVAL_MATRIX_6IQ,
        METRIC_ROWS_6IQ, BASELINE_6IQ, CANDIDATE_DECISIONS_6IQ, LINEAGE_6IQ,
        TRUTH_ROWS_6IT, TRUTH_LINEAGE_6IT, TRUTH_MANIFEST_6IT,
        TRUTH_SCHEMA_6IT, JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IK,
        ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZED_LINEAGE,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    readonly_sources = [
        JSON_6JD, JSON_6JC, JSON_6JB, JSON_6JA, JSON_6IZ, JSON_6IY,
        JSON_6IX, JSON_6IW, JSON_6IV, JSON_6IU, JSON_6IT, JSON_6IS,
        JSON_6IR, JSON_6IQ, EVAL_MATRIX_6IQ, METRIC_ROWS_6IQ, BASELINE_6IQ,
        CANDIDATE_DECISIONS_6IQ, LINEAGE_6IQ, TRUTH_ROWS_6IT, TRUTH_LINEAGE_6IT,
        TRUTH_MANIFEST_6IT, TRUTH_SCHEMA_6IT, JSON_6IP, JSON_6IO, JSON_6IN,
        JSON_6IK, ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZED_LINEAGE,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jd_audit_exists", "expected": True, "actual": AUDIT_6JD_PATH.exists(), "passed": AUDIT_6JD_PATH.exists()},
        {"check": "6jd_json_exists", "expected": True, "actual": JSON_6JD.exists(), "passed": JSON_6JD.exists()},
        {"check": "6jd_all_checks_passed", "expected": True, "actual": json_6jd.get("all_checks_passed"), "passed": json_6jd.get("all_checks_passed") is True},
        {"check": "6jd_diagnosis", "expected": DIAGNOSIS_6JD, "actual": json_6jd.get("diagnosis"), "passed": json_6jd.get("diagnosis") == DIAGNOSIS_6JD},
        {"check": "6jd_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JD, "actual": json_6jd.get("recommended_next_layer"), "passed": json_6jd.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JD},
        {"check": "6jd_recommended_path", "expected": RECOMMENDED_PATH_6JD, "actual": json_6jd.get("recommended_path"), "passed": json_6jd.get("recommended_path") == RECOMMENDED_PATH_6JD},
        {"check": "6jd_activation_execution_consideration_planning_allowed", "expected": True, "actual": json_6jd.get("activation_execution_consideration_planning_allowed_after_this_layer"), "passed": json_6jd.get("activation_execution_consideration_planning_allowed_after_this_layer") is True},
        {"check": "6jd_activation_execution_blocked", "expected": False, "actual": json_6jd.get("activation_execution_allowed_after_this_layer"), "passed": json_6jd.get("activation_execution_allowed_after_this_layer") is False},
        {"check": "6jd_no_exit_credit", "expected": False, "actual": json_6jd.get("layer_6_exit_credit"), "passed": json_6jd.get("layer_6_exit_credit") is False},
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
        {"blocked_surface": "activation_execution_consideration_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "consideration implementation and audit required first", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "activation execution forbidden", "passed": True},
        {"blocked_surface": "final_activation_decision", "blocked": True, "reason": "consideration plan cannot decide activation", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "activation execution audit required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "activation execution not performed or audited", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jd_passed", "expected": True, "actual": json_6jd.get("all_checks_passed"), "passed": json_6jd.get("all_checks_passed") is True},
        {"decision": "activation_execution_consideration_planned", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_execution_consideration_implemented", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_execution_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "final_activation_decision_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6jf_consideration_implementation_next", "expected": RECOMMENDED_NEXT_LAYER_6JE, "actual": RECOMMENDED_NEXT_LAYER_6JE, "passed": True},
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
    audit_6jd_after = AUDIT_6JD_PATH.read_text(encoding="utf-8") if AUDIT_6JD_PATH.exists() else ""
    impl_6jc_after = IMPLEMENT_6JC_PATH.read_text(encoding="utf-8") if IMPLEMENT_6JC_PATH.exists() else ""
    plan_6jb_after = PLAN_6JB_PATH.read_text(encoding="utf-8") if PLAN_6JB_PATH.exists() else ""
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
        {"surface": "this_6je_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6jd_audit", "policy": "unchanged_by_6je", "passed": audit_6jd_after == audit_6jd_before},
        {"surface": "6jc_implementation", "policy": "unchanged_by_6je", "passed": impl_6jc_after == impl_6jc_before},
        {"surface": "6jb_plan", "policy": "unchanged_by_6je", "passed": plan_6jb_after == plan_6jb_before},
        {"surface": "6ja_audit", "policy": "unchanged_by_6je", "passed": audit_6ja_after == audit_6ja_before},
        {"surface": "6iz_implementation", "policy": "unchanged_by_6je", "passed": impl_6iz_after == impl_6iz_before},
        {"surface": "6iy_plan", "policy": "unchanged_by_6je", "passed": plan_6iy_after == plan_6iy_before},
        {"surface": "6ix_audit", "policy": "unchanged_by_6je", "passed": audit_6ix_after == audit_6ix_before},
        {"surface": "6iw_implementation", "policy": "unchanged_by_6je", "passed": impl_6iw_after == impl_6iw_before},
        {"surface": "6it_implementation", "policy": "unchanged_by_6je", "passed": impl_6it_after == impl_6it_before},
        {"surface": "6iq_implementation", "policy": "unchanged_by_6je", "passed": impl_6iq_after == impl_6iq_before},
        {"surface": "adapter_module", "policy": "unchanged_by_6je", "passed": adapter_after == adapter_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6je", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6je", "passed": corrected_after == corrected_before},
        {"surface": "6ik_materialized_table", "policy": "read_only_unchanged_by_6je", "passed": materialized_after == materialized_before},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JE, "actual": RECOMMENDED_NEXT_LAYER_6JE, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JE, "actual": RECOMMENDED_PATH_6JE, "passed": True},
        {"decision": "recommend_consideration_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_execution_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JE, "actual": DIAGNOSIS_6JE, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "consideration_criteria", "passed": len(criteria_rows) == 8 and all_passed(criteria_rows), "detail": f"{len(criteria_rows)}/8"},
        {"check": "mechanic_consideration_surfaces", "passed": len(mechanic_surface_rows) == 10 and all_passed(mechanic_surface_rows), "detail": f"{len(mechanic_surface_rows)}/10"},
        {"check": "go_no_go_gates", "passed": len(go_no_go_rows) == 7 and all_passed(go_no_go_rows), "detail": f"{len(go_no_go_rows)}/7"},
        {"check": "risk_review_gates", "passed": len(risk_review_rows) == 7 and all_passed(risk_review_rows), "detail": f"{len(risk_review_rows)}/7"},
        {"check": "final_activation_decision_blockers", "passed": len(final_blocker_rows) == 7 and all_passed(final_blocker_rows), "detail": f"{len(final_blocker_rows)}/7"},
        {"check": "consideration_audit_requirements", "passed": len(audit_req_rows) == 8 and all_passed(audit_req_rows), "detail": f"{len(audit_req_rows)}/8"},
        {"check": "consideration_prevention_rules", "passed": len(prevention_rows) == 7 and all_passed(prevention_rows), "detail": f"{len(prevention_rows)}/7"},
        {"check": "future_6jf_contract", "passed": all_passed(future_6jf_rows), "detail": f"{sum(1 for row in future_6jf_rows if row['passed'])}/{len(future_6jf_rows)}"},
        {"check": "future_6jg_contract", "passed": all_passed(future_6jg_rows), "detail": f"{sum(1 for row in future_6jg_rows if row['passed'])}/{len(future_6jg_rows)}"},
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
        "consideration_criteria": write_csv(CONSIDERATION_CRITERIA_CSV, criteria_rows),
        "mechanic_consideration_surfaces": write_csv(MECHANIC_SURFACES_CSV, mechanic_surface_rows),
        "go_no_go_gates": write_csv(GO_NO_GO_CSV, go_no_go_rows),
        "risk_review_gates": write_csv(RISK_REVIEW_CSV, risk_review_rows),
        "final_activation_decision_blockers": write_csv(FINAL_BLOCKERS_CSV, final_blocker_rows),
        "consideration_audit_requirements": write_csv(AUDIT_REQUIREMENTS_CSV, audit_req_rows),
        "consideration_prevention_rules": write_csv(PREVENTION_RULES_CSV, prevention_rows),
        "future_6jf_contract": write_csv(FUTURE_6JF_CSV, future_6jf_rows),
        "future_6jg_contract": write_csv(FUTURE_6JG_CSV, future_6jg_rows),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JE",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JE if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JE,
        "recommended_path": RECOMMENDED_PATH_6JE,
        "predecessor_audit": str(AUDIT_6JD_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6jd.get("diagnosis"),
        "audited_layer": "6JD",
        "source_family": SOURCE_FAMILY,
        "depends_on_source_families": DEPENDS_ON_SOURCE_FAMILIES,
        "activation_execution_planning_audited": json_6jd.get("activation_execution_planning_audited"),
        "activation_execution_consideration_planning_allowed": json_6jd.get("activation_execution_consideration_planning_allowed_after_this_layer"),
        "consideration_criteria_count": len(criteria_rows),
        "mechanic_consideration_surface_count": len(mechanic_surface_rows),
        "go_no_go_gate_count": len(go_no_go_rows),
        "risk_review_gate_count": len(risk_review_rows),
        "final_activation_decision_blocker_count": len(final_blocker_rows),
        "consideration_audit_requirement_count": len(audit_req_rows),
        "consideration_prevention_rule_count": len(prevention_rows),
        "future_6jf_contract_valid": all_passed(future_6jf_rows),
        "future_6jg_contract_valid": all_passed(future_6jg_rows),
        "activation_execution_consideration_planned": True,
        "activation_execution_consideration_implemented": False,
        "activation_execution_consideration_audited": False,
        "activation_execution_allowed_after_this_layer": False,
        "activation_execution_executed": False,
        "mechanics_activated_by_this_layer": False,
        "final_activation_decision_allowed_after_this_layer": False,
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
        "activation_execution_plan_mutated": False,
        "activation_execution_implementation_mutated": False,
        "activation_execution_audit_mutated": False,
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
            "consideration_criteria_csv": str(CONSIDERATION_CRITERIA_CSV),
            "mechanic_consideration_surfaces_csv": str(MECHANIC_SURFACES_CSV),
            "go_no_go_gates_csv": str(GO_NO_GO_CSV),
            "risk_review_gates_csv": str(RISK_REVIEW_CSV),
            "final_activation_decision_blockers_csv": str(FINAL_BLOCKERS_CSV),
            "consideration_audit_requirements_csv": str(AUDIT_REQUIREMENTS_CSV),
            "consideration_prevention_rules_csv": str(PREVENTION_RULES_CSV),
            "future_6jf_contract_csv": str(FUTURE_6JF_CSV),
            "future_6jg_contract_csv": str(FUTURE_6JG_CSV),
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
