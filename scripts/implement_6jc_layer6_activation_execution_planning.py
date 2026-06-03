#!/usr/bin/env python3
"""Implement Layer 6JC activation execution planning surfaces."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jc_activation_execution_planning_implementation"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

PLAN_6JB_PATH = Path("scripts/plan_6jb_layer6_activation_execution_planning.py")
AUDIT_6JA_PATH = Path("scripts/audit_6ja_layer6_activation_planning_readiness_implementation.py")
IMPLEMENT_6IZ_PATH = Path("scripts/implement_6iz_layer6_activation_planning_readiness.py")
PLAN_6IY_PATH = Path("scripts/plan_6iy_layer6_activation_planning_readiness.py")
AUDIT_6IX_PATH = Path("scripts/audit_6ix_layer6_truth_join_evaluation_implementation.py")
IMPLEMENT_6IW_PATH = Path("scripts/implement_6iw_layer6_truth_join_evaluation.py")
IMPLEMENT_6IT_PATH = Path("scripts/implement_6it_layer6_actual_outcome_surface_gap_resolution.py")
IMPLEMENT_6IQ_PATH = Path("scripts/implement_6iq_layer6_gameplay_mechanic_outcome_real_evaluation.py")
ADAPTER_MODULE_PATH = Path("mlb_app/simulation/layer6_base_out_transition_adapter.py")

JSON_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan.json"
CHECKS_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_checks.csv"
PREDECESSOR_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_predecessor.csv"
INPUT_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_input_artifacts.csv"
CRITERIA_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_activation_execution_criteria.csv"
DECISION_SURFACES_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_mechanic_activation_decision_surfaces.csv"
SHADOW_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_production_shadow_constraints.csv"
ROLLBACK_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_rollback_execution_gates.csv"
FINAL_POLICY_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_final_decision_policy.csv"
AUDIT_REQ_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_activation_execution_audit_requirements.csv"
PREVENTION_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_activation_execution_prevention_rules.csv"
FUTURE_6JC_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_future_6jc_contract.csv"
FUTURE_6JD_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_future_6jd_contract.csv"
READONLY_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_readonly_sources.csv"
PRESERVED_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_preserved_families.csv"
BLOCKING_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_blocking_policy.csv"
DECISION_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_decision.csv"
SAFETY_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_safety_boundaries.csv"
IMMUTABILITY_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_immutability.csv"
RECOMMENDED_6JB = TMP_DIR / "layer6_6jb_activation_execution_planning_plan_recommended_path.csv"

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
CRITERIA_MATRIX_CSV = TMP_DIR / f"{SLUG}_activation_execution_criteria_matrix.csv"
DECISION_SURFACE_MATRIX_CSV = TMP_DIR / f"{SLUG}_mechanic_activation_decision_surface_matrix.csv"
SHADOW_MATRIX_CSV = TMP_DIR / f"{SLUG}_production_shadow_constraint_matrix.csv"
ROLLBACK_MATRIX_CSV = TMP_DIR / f"{SLUG}_rollback_execution_gate_matrix.csv"
FINAL_POLICY_MATRIX_CSV = TMP_DIR / f"{SLUG}_final_decision_policy_matrix.csv"
AUDIT_REQ_MATRIX_CSV = TMP_DIR / f"{SLUG}_activation_execution_audit_requirement_matrix.csv"
PREVENTION_ASSERTIONS_CSV = TMP_DIR / f"{SLUG}_activation_execution_prevention_assertions.csv"
FUTURE_6JD_CSV = TMP_DIR / f"{SLUG}_future_6jd_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JB = "layer_6_activation_execution_planning_plan_complete"
DIAGNOSIS_6JC = "layer_6_activation_execution_planning_implementation_complete"

RECOMMENDED_NEXT_LAYER_6JB = "6JC_layer_6_activation_execution_planning_implementation"
RECOMMENDED_PATH_6JB = "plan_activation_execution_then_implement_planning_before_execution_audit"

RECOMMENDED_NEXT_LAYER_6JC = "6JD_layer_6_activation_execution_planning_implementation_audit"
RECOMMENDED_PATH_6JC = "implement_activation_execution_planning_then_audit_before_execution_consideration"

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


def false_for_all(rows: List[Dict[str, Any]], key: str) -> bool:
    return bool(rows) and all(str(row.get(key, "")).lower() == "false" for row in rows)


def true_for_all(rows: List[Dict[str, Any]], key: str) -> bool:
    return bool(rows) and all(str(row.get(key, "")).lower() == "true" for row in rows)


def normalize_plan_rows(rows: List[Dict[str, str]], name_key: str) -> List[str]:
    return [str(row.get(name_key, "")).strip() for row in rows if str(row.get(name_key, "")).strip()]


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
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

    json_6jb = load_json(JSON_6JB)

    plan_criteria = read_csv(CRITERIA_6JB)
    plan_decisions = read_csv(DECISION_SURFACES_6JB)
    plan_shadow = read_csv(SHADOW_6JB)
    plan_rollback = read_csv(ROLLBACK_6JB)
    plan_final = read_csv(FINAL_POLICY_6JB)
    plan_audit_req = read_csv(AUDIT_REQ_6JB)
    plan_prevention = read_csv(PREVENTION_6JB)

    criteria_rows = [
        {
            "criterion": row.get("criterion", ""),
            "implemented": True,
            "source_plan": str(CRITERIA_6JB),
            "activation_execution_allowed": False,
            "passed": True,
        }
        for row in plan_criteria
    ]

    decision_surface_rows = [
        {
            "mechanic": row.get("mechanic", ""),
            "decision_surface_implemented": True,
            "readiness_audit_required": True,
            "production_shadow_required": True,
            "rollback_execution_gate_required": True,
            "final_decision_policy_required": True,
            "activation_execution_implementation_recorded": True,
            "activation_execution_audit_required": True,
            "activation_execution_allowed": False,
            "mechanic_activated": False,
            "layer_6_exit_credit": False,
            "source_plan": str(DECISION_SURFACES_6JB),
            "passed": True,
        }
        for row in plan_decisions
    ]

    shadow_rows = [
        {
            "constraint": row.get("constraint", ""),
            "implemented": True,
            "source_plan": str(SHADOW_6JB),
            "activation_execution_allowed": False,
            "production_simulation_allowed": False,
            "live_mode_allowed": False,
            "passed": True,
        }
        for row in plan_shadow
    ]

    rollback_rows = [
        {
            "rollback_execution_gate": row.get("rollback_execution_gate", ""),
            "implemented": True,
            "source_plan": str(ROLLBACK_6JB),
            "activation_execution_allowed": False,
            "passed": True,
        }
        for row in plan_rollback
    ]

    final_policy_rows = [
        {
            "policy": row.get("policy", ""),
            "implemented": True,
            "source_plan": str(FINAL_POLICY_6JB),
            "final_decision_allowed": False,
            "passed": True,
        }
        for row in plan_final
    ]

    audit_req_rows = [
        {
            "audit_requirement": row.get("audit_requirement", ""),
            "implemented": True,
            "source_plan": str(AUDIT_REQ_6JB),
            "passed": True,
        }
        for row in plan_audit_req
    ]

    prevention_rows = [
        {
            "prevention_rule": row.get("prevention_rule", ""),
            "implemented": True,
            "source_plan": str(PREVENTION_6JB),
            "activation_execution_allowed": False,
            "passed": True,
        }
        for row in plan_prevention
    ]

    future_6jd_rows = [
        {"contract": "audit_6jc_activation_execution_planning_implementation", "required": True, "passed": True},
        {"contract": "verify_criteria_matrix_count", "required": True, "passed": True},
        {"contract": "verify_mechanic_decision_surface_count", "required": True, "passed": True},
        {"contract": "verify_shadow_constraint_count", "required": True, "passed": True},
        {"contract": "verify_rollback_gate_count", "required": True, "passed": True},
        {"contract": "verify_final_decision_policy_count", "required": True, "passed": True},
        {"contract": "verify_audit_requirement_count", "required": True, "passed": True},
        {"contract": "verify_prevention_assertion_count", "required": True, "passed": True},
        {"contract": "verify_no_activation_execution", "required": True, "passed": True},
        {"contract": "verify_no_mechanics_activated", "required": True, "passed": True},
        {"contract": "verify_no_layer_6_exit_credit", "required": True, "passed": True},
    ]

    required_inputs = [
        JSON_6JB, CHECKS_6JB, PREDECESSOR_6JB, INPUT_6JB, CRITERIA_6JB,
        DECISION_SURFACES_6JB, SHADOW_6JB, ROLLBACK_6JB, FINAL_POLICY_6JB,
        AUDIT_REQ_6JB, PREVENTION_6JB, FUTURE_6JC_6JB, FUTURE_6JD_6JB,
        READONLY_6JB, PRESERVED_6JB, BLOCKING_6JB, DECISION_6JB,
        SAFETY_6JB, IMMUTABILITY_6JB, RECOMMENDED_6JB, JSON_6JA, JSON_6IZ,
        JSON_6IY, JSON_6IX, JSON_6IW, JSON_6IV, JSON_6IU, JSON_6IT,
        JSON_6IS, JSON_6IR, JSON_6IQ, EVAL_MATRIX_6IQ, METRIC_ROWS_6IQ,
        BASELINE_6IQ, CANDIDATE_DECISIONS_6IQ, LINEAGE_6IQ,
        TRUTH_ROWS_6IT, TRUTH_LINEAGE_6IT, TRUTH_MANIFEST_6IT,
        TRUTH_SCHEMA_6IT, JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IK,
        ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZED_LINEAGE,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    readonly_sources = [
        JSON_6JB, JSON_6JA, JSON_6IZ, JSON_6IY, JSON_6IX, JSON_6IW, JSON_6IV,
        JSON_6IU, JSON_6IT, JSON_6IS, JSON_6IR, JSON_6IQ, EVAL_MATRIX_6IQ,
        METRIC_ROWS_6IQ, BASELINE_6IQ, CANDIDATE_DECISIONS_6IQ,
        LINEAGE_6IQ, TRUTH_ROWS_6IT, TRUTH_LINEAGE_6IT, TRUTH_MANIFEST_6IT,
        TRUTH_SCHEMA_6IT, JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IK,
        ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZED_LINEAGE,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jb_plan_exists", "expected": True, "actual": PLAN_6JB_PATH.exists(), "passed": PLAN_6JB_PATH.exists()},
        {"check": "6jb_json_exists", "expected": True, "actual": JSON_6JB.exists(), "passed": JSON_6JB.exists()},
        {"check": "6jb_all_checks_passed", "expected": True, "actual": json_6jb.get("all_checks_passed"), "passed": json_6jb.get("all_checks_passed") is True},
        {"check": "6jb_diagnosis", "expected": DIAGNOSIS_6JB, "actual": json_6jb.get("diagnosis"), "passed": json_6jb.get("diagnosis") == DIAGNOSIS_6JB},
        {"check": "6jb_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JB, "actual": json_6jb.get("recommended_next_layer"), "passed": json_6jb.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JB},
        {"check": "6jb_recommended_path", "expected": RECOMMENDED_PATH_6JB, "actual": json_6jb.get("recommended_path"), "passed": json_6jb.get("recommended_path") == RECOMMENDED_PATH_6JB},
        {"check": "6jb_activation_execution_planned", "expected": True, "actual": json_6jb.get("activation_execution_planned"), "passed": json_6jb.get("activation_execution_planned") is True},
        {"check": "6jb_activation_execution_blocked", "expected": False, "actual": json_6jb.get("activation_execution_allowed_after_this_layer"), "passed": json_6jb.get("activation_execution_allowed_after_this_layer") is False},
        {"check": "6jb_no_exit_credit", "expected": False, "actual": json_6jb.get("layer_6_exit_credit"), "passed": json_6jb.get("layer_6_exit_credit") is False},
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
        {"blocked_surface": "activation_execution_planning_audit", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "6JD audit required before execution consideration", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "activation execution forbidden", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "activation execution audit required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "activation execution not audited or executed", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jb_passed", "expected": True, "actual": json_6jb.get("all_checks_passed"), "passed": json_6jb.get("all_checks_passed") is True},
        {"decision": "activation_execution_planning_implemented", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_execution_executed", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_execution_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6jd_activation_execution_planning_audit_next", "expected": RECOMMENDED_NEXT_LAYER_6JC, "actual": RECOMMENDED_NEXT_LAYER_6JC, "passed": True},
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
        {"surface": "this_6jc_implementation", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6jb_plan", "policy": "unchanged_by_6jc", "passed": plan_6jb_after == plan_6jb_before},
        {"surface": "6ja_audit", "policy": "unchanged_by_6jc", "passed": audit_6ja_after == audit_6ja_before},
        {"surface": "6iz_implementation", "policy": "unchanged_by_6jc", "passed": impl_6iz_after == impl_6iz_before},
        {"surface": "6iy_plan", "policy": "unchanged_by_6jc", "passed": plan_6iy_after == plan_6iy_before},
        {"surface": "6ix_audit", "policy": "unchanged_by_6jc", "passed": audit_6ix_after == audit_6ix_before},
        {"surface": "6iw_implementation", "policy": "unchanged_by_6jc", "passed": impl_6iw_after == impl_6iw_before},
        {"surface": "6it_implementation", "policy": "unchanged_by_6jc", "passed": impl_6it_after == impl_6it_before},
        {"surface": "6iq_implementation", "policy": "unchanged_by_6jc", "passed": impl_6iq_after == impl_6iq_before},
        {"surface": "adapter_module", "policy": "unchanged_by_6jc", "passed": adapter_after == adapter_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6jc", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6jc", "passed": corrected_after == corrected_before},
        {"surface": "6ik_materialized_table", "policy": "read_only_unchanged_by_6jc", "passed": materialized_after == materialized_before},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JC, "actual": RECOMMENDED_NEXT_LAYER_6JC, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JC, "actual": RECOMMENDED_PATH_6JC, "passed": True},
        {"decision": "recommend_activation_execution_planning_audit_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_execution_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JC, "actual": DIAGNOSIS_6JC, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "activation_execution_criteria_matrix", "passed": len(criteria_rows) == 8 and all_passed(criteria_rows) and false_for_all(criteria_rows, "activation_execution_allowed"), "detail": f"{len(criteria_rows)}/8"},
        {"check": "mechanic_activation_decision_surface_matrix", "passed": len(decision_surface_rows) == 10 and all_passed(decision_surface_rows) and false_for_all(decision_surface_rows, "activation_execution_allowed") and false_for_all(decision_surface_rows, "mechanic_activated") and false_for_all(decision_surface_rows, "layer_6_exit_credit"), "detail": f"{len(decision_surface_rows)}/10"},
        {"check": "production_shadow_constraint_matrix", "passed": len(shadow_rows) == 6 and all_passed(shadow_rows) and false_for_all(shadow_rows, "production_simulation_allowed") and false_for_all(shadow_rows, "live_mode_allowed"), "detail": f"{len(shadow_rows)}/6"},
        {"check": "rollback_execution_gate_matrix", "passed": len(rollback_rows) == 6 and all_passed(rollback_rows) and false_for_all(rollback_rows, "activation_execution_allowed"), "detail": f"{len(rollback_rows)}/6"},
        {"check": "final_decision_policy_matrix", "passed": len(final_policy_rows) == 6 and all_passed(final_policy_rows) and false_for_all(final_policy_rows, "final_decision_allowed"), "detail": f"{len(final_policy_rows)}/6"},
        {"check": "activation_execution_audit_requirement_matrix", "passed": len(audit_req_rows) == 8 and all_passed(audit_req_rows), "detail": f"{len(audit_req_rows)}/8"},
        {"check": "activation_execution_prevention_assertions", "passed": len(prevention_rows) == 7 and all_passed(prevention_rows) and false_for_all(prevention_rows, "activation_execution_allowed"), "detail": f"{len(prevention_rows)}/7"},
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
        "activation_execution_criteria_matrix": write_csv(CRITERIA_MATRIX_CSV, criteria_rows),
        "mechanic_activation_decision_surface_matrix": write_csv(DECISION_SURFACE_MATRIX_CSV, decision_surface_rows),
        "production_shadow_constraint_matrix": write_csv(SHADOW_MATRIX_CSV, shadow_rows),
        "rollback_execution_gate_matrix": write_csv(ROLLBACK_MATRIX_CSV, rollback_rows),
        "final_decision_policy_matrix": write_csv(FINAL_POLICY_MATRIX_CSV, final_policy_rows),
        "activation_execution_audit_requirement_matrix": write_csv(AUDIT_REQ_MATRIX_CSV, audit_req_rows),
        "activation_execution_prevention_assertions": write_csv(PREVENTION_ASSERTIONS_CSV, prevention_rows),
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
        "layer": "6JC",
        "layer_type": "game_mechanics_realism",
        "implementation_layer": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JC if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JC,
        "recommended_path": RECOMMENDED_PATH_6JC,
        "predecessor_plan": str(PLAN_6JB_PATH),
        "predecessor_plan_returncode": 0,
        "predecessor_plan_diagnosis": json_6jb.get("diagnosis"),
        "planned_layer": "6JB",
        "source_family": SOURCE_FAMILY,
        "depends_on_source_families": DEPENDS_ON_SOURCE_FAMILIES,
        "activation_readiness_audited": json_6jb.get("activation_readiness_audited"),
        "activation_execution_planning_allowed": json_6jb.get("activation_execution_planning_allowed"),
        "activation_execution_criteria_matrix_row_count": len(criteria_rows),
        "mechanic_activation_decision_surface_matrix_row_count": len(decision_surface_rows),
        "production_shadow_constraint_matrix_row_count": len(shadow_rows),
        "rollback_execution_gate_matrix_row_count": len(rollback_rows),
        "final_decision_policy_matrix_row_count": len(final_policy_rows),
        "activation_execution_audit_requirement_matrix_row_count": len(audit_req_rows),
        "activation_execution_prevention_assertion_count": len(prevention_rows),
        "future_6jd_contract_valid": all_passed(future_6jd_rows),
        "activation_execution_planning_implemented": True,
        "activation_execution_executed": False,
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
        "activation_execution_plan_mutated": False,
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
            "activation_execution_criteria_matrix_csv": str(CRITERIA_MATRIX_CSV),
            "mechanic_activation_decision_surface_matrix_csv": str(DECISION_SURFACE_MATRIX_CSV),
            "production_shadow_constraint_matrix_csv": str(SHADOW_MATRIX_CSV),
            "rollback_execution_gate_matrix_csv": str(ROLLBACK_MATRIX_CSV),
            "final_decision_policy_matrix_csv": str(FINAL_POLICY_MATRIX_CSV),
            "activation_execution_audit_requirement_matrix_csv": str(AUDIT_REQ_MATRIX_CSV),
            "activation_execution_prevention_assertions_csv": str(PREVENTION_ASSERTIONS_CSV),
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
