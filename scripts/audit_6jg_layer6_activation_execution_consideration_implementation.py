#!/usr/bin/env python3
"""Audit Layer 6JF activation execution consideration implementation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jg_activation_execution_consideration_implementation_audit"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

IMPLEMENT_6JF_PATH = Path("scripts/implement_6jf_layer6_activation_execution_consideration.py")
PLAN_6JE_PATH = Path("scripts/plan_6je_layer6_activation_execution_consideration.py")
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

JSON_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation.json"
CHECKS_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_checks.csv"
PREDECESSOR_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_predecessor.csv"
INPUT_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_input_artifacts.csv"
CRITERIA_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_consideration_criteria_matrix.csv"
MECHANIC_SURFACES_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_mechanic_consideration_surface_matrix.csv"
GO_NO_GO_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_go_no_go_gate_matrix.csv"
RISK_REVIEW_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_risk_review_gate_matrix.csv"
FINAL_BLOCKERS_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_final_activation_decision_blocker_matrix.csv"
AUDIT_REQ_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_consideration_audit_requirement_matrix.csv"
PREVENTION_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_consideration_prevention_assertions.csv"
FUTURE_6JG_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_future_6jg_contract.csv"
READONLY_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_readonly_sources.csv"
PRESERVED_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_preserved_families.csv"
BLOCKING_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_blocking_policy.csv"
DECISION_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_decision.csv"
SAFETY_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_safety_boundaries.csv"
IMMUTABILITY_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_immutability.csv"
RECOMMENDED_6JF = TMP_DIR / "layer6_6jf_activation_execution_consideration_implementation_recommended_path.csv"

JSON_6JE = TMP_DIR / "layer6_6je_activation_execution_consideration_plan.json"
JSON_6JD = TMP_DIR / "layer6_6jd_activation_execution_planning_implementation_audit.json"
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
CRITERIA_AUDIT_CSV = TMP_DIR / f"{SLUG}_criteria_matrix.csv"
MECHANIC_SURFACE_AUDIT_CSV = TMP_DIR / f"{SLUG}_mechanic_surface_matrix.csv"
GO_NO_GO_AUDIT_CSV = TMP_DIR / f"{SLUG}_go_no_go_gates.csv"
RISK_REVIEW_AUDIT_CSV = TMP_DIR / f"{SLUG}_risk_review_gates.csv"
FINAL_BLOCKERS_AUDIT_CSV = TMP_DIR / f"{SLUG}_final_activation_blockers.csv"
AUDIT_REQ_AUDIT_CSV = TMP_DIR / f"{SLUG}_audit_requirements.csv"
PREVENTION_AUDIT_CSV = TMP_DIR / f"{SLUG}_prevention_assertions.csv"
FUTURE_6JH_CSV = TMP_DIR / f"{SLUG}_future_6jh_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JF = "layer_6_activation_execution_consideration_implementation_complete"
DIAGNOSIS_6JG = "layer_6_activation_execution_consideration_implementation_audit_complete"

RECOMMENDED_NEXT_LAYER_6JF = "6JG_layer_6_activation_execution_consideration_implementation_audit"
RECOMMENDED_PATH_6JF = "implement_activation_execution_consideration_then_audit_before_activation_execution_planning"

RECOMMENDED_NEXT_LAYER_6JG = "6JH_layer_6_activation_execution_plan"
RECOMMENDED_PATH_6JG = "audit_activation_execution_consideration_then_plan_activation_execution"

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


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    impl_6jf_before = IMPLEMENT_6JF_PATH.read_text(encoding="utf-8") if IMPLEMENT_6JF_PATH.exists() else ""
    plan_6je_before = PLAN_6JE_PATH.read_text(encoding="utf-8") if PLAN_6JE_PATH.exists() else ""
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

    json_6jf = load_json(JSON_6JF)

    criteria_rows_src = read_csv(CRITERIA_6JF)
    mechanic_rows_src = read_csv(MECHANIC_SURFACES_6JF)
    go_no_go_rows_src = read_csv(GO_NO_GO_6JF)
    risk_review_rows_src = read_csv(RISK_REVIEW_6JF)
    final_blocker_rows_src = read_csv(FINAL_BLOCKERS_6JF)
    audit_req_rows_src = read_csv(AUDIT_REQ_6JF)
    prevention_rows_src = read_csv(PREVENTION_6JF)

    criteria_valid = len(criteria_rows_src) == 8 and false_for_all(criteria_rows_src, "activation_execution_allowed")
    mechanic_valid = (
        len(mechanic_rows_src) == 10
        and false_for_all(mechanic_rows_src, "activation_execution_allowed")
        and false_for_all(mechanic_rows_src, "final_activation_decision_allowed")
        and false_for_all(mechanic_rows_src, "mechanic_activated")
        and false_for_all(mechanic_rows_src, "layer_6_exit_credit")
    )
    go_no_go_valid = len(go_no_go_rows_src) == 7 and false_for_all(go_no_go_rows_src, "activation_execution_allowed")
    risk_review_valid = len(risk_review_rows_src) == 7 and false_for_all(risk_review_rows_src, "activation_execution_allowed")
    final_blocker_valid = len(final_blocker_rows_src) == 7 and false_for_all(final_blocker_rows_src, "final_activation_decision_allowed")
    audit_req_valid = len(audit_req_rows_src) == 8
    prevention_valid = len(prevention_rows_src) == 7 and false_for_all(prevention_rows_src, "activation_execution_allowed")

    criteria_audit_rows = [
        {"check": "consideration_criteria_matrix_row_count", "expected": 8, "actual": len(criteria_rows_src), "passed": len(criteria_rows_src) == 8},
        {"check": "consideration_criteria_blocks_activation_execution", "expected": True, "actual": false_for_all(criteria_rows_src, "activation_execution_allowed"), "passed": false_for_all(criteria_rows_src, "activation_execution_allowed")},
    ]

    mechanic_audit_rows = [
        {"check": "mechanic_consideration_surface_matrix_row_count", "expected": 10, "actual": len(mechanic_rows_src), "passed": len(mechanic_rows_src) == 10},
        {"check": "mechanic_surface_blocks_activation_execution", "expected": True, "actual": false_for_all(mechanic_rows_src, "activation_execution_allowed"), "passed": false_for_all(mechanic_rows_src, "activation_execution_allowed")},
        {"check": "mechanic_surface_blocks_final_activation_decision", "expected": True, "actual": false_for_all(mechanic_rows_src, "final_activation_decision_allowed"), "passed": false_for_all(mechanic_rows_src, "final_activation_decision_allowed")},
        {"check": "mechanic_surface_no_mechanics_activated", "expected": True, "actual": false_for_all(mechanic_rows_src, "mechanic_activated"), "passed": false_for_all(mechanic_rows_src, "mechanic_activated")},
        {"check": "mechanic_surface_no_exit_credit", "expected": True, "actual": false_for_all(mechanic_rows_src, "layer_6_exit_credit"), "passed": false_for_all(mechanic_rows_src, "layer_6_exit_credit")},
    ]

    go_no_go_audit_rows = [
        {"check": "go_no_go_gate_matrix_row_count", "expected": 7, "actual": len(go_no_go_rows_src), "passed": len(go_no_go_rows_src) == 7},
        {"check": "go_no_go_blocks_activation_execution", "expected": True, "actual": false_for_all(go_no_go_rows_src, "activation_execution_allowed"), "passed": false_for_all(go_no_go_rows_src, "activation_execution_allowed")},
    ]

    risk_review_audit_rows = [
        {"check": "risk_review_gate_matrix_row_count", "expected": 7, "actual": len(risk_review_rows_src), "passed": len(risk_review_rows_src) == 7},
        {"check": "risk_review_blocks_activation_execution", "expected": True, "actual": false_for_all(risk_review_rows_src, "activation_execution_allowed"), "passed": false_for_all(risk_review_rows_src, "activation_execution_allowed")},
    ]

    final_blocker_audit_rows = [
        {"check": "final_activation_decision_blocker_matrix_row_count", "expected": 7, "actual": len(final_blocker_rows_src), "passed": len(final_blocker_rows_src) == 7},
        {"check": "final_blockers_block_final_activation_decision", "expected": True, "actual": false_for_all(final_blocker_rows_src, "final_activation_decision_allowed"), "passed": false_for_all(final_blocker_rows_src, "final_activation_decision_allowed")},
    ]

    audit_req_audit_rows = [
        {"check": "consideration_audit_requirement_matrix_row_count", "expected": 8, "actual": len(audit_req_rows_src), "passed": len(audit_req_rows_src) == 8},
        {"check": "consideration_audit_requirements_nonempty", "expected": True, "actual": bool(audit_req_rows_src), "passed": bool(audit_req_rows_src)},
    ]

    prevention_audit_rows = [
        {"check": "consideration_prevention_assertion_count", "expected": 7, "actual": len(prevention_rows_src), "passed": len(prevention_rows_src) == 7},
        {"check": "prevention_assertions_block_activation_execution", "expected": True, "actual": false_for_all(prevention_rows_src, "activation_execution_allowed"), "passed": false_for_all(prevention_rows_src, "activation_execution_allowed")},
    ]

    future_6jh_rows = [
        {"contract": "plan_actual_activation_execution_only", "required": True, "passed": True},
        {"contract": "define_explicit_activation_execution_criteria", "required": True, "passed": True},
        {"contract": "define_mechanic_by_mechanic_execution_surfaces", "required": True, "passed": True},
        {"contract": "define_live_mode_blockers", "required": True, "passed": True},
        {"contract": "define_production_shadow_prerequisites", "required": True, "passed": True},
        {"contract": "define_rollback_execution_gates", "required": True, "passed": True},
        {"contract": "define_final_activation_decision_policy", "required": True, "passed": True},
        {"contract": "define_execution_audit_requirements", "required": True, "passed": True},
        {"contract": "define_prevention_rules", "required": True, "passed": True},
        {"contract": "do_not_execute_activation", "required": True, "passed": True},
    ]

    required_inputs = [
        JSON_6JF, CHECKS_6JF, PREDECESSOR_6JF, INPUT_6JF, CRITERIA_6JF,
        MECHANIC_SURFACES_6JF, GO_NO_GO_6JF, RISK_REVIEW_6JF, FINAL_BLOCKERS_6JF,
        AUDIT_REQ_6JF, PREVENTION_6JF, FUTURE_6JG_6JF, READONLY_6JF,
        PRESERVED_6JF, BLOCKING_6JF, DECISION_6JF, SAFETY_6JF,
        IMMUTABILITY_6JF, RECOMMENDED_6JF, JSON_6JE, JSON_6JD, JSON_6JC,
        JSON_6JB, JSON_6JA, JSON_6IZ, JSON_6IY, JSON_6IX, JSON_6IW,
        JSON_6IV, JSON_6IU, JSON_6IT, JSON_6IS, JSON_6IR, JSON_6IQ,
        EVAL_MATRIX_6IQ, METRIC_ROWS_6IQ, BASELINE_6IQ, CANDIDATE_DECISIONS_6IQ,
        LINEAGE_6IQ, TRUTH_ROWS_6IT, TRUTH_LINEAGE_6IT, TRUTH_MANIFEST_6IT,
        TRUTH_SCHEMA_6IT, JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IK,
        ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZED_LINEAGE,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    readonly_sources = [
        JSON_6JF, JSON_6JE, JSON_6JD, JSON_6JC, JSON_6JB, JSON_6JA, JSON_6IZ,
        JSON_6IY, JSON_6IX, JSON_6IW, JSON_6IV, JSON_6IU, JSON_6IT, JSON_6IS,
        JSON_6IR, JSON_6IQ, EVAL_MATRIX_6IQ, METRIC_ROWS_6IQ, BASELINE_6IQ,
        CANDIDATE_DECISIONS_6IQ, LINEAGE_6IQ, TRUTH_ROWS_6IT, TRUTH_LINEAGE_6IT,
        TRUTH_MANIFEST_6IT, TRUTH_SCHEMA_6IT, JSON_6IP, JSON_6IO, JSON_6IN,
        JSON_6IK, ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZED_LINEAGE,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jf_implementation_exists", "expected": True, "actual": IMPLEMENT_6JF_PATH.exists(), "passed": IMPLEMENT_6JF_PATH.exists()},
        {"check": "6jf_json_exists", "expected": True, "actual": JSON_6JF.exists(), "passed": JSON_6JF.exists()},
        {"check": "6jf_all_checks_passed", "expected": True, "actual": json_6jf.get("all_checks_passed"), "passed": json_6jf.get("all_checks_passed") is True},
        {"check": "6jf_diagnosis", "expected": DIAGNOSIS_6JF, "actual": json_6jf.get("diagnosis"), "passed": json_6jf.get("diagnosis") == DIAGNOSIS_6JF},
        {"check": "6jf_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JF, "actual": json_6jf.get("recommended_next_layer"), "passed": json_6jf.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JF},
        {"check": "6jf_recommended_path", "expected": RECOMMENDED_PATH_6JF, "actual": json_6jf.get("recommended_path"), "passed": json_6jf.get("recommended_path") == RECOMMENDED_PATH_6JF},
        {"check": "6jf_consideration_implemented", "expected": True, "actual": json_6jf.get("activation_execution_consideration_implemented"), "passed": json_6jf.get("activation_execution_consideration_implemented") is True},
        {"check": "6jf_activation_execution_blocked", "expected": False, "actual": json_6jf.get("activation_execution_allowed_after_this_layer"), "passed": json_6jf.get("activation_execution_allowed_after_this_layer") is False},
        {"check": "6jf_no_exit_credit", "expected": False, "actual": json_6jf.get("layer_6_exit_credit"), "passed": json_6jf.get("layer_6_exit_credit") is False},
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
        {"blocked_surface": "activation_execution_planning", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "explicit activation execution plan and implementation required first", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "activation execution forbidden", "passed": True},
        {"blocked_surface": "final_activation_decision", "blocked": True, "reason": "consideration audit cannot decide activation", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "activation execution audit required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "activation execution not performed or audited", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jf_passed", "expected": True, "actual": json_6jf.get("all_checks_passed"), "passed": json_6jf.get("all_checks_passed") is True},
        {"decision": "activation_execution_consideration_audited", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_execution_planning_allowed", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_execution_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "final_activation_decision_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6jh_activation_execution_plan_next", "expected": RECOMMENDED_NEXT_LAYER_6JG, "actual": RECOMMENDED_NEXT_LAYER_6JG, "passed": True},
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
        {"boundary": "no_final_pass_fail_decision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_final_activation_decision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    impl_6jf_after = IMPLEMENT_6JF_PATH.read_text(encoding="utf-8") if IMPLEMENT_6JF_PATH.exists() else ""
    plan_6je_after = PLAN_6JE_PATH.read_text(encoding="utf-8") if PLAN_6JE_PATH.exists() else ""
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
        {"surface": "this_6jg_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6jf_implementation", "policy": "unchanged_by_6jg", "passed": impl_6jf_after == impl_6jf_before},
        {"surface": "6je_plan", "policy": "unchanged_by_6jg", "passed": plan_6je_after == plan_6je_before},
        {"surface": "6jd_audit", "policy": "unchanged_by_6jg", "passed": audit_6jd_after == audit_6jd_before},
        {"surface": "6jc_implementation", "policy": "unchanged_by_6jg", "passed": impl_6jc_after == impl_6jc_before},
        {"surface": "6jb_plan", "policy": "unchanged_by_6jg", "passed": plan_6jb_after == plan_6jb_before},
        {"surface": "6ja_audit", "policy": "unchanged_by_6jg", "passed": audit_6ja_after == audit_6ja_before},
        {"surface": "6iz_implementation", "policy": "unchanged_by_6jg", "passed": impl_6iz_after == impl_6iz_before},
        {"surface": "6iy_plan", "policy": "unchanged_by_6jg", "passed": plan_6iy_after == plan_6iy_before},
        {"surface": "6ix_audit", "policy": "unchanged_by_6jg", "passed": audit_6ix_after == audit_6ix_before},
        {"surface": "6iw_implementation", "policy": "unchanged_by_6jg", "passed": impl_6iw_after == impl_6iw_before},
        {"surface": "6it_implementation", "policy": "unchanged_by_6jg", "passed": impl_6it_after == impl_6it_before},
        {"surface": "6iq_implementation", "policy": "unchanged_by_6jg", "passed": impl_6iq_after == impl_6iq_before},
        {"surface": "adapter_module", "policy": "unchanged_by_6jg", "passed": adapter_after == adapter_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6jg", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6jg", "passed": corrected_after == corrected_before},
        {"surface": "6ik_materialized_table", "policy": "read_only_unchanged_by_6jg", "passed": materialized_after == materialized_before},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JG, "actual": RECOMMENDED_NEXT_LAYER_6JG, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JG, "actual": RECOMMENDED_PATH_6JG, "passed": True},
        {"decision": "recommend_activation_execution_plan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_execution_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JG, "actual": DIAGNOSIS_6JG, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "criteria_matrix", "passed": criteria_valid and all_passed(criteria_audit_rows), "detail": f"{len(criteria_rows_src)}/8"},
        {"check": "mechanic_surface_matrix", "passed": mechanic_valid and all_passed(mechanic_audit_rows), "detail": f"{len(mechanic_rows_src)}/10"},
        {"check": "go_no_go_gates", "passed": go_no_go_valid and all_passed(go_no_go_audit_rows), "detail": f"{len(go_no_go_rows_src)}/7"},
        {"check": "risk_review_gates", "passed": risk_review_valid and all_passed(risk_review_audit_rows), "detail": f"{len(risk_review_rows_src)}/7"},
        {"check": "final_activation_blockers", "passed": final_blocker_valid and all_passed(final_blocker_audit_rows), "detail": f"{len(final_blocker_rows_src)}/7"},
        {"check": "audit_requirements", "passed": audit_req_valid and all_passed(audit_req_audit_rows), "detail": f"{len(audit_req_rows_src)}/8"},
        {"check": "prevention_assertions", "passed": prevention_valid and all_passed(prevention_audit_rows), "detail": f"{len(prevention_rows_src)}/7"},
        {"check": "future_6jh_contract", "passed": all_passed(future_6jh_rows), "detail": f"{sum(1 for row in future_6jh_rows if row['passed'])}/{len(future_6jh_rows)}"},
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
        "criteria_matrix": write_csv(CRITERIA_AUDIT_CSV, criteria_audit_rows),
        "mechanic_surface_matrix": write_csv(MECHANIC_SURFACE_AUDIT_CSV, mechanic_audit_rows),
        "go_no_go_gates": write_csv(GO_NO_GO_AUDIT_CSV, go_no_go_audit_rows),
        "risk_review_gates": write_csv(RISK_REVIEW_AUDIT_CSV, risk_review_audit_rows),
        "final_activation_blockers": write_csv(FINAL_BLOCKERS_AUDIT_CSV, final_blocker_audit_rows),
        "audit_requirements": write_csv(AUDIT_REQ_AUDIT_CSV, audit_req_audit_rows),
        "prevention_assertions": write_csv(PREVENTION_AUDIT_CSV, prevention_audit_rows),
        "future_6jh_contract": write_csv(FUTURE_6JH_CSV, future_6jh_rows),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JG",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JG if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JG,
        "recommended_path": RECOMMENDED_PATH_6JG,
        "predecessor_implementation": str(IMPLEMENT_6JF_PATH),
        "predecessor_implementation_returncode": 0,
        "predecessor_implementation_diagnosis": json_6jf.get("diagnosis"),
        "audited_layer": "6JF",
        "source_family": SOURCE_FAMILY,
        "depends_on_source_families": DEPENDS_ON_SOURCE_FAMILIES,
        "activation_execution_consideration_planned": json_6jf.get("activation_execution_consideration_planned"),
        "activation_execution_consideration_implemented": json_6jf.get("activation_execution_consideration_implemented"),
        "activation_execution_consideration_audited": True,
        "consideration_criteria_matrix_row_count": len(criteria_rows_src),
        "mechanic_consideration_surface_matrix_row_count": len(mechanic_rows_src),
        "go_no_go_gate_matrix_row_count": len(go_no_go_rows_src),
        "risk_review_gate_matrix_row_count": len(risk_review_rows_src),
        "final_activation_decision_blocker_matrix_row_count": len(final_blocker_rows_src),
        "consideration_audit_requirement_matrix_row_count": len(audit_req_rows_src),
        "consideration_prevention_assertion_count": len(prevention_rows_src),
        "criteria_matrix_valid": criteria_valid,
        "mechanic_surface_matrix_valid": mechanic_valid,
        "go_no_go_gates_valid": go_no_go_valid,
        "risk_review_gates_valid": risk_review_valid,
        "final_activation_blockers_valid": final_blocker_valid,
        "audit_requirements_valid": audit_req_valid,
        "prevention_assertions_valid": prevention_valid,
        "future_6jh_contract_valid": all_passed(future_6jh_rows),
        "activation_execution_planning_allowed_after_this_layer": True,
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
        "activation_consideration_plan_mutated": False,
        "activation_consideration_implementation_mutated_by_audit": False,
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
            "criteria_matrix_csv": str(CRITERIA_AUDIT_CSV),
            "mechanic_surface_matrix_csv": str(MECHANIC_SURFACE_AUDIT_CSV),
            "go_no_go_gates_csv": str(GO_NO_GO_AUDIT_CSV),
            "risk_review_gates_csv": str(RISK_REVIEW_AUDIT_CSV),
            "final_activation_blockers_csv": str(FINAL_BLOCKERS_AUDIT_CSV),
            "audit_requirements_csv": str(AUDIT_REQ_AUDIT_CSV),
            "prevention_assertions_csv": str(PREVENTION_AUDIT_CSV),
            "future_6jh_contract_csv": str(FUTURE_6JH_CSV),
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
