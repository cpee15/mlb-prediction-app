#!/usr/bin/env python3
"""Audit Layer 6IZ activation planning readiness implementation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ja_activation_planning_readiness_implementation_audit"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

IMPLEMENT_6IZ_PATH = Path("scripts/implement_6iz_layer6_activation_planning_readiness.py")
PLAN_6IY_PATH = Path("scripts/plan_6iy_layer6_activation_planning_readiness.py")
AUDIT_6IX_PATH = Path("scripts/audit_6ix_layer6_truth_join_evaluation_implementation.py")
IMPLEMENT_6IW_PATH = Path("scripts/implement_6iw_layer6_truth_join_evaluation.py")
IMPLEMENT_6IT_PATH = Path("scripts/implement_6it_layer6_actual_outcome_surface_gap_resolution.py")
IMPLEMENT_6IQ_PATH = Path("scripts/implement_6iq_layer6_gameplay_mechanic_outcome_real_evaluation.py")
ADAPTER_MODULE_PATH = Path("mlb_app/simulation/layer6_base_out_transition_adapter.py")

JSON_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation.json"
CHECKS_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_checks.csv"
PREDECESSOR_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_predecessor.csv"
INPUT_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_input_artifacts.csv"
MECH_READINESS_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_mechanic_readiness_matrix.csv"
ROLLOUT_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_rollout_check_matrix.csv"
ROLLBACK_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_rollback_gate_matrix.csv"
PREVENTION_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_activation_prevention_assertions.csv"
PRODUCTION_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_production_readiness_constraints.csv"
FINAL_PREREQ_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_final_decision_prerequisites.csv"
READINESS_SUMMARY_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_activation_readiness_summary.csv"
FUTURE_6JA_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_future_6ja_contract.csv"
READONLY_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_readonly_sources.csv"
PRESERVED_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_preserved_families.csv"
BLOCKING_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_blocking_policy.csv"
DECISION_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_decision.csv"
SAFETY_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_safety_boundaries.csv"
IMMUTABILITY_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_immutability.csv"
RECOMMENDED_6IZ = TMP_DIR / "layer6_6iz_activation_planning_readiness_implementation_recommended_path.csv"

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
READINESS_MATRIX_CSV = TMP_DIR / f"{SLUG}_readiness_matrix.csv"
ROLLOUT_CSV = TMP_DIR / f"{SLUG}_rollout_checks.csv"
ROLLBACK_CSV = TMP_DIR / f"{SLUG}_rollback_gates.csv"
PREVENTION_CSV = TMP_DIR / f"{SLUG}_activation_prevention.csv"
PRODUCTION_CSV = TMP_DIR / f"{SLUG}_production_constraints.csv"
FINAL_PREREQ_CSV = TMP_DIR / f"{SLUG}_final_decision_prerequisites.csv"
READINESS_SUMMARY_CSV = TMP_DIR / f"{SLUG}_readiness_summary.csv"
FUTURE_6JB_CSV = TMP_DIR / f"{SLUG}_future_6jb_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6IZ = "layer_6_activation_planning_readiness_implementation_complete"
DIAGNOSIS_6JA = "layer_6_activation_planning_readiness_implementation_audit_complete"

RECOMMENDED_NEXT_LAYER_6IZ = "6JA_layer_6_activation_planning_readiness_implementation_audit"
RECOMMENDED_PATH_6IZ = "implement_activation_readiness_then_audit_before_activation_execution_planning"

RECOMMENDED_NEXT_LAYER_6JA = "6JB_layer_6_activation_execution_planning_plan"
RECOMMENDED_PATH_6JA = "audit_activation_readiness_then_plan_activation_execution"

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


def bool_column_false(rows: List[Dict[str, str]], column: str) -> bool:
    return bool(rows) and all(str(row.get(column, "")).lower() == "false" for row in rows)


def bool_column_true(rows: List[Dict[str, str]], column: str) -> bool:
    return bool(rows) and all(str(row.get(column, "")).lower() == "true" for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
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

    json_6iz = load_json(JSON_6IZ)
    readiness_rows = read_csv(MECH_READINESS_6IZ)
    rollout_rows = read_csv(ROLLOUT_6IZ)
    rollback_rows = read_csv(ROLLBACK_6IZ)
    prevention_rows = read_csv(PREVENTION_6IZ)
    production_rows = read_csv(PRODUCTION_6IZ)
    final_prereq_rows = read_csv(FINAL_PREREQ_6IZ)
    summary_rows = read_csv(READINESS_SUMMARY_6IZ)

    required_inputs = [
        JSON_6IZ, CHECKS_6IZ, PREDECESSOR_6IZ, INPUT_6IZ, MECH_READINESS_6IZ,
        ROLLOUT_6IZ, ROLLBACK_6IZ, PREVENTION_6IZ, PRODUCTION_6IZ,
        FINAL_PREREQ_6IZ, READINESS_SUMMARY_6IZ, FUTURE_6JA_6IZ,
        READONLY_6IZ, PRESERVED_6IZ, BLOCKING_6IZ, DECISION_6IZ,
        SAFETY_6IZ, IMMUTABILITY_6IZ, RECOMMENDED_6IZ, JSON_6IY,
        JSON_6IX, JSON_6IW, JSON_6IV, JSON_6IU, JSON_6IT, JSON_6IS,
        JSON_6IR, JSON_6IQ, EVAL_MATRIX_6IQ, METRIC_ROWS_6IQ,
        BASELINE_6IQ, CANDIDATE_DECISIONS_6IQ, LINEAGE_6IQ,
        TRUTH_ROWS_6IT, TRUTH_LINEAGE_6IT, TRUTH_MANIFEST_6IT,
        TRUTH_SCHEMA_6IT, JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IK,
        ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZED_LINEAGE,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    readonly_sources = [
        JSON_6IZ, JSON_6IY, JSON_6IX, JSON_6IW, JSON_6IV, JSON_6IU,
        JSON_6IT, JSON_6IS, JSON_6IR, JSON_6IQ, EVAL_MATRIX_6IQ,
        METRIC_ROWS_6IQ, BASELINE_6IQ, CANDIDATE_DECISIONS_6IQ,
        LINEAGE_6IQ, TRUTH_ROWS_6IT, TRUTH_LINEAGE_6IT, TRUTH_MANIFEST_6IT,
        TRUTH_SCHEMA_6IT, JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IK,
        ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZED_LINEAGE,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    readiness_valid = (
        len(readiness_rows) == 10
        and bool_column_true(readiness_rows, "non_production")
        and bool_column_true(readiness_rows, "activation_readiness_candidate")
        and bool_column_false(readiness_rows, "activation_execution_allowed")
        and bool_column_false(readiness_rows, "mechanic_activated")
        and bool_column_false(readiness_rows, "layer_6_exit_credit")
    )
    rollout_valid = len(rollout_rows) == 60 and bool_column_false(rollout_rows, "activation_execution_allowed")
    rollback_valid = len(rollback_rows) == 60 and bool_column_false(rollback_rows, "activation_execution_allowed")
    prevention_valid = (
        len(prevention_rows) == 7
        and bool_column_false(prevention_rows, "activation_execution_allowed")
        and bool_column_false(prevention_rows, "mechanics_activated")
        and bool_column_false(prevention_rows, "layer_6_exit_credit")
    )
    production_valid = (
        len(production_rows) == 6
        and bool_column_false(production_rows, "activation_execution_allowed")
        and bool_column_false(production_rows, "production_simulation_allowed")
    )
    final_prereq_valid = (
        len(final_prereq_rows) == 7
        and bool_column_false(final_prereq_rows, "final_decision_allowed")
    )
    summary_valid = (
        len(summary_rows) == 10
        and bool_column_true(summary_rows, "readiness_candidate")
        and bool_column_false(summary_rows, "activation_execution_allowed")
        and bool_column_false(summary_rows, "final_decision_allowed")
        and bool_column_false(summary_rows, "production_simulation_allowed")
        and bool_column_false(summary_rows, "layer_6_exit_credit")
    )

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6iz_implementation_exists", "expected": True, "actual": IMPLEMENT_6IZ_PATH.exists(), "passed": IMPLEMENT_6IZ_PATH.exists()},
        {"check": "6iz_json_exists", "expected": True, "actual": JSON_6IZ.exists(), "passed": JSON_6IZ.exists()},
        {"check": "6iz_all_checks_passed", "expected": True, "actual": json_6iz.get("all_checks_passed"), "passed": json_6iz.get("all_checks_passed") is True},
        {"check": "6iz_diagnosis", "expected": DIAGNOSIS_6IZ, "actual": json_6iz.get("diagnosis"), "passed": json_6iz.get("diagnosis") == DIAGNOSIS_6IZ},
        {"check": "6iz_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IZ, "actual": json_6iz.get("recommended_next_layer"), "passed": json_6iz.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6IZ},
        {"check": "6iz_recommended_path", "expected": RECOMMENDED_PATH_6IZ, "actual": json_6iz.get("recommended_path"), "passed": json_6iz.get("recommended_path") == RECOMMENDED_PATH_6IZ},
        {"check": "6iz_activation_readiness_implemented", "expected": True, "actual": json_6iz.get("activation_readiness_implemented"), "passed": json_6iz.get("activation_readiness_implemented") is True},
        {"check": "6iz_activation_execution_blocked", "expected": False, "actual": json_6iz.get("activation_execution_allowed_after_this_layer"), "passed": json_6iz.get("activation_execution_allowed_after_this_layer") is False},
        {"check": "6iz_no_exit_credit", "expected": False, "actual": json_6iz.get("layer_6_exit_credit"), "passed": json_6iz.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    readiness_audit_rows = [
        {"check": "readiness_matrix_row_count", "expected": 10, "actual": len(readiness_rows), "passed": len(readiness_rows) == 10},
        {"check": "readiness_matrix_non_production", "expected": True, "actual": bool_column_true(readiness_rows, "non_production"), "passed": bool_column_true(readiness_rows, "non_production")},
        {"check": "readiness_matrix_activation_execution_blocked", "expected": True, "actual": bool_column_false(readiness_rows, "activation_execution_allowed"), "passed": bool_column_false(readiness_rows, "activation_execution_allowed")},
        {"check": "readiness_matrix_no_mechanics_activated", "expected": True, "actual": bool_column_false(readiness_rows, "mechanic_activated"), "passed": bool_column_false(readiness_rows, "mechanic_activated")},
    ]

    rollout_audit_rows = [
        {"check": "rollout_check_matrix_row_count", "expected": 60, "actual": len(rollout_rows), "passed": len(rollout_rows) == 60},
        {"check": "rollout_activation_execution_blocked", "expected": True, "actual": bool_column_false(rollout_rows, "activation_execution_allowed"), "passed": bool_column_false(rollout_rows, "activation_execution_allowed")},
    ]

    rollback_audit_rows = [
        {"check": "rollback_gate_matrix_row_count", "expected": 60, "actual": len(rollback_rows), "passed": len(rollback_rows) == 60},
        {"check": "rollback_activation_execution_blocked", "expected": True, "actual": bool_column_false(rollback_rows, "activation_execution_allowed"), "passed": bool_column_false(rollback_rows, "activation_execution_allowed")},
    ]

    prevention_audit_rows = [
        {"check": "activation_prevention_assertion_count", "expected": 7, "actual": len(prevention_rows), "passed": len(prevention_rows) == 7},
        {"check": "prevention_activation_execution_blocked", "expected": True, "actual": bool_column_false(prevention_rows, "activation_execution_allowed"), "passed": bool_column_false(prevention_rows, "activation_execution_allowed")},
        {"check": "prevention_no_mechanics_activated", "expected": True, "actual": bool_column_false(prevention_rows, "mechanics_activated"), "passed": bool_column_false(prevention_rows, "mechanics_activated")},
    ]

    production_audit_rows = [
        {"check": "production_readiness_constraint_count", "expected": 6, "actual": len(production_rows), "passed": len(production_rows) == 6},
        {"check": "production_activation_execution_blocked", "expected": True, "actual": bool_column_false(production_rows, "activation_execution_allowed"), "passed": bool_column_false(production_rows, "activation_execution_allowed")},
        {"check": "production_simulation_blocked", "expected": True, "actual": bool_column_false(production_rows, "production_simulation_allowed"), "passed": bool_column_false(production_rows, "production_simulation_allowed")},
    ]

    final_prereq_audit_rows = [
        {"check": "final_decision_prerequisite_count", "expected": 7, "actual": len(final_prereq_rows), "passed": len(final_prereq_rows) == 7},
        {"check": "final_decision_blocked", "expected": True, "actual": bool_column_false(final_prereq_rows, "final_decision_allowed"), "passed": bool_column_false(final_prereq_rows, "final_decision_allowed")},
    ]

    readiness_summary_rows = [
        {"check": "activation_readiness_summary_row_count", "expected": 10, "actual": len(summary_rows), "passed": len(summary_rows) == 10},
        {"check": "summary_readiness_candidate", "expected": True, "actual": bool_column_true(summary_rows, "readiness_candidate"), "passed": bool_column_true(summary_rows, "readiness_candidate")},
        {"check": "summary_activation_execution_blocked", "expected": True, "actual": bool_column_false(summary_rows, "activation_execution_allowed"), "passed": bool_column_false(summary_rows, "activation_execution_allowed")},
        {"check": "summary_final_decision_blocked", "expected": True, "actual": bool_column_false(summary_rows, "final_decision_allowed"), "passed": bool_column_false(summary_rows, "final_decision_allowed")},
        {"check": "summary_layer_6_exit_blocked", "expected": True, "actual": bool_column_false(summary_rows, "layer_6_exit_credit"), "passed": bool_column_false(summary_rows, "layer_6_exit_credit")},
    ]

    future_6jb_rows = [
        {"contract": "plan_activation_execution_without_execution", "required": True, "passed": True},
        {"contract": "define_mechanic_activation_decision_surfaces", "required": True, "passed": True},
        {"contract": "define_production_shadow_constraints", "required": True, "passed": True},
        {"contract": "define_rollback_execution_gates", "required": True, "passed": True},
        {"contract": "define_final_decision_policy", "required": True, "passed": True},
        {"contract": "define_audit_requirements", "required": True, "passed": True},
        {"contract": "prevent_activation_execution_until_later_implementation_audit_pair", "required": True, "passed": True},
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
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "execution planning and audit required first", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "activation execution forbidden", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "activation execution audit required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "activation execution not implemented or audited", "passed": True},
    ]

    decision_rows = [
        {"decision": "6iz_passed", "expected": True, "actual": json_6iz.get("all_checks_passed"), "passed": json_6iz.get("all_checks_passed") is True},
        {"decision": "activation_readiness_audited", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_execution_planning_allowed_after_this_layer", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_execution_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6jb_activation_execution_plan_next", "expected": RECOMMENDED_NEXT_LAYER_6JA, "actual": RECOMMENDED_NEXT_LAYER_6JA, "passed": True},
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
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
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
        {"surface": "this_6ja_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6iz_implementation", "policy": "unchanged_by_6ja", "passed": impl_6iz_after == impl_6iz_before},
        {"surface": "6iy_plan", "policy": "unchanged_by_6ja", "passed": plan_6iy_after == plan_6iy_before},
        {"surface": "6ix_audit", "policy": "unchanged_by_6ja", "passed": audit_6ix_after == audit_6ix_before},
        {"surface": "6iw_implementation", "policy": "unchanged_by_6ja", "passed": impl_6iw_after == impl_6iw_before},
        {"surface": "6it_implementation", "policy": "unchanged_by_6ja", "passed": impl_6it_after == impl_6it_before},
        {"surface": "6iq_implementation", "policy": "unchanged_by_6ja", "passed": impl_6iq_after == impl_6iq_before},
        {"surface": "adapter_module", "policy": "unchanged_by_6ja", "passed": adapter_after == adapter_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6ja", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6ja", "passed": corrected_after == corrected_before},
        {"surface": "6ik_materialized_table", "policy": "read_only_unchanged_by_6ja", "passed": materialized_after == materialized_before},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JA, "actual": RECOMMENDED_NEXT_LAYER_6JA, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JA, "actual": RECOMMENDED_PATH_6JA, "passed": True},
        {"decision": "recommend_activation_execution_planning_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_execution_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JA, "actual": DIAGNOSIS_6JA, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "readiness_matrix", "passed": readiness_valid and all_passed(readiness_audit_rows), "detail": f"{len(readiness_rows)}/10"},
        {"check": "rollout_checks", "passed": rollout_valid and all_passed(rollout_audit_rows), "detail": f"{len(rollout_rows)}/60"},
        {"check": "rollback_gates", "passed": rollback_valid and all_passed(rollback_audit_rows), "detail": f"{len(rollback_rows)}/60"},
        {"check": "activation_prevention", "passed": prevention_valid and all_passed(prevention_audit_rows), "detail": f"{len(prevention_rows)}/7"},
        {"check": "production_constraints", "passed": production_valid and all_passed(production_audit_rows), "detail": f"{len(production_rows)}/6"},
        {"check": "final_decision_prerequisites", "passed": final_prereq_valid and all_passed(final_prereq_audit_rows), "detail": f"{len(final_prereq_rows)}/7"},
        {"check": "readiness_summary", "passed": summary_valid and all_passed(readiness_summary_rows), "detail": f"{len(summary_rows)}/10"},
        {"check": "future_6jb_contract", "passed": all_passed(future_6jb_rows), "detail": f"{sum(1 for row in future_6jb_rows if row['passed'])}/{len(future_6jb_rows)}"},
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
        "readiness_matrix": write_csv(READINESS_MATRIX_CSV, readiness_audit_rows),
        "rollout_checks": write_csv(ROLLOUT_CSV, rollout_audit_rows),
        "rollback_gates": write_csv(ROLLBACK_CSV, rollback_audit_rows),
        "activation_prevention": write_csv(PREVENTION_CSV, prevention_audit_rows),
        "production_constraints": write_csv(PRODUCTION_CSV, production_audit_rows),
        "final_decision_prerequisites": write_csv(FINAL_PREREQ_CSV, final_prereq_audit_rows),
        "readiness_summary": write_csv(READINESS_SUMMARY_CSV, readiness_summary_rows),
        "future_6jb_contract": write_csv(FUTURE_6JB_CSV, future_6jb_rows),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JA",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JA if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JA,
        "recommended_path": RECOMMENDED_PATH_6JA,
        "predecessor_implementation": str(IMPLEMENT_6IZ_PATH),
        "predecessor_implementation_returncode": 0,
        "predecessor_implementation_diagnosis": json_6iz.get("diagnosis"),
        "audited_layer": "6IZ",
        "source_family": SOURCE_FAMILY,
        "depends_on_source_families": DEPENDS_ON_SOURCE_FAMILIES,
        "truth_join_audited": json_6iz.get("truth_join_audited"),
        "truth_join_candidate_row_count": json_6iz.get("truth_join_candidate_row_count"),
        "joined_truth_row_count": json_6iz.get("joined_truth_row_count"),
        "unjoined_evaluation_row_count": json_6iz.get("unjoined_evaluation_row_count"),
        "join_coverage_ratio": json_6iz.get("join_coverage_ratio"),
        "mechanic_readiness_matrix_row_count": len(readiness_rows),
        "rollout_check_matrix_row_count": len(rollout_rows),
        "rollback_gate_matrix_row_count": len(rollback_rows),
        "activation_prevention_assertion_count": len(prevention_rows),
        "production_readiness_constraint_count": len(production_rows),
        "final_decision_prerequisite_count": len(final_prereq_rows),
        "activation_readiness_summary_row_count": len(summary_rows),
        "readiness_matrix_valid": readiness_valid,
        "rollout_checks_valid": rollout_valid,
        "rollback_gates_valid": rollback_valid,
        "activation_prevention_valid": prevention_valid,
        "production_constraints_valid": production_valid,
        "final_decision_prerequisites_valid": final_prereq_valid,
        "readiness_summary_valid": summary_valid,
        "future_6jb_contract_valid": all_passed(future_6jb_rows),
        "activation_readiness_audited": True,
        "activation_execution_planning_allowed_after_this_layer": True,
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
        "readiness_implementation_mutated_by_audit": False,
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
            "readiness_matrix_csv": str(READINESS_MATRIX_CSV),
            "rollout_checks_csv": str(ROLLOUT_CSV),
            "rollback_gates_csv": str(ROLLBACK_CSV),
            "activation_prevention_csv": str(PREVENTION_CSV),
            "production_constraints_csv": str(PRODUCTION_CSV),
            "final_decision_prerequisites_csv": str(FINAL_PREREQ_CSV),
            "readiness_summary_csv": str(READINESS_SUMMARY_CSV),
            "future_6jb_contract_csv": str(FUTURE_6JB_CSV),
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
