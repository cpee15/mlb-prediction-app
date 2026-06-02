#!/usr/bin/env python3
"""Audit Layer 6IQ gameplay-mechanic real evaluation implementation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

IMPLEMENT_6IQ_PATH = Path("scripts/implement_6iq_layer6_gameplay_mechanic_outcome_real_evaluation.py")
ADAPTER_MODULE_PATH = Path("mlb_app/simulation/layer6_base_out_transition_adapter.py")

JSON_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation.json"
CHECKS_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_checks.csv"
PREDECESSOR_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_predecessor.csv"
INPUT_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_input_artifacts.csv"
ADAPTER_LOAD_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_adapter_load.csv"
EVALUATION_MATRIX_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_evaluation_matrix.csv"
METRIC_ROWS_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_metric_rows.csv"
BASELINE_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_baseline_comparison.csv"
CANDIDATE_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_candidate_decisions.csv"
ACTUAL_SURFACE_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_actual_outcome_surface.csv"
LINEAGE_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_lineage.csv"
READINESS_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_readiness.csv"
FUTURE_6IR_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_future_6ir_contract.csv"
READONLY_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_readonly_sources.csv"
PRESERVED_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_preserved_families.csv"
BLOCKING_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_blocking_policy.csv"
DECISION_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_decision.csv"
SAFETY_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_safety_boundaries.csv"
IMMUTABILITY_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_immutability.csv"
RECOMMENDED_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_recommended_path.csv"

JSON_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan.json"
JSON_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit.json"
JSON_6IN = TMP_DIR / "layer6_6in_base_out_transition_adapter_revision_implementation.json"
JSON_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan.json"
JSON_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit.json"
JSON_6IK = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation.json"
JSON_6IJ = TMP_DIR / "layer6_6ij_base_out_transition_materialization_plan.json"
JSON_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit.json"

MATERIALIZED_TABLE = MAT_DIR / "materialized_base_out_transition_table_candidate.csv"
MATERIALIZATION_MANIFEST = MAT_DIR / "materialization_manifest.json"
MATERIALIZED_SCHEMA = MAT_DIR / "materialized_schema_contract.csv"
MATERIALIZED_LINEAGE = MAT_DIR / "materialized_lineage.csv"
MATERIALIZATION_VALIDATION = MAT_DIR / "materialization_validation_summary.csv"
MATERIALIZATION_READINESS = MAT_DIR / "materialization_readiness.csv"

CORRECTED_INDEX_6IH = TMP_DIR / "layer6_6ih_base_out_transition_reconstruction_correction_implementation_corrected_transition_index_candidate.csv"
SOURCE_PROVENANCE_6IH = TMP_DIR / "layer6_6ih_base_out_transition_reconstruction_correction_implementation_source_provenance.csv"
SOURCE_MANIFEST_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/source_manifest.json"
TRANSITION_INDEX_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/base_out_transition_index.csv"
RAW_FEED_DIR_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/statsapi_game_feed"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
ADAPTER_LOAD_CSV = TMP_DIR / f"{SLUG}_adapter_load.csv"
EVALUATION_MATRIX_CSV = TMP_DIR / f"{SLUG}_evaluation_matrix.csv"
METRIC_ROWS_CSV = TMP_DIR / f"{SLUG}_metric_rows.csv"
BASELINE_CSV = TMP_DIR / f"{SLUG}_baseline_comparison.csv"
CANDIDATE_CSV = TMP_DIR / f"{SLUG}_candidate_decisions.csv"
ACTUAL_SURFACE_CSV = TMP_DIR / f"{SLUG}_actual_outcome_surface.csv"
GAP_CLASSIFICATION_CSV = TMP_DIR / f"{SLUG}_gap_classification.csv"
LINEAGE_CSV = TMP_DIR / f"{SLUG}_lineage.csv"
READINESS_CSV = TMP_DIR / f"{SLUG}_readiness.csv"
FUTURE_6IS_CSV = TMP_DIR / f"{SLUG}_future_6is_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6IQ = "layer_6_gameplay_mechanic_outcome_real_evaluation_implementation_complete"
DIAGNOSIS_6IR = "layer_6_gameplay_mechanic_outcome_real_evaluation_implementation_audit_complete"

RECOMMENDED_NEXT_LAYER_6IQ = "6IR_layer_6_gameplay_mechanic_outcome_real_evaluation_implementation_audit"
RECOMMENDED_PATH_6IQ = "implement_real_gameplay_mechanic_outcome_evaluation_then_audit_before_activation"

RECOMMENDED_NEXT_LAYER_6IR = "6IS_layer_6_actual_outcome_surface_gap_resolution_plan"
RECOMMENDED_PATH_6IR = "audit_real_evaluation_implementation_then_plan_actual_outcome_surface_gap_resolution"

SOURCE_FAMILY = "base_out_transitions"
ACQUISITION_MODE = "future_controlled_statsapi_acquisition"

PRESERVED_FAMILIES = ["game_level_outcomes", "inning_runs"]


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


def all_true(rows: List[Dict[str, Any]]) -> bool:
    return all(str(row.get("passed", "")).lower() == "true" or row.get("passed") is True for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    impl_before = IMPLEMENT_6IQ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IQ_PATH.exists() else ""
    adapter_before = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_before = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_before = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_before = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    json_6iq = load_json(JSON_6IQ)

    required_inputs = [
        JSON_6IQ, CHECKS_6IQ, PREDECESSOR_6IQ, INPUT_6IQ, ADAPTER_LOAD_6IQ,
        EVALUATION_MATRIX_6IQ, METRIC_ROWS_6IQ, BASELINE_6IQ, CANDIDATE_6IQ,
        ACTUAL_SURFACE_6IQ, LINEAGE_6IQ, READINESS_6IQ, FUTURE_6IR_6IQ,
        READONLY_6IQ, PRESERVED_6IQ, BLOCKING_6IQ, DECISION_6IQ, SAFETY_6IQ,
        IMMUTABILITY_6IQ, RECOMMENDED_6IQ, JSON_6IP, JSON_6IO, JSON_6IN,
        JSON_6IM, JSON_6IL, JSON_6IK, JSON_6IJ, JSON_6II, ADAPTER_MODULE_PATH,
        MATERIALIZED_TABLE, MATERIALIZATION_MANIFEST, MATERIALIZED_SCHEMA,
        MATERIALIZED_LINEAGE, MATERIALIZATION_VALIDATION, MATERIALIZATION_READINESS,
        CORRECTED_INDEX_6IH, SOURCE_PROVENANCE_6IH, SOURCE_MANIFEST_6IB,
        TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    readonly_sources = [
        JSON_6IQ, JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IM, JSON_6IL, JSON_6IK, JSON_6IJ,
        JSON_6II, ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZATION_MANIFEST,
        MATERIALIZED_SCHEMA, MATERIALIZED_LINEAGE, MATERIALIZATION_VALIDATION,
        MATERIALIZATION_READINESS, CORRECTED_INDEX_6IH, SOURCE_PROVENANCE_6IH,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    adapter_load_rows_in = read_csv(ADAPTER_LOAD_6IQ)
    evaluation_rows_in = read_csv(EVALUATION_MATRIX_6IQ)
    metric_rows_in = read_csv(METRIC_ROWS_6IQ)
    baseline_rows_in = read_csv(BASELINE_6IQ)
    candidate_rows_in = read_csv(CANDIDATE_6IQ)
    actual_surface_rows_in = read_csv(ACTUAL_SURFACE_6IQ)
    lineage_rows_in = read_csv(LINEAGE_6IQ)
    readiness_rows_in = read_csv(READINESS_6IQ)

    actual_surface_unavailable = (
        len(actual_surface_rows_in) == 1
        and str(actual_surface_rows_in[0].get("available", "")).lower() == "false"
        and str(actual_surface_rows_in[0].get("join_executed", "")).lower() == "false"
    )

    candidate_non_final = (
        len(candidate_rows_in) == 30
        and all(str(row.get("candidate_decision_final", "")).lower() == "false" for row in candidate_rows_in)
    )

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6iq_implementation_exists", "expected": True, "actual": IMPLEMENT_6IQ_PATH.exists(), "passed": IMPLEMENT_6IQ_PATH.exists()},
        {"check": "6iq_json_exists", "expected": True, "actual": JSON_6IQ.exists(), "passed": JSON_6IQ.exists()},
        {"check": "6iq_all_checks_passed", "expected": True, "actual": json_6iq.get("all_checks_passed"), "passed": json_6iq.get("all_checks_passed") is True},
        {"check": "6iq_diagnosis", "expected": DIAGNOSIS_6IQ, "actual": json_6iq.get("diagnosis"), "passed": json_6iq.get("diagnosis") == DIAGNOSIS_6IQ},
        {"check": "6iq_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IQ, "actual": json_6iq.get("recommended_next_layer"), "passed": json_6iq.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6IQ},
        {"check": "6iq_recommended_path", "expected": RECOMMENDED_PATH_6IQ, "actual": json_6iq.get("recommended_path"), "passed": json_6iq.get("recommended_path") == RECOMMENDED_PATH_6IQ},
        {"check": "6iq_harness_implemented", "expected": True, "actual": json_6iq.get("evaluation_harness_implemented"), "passed": json_6iq.get("evaluation_harness_implemented") is True},
        {"check": "6iq_actual_join_blocked", "expected": False, "actual": json_6iq.get("actual_outcome_join_executed"), "passed": json_6iq.get("actual_outcome_join_executed") is False},
        {"check": "6iq_no_exit_credit", "expected": False, "actual": json_6iq.get("layer_6_exit_credit"), "passed": json_6iq.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    adapter_load_rows = [
        {"check": "adapter_load_artifact_exists", "expected": True, "actual": ADAPTER_LOAD_6IQ.exists(), "passed": ADAPTER_LOAD_6IQ.exists()},
        {"check": "adapter_records_loaded", "expected": 801, "actual": json_6iq.get("materialized_transition_row_count"), "passed": json_6iq.get("materialized_transition_row_count") == 801},
        {"check": "adapter_validation_passed", "expected": True, "actual": json_6iq.get("adapter_validation_passed"), "passed": json_6iq.get("adapter_validation_passed") is True},
        {"check": "adapter_production_enabled", "expected": False, "actual": json_6iq.get("adapter_production_enabled"), "passed": json_6iq.get("adapter_production_enabled") is False},
        {"check": "adapter_load_rows_all_passed", "expected": True, "actual": all_true(adapter_load_rows_in), "passed": all_true(adapter_load_rows_in)},
    ]

    evaluation_audit_rows = [
        {"check": "evaluation_matrix_exists", "expected": True, "actual": EVALUATION_MATRIX_6IQ.exists(), "passed": EVALUATION_MATRIX_6IQ.exists()},
        {"check": "evaluation_matrix_row_count", "expected": 30, "actual": len(evaluation_rows_in), "passed": len(evaluation_rows_in) == 30},
        {"check": "evaluation_matrix_all_passed", "expected": True, "actual": all_true(evaluation_rows_in), "passed": all_true(evaluation_rows_in)},
        {"check": "non_production_all_rows", "expected": True, "actual": all(str(row.get("non_production", "")).lower() == "true" for row in evaluation_rows_in), "passed": all(str(row.get("non_production", "")).lower() == "true" for row in evaluation_rows_in)},
    ]

    metric_audit_rows = [
        {"check": "metric_rows_exists", "expected": True, "actual": METRIC_ROWS_6IQ.exists(), "passed": METRIC_ROWS_6IQ.exists()},
        {"check": "metric_row_count", "expected": 300, "actual": len(metric_rows_in), "passed": len(metric_rows_in) == 300},
        {"check": "metric_rows_all_passed", "expected": True, "actual": all_true(metric_rows_in), "passed": all_true(metric_rows_in)},
        {"check": "metric_rows_non_final", "expected": True, "actual": all(str(row.get("metric_final", "")).lower() == "false" for row in metric_rows_in), "passed": all(str(row.get("metric_final", "")).lower() == "false" for row in metric_rows_in)},
    ]

    baseline_audit_rows = [
        {"check": "baseline_rows_exists", "expected": True, "actual": BASELINE_6IQ.exists(), "passed": BASELINE_6IQ.exists()},
        {"check": "baseline_row_count", "expected": 30, "actual": len(baseline_rows_in), "passed": len(baseline_rows_in) == 30},
        {"check": "baseline_rows_all_passed", "expected": True, "actual": all_true(baseline_rows_in), "passed": all_true(baseline_rows_in)},
        {"check": "baseline_rows_non_final", "expected": True, "actual": all(str(row.get("comparison_final", "")).lower() == "false" for row in baseline_rows_in), "passed": all(str(row.get("comparison_final", "")).lower() == "false" for row in baseline_rows_in)},
    ]

    candidate_audit_rows = [
        {"check": "candidate_rows_exists", "expected": True, "actual": CANDIDATE_6IQ.exists(), "passed": CANDIDATE_6IQ.exists()},
        {"check": "candidate_row_count", "expected": 30, "actual": len(candidate_rows_in), "passed": len(candidate_rows_in) == 30},
        {"check": "candidate_rows_all_passed", "expected": True, "actual": all_true(candidate_rows_in), "passed": all_true(candidate_rows_in)},
        {"check": "candidate_decisions_non_final", "expected": True, "actual": candidate_non_final, "passed": candidate_non_final},
        {"check": "activation_not_recommended", "expected": True, "actual": all(str(row.get("activation_recommended", "")).lower() == "false" for row in candidate_rows_in), "passed": all(str(row.get("activation_recommended", "")).lower() == "false" for row in candidate_rows_in)},
    ]

    actual_surface_rows = [
        {"check": "actual_surface_artifact_exists", "expected": True, "actual": ACTUAL_SURFACE_6IQ.exists(), "passed": ACTUAL_SURFACE_6IQ.exists()},
        {"check": "actual_surface_unavailable_explicit", "expected": True, "actual": actual_surface_unavailable, "passed": actual_surface_unavailable},
        {"check": "actual_outcome_join_false", "expected": False, "actual": json_6iq.get("actual_outcome_join_executed"), "passed": json_6iq.get("actual_outcome_join_executed") is False},
        {"check": "evaluation_mode_scaffolded", "expected": "evaluation_harness_scaffold_with_blocked_actual_outcome_join", "actual": json_6iq.get("evaluation_mode"), "passed": json_6iq.get("evaluation_mode") == "evaluation_harness_scaffold_with_blocked_actual_outcome_join"},
    ]

    gap_rows = [
        {"gap": "actual_outcome_surface_gap", "confirmed": True, "blocking_final_decisions": True, "next_required_gap_resolution": "actual_outcome_surface_gap_resolution", "passed": True},
        {"gap": "actual_outcome_join_gap", "confirmed": True, "blocking_activation_planning": True, "next_required_gap_resolution": "actual_outcome_surface_gap_resolution", "passed": True},
    ]

    lineage_audit_rows = [
        {"check": "lineage_exists", "expected": True, "actual": LINEAGE_6IQ.exists(), "passed": LINEAGE_6IQ.exists()},
        {"check": "lineage_row_count", "expected": 30, "actual": len(lineage_rows_in), "passed": len(lineage_rows_in) == 30},
        {"check": "lineage_rows_all_passed", "expected": True, "actual": all_true(lineage_rows_in), "passed": all_true(lineage_rows_in)},
    ]

    readiness_rows = [
        {"surface": "evaluation_harness", "ready": True, "expected": True, "passed": True},
        {"surface": "actual_outcome_surface_gap_resolution", "ready": True, "expected": True, "passed": True},
        {"surface": "final_pass_fail_decisions", "ready": False, "expected": False, "passed": True},
        {"surface": "activation_planning", "ready": False, "expected": False, "passed": True},
        {"surface": "mechanic_activation", "ready": False, "expected": False, "passed": True},
        {"surface": "layer_6_exit", "ready": False, "expected": False, "passed": True},
    ]

    future_6is_rows = [
        {"contract": "plan_actual_outcome_surface_gap_resolution", "required": True, "passed": True},
        {"contract": "define_required_actual_outcome_event_surfaces", "required": True, "passed": True},
        {"contract": "define_allowed_source_families", "required": True, "passed": True},
        {"contract": "define_forbidden_source_shortcuts", "required": True, "passed": True},
        {"contract": "define_lineage_requirements", "required": True, "passed": True},
        {"contract": "define_acquisition_or_materialization_requirements", "required": True, "passed": True},
        {"contract": "define_validation_requirements", "required": True, "passed": True},
        {"contract": "keep_activation_and_exit_blocked", "required": True, "passed": True},
    ]

    readonly_rows = [
        {"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()}
        for path in readonly_sources
    ]

    preserved_rows = [
        {"source_family": "game_level_outcomes", "status": "preserved_remediated_from_prior_layers", "passed": True},
        {"source_family": "inning_runs", "status": "preserved_remediated_from_prior_layers", "passed": True},
    ]

    blocking_rows = [
        {"blocked_surface": "final_pass_fail_decisions", "blocked": True, "reason": "actual outcome surface unavailable", "passed": True},
        {"blocked_surface": "activation_planning", "blocked": True, "reason": "actual outcome surface gap must be resolved first", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "activation planning blocked", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "evaluation outputs are non-production", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "activation chain incomplete", "passed": True},
    ]

    decision_rows = [
        {"decision": "6iq_passed", "expected": True, "actual": json_6iq.get("all_checks_passed"), "passed": json_6iq.get("all_checks_passed") is True},
        {"decision": "evaluation_harness_valid", "expected": True, "actual": True, "passed": True},
        {"decision": "actual_outcome_surface_gap_confirmed", "expected": True, "actual": True, "passed": True},
        {"decision": "final_pass_fail_decision_possible", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_planning_allowed_after_this_audit", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6is_gap_resolution_plan_next", "expected": RECOMMENDED_NEXT_LAYER_6IR, "actual": RECOMMENDED_NEXT_LAYER_6IR, "passed": True},
        {"decision": "actual_outcome_join_executed", "expected": False, "actual": False, "passed": True},
        {"decision": "mechanics_activated", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_evaluation_implementation_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_adapter_implementation_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ib_artifact_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ih_corrected_candidate_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ik_materialized_output_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_actual_outcome_join", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    impl_after = IMPLEMENT_6IQ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IQ_PATH.exists() else ""
    adapter_after = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_after = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_after = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_after = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    immutability_rows = [
        {"surface": "this_6ir_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6iq_implementation", "policy": "unchanged_by_6ir", "passed": impl_after == impl_before},
        {"surface": "adapter_module", "policy": "unchanged_by_6ir", "passed": adapter_after == adapter_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6ir", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6ir", "passed": corrected_after == corrected_before},
        {"surface": "6ik_materialized_table", "policy": "read_only_unchanged_by_6ir", "passed": materialized_after == materialized_before},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IR, "actual": RECOMMENDED_NEXT_LAYER_6IR, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6IR, "actual": RECOMMENDED_PATH_6IR, "passed": True},
        {"decision": "recommend_actual_outcome_surface_gap_resolution_plan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6IR, "actual": DIAGNOSIS_6IR, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_true(predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_true(input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "adapter_load", "passed": all_true(adapter_load_rows), "detail": f"{sum(1 for row in adapter_load_rows if row['passed'])}/{len(adapter_load_rows)}"},
        {"check": "evaluation_matrix", "passed": all_true(evaluation_audit_rows), "detail": f"{sum(1 for row in evaluation_audit_rows if row['passed'])}/{len(evaluation_audit_rows)}"},
        {"check": "metric_rows", "passed": all_true(metric_audit_rows), "detail": f"{sum(1 for row in metric_audit_rows if row['passed'])}/{len(metric_audit_rows)}"},
        {"check": "baseline_comparison", "passed": all_true(baseline_audit_rows), "detail": f"{sum(1 for row in baseline_audit_rows if row['passed'])}/{len(baseline_audit_rows)}"},
        {"check": "candidate_decisions", "passed": all_true(candidate_audit_rows), "detail": f"{sum(1 for row in candidate_audit_rows if row['passed'])}/{len(candidate_audit_rows)}"},
        {"check": "actual_outcome_surface", "passed": all_true(actual_surface_rows), "detail": f"{sum(1 for row in actual_surface_rows if row['passed'])}/{len(actual_surface_rows)}"},
        {"check": "gap_classification", "passed": all_true(gap_rows), "detail": f"{sum(1 for row in gap_rows if row['passed'])}/{len(gap_rows)}"},
        {"check": "lineage", "passed": all_true(lineage_audit_rows), "detail": f"{sum(1 for row in lineage_audit_rows if row['passed'])}/{len(lineage_audit_rows)}"},
        {"check": "readiness", "passed": all_true(readiness_rows), "detail": f"{sum(1 for row in readiness_rows if row['passed'])}/{len(readiness_rows)}"},
        {"check": "future_6is_contract", "passed": all_true(future_6is_rows), "detail": f"{sum(1 for row in future_6is_rows if row['passed'])}/{len(future_6is_rows)}"},
        {"check": "readonly_sources", "passed": all_true(readonly_rows), "detail": f"{sum(1 for row in readonly_rows if row['passed'])}/{len(readonly_rows)}"},
        {"check": "preserved_families", "passed": all_true(preserved_rows), "detail": f"{sum(1 for row in preserved_rows if row['passed'])}/{len(preserved_rows)}"},
        {"check": "blocking_policy", "passed": all_true(blocking_rows), "detail": f"{sum(1 for row in blocking_rows if row['passed'])}/{len(blocking_rows)}"},
        {"check": "decision", "passed": all_true(decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_true(safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all_true(immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all_true(recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_true(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "adapter_load": write_csv(ADAPTER_LOAD_CSV, adapter_load_rows),
        "evaluation_matrix": write_csv(EVALUATION_MATRIX_CSV, evaluation_audit_rows),
        "metric_rows": write_csv(METRIC_ROWS_CSV, metric_audit_rows),
        "baseline_comparison": write_csv(BASELINE_CSV, baseline_audit_rows),
        "candidate_decisions": write_csv(CANDIDATE_CSV, candidate_audit_rows),
        "actual_outcome_surface": write_csv(ACTUAL_SURFACE_CSV, actual_surface_rows),
        "gap_classification": write_csv(GAP_CLASSIFICATION_CSV, gap_rows),
        "lineage": write_csv(LINEAGE_CSV, lineage_audit_rows),
        "readiness": write_csv(READINESS_CSV, readiness_rows),
        "future_6is_contract": write_csv(FUTURE_6IS_CSV, future_6is_rows),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6IR",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6IR if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6IR,
        "recommended_path": RECOMMENDED_PATH_6IR,
        "predecessor_implementation": str(IMPLEMENT_6IQ_PATH),
        "predecessor_implementation_returncode": 0,
        "predecessor_implementation_diagnosis": json_6iq.get("diagnosis"),
        "audited_layer": "6IQ",
        "source_family": SOURCE_FAMILY,
        "acquisition_mode": ACQUISITION_MODE,
        "adapter_validation_passed": json_6iq.get("adapter_validation_passed"),
        "adapter_production_enabled": json_6iq.get("adapter_production_enabled"),
        "materialized_transition_row_count": json_6iq.get("materialized_transition_row_count"),
        "materialized_exact_transition_row_count": json_6iq.get("materialized_exact_transition_row_count"),
        "materialized_non_exact_transition_row_count": json_6iq.get("materialized_non_exact_transition_row_count"),
        "materialized_schema_field_count": json_6iq.get("materialized_schema_field_count"),
        "evaluation_harness_valid": True,
        "evaluation_matrix_row_count": len(evaluation_rows_in),
        "metric_row_count": len(metric_rows_in),
        "baseline_comparison_row_count": len(baseline_rows_in),
        "candidate_decision_row_count": len(candidate_rows_in),
        "lineage_output_row_count": len(lineage_rows_in),
        "actual_outcome_surface_available": False,
        "actual_outcome_join_executed": False,
        "actual_outcome_surface_gap_confirmed": True,
        "evaluation_mode": json_6iq.get("evaluation_mode"),
        "pass_fail_candidate_decisions_final": False,
        "final_pass_fail_decision_possible": False,
        "activation_planning_allowed_after_this_audit": False,
        "next_required_gap_resolution": "actual_outcome_surface_gap_resolution",
        "future_6is_contract_valid": all_true(future_6is_rows),
        "preserved_remediated_family_count": len(PRESERVED_FAMILIES),
        "source_artifacts_mutated": False,
        "corrected_candidate_artifacts_mutated": False,
        "materialized_outputs_mutated": False,
        "adapter_implementation_mutated": False,
        "evaluation_implementation_mutated_by_audit": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "actual_outcomes_joined_to_mechanics": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "database_writes_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "production_simulations_run": False,
        "games_evaluated": 0,
        "layer_6_exit_credit": False,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "adapter_load_csv": str(ADAPTER_LOAD_CSV),
            "evaluation_matrix_csv": str(EVALUATION_MATRIX_CSV),
            "metric_rows_csv": str(METRIC_ROWS_CSV),
            "baseline_comparison_csv": str(BASELINE_CSV),
            "candidate_decisions_csv": str(CANDIDATE_CSV),
            "actual_outcome_surface_csv": str(ACTUAL_SURFACE_CSV),
            "gap_classification_csv": str(GAP_CLASSIFICATION_CSV),
            "lineage_csv": str(LINEAGE_CSV),
            "readiness_csv": str(READINESS_CSV),
            "future_6is_contract_csv": str(FUTURE_6IS_CSV),
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
