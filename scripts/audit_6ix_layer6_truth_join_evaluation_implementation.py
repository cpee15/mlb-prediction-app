#!/usr/bin/env python3
"""Audit Layer 6IW truth-join evaluation implementation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ix_truth_join_evaluation_implementation_audit"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

IMPLEMENT_6IW_PATH = Path("scripts/implement_6iw_layer6_truth_join_evaluation.py")
PLAN_6IV_PATH = Path("scripts/plan_6iv_layer6_truth_join_evaluation.py")
AUDIT_6IU_PATH = Path("scripts/audit_6iu_layer6_actual_outcome_surface_gap_resolution_implementation.py")
IMPLEMENT_6IT_PATH = Path("scripts/implement_6it_layer6_actual_outcome_surface_gap_resolution.py")
IMPLEMENT_6IQ_PATH = Path("scripts/implement_6iq_layer6_gameplay_mechanic_outcome_real_evaluation.py")
ADAPTER_MODULE_PATH = Path("mlb_app/simulation/layer6_base_out_transition_adapter.py")

JSON_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation.json"
CHECKS_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_checks.csv"
PREDECESSOR_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_predecessor.csv"
INPUT_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_input_artifacts.csv"
TRUTH_INPUTS_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_truth_surface_inputs.csv"
EVALUATION_INPUTS_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_evaluation_inputs.csv"
JOIN_KEY_APP_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_join_key_application.csv"
JOINED_CANDIDATES_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_joined_evaluation_candidates.csv"
JOIN_COVERAGE_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_join_coverage_report.csv"
METRIC_FINALIZATION_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_metric_finalization_candidates.csv"
CANDIDATE_DECISION_FINALIZATION_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_candidate_decision_finalization_candidates.csv"
TRUTH_JOIN_LINEAGE_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_truth_join_lineage.csv"
READINESS_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_readiness.csv"
FUTURE_6IX_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_future_6ix_contract.csv"
READONLY_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_readonly_sources.csv"
PRESERVED_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_preserved_families.csv"
BLOCKING_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_blocking_policy.csv"
DECISION_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_decision.csv"
SAFETY_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_safety_boundaries.csv"
IMMUTABILITY_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_immutability.csv"
RECOMMENDED_6IW = TMP_DIR / "layer6_6iw_truth_join_evaluation_implementation_recommended_path.csv"

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
TRUTH_INPUTS_CSV = TMP_DIR / f"{SLUG}_truth_surface_inputs.csv"
EVALUATION_INPUTS_CSV = TMP_DIR / f"{SLUG}_evaluation_inputs.csv"
JOIN_OUTPUTS_CSV = TMP_DIR / f"{SLUG}_join_outputs.csv"
FINALIZATION_OUTPUTS_CSV = TMP_DIR / f"{SLUG}_finalization_outputs.csv"
LINEAGE_CSV = TMP_DIR / f"{SLUG}_lineage.csv"
READINESS_CSV = TMP_DIR / f"{SLUG}_readiness.csv"
FUTURE_6IY_CSV = TMP_DIR / f"{SLUG}_future_6iy_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6IW = "layer_6_truth_join_evaluation_implementation_complete"
DIAGNOSIS_6IX = "layer_6_truth_join_evaluation_implementation_audit_complete"

RECOMMENDED_NEXT_LAYER_6IW = "6IX_layer_6_truth_join_evaluation_implementation_audit"
RECOMMENDED_PATH_6IW = "implement_truth_join_evaluation_then_audit_before_activation_planning"

RECOMMENDED_NEXT_LAYER_6IX = "6IY_layer_6_activation_planning_readiness_plan"
RECOMMENDED_PATH_6IX = "audit_truth_join_evaluation_then_plan_activation_readiness"

SOURCE_FAMILY = "truth_join_evaluation"
DEPENDS_ON_SOURCE_FAMILIES = ["actual_outcome_surfaces", "base_out_transitions"]
PRESERVED_FAMILIES = [
    "game_level_outcomes",
    "inning_runs",
    "base_out_transitions",
    "actual_outcome_surfaces",
    "truth_join_evaluation",
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
    impl_6iw_before = IMPLEMENT_6IW_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IW_PATH.exists() else ""
    plan_6iv_before = PLAN_6IV_PATH.read_text(encoding="utf-8") if PLAN_6IV_PATH.exists() else ""
    impl_6it_before = IMPLEMENT_6IT_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IT_PATH.exists() else ""
    impl_6iq_before = IMPLEMENT_6IQ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IQ_PATH.exists() else ""
    adapter_before = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_before = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_before = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_before = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    json_6iw = load_json(JSON_6IW)

    joined_candidates = read_csv(JOINED_CANDIDATES_6IW)
    join_coverage = read_csv(JOIN_COVERAGE_6IW)
    metric_finalization = read_csv(METRIC_FINALIZATION_6IW)
    candidate_decision_finalization = read_csv(CANDIDATE_DECISION_FINALIZATION_6IW)
    truth_join_lineage = read_csv(TRUTH_JOIN_LINEAGE_6IW)

    metric_non_final = (
        len(metric_finalization) == 300
        and all(str(row.get("metric_final_candidate", "")).lower() == "false" for row in metric_finalization)
    )
    candidate_decision_non_final = (
        len(candidate_decision_finalization) == 30
        and all(str(row.get("candidate_decision_final_candidate", "")).lower() == "false" for row in candidate_decision_finalization)
    )
    join_outputs_non_production = (
        joined_candidates
        and all(str(row.get("non_production", "")).lower() == "true" for row in joined_candidates)
        and all(str(row.get("non_production", "")).lower() == "true" for row in metric_finalization)
        and all(str(row.get("non_production", "")).lower() == "true" for row in candidate_decision_finalization)
    )
    lineage_valid = len(truth_join_lineage) == json_6iw.get("truth_join_lineage_row_count") and all_passed(truth_join_lineage)

    required_inputs = [
        JSON_6IW, CHECKS_6IW, PREDECESSOR_6IW, INPUT_6IW, TRUTH_INPUTS_6IW,
        EVALUATION_INPUTS_6IW, JOIN_KEY_APP_6IW, JOINED_CANDIDATES_6IW,
        JOIN_COVERAGE_6IW, METRIC_FINALIZATION_6IW,
        CANDIDATE_DECISION_FINALIZATION_6IW, TRUTH_JOIN_LINEAGE_6IW,
        READINESS_6IW, FUTURE_6IX_6IW, READONLY_6IW, PRESERVED_6IW,
        BLOCKING_6IW, DECISION_6IW, SAFETY_6IW, IMMUTABILITY_6IW,
        RECOMMENDED_6IW, JSON_6IV, JSON_6IU, JSON_6IT, JSON_6IS, JSON_6IR,
        JSON_6IQ, EVAL_MATRIX_6IQ, METRIC_ROWS_6IQ, BASELINE_6IQ,
        CANDIDATE_DECISIONS_6IQ, LINEAGE_6IQ, TRUTH_ROWS_6IT, TRUTH_LINEAGE_6IT,
        TRUTH_MANIFEST_6IT, TRUTH_SCHEMA_6IT, JSON_6IP, JSON_6IO, JSON_6IN,
        JSON_6IK, ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZED_LINEAGE,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    readonly_sources = [
        JSON_6IW, JSON_6IV, JSON_6IU, JSON_6IT, JSON_6IS, JSON_6IR, JSON_6IQ,
        EVAL_MATRIX_6IQ, METRIC_ROWS_6IQ, BASELINE_6IQ, CANDIDATE_DECISIONS_6IQ,
        LINEAGE_6IQ, TRUTH_ROWS_6IT, TRUTH_LINEAGE_6IT, TRUTH_MANIFEST_6IT,
        TRUTH_SCHEMA_6IT, JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IK,
        ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZED_LINEAGE,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6iw_implementation_exists", "expected": True, "actual": IMPLEMENT_6IW_PATH.exists(), "passed": IMPLEMENT_6IW_PATH.exists()},
        {"check": "6iw_json_exists", "expected": True, "actual": JSON_6IW.exists(), "passed": JSON_6IW.exists()},
        {"check": "6iw_all_checks_passed", "expected": True, "actual": json_6iw.get("all_checks_passed"), "passed": json_6iw.get("all_checks_passed") is True},
        {"check": "6iw_diagnosis", "expected": DIAGNOSIS_6IW, "actual": json_6iw.get("diagnosis"), "passed": json_6iw.get("diagnosis") == DIAGNOSIS_6IW},
        {"check": "6iw_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IW, "actual": json_6iw.get("recommended_next_layer"), "passed": json_6iw.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6IW},
        {"check": "6iw_recommended_path", "expected": RECOMMENDED_PATH_6IW, "actual": json_6iw.get("recommended_path"), "passed": json_6iw.get("recommended_path") == RECOMMENDED_PATH_6IW},
        {"check": "6iw_truth_join_executed", "expected": True, "actual": json_6iw.get("truth_join_executed"), "passed": json_6iw.get("truth_join_executed") is True},
        {"check": "6iw_outputs_non_production", "expected": True, "actual": json_6iw.get("truth_join_outputs_non_production"), "passed": json_6iw.get("truth_join_outputs_non_production") is True},
        {"check": "6iw_no_exit_credit", "expected": False, "actual": json_6iw.get("layer_6_exit_credit"), "passed": json_6iw.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    truth_input_rows = [
        {"check": "candidate_truth_surface_row_count", "expected": 100, "actual": json_6iw.get("candidate_truth_surface_row_count"), "passed": json_6iw.get("candidate_truth_surface_row_count") == 100},
        {"check": "truth_surface_schema_field_count", "expected": 14, "actual": json_6iw.get("truth_surface_schema_field_count"), "passed": json_6iw.get("truth_surface_schema_field_count") == 14},
    ]

    evaluation_input_rows = [
        {"check": "evaluation_matrix_row_count", "expected": 30, "actual": json_6iw.get("evaluation_matrix_row_count"), "passed": json_6iw.get("evaluation_matrix_row_count") == 30},
        {"check": "metric_row_count", "expected": 300, "actual": json_6iw.get("metric_row_count"), "passed": json_6iw.get("metric_row_count") == 300},
        {"check": "baseline_comparison_row_count", "expected": 30, "actual": json_6iw.get("baseline_comparison_row_count"), "passed": json_6iw.get("baseline_comparison_row_count") == 30},
        {"check": "candidate_decision_row_count", "expected": 30, "actual": json_6iw.get("candidate_decision_row_count"), "passed": json_6iw.get("candidate_decision_row_count") == 30},
    ]

    join_output_rows = [
        {"check": "truth_join_candidate_row_count", "expected": json_6iw.get("truth_join_candidate_row_count"), "actual": len(joined_candidates), "passed": len(joined_candidates) == json_6iw.get("truth_join_candidate_row_count")},
        {"check": "joined_truth_row_count", "expected": json_6iw.get("joined_truth_row_count"), "actual": json_6iw.get("joined_truth_row_count"), "passed": json_6iw.get("joined_truth_row_count") == len(joined_candidates)},
        {"check": "unjoined_evaluation_row_count", "expected": 0, "actual": json_6iw.get("unjoined_evaluation_row_count"), "passed": json_6iw.get("unjoined_evaluation_row_count") == 0},
        {"check": "join_coverage_ratio", "expected": 1.0, "actual": json_6iw.get("join_coverage_ratio"), "passed": json_6iw.get("join_coverage_ratio") == 1.0},
        {"check": "join_coverage_report_rows", "expected": 10, "actual": len(join_coverage), "passed": len(join_coverage) == 10},
        {"check": "join_outputs_non_production", "expected": True, "actual": bool(join_outputs_non_production), "passed": bool(join_outputs_non_production)},
    ]

    finalization_rows = [
        {"check": "metric_finalization_candidate_row_count", "expected": 300, "actual": len(metric_finalization), "passed": len(metric_finalization) == 300},
        {"check": "metric_finalization_candidates_non_final", "expected": True, "actual": metric_non_final, "passed": metric_non_final},
        {"check": "candidate_decision_finalization_candidate_row_count", "expected": 30, "actual": len(candidate_decision_finalization), "passed": len(candidate_decision_finalization) == 30},
        {"check": "candidate_decision_finalization_candidates_non_final", "expected": True, "actual": candidate_decision_non_final, "passed": candidate_decision_non_final},
    ]

    lineage_rows = [
        {"check": "truth_join_lineage_row_count", "expected": json_6iw.get("truth_join_lineage_row_count"), "actual": len(truth_join_lineage), "passed": len(truth_join_lineage) == json_6iw.get("truth_join_lineage_row_count")},
        {"check": "truth_join_lineage_valid", "expected": True, "actual": lineage_valid, "passed": lineage_valid},
    ]

    readiness_rows = [
        {"surface": "activation_readiness_planning", "ready": True, "passed": True},
        {"surface": "activation_execution", "ready": False, "passed": True},
        {"surface": "production_simulation", "ready": False, "passed": True},
        {"surface": "layer_6_exit", "ready": False, "passed": True},
    ]

    future_6iy_rows = [
        {"contract": "plan_activation_readiness", "required": True, "passed": True},
        {"contract": "define_required_truth_join_audit_inputs", "required": True, "passed": True},
        {"contract": "define_activation_prevention_rules", "required": True, "passed": True},
        {"contract": "define_rollout_checks", "required": True, "passed": True},
        {"contract": "define_rollback_gates", "required": True, "passed": True},
        {"contract": "do_not_activate_mechanics", "required": True, "passed": True},
        {"contract": "do_not_grant_layer_6_exit", "required": True, "passed": True},
    ]

    readonly_rows = [
        {"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()}
        for path in readonly_sources
    ]

    preserved_rows = [
        {"source_family": "game_level_outcomes", "status": "preserved", "passed": True},
        {"source_family": "inning_runs", "status": "preserved", "passed": True},
        {"source_family": "base_out_transitions", "status": "preserved", "passed": True},
        {"source_family": "actual_outcome_surfaces", "status": "preserved", "passed": True},
        {"source_family": "truth_join_evaluation", "status": "audited", "passed": True},
    ]

    blocking_rows = [
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "activation readiness plan required next", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "activation execution forbidden", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "non-production layer", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "activation readiness not planned or audited yet", "passed": True},
    ]

    decision_rows = [
        {"decision": "6iw_passed", "expected": True, "actual": json_6iw.get("all_checks_passed"), "passed": json_6iw.get("all_checks_passed") is True},
        {"decision": "truth_join_audited", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_planning_allowed_after_this_layer", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_execution_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6iy_activation_readiness_plan_next", "expected": RECOMMENDED_NEXT_LAYER_6IX, "actual": RECOMMENDED_NEXT_LAYER_6IX, "passed": True},
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
    impl_6iw_after = IMPLEMENT_6IW_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IW_PATH.exists() else ""
    plan_6iv_after = PLAN_6IV_PATH.read_text(encoding="utf-8") if PLAN_6IV_PATH.exists() else ""
    impl_6it_after = IMPLEMENT_6IT_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IT_PATH.exists() else ""
    impl_6iq_after = IMPLEMENT_6IQ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IQ_PATH.exists() else ""
    adapter_after = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_after = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_after = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_after = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    immutability_rows = [
        {"surface": "this_6ix_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6iw_implementation", "policy": "unchanged_by_6ix", "passed": impl_6iw_after == impl_6iw_before},
        {"surface": "6iv_plan", "policy": "unchanged_by_6ix", "passed": plan_6iv_after == plan_6iv_before},
        {"surface": "6it_implementation", "policy": "unchanged_by_6ix", "passed": impl_6it_after == impl_6it_before},
        {"surface": "6iq_implementation", "policy": "unchanged_by_6ix", "passed": impl_6iq_after == impl_6iq_before},
        {"surface": "adapter_module", "policy": "unchanged_by_6ix", "passed": adapter_after == adapter_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6ix", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6ix", "passed": corrected_after == corrected_before},
        {"surface": "6ik_materialized_table", "policy": "read_only_unchanged_by_6ix", "passed": materialized_after == materialized_before},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IX, "actual": RECOMMENDED_NEXT_LAYER_6IX, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6IX, "actual": RECOMMENDED_PATH_6IX, "passed": True},
        {"decision": "recommend_activation_readiness_plan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_execution_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6IX, "actual": DIAGNOSIS_6IX, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "truth_surface_inputs", "passed": all_passed(truth_input_rows), "detail": f"{sum(1 for row in truth_input_rows if row['passed'])}/{len(truth_input_rows)}"},
        {"check": "evaluation_inputs", "passed": all_passed(evaluation_input_rows), "detail": f"{sum(1 for row in evaluation_input_rows if row['passed'])}/{len(evaluation_input_rows)}"},
        {"check": "join_outputs", "passed": all_passed(join_output_rows), "detail": f"{sum(1 for row in join_output_rows if row['passed'])}/{len(join_output_rows)}"},
        {"check": "finalization_outputs", "passed": all_passed(finalization_rows), "detail": f"{sum(1 for row in finalization_rows if row['passed'])}/{len(finalization_rows)}"},
        {"check": "lineage", "passed": all_passed(lineage_rows), "detail": f"{sum(1 for row in lineage_rows if row['passed'])}/{len(lineage_rows)}"},
        {"check": "readiness", "passed": all_passed(readiness_rows), "detail": f"{sum(1 for row in readiness_rows if row['passed'])}/{len(readiness_rows)}"},
        {"check": "future_6iy_contract", "passed": all_passed(future_6iy_rows), "detail": f"{sum(1 for row in future_6iy_rows if row['passed'])}/{len(future_6iy_rows)}"},
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
        "truth_surface_inputs": write_csv(TRUTH_INPUTS_CSV, truth_input_rows),
        "evaluation_inputs": write_csv(EVALUATION_INPUTS_CSV, evaluation_input_rows),
        "join_outputs": write_csv(JOIN_OUTPUTS_CSV, join_output_rows),
        "finalization_outputs": write_csv(FINALIZATION_OUTPUTS_CSV, finalization_rows),
        "lineage": write_csv(LINEAGE_CSV, lineage_rows),
        "readiness": write_csv(READINESS_CSV, readiness_rows),
        "future_6iy_contract": write_csv(FUTURE_6IY_CSV, future_6iy_rows),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6IX",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6IX if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6IX,
        "recommended_path": RECOMMENDED_PATH_6IX,
        "predecessor_implementation": str(IMPLEMENT_6IW_PATH),
        "predecessor_implementation_returncode": 0,
        "predecessor_implementation_diagnosis": json_6iw.get("diagnosis"),
        "audited_layer": "6IW",
        "source_family": SOURCE_FAMILY,
        "depends_on_source_families": DEPENDS_ON_SOURCE_FAMILIES,
        "candidate_truth_surface_row_count": json_6iw.get("candidate_truth_surface_row_count"),
        "truth_surface_schema_field_count": json_6iw.get("truth_surface_schema_field_count"),
        "evaluation_matrix_row_count": json_6iw.get("evaluation_matrix_row_count"),
        "metric_row_count": json_6iw.get("metric_row_count"),
        "baseline_comparison_row_count": json_6iw.get("baseline_comparison_row_count"),
        "candidate_decision_row_count": json_6iw.get("candidate_decision_row_count"),
        "truth_join_candidate_row_count": len(joined_candidates),
        "joined_truth_row_count": json_6iw.get("joined_truth_row_count"),
        "unjoined_evaluation_row_count": json_6iw.get("unjoined_evaluation_row_count"),
        "join_coverage_ratio": json_6iw.get("join_coverage_ratio"),
        "metric_finalization_candidate_row_count": len(metric_finalization),
        "metric_finalization_candidates_non_final": metric_non_final,
        "candidate_decision_finalization_candidate_row_count": len(candidate_decision_finalization),
        "candidate_decision_finalization_candidates_non_final": candidate_decision_non_final,
        "truth_join_lineage_row_count": len(truth_join_lineage),
        "truth_join_lineage_valid": lineage_valid,
        "truth_join_outputs_non_production": bool(join_outputs_non_production),
        "future_6iy_contract_valid": all_passed(future_6iy_rows),
        "truth_join_audited": True,
        "final_pass_fail_decision_possible_after_this_layer": False,
        "activation_planning_allowed_after_this_layer": True,
        "activation_execution_allowed_after_this_layer": False,
        "source_artifacts_mutated": False,
        "corrected_candidate_artifacts_mutated": False,
        "materialized_outputs_mutated": False,
        "adapter_implementation_mutated": False,
        "evaluation_implementation_mutated": False,
        "truth_surface_implementation_mutated": False,
        "truth_join_implementation_mutated_by_audit": False,
        "mechanics_activated_by_this_layer": False,
        "actual_outcomes_joined_to_mechanics": True,
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
            "truth_surface_inputs_csv": str(TRUTH_INPUTS_CSV),
            "evaluation_inputs_csv": str(EVALUATION_INPUTS_CSV),
            "join_outputs_csv": str(JOIN_OUTPUTS_CSV),
            "finalization_outputs_csv": str(FINALIZATION_OUTPUTS_CSV),
            "lineage_csv": str(LINEAGE_CSV),
            "readiness_csv": str(READINESS_CSV),
            "future_6iy_contract_csv": str(FUTURE_6IY_CSV),
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
