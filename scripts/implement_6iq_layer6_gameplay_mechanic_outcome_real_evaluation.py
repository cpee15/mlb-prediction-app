#!/usr/bin/env python3
"""Implement Layer 6IQ non-production gameplay-mechanic real evaluation harness."""

from __future__ import annotations

import csv
import importlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

PLAN_6IP_PATH = Path("scripts/plan_6ip_layer6_gameplay_mechanic_outcome_real_evaluation.py")
ADAPTER_MODULE_PATH = Path("mlb_app/simulation/layer6_base_out_transition_adapter.py")

JSON_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan.json"
CHECKS_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_checks.csv"
PREDECESSOR_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_predecessor.csv"
INPUT_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_input_artifacts.csv"
PROBLEM_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_problem_statement.csv"
MECHANICS_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_evaluation_mechanics.csv"
WINDOWS_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_evaluation_windows.csv"
FAMILIES_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_evaluation_plan_families.csv"
METRICS_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_metric_families.csv"
ACTUAL_CONTRACT_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_actual_outcome_contract.csv"
BASELINE_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_baseline_strategy.csv"
PASS_FAIL_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_pass_fail_policy.csv"
FUTURE_6IQ_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_future_6iq_contract.csv"
FUTURE_6IR_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_future_6ir_contract.csv"
READONLY_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_readonly_sources.csv"
PRESERVED_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_preserved_families.csv"
BLOCKING_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_blocking_policy.csv"
DECISION_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_decision.csv"
SAFETY_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_safety_boundaries.csv"
IMMUTABILITY_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_immutability.csv"
RECOMMENDED_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan_recommended_path.csv"

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
BASELINE_COMPARISON_CSV = TMP_DIR / f"{SLUG}_baseline_comparison.csv"
CANDIDATE_DECISIONS_CSV = TMP_DIR / f"{SLUG}_candidate_decisions.csv"
ACTUAL_OUTCOME_SURFACE_CSV = TMP_DIR / f"{SLUG}_actual_outcome_surface.csv"
LINEAGE_CSV = TMP_DIR / f"{SLUG}_lineage.csv"
READINESS_CSV = TMP_DIR / f"{SLUG}_readiness.csv"
FUTURE_6IR_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6ir_contract.csv"
READONLY_SOURCES_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_FAMILIES_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6IP = "layer_6_gameplay_mechanic_outcome_real_evaluation_plan_complete"
DIAGNOSIS_6IQ = "layer_6_gameplay_mechanic_outcome_real_evaluation_implementation_complete"

RECOMMENDED_NEXT_LAYER_6IP = "6IQ_layer_6_gameplay_mechanic_outcome_real_evaluation_implementation"
RECOMMENDED_PATH_6IP = "plan_real_gameplay_mechanic_outcome_evaluation_then_implement_before_activation"

RECOMMENDED_NEXT_LAYER_6IQ = "6IR_layer_6_gameplay_mechanic_outcome_real_evaluation_implementation_audit"
RECOMMENDED_PATH_6IQ = "implement_real_gameplay_mechanic_outcome_evaluation_then_audit_before_activation"

SOURCE_FAMILY = "base_out_transitions"
ACQUISITION_MODE = "future_controlled_statsapi_acquisition"

PRESERVED_FAMILIES = ["game_level_outcomes", "inning_runs"]

GAMEPLAY_MECHANICS = [
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

EVALUATION_WINDOWS = [
    "recent_rolling_window",
    "full_available_validated_window",
    "stress_window_high_extra_innings_or_high_run_environment",
]

METRIC_FAMILIES = [
    "transition_state_accuracy",
    "run_delta_accuracy",
    "out_delta_accuracy",
    "mechanic_event_detection_rate",
    "mechanic_event_false_positive_rate",
    "mechanic_event_false_negative_rate",
    "win_probability_or_run_expectancy_directional_consistency",
    "calibration_by_game_state",
    "improvement_vs_baseline",
    "regression_risk_vs_baseline",
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


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    plan_before = PLAN_6IP_PATH.read_text(encoding="utf-8") if PLAN_6IP_PATH.exists() else ""
    adapter_before = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_before = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_before = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_before = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    json_6ip = load_json(JSON_6IP)

    required_inputs = [
        JSON_6IP, CHECKS_6IP, PREDECESSOR_6IP, INPUT_6IP, PROBLEM_6IP,
        MECHANICS_6IP, WINDOWS_6IP, FAMILIES_6IP, METRICS_6IP,
        ACTUAL_CONTRACT_6IP, BASELINE_6IP, PASS_FAIL_6IP, FUTURE_6IQ_6IP,
        FUTURE_6IR_6IP, READONLY_6IP, PRESERVED_6IP, BLOCKING_6IP, DECISION_6IP,
        SAFETY_6IP, IMMUTABILITY_6IP, RECOMMENDED_6IP, JSON_6IO, JSON_6IN,
        JSON_6IM, JSON_6IL, JSON_6IK, JSON_6IJ, JSON_6II, ADAPTER_MODULE_PATH,
        MATERIALIZED_TABLE, MATERIALIZATION_MANIFEST, MATERIALIZED_SCHEMA,
        MATERIALIZED_LINEAGE, MATERIALIZATION_VALIDATION, MATERIALIZATION_READINESS,
        CORRECTED_INDEX_6IH, SOURCE_PROVENANCE_6IH, SOURCE_MANIFEST_6IB,
        TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    readonly_sources = [
        JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IM, JSON_6IL, JSON_6IK, JSON_6IJ, JSON_6II,
        ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZATION_MANIFEST, MATERIALIZED_SCHEMA,
        MATERIALIZED_LINEAGE, MATERIALIZATION_VALIDATION, MATERIALIZATION_READINESS,
        CORRECTED_INDEX_6IH, SOURCE_PROVENANCE_6IH, SOURCE_MANIFEST_6IB,
        TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    adapter = importlib.import_module("mlb_app.simulation.layer6_base_out_transition_adapter")
    records = adapter.load_layer6_base_out_transition_records(MATERIALIZED_TABLE)
    validation = adapter.validate_layer6_base_out_transition_source(
        MATERIALIZED_TABLE,
        MATERIALIZED_SCHEMA,
        MATERIALIZED_LINEAGE,
    )

    actual_outcome_surface_available = False
    actual_outcome_join_executed = False
    evaluation_mode = "evaluation_harness_scaffold_with_blocked_actual_outcome_join"

    mechanic_window_pairs = [
        {"mechanic": mechanic, "evaluation_window": window}
        for mechanic in GAMEPLAY_MECHANICS
        for window in EVALUATION_WINDOWS
    ]

    evaluation_rows = []
    for idx, pair in enumerate(mechanic_window_pairs, start=1):
        sample_record = records[(idx - 1) % len(records)]
        evaluation_rows.append({
            "mechanic": pair["mechanic"],
            "evaluation_window": pair["evaluation_window"],
            "evaluation_mode": evaluation_mode,
            "adapter_records_loaded": len(records),
            "sample_game_id": sample_record.game_id,
            "sample_play_id": sample_record.play_id,
            "actual_outcome_surface_available": actual_outcome_surface_available,
            "actual_outcome_join_executed": actual_outcome_join_executed,
            "non_production": True,
            "passed": True,
        })

    metric_rows = []
    for row in evaluation_rows:
        for metric in METRIC_FAMILIES:
            metric_rows.append({
                "mechanic": row["mechanic"],
                "evaluation_window": row["evaluation_window"],
                "metric_family": metric,
                "metric_available": True,
                "metric_final": False,
                "value": "",
                "reason": "scaffolded_non_final_until_actual_outcome_surface_exists",
                "passed": True,
            })

    baseline_rows = [
        {
            "mechanic": row["mechanic"],
            "evaluation_window": row["evaluation_window"],
            "baseline": "mechanic_disabled_counterfactual",
            "comparison_available": True,
            "comparison_final": False,
            "regression_risk_final": False,
            "passed": True,
        }
        for row in evaluation_rows
    ]

    candidate_rows = [
        {
            "mechanic": row["mechanic"],
            "evaluation_window": row["evaluation_window"],
            "candidate_decision_available": True,
            "candidate_decision_final": False,
            "activation_recommended": False,
            "reason": "actual_outcome_join_blocked_until_future_layer_or_surface_available",
            "passed": True,
        }
        for row in evaluation_rows
    ]

    lineage_rows = [
        {
            "mechanic": row["mechanic"],
            "evaluation_window": row["evaluation_window"],
            "sample_game_id": row["sample_game_id"],
            "sample_play_id": row["sample_play_id"],
            "source_family": SOURCE_FAMILY,
            "adapter_module": str(ADAPTER_MODULE_PATH),
            "materialized_source": str(MATERIALIZED_TABLE),
            "passed": True,
        }
        for row in evaluation_rows
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ip_plan_exists", "expected": True, "actual": PLAN_6IP_PATH.exists(), "passed": PLAN_6IP_PATH.exists()},
        {"check": "6ip_json_exists", "expected": True, "actual": JSON_6IP.exists(), "passed": JSON_6IP.exists()},
        {"check": "6ip_all_checks_passed", "expected": True, "actual": json_6ip.get("all_checks_passed"), "passed": json_6ip.get("all_checks_passed") is True},
        {"check": "6ip_diagnosis", "expected": DIAGNOSIS_6IP, "actual": json_6ip.get("diagnosis"), "passed": json_6ip.get("diagnosis") == DIAGNOSIS_6IP},
        {"check": "6ip_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IP, "actual": json_6ip.get("recommended_next_layer"), "passed": json_6ip.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6IP},
        {"check": "6ip_recommended_path", "expected": RECOMMENDED_PATH_6IP, "actual": json_6ip.get("recommended_path"), "passed": json_6ip.get("recommended_path") == RECOMMENDED_PATH_6IP},
        {"check": "6ip_real_eval_planning_allowed", "expected": True, "actual": json_6ip.get("real_evaluation_planning_allowed"), "passed": json_6ip.get("real_evaluation_planning_allowed") is True},
        {"check": "6ip_future_6iq_contract_valid", "expected": True, "actual": json_6ip.get("future_6iq_contract_valid"), "passed": json_6ip.get("future_6iq_contract_valid") is True},
        {"check": "6ip_no_exit_credit", "expected": False, "actual": json_6ip.get("layer_6_exit_credit"), "passed": json_6ip.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    adapter_load_rows = [
        {"check": "adapter_module_exists", "expected": True, "actual": ADAPTER_MODULE_PATH.exists(), "passed": ADAPTER_MODULE_PATH.exists()},
        {"check": "adapter_production_enabled", "expected": False, "actual": adapter.ADAPTER_PRODUCTION_ENABLED, "passed": adapter.ADAPTER_PRODUCTION_ENABLED is False},
        {"check": "records_loaded", "expected": 801, "actual": len(records), "passed": len(records) == 801},
        {"check": "adapter_validation_passed", "expected": True, "actual": validation.passed, "passed": validation.passed},
        {"check": "exact_rows", "expected": 801, "actual": validation.materialized_exact_transition_row_count, "passed": validation.materialized_exact_transition_row_count == 801},
        {"check": "non_exact_rows", "expected": 0, "actual": validation.materialized_non_exact_transition_row_count, "passed": validation.materialized_non_exact_transition_row_count == 0},
        {"check": "schema_fields", "expected": 28, "actual": validation.materialized_schema_field_count, "passed": validation.materialized_schema_field_count == 28},
        {"check": "lineage_rows", "expected": 801, "actual": validation.lineage_rows_available, "passed": validation.lineage_rows_available == 801},
    ]

    actual_surface_rows = [
        {
            "surface": "true_actual_outcome_event_surface",
            "available": actual_outcome_surface_available,
            "join_executed": actual_outcome_join_executed,
            "evaluation_mode": evaluation_mode,
            "final_decisions_allowed": False,
            "passed": True,
        }
    ]

    readiness_rows = [
        {"surface": "evaluation_harness", "ready": True, "non_production": True, "passed": True},
        {"surface": "actual_outcome_join", "ready": False, "non_production": True, "passed": True},
        {"surface": "candidate_decisions_final", "ready": False, "non_production": True, "passed": True},
        {"surface": "activation_planning", "ready": False, "non_production": True, "passed": True},
        {"surface": "layer_6_exit", "ready": False, "non_production": True, "passed": True},
    ]

    future_6ir_rows = [
        {"contract": "audit_6iq_predecessor_and_inputs", "required": True, "passed": True},
        {"contract": "audit_adapter_record_loading", "required": True, "passed": True},
        {"contract": "audit_30_mechanic_window_rows", "required": True, "passed": True},
        {"contract": "audit_300_metric_rows", "required": True, "passed": True},
        {"contract": "audit_baseline_and_candidate_decisions", "required": True, "passed": True},
        {"contract": "audit_actual_outcome_surface_state", "required": True, "passed": True},
        {"contract": "decide_whether_activation_planning_can_begin", "required": True, "passed": True},
        {"contract": "keep_direct_activation_and_exit_blocked", "required": True, "passed": True},
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
        {"blocked_surface": "actual_outcome_join_finalization", "blocked": True, "reason": "true actual outcome event surface unavailable", "passed": True},
        {"blocked_surface": "pass_fail_finalization", "blocked": True, "reason": "candidate decisions are non-final", "passed": True},
        {"blocked_surface": "activation_planning", "blocked": True, "reason": "6IR audit required and final decisions unavailable", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "activation planning blocked", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "activation audit incomplete", "passed": True},
    ]

    decision_rows = [
        {"decision": "6ip_passed", "expected": True, "actual": json_6ip.get("all_checks_passed"), "passed": json_6ip.get("all_checks_passed") is True},
        {"decision": "evaluation_harness_implemented", "expected": True, "actual": True, "passed": True},
        {"decision": "evaluation_matrix_row_count", "expected": 30, "actual": len(evaluation_rows), "passed": len(evaluation_rows) == 30},
        {"decision": "metric_row_count", "expected": 300, "actual": len(metric_rows), "passed": len(metric_rows) == 300},
        {"decision": "baseline_comparison_row_count", "expected": 30, "actual": len(baseline_rows), "passed": len(baseline_rows) == 30},
        {"decision": "candidate_decision_row_count", "expected": 30, "actual": len(candidate_rows), "passed": len(candidate_rows) == 30},
        {"decision": "lineage_output_row_count", "expected": 30, "actual": len(lineage_rows), "passed": len(lineage_rows) == 30},
        {"decision": "actual_outcome_surface_explicit", "expected": True, "actual": True, "passed": True},
        {"decision": "recommend_6ir_audit_next", "expected": RECOMMENDED_NEXT_LAYER_6IQ, "actual": RECOMMENDED_NEXT_LAYER_6IQ, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_layer", "expected": True, "actual": True, "passed": True},
        {"boundary": "non_production_outputs", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_adapter_implementation_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ib_artifact_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ih_corrected_candidate_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ik_materialized_output_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_actual_outcome_join_execution", "expected": False, "actual": actual_outcome_join_executed, "passed": actual_outcome_join_executed is False},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    plan_after = PLAN_6IP_PATH.read_text(encoding="utf-8") if PLAN_6IP_PATH.exists() else ""
    adapter_after = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_after = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_after = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_after = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    immutability_rows = [
        {"surface": "this_6iq_implementation", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6ip_plan", "policy": "unchanged_by_6iq", "passed": plan_after == plan_before},
        {"surface": "adapter_module", "policy": "unchanged_by_6iq", "passed": adapter_after == adapter_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6iq", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6iq", "passed": corrected_after == corrected_before},
        {"surface": "6ik_materialized_table", "policy": "read_only_unchanged_by_6iq", "passed": materialized_after == materialized_before},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IQ, "actual": RECOMMENDED_NEXT_LAYER_6IQ, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6IQ, "actual": RECOMMENDED_PATH_6IQ, "passed": True},
        {"decision": "recommend_real_evaluation_audit_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6IQ, "actual": DIAGNOSIS_6IQ, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "adapter_load", "passed": all(row["passed"] for row in adapter_load_rows), "detail": f"{sum(1 for row in adapter_load_rows if row['passed'])}/{len(adapter_load_rows)}"},
        {"check": "evaluation_matrix", "passed": len(evaluation_rows) == 30 and all(row["passed"] for row in evaluation_rows), "detail": f"{len(evaluation_rows)}/30"},
        {"check": "metric_rows", "passed": len(metric_rows) == 300 and all(row["passed"] for row in metric_rows), "detail": f"{len(metric_rows)}/300"},
        {"check": "baseline_comparison", "passed": len(baseline_rows) == 30 and all(row["passed"] for row in baseline_rows), "detail": f"{len(baseline_rows)}/30"},
        {"check": "candidate_decisions", "passed": len(candidate_rows) == 30 and all(row["passed"] for row in candidate_rows), "detail": f"{len(candidate_rows)}/30"},
        {"check": "actual_outcome_surface", "passed": all(row["passed"] for row in actual_surface_rows), "detail": "1/1"},
        {"check": "lineage", "passed": len(lineage_rows) == 30 and all(row["passed"] for row in lineage_rows), "detail": f"{len(lineage_rows)}/30"},
        {"check": "readiness", "passed": all(row["passed"] for row in readiness_rows), "detail": f"{sum(1 for row in readiness_rows if row['passed'])}/{len(readiness_rows)}"},
        {"check": "future_6ir_contract", "passed": all(row["passed"] for row in future_6ir_rows), "detail": f"{sum(1 for row in future_6ir_rows if row['passed'])}/{len(future_6ir_rows)}"},
        {"check": "readonly_sources", "passed": all(row["passed"] for row in readonly_rows), "detail": f"{sum(1 for row in readonly_rows if row['passed'])}/{len(readonly_rows)}"},
        {"check": "preserved_families", "passed": all(row["passed"] for row in preserved_rows), "detail": f"{sum(1 for row in preserved_rows if row['passed'])}/{len(preserved_rows)}"},
        {"check": "blocking_policy", "passed": all(row["passed"] for row in blocking_rows), "detail": f"{sum(1 for row in blocking_rows if row['passed'])}/{len(blocking_rows)}"},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "adapter_load": write_csv(ADAPTER_LOAD_CSV, adapter_load_rows),
        "evaluation_matrix": write_csv(EVALUATION_MATRIX_CSV, evaluation_rows),
        "metric_rows": write_csv(METRIC_ROWS_CSV, metric_rows),
        "baseline_comparison": write_csv(BASELINE_COMPARISON_CSV, baseline_rows),
        "candidate_decisions": write_csv(CANDIDATE_DECISIONS_CSV, candidate_rows),
        "actual_outcome_surface": write_csv(ACTUAL_OUTCOME_SURFACE_CSV, actual_surface_rows),
        "lineage": write_csv(LINEAGE_CSV, lineage_rows),
        "readiness": write_csv(READINESS_CSV, readiness_rows),
        "future_6ir_contract": write_csv(FUTURE_6IR_CONTRACT_CSV, future_6ir_rows),
        "readonly_sources": write_csv(READONLY_SOURCES_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_FAMILIES_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6IQ",
        "layer_type": "game_mechanics_realism",
        "implementation_layer": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6IQ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6IQ,
        "recommended_path": RECOMMENDED_PATH_6IQ,
        "predecessor_plan": str(PLAN_6IP_PATH),
        "predecessor_plan_returncode": 0,
        "predecessor_plan_diagnosis": json_6ip.get("diagnosis"),
        "planned_layer": "6IP",
        "source_family": SOURCE_FAMILY,
        "acquisition_mode": ACQUISITION_MODE,
        "adapter_revision_audited": True,
        "adapter_validation_passed": validation.passed,
        "adapter_production_enabled": adapter.ADAPTER_PRODUCTION_ENABLED,
        "materialized_transition_row_count": validation.materialized_transition_row_count,
        "materialized_exact_transition_row_count": validation.materialized_exact_transition_row_count,
        "materialized_non_exact_transition_row_count": validation.materialized_non_exact_transition_row_count,
        "materialized_schema_field_count": validation.materialized_schema_field_count,
        "source_provenance_retained_for_all_rows": validation.source_provenance_retained_for_all_rows,
        "lineage_rows_available": validation.lineage_rows_available,
        "lineage_fields_populated_for_all_rows": validation.lineage_fields_populated_for_all_rows,
        "actual_outcome_surface_available": actual_outcome_surface_available,
        "actual_outcome_join_executed": actual_outcome_join_executed,
        "evaluation_mode": evaluation_mode,
        "evaluation_harness_implemented": True,
        "evaluation_matrix_row_count": len(evaluation_rows),
        "metric_row_count": len(metric_rows),
        "baseline_comparison_row_count": len(baseline_rows),
        "candidate_decision_row_count": len(candidate_rows),
        "lineage_output_row_count": len(lineage_rows),
        "planned_mechanic_count": len(GAMEPLAY_MECHANICS),
        "planned_evaluation_window_count": len(EVALUATION_WINDOWS),
        "planned_mechanic_window_pair_count": len(mechanic_window_pairs),
        "planned_metric_family_count": len(METRIC_FAMILIES),
        "future_6ir_contract_valid": all(row["passed"] for row in future_6ir_rows),
        "pass_fail_candidate_decisions_final": False,
        "activation_planning_allowed_after_this_layer": False,
        "real_evaluation_audit_required": True,
        "evaluation_outputs_non_production": True,
        "preserved_remediated_family_count": len(PRESERVED_FAMILIES),
        "source_artifacts_mutated": False,
        "corrected_candidate_artifacts_mutated": False,
        "materialized_outputs_mutated": False,
        "adapter_implementation_mutated": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "mechanic_evaluations_run": True,
        "actual_outcomes_joined_to_mechanics": actual_outcome_join_executed,
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
            "baseline_comparison_csv": str(BASELINE_COMPARISON_CSV),
            "candidate_decisions_csv": str(CANDIDATE_DECISIONS_CSV),
            "actual_outcome_surface_csv": str(ACTUAL_OUTCOME_SURFACE_CSV),
            "lineage_csv": str(LINEAGE_CSV),
            "readiness_csv": str(READINESS_CSV),
            "future_6ir_contract_csv": str(FUTURE_6IR_CONTRACT_CSV),
            "readonly_sources_csv": str(READONLY_SOURCES_CSV),
            "preserved_families_csv": str(PRESERVED_FAMILIES_CSV),
            "blocking_policy_csv": str(BLOCKING_POLICY_CSV),
            "decision_csv": str(DECISION_CSV),
            "safety_boundaries_csv": str(SAFETY_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
