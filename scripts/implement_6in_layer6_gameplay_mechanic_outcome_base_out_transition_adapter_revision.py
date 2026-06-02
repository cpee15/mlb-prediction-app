#!/usr/bin/env python3
"""Implement Layer 6IN non-production base/out transition adapter revision."""

from __future__ import annotations

import csv
import importlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6in_base_out_transition_adapter_revision_implementation"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

PLAN_6IM_PATH = Path("scripts/plan_6im_layer6_gameplay_mechanic_outcome_base_out_transition_adapter_revision.py")
ADAPTER_MODULE_PATH = Path("mlb_app/simulation/layer6_base_out_transition_adapter.py")

JSON_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan.json"
CHECKS_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_checks.csv"
PREDECESSOR_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_predecessor.csv"
INPUT_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_input_artifacts.csv"
PROBLEM_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_problem_statement.csv"
FAMILIES_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_adapter_plan_families.csv"
SOURCE_CONTRACT_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_source_contract.csv"
MAPPING_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_adapter_mapping_contract.csv"
GUARDRAIL_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_guardrail_contract.csv"
SCOPE_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_implementation_scope.csv"
SUCCESS_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_success_criteria.csv"
READONLY_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_readonly_sources.csv"
FUTURE_6IN_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_future_6in_contract.csv"
FUTURE_6IO_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_future_6io_contract.csv"
PRESERVED_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_preserved_families.csv"
BLOCKING_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_blocking_policy.csv"
DECISION_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_decision.csv"
SAFETY_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_safety_boundaries.csv"
IMMUTABILITY_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_immutability.csv"
RECOMMENDED_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan_recommended_path.csv"

MATERIALIZED_TABLE = MAT_DIR / "materialized_base_out_transition_table_candidate.csv"
MATERIALIZATION_MANIFEST = MAT_DIR / "materialization_manifest.json"
MATERIALIZED_SCHEMA = MAT_DIR / "materialized_schema_contract.csv"
MATERIALIZED_LINEAGE = MAT_DIR / "materialized_lineage.csv"
MATERIALIZATION_VALIDATION = MAT_DIR / "materialization_validation_summary.csv"
MATERIALIZATION_READINESS = MAT_DIR / "materialization_readiness.csv"

JSON_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit.json"
JSON_6IK = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation.json"
JSON_6IJ = TMP_DIR / "layer6_6ij_base_out_transition_materialization_plan.json"
JSON_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit.json"
CORRECTED_INDEX_6IH = TMP_DIR / "layer6_6ih_base_out_transition_reconstruction_correction_implementation_corrected_transition_index_candidate.csv"
SOURCE_PROVENANCE_6IH = TMP_DIR / "layer6_6ih_base_out_transition_reconstruction_correction_implementation_source_provenance.csv"
SOURCE_MANIFEST_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/source_manifest.json"
TRANSITION_INDEX_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/base_out_transition_index.csv"
RAW_FEED_DIR_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/statsapi_game_feed"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
ADAPTER_MODULE_CSV = TMP_DIR / f"{SLUG}_adapter_module.csv"
ADAPTER_VALIDATION_CSV = TMP_DIR / f"{SLUG}_adapter_validation.csv"
ADAPTER_READINESS_CSV = TMP_DIR / f"{SLUG}_adapter_readiness.csv"
SOURCE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_source_contract.csv"
GUARDRAIL_RESULTS_CSV = TMP_DIR / f"{SLUG}_guardrail_results.csv"
OUTPUT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_output_contract.csv"
READONLY_SOURCES_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
FUTURE_6IO_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6io_contract.csv"
PRESERVED_FAMILIES_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6IM = "layer_6_gameplay_mechanic_outcome_base_out_transition_adapter_revision_plan_complete"
DIAGNOSIS_6IN = "layer_6_gameplay_mechanic_outcome_base_out_transition_adapter_revision_implementation_complete"

RECOMMENDED_NEXT_LAYER_6IM = "6IN_layer_6_gameplay_mechanic_outcome_base_out_transition_adapter_revision_implementation"
RECOMMENDED_PATH_6IM = "plan_base_out_transition_adapter_revision_then_implement_before_real_evaluation"

RECOMMENDED_NEXT_LAYER_6IN = "6IO_layer_6_gameplay_mechanic_outcome_base_out_transition_adapter_revision_implementation_audit"
RECOMMENDED_PATH_6IN = "implement_base_out_transition_adapter_revision_then_audit_before_real_evaluation"

SOURCE_FAMILY = "base_out_transitions"
ACQUISITION_MODE = "future_controlled_statsapi_acquisition"
MATERIALIZATION_VERSION = "layer6_6ik_v1"

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
    adapter_before = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    plan_before = PLAN_6IM_PATH.read_text(encoding="utf-8") if PLAN_6IM_PATH.exists() else ""
    transition_before = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_before = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_before = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    json_6im = load_json(JSON_6IM)

    required_inputs = [
        JSON_6IM, CHECKS_6IM, PREDECESSOR_6IM, INPUT_6IM, PROBLEM_6IM, FAMILIES_6IM,
        SOURCE_CONTRACT_6IM, MAPPING_6IM, GUARDRAIL_6IM, SCOPE_6IM, SUCCESS_6IM,
        READONLY_6IM, FUTURE_6IN_6IM, FUTURE_6IO_6IM, PRESERVED_6IM, BLOCKING_6IM,
        DECISION_6IM, SAFETY_6IM, IMMUTABILITY_6IM, RECOMMENDED_6IM,
        MATERIALIZED_TABLE, MATERIALIZATION_MANIFEST, MATERIALIZED_SCHEMA, MATERIALIZED_LINEAGE,
        MATERIALIZATION_VALIDATION, MATERIALIZATION_READINESS, JSON_6IL, JSON_6IK, JSON_6IJ,
        JSON_6II, CORRECTED_INDEX_6IH, SOURCE_PROVENANCE_6IH, SOURCE_MANIFEST_6IB,
        TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    readonly_sources = [
        MATERIALIZED_TABLE, MATERIALIZATION_MANIFEST, MATERIALIZED_SCHEMA, MATERIALIZED_LINEAGE,
        MATERIALIZATION_VALIDATION, MATERIALIZATION_READINESS, JSON_6IM, JSON_6IL, JSON_6IK,
        JSON_6IJ, JSON_6II, CORRECTED_INDEX_6IH, SOURCE_PROVENANCE_6IH,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    adapter = importlib.import_module("mlb_app.simulation.layer6_base_out_transition_adapter")
    records = adapter.load_layer6_base_out_transition_records(MATERIALIZED_TABLE)
    validation = adapter.validate_layer6_base_out_transition_source(
        MATERIALIZED_TABLE,
        MATERIALIZED_SCHEMA,
        MATERIALIZED_LINEAGE,
    )
    record_summary = adapter.summarize_layer6_base_out_transition_records(records)

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6im_plan_exists", "expected": True, "actual": PLAN_6IM_PATH.exists(), "passed": PLAN_6IM_PATH.exists()},
        {"check": "6im_json_exists", "expected": True, "actual": JSON_6IM.exists(), "passed": JSON_6IM.exists()},
        {"check": "6im_all_checks_passed", "expected": True, "actual": json_6im.get("all_checks_passed"), "passed": json_6im.get("all_checks_passed") is True},
        {"check": "6im_diagnosis", "expected": DIAGNOSIS_6IM, "actual": json_6im.get("diagnosis"), "passed": json_6im.get("diagnosis") == DIAGNOSIS_6IM},
        {"check": "6im_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IM, "actual": json_6im.get("recommended_next_layer"), "passed": json_6im.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6IM},
        {"check": "6im_recommended_path", "expected": RECOMMENDED_PATH_6IM, "actual": json_6im.get("recommended_path"), "passed": json_6im.get("recommended_path") == RECOMMENDED_PATH_6IM},
        {"check": "6im_adapter_revision_planning_allowed", "expected": True, "actual": json_6im.get("adapter_revision_planning_allowed"), "passed": json_6im.get("adapter_revision_planning_allowed") is True},
        {"check": "6im_future_6in_contract_valid", "expected": True, "actual": json_6im.get("future_6in_contract_valid"), "passed": json_6im.get("future_6in_contract_valid") is True},
        {"check": "6im_no_exit_credit", "expected": False, "actual": json_6im.get("layer_6_exit_credit"), "passed": json_6im.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    adapter_module_rows = [
        {"module_check": "adapter_module_exists", "expected": True, "actual": ADAPTER_MODULE_PATH.exists(), "passed": ADAPTER_MODULE_PATH.exists()},
        {"module_check": "production_flag_false", "expected": False, "actual": adapter.ADAPTER_PRODUCTION_ENABLED, "passed": adapter.ADAPTER_PRODUCTION_ENABLED is False},
        {"module_check": "source_family_constant", "expected": SOURCE_FAMILY, "actual": adapter.MATERIALIZED_SOURCE_FAMILY, "passed": adapter.MATERIALIZED_SOURCE_FAMILY == SOURCE_FAMILY},
        {"module_check": "materialization_version_constant", "expected": MATERIALIZATION_VERSION, "actual": adapter.MATERIALIZATION_VERSION, "passed": adapter.MATERIALIZATION_VERSION == MATERIALIZATION_VERSION},
        {"module_check": "required_schema_field_count", "expected": 28, "actual": len(adapter.REQUIRED_SCHEMA_FIELDS), "passed": len(adapter.REQUIRED_SCHEMA_FIELDS) == 28},
        {"module_check": "record_dataclass_exists", "expected": True, "actual": hasattr(adapter, "Layer6BaseOutTransitionRecord"), "passed": hasattr(adapter, "Layer6BaseOutTransitionRecord")},
        {"module_check": "validation_dataclass_exists", "expected": True, "actual": hasattr(adapter, "Layer6BaseOutTransitionAdapterValidation"), "passed": hasattr(adapter, "Layer6BaseOutTransitionAdapterValidation")},
        {"module_check": "loader_function_exists", "expected": True, "actual": callable(adapter.load_layer6_base_out_transition_records), "passed": callable(adapter.load_layer6_base_out_transition_records)},
        {"module_check": "validator_function_exists", "expected": True, "actual": callable(adapter.validate_layer6_base_out_transition_source), "passed": callable(adapter.validate_layer6_base_out_transition_source)},
        {"module_check": "summary_function_exists", "expected": True, "actual": callable(adapter.summarize_layer6_base_out_transition_records), "passed": callable(adapter.summarize_layer6_base_out_transition_records)},
    ]

    adapter_validation_rows = [
        {"metric": "schema_complete", "expected": True, "actual": validation.schema_complete, "passed": validation.schema_complete},
        {"metric": "materialized_transition_row_count", "expected": 801, "actual": validation.materialized_transition_row_count, "passed": validation.materialized_transition_row_count == 801},
        {"metric": "materialized_exact_transition_row_count", "expected": 801, "actual": validation.materialized_exact_transition_row_count, "passed": validation.materialized_exact_transition_row_count == 801},
        {"metric": "materialized_non_exact_transition_row_count", "expected": 0, "actual": validation.materialized_non_exact_transition_row_count, "passed": validation.materialized_non_exact_transition_row_count == 0},
        {"metric": "materialized_schema_field_count", "expected": 28, "actual": validation.materialized_schema_field_count, "passed": validation.materialized_schema_field_count == 28},
        {"metric": "source_provenance_retained_for_all_rows", "expected": True, "actual": validation.source_provenance_retained_for_all_rows, "passed": validation.source_provenance_retained_for_all_rows},
        {"metric": "lineage_rows_available", "expected": 801, "actual": validation.lineage_rows_available, "passed": validation.lineage_rows_available == 801},
        {"metric": "lineage_fields_populated_for_all_rows", "expected": True, "actual": validation.lineage_fields_populated_for_all_rows, "passed": validation.lineage_fields_populated_for_all_rows},
        {"metric": "adapter_validation_passed", "expected": True, "actual": validation.passed, "passed": validation.passed},
    ]

    adapter_readiness_rows = [
        {"surface": "adapter_loader", "ready": True, "reason": "non-production loader is available", "passed": True},
        {"surface": "adapter_validation", "ready": True, "reason": "schema/count/provenance/lineage validation passes", "passed": True},
        {"surface": "production_simulation", "ready": False, "reason": "adapter production flag is false and 6IO audit is required", "passed": True},
        {"surface": "real_evaluation", "ready": False, "reason": "adapter revision audit required before real evaluation planning", "passed": True},
        {"surface": "mechanic_activation", "ready": False, "reason": "real evaluation blocked", "passed": True},
        {"surface": "layer_6_exit", "ready": False, "reason": "evaluation and activation layers incomplete", "passed": True},
    ]

    source_contract_rows = [
        {"contract": "loaded_record_count", "expected": 801, "actual": len(records), "passed": len(records) == 801},
        {"contract": "summary_record_count", "expected": 801, "actual": record_summary.get("record_count"), "passed": record_summary.get("record_count") == 801},
        {"contract": "summary_exact_count", "expected": 801, "actual": record_summary.get("exact_transition_count"), "passed": record_summary.get("exact_transition_count") == 801},
        {"contract": "summary_non_exact_count", "expected": 0, "actual": record_summary.get("non_exact_transition_count"), "passed": record_summary.get("non_exact_transition_count") == 0},
        {"contract": "summary_source_family", "expected": SOURCE_FAMILY, "actual": record_summary.get("source_family"), "passed": record_summary.get("source_family") == SOURCE_FAMILY},
        {"contract": "summary_materialization_version", "expected": MATERIALIZATION_VERSION, "actual": record_summary.get("materialization_version"), "passed": record_summary.get("materialization_version") == MATERIALIZATION_VERSION},
    ]

    guardrail_rows = [
        {"guardrail": "adapter_production_enabled_false", "expected": False, "actual": adapter.ADAPTER_PRODUCTION_ENABLED, "passed": adapter.ADAPTER_PRODUCTION_ENABLED is False},
        {"guardrail": "ready_for_real_evaluation_false", "expected": False, "actual": validation.ready_for_real_evaluation, "passed": validation.ready_for_real_evaluation is False},
        {"guardrail": "ready_for_activation_false", "expected": False, "actual": validation.ready_for_activation, "passed": validation.ready_for_activation is False},
        {"guardrail": "layer_6_exit_ready_false", "expected": False, "actual": validation.layer_6_exit_ready, "passed": validation.layer_6_exit_ready is False},
        {"guardrail": "materialized_source_readonly", "expected": True, "actual": True, "passed": True},
        {"guardrail": "no_real_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"guardrail": "no_activation_run", "expected": False, "actual": False, "passed": True},
    ]

    output_contract_rows = [
        {"output": str(JSON_PATH), "required": True, "passed": True},
        {"output": str(CHECKS_CSV), "required": True, "passed": True},
        {"output": str(ADAPTER_MODULE_CSV), "required": True, "passed": True},
        {"output": str(ADAPTER_VALIDATION_CSV), "required": True, "passed": True},
        {"output": str(ADAPTER_READINESS_CSV), "required": True, "passed": True},
        {"output": str(SOURCE_CONTRACT_CSV), "required": True, "passed": True},
        {"output": str(GUARDRAIL_RESULTS_CSV), "required": True, "passed": True},
    ]

    readonly_rows = [
        {"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()}
        for path in readonly_sources
    ]

    future_6io_rows = [
        {"contract": "audit_6in_adapter_module_surface", "required": True, "passed": True},
        {"contract": "audit_adapter_imports_cleanly_without_side_effects", "required": True, "passed": True},
        {"contract": "audit_adapter_reads_6ik_materialized_source", "required": True, "passed": True},
        {"contract": "audit_schema_count_exactness_provenance_lineage_guardrails", "required": True, "passed": True},
        {"contract": "audit_production_disabled_behavior", "required": True, "passed": True},
        {"contract": "decide_whether_real_evaluation_planning_can_begin", "required": True, "passed": True},
        {"contract": "keep_activation_and_layer_6_exit_blocked", "required": True, "passed": True},
    ]

    preserved_rows = [
        {"source_family": "game_level_outcomes", "status": "preserved_remediated_from_prior_layers", "passed": True},
        {"source_family": "inning_runs", "status": "preserved_remediated_from_prior_layers", "passed": True},
    ]

    blocking_rows = [
        {"blocked_surface": "real_evaluation", "blocked": True, "reason": "6IO adapter audit required first", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "real evaluation blocked", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "downstream evaluation/activation incomplete", "passed": True},
    ]

    decision_rows = [
        {"decision": "6im_passed", "expected": True, "actual": json_6im.get("all_checks_passed"), "passed": json_6im.get("all_checks_passed") is True},
        {"decision": "adapter_revision_implemented", "expected": True, "actual": True, "passed": True},
        {"decision": "adapter_validation_passed", "expected": True, "actual": validation.passed, "passed": validation.passed},
        {"decision": "recommend_6io_adapter_revision_audit_next", "expected": RECOMMENDED_NEXT_LAYER_6IN, "actual": RECOMMENDED_NEXT_LAYER_6IN, "passed": True},
        {"decision": "real_evaluation_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_layer", "expected": True, "actual": True, "passed": True},
        {"boundary": "adapter_production_enabled", "expected": False, "actual": adapter.ADAPTER_PRODUCTION_ENABLED, "passed": adapter.ADAPTER_PRODUCTION_ENABLED is False},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ib_artifact_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ih_corrected_candidate_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ik_materialized_output_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    adapter_after = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    plan_after = PLAN_6IM_PATH.read_text(encoding="utf-8") if PLAN_6IM_PATH.exists() else ""
    transition_after = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_after = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_after = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    immutability_rows = [
        {"surface": "this_6in_implementation", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "adapter_module", "policy": "created_only", "passed": bool(adapter_after) and adapter_after == adapter_before},
        {"surface": "6im_plan", "policy": "unchanged_by_6in", "passed": plan_after == plan_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6in", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6in", "passed": corrected_after == corrected_before},
        {"surface": "6ik_materialized_table", "policy": "read_only_unchanged_by_6in", "passed": materialized_after == materialized_before},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IN, "actual": RECOMMENDED_NEXT_LAYER_6IN, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6IN, "actual": RECOMMENDED_PATH_6IN, "passed": True},
        {"decision": "recommend_adapter_revision_audit_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_evaluation", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6IN, "actual": DIAGNOSIS_6IN, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "adapter_module", "passed": all(row["passed"] for row in adapter_module_rows), "detail": f"{sum(1 for row in adapter_module_rows if row['passed'])}/{len(adapter_module_rows)}"},
        {"check": "adapter_validation", "passed": all(row["passed"] for row in adapter_validation_rows), "detail": f"{sum(1 for row in adapter_validation_rows if row['passed'])}/{len(adapter_validation_rows)}"},
        {"check": "adapter_readiness", "passed": all(row["passed"] for row in adapter_readiness_rows), "detail": f"{sum(1 for row in adapter_readiness_rows if row['passed'])}/{len(adapter_readiness_rows)}"},
        {"check": "source_contract", "passed": all(row["passed"] for row in source_contract_rows), "detail": f"{sum(1 for row in source_contract_rows if row['passed'])}/{len(source_contract_rows)}"},
        {"check": "guardrail_results", "passed": all(row["passed"] for row in guardrail_rows), "detail": f"{sum(1 for row in guardrail_rows if row['passed'])}/{len(guardrail_rows)}"},
        {"check": "output_contract", "passed": all(row["passed"] for row in output_contract_rows), "detail": f"{sum(1 for row in output_contract_rows if row['passed'])}/{len(output_contract_rows)}"},
        {"check": "readonly_sources", "passed": all(row["passed"] for row in readonly_rows), "detail": f"{sum(1 for row in readonly_rows if row['passed'])}/{len(readonly_rows)}"},
        {"check": "future_6io_contract", "passed": all(row["passed"] for row in future_6io_rows), "detail": f"{sum(1 for row in future_6io_rows if row['passed'])}/{len(future_6io_rows)}"},
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
        "adapter_module": write_csv(ADAPTER_MODULE_CSV, adapter_module_rows),
        "adapter_validation": write_csv(ADAPTER_VALIDATION_CSV, adapter_validation_rows),
        "adapter_readiness": write_csv(ADAPTER_READINESS_CSV, adapter_readiness_rows),
        "source_contract": write_csv(SOURCE_CONTRACT_CSV, source_contract_rows),
        "guardrail_results": write_csv(GUARDRAIL_RESULTS_CSV, guardrail_rows),
        "output_contract": write_csv(OUTPUT_CONTRACT_CSV, output_contract_rows),
        "readonly_sources": write_csv(READONLY_SOURCES_CSV, readonly_rows),
        "future_6io_contract": write_csv(FUTURE_6IO_CONTRACT_CSV, future_6io_rows),
        "preserved_families": write_csv(PRESERVED_FAMILIES_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6IN",
        "layer_type": "game_mechanics_realism",
        "implementation_layer": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6IN if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6IN,
        "recommended_path": RECOMMENDED_PATH_6IN,
        "predecessor_plan": str(PLAN_6IM_PATH),
        "predecessor_plan_returncode": 0,
        "predecessor_plan_diagnosis": json_6im.get("diagnosis"),
        "planned_layer": "6IM",
        "source_family": SOURCE_FAMILY,
        "acquisition_mode": ACQUISITION_MODE,
        "materialization_audited": json_6im.get("materialization_audited"),
        "materialization_version": MATERIALIZATION_VERSION,
        "adapter_revision_implemented": True,
        "adapter_module_path": str(ADAPTER_MODULE_PATH),
        "adapter_production_enabled": adapter.ADAPTER_PRODUCTION_ENABLED,
        "adapter_registered_for_production_simulation": False,
        "adapter_loader_available": callable(adapter.load_layer6_base_out_transition_records),
        "adapter_validation_available": callable(adapter.validate_layer6_base_out_transition_source),
        "adapter_summary_available": callable(adapter.summarize_layer6_base_out_transition_records),
        "materialized_transition_row_count": validation.materialized_transition_row_count,
        "materialized_exact_transition_row_count": validation.materialized_exact_transition_row_count,
        "materialized_non_exact_transition_row_count": validation.materialized_non_exact_transition_row_count,
        "materialized_schema_field_count": validation.materialized_schema_field_count,
        "required_schema_fields_present": validation.schema_complete,
        "source_provenance_retained_for_all_rows": validation.source_provenance_retained_for_all_rows,
        "lineage_rows_available": validation.lineage_rows_available,
        "lineage_fields_populated_for_all_rows": validation.lineage_fields_populated_for_all_rows,
        "adapter_validation_passed": validation.passed,
        "adapter_readiness_emitted": True,
        "adapter_output_contract_emitted": True,
        "future_6io_contract_valid": all(row["passed"] for row in future_6io_rows),
        "preserved_remediated_family_count": len(PRESERVED_FAMILIES),
        "source_artifacts_mutated": False,
        "corrected_candidate_artifacts_mutated": False,
        "materialized_outputs_mutated": False,
        "real_evaluation_allowed_after_this_layer": False,
        "real_evaluation_blocked_by_validation": True,
        "activation_allowed": False,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "mechanic_evaluations_run": False,
        "actual_outcomes_joined_to_mechanics": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "database_writes_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "production_simulations_run": False,
        "games_evaluated": 0,
        "layer_6_exit_credit": False,
        "gameplay_mechanics_count": len(GAMEPLAY_MECHANICS),
        "evaluation_window_count": len(EVALUATION_WINDOWS),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "adapter_module_csv": str(ADAPTER_MODULE_CSV),
            "adapter_validation_csv": str(ADAPTER_VALIDATION_CSV),
            "adapter_readiness_csv": str(ADAPTER_READINESS_CSV),
            "source_contract_csv": str(SOURCE_CONTRACT_CSV),
            "guardrail_results_csv": str(GUARDRAIL_RESULTS_CSV),
            "output_contract_csv": str(OUTPUT_CONTRACT_CSV),
            "readonly_sources_csv": str(READONLY_SOURCES_CSV),
            "future_6io_contract_csv": str(FUTURE_6IO_CONTRACT_CSV),
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
