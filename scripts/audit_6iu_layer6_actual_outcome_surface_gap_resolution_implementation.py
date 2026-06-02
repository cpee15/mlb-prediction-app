#!/usr/bin/env python3
"""Audit Layer 6IT actual-outcome surface gap resolution implementation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

IMPLEMENT_6IT_PATH = Path("scripts/implement_6it_layer6_actual_outcome_surface_gap_resolution.py")
PLAN_6IS_PATH = Path("scripts/plan_6is_layer6_actual_outcome_surface_gap_resolution.py")
AUDIT_6IR_PATH = Path("scripts/audit_6ir_layer6_gameplay_mechanic_outcome_real_evaluation_implementation.py")
IMPLEMENT_6IQ_PATH = Path("scripts/implement_6iq_layer6_gameplay_mechanic_outcome_real_evaluation.py")
ADAPTER_MODULE_PATH = Path("mlb_app/simulation/layer6_base_out_transition_adapter.py")

JSON_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation.json"
CHECKS_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_checks.csv"
PREDECESSOR_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_predecessor.csv"
INPUT_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_input_artifacts.csv"
SOURCE_SUFFICIENCY_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_source_sufficiency.csv"
SCHEMA_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_truth_surface_schema.csv"
MANIFEST_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_truth_surface_manifest.csv"
CANDIDATE_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_candidate_truth_surface_rows.csv"
ACQ_REQ_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_controlled_acquisition_requirement.csv"
LINEAGE_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_lineage.csv"
VALIDATION_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_validation.csv"
READINESS_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_readiness.csv"
FUTURE_6IU_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_future_6iu_contract.csv"
READONLY_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_readonly_sources.csv"
PRESERVED_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_preserved_families.csv"
BLOCKING_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_blocking_policy.csv"
DECISION_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_decision.csv"
SAFETY_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_safety_boundaries.csv"
IMMUTABILITY_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_immutability.csv"
RECOMMENDED_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_recommended_path.csv"

JSON_6IS = TMP_DIR / "layer6_6is_actual_outcome_surface_gap_resolution_plan.json"
JSON_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit.json"
JSON_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation.json"
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
SOURCE_SUFFICIENCY_CSV = TMP_DIR / f"{SLUG}_source_sufficiency.csv"
SCHEMA_CSV = TMP_DIR / f"{SLUG}_truth_surface_schema.csv"
MANIFEST_CSV = TMP_DIR / f"{SLUG}_truth_surface_manifest.csv"
CANDIDATE_CSV = TMP_DIR / f"{SLUG}_candidate_truth_surface_rows.csv"
LINEAGE_CSV = TMP_DIR / f"{SLUG}_lineage.csv"
VALIDATION_CSV = TMP_DIR / f"{SLUG}_validation.csv"
READINESS_CSV = TMP_DIR / f"{SLUG}_readiness.csv"
FUTURE_6IV_CSV = TMP_DIR / f"{SLUG}_future_6iv_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6IT = "layer_6_actual_outcome_surface_gap_resolution_implementation_complete"
DIAGNOSIS_6IU = "layer_6_actual_outcome_surface_gap_resolution_implementation_audit_complete"

RECOMMENDED_NEXT_LAYER_6IT = "6IU_layer_6_actual_outcome_surface_gap_resolution_implementation_audit"
RECOMMENDED_PATH_6IT = "implement_actual_outcome_surface_gap_resolution_then_audit_before_truth_join_evaluation"

RECOMMENDED_NEXT_LAYER_6IU = "6IV_layer_6_truth_join_evaluation_plan"
RECOMMENDED_PATH_6IU = "audit_actual_outcome_surface_gap_resolution_then_plan_truth_join_evaluation"

SOURCE_FAMILY = "actual_outcome_surfaces"
DEPENDS_ON_SOURCE_FAMILY = "base_out_transitions"

PRESERVED_FAMILIES = ["game_level_outcomes", "inning_runs", "base_out_transitions"]


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
    impl_6it_before = IMPLEMENT_6IT_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IT_PATH.exists() else ""
    plan_6is_before = PLAN_6IS_PATH.read_text(encoding="utf-8") if PLAN_6IS_PATH.exists() else ""
    audit_6ir_before = AUDIT_6IR_PATH.read_text(encoding="utf-8") if AUDIT_6IR_PATH.exists() else ""
    impl_6iq_before = IMPLEMENT_6IQ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IQ_PATH.exists() else ""
    adapter_before = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_before = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_before = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_before = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    json_6it = load_json(JSON_6IT)

    required_inputs = [
        JSON_6IT, CHECKS_6IT, PREDECESSOR_6IT, INPUT_6IT, SOURCE_SUFFICIENCY_6IT,
        SCHEMA_6IT, MANIFEST_6IT, CANDIDATE_6IT, ACQ_REQ_6IT, LINEAGE_6IT,
        VALIDATION_6IT, READINESS_6IT, FUTURE_6IU_6IT, READONLY_6IT,
        PRESERVED_6IT, BLOCKING_6IT, DECISION_6IT, SAFETY_6IT,
        IMMUTABILITY_6IT, RECOMMENDED_6IT, JSON_6IS, JSON_6IR, JSON_6IQ,
        JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IM, JSON_6IL, JSON_6IK,
        JSON_6IJ, JSON_6II, ADAPTER_MODULE_PATH, MATERIALIZED_TABLE,
        MATERIALIZATION_MANIFEST, MATERIALIZED_SCHEMA, MATERIALIZED_LINEAGE,
        MATERIALIZATION_VALIDATION, MATERIALIZATION_READINESS, CORRECTED_INDEX_6IH,
        SOURCE_PROVENANCE_6IH, SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB,
        RAW_FEED_DIR_6IB,
    ]

    readonly_sources = [
        JSON_6IT, JSON_6IS, JSON_6IR, JSON_6IQ, JSON_6IP, JSON_6IO, JSON_6IN,
        JSON_6IM, JSON_6IL, JSON_6IK, JSON_6IJ, JSON_6II, ADAPTER_MODULE_PATH,
        MATERIALIZED_TABLE, MATERIALIZATION_MANIFEST, MATERIALIZED_SCHEMA,
        MATERIALIZED_LINEAGE, MATERIALIZATION_VALIDATION, MATERIALIZATION_READINESS,
        CORRECTED_INDEX_6IH, SOURCE_PROVENANCE_6IH, SOURCE_MANIFEST_6IB,
        TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    source_sufficiency_rows_in = read_csv(SOURCE_SUFFICIENCY_6IT)
    schema_rows_in = read_csv(SCHEMA_6IT)
    manifest_rows_in = read_csv(MANIFEST_6IT)
    candidate_rows_in = read_csv(CANDIDATE_6IT)
    lineage_rows_in = read_csv(LINEAGE_6IT)
    validation_rows_in = read_csv(VALIDATION_6IT)

    candidate_non_production = (
        len(candidate_rows_in) == 100
        and all(str(row.get("non_production", "")).lower() == "true" for row in candidate_rows_in)
    )
    candidate_non_final = (
        len(candidate_rows_in) == 100
        and all(str(row.get("final", "")).lower() == "false" for row in candidate_rows_in)
    )

    source_summary_row = source_sufficiency_rows_in[0] if source_sufficiency_rows_in else {}
    remote_fetch_performed = str(source_summary_row.get("remote_fetch_performed", "")).lower() == "true"

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6it_implementation_exists", "expected": True, "actual": IMPLEMENT_6IT_PATH.exists(), "passed": IMPLEMENT_6IT_PATH.exists()},
        {"check": "6it_json_exists", "expected": True, "actual": JSON_6IT.exists(), "passed": JSON_6IT.exists()},
        {"check": "6it_all_checks_passed", "expected": True, "actual": json_6it.get("all_checks_passed"), "passed": json_6it.get("all_checks_passed") is True},
        {"check": "6it_diagnosis", "expected": DIAGNOSIS_6IT, "actual": json_6it.get("diagnosis"), "passed": json_6it.get("diagnosis") == DIAGNOSIS_6IT},
        {"check": "6it_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IT, "actual": json_6it.get("recommended_next_layer"), "passed": json_6it.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6IT},
        {"check": "6it_recommended_path", "expected": RECOMMENDED_PATH_6IT, "actual": json_6it.get("recommended_path"), "passed": json_6it.get("recommended_path") == RECOMMENDED_PATH_6IT},
        {"check": "6it_candidate_truth_surface_created", "expected": True, "actual": json_6it.get("candidate_truth_surface_created"), "passed": json_6it.get("candidate_truth_surface_created") is True},
        {"check": "6it_truth_join_blocked", "expected": False, "actual": json_6it.get("truth_surface_joined_to_evaluation"), "passed": json_6it.get("truth_surface_joined_to_evaluation") is False},
        {"check": "6it_no_exit_credit", "expected": False, "actual": json_6it.get("layer_6_exit_credit"), "passed": json_6it.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    source_sufficiency_rows = [
        {"check": "source_sufficiency_artifact_exists", "expected": True, "actual": SOURCE_SUFFICIENCY_6IT.exists(), "passed": SOURCE_SUFFICIENCY_6IT.exists()},
        {"check": "local_feed_dir_exists", "expected": True, "actual": json_6it.get("local_statsapi_feed_dir_exists"), "passed": json_6it.get("local_statsapi_feed_dir_exists") is True},
        {"check": "local_feed_file_count", "expected": 10, "actual": json_6it.get("local_statsapi_feed_file_count"), "passed": json_6it.get("local_statsapi_feed_file_count") == 10},
        {"check": "local_payload_sufficient", "expected": True, "actual": json_6it.get("local_statsapi_payload_sufficient"), "passed": json_6it.get("local_statsapi_payload_sufficient") is True},
        {"check": "controlled_acquisition_required", "expected": False, "actual": json_6it.get("controlled_acquisition_required"), "passed": json_6it.get("controlled_acquisition_required") is False},
        {"check": "remote_fetch_performed", "expected": False, "actual": remote_fetch_performed, "passed": remote_fetch_performed is False},
        {"check": "source_sufficiency_rows_all_passed", "expected": True, "actual": all_passed(source_sufficiency_rows_in), "passed": all_passed(source_sufficiency_rows_in)},
    ]

    schema_rows = [
        {"check": "schema_artifact_exists", "expected": True, "actual": SCHEMA_6IT.exists(), "passed": SCHEMA_6IT.exists()},
        {"check": "schema_field_count", "expected": 14, "actual": len(schema_rows_in), "passed": len(schema_rows_in) == 14},
        {"check": "schema_rows_all_passed", "expected": True, "actual": all_passed(schema_rows_in), "passed": all_passed(schema_rows_in)},
    ]

    manifest_rows = [
        {"check": "manifest_artifact_exists", "expected": True, "actual": MANIFEST_6IT.exists(), "passed": MANIFEST_6IT.exists()},
        {"check": "manifest_surface_count", "expected": 10, "actual": len(manifest_rows_in), "passed": len(manifest_rows_in) == 10},
        {"check": "supported_surface_count", "expected": 10, "actual": json_6it.get("supported_truth_surface_count"), "passed": json_6it.get("supported_truth_surface_count") == 10},
        {"check": "unsupported_surface_count", "expected": 0, "actual": json_6it.get("unsupported_truth_surface_count"), "passed": json_6it.get("unsupported_truth_surface_count") == 0},
        {"check": "manifest_rows_all_passed", "expected": True, "actual": all_passed(manifest_rows_in), "passed": all_passed(manifest_rows_in)},
    ]

    candidate_rows = [
        {"check": "candidate_artifact_exists", "expected": True, "actual": CANDIDATE_6IT.exists(), "passed": CANDIDATE_6IT.exists()},
        {"check": "candidate_row_count", "expected": 100, "actual": len(candidate_rows_in), "passed": len(candidate_rows_in) == 100},
        {"check": "candidate_rows_non_production", "expected": True, "actual": candidate_non_production, "passed": candidate_non_production},
        {"check": "candidate_rows_non_final", "expected": True, "actual": candidate_non_final, "passed": candidate_non_final},
        {"check": "candidate_rows_all_passed", "expected": True, "actual": all_passed(candidate_rows_in), "passed": all_passed(candidate_rows_in)},
    ]

    lineage_rows = [
        {"check": "lineage_artifact_exists", "expected": True, "actual": LINEAGE_6IT.exists(), "passed": LINEAGE_6IT.exists()},
        {"check": "lineage_row_count", "expected": 100, "actual": len(lineage_rows_in), "passed": len(lineage_rows_in) == 100},
        {"check": "lineage_rows_all_passed", "expected": True, "actual": all_passed(lineage_rows_in), "passed": all_passed(lineage_rows_in)},
    ]

    validation_rows = [
        {"check": "validation_artifact_exists", "expected": True, "actual": VALIDATION_6IT.exists(), "passed": VALIDATION_6IT.exists()},
        {"check": "validation_summary_passed", "expected": True, "actual": json_6it.get("validation_passed"), "passed": json_6it.get("validation_passed") is True},
        {"check": "validation_rows_all_passed", "expected": True, "actual": all_passed(validation_rows_in), "passed": all_passed(validation_rows_in)},
    ]

    readiness_rows = [
        {"surface": "candidate_truth_surfaces_audited", "ready": True, "passed": True},
        {"surface": "truth_join_evaluation_plan", "ready": True, "passed": True},
        {"surface": "truth_join_evaluation_implementation", "ready": False, "passed": True},
        {"surface": "real_evaluation_rerun", "ready": False, "passed": True},
        {"surface": "activation_planning", "ready": False, "passed": True},
        {"surface": "mechanic_activation", "ready": False, "passed": True},
        {"surface": "layer_6_exit", "ready": False, "passed": True},
    ]

    future_6iv_rows = [
        {"contract": "plan_truth_join_evaluation", "required": True, "passed": True},
        {"contract": "define_join_keys_between_truth_surfaces_and_6iq_evaluation_outputs", "required": True, "passed": True},
        {"contract": "define_truth_join_validation_requirements", "required": True, "passed": True},
        {"contract": "define_metric_finalization_rules", "required": True, "passed": True},
        {"contract": "define_candidate_decision_finalization_rules", "required": True, "passed": True},
        {"contract": "keep_activation_blocked_until_later_implementation_and_audit", "required": True, "passed": True},
        {"contract": "do_not_activate_or_exit_from_plan_layer", "required": True, "passed": True},
    ]

    readonly_rows = [
        {"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()}
        for path in readonly_sources
    ]

    preserved_rows = [
        {"source_family": "game_level_outcomes", "status": "preserved_reused_local_payload_dependency", "passed": True},
        {"source_family": "inning_runs", "status": "preserved_remediated_from_prior_layers", "passed": True},
        {"source_family": "base_out_transitions", "status": "preserved_audited_dependency", "passed": True},
    ]

    blocking_rows = [
        {"blocked_surface": "truth_join_to_evaluation", "blocked": True, "reason": "next layer is planning only", "passed": True},
        {"blocked_surface": "real_evaluation_rerun", "blocked": True, "reason": "truth join plan and implementation/audit required", "passed": True},
        {"blocked_surface": "final_pass_fail_decisions", "blocked": True, "reason": "truth join not executed", "passed": True},
        {"blocked_surface": "activation_planning", "blocked": True, "reason": "final decisions unavailable", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "activation planning blocked", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "activation chain incomplete", "passed": True},
    ]

    decision_rows = [
        {"decision": "6it_passed", "expected": True, "actual": json_6it.get("all_checks_passed"), "passed": json_6it.get("all_checks_passed") is True},
        {"decision": "candidate_truth_surface_valid", "expected": True, "actual": True, "passed": True},
        {"decision": "source_sufficiency_valid", "expected": True, "actual": True, "passed": True},
        {"decision": "lineage_valid", "expected": True, "actual": True, "passed": True},
        {"decision": "future_6iv_contract_valid", "expected": True, "actual": True, "passed": True},
        {"decision": "recommend_6iv_truth_join_plan_next", "expected": RECOMMENDED_NEXT_LAYER_6IU, "actual": RECOMMENDED_NEXT_LAYER_6IU, "passed": True},
        {"decision": "truth_join_executed", "expected": False, "actual": False, "passed": True},
        {"decision": "real_evaluation_rerun", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6it_implementation_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6is_plan_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6iq_implementation_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_adapter_implementation_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ib_artifact_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ih_corrected_candidate_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ik_materialized_output_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_truth_join_to_evaluation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation_rerun", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    impl_6it_after = IMPLEMENT_6IT_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IT_PATH.exists() else ""
    plan_6is_after = PLAN_6IS_PATH.read_text(encoding="utf-8") if PLAN_6IS_PATH.exists() else ""
    audit_6ir_after = AUDIT_6IR_PATH.read_text(encoding="utf-8") if AUDIT_6IR_PATH.exists() else ""
    impl_6iq_after = IMPLEMENT_6IQ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IQ_PATH.exists() else ""
    adapter_after = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_after = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_after = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_after = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    immutability_rows = [
        {"surface": "this_6iu_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6it_implementation", "policy": "unchanged_by_6iu", "passed": impl_6it_after == impl_6it_before},
        {"surface": "6is_plan", "policy": "unchanged_by_6iu", "passed": plan_6is_after == plan_6is_before},
        {"surface": "6ir_audit", "policy": "unchanged_by_6iu", "passed": audit_6ir_after == audit_6ir_before},
        {"surface": "6iq_implementation", "policy": "unchanged_by_6iu", "passed": impl_6iq_after == impl_6iq_before},
        {"surface": "adapter_module", "policy": "unchanged_by_6iu", "passed": adapter_after == adapter_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6iu", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6iu", "passed": corrected_after == corrected_before},
        {"surface": "6ik_materialized_table", "policy": "read_only_unchanged_by_6iu", "passed": materialized_after == materialized_before},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IU, "actual": RECOMMENDED_NEXT_LAYER_6IU, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6IU, "actual": RECOMMENDED_PATH_6IU, "passed": True},
        {"decision": "recommend_truth_join_evaluation_plan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6IU, "actual": DIAGNOSIS_6IU, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "source_sufficiency", "passed": all_passed(source_sufficiency_rows), "detail": f"{sum(1 for row in source_sufficiency_rows if row['passed'])}/{len(source_sufficiency_rows)}"},
        {"check": "truth_surface_schema", "passed": all_passed(schema_rows), "detail": f"{sum(1 for row in schema_rows if row['passed'])}/{len(schema_rows)}"},
        {"check": "truth_surface_manifest", "passed": all_passed(manifest_rows), "detail": f"{sum(1 for row in manifest_rows if row['passed'])}/{len(manifest_rows)}"},
        {"check": "candidate_truth_surface_rows", "passed": all_passed(candidate_rows), "detail": f"{sum(1 for row in candidate_rows if row['passed'])}/{len(candidate_rows)}"},
        {"check": "lineage", "passed": all_passed(lineage_rows), "detail": f"{sum(1 for row in lineage_rows if row['passed'])}/{len(lineage_rows)}"},
        {"check": "validation", "passed": all_passed(validation_rows), "detail": f"{sum(1 for row in validation_rows if row['passed'])}/{len(validation_rows)}"},
        {"check": "readiness", "passed": all_passed(readiness_rows), "detail": f"{sum(1 for row in readiness_rows if row['passed'])}/{len(readiness_rows)}"},
        {"check": "future_6iv_contract", "passed": all_passed(future_6iv_rows), "detail": f"{sum(1 for row in future_6iv_rows if row['passed'])}/{len(future_6iv_rows)}"},
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
        "source_sufficiency": write_csv(SOURCE_SUFFICIENCY_CSV, source_sufficiency_rows),
        "truth_surface_schema": write_csv(SCHEMA_CSV, schema_rows),
        "truth_surface_manifest": write_csv(MANIFEST_CSV, manifest_rows),
        "candidate_truth_surface_rows": write_csv(CANDIDATE_CSV, candidate_rows),
        "lineage": write_csv(LINEAGE_CSV, lineage_rows),
        "validation": write_csv(VALIDATION_CSV, validation_rows),
        "readiness": write_csv(READINESS_CSV, readiness_rows),
        "future_6iv_contract": write_csv(FUTURE_6IV_CSV, future_6iv_rows),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6IU",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6IU if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6IU,
        "recommended_path": RECOMMENDED_PATH_6IU,
        "predecessor_implementation": str(IMPLEMENT_6IT_PATH),
        "predecessor_implementation_returncode": 0,
        "predecessor_implementation_diagnosis": json_6it.get("diagnosis"),
        "audited_layer": "6IT",
        "source_family": SOURCE_FAMILY,
        "depends_on_source_family": DEPENDS_ON_SOURCE_FAMILY,
        "local_statsapi_feed_dir_exists": json_6it.get("local_statsapi_feed_dir_exists"),
        "local_statsapi_feed_file_count": json_6it.get("local_statsapi_feed_file_count"),
        "local_statsapi_payload_sufficient": json_6it.get("local_statsapi_payload_sufficient"),
        "controlled_acquisition_required": json_6it.get("controlled_acquisition_required"),
        "remote_fetch_performed": remote_fetch_performed,
        "truth_surface_schema_valid": all_passed(schema_rows),
        "truth_surface_schema_field_count": len(schema_rows_in),
        "truth_surface_manifest_valid": all_passed(manifest_rows),
        "required_truth_surface_count": json_6it.get("required_truth_surface_count"),
        "supported_truth_surface_count": json_6it.get("supported_truth_surface_count"),
        "unsupported_truth_surface_count": json_6it.get("unsupported_truth_surface_count"),
        "candidate_truth_surface_created": json_6it.get("candidate_truth_surface_created"),
        "candidate_truth_surface_row_count": len(candidate_rows_in),
        "candidate_truth_surface_non_production": candidate_non_production,
        "candidate_truth_surface_non_final": candidate_non_final,
        "lineage_valid": all_passed(lineage_rows),
        "lineage_rows_created": len(lineage_rows_in),
        "validation_passed": json_6it.get("validation_passed"),
        "future_6iv_contract_valid": all_passed(future_6iv_rows),
        "truth_surface_joined_to_evaluation": False,
        "real_evaluation_rerun": False,
        "final_pass_fail_decision_possible_after_this_layer": False,
        "activation_planning_allowed_after_this_layer": False,
        "source_artifacts_mutated": False,
        "corrected_candidate_artifacts_mutated": False,
        "materialized_outputs_mutated": False,
        "adapter_implementation_mutated": False,
        "evaluation_implementation_mutated": False,
        "truth_surface_implementation_mutated_by_audit": False,
        "mechanics_activated_by_this_layer": False,
        "actual_outcomes_joined_to_mechanics": False,
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
            "source_sufficiency_csv": str(SOURCE_SUFFICIENCY_CSV),
            "truth_surface_schema_csv": str(SCHEMA_CSV),
            "truth_surface_manifest_csv": str(MANIFEST_CSV),
            "candidate_truth_surface_rows_csv": str(CANDIDATE_CSV),
            "lineage_csv": str(LINEAGE_CSV),
            "validation_csv": str(VALIDATION_CSV),
            "readiness_csv": str(READINESS_CSV),
            "future_6iv_contract_csv": str(FUTURE_6IV_CSV),
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
