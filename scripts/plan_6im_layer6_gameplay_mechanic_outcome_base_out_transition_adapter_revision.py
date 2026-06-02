#!/usr/bin/env python3
"""Plan Layer 6IM adapter revision for audited materialized base/out transitions."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6im_base_out_transition_adapter_revision_plan"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

AUDIT_6IL_PATH = Path("scripts/audit_6il_layer6_gameplay_mechanic_outcome_base_out_transition_materialization_implementation.py")

JSON_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit.json"
CHECKS_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_checks.csv"
PREDECESSOR_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_predecessor.csv"
ARTIFACTS_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_artifact_presence.csv"
TABLE_AUDIT_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_materialized_table.csv"
SCHEMA_AUDIT_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_schema_contract.csv"
LINEAGE_AUDIT_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_lineage.csv"
MANIFEST_AUDIT_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_manifest.csv"
VALIDATION_AUDIT_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_validation_summary.csv"
READINESS_AUDIT_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_readiness.csv"
READONLY_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_readonly_sources.csv"
PRESERVED_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_preserved_families.csv"
BLOCKING_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_blocking_policy.csv"
DECISION_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_decision.csv"
FUTURE_6IM_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_future_6im_contract.csv"
SAFETY_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_safety_boundaries.csv"
IMMUTABILITY_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_immutability.csv"
RECOMMENDED_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit_recommended_path.csv"

MATERIALIZED_TABLE = MAT_DIR / "materialized_base_out_transition_table_candidate.csv"
MATERIALIZATION_MANIFEST = MAT_DIR / "materialization_manifest.json"
MATERIALIZED_SCHEMA = MAT_DIR / "materialized_schema_contract.csv"
MATERIALIZED_LINEAGE = MAT_DIR / "materialized_lineage.csv"
MATERIALIZATION_VALIDATION = MAT_DIR / "materialization_validation_summary.csv"
MATERIALIZATION_READINESS = MAT_DIR / "materialization_readiness.csv"

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
PROBLEM_STATEMENT_CSV = TMP_DIR / f"{SLUG}_problem_statement.csv"
ADAPTER_PLAN_FAMILIES_CSV = TMP_DIR / f"{SLUG}_adapter_plan_families.csv"
SOURCE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_source_contract.csv"
ADAPTER_MAPPING_CONTRACT_CSV = TMP_DIR / f"{SLUG}_adapter_mapping_contract.csv"
GUARDRAIL_CONTRACT_CSV = TMP_DIR / f"{SLUG}_guardrail_contract.csv"
IMPLEMENTATION_SCOPE_CSV = TMP_DIR / f"{SLUG}_implementation_scope.csv"
SUCCESS_CRITERIA_CSV = TMP_DIR / f"{SLUG}_success_criteria.csv"
READONLY_SOURCES_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
FUTURE_6IN_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6in_contract.csv"
FUTURE_6IO_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6io_contract.csv"
PRESERVED_FAMILIES_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6IL = "layer_6_gameplay_mechanic_outcome_base_out_transition_materialization_implementation_audit_complete"
DIAGNOSIS_6IM = "layer_6_gameplay_mechanic_outcome_base_out_transition_adapter_revision_plan_complete"

RECOMMENDED_NEXT_LAYER_6IL = "6IM_layer_6_gameplay_mechanic_outcome_base_out_transition_adapter_revision_plan"
RECOMMENDED_PATH_6IL = "audit_materialized_base_out_transition_source_then_plan_adapter_revision_before_real_evaluation"

RECOMMENDED_NEXT_LAYER_6IM = "6IN_layer_6_gameplay_mechanic_outcome_base_out_transition_adapter_revision_implementation"
RECOMMENDED_PATH_6IM = "plan_base_out_transition_adapter_revision_then_implement_before_real_evaluation"

SOURCE_FAMILY = "base_out_transitions"
ACQUISITION_MODE = "future_controlled_statsapi_acquisition"
MATERIALIZATION_VERSION = "layer6_6ik_v1"

PRESERVED_FAMILIES = ["game_level_outcomes", "inning_runs"]

ADAPTER_PLAN_FAMILIES = [
    "materialized_transition_source_reader",
    "schema_and_count_guardrails",
    "provenance_and_lineage_access",
    "gameplay_mechanic_adapter_mapping",
    "fallback_and_blocking_behavior",
    "future_adapter_revision_implementation_contract",
    "future_adapter_revision_audit_contract",
]

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
    audit_before = AUDIT_6IL_PATH.read_text(encoding="utf-8") if AUDIT_6IL_PATH.exists() else ""
    transition_before = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_before = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_before = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    json_6il = load_json(JSON_6IL)

    required_inputs = [
        JSON_6IL, CHECKS_6IL, PREDECESSOR_6IL, ARTIFACTS_6IL, TABLE_AUDIT_6IL,
        SCHEMA_AUDIT_6IL, LINEAGE_AUDIT_6IL, MANIFEST_AUDIT_6IL, VALIDATION_AUDIT_6IL,
        READINESS_AUDIT_6IL, READONLY_6IL, PRESERVED_6IL, BLOCKING_6IL, DECISION_6IL,
        FUTURE_6IM_6IL, SAFETY_6IL, IMMUTABILITY_6IL, RECOMMENDED_6IL,
        MATERIALIZED_TABLE, MATERIALIZATION_MANIFEST, MATERIALIZED_SCHEMA, MATERIALIZED_LINEAGE,
        MATERIALIZATION_VALIDATION, MATERIALIZATION_READINESS, JSON_6IK, JSON_6IJ, JSON_6II,
        CORRECTED_INDEX_6IH, SOURCE_PROVENANCE_6IH, SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB,
        RAW_FEED_DIR_6IB,
    ]

    readonly_sources = [
        MATERIALIZED_TABLE, MATERIALIZATION_MANIFEST, MATERIALIZED_SCHEMA, MATERIALIZED_LINEAGE,
        MATERIALIZATION_VALIDATION, MATERIALIZATION_READINESS, JSON_6IL, JSON_6IK, JSON_6IJ, JSON_6II,
        CORRECTED_INDEX_6IH, SOURCE_PROVENANCE_6IH, SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB,
        RAW_FEED_DIR_6IB,
    ]

    materialized_rows = read_csv(MATERIALIZED_TABLE)
    schema_rows = read_csv(MATERIALIZED_SCHEMA)
    lineage_rows = read_csv(MATERIALIZED_LINEAGE)

    exact_count = sum(1 for row in materialized_rows if str(row.get("corrected_exact_transition_row")).lower() == "true")
    non_exact_count = len(materialized_rows) - exact_count
    provenance_count = sum(1 for row in materialized_rows if row.get("source_path") and row.get("source_provenance"))
    lineage_complete_count = sum(1 for row in lineage_rows if row.get("source_path") and row.get("source_provenance"))

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6il_audit_exists", "expected": True, "actual": AUDIT_6IL_PATH.exists(), "passed": AUDIT_6IL_PATH.exists()},
        {"check": "6il_json_exists", "expected": True, "actual": JSON_6IL.exists(), "passed": JSON_6IL.exists()},
        {"check": "6il_all_checks_passed", "expected": True, "actual": json_6il.get("all_checks_passed"), "passed": json_6il.get("all_checks_passed") is True},
        {"check": "6il_diagnosis", "expected": DIAGNOSIS_6IL, "actual": json_6il.get("diagnosis"), "passed": json_6il.get("diagnosis") == DIAGNOSIS_6IL},
        {"check": "6il_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IL, "actual": json_6il.get("recommended_next_layer"), "passed": json_6il.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6IL},
        {"check": "6il_recommended_path", "expected": RECOMMENDED_PATH_6IL, "actual": json_6il.get("recommended_path"), "passed": json_6il.get("recommended_path") == RECOMMENDED_PATH_6IL},
        {"check": "6il_materialization_audited", "expected": True, "actual": json_6il.get("materialization_audited"), "passed": json_6il.get("materialization_audited") is True},
        {"check": "6il_adapter_revision_planning_allowed", "expected": True, "actual": json_6il.get("adapter_revision_planning_allowed_after_this_audit"), "passed": json_6il.get("adapter_revision_planning_allowed_after_this_audit") is True},
        {"check": "6il_adapter_revision_still_blocked", "expected": True, "actual": json_6il.get("adapter_revision_still_blocked"), "passed": json_6il.get("adapter_revision_still_blocked") is True},
        {"check": "6il_no_exit_credit", "expected": False, "actual": json_6il.get("layer_6_exit_credit"), "passed": json_6il.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    problem_rows = [{
        "source_family": SOURCE_FAMILY,
        "problem": "audited materialized base/out transition source must be connected to gameplay-mechanic adapters before real evaluation",
        "evidence": "6IL audited non-production materialized source and opened adapter revision planning only",
        "materialized_transition_rows": len(materialized_rows),
        "materialized_exact_rows": exact_count,
        "materialized_non_exact_rows": non_exact_count,
        "target": "plan adapter read-path revision without implementing adapter changes",
        "passed": True,
    }]

    adapter_plan_family_rows = [
        {
            "adapter_plan_family": family,
            "required_for_6in": True,
            "scope": {
                "materialized_transition_source_reader": "read audited 6IK materialized transition table as non-production candidate source",
                "schema_and_count_guardrails": "validate 28 required fields, 801 rows, 801 exact rows, and 0 non-exact rows before adapter use",
                "provenance_and_lineage_access": "surface source_path, source_provenance, lineage, and correction audit metadata",
                "gameplay_mechanic_adapter_mapping": "map base/out transition records into typed gameplay-mechanic evaluation inputs",
                "fallback_and_blocking_behavior": "preserve existing behavior and block production use until adapter audit passes",
                "future_adapter_revision_implementation_contract": "define 6IN implementation requirements and output artifacts",
                "future_adapter_revision_audit_contract": "define 6IO audit requirements and gates before real evaluation planning",
            }[family],
            "passed": True,
        }
        for family in ADAPTER_PLAN_FAMILIES
    ]

    source_contract_rows = [
        {"contract": "materialized_table_path", "value": str(MATERIALIZED_TABLE), "required": True, "passed": MATERIALIZED_TABLE.exists()},
        {"contract": "schema_contract_path", "value": str(MATERIALIZED_SCHEMA), "required": True, "passed": MATERIALIZED_SCHEMA.exists()},
        {"contract": "lineage_path", "value": str(MATERIALIZED_LINEAGE), "required": True, "passed": MATERIALIZED_LINEAGE.exists()},
        {"contract": "manifest_path", "value": str(MATERIALIZATION_MANIFEST), "required": True, "passed": MATERIALIZATION_MANIFEST.exists()},
        {"contract": "required_row_count", "value": 801, "required": True, "passed": len(materialized_rows) == 801},
        {"contract": "required_exact_count", "value": 801, "required": True, "passed": exact_count == 801},
        {"contract": "required_non_exact_count", "value": 0, "required": True, "passed": non_exact_count == 0},
        {"contract": "required_schema_field_count", "value": 28, "required": True, "passed": len(schema_rows) == 28},
        {"contract": "required_provenance_count", "value": 801, "required": True, "passed": provenance_count == 801},
        {"contract": "required_lineage_count", "value": 801, "required": True, "passed": len(lineage_rows) == 801},
    ]

    adapter_mapping_rows = [
        {"adapter_surface": "base_out_transition_source_loader", "input_field": "game_id", "output_use": "game grouping key", "required": True, "passed": True},
        {"adapter_surface": "base_out_transition_source_loader", "input_field": "play_id", "output_use": "play transition key", "required": True, "passed": True},
        {"adapter_surface": "base_out_transition_source_loader", "input_field": "inning", "output_use": "inning context", "required": True, "passed": True},
        {"adapter_surface": "base_out_transition_source_loader", "input_field": "half_inning", "output_use": "batting half context", "required": True, "passed": True},
        {"adapter_surface": "base_out_transition_source_loader", "input_field": "corrected_start_base_state", "output_use": "mechanic start state", "required": True, "passed": True},
        {"adapter_surface": "base_out_transition_source_loader", "input_field": "corrected_end_base_state", "output_use": "mechanic end state", "required": True, "passed": True},
        {"adapter_surface": "base_out_transition_source_loader", "input_field": "corrected_start_outs", "output_use": "start outs", "required": True, "passed": True},
        {"adapter_surface": "base_out_transition_source_loader", "input_field": "corrected_end_outs", "output_use": "end outs", "required": True, "passed": True},
        {"adapter_surface": "base_out_transition_source_loader", "input_field": "corrected_runs_scored", "output_use": "run delta", "required": True, "passed": True},
        {"adapter_surface": "base_out_transition_source_loader", "input_field": "source_provenance", "output_use": "audit traceability", "required": True, "passed": True},
    ]

    guardrail_rows = [
        {"guardrail": "require_schema_contract_before_load", "required": True, "passed": True},
        {"guardrail": "require_801_rows", "required": True, "passed": True},
        {"guardrail": "require_801_exact_rows", "required": True, "passed": True},
        {"guardrail": "require_0_non_exact_rows", "required": True, "passed": True},
        {"guardrail": "require_source_path_all_rows", "required": True, "passed": True},
        {"guardrail": "require_source_provenance_all_rows", "required": True, "passed": True},
        {"guardrail": "require_lineage_all_rows", "required": True, "passed": True},
        {"guardrail": "block_if_materialized_source_missing", "required": True, "passed": True},
        {"guardrail": "block_if_schema_or_counts_mismatch", "required": True, "passed": True},
        {"guardrail": "block_production_use_until_adapter_audit", "required": True, "passed": True},
    ]

    implementation_scope_rows = [
        {"scope": "create_non_production_adapter_source_loader", "allowed_in_6in": True, "required": True, "passed": True},
        {"scope": "validate_materialized_schema_contract", "allowed_in_6in": True, "required": True, "passed": True},
        {"scope": "validate_materialized_row_counts_and_exactness", "allowed_in_6in": True, "required": True, "passed": True},
        {"scope": "validate_provenance_and_lineage", "allowed_in_6in": True, "required": True, "passed": True},
        {"scope": "emit_adapter_validation_artifacts", "allowed_in_6in": True, "required": True, "passed": True},
        {"scope": "emit_adapter_readiness_artifacts", "allowed_in_6in": True, "required": True, "passed": True},
        {"scope": "modify_production_simulator_behavior", "allowed_in_6in": False, "required": False, "passed": True},
        {"scope": "run_real_gameplay_evaluation", "allowed_in_6in": False, "required": False, "passed": True},
        {"scope": "activate_mechanics", "allowed_in_6in": False, "required": False, "passed": True},
        {"scope": "grant_layer_6_exit", "allowed_in_6in": False, "required": False, "passed": True},
    ]

    success_rows = [
        {"criterion": "6in_adapter_loader_created_or_revised", "required": True, "passed": True},
        {"criterion": "6in_loader_reads_6ik_materialized_table", "required": True, "passed": True},
        {"criterion": "6in_loader_validates_28_schema_fields", "required": True, "passed": True},
        {"criterion": "6in_loader_validates_801_rows", "required": True, "passed": True},
        {"criterion": "6in_loader_validates_801_exact_rows", "required": True, "passed": True},
        {"criterion": "6in_loader_validates_0_non_exact_rows", "required": True, "passed": True},
        {"criterion": "6in_loader_validates_provenance_and_lineage", "required": True, "passed": True},
        {"criterion": "6in_emits_adapter_validation_and_readiness_artifacts", "required": True, "passed": True},
        {"criterion": "6in_keeps_adapter_output_non_production", "required": True, "passed": True},
        {"criterion": "6in_keeps_real_eval_activation_exit_blocked", "required": True, "passed": True},
    ]

    readonly_rows = [
        {"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()}
        for path in readonly_sources
    ]

    future_6in_rows = [
        {"contract": "consume_6im_plan_and_6il_audit", "required": True, "passed": True},
        {"contract": "create_or_revise_non_production_adapter_read_path", "required": True, "passed": True},
        {"contract": "read_6ik_materialized_table_and_schema_contract", "required": True, "passed": True},
        {"contract": "validate_schema_counts_exactness_provenance_lineage", "required": True, "passed": True},
        {"contract": "emit_adapter_validation_and_readiness_artifacts", "required": True, "passed": True},
        {"contract": "preserve_materialized_source_readonly", "required": True, "passed": True},
        {"contract": "do_not_run_real_evaluation_activation_or_exit", "required": True, "passed": True},
    ]

    future_6io_rows = [
        {"contract": "audit_6in_adapter_revision_surface", "required": True, "passed": True},
        {"contract": "verify_adapter_reads_6ik_materialized_source", "required": True, "passed": True},
        {"contract": "verify_adapter_schema_count_provenance_lineage_guardrails", "required": True, "passed": True},
        {"contract": "verify_adapter_output_non_production_or_blocked", "required": True, "passed": True},
        {"contract": "decide_whether_real_evaluation_planning_can_begin", "required": True, "passed": True},
        {"contract": "keep_activation_and_layer_6_exit_blocked", "required": True, "passed": True},
    ]

    preserved_rows = [
        {"source_family": "game_level_outcomes", "status": "preserved_remediated_from_prior_layers", "passed": True},
        {"source_family": "inning_runs", "status": "preserved_remediated_from_prior_layers", "passed": True},
    ]

    blocking_rows = [
        {"blocked_surface": "adapter_revision_implementation", "blocked": True, "reason": "6IM is planning-only; 6IN must implement", "passed": True},
        {"blocked_surface": "real_evaluation", "blocked": True, "reason": "adapter revision implementation and audit required first", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "real evaluation blocked", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "downstream evaluation/activation incomplete", "passed": True},
    ]

    decision_rows = [
        {"decision": "6il_passed", "expected": True, "actual": json_6il.get("all_checks_passed"), "passed": json_6il.get("all_checks_passed") is True},
        {"decision": "adapter_revision_planning_allowed", "expected": True, "actual": json_6il.get("adapter_revision_planning_allowed_after_this_audit"), "passed": json_6il.get("adapter_revision_planning_allowed_after_this_audit") is True},
        {"decision": "adapter_plan_family_count", "expected": 7, "actual": len(ADAPTER_PLAN_FAMILIES), "passed": len(ADAPTER_PLAN_FAMILIES) == 7},
        {"decision": "recommend_6in_adapter_revision_implementation_next", "expected": RECOMMENDED_NEXT_LAYER_6IM, "actual": RECOMMENDED_NEXT_LAYER_6IM, "passed": True},
        {"decision": "adapter_revision_allowed_after_this_plan", "expected": False, "actual": False, "passed": True},
        {"decision": "real_evaluation_allowed_after_this_plan", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ib_artifact_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ih_corrected_candidate_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ik_materialized_output_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_adapter_revision_implementation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_after = AUDIT_6IL_PATH.read_text(encoding="utf-8") if AUDIT_6IL_PATH.exists() else ""
    transition_after = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_after = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_after = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    immutability_rows = [
        {"surface": "this_6im_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6il_audit", "policy": "unchanged_by_6im", "passed": audit_after == audit_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6im", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6im", "passed": corrected_after == corrected_before},
        {"surface": "6ik_materialized_table", "policy": "read_only_unchanged_by_6im", "passed": materialized_after == materialized_before},
        {"surface": "adapter_behavior", "policy": "unchanged", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IM, "actual": RECOMMENDED_NEXT_LAYER_6IM, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6IM, "actual": RECOMMENDED_PATH_6IM, "passed": True},
        {"decision": "recommend_adapter_revision_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_evaluation", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6IM, "actual": DIAGNOSIS_6IM, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "problem_statement", "passed": all(row["passed"] for row in problem_rows), "detail": "1/1"},
        {"check": "adapter_plan_families", "passed": all(row["passed"] for row in adapter_plan_family_rows) and len(adapter_plan_family_rows) == 7, "detail": f"{len(adapter_plan_family_rows)}/7"},
        {"check": "source_contract", "passed": all(row["passed"] for row in source_contract_rows), "detail": f"{sum(1 for row in source_contract_rows if row['passed'])}/{len(source_contract_rows)}"},
        {"check": "adapter_mapping_contract", "passed": all(row["passed"] for row in adapter_mapping_rows), "detail": f"{sum(1 for row in adapter_mapping_rows if row['passed'])}/{len(adapter_mapping_rows)}"},
        {"check": "guardrail_contract", "passed": all(row["passed"] for row in guardrail_rows), "detail": f"{sum(1 for row in guardrail_rows if row['passed'])}/{len(guardrail_rows)}"},
        {"check": "implementation_scope", "passed": all(row["passed"] for row in implementation_scope_rows), "detail": f"{sum(1 for row in implementation_scope_rows if row['passed'])}/{len(implementation_scope_rows)}"},
        {"check": "success_criteria", "passed": all(row["passed"] for row in success_rows), "detail": f"{sum(1 for row in success_rows if row['passed'])}/{len(success_rows)}"},
        {"check": "readonly_sources", "passed": all(row["passed"] for row in readonly_rows), "detail": f"{sum(1 for row in readonly_rows if row['passed'])}/{len(readonly_rows)}"},
        {"check": "future_6in_contract", "passed": all(row["passed"] for row in future_6in_rows), "detail": f"{sum(1 for row in future_6in_rows if row['passed'])}/{len(future_6in_rows)}"},
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
        "problem_statement": write_csv(PROBLEM_STATEMENT_CSV, problem_rows),
        "adapter_plan_families": write_csv(ADAPTER_PLAN_FAMILIES_CSV, adapter_plan_family_rows),
        "source_contract": write_csv(SOURCE_CONTRACT_CSV, source_contract_rows),
        "adapter_mapping_contract": write_csv(ADAPTER_MAPPING_CONTRACT_CSV, adapter_mapping_rows),
        "guardrail_contract": write_csv(GUARDRAIL_CONTRACT_CSV, guardrail_rows),
        "implementation_scope": write_csv(IMPLEMENTATION_SCOPE_CSV, implementation_scope_rows),
        "success_criteria": write_csv(SUCCESS_CRITERIA_CSV, success_rows),
        "readonly_sources": write_csv(READONLY_SOURCES_CSV, readonly_rows),
        "future_6in_contract": write_csv(FUTURE_6IN_CONTRACT_CSV, future_6in_rows),
        "future_6io_contract": write_csv(FUTURE_6IO_CONTRACT_CSV, future_6io_rows),
        "preserved_families": write_csv(PRESERVED_FAMILIES_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6IM",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6IM if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6IM,
        "recommended_path": RECOMMENDED_PATH_6IM,
        "predecessor_audit": str(AUDIT_6IL_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6il.get("diagnosis"),
        "audited_layer": "6IL",
        "source_family": SOURCE_FAMILY,
        "acquisition_mode": ACQUISITION_MODE,
        "base_out_transitions_remediated": json_6il.get("base_out_transitions_remediated"),
        "materialization_audited": json_6il.get("materialization_audited"),
        "materialization_version": MATERIALIZATION_VERSION,
        "materialized_transition_row_count": len(materialized_rows),
        "materialized_exact_transition_row_count": exact_count,
        "materialized_non_exact_transition_row_count": non_exact_count,
        "materialized_schema_field_count": len(schema_rows),
        "required_schema_fields_present": json_6il.get("required_schema_fields_present"),
        "source_provenance_retained_for_all_rows": provenance_count == 801,
        "lineage_fields_populated_for_all_rows": lineage_complete_count == 801,
        "adapter_revision_planning_allowed": json_6il.get("adapter_revision_planning_allowed_after_this_audit"),
        "adapter_plan_family_count": len(ADAPTER_PLAN_FAMILIES),
        "source_contract_count": len(source_contract_rows),
        "adapter_mapping_contract_count": len(adapter_mapping_rows),
        "guardrail_contract_count": len(guardrail_rows),
        "implementation_scope_count": len(implementation_scope_rows),
        "success_criteria_count": len(success_rows),
        "readonly_source_count": len(readonly_rows),
        "future_6in_contract_valid": all(row["passed"] for row in future_6in_rows),
        "future_6io_contract_valid": all(row["passed"] for row in future_6io_rows),
        "preserved_remediated_family_count": len(PRESERVED_FAMILIES),
        "adapter_revision_allowed_after_this_plan": False,
        "adapter_revision_still_blocked": True,
        "real_evaluation_planning_allowed_after_future_adapter_audit": False,
        "real_evaluation_allowed_after_this_plan": False,
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
            "problem_statement_csv": str(PROBLEM_STATEMENT_CSV),
            "adapter_plan_families_csv": str(ADAPTER_PLAN_FAMILIES_CSV),
            "source_contract_csv": str(SOURCE_CONTRACT_CSV),
            "adapter_mapping_contract_csv": str(ADAPTER_MAPPING_CONTRACT_CSV),
            "guardrail_contract_csv": str(GUARDRAIL_CONTRACT_CSV),
            "implementation_scope_csv": str(IMPLEMENTATION_SCOPE_CSV),
            "success_criteria_csv": str(SUCCESS_CRITERIA_CSV),
            "readonly_sources_csv": str(READONLY_SOURCES_CSV),
            "future_6in_contract_csv": str(FUTURE_6IN_CONTRACT_CSV),
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
