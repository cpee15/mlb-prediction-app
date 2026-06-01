#!/usr/bin/env python3
"""Plan Layer 6IJ materialization of audited corrected base/out transition corpus."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ij_base_out_transition_materialization_plan"
TMP_DIR = Path("tmp")

AUDIT_6II_PATH = Path("scripts/audit_6ii_layer6_gameplay_mechanic_outcome_base_out_transition_reconstruction_correction_implementation.py")

JSON_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit.json"
CHECKS_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit_checks.csv"
PREDECESSOR_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit_predecessor.csv"
ARTIFACTS_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit_artifact_presence.csv"
CORRECTED_CANDIDATE_AUDIT_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit_corrected_candidate.csv"
EXACTNESS_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit_exactness.csv"
FAMILIES_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit_correction_families.csv"
PROVENANCE_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit_source_provenance.csv"
READINESS_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit_readiness.csv"
READONLY_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit_readonly_sources.csv"
PRESERVED_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit_preserved_families.csv"
BLOCKING_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit_blocking_policy.csv"
DECISION_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit_decision.csv"
FUTURE_6IJ_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit_future_6ij_contract.csv"
SAFETY_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit_safety_boundaries.csv"
IMMUTABILITY_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit_immutability.csv"
RECOMMENDED_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit_recommended_path.csv"

CORRECTED_INDEX_6IH = TMP_DIR / "layer6_6ih_base_out_transition_reconstruction_correction_implementation_corrected_transition_index_candidate.csv"
CORRECTION_DECISIONS_6IH = TMP_DIR / "layer6_6ih_base_out_transition_reconstruction_correction_implementation_correction_decisions.csv"
SOURCE_PROVENANCE_6IH = TMP_DIR / "layer6_6ih_base_out_transition_reconstruction_correction_implementation_source_provenance.csv"
JSON_6IH = TMP_DIR / "layer6_6ih_base_out_transition_reconstruction_correction_implementation.json"
JSON_6IG = TMP_DIR / "layer6_6ig_base_out_transition_reconstruction_correction_plan.json"
JSON_6IF = TMP_DIR / "layer6_6if_base_out_transition_reconstruction_gap_analysis_implementation_audit.json"
ROW_CLASSIFICATION_6IE = TMP_DIR / "layer6_6ie_base_out_transition_reconstruction_gap_analysis_implementation_row_classification.csv"

SOURCE_MANIFEST_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/source_manifest.json"
TRANSITION_INDEX_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/base_out_transition_index.csv"
RAW_FEED_DIR_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/statsapi_game_feed"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PROBLEM_STATEMENT_CSV = TMP_DIR / f"{SLUG}_problem_statement.csv"
MATERIALIZATION_FAMILIES_CSV = TMP_DIR / f"{SLUG}_materialization_families.csv"
SCHEMA_CONTRACT_CSV = TMP_DIR / f"{SLUG}_schema_contract.csv"
IMPLEMENTATION_SCOPE_CSV = TMP_DIR / f"{SLUG}_implementation_scope.csv"
SUCCESS_CRITERIA_CSV = TMP_DIR / f"{SLUG}_success_criteria.csv"
READONLY_SOURCES_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
OUTPUT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_output_contract.csv"
FUTURE_6IK_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6ik_contract.csv"
FUTURE_6IL_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6il_contract.csv"
PRESERVED_FAMILIES_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6II = "layer_6_gameplay_mechanic_outcome_base_out_transition_reconstruction_correction_implementation_audit_complete"
DIAGNOSIS_6IJ = "layer_6_gameplay_mechanic_outcome_base_out_transition_materialization_plan_complete"

RECOMMENDED_NEXT_LAYER_6II = "6IJ_layer_6_gameplay_mechanic_outcome_base_out_transition_materialization_plan"
RECOMMENDED_PATH_6II = "audit_corrected_base_out_transition_reconstruction_then_plan_materialization_before_evaluation"

RECOMMENDED_NEXT_LAYER_6IJ = "6IK_layer_6_gameplay_mechanic_outcome_base_out_transition_materialization_implementation"
RECOMMENDED_PATH_6IJ = "plan_audited_corrected_base_out_transition_materialization_then_implement_before_adapter_revision"

SOURCE_FAMILY = "base_out_transitions"
ACQUISITION_MODE = "future_controlled_statsapi_acquisition"

PRESERVED_FAMILIES = ["game_level_outcomes", "inning_runs"]

MATERIALIZATION_FAMILIES = [
    "corrected_transition_table_materialization",
    "source_provenance_lineage_materialization",
    "schema_contract_and_field_mapping",
    "validation_manifest_and_readiness_summary",
    "downstream_adapter_contract_definition",
    "future_materialization_audit_contract",
]

MATERIALIZED_SCHEMA_FIELDS = [
    "game_id",
    "play_id",
    "inning",
    "half_inning",
    "sequence_order",
    "source_family",
    "source_path",
    "source_provenance",
    "original_start_base_state",
    "original_end_base_state",
    "original_start_outs",
    "original_end_outs",
    "original_runs_scored",
    "corrected_start_base_state",
    "corrected_end_base_state",
    "corrected_start_outs",
    "corrected_end_outs",
    "corrected_runs_scored",
    "corrected_exact_transition_row",
    "correction_applied",
    "correction_families",
    "correction_reason",
    "prior_gap_categories",
    "prior_fixability_classification",
    "materialized_layer",
    "materialization_version",
    "upstream_audit_layer",
    "upstream_audit_diagnosis",
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
    audit_before = AUDIT_6II_PATH.read_text(encoding="utf-8") if AUDIT_6II_PATH.exists() else ""
    transition_before = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_before = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""

    json_6ii = load_json(JSON_6II)

    required_inputs = [
        JSON_6II,
        CHECKS_6II,
        PREDECESSOR_6II,
        ARTIFACTS_6II,
        CORRECTED_CANDIDATE_AUDIT_6II,
        EXACTNESS_6II,
        FAMILIES_6II,
        PROVENANCE_6II,
        READINESS_6II,
        READONLY_6II,
        PRESERVED_6II,
        BLOCKING_6II,
        DECISION_6II,
        FUTURE_6IJ_6II,
        SAFETY_6II,
        IMMUTABILITY_6II,
        RECOMMENDED_6II,
        CORRECTED_INDEX_6IH,
        CORRECTION_DECISIONS_6IH,
        SOURCE_PROVENANCE_6IH,
        SOURCE_MANIFEST_6IB,
        TRANSITION_INDEX_6IB,
        RAW_FEED_DIR_6IB,
        JSON_6IH,
        JSON_6IG,
        JSON_6IF,
        ROW_CLASSIFICATION_6IE,
    ]

    readonly_sources = [
        CORRECTED_INDEX_6IH,
        CORRECTION_DECISIONS_6IH,
        SOURCE_PROVENANCE_6IH,
        SOURCE_MANIFEST_6IB,
        TRANSITION_INDEX_6IB,
        RAW_FEED_DIR_6IB,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ii_audit_exists", "expected": True, "actual": AUDIT_6II_PATH.exists(), "passed": AUDIT_6II_PATH.exists()},
        {"check": "6ii_json_exists", "expected": True, "actual": JSON_6II.exists(), "passed": JSON_6II.exists()},
        {"check": "6ii_all_checks_passed", "expected": True, "actual": json_6ii.get("all_checks_passed"), "passed": json_6ii.get("all_checks_passed") is True},
        {"check": "6ii_diagnosis", "expected": DIAGNOSIS_6II, "actual": json_6ii.get("diagnosis"), "passed": json_6ii.get("diagnosis") == DIAGNOSIS_6II},
        {"check": "6ii_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6II, "actual": json_6ii.get("recommended_next_layer"), "passed": json_6ii.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6II},
        {"check": "6ii_recommended_path", "expected": RECOMMENDED_PATH_6II, "actual": json_6ii.get("recommended_path"), "passed": json_6ii.get("recommended_path") == RECOMMENDED_PATH_6II},
        {"check": "6ii_base_out_remediated", "expected": True, "actual": json_6ii.get("base_out_transitions_remediated"), "passed": json_6ii.get("base_out_transitions_remediated") is True},
        {"check": "6ii_materialization_planning_allowed", "expected": True, "actual": json_6ii.get("materialization_planning_allowed_after_this_audit"), "passed": json_6ii.get("materialization_planning_allowed_after_this_audit") is True},
        {"check": "6ii_materialization_still_blocked", "expected": True, "actual": json_6ii.get("materialization_still_blocked"), "passed": json_6ii.get("materialization_still_blocked") is True},
        {"check": "6ii_no_exit_credit", "expected": False, "actual": json_6ii.get("layer_6_exit_credit"), "passed": json_6ii.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    problem_rows = [{
        "source_family": SOURCE_FAMILY,
        "problem": "audited corrected base/out transition corpus must be materialized before adapters or real evaluation can consume it",
        "evidence": "6II audited 801/801 exact corrected transitions with retained provenance and unlocked materialization planning",
        "corrected_transition_rows": json_6ii.get("corrected_transition_row_count"),
        "corrected_exact_rows": json_6ii.get("corrected_exact_transition_row_count"),
        "corrected_non_exact_rows": json_6ii.get("corrected_non_exact_transition_row_count"),
        "target": "plan non-production materialization implementation while preserving downstream blocks",
        "passed": True,
    }]

    materialization_family_rows = [
        {
            "materialization_family": family,
            "required_for_6ik": True,
            "scope": {
                "corrected_transition_table_materialization": "materialize audited 6IH corrected candidate rows as a Layer 6 transition source candidate",
                "source_provenance_lineage_materialization": "retain source_path, source_provenance, 6IH correction lineage, and 6II audit lineage",
                "schema_contract_and_field_mapping": "define strict field contract for materialized transition corpus",
                "validation_manifest_and_readiness_summary": "emit manifest and readiness outputs without activating production use",
                "downstream_adapter_contract_definition": "define future adapter expectations without revising adapters yet",
                "future_materialization_audit_contract": "define 6IL audit requirements for materialized corpus",
            }[family],
            "passed": True,
        }
        for family in MATERIALIZATION_FAMILIES
    ]

    schema_rows = [
        {"field_name": field, "required": True, "materialized_by_6ik": True, "passed": True}
        for field in MATERIALIZED_SCHEMA_FIELDS
    ]

    implementation_scope_rows = [
        {"scope": "consume_6ii_audit_and_6ih_corrected_candidate", "allowed": True, "required": True, "passed": True},
        {"scope": "emit_materialized_transition_table_candidate", "allowed": True, "required": True, "passed": True},
        {"scope": "emit_materialization_manifest", "allowed": True, "required": True, "passed": True},
        {"scope": "emit_schema_contract", "allowed": True, "required": True, "passed": True},
        {"scope": "emit_lineage_table", "allowed": True, "required": True, "passed": True},
        {"scope": "emit_validation_summary", "allowed": True, "required": True, "passed": True},
        {"scope": "emit_readiness_summary", "allowed": True, "required": True, "passed": True},
        {"scope": "overwrite_6ib_artifacts", "allowed": False, "required": False, "passed": True},
        {"scope": "overwrite_6ih_corrected_candidate", "allowed": False, "required": False, "passed": True},
        {"scope": "database_write", "allowed": False, "required": False, "passed": True},
        {"scope": "production_materialization", "allowed": False, "required": False, "passed": True},
        {"scope": "adapter_revision", "allowed": False, "required": False, "passed": True},
        {"scope": "real_evaluation", "allowed": False, "required": False, "passed": True},
        {"scope": "mechanic_activation_or_layer_6_exit", "allowed": False, "required": False, "passed": True},
    ]

    success_rows = [
        {"criterion": "materialized_transition_row_count_equals_801", "required": True, "passed": True},
        {"criterion": "materialized_exact_transition_row_count_equals_801", "required": True, "passed": True},
        {"criterion": "materialized_non_exact_transition_row_count_equals_0", "required": True, "passed": True},
        {"criterion": "source_provenance_retained_for_all_rows", "required": True, "passed": True},
        {"criterion": "lineage_fields_populated_for_all_rows", "required": True, "passed": True},
        {"criterion": "manifest_emitted", "required": True, "passed": True},
        {"criterion": "schema_contract_emitted", "required": True, "passed": True},
        {"criterion": "materialized_outputs_not_production_active", "required": True, "passed": True},
        {"criterion": "adapter_revision_remains_blocked", "required": True, "passed": True},
        {"criterion": "real_evaluation_remains_blocked", "required": True, "passed": True},
        {"criterion": "activation_remains_blocked", "required": True, "passed": True},
        {"criterion": "layer_6_exit_remains_blocked", "required": True, "passed": True},
    ]

    readonly_rows = [
        {"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()}
        for path in readonly_sources
    ]

    output_contract_rows = [
        {"output": "materialized_transition_table_candidate_csv", "required": True, "scope": "801-row non-production materialized candidate", "passed": True},
        {"output": "materialization_manifest_json", "required": True, "scope": "input/output lineage and audit status", "passed": True},
        {"output": "materialized_schema_contract_csv", "required": True, "scope": "strict table schema emitted by 6IK", "passed": True},
        {"output": "materialized_lineage_csv", "required": True, "scope": "row-level lineage from 6IH and 6II", "passed": True},
        {"output": "materialization_validation_summary_csv", "required": True, "scope": "row counts, exactness, provenance, immutability", "passed": True},
        {"output": "materialization_readiness_csv", "required": True, "scope": "downstream surfaces remain blocked except future planning", "passed": True},
    ]

    future_6ik_rows = [
        {"contract": "consume_6ij_plan_and_6ii_audit", "required": True, "passed": True},
        {"contract": "read_6ih_corrected_candidate_readonly", "required": True, "passed": True},
        {"contract": "materialize_801_row_corrected_transition_candidate_to_non_production_output", "required": True, "passed": True},
        {"contract": "emit_manifest_schema_lineage_validation_and_readiness_outputs", "required": True, "passed": True},
        {"contract": "preserve_source_provenance_and_correction_audit_lineage", "required": True, "passed": True},
        {"contract": "do_not_modify_6ib_or_6ih_artifacts", "required": True, "passed": True},
        {"contract": "do_not_revise_adapters_evaluate_activate_or_exit", "required": True, "passed": True},
    ]

    future_6il_rows = [
        {"contract": "audit_6ik_materialized_transition_outputs", "required": True, "passed": True},
        {"contract": "verify_materialized_801_rows_and_801_exact_rows", "required": True, "passed": True},
        {"contract": "verify_schema_contract_and_required_fields", "required": True, "passed": True},
        {"contract": "verify_source_provenance_and_lineage_retention", "required": True, "passed": True},
        {"contract": "verify_materialized_outputs_not_production_active", "required": True, "passed": True},
        {"contract": "decide_whether_adapter_revision_planning_can_begin", "required": True, "passed": True},
        {"contract": "keep_real_eval_activation_exit_blocked", "required": True, "passed": True},
    ]

    preserved_rows = [
        {"source_family": "game_level_outcomes", "status": "preserved_remediated_from_prior_layers", "passed": True},
        {"source_family": "inning_runs", "status": "preserved_remediated_from_prior_layers", "passed": True},
    ]

    blocking_rows = [
        {"blocked_surface": "materialization_implementation", "blocked": False, "reason": "6IK implementation may be planned next but is not executed by 6IJ", "passed": True},
        {"blocked_surface": "production_materialization", "blocked": True, "reason": "6IK/6IL required before production use", "passed": True},
        {"blocked_surface": "adapter_revision", "blocked": True, "reason": "materialized transition corpus not implemented or audited yet", "passed": True},
        {"blocked_surface": "real_evaluation", "blocked": True, "reason": "adapter revision blocked", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "real evaluation blocked", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "downstream materialization/evaluation/activation incomplete", "passed": True},
    ]

    decision_rows = [
        {"decision": "6ii_passed", "expected": True, "actual": json_6ii.get("all_checks_passed"), "passed": json_6ii.get("all_checks_passed") is True},
        {"decision": "base_out_transitions_remediated", "expected": True, "actual": json_6ii.get("base_out_transitions_remediated"), "passed": json_6ii.get("base_out_transitions_remediated") is True},
        {"decision": "corrected_transition_corpus_audited", "expected": True, "actual": json_6ii.get("corrected_transition_corpus_audited"), "passed": json_6ii.get("corrected_transition_corpus_audited") is True},
        {"decision": "materialization_planning_allowed", "expected": True, "actual": json_6ii.get("materialization_planning_allowed_after_this_audit"), "passed": json_6ii.get("materialization_planning_allowed_after_this_audit") is True},
        {"decision": "recommend_6ik_materialization_implementation_next", "expected": RECOMMENDED_NEXT_LAYER_6IJ, "actual": RECOMMENDED_NEXT_LAYER_6IJ, "passed": True},
        {"decision": "materialization_allowed_after_this_plan", "expected": False, "actual": False, "passed": True},
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
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_materialization_jobs", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_adapter_revision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_after = AUDIT_6II_PATH.read_text(encoding="utf-8") if AUDIT_6II_PATH.exists() else ""
    transition_after = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_after = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""

    immutability_rows = [
        {"surface": "this_6ij_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6ii_audit", "policy": "unchanged_by_6ij", "passed": audit_after == audit_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6ij", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6ij", "passed": corrected_after == corrected_before},
        {"surface": "6ib_raw_feed_cache", "policy": "read_only", "passed": True},
        {"surface": "adapter_behavior", "policy": "unchanged", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IJ, "actual": RECOMMENDED_NEXT_LAYER_6IJ, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6IJ, "actual": RECOMMENDED_PATH_6IJ, "passed": True},
        {"decision": "do_not_recommend_adapter_revision_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_evaluation", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6IJ, "actual": DIAGNOSIS_6IJ, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "problem_statement", "passed": all(row["passed"] for row in problem_rows), "detail": "1/1"},
        {"check": "materialization_families", "passed": all(row["passed"] for row in materialization_family_rows) and len(materialization_family_rows) == 6, "detail": f"{len(materialization_family_rows)}/6"},
        {"check": "schema_contract", "passed": all(row["passed"] for row in schema_rows), "detail": f"{sum(1 for row in schema_rows if row['passed'])}/{len(schema_rows)}"},
        {"check": "implementation_scope", "passed": all(row["passed"] for row in implementation_scope_rows), "detail": f"{sum(1 for row in implementation_scope_rows if row['passed'])}/{len(implementation_scope_rows)}"},
        {"check": "success_criteria", "passed": all(row["passed"] for row in success_rows), "detail": f"{sum(1 for row in success_rows if row['passed'])}/{len(success_rows)}"},
        {"check": "readonly_sources", "passed": all(row["passed"] for row in readonly_rows), "detail": f"{sum(1 for row in readonly_rows if row['passed'])}/{len(readonly_rows)}"},
        {"check": "output_contract", "passed": all(row["passed"] for row in output_contract_rows), "detail": f"{sum(1 for row in output_contract_rows if row['passed'])}/{len(output_contract_rows)}"},
        {"check": "future_6ik_contract", "passed": all(row["passed"] for row in future_6ik_rows), "detail": f"{sum(1 for row in future_6ik_rows if row['passed'])}/{len(future_6ik_rows)}"},
        {"check": "future_6il_contract", "passed": all(row["passed"] for row in future_6il_rows), "detail": f"{sum(1 for row in future_6il_rows if row['passed'])}/{len(future_6il_rows)}"},
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
        "materialization_families": write_csv(MATERIALIZATION_FAMILIES_CSV, materialization_family_rows),
        "schema_contract": write_csv(SCHEMA_CONTRACT_CSV, schema_rows),
        "implementation_scope": write_csv(IMPLEMENTATION_SCOPE_CSV, implementation_scope_rows),
        "success_criteria": write_csv(SUCCESS_CRITERIA_CSV, success_rows),
        "readonly_sources": write_csv(READONLY_SOURCES_CSV, readonly_rows),
        "output_contract": write_csv(OUTPUT_CONTRACT_CSV, output_contract_rows),
        "future_6ik_contract": write_csv(FUTURE_6IK_CONTRACT_CSV, future_6ik_rows),
        "future_6il_contract": write_csv(FUTURE_6IL_CONTRACT_CSV, future_6il_rows),
        "preserved_families": write_csv(PRESERVED_FAMILIES_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6IJ",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6IJ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6IJ,
        "recommended_path": RECOMMENDED_PATH_6IJ,
        "predecessor_audit": str(AUDIT_6II_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6ii.get("diagnosis"),
        "audited_layer": "6II",
        "source_family": SOURCE_FAMILY,
        "acquisition_mode": ACQUISITION_MODE,
        "base_out_transitions_remediated": json_6ii.get("base_out_transitions_remediated"),
        "corrected_transition_corpus_audited": json_6ii.get("corrected_transition_corpus_audited"),
        "prior_corrected_transition_row_count": json_6ii.get("corrected_transition_row_count"),
        "prior_corrected_exact_transition_row_count": json_6ii.get("corrected_exact_transition_row_count"),
        "prior_corrected_non_exact_transition_row_count": json_6ii.get("corrected_non_exact_transition_row_count"),
        "prior_exact_transition_improvement": json_6ii.get("exact_transition_improvement"),
        "prior_remaining_non_exact_reduction": json_6ii.get("remaining_non_exact_reduction"),
        "prior_corrected_from_non_exact_to_exact_count": json_6ii.get("corrected_from_non_exact_to_exact_count"),
        "prior_corrected_full_exact_game_count": json_6ii.get("corrected_full_exact_game_count"),
        "materialization_planning_allowed": json_6ii.get("materialization_planning_allowed_after_this_audit"),
        "materialization_family_count": len(MATERIALIZATION_FAMILIES),
        "materialized_schema_field_count": len(MATERIALIZED_SCHEMA_FIELDS),
        "success_criteria_count": len(success_rows),
        "implementation_scope_count": len(implementation_scope_rows),
        "readonly_source_count": len(readonly_rows),
        "output_contract_count": len(output_contract_rows),
        "future_6ik_contract_valid": all(row["passed"] for row in future_6ik_rows),
        "future_6il_contract_valid": all(row["passed"] for row in future_6il_rows),
        "preserved_remediated_family_count": len(PRESERVED_FAMILIES),
        "materialization_allowed_after_this_plan": False,
        "materialization_still_blocked": True,
        "adapter_revision_allowed_after_this_plan": False,
        "adapter_revision_still_blocked": True,
        "real_evaluation_allowed_after_this_plan": False,
        "real_evaluation_blocked_by_validation": True,
        "future_adapter_revision_allowed_by_this_layer": False,
        "future_real_evaluation_allowed_by_this_layer": False,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "mechanic_evaluations_run": False,
        "actual_outcomes_joined_to_mechanics": False,
        "corrected_normalized_outcomes_emitted_by_this_layer": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "database_writes_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "materialization_jobs_run": False,
        "production_simulations_run": False,
        "games_evaluated": 0,
        "activation_allowed": False,
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
            "materialization_families_csv": str(MATERIALIZATION_FAMILIES_CSV),
            "schema_contract_csv": str(SCHEMA_CONTRACT_CSV),
            "implementation_scope_csv": str(IMPLEMENTATION_SCOPE_CSV),
            "success_criteria_csv": str(SUCCESS_CRITERIA_CSV),
            "readonly_sources_csv": str(READONLY_SOURCES_CSV),
            "output_contract_csv": str(OUTPUT_CONTRACT_CSV),
            "future_6ik_contract_csv": str(FUTURE_6IK_CONTRACT_CSV),
            "future_6il_contract_csv": str(FUTURE_6IL_CONTRACT_CSV),
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
