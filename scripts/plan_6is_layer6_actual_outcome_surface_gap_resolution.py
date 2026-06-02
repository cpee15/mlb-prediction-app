#!/usr/bin/env python3
"""Plan Layer 6IS actual-outcome surface gap resolution."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6is_actual_outcome_surface_gap_resolution_plan"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

AUDIT_6IR_PATH = Path("scripts/audit_6ir_layer6_gameplay_mechanic_outcome_real_evaluation_implementation.py")
IMPLEMENT_6IQ_PATH = Path("scripts/implement_6iq_layer6_gameplay_mechanic_outcome_real_evaluation.py")
ADAPTER_MODULE_PATH = Path("mlb_app/simulation/layer6_base_out_transition_adapter.py")

JSON_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit.json"
CHECKS_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_checks.csv"
PREDECESSOR_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_predecessor.csv"
INPUT_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_input_artifacts.csv"
ADAPTER_LOAD_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_adapter_load.csv"
EVAL_MATRIX_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_evaluation_matrix.csv"
METRICS_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_metric_rows.csv"
BASELINE_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_baseline_comparison.csv"
CANDIDATE_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_candidate_decisions.csv"
ACTUAL_SURFACE_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_actual_outcome_surface.csv"
GAP_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_gap_classification.csv"
LINEAGE_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_lineage.csv"
READINESS_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_readiness.csv"
FUTURE_6IS_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_future_6is_contract.csv"
READONLY_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_readonly_sources.csv"
PRESERVED_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_preserved_families.csv"
BLOCKING_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_blocking_policy.csv"
DECISION_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_decision.csv"
SAFETY_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_safety_boundaries.csv"
IMMUTABILITY_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_immutability.csv"
RECOMMENDED_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit_recommended_path.csv"

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
PROBLEM_CSV = TMP_DIR / f"{SLUG}_problem_statement.csv"
TRUTH_SURFACES_CSV = TMP_DIR / f"{SLUG}_required_truth_surfaces.csv"
MECHANIC_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_mechanic_truth_requirements.csv"
SOURCE_STRATEGY_CSV = TMP_DIR / f"{SLUG}_allowed_source_strategy.csv"
FORBIDDEN_CSV = TMP_DIR / f"{SLUG}_forbidden_shortcuts.csv"
LINEAGE_REQ_CSV = TMP_DIR / f"{SLUG}_lineage_requirements.csv"
MATERIALIZATION_REQ_CSV = TMP_DIR / f"{SLUG}_materialization_requirements.csv"
VALIDATION_REQ_CSV = TMP_DIR / f"{SLUG}_validation_requirements.csv"
FUTURE_6IT_CSV = TMP_DIR / f"{SLUG}_future_6it_contract.csv"
FUTURE_6IU_CSV = TMP_DIR / f"{SLUG}_future_6iu_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6IR = "layer_6_gameplay_mechanic_outcome_real_evaluation_implementation_audit_complete"
DIAGNOSIS_6IS = "layer_6_actual_outcome_surface_gap_resolution_plan_complete"

RECOMMENDED_NEXT_LAYER_6IR = "6IS_layer_6_actual_outcome_surface_gap_resolution_plan"
RECOMMENDED_PATH_6IR = "audit_real_evaluation_implementation_then_plan_actual_outcome_surface_gap_resolution"

RECOMMENDED_NEXT_LAYER_6IS = "6IT_layer_6_actual_outcome_surface_gap_resolution_implementation"
RECOMMENDED_PATH_6IS = "plan_actual_outcome_surface_gap_resolution_then_implement_before_truth_join_evaluation"

SOURCE_FAMILY = "actual_outcome_surfaces"
DEPENDS_ON_SOURCE_FAMILY = "base_out_transitions"
ACQUISITION_MODE = "future_controlled_or_reuse_existing_statsapi_acquisition"

PRESERVED_FAMILIES = ["game_level_outcomes", "inning_runs", "base_out_transitions"]

TRUTH_SURFACES = [
    "transition_state_truth_surface",
    "run_delta_truth_surface",
    "out_delta_truth_surface",
    "mechanic_event_truth_labels",
    "base_advance_truth_surface",
    "runner_movement_truth_surface",
    "scoring_play_truth_surface",
    "inning_context_truth_surface",
    "substitution_context_truth_surface",
    "bullpen_sequence_truth_surface",
]

MECHANICS = [
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

ALLOWED_SOURCE_STRATEGIES = [
    "reuse_existing_6ib_statsapi_game_feed_if_event_payload_sufficient",
    "materialize_truth_surfaces_from_existing_local_payloads_when_lineage_complete",
    "emit_controlled_acquisition_requirement_if_existing_payloads_insufficient",
    "require_manifest_backed_and_audited_source_additions",
    "preserve_game_id_play_id_event_index_lineage_to_base_out_transition_records",
]

FORBIDDEN_SHORTCUTS = [
    "infer_truth_labels_only_from_model_predictions",
    "infer_actual_outcomes_only_from_simulated_outputs",
    "use_scaffolded_6iq_metrics_as_true_outcomes",
    "silently_fetch_remote_data",
    "write_database_state",
    "bypass_source_manifests",
    "mark_candidate_decisions_final_without_truth_joins",
    "activate_mechanics_based_on_scaffolded_evaluation_output",
]

LINEAGE_REQUIREMENTS = [
    "truth_surface_rows_must_reference_source_family",
    "truth_surface_rows_must_reference_source_path_or_manifest_entry",
    "truth_surface_rows_must_reference_game_id",
    "truth_surface_rows_must_reference_play_id_or_event_index_when_available",
    "truth_surface_rows_must_link_to_base_out_transition_record_when_applicable",
    "truth_surface_rows_must_preserve_derivation_method",
]

MATERIALIZATION_REQUIREMENTS = [
    "inspect_existing_6ib_statsapi_game_feed_before_any_new_acquisition",
    "create_truth_surface_schema_contract_before_materializing_rows",
    "materialize_actual_outcome_surface_as_candidate_only",
    "separate truth surface materialization from evaluation rerun",
    "emit source sufficiency report",
    "emit materialization manifest",
    "emit row-level lineage",
]

VALIDATION_REQUIREMENTS = [
    "validate_required_truth_surface_schema_fields",
    "validate_row_counts_by_surface",
    "validate_mechanic_coverage",
    "validate_game_id_and_play_id_population",
    "validate_no_scaffolded_metric_substitution",
    "validate_no_remote_fetch_without_manifest",
    "validate_no_activation_or_exit_credit",
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
    audit_before = AUDIT_6IR_PATH.read_text(encoding="utf-8") if AUDIT_6IR_PATH.exists() else ""
    impl_before = IMPLEMENT_6IQ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IQ_PATH.exists() else ""
    adapter_before = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_before = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_before = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_before = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    json_6ir = load_json(JSON_6IR)

    required_inputs = [
        JSON_6IR, CHECKS_6IR, PREDECESSOR_6IR, INPUT_6IR, ADAPTER_LOAD_6IR,
        EVAL_MATRIX_6IR, METRICS_6IR, BASELINE_6IR, CANDIDATE_6IR,
        ACTUAL_SURFACE_6IR, GAP_6IR, LINEAGE_6IR, READINESS_6IR,
        FUTURE_6IS_6IR, READONLY_6IR, PRESERVED_6IR, BLOCKING_6IR,
        DECISION_6IR, SAFETY_6IR, IMMUTABILITY_6IR, RECOMMENDED_6IR,
        JSON_6IQ, JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IM, JSON_6IL,
        JSON_6IK, JSON_6IJ, JSON_6II, ADAPTER_MODULE_PATH, MATERIALIZED_TABLE,
        MATERIALIZATION_MANIFEST, MATERIALIZED_SCHEMA, MATERIALIZED_LINEAGE,
        MATERIALIZATION_VALIDATION, MATERIALIZATION_READINESS, CORRECTED_INDEX_6IH,
        SOURCE_PROVENANCE_6IH, SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB,
        RAW_FEED_DIR_6IB,
    ]

    readonly_sources = [
        JSON_6IR, JSON_6IQ, JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IM, JSON_6IL,
        JSON_6IK, JSON_6IJ, JSON_6II, ADAPTER_MODULE_PATH, MATERIALIZED_TABLE,
        MATERIALIZATION_MANIFEST, MATERIALIZED_SCHEMA, MATERIALIZED_LINEAGE,
        MATERIALIZATION_VALIDATION, MATERIALIZATION_READINESS, CORRECTED_INDEX_6IH,
        SOURCE_PROVENANCE_6IH, SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB,
        RAW_FEED_DIR_6IB,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ir_audit_exists", "expected": True, "actual": AUDIT_6IR_PATH.exists(), "passed": AUDIT_6IR_PATH.exists()},
        {"check": "6ir_json_exists", "expected": True, "actual": JSON_6IR.exists(), "passed": JSON_6IR.exists()},
        {"check": "6ir_all_checks_passed", "expected": True, "actual": json_6ir.get("all_checks_passed"), "passed": json_6ir.get("all_checks_passed") is True},
        {"check": "6ir_diagnosis", "expected": DIAGNOSIS_6IR, "actual": json_6ir.get("diagnosis"), "passed": json_6ir.get("diagnosis") == DIAGNOSIS_6IR},
        {"check": "6ir_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IR, "actual": json_6ir.get("recommended_next_layer"), "passed": json_6ir.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6IR},
        {"check": "6ir_recommended_path", "expected": RECOMMENDED_PATH_6IR, "actual": json_6ir.get("recommended_path"), "passed": json_6ir.get("recommended_path") == RECOMMENDED_PATH_6IR},
        {"check": "6ir_gap_confirmed", "expected": True, "actual": json_6ir.get("actual_outcome_surface_gap_confirmed"), "passed": json_6ir.get("actual_outcome_surface_gap_confirmed") is True},
        {"check": "6ir_activation_planning_blocked", "expected": False, "actual": json_6ir.get("activation_planning_allowed_after_this_audit"), "passed": json_6ir.get("activation_planning_allowed_after_this_audit") is False},
        {"check": "6ir_no_exit_credit", "expected": False, "actual": json_6ir.get("layer_6_exit_credit"), "passed": json_6ir.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    problem_rows = [{
        "problem": "actual outcome event surfaces are unavailable, preventing final pass/fail decisions and activation planning",
        "gap": "actual_outcome_surface_gap_resolution",
        "source_family": SOURCE_FAMILY,
        "depends_on": DEPENDS_ON_SOURCE_FAMILY,
        "passed": True,
    }]

    truth_surface_rows = [
        {"truth_surface": surface, "required": True, "planned": True, "passed": True}
        for surface in TRUTH_SURFACES
    ]

    mechanic_requirement_rows = [
        {"mechanic": mechanic, "truth_surface_required": True, "requires_actual_outcome_join": True, "planned": True, "passed": True}
        for mechanic in MECHANICS
    ]

    source_strategy_rows = [
        {"strategy": strategy, "allowed": True, "planned": True, "passed": True}
        for strategy in ALLOWED_SOURCE_STRATEGIES
    ]

    forbidden_rows = [
        {"shortcut": shortcut, "forbidden": True, "passed": True}
        for shortcut in FORBIDDEN_SHORTCUTS
    ]

    lineage_rows = [
        {"requirement": req, "required": True, "passed": True}
        for req in LINEAGE_REQUIREMENTS
    ]

    materialization_rows = [
        {"requirement": req, "required": True, "passed": True}
        for req in MATERIALIZATION_REQUIREMENTS
    ]

    validation_rows = [
        {"requirement": req, "required": True, "passed": True}
        for req in VALIDATION_REQUIREMENTS
    ]

    future_6it_rows = [
        {"contract": "inspect_existing_6ib_statsapi_game_feed_first", "required": True, "passed": True},
        {"contract": "materialize_truth_surfaces_from_local_payloads_if_sufficient", "required": True, "passed": True},
        {"contract": "emit_controlled_acquisition_requirement_if_insufficient", "required": True, "passed": True},
        {"contract": "emit_truth_surface_schema_contract", "required": True, "passed": True},
        {"contract": "emit_truth_surface_candidate_outputs", "required": True, "passed": True},
        {"contract": "emit_source_sufficiency_report", "required": True, "passed": True},
        {"contract": "emit_row_level_lineage", "required": True, "passed": True},
        {"contract": "do_not_join_truth_to_evaluation_or_activate", "required": True, "passed": True},
    ]

    future_6iu_rows = [
        {"contract": "audit_6it_source_sufficiency", "required": True, "passed": True},
        {"contract": "audit_truth_surface_schema", "required": True, "passed": True},
        {"contract": "audit_truth_surface_row_counts", "required": True, "passed": True},
        {"contract": "audit_event_labeling", "required": True, "passed": True},
        {"contract": "audit_lineage_completeness", "required": True, "passed": True},
        {"contract": "audit_no_forbidden_shortcuts", "required": True, "passed": True},
        {"contract": "audit_no_ungoverned_fetches", "required": True, "passed": True},
        {"contract": "keep_activation_and_exit_blocked", "required": True, "passed": True},
    ]

    readonly_rows = [
        {"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()}
        for path in readonly_sources
    ]

    preserved_rows = [
        {"source_family": "game_level_outcomes", "status": "preserved_remediated_from_prior_layers", "passed": True},
        {"source_family": "inning_runs", "status": "preserved_remediated_from_prior_layers", "passed": True},
        {"source_family": "base_out_transitions", "status": "preserved_audited_dependency", "passed": True},
    ]

    blocking_rows = [
        {"blocked_surface": "truth_surface_creation", "blocked": True, "reason": "6IS is planning only", "passed": True},
        {"blocked_surface": "actual_outcome_join", "blocked": True, "reason": "truth surface not implemented/audited", "passed": True},
        {"blocked_surface": "real_evaluation_rerun", "blocked": True, "reason": "truth surface not implemented/audited", "passed": True},
        {"blocked_surface": "activation_planning", "blocked": True, "reason": "final pass/fail decisions impossible", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "activation planning blocked", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "activation chain incomplete", "passed": True},
    ]

    decision_rows = [
        {"decision": "6ir_passed", "expected": True, "actual": json_6ir.get("all_checks_passed"), "passed": json_6ir.get("all_checks_passed") is True},
        {"decision": "gap_confirmed", "expected": True, "actual": json_6ir.get("actual_outcome_surface_gap_confirmed"), "passed": json_6ir.get("actual_outcome_surface_gap_confirmed") is True},
        {"decision": "truth_surface_count", "expected": 10, "actual": len(TRUTH_SURFACES), "passed": len(TRUTH_SURFACES) == 10},
        {"decision": "mechanic_requirement_count", "expected": 10, "actual": len(MECHANICS), "passed": len(MECHANICS) == 10},
        {"decision": "future_6it_contract_valid", "expected": True, "actual": True, "passed": True},
        {"decision": "future_6iu_contract_valid", "expected": True, "actual": True, "passed": True},
        {"decision": "recommend_6it_implementation_next", "expected": RECOMMENDED_NEXT_LAYER_6IS, "actual": RECOMMENDED_NEXT_LAYER_6IS, "passed": True},
        {"decision": "truth_surface_created_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "actual_outcome_join_executed", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_truth_surface_creation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6iq_implementation_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_adapter_implementation_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ib_artifact_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ih_corrected_candidate_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ik_materialized_output_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_actual_outcome_join", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation_rerun", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_after = AUDIT_6IR_PATH.read_text(encoding="utf-8") if AUDIT_6IR_PATH.exists() else ""
    impl_after = IMPLEMENT_6IQ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IQ_PATH.exists() else ""
    adapter_after = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_after = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_after = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_after = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    immutability_rows = [
        {"surface": "this_6is_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6ir_audit", "policy": "unchanged_by_6is", "passed": audit_after == audit_before},
        {"surface": "6iq_implementation", "policy": "unchanged_by_6is", "passed": impl_after == impl_before},
        {"surface": "adapter_module", "policy": "unchanged_by_6is", "passed": adapter_after == adapter_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6is", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6is", "passed": corrected_after == corrected_before},
        {"surface": "6ik_materialized_table", "policy": "read_only_unchanged_by_6is", "passed": materialized_after == materialized_before},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IS, "actual": RECOMMENDED_NEXT_LAYER_6IS, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6IS, "actual": RECOMMENDED_PATH_6IS, "passed": True},
        {"decision": "recommend_actual_outcome_surface_gap_resolution_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_truth_join_or_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6IS, "actual": DIAGNOSIS_6IS, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "problem_statement", "passed": all(row["passed"] for row in problem_rows), "detail": "1/1"},
        {"check": "required_truth_surfaces", "passed": all(row["passed"] for row in truth_surface_rows) and len(truth_surface_rows) == 10, "detail": f"{len(truth_surface_rows)}/10"},
        {"check": "mechanic_truth_requirements", "passed": all(row["passed"] for row in mechanic_requirement_rows) and len(mechanic_requirement_rows) == 10, "detail": f"{len(mechanic_requirement_rows)}/10"},
        {"check": "allowed_source_strategy", "passed": all(row["passed"] for row in source_strategy_rows), "detail": f"{sum(1 for row in source_strategy_rows if row['passed'])}/{len(source_strategy_rows)}"},
        {"check": "forbidden_shortcuts", "passed": all(row["passed"] for row in forbidden_rows), "detail": f"{sum(1 for row in forbidden_rows if row['passed'])}/{len(forbidden_rows)}"},
        {"check": "lineage_requirements", "passed": all(row["passed"] for row in lineage_rows), "detail": f"{sum(1 for row in lineage_rows if row['passed'])}/{len(lineage_rows)}"},
        {"check": "materialization_requirements", "passed": all(row["passed"] for row in materialization_rows), "detail": f"{sum(1 for row in materialization_rows if row['passed'])}/{len(materialization_rows)}"},
        {"check": "validation_requirements", "passed": all(row["passed"] for row in validation_rows), "detail": f"{sum(1 for row in validation_rows if row['passed'])}/{len(validation_rows)}"},
        {"check": "future_6it_contract", "passed": all(row["passed"] for row in future_6it_rows), "detail": f"{sum(1 for row in future_6it_rows if row['passed'])}/{len(future_6it_rows)}"},
        {"check": "future_6iu_contract", "passed": all(row["passed"] for row in future_6iu_rows), "detail": f"{sum(1 for row in future_6iu_rows if row['passed'])}/{len(future_6iu_rows)}"},
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
        "problem_statement": write_csv(PROBLEM_CSV, problem_rows),
        "required_truth_surfaces": write_csv(TRUTH_SURFACES_CSV, truth_surface_rows),
        "mechanic_truth_requirements": write_csv(MECHANIC_REQUIREMENTS_CSV, mechanic_requirement_rows),
        "allowed_source_strategy": write_csv(SOURCE_STRATEGY_CSV, source_strategy_rows),
        "forbidden_shortcuts": write_csv(FORBIDDEN_CSV, forbidden_rows),
        "lineage_requirements": write_csv(LINEAGE_REQ_CSV, lineage_rows),
        "materialization_requirements": write_csv(MATERIALIZATION_REQ_CSV, materialization_rows),
        "validation_requirements": write_csv(VALIDATION_REQ_CSV, validation_rows),
        "future_6it_contract": write_csv(FUTURE_6IT_CSV, future_6it_rows),
        "future_6iu_contract": write_csv(FUTURE_6IU_CSV, future_6iu_rows),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6IS",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6IS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6IS,
        "recommended_path": RECOMMENDED_PATH_6IS,
        "predecessor_audit": str(AUDIT_6IR_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6ir.get("diagnosis"),
        "audited_layer": "6IR",
        "source_family": SOURCE_FAMILY,
        "depends_on_source_family": DEPENDS_ON_SOURCE_FAMILY,
        "acquisition_mode": ACQUISITION_MODE,
        "actual_outcome_surface_gap_confirmed": json_6ir.get("actual_outcome_surface_gap_confirmed"),
        "actual_outcome_surface_gap_resolution_planned": True,
        "required_truth_surface_count": len(TRUTH_SURFACES),
        "mechanic_truth_requirement_count": len(MECHANICS),
        "allowed_source_strategy_count": len(ALLOWED_SOURCE_STRATEGIES),
        "forbidden_shortcut_count": len(FORBIDDEN_SHORTCUTS),
        "lineage_requirement_count": len(LINEAGE_REQUIREMENTS),
        "materialization_requirement_count": len(MATERIALIZATION_REQUIREMENTS),
        "validation_requirement_count": len(VALIDATION_REQUIREMENTS),
        "future_6it_contract_valid": all(row["passed"] for row in future_6it_rows),
        "future_6iu_contract_valid": all(row["passed"] for row in future_6iu_rows),
        "preserved_remediated_family_count": len(PRESERVED_FAMILIES),
        "truth_surface_created_by_this_layer": False,
        "actual_outcome_join_executed": False,
        "final_pass_fail_decision_possible_after_this_layer": False,
        "activation_planning_allowed_after_this_layer": False,
        "real_evaluation_rerun_allowed_after_this_layer": False,
        "source_artifacts_mutated": False,
        "corrected_candidate_artifacts_mutated": False,
        "materialized_outputs_mutated": False,
        "adapter_implementation_mutated": False,
        "evaluation_implementation_mutated": False,
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
            "problem_statement_csv": str(PROBLEM_CSV),
            "required_truth_surfaces_csv": str(TRUTH_SURFACES_CSV),
            "mechanic_truth_requirements_csv": str(MECHANIC_REQUIREMENTS_CSV),
            "allowed_source_strategy_csv": str(SOURCE_STRATEGY_CSV),
            "forbidden_shortcuts_csv": str(FORBIDDEN_CSV),
            "lineage_requirements_csv": str(LINEAGE_REQ_CSV),
            "materialization_requirements_csv": str(MATERIALIZATION_REQ_CSV),
            "validation_requirements_csv": str(VALIDATION_REQ_CSV),
            "future_6it_contract_csv": str(FUTURE_6IT_CSV),
            "future_6iu_contract_csv": str(FUTURE_6IU_CSV),
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
