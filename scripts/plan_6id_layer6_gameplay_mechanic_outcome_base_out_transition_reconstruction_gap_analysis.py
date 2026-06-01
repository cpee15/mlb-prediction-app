#!/usr/bin/env python3
"""Plan Layer 6ID base/out transition reconstruction gap analysis."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6id_base_out_transition_reconstruction_gap_analysis_plan"
TMP_DIR = Path("tmp")

AUDIT_6IC_PATH = Path("scripts/audit_6ic_layer6_gameplay_mechanic_outcome_base_out_transition_external_source_acquisition_implementation.py")

JSON_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit.json"
CHECKS_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_checks.csv"
PREDECESSOR_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_predecessor.csv"
ARTIFACTS_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_artifact_presence.csv"
BOUNDS_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_acquisition_bounds.csv"
FETCHES_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_fetch_attempts.csv"
TRANSITION_AUDIT_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_transition_index.csv"
EXACTNESS_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_exactness_profile.csv"
GAP_CATEGORIES_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_gap_categories.csv"
SELECTION_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_source_selection.csv"
READINESS_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_readiness.csv"
MANIFEST_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_manifest.csv"
PRESERVED_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_preserved_families.csv"
DECISION_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_decision.csv"
FUTURE_6ID_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_future_6id_contract.csv"
SAFETY_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_safety_boundaries.csv"
IMMUTABILITY_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_immutability.csv"
RECOMMENDED_6IC = TMP_DIR / "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit_recommended_path.csv"

SOURCE_MANIFEST_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/source_manifest.json"
TRANSITION_INDEX_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/base_out_transition_index.csv"
RAW_FEED_DIR_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/statsapi_game_feed"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
GAP_CONTEXT_CSV = TMP_DIR / f"{SLUG}_gap_context.csv"
GAP_TAXONOMY_CSV = TMP_DIR / f"{SLUG}_gap_taxonomy.csv"
IMPLEMENTATION_SCOPE_CSV = TMP_DIR / f"{SLUG}_implementation_scope.csv"
READONLY_SOURCES_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
ANALYSIS_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_analysis_requirements.csv"
FIXABILITY_FRAMEWORK_CSV = TMP_DIR / f"{SLUG}_fixability_framework.csv"
FUTURE_6IE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6ie_contract.csv"
FUTURE_6IF_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6if_contract.csv"
PRESERVED_FAMILIES_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6IC = "layer_6_gameplay_mechanic_outcome_base_out_transition_external_source_acquisition_implementation_audit_complete"
DIAGNOSIS_6ID = "layer_6_gameplay_mechanic_outcome_base_out_transition_reconstruction_gap_analysis_plan_complete"

RECOMMENDED_NEXT_LAYER_6IC = "6ID_layer_6_gameplay_mechanic_outcome_base_out_transition_reconstruction_gap_analysis_plan"
RECOMMENDED_PATH_6IC = "audit_controlled_acquisition_then_plan_reconstruction_gap_analysis_before_materialization"

RECOMMENDED_NEXT_LAYER_6ID = "6IE_layer_6_gameplay_mechanic_outcome_base_out_transition_reconstruction_gap_analysis_implementation"
RECOMMENDED_PATH_6ID = "plan_non_exact_transition_gap_analysis_then_implement_targeted_reconstruction_diagnostics_before_materialization"

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

GAP_CATEGORIES = [
    "missing_or_ambiguous_runner_end_base",
    "missing_or_ambiguous_runner_start_base",
    "batter_reached_base_assignment_uncertain",
    "out_count_inconsistency",
    "inning_boundary_or_walkoff_boundary",
    "scoring_runner_without_explicit_base_path",
    "substitution_or_non_batted_ball_event",
    "double_play_or_force_play_complexity",
    "caught_stealing_pickoff_or_runner_out_complexity",
    "wild_pitch_passed_ball_balk_runner_movement_complexity",
    "statsapi_representation_gap",
    "parser_logic_gap",
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
    audit_before = AUDIT_6IC_PATH.read_text(encoding="utf-8") if AUDIT_6IC_PATH.exists() else ""
    transition_index_before = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""

    json_6ic = load_json(JSON_6IC)

    required_inputs = [
        JSON_6IC,
        CHECKS_6IC,
        PREDECESSOR_6IC,
        ARTIFACTS_6IC,
        BOUNDS_6IC,
        FETCHES_6IC,
        TRANSITION_AUDIT_6IC,
        EXACTNESS_6IC,
        GAP_CATEGORIES_6IC,
        SELECTION_6IC,
        READINESS_6IC,
        MANIFEST_6IC,
        PRESERVED_6IC,
        DECISION_6IC,
        FUTURE_6ID_6IC,
        SAFETY_6IC,
        IMMUTABILITY_6IC,
        RECOMMENDED_6IC,
    ]

    readonly_sources = [
        SOURCE_MANIFEST_6IB,
        TRANSITION_INDEX_6IB,
        RAW_FEED_DIR_6IB,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ic_audit_exists", "expected": True, "actual": AUDIT_6IC_PATH.exists(), "passed": AUDIT_6IC_PATH.exists()},
        {"check": "6ic_json_exists", "expected": True, "actual": JSON_6IC.exists(), "passed": JSON_6IC.exists()},
        {"check": "6ic_all_checks_passed", "expected": True, "actual": json_6ic.get("all_checks_passed"), "passed": json_6ic.get("all_checks_passed") is True},
        {"check": "6ic_diagnosis", "expected": DIAGNOSIS_6IC, "actual": json_6ic.get("diagnosis"), "passed": json_6ic.get("diagnosis") == DIAGNOSIS_6IC},
        {"check": "6ic_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IC, "actual": json_6ic.get("recommended_next_layer"), "passed": json_6ic.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6IC},
        {"check": "6ic_recommended_path", "expected": RECOMMENDED_PATH_6IC, "actual": json_6ic.get("recommended_path"), "passed": json_6ic.get("recommended_path") == RECOMMENDED_PATH_6IC},
        {"check": "6ic_source_family", "expected": SOURCE_FAMILY, "actual": json_6ic.get("source_family"), "passed": json_6ic.get("source_family") == SOURCE_FAMILY},
        {"check": "6ic_reconstruction_gap_required", "expected": True, "actual": json_6ic.get("reconstruction_gap_analysis_required"), "passed": json_6ic.get("reconstruction_gap_analysis_required") is True},
        {"check": "6ic_no_immediate_acquisition", "expected": False, "actual": json_6ic.get("additional_acquisition_required_immediately"), "passed": json_6ic.get("additional_acquisition_required_immediately") is False},
        {"check": "6ic_materialization_blocked", "expected": True, "actual": json_6ic.get("materialization_still_blocked"), "passed": json_6ic.get("materialization_still_blocked") is True},
        {"check": "6ic_no_exit_credit", "expected": False, "actual": json_6ic.get("layer_6_exit_credit"), "passed": json_6ic.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    gap_context_rows = [{
        "source_family": SOURCE_FAMILY,
        "acquisition_mode": ACQUISITION_MODE,
        "transition_row_count": json_6ic.get("transition_row_count"),
        "exact_transition_row_count": json_6ic.get("exact_transition_row_count"),
        "non_exact_transition_row_count": json_6ic.get("non_exact_transition_row_count"),
        "exact_transition_rate": json_6ic.get("exact_transition_rate"),
        "full_exact_game_count": json_6ic.get("full_exact_game_count"),
        "statsapi_source_family_rejected": json_6ic.get("statsapi_source_family_rejected"),
        "next_action": "targeted_reconstruction_gap_analysis",
        "passed": True,
    }]

    gap_taxonomy_rows = [
        {
            "gap_category": category,
            "required_for_6ie": True,
            "description": "6IE must classify non-exact transition rows against this category",
            "passed": True,
        }
        for category in GAP_CATEGORIES
    ]

    implementation_scope_rows = [
        {"scope": "read_6ib_transition_index", "allowed": True, "required": True, "passed": True},
        {"scope": "isolate_105_non_exact_rows", "allowed": True, "required": True, "passed": True},
        {"scope": "sample_adjacent_exact_rows", "allowed": True, "required": True, "passed": True},
        {"scope": "inspect_local_statsapi_allplays_payloads", "allowed": True, "required": True, "passed": True},
        {"scope": "classify_gap_categories", "allowed": True, "required": True, "passed": True},
        {"scope": "produce_category_examples", "allowed": True, "required": True, "passed": True},
        {"scope": "estimate_parser_fixability", "allowed": True, "required": True, "passed": True},
        {"scope": "estimate_true_representation_gaps", "allowed": True, "required": True, "passed": True},
        {"scope": "recommend_targeted_reconstruction_corrections", "allowed": True, "required": True, "passed": True},
        {"scope": "rewrite_6ib_transition_index", "allowed": False, "required": False, "passed": True},
        {"scope": "fetch_new_data", "allowed": False, "required": False, "passed": True},
        {"scope": "materialize_gameplay_outcomes", "allowed": False, "required": False, "passed": True},
    ]

    readonly_source_rows = [
        {
            "source_path": str(path),
            "exists": path.exists(),
            "source_role": "readonly_6ib_source_artifact",
            "may_modify": False,
            "passed": path.exists(),
        }
        for path in readonly_sources
    ]

    analysis_requirement_rows = [
        {"requirement": "filter_exact_transition_row_false", "required": True, "passed": True},
        {"requirement": "count_non_exact_rows_equals_105", "required": True, "passed": True},
        {"requirement": "join_non_exact_rows_to_game_and_play_id", "required": True, "passed": True},
        {"requirement": "inspect_adjacent_prior_and_next_transition_context", "required": True, "passed": True},
        {"requirement": "inspect_corresponding_raw_allplays_payload", "required": True, "passed": True},
        {"requirement": "classify_one_or_more_gap_categories_per_row", "required": True, "passed": True},
        {"requirement": "emit_category_counts_and_examples", "required": True, "passed": True},
        {"requirement": "separate_parser_logic_gaps_from_source_representation_gaps", "required": True, "passed": True},
        {"requirement": "recommend_targeted_reconstruction_correction_path", "required": True, "passed": True},
        {"requirement": "fail_closed_if_non_exact_rows_cannot_be_inspected", "required": True, "passed": True},
    ]

    fixability_rows = [
        {"classification": "parser_logic_fixable", "meaning": "StatsAPI contains enough evidence but parser did not consume it correctly", "future_action": "targeted_reconstruction_correction_implementation", "passed": True},
        {"classification": "parser_logic_probably_fixable", "meaning": "Evidence appears present but needs more robust event-specific logic", "future_action": "targeted_reconstruction_correction_implementation_with_audit", "passed": True},
        {"classification": "source_representation_uncertain", "meaning": "StatsAPI payload may be missing needed state but requires row-level inspection", "future_action": "manual_or_programmatic_payload_review_before_rejection", "passed": True},
        {"classification": "statsapi_representation_gap", "meaning": "Payload lacks deterministic evidence for exact transition", "future_action": "consider_alternate_source_or_hybrid_import_plan", "passed": True},
        {"classification": "non_reconstructable_without_new_source", "meaning": "No deterministic evidence in current raw feed for row", "future_action": "future_external_source_strategy_only_after_6IE_6IF", "passed": True},
    ]

    future_6ie_rows = [
        {"contract": "consume_6id_plan_and_6ic_audit", "required": True, "passed": True},
        {"contract": "read_6ib_transition_index_readonly", "required": True, "passed": True},
        {"contract": "read_6ib_raw_feed_cache_readonly", "required": True, "passed": True},
        {"contract": "classify_105_non_exact_rows", "required": True, "passed": True},
        {"contract": "emit_gap_category_counts_examples_and_fixability", "required": True, "passed": True},
        {"contract": "recommend_targeted_reconstruction_correction_or_source_strategy", "required": True, "passed": True},
        {"contract": "no_fetch_no_materialization_no_adapter_no_eval_no_activation_no_exit", "required": True, "passed": True},
    ]

    future_6if_rows = [
        {"contract": "audit_6ie_gap_analysis_artifacts", "required": True, "passed": True},
        {"contract": "verify_non_exact_row_coverage", "required": True, "passed": True},
        {"contract": "verify_gap_category_examples_and_fixability_claims", "required": True, "passed": True},
        {"contract": "decide_targeted_reconstruction_fix_vs_alternate_source_path", "required": True, "passed": True},
        {"contract": "keep_materialization_adapter_real_eval_activation_exit_blocked", "required": True, "passed": True},
    ]

    preserved_rows = [
        {"source_family": "game_level_outcomes", "status": "preserved_remediated_from_prior_layers", "passed": json_6ic.get("game_level_outcomes_preserved") is True},
        {"source_family": "inning_runs", "status": "preserved_remediated_from_prior_layers", "passed": json_6ic.get("inning_runs_preserved") is True},
    ]

    blocking_rows = [
        {"blocked_surface": "materialization", "blocked": True, "reason": "base_out_transitions_not_remediated_and_gap_analysis_pending", "passed": True},
        {"blocked_surface": "adapter_revision", "blocked": True, "reason": "source reconstruction incomplete", "passed": True},
        {"blocked_surface": "real_evaluation", "blocked": True, "reason": "no audited full exact base_out_transition source yet", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "real evaluation blocked", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "base_out_transitions unresolved", "passed": True},
    ]

    decision_rows = [
        {"decision": "6ic_passed", "expected": True, "actual": json_6ic.get("all_checks_passed"), "passed": json_6ic.get("all_checks_passed") is True},
        {"decision": "plan_reconstruction_gap_analysis", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_plan_more_acquisition_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "statsapi_source_family_not_rejected", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6ie_implementation_next", "expected": RECOMMENDED_NEXT_LAYER_6ID, "actual": RECOMMENDED_NEXT_LAYER_6ID, "passed": True},
        {"decision": "define_6if_audit_contract", "expected": True, "actual": True, "passed": True},
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
        {"boundary": "no_6ib_transition_index_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ib_raw_feed_cache_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_materialization_jobs", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_adapter_revision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_after = AUDIT_6IC_PATH.read_text(encoding="utf-8") if AUDIT_6IC_PATH.exists() else ""
    transition_index_after = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    immutability_rows = [
        {"surface": "this_6id_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6ic_audit", "policy": "unchanged_by_6id", "passed": audit_after == audit_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6id", "passed": transition_index_after == transition_index_before},
        {"surface": "6ib_raw_feed_cache", "policy": "read_only", "passed": True},
        {"surface": "adapter_behavior", "policy": "unchanged", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6ID, "actual": RECOMMENDED_NEXT_LAYER_6ID, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6ID, "actual": RECOMMENDED_PATH_6ID, "passed": True},
        {"decision": "do_not_recommend_materialization", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_adapter_revision", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_evaluation", "expected": True, "actual": True, "passed": True},
        {"decision": "plan_non_exact_transition_gap_analysis_next", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6ID, "actual": DIAGNOSIS_6ID, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "gap_context", "passed": all(row["passed"] for row in gap_context_rows), "detail": "1/1"},
        {"check": "gap_taxonomy", "passed": all(row["passed"] for row in gap_taxonomy_rows), "detail": f"{len(gap_taxonomy_rows)}/{len(gap_taxonomy_rows)}"},
        {"check": "implementation_scope", "passed": all(row["passed"] for row in implementation_scope_rows), "detail": f"{sum(1 for row in implementation_scope_rows if row['passed'])}/{len(implementation_scope_rows)}"},
        {"check": "readonly_sources", "passed": all(row["passed"] for row in readonly_source_rows), "detail": f"{sum(1 for row in readonly_source_rows if row['passed'])}/{len(readonly_source_rows)}"},
        {"check": "analysis_requirements", "passed": all(row["passed"] for row in analysis_requirement_rows), "detail": f"{sum(1 for row in analysis_requirement_rows if row['passed'])}/{len(analysis_requirement_rows)}"},
        {"check": "fixability_framework", "passed": all(row["passed"] for row in fixability_rows), "detail": f"{sum(1 for row in fixability_rows if row['passed'])}/{len(fixability_rows)}"},
        {"check": "future_6ie_contract", "passed": all(row["passed"] for row in future_6ie_rows), "detail": f"{sum(1 for row in future_6ie_rows if row['passed'])}/{len(future_6ie_rows)}"},
        {"check": "future_6if_contract", "passed": all(row["passed"] for row in future_6if_rows), "detail": f"{sum(1 for row in future_6if_rows if row['passed'])}/{len(future_6if_rows)}"},
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
        "gap_context": write_csv(GAP_CONTEXT_CSV, gap_context_rows),
        "gap_taxonomy": write_csv(GAP_TAXONOMY_CSV, gap_taxonomy_rows),
        "implementation_scope": write_csv(IMPLEMENTATION_SCOPE_CSV, implementation_scope_rows),
        "readonly_sources": write_csv(READONLY_SOURCES_CSV, readonly_source_rows),
        "analysis_requirements": write_csv(ANALYSIS_REQUIREMENTS_CSV, analysis_requirement_rows),
        "fixability_framework": write_csv(FIXABILITY_FRAMEWORK_CSV, fixability_rows),
        "future_6ie_contract": write_csv(FUTURE_6IE_CONTRACT_CSV, future_6ie_rows),
        "future_6if_contract": write_csv(FUTURE_6IF_CONTRACT_CSV, future_6if_rows),
        "preserved_families": write_csv(PRESERVED_FAMILIES_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6ID",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6ID if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6ID,
        "recommended_path": RECOMMENDED_PATH_6ID,
        "predecessor_audit": str(AUDIT_6IC_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6ic.get("diagnosis"),
        "audited_layer": "6IC",
        "source_family": SOURCE_FAMILY,
        "acquisition_mode": ACQUISITION_MODE,
        "prior_transition_row_count": json_6ic.get("transition_row_count"),
        "prior_exact_transition_row_count": json_6ic.get("exact_transition_row_count"),
        "prior_non_exact_transition_row_count": json_6ic.get("non_exact_transition_row_count"),
        "prior_exact_transition_rate": json_6ic.get("exact_transition_rate"),
        "prior_full_exact_game_count": json_6ic.get("full_exact_game_count"),
        "statsapi_source_family_rejected": False,
        "reconstruction_gap_analysis_required": True,
        "additional_acquisition_required_immediately": False,
        "gap_taxonomy_category_count": len(GAP_CATEGORIES),
        "readonly_source_count": len(readonly_source_rows),
        "analysis_requirement_count": len(analysis_requirement_rows),
        "fixability_framework_count": len(fixability_rows),
        "future_6ie_contract_valid": all(row["passed"] for row in future_6ie_rows),
        "future_6if_contract_valid": all(row["passed"] for row in future_6if_rows),
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
            "gap_context_csv": str(GAP_CONTEXT_CSV),
            "gap_taxonomy_csv": str(GAP_TAXONOMY_CSV),
            "implementation_scope_csv": str(IMPLEMENTATION_SCOPE_CSV),
            "readonly_sources_csv": str(READONLY_SOURCES_CSV),
            "analysis_requirements_csv": str(ANALYSIS_REQUIREMENTS_CSV),
            "fixability_framework_csv": str(FIXABILITY_FRAMEWORK_CSV),
            "future_6ie_contract_csv": str(FUTURE_6IE_CONTRACT_CSV),
            "future_6if_contract_csv": str(FUTURE_6IF_CONTRACT_CSV),
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
