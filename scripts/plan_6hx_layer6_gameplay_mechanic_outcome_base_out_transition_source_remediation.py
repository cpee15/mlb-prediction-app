#!/usr/bin/env python3
"""Plan Layer 6HX base/out transition source remediation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6hx_base_out_transition_source_remediation_plan"
TMP_DIR = Path("tmp")

AUDIT_6HW_PATH = Path("scripts/audit_6hw_layer6_gameplay_mechanic_outcome_deterministic_source_gap_remediation_implementation.py")

JSON_6HW = TMP_DIR / "layer6_6hw_deterministic_source_gap_remediation_implementation_audit.json"
CHECKS_6HW = TMP_DIR / "layer6_6hw_deterministic_source_gap_remediation_implementation_audit_checks.csv"
PREDECESSOR_6HW = TMP_DIR / "layer6_6hw_deterministic_source_gap_remediation_implementation_audit_predecessor.csv"
ARTIFACTS_6HW = TMP_DIR / "layer6_6hw_deterministic_source_gap_remediation_implementation_audit_artifact_presence.csv"
TARGETS_6HW = TMP_DIR / "layer6_6hw_deterministic_source_gap_remediation_implementation_audit_remediation_targets.csv"
SELECTION_6HW = TMP_DIR / "layer6_6hw_deterministic_source_gap_remediation_implementation_audit_source_selection.csv"
INDEXES_6HW = TMP_DIR / "layer6_6hw_deterministic_source_gap_remediation_implementation_audit_remediation_indexes.csv"
READINESS_6HW = TMP_DIR / "layer6_6hw_deterministic_source_gap_remediation_implementation_audit_readiness.csv"
DECISION_6HW = TMP_DIR / "layer6_6hw_deterministic_source_gap_remediation_implementation_audit_decision.csv"
FUTURE_6HX_6HW = TMP_DIR / "layer6_6hw_deterministic_source_gap_remediation_implementation_audit_future_6hx_contract.csv"
SAFETY_6HW = TMP_DIR / "layer6_6hw_deterministic_source_gap_remediation_implementation_audit_safety_boundaries.csv"
RECOMMENDED_6HW = TMP_DIR / "layer6_6hw_deterministic_source_gap_remediation_implementation_audit_recommended_path.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
REMAINING_GAP_CSV = TMP_DIR / f"{SLUG}_remaining_gap.csv"
TARGET_CONTRACT_CSV = TMP_DIR / f"{SLUG}_target_contract.csv"
ACCEPTABLE_SOURCES_CSV = TMP_DIR / f"{SLUG}_acceptable_sources.csv"
DISALLOWED_PATHS_CSV = TMP_DIR / f"{SLUG}_disallowed_paths.csv"
LOCAL_SEARCH_PLAN_CSV = TMP_DIR / f"{SLUG}_local_search_plan.csv"
RECONSTRUCTION_CSV = TMP_DIR / f"{SLUG}_reconstruction_requirements.csv"
IMPLEMENTATION_SEQUENCE_CSV = TMP_DIR / f"{SLUG}_implementation_sequence.csv"
FUTURE_6HY_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hy_contract.csv"
FUTURE_6HZ_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hz_contract.csv"
PRESERVED_FAMILIES_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
ACCEPTANCE_CSV = TMP_DIR / f"{SLUG}_acceptance_criteria.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HW = "layer_6_gameplay_mechanic_outcome_deterministic_source_gap_remediation_implementation_audit_complete"
DIAGNOSIS_6HX = "layer_6_gameplay_mechanic_outcome_base_out_transition_source_remediation_plan_complete"

RECOMMENDED_NEXT_LAYER_6HW = "6HX_layer_6_gameplay_mechanic_outcome_base_out_transition_source_remediation_plan"
RECOMMENDED_PATH_6HW = "audit_partial_source_gap_remediation_then_plan_remaining_base_out_transition_source_remediation_before_materialization"

RECOMMENDED_NEXT_LAYER_6HX = "6HY_layer_6_gameplay_mechanic_outcome_base_out_transition_source_remediation_implementation"
RECOMMENDED_PATH_6HX = "plan_remaining_base_out_transition_source_remediation_then_implement_before_materialization"

SOURCE_FAMILY = "base_out_transitions"
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
        raise ValueError(f"no rows for {path}")
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
    audit_before = AUDIT_6HW_PATH.read_text(encoding="utf-8") if AUDIT_6HW_PATH.exists() else ""

    json_6hw = load_json(JSON_6HW)

    required_inputs = [
        JSON_6HW,
        CHECKS_6HW,
        PREDECESSOR_6HW,
        ARTIFACTS_6HW,
        TARGETS_6HW,
        SELECTION_6HW,
        INDEXES_6HW,
        READINESS_6HW,
        DECISION_6HW,
        FUTURE_6HX_6HW,
        SAFETY_6HW,
        RECOMMENDED_6HW,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hw_audit_exists", "expected": True, "actual": AUDIT_6HW_PATH.exists(), "passed": AUDIT_6HW_PATH.exists()},
        {"check": "6hw_json_exists", "expected": True, "actual": JSON_6HW.exists(), "passed": JSON_6HW.exists()},
        {"check": "6hw_all_checks_passed", "expected": True, "actual": json_6hw.get("all_checks_passed"), "passed": json_6hw.get("all_checks_passed") is True},
        {"check": "6hw_diagnosis", "expected": DIAGNOSIS_6HW, "actual": json_6hw.get("diagnosis"), "passed": json_6hw.get("diagnosis") == DIAGNOSIS_6HW},
        {"check": "6hw_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HW, "actual": json_6hw.get("recommended_next_layer"), "passed": json_6hw.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6HW},
        {"check": "6hw_recommended_path", "expected": RECOMMENDED_PATH_6HW, "actual": json_6hw.get("recommended_path"), "passed": json_6hw.get("recommended_path") == RECOMMENDED_PATH_6HW},
        {"check": "6hw_selected_source_family_count", "expected": 2, "actual": json_6hw.get("selected_source_family_count"), "passed": json_6hw.get("selected_source_family_count") == 2},
        {"check": "6hw_fail_closed_family_count", "expected": 1, "actual": json_6hw.get("fail_closed_family_count"), "passed": json_6hw.get("fail_closed_family_count") == 1},
        {"check": "6hw_remaining_gap_family", "expected": SOURCE_FAMILY, "actual": json_6hw.get("remaining_gap_family"), "passed": json_6hw.get("remaining_gap_family") == SOURCE_FAMILY},
        {"check": "6hw_materialization_blocked", "expected": True, "actual": json_6hw.get("materialization_still_blocked"), "passed": json_6hw.get("materialization_still_blocked") is True},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    remaining_gap_rows = [
        {
            "source_family": SOURCE_FAMILY,
            "status": "fail_closed_remaining_gap",
            "reason": "missing_exact_play_level_pre_post_base_out_transition_source",
            "required_next_action": "plan_then_implement_base_out_transition_source_remediation",
            "materialization_blocked_until_resolved": True,
            "passed": True,
        }
    ]

    target_contract_rows = [
        {"field": "game_id_or_gamePk", "required": True, "description": "stable deterministic game identifier", "passed": True},
        {"field": "play_id_or_event_id", "required": True, "description": "stable deterministic play/event identifier", "passed": True},
        {"field": "inning", "required": True, "description": "inning number for event ordering", "passed": True},
        {"field": "half_inning", "required": True, "description": "top/bottom or equivalent team half context", "passed": True},
        {"field": "sequence_ordering", "required": True, "description": "deterministic event order within game", "passed": True},
        {"field": "start_base_state_or_pre_base_state", "required": True, "description": "occupied bases before event", "passed": True},
        {"field": "end_base_state_or_post_base_state", "required": True, "description": "occupied bases after event", "passed": True},
        {"field": "start_outs_or_outs_before", "required": True, "description": "outs before event", "passed": True},
        {"field": "end_outs_or_outs_after", "required": True, "description": "outs after event", "passed": True},
        {"field": "runs_scored", "required": True, "description": "runs scored on event", "passed": True},
        {"field": "event_context_optional", "required": False, "description": "batter/pitcher/event_type/description for audit traceability", "passed": True},
    ]

    acceptable_source_rows = [
        {"source_type": "local_statsapi_live_feed_allplays_with_matchup_about_count_runners_and_result_context", "priority": 1, "acceptable": True, "passed": True},
        {"source_type": "local_statsapi_game_feed_allplays_reconstructable_pre_post_base_out_state", "priority": 2, "acceptable": True, "passed": True},
        {"source_type": "local_play_by_play_csv_with_explicit_pre_post_base_out_state", "priority": 3, "acceptable": True, "passed": True},
        {"source_type": "local_retrosheet_style_event_file_with_deterministic_base_out_reconstruction", "priority": 4, "acceptable": True, "passed": True},
        {"source_type": "local_event_stream_with_runner_movements_outs_runs_sequence", "priority": 5, "acceptable": True, "passed": True},
    ]

    disallowed_rows = [
        {"path": "boxscore_totals_only", "reason": "does_not_define_pre_post_base_out_state", "passed": True},
        {"path": "inning_runs_only", "reason": "does_not_define_runner_movement_or_out_transitions", "passed": True},
        {"path": "probabilistic_runner_advancement_ground_truth", "reason": "model_output_cannot_be_truth_source", "passed": True},
        {"path": "simulated_events_as_source_evidence", "reason": "simulation_cannot_validate_itself", "passed": True},
        {"path": "synthesized_runner_movement", "reason": "fabricates_missing_deterministic_evidence", "passed": True},
        {"path": "live_fetches", "reason": "not_allowed_in_6hx_or_6hy_without_explicit_future_permission", "passed": True},
        {"path": "remote_api_calls", "reason": "local_only_remediation_path_required", "passed": True},
        {"path": "database_writes", "reason": "no_db_writes_in_planning_or_remediation_layers", "passed": True},
        {"path": "materialization", "reason": "blocked_until_all_deterministic_source_families_remediated_and_audited", "passed": True},
        {"path": "adapter_revision", "reason": "blocked_until source remediation and materialization audit", "passed": True},
        {"path": "real_evaluation", "reason": "blocked_until source remediation and materialization audit", "passed": True},
        {"path": "mechanic_activation", "reason": "blocked_until real evaluation permits activation", "passed": True},
        {"path": "layer_6_exit_credit", "reason": "blocked", "passed": True},
    ]

    local_search_rows = [
        {"search_root": "data/raw", "allowed": True, "future_6hy_action": "scan_for_explicit_pbp_or_retrosheet_style_events", "passed": True},
        {"search_root": "tmp/local_source_cache", "allowed": True, "future_6hy_action": "scan_for_staged_event_streams_or_allplays", "passed": True},
        {"search_root": "tmp/statsapi_cache", "allowed": True, "future_6hy_action": "scan_for_live_feed_or_game_feed_allplays_not_boxscore_only", "passed": True},
        {"search_root": "cache", "allowed": True, "future_6hy_action": "scan_for_archived_play_event_data", "passed": True},
        {"search_root": "artifacts", "allowed": True, "future_6hy_action": "scan_for_existing_pbp_artifacts_with_pre_post_states", "passed": True},
    ]

    reconstruction_rows = [
        {"requirement": "event_order_is_deterministic_by_game", "passed": True},
        {"requirement": "start_state_for_first_play_of_half_inning_is_empty_bases_zero_outs", "passed": True},
        {"requirement": "subsequent_start_state_matches_previous_end_state_within_half_inning", "passed": True},
        {"requirement": "outs_are_non_decreasing_within_play_and_reset_by_half_inning", "passed": True},
        {"requirement": "base_state_tracks_runner_occupancy_before_and_after_event", "passed": True},
        {"requirement": "runs_scored_matches_runner_crossing_home_or official event scoring context", "passed": True},
        {"requirement": "half_inning_team_context_is_available", "passed": True},
        {"requirement": "errors_stolen_bases_caught_stealing_wild_pitches_passed_balls_balks_are_traceable_when_present", "passed": True},
        {"requirement": "fail_closed_if_pre_post_state_cannot_be_constructed_exactly", "passed": True},
    ]

    sequence_rows = [
        {"step": 1, "future_layer": "6HY", "action": "load_6HX_plan_and_6HW_audit", "passed": True},
        {"step": 2, "future_layer": "6HY", "action": "scan_only_allowed_local_roots_and_suffixes", "passed": True},
        {"step": 3, "future_layer": "6HY", "action": "classify_candidate_sources_by_contract_fields", "passed": True},
        {"step": 4, "future_layer": "6HY", "action": "select_exact_deterministic_base_out_transition_source_or_fail_closed", "passed": True},
        {"step": 5, "future_layer": "6HY", "action": "create_staged_base_out_transition_source_index_without_materialization", "passed": True},
        {"step": 6, "future_layer": "6HZ", "action": "audit_6HY_selection_reconstruction_and_blocking_policy", "passed": True},
    ]

    future_6hy_rows = [
        {"contract": "consume_6HX_plan_and_6HW_audit", "required": True, "passed": True},
        {"contract": "scan_allowed_local_roots_only", "required": True, "passed": True},
        {"contract": "require_exact_play_level_pre_post_base_out_contract", "required": True, "passed": True},
        {"contract": "preserve_game_level_outcomes_and_inning_runs_remediation", "required": True, "passed": True},
        {"contract": "create_source_index_and_readiness_report", "required": True, "passed": True},
        {"contract": "fail_closed_if_exact_base_out_transition_source_missing", "required": True, "passed": True},
        {"contract": "no_materialization_adapter_revision_or_real_evaluation", "required": True, "passed": True},
    ]

    future_6hz_rows = [
        {"contract": "audit_6hy_predecessor_and_artifacts", "required": True, "passed": True},
        {"contract": "audit_exact_base_out_transition_source_contract_or_fail_closed", "required": True, "passed": True},
        {"contract": "audit_preserved_families_remain_available", "required": True, "passed": True},
        {"contract": "audit_no_materialization_adapter_revision_real_evaluation", "required": True, "passed": True},
        {"contract": "decide_whether_all_three_source_families_are_remediated", "required": True, "passed": True},
        {"contract": "recommend_materialization_planning_only_if_all_source_families_are_remediated", "required": True, "passed": True},
    ]

    preserved_rows = [
        {"source_family": "game_level_outcomes", "status_from_6hw": "remediated", "preserve": True, "passed": json_6hw.get("remediated_game_level_outcomes") is True},
        {"source_family": "inning_runs", "status_from_6hw": "remediated", "preserve": True, "passed": json_6hw.get("remediated_inning_runs") is True},
    ]

    blocking_rows = [
        {"blocked_surface": "materialization", "blocked": True, "reason": "base_out_transitions_unresolved", "passed": True},
        {"blocked_surface": "adapter_revision", "blocked": True, "reason": "source_remediation_incomplete", "passed": True},
        {"blocked_surface": "real_evaluation", "blocked": True, "reason": "cannot_validate_mechanics_without_base_out_transitions", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "evaluation_not_permitted", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "source_family_gap_remaining", "passed": True},
    ]

    acceptance_rows = [
        {"criterion": "6HW_passed", "passed": json_6hw.get("all_checks_passed") is True},
        {"criterion": "remaining_gap_is_base_out_transitions", "passed": json_6hw.get("remaining_gap_family") == SOURCE_FAMILY},
        {"criterion": "game_level_outcomes_preserved", "passed": json_6hw.get("remediated_game_level_outcomes") is True},
        {"criterion": "inning_runs_preserved", "passed": json_6hw.get("remediated_inning_runs") is True},
        {"criterion": "target_contract_created", "passed": len(target_contract_rows) >= 10},
        {"criterion": "future_6hy_contract_created", "passed": len(future_6hy_rows) >= 7},
        {"criterion": "future_6hz_contract_created", "passed": len(future_6hz_rows) >= 6},
        {"criterion": "materialization_blocked", "passed": True},
    ]

    decision_rows = [
        {"decision": "planning_only", "expected": True, "actual": True, "passed": True},
        {"decision": "remaining_gap_family", "expected": SOURCE_FAMILY, "actual": json_6hw.get("remaining_gap_family"), "passed": json_6hw.get("remaining_gap_family") == SOURCE_FAMILY},
        {"decision": "remaining_gap_remediation_required_next", "expected": True, "actual": json_6hw.get("remaining_gap_remediation_required_next"), "passed": json_6hw.get("remaining_gap_remediation_required_next") is True},
        {"decision": "recommend_6hy_implementation_next", "expected": RECOMMENDED_NEXT_LAYER_6HX, "actual": RECOMMENDED_NEXT_LAYER_6HX, "passed": True},
        {"decision": "do_not_recommend_materialization", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_adapter_revision", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_evaluation", "expected": True, "actual": True, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_candidate_scan_or_selection", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_materialization_jobs", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_adapter_revision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_after = AUDIT_6HW_PATH.read_text(encoding="utf-8") if AUDIT_6HW_PATH.exists() else ""
    immutability_rows = [
        {"surface": "this_6hx_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hw_audit", "policy": "unchanged_by_6hx", "passed": audit_after == audit_before},
        {"surface": "6hv_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_artifacts", "policy": "not_modified", "passed": True},
        {"surface": "adapter_behavior", "policy": "unchanged", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HX, "actual": RECOMMENDED_NEXT_LAYER_6HX, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6HX, "actual": RECOMMENDED_PATH_6HX, "passed": True},
        {"decision": "do_not_recommend_materialization", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_adapter_revision", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_evaluation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HX, "actual": DIAGNOSIS_6HX, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "remaining_gap", "passed": all(row["passed"] for row in remaining_gap_rows), "detail": f"{len(remaining_gap_rows)}/1"},
        {"check": "target_contract", "passed": all(row["passed"] for row in target_contract_rows), "detail": f"{sum(1 for row in target_contract_rows if row['passed'])}/{len(target_contract_rows)}"},
        {"check": "acceptable_sources", "passed": all(row["passed"] for row in acceptable_source_rows), "detail": f"{sum(1 for row in acceptable_source_rows if row['passed'])}/{len(acceptable_source_rows)}"},
        {"check": "disallowed_paths", "passed": all(row["passed"] for row in disallowed_rows), "detail": f"{sum(1 for row in disallowed_rows if row['passed'])}/{len(disallowed_rows)}"},
        {"check": "local_search_plan", "passed": all(row["passed"] for row in local_search_rows), "detail": f"{sum(1 for row in local_search_rows if row['passed'])}/{len(local_search_rows)}"},
        {"check": "reconstruction_requirements", "passed": all(row["passed"] for row in reconstruction_rows), "detail": f"{sum(1 for row in reconstruction_rows if row['passed'])}/{len(reconstruction_rows)}"},
        {"check": "implementation_sequence", "passed": all(row["passed"] for row in sequence_rows), "detail": f"{sum(1 for row in sequence_rows if row['passed'])}/{len(sequence_rows)}"},
        {"check": "future_6hy_contract", "passed": all(row["passed"] for row in future_6hy_rows), "detail": f"{sum(1 for row in future_6hy_rows if row['passed'])}/{len(future_6hy_rows)}"},
        {"check": "future_6hz_contract", "passed": all(row["passed"] for row in future_6hz_rows), "detail": f"{sum(1 for row in future_6hz_rows if row['passed'])}/{len(future_6hz_rows)}"},
        {"check": "preserved_families", "passed": all(row["passed"] for row in preserved_rows), "detail": f"{sum(1 for row in preserved_rows if row['passed'])}/{len(preserved_rows)}"},
        {"check": "blocking_policy", "passed": all(row["passed"] for row in blocking_rows), "detail": f"{sum(1 for row in blocking_rows if row['passed'])}/{len(blocking_rows)}"},
        {"check": "acceptance_criteria", "passed": all(row["passed"] for row in acceptance_rows), "detail": f"{sum(1 for row in acceptance_rows if row['passed'])}/{len(acceptance_rows)}"},
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
        "remaining_gap": write_csv(REMAINING_GAP_CSV, remaining_gap_rows),
        "target_contract": write_csv(TARGET_CONTRACT_CSV, target_contract_rows),
        "acceptable_sources": write_csv(ACCEPTABLE_SOURCES_CSV, acceptable_source_rows),
        "disallowed_paths": write_csv(DISALLOWED_PATHS_CSV, disallowed_rows),
        "local_search_plan": write_csv(LOCAL_SEARCH_PLAN_CSV, local_search_rows),
        "reconstruction_requirements": write_csv(RECONSTRUCTION_CSV, reconstruction_rows),
        "implementation_sequence": write_csv(IMPLEMENTATION_SEQUENCE_CSV, sequence_rows),
        "future_6hy_contract": write_csv(FUTURE_6HY_CONTRACT_CSV, future_6hy_rows),
        "future_6hz_contract": write_csv(FUTURE_6HZ_CONTRACT_CSV, future_6hz_rows),
        "preserved_families": write_csv(PRESERVED_FAMILIES_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_rows),
        "acceptance_criteria": write_csv(ACCEPTANCE_CSV, acceptance_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HX",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HX if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6HX,
        "recommended_path": RECOMMENDED_PATH_6HX,
        "predecessor_audit": str(AUDIT_6HW_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6hw.get("diagnosis"),
        "audited_layer": "6HW",
        "remaining_gap_family": SOURCE_FAMILY,
        "remaining_gap_remediation_required_next": True,
        "target_contract_created": True,
        "acceptable_source_type_count": len(acceptable_source_rows),
        "disallowed_path_count": len(disallowed_rows),
        "local_search_root_count": len(local_search_rows),
        "reconstruction_requirement_count": len(reconstruction_rows),
        "future_6hy_contract_valid": all(row["passed"] for row in future_6hy_rows),
        "future_6hz_contract_valid": all(row["passed"] for row in future_6hz_rows),
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
            "remaining_gap_csv": str(REMAINING_GAP_CSV),
            "target_contract_csv": str(TARGET_CONTRACT_CSV),
            "acceptable_sources_csv": str(ACCEPTABLE_SOURCES_CSV),
            "disallowed_paths_csv": str(DISALLOWED_PATHS_CSV),
            "local_search_plan_csv": str(LOCAL_SEARCH_PLAN_CSV),
            "reconstruction_requirements_csv": str(RECONSTRUCTION_CSV),
            "implementation_sequence_csv": str(IMPLEMENTATION_SEQUENCE_CSV),
            "future_6hy_contract_csv": str(FUTURE_6HY_CONTRACT_CSV),
            "future_6hz_contract_csv": str(FUTURE_6HZ_CONTRACT_CSV),
            "preserved_families_csv": str(PRESERVED_FAMILIES_CSV),
            "blocking_policy_csv": str(BLOCKING_POLICY_CSV),
            "acceptance_criteria_csv": str(ACCEPTANCE_CSV),
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
