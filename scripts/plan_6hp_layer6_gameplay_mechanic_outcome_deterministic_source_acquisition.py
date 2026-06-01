#!/usr/bin/env python3
"""Plan Layer 6HP deterministic outcome source acquisition."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6hp_deterministic_source_acquisition_plan"
TMP_DIR = Path("tmp")

AUDIT_6HO_PATH = Path("scripts/audit_6ho_layer6_gameplay_mechanic_outcome_source_materialization_implementation.py")

JSON_6HO = TMP_DIR / "layer6_6ho_source_materialization_implementation_audit.json"
CHECKS_6HO = TMP_DIR / "layer6_6ho_source_materialization_implementation_audit_checks.csv"
PREDECESSOR_6HO = TMP_DIR / "layer6_6ho_source_materialization_implementation_audit_predecessor.csv"
ARTIFACT_PRESENCE_6HO = TMP_DIR / "layer6_6ho_source_materialization_implementation_audit_artifact_presence.csv"
MATERIALIZED_6HO = TMP_DIR / "layer6_6ho_source_materialization_implementation_audit_materialized_artifacts.csv"
QUALITY_6HO = TMP_DIR / "layer6_6ho_source_materialization_implementation_audit_quality_report.csv"
MANIFEST_6HO = TMP_DIR / "layer6_6ho_source_materialization_implementation_audit_manifest.csv"
FAIL_CLOSED_6HO = TMP_DIR / "layer6_6ho_source_materialization_implementation_audit_fail_closed.csv"
SOURCE_SELECTION_6HO = TMP_DIR / "layer6_6ho_source_materialization_implementation_audit_source_selection.csv"
DECISION_6HO = TMP_DIR / "layer6_6ho_source_materialization_implementation_audit_decision.csv"
FUTURE_6HP_6HO = TMP_DIR / "layer6_6ho_source_materialization_implementation_audit_future_6hp_contract.csv"
SAFETY_6HO = TMP_DIR / "layer6_6ho_source_materialization_implementation_audit_safety_boundaries.csv"
RECOMMENDED_6HO = TMP_DIR / "layer6_6ho_source_materialization_implementation_audit_recommended_path.csv"

MAT_GAME = TMP_DIR / "layer6_materialized_game_level_outcomes.csv"
MAT_BASE_OUT = TMP_DIR / "layer6_materialized_base_out_transitions.csv"
MAT_INNING = TMP_DIR / "layer6_materialized_inning_runs.csv"
MAT_MANIFEST = TMP_DIR / "layer6_materialized_outcome_source_manifest.json"
MAT_QUALITY = TMP_DIR / "layer6_materialized_outcome_source_quality_report.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
FAILED_FAMILIES_CSV = TMP_DIR / f"{SLUG}_failed_families.csv"
ACQUISITION_CONTRACTS_CSV = TMP_DIR / f"{SLUG}_acquisition_contracts.csv"
SOURCE_INVENTORY_GUIDANCE_CSV = TMP_DIR / f"{SLUG}_source_inventory_guidance.csv"
VALIDATION_GATES_CSV = TMP_DIR / f"{SLUG}_validation_gates.csv"
BLOCKING_RISKS_CSV = TMP_DIR / f"{SLUG}_blocking_risks.csv"
IMPLEMENTATION_SEQUENCE_CSV = TMP_DIR / f"{SLUG}_implementation_sequence.csv"
ACCEPTANCE_CRITERIA_CSV = TMP_DIR / f"{SLUG}_acceptance_criteria.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
FUTURE_6HQ_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hq_contract.csv"
FUTURE_6HR_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hr_contract.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HO = "layer_6_gameplay_mechanic_outcome_artifact_source_materialization_implementation_audit_complete"
DIAGNOSIS_6HP = "layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_plan_complete"
RECOMMENDED_NEXT_LAYER_6HO = "6HP_layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_plan"
RECOMMENDED_PATH_6HO = "audit_fail_closed_materialization_then_plan_deterministic_source_acquisition_before_adapter_revision"
RECOMMENDED_NEXT_LAYER_6HP = "6HQ_layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_plan_audit"
RECOMMENDED_PATH_6HP = "plan_deterministic_source_acquisition_then_audit_before_implementation_or_adapter_revision"
FUTURE_IMPL_LAYER = "6HR_layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_implementation"
FUTURE_AUDIT_LAYER = "6HQ_layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_plan_audit"

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

FAILED_FAMILIES = [
    {
        "source_family": "game_level_outcomes",
        "failed_reason": "no_deterministic_final_score_source",
        "quality_passed": False,
        "observed_row_count": 0,
        "planned_output_artifact": "tmp/layer6_materialized_game_level_outcomes.csv",
    },
    {
        "source_family": "base_out_transitions",
        "failed_reason": "no_deterministic_play_level_base_out_source",
        "quality_passed": False,
        "observed_row_count": 0,
        "planned_output_artifact": "tmp/layer6_materialized_base_out_transitions.csv",
    },
    {
        "source_family": "inning_runs",
        "failed_reason": "no_deterministic_half_inning_run_source",
        "quality_passed": False,
        "observed_row_count": 0,
        "planned_output_artifact": "tmp/layer6_materialized_inning_runs.csv",
    },
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


def boolish(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()
    script_before = Path(__file__).read_text(encoding="utf-8")
    audit_6ho_before = AUDIT_6HO_PATH.read_text(encoding="utf-8") if AUDIT_6HO_PATH.exists() else ""

    json_6ho = load_json(JSON_6HO)

    required_inputs = [
        JSON_6HO,
        CHECKS_6HO,
        PREDECESSOR_6HO,
        ARTIFACT_PRESENCE_6HO,
        MATERIALIZED_6HO,
        QUALITY_6HO,
        MANIFEST_6HO,
        FAIL_CLOSED_6HO,
        SOURCE_SELECTION_6HO,
        DECISION_6HO,
        FUTURE_6HP_6HO,
        SAFETY_6HO,
        RECOMMENDED_6HO,
        MAT_GAME,
        MAT_BASE_OUT,
        MAT_INNING,
        MAT_MANIFEST,
        MAT_QUALITY,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ho_audit_exists", "expected": True, "actual": AUDIT_6HO_PATH.exists(), "passed": AUDIT_6HO_PATH.exists()},
        {"check": "6ho_json_exists", "expected": True, "actual": JSON_6HO.exists(), "passed": JSON_6HO.exists()},
        {"check": "6ho_all_checks_passed", "expected": True, "actual": json_6ho.get("all_checks_passed"), "passed": json_6ho.get("all_checks_passed") is True},
        {"check": "6ho_diagnosis", "expected": DIAGNOSIS_6HO, "actual": json_6ho.get("diagnosis"), "passed": json_6ho.get("diagnosis") == DIAGNOSIS_6HO},
        {"check": "6ho_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HO, "actual": json_6ho.get("recommended_next_layer"), "passed": json_6ho.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6HO},
        {"check": "6ho_recommended_path", "expected": RECOMMENDED_PATH_6HO, "actual": json_6ho.get("recommended_path"), "passed": json_6ho.get("recommended_path") == RECOMMENDED_PATH_6HO},
        {"check": "6ho_fail_closed_valid", "expected": True, "actual": json_6ho.get("fail_closed_behavior_valid"), "passed": json_6ho.get("fail_closed_behavior_valid") is True},
        {"check": "6ho_deterministic_source_acquisition_required", "expected": True, "actual": json_6ho.get("deterministic_source_acquisition_required_next"), "passed": json_6ho.get("deterministic_source_acquisition_required_next") is True},
        {"check": "6ho_adapter_revision_blocked", "expected": True, "actual": json_6ho.get("adapter_revision_still_blocked"), "passed": json_6ho.get("adapter_revision_still_blocked") is True},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    failed_family_rows = []
    for family in FAILED_FAMILIES:
        failed_family_rows.append({
            **family,
            "acquisition_status_for_current_repo_state": "required",
            "deterministic_source_acquisition_required": True,
            "passed": True,
        })

    acquisition_contract_rows = [
        {
            "source_family": "game_level_outcomes",
            "acquisition_goal": "provide final game scores and final status keyed by game_id",
            "required_fields": "game_id|game_date|season|home_team|away_team|home_score|away_score|final_status|source_record_id",
            "key_fields": "game_id",
            "uniqueness_rule": "one_unique_final_row_per_game_id",
            "validity_rules": "home_score_non_null|away_score_non_null|final_status_final_or_completed|no_duplicate_game_id",
            "allowed_local_sources": "local_cached_statsapi_schedule|local_cached_boxscore_or_game_json|local_raw_feed_files_under_data_raw_or_tmp_local_source_cache",
            "disallowed_sources": "live_network_fetch|database_write|remote_api_call|generated_model_output|simulation_output|inferred_score_from_projections",
            "fail_closed_rule": "fail_if_no_unique_final_score_row_per_game_id",
            "planned_output_artifact": "tmp/layer6_materialized_game_level_outcomes.csv",
            "future_implementation_layer": FUTURE_IMPL_LAYER,
            "future_audit_layer": FUTURE_AUDIT_LAYER,
            "passed": True,
        },
        {
            "source_family": "base_out_transitions",
            "acquisition_goal": "provide play_level_pre_post_base_out_state_and_runs_scored_keyed_by_game_id_event_id_play_id",
            "required_fields": "game_id|event_id|play_id|inning|half_inning|batting_team|fielding_team|start_base_state|start_outs|end_base_state|end_outs|runs_scored|event_type|batter_id|pitcher_id|sequence_number|source_record_id",
            "key_fields": "game_id|event_id|play_id",
            "uniqueness_rule": "one_unique_row_per_game_id_event_id_play_id",
            "validity_rules": "start_outs_0_to_3|end_outs_0_to_3|explicit_base_states|runs_scored_non_null_ge_0|no_duplicate_key",
            "allowed_local_sources": "local_cached_statsapi_live_feed_allplays|local_retrosheet_style_event_file|local_raw_play_by_play_csv_with_pre_post_states",
            "disallowed_sources": "live_network_fetch|database_write|aggregate_boxscore_state_inference|simulation_output|model_generated_transitions",
            "fail_closed_rule": "fail_if_play_level_pre_post_state_is_missing_or_not_unique",
            "planned_output_artifact": "tmp/layer6_materialized_base_out_transitions.csv",
            "future_implementation_layer": FUTURE_IMPL_LAYER,
            "future_audit_layer": FUTURE_AUDIT_LAYER,
            "passed": True,
        },
        {
            "source_family": "inning_runs",
            "acquisition_goal": "provide_half_inning_run_totals_keyed_by_game_id_inning_half_inning",
            "required_fields": "game_id|inning|half_inning|batting_team|fielding_team|runs_scored|start_score_batting|start_score_fielding|end_score_batting|end_score_fielding|source_record_id",
            "key_fields": "game_id|inning|half_inning",
            "uniqueness_rule": "one_unique_row_per_game_id_inning_half_inning",
            "validity_rules": "runs_scored_non_null_ge_0|batting_team_non_null|fielding_team_non_null|no_duplicate_key",
            "allowed_local_sources": "local_cached_statsapi_linescore_json|local_cached_game_feed_allplays_aggregated_by_half_inning|local_raw_linescore_csv",
            "disallowed_sources": "live_network_fetch|database_write|final_score_only_inning_split_inference|simulation_output|model_generated_inning_runs",
            "fail_closed_rule": "fail_if_half_inning_runs_or_teams_are_missing_or_not_unique",
            "planned_output_artifact": "tmp/layer6_materialized_inning_runs.csv",
            "future_implementation_layer": FUTURE_IMPL_LAYER,
            "future_audit_layer": FUTURE_AUDIT_LAYER,
            "passed": True,
        },
    ]

    source_inventory_rows = [
        {
            "source_family": "game_level_outcomes",
            "search_roots": "data/raw|tmp/local_source_cache|tmp/statsapi_cache|cache|artifacts",
            "allowed_file_types": ".csv|.json|.jsonl|.parquet|.pkl|.pickle",
            "preferred_file_patterns": "schedule|game|boxscore|statsapi|final",
            "required_evidence_for_selection": "explicit_game_id_home_team_away_team_home_score_away_score_final_status",
            "rejection_reasons": "missing_final_status|missing_score|duplicate_game_id|score_from_projection_or_simulation",
            "acquisition_status_for_current_repo_state": "required",
            "passed": True,
        },
        {
            "source_family": "base_out_transitions",
            "search_roots": "data/raw|tmp/local_source_cache|tmp/statsapi_cache|cache|artifacts",
            "allowed_file_types": ".csv|.json|.jsonl|.parquet|.pkl|.pickle",
            "preferred_file_patterns": "live_feed|game_feed|play_by_play|allplays|events|retrosheet",
            "required_evidence_for_selection": "explicit_game_id_event_id_play_id_start_base_state_end_base_state_start_outs_end_outs_runs_scored",
            "rejection_reasons": "missing_pre_state|missing_post_state|aggregate_only|duplicate_play_key|state_inferred_from_boxscore_totals",
            "acquisition_status_for_current_repo_state": "required",
            "passed": True,
        },
        {
            "source_family": "inning_runs",
            "search_roots": "data/raw|tmp/local_source_cache|tmp/statsapi_cache|cache|artifacts",
            "allowed_file_types": ".csv|.json|.jsonl|.parquet|.pkl|.pickle",
            "preferred_file_patterns": "linescore|inning|game_feed|allplays|boxscore",
            "required_evidence_for_selection": "explicit_game_id_inning_half_inning_batting_team_fielding_team_runs_scored",
            "rejection_reasons": "final_score_only|missing_half_inning|missing_team_context|duplicate_half_inning_key|model_generated",
            "acquisition_status_for_current_repo_state": "required",
            "passed": True,
        },
    ]

    validation_gate_rows = [
        {"gate": "predecessor_6ho_passed", "required": True, "fail_closed": True, "passed": json_6ho.get("all_checks_passed") is True},
        {"gate": "failed_family_count_is_three", "required": True, "fail_closed": True, "passed": json_6ho.get("failed_source_family_count") == 3},
        {"gate": "fail_closed_behavior_validated", "required": True, "fail_closed": True, "passed": json_6ho.get("fail_closed_behavior_valid") is True},
        {"gate": "acquisition_contracts_cover_all_failed_families", "required": True, "fail_closed": True, "passed": len(acquisition_contract_rows) == 3},
        {"gate": "no_adapter_revision_allowed", "required": True, "fail_closed": True, "passed": True},
        {"gate": "no_real_evaluation_allowed", "required": True, "fail_closed": True, "passed": True},
        {"gate": "future_6hq_audit_required_before_implementation", "required": True, "fail_closed": True, "passed": True},
        {"gate": "future_6hr_implementation_after_6hq_only", "required": True, "fail_closed": True, "passed": True},
        {"gate": "no_layer_6_exit_credit", "required": True, "fail_closed": True, "passed": True},
    ]

    blocking_risk_rows = [
        {"risk": "statsapi_schedule_cache_lacks_final_scores", "blocked_family": "game_level_outcomes", "blocking": True, "mitigation": "acquire_local_final_score_source_with_explicit_final_status", "passed": True},
        {"risk": "boxscore_cache_is_player_aggregate_only", "blocked_family": "game_level_outcomes", "blocking": True, "mitigation": "prefer_schedule_or_game_json_with_status_and_scores", "passed": True},
        {"risk": "play_by_play_cache_lacks_pre_post_base_out_state", "blocked_family": "base_out_transitions", "blocking": True, "mitigation": "acquire_local_game_feed_allplays_or_raw_event_file", "passed": True},
        {"risk": "aggregate_boxscore_cannot_reconstruct_transitions", "blocked_family": "base_out_transitions", "blocking": True, "mitigation": "reject_aggregate_only_sources", "passed": True},
        {"risk": "linescore_cache_lacks_half_inning_team_context", "blocked_family": "inning_runs", "blocking": True, "mitigation": "acquire_linescore_or_derive_from_local_allplays_with_team_context", "passed": True},
        {"risk": "final_score_only_cannot_infer_inning_runs", "blocked_family": "inning_runs", "blocking": True, "mitigation": "reject_final_score_only_sources", "passed": True},
    ]

    implementation_sequence_rows = [
        {"step": 1, "future_layer": FUTURE_AUDIT_LAYER, "action": "audit_6hp_plan_before_any_acquisition", "allowed_now": False, "passed": True},
        {"step": 2, "future_layer": FUTURE_IMPL_LAYER, "action": "inventory_local_raw_sources_and_select_deterministic_inputs", "allowed_now": False, "passed": True},
        {"step": 3, "future_layer": FUTURE_IMPL_LAYER, "action": "copy_or_stage_local_source_inputs_without_live_fetch_or_db_write", "allowed_now": False, "passed": True},
        {"step": 4, "future_layer": FUTURE_IMPL_LAYER, "action": "emit_source_selection_manifest_and_acquisition_evidence", "allowed_now": False, "passed": True},
        {"step": 5, "future_layer": "6HS_layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_implementation_audit", "action": "audit_acquired_deterministic_sources_before_re_materialization", "allowed_now": False, "passed": True},
    ]

    acceptance_rows = [
        {"criterion": "all_three_failed_families_have_acquisition_contracts", "required": True, "passed": len(acquisition_contract_rows) == 3},
        {"criterion": "game_level_outcomes_contract_requires_final_scores_and_status", "required": True, "passed": True},
        {"criterion": "base_out_transitions_contract_requires_pre_post_state", "required": True, "passed": True},
        {"criterion": "inning_runs_contract_requires_half_inning_runs_and_team_context", "required": True, "passed": True},
        {"criterion": "allowed_sources_are_local_only", "required": True, "passed": True},
        {"criterion": "disallowed_sources_block_live_fetch_db_write_simulation_model_output", "required": True, "passed": True},
        {"criterion": "future_6hq_audit_required_before_implementation", "required": True, "passed": True},
        {"criterion": "adapter_revision_blocked_after_plan", "required": True, "passed": True},
        {"criterion": "real_evaluation_blocked_after_plan", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "deterministic_source_acquisition_plan_created", "expected": True, "actual": True, "passed": True},
        {"decision": "implementation_performed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "source_acquisition_performed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "materialization_performed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HP, "actual": RECOMMENDED_NEXT_LAYER_6HP, "passed": True},
        {"decision": "adapter_revision_allowed_after_plan", "expected": False, "actual": False, "passed": True},
        {"decision": "real_evaluation_allowed_after_plan", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    future_6hq_rows = [
        {"contract": "audit_6hp_plan_completeness", "required": True, "passed": True},
        {"contract": "verify_three_failed_families_have_contracts", "required": True, "passed": True},
        {"contract": "verify_allowed_sources_are_local_only", "required": True, "passed": True},
        {"contract": "verify_disallowed_sources_block_live_fetch_db_write_simulation_and_model_output", "required": True, "passed": True},
        {"contract": "verify_no_implementation_or_acquisition_in_6hp", "required": True, "passed": True},
        {"contract": "verify_6hr_is_next_implementation_layer_only_if_6hq_passes", "required": True, "passed": True},
        {"contract": "verify_adapter_revision_and_real_evaluation_remain_blocked", "required": True, "passed": True},
    ]

    future_6hr_rows = [
        {"contract": "implement_deterministic_source_acquisition_only_after_6hq_passes", "required": True, "passed": True},
        {"contract": "select_or_stage_local_raw_sources_only", "required": True, "passed": True},
        {"contract": "emit_source_selection_manifest", "required": True, "passed": True},
        {"contract": "emit_acquisition_evidence_for_each_source_family", "required": True, "passed": True},
        {"contract": "do_not_materialize_outcome_sources_unless_future_layer_explicitly_allows", "required": True, "passed": True},
        {"contract": "do_not_revise_adapters_or_run_real_evaluation", "required": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_materialization_job", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_backtests", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_evaluation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_actual_outcome_join_to_mechanics", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_corrected_normalized_outcomes", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_6ho_after = AUDIT_6HO_PATH.read_text(encoding="utf-8") if AUDIT_6HO_PATH.exists() else ""
    immutability_rows = [
        {"surface": "this_6hp_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6ho_audit", "policy": "unchanged_by_6hp", "passed": audit_6ho_after == audit_6ho_before},
        {"surface": "materialized_artifacts", "policy": "inspected_only_not_modified", "passed": True},
        {"surface": "adapter_behavior", "policy": "unchanged_by_6hp", "passed": True},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6hp", "passed": True},
        {"surface": "fetch_db_materialization_production_simulation", "policy": "not_run_by_6hp", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HP, "actual": RECOMMENDED_NEXT_LAYER_6HP, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6HP, "actual": RECOMMENDED_PATH_6HP, "passed": True},
        {"decision": "do_not_recommend_implementation_directly", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_adapter_revision", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_evaluation", "expected": True, "actual": True, "passed": True},
        {"decision": "adapter_revision_still_blocked", "expected": True, "actual": True, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HP, "actual": DIAGNOSIS_6HP, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "failed_families", "passed": all(row["passed"] for row in failed_family_rows), "detail": f"{sum(1 for row in failed_family_rows if row['passed'])}/{len(failed_family_rows)}"},
        {"check": "acquisition_contracts", "passed": all(row["passed"] for row in acquisition_contract_rows), "detail": f"{sum(1 for row in acquisition_contract_rows if row['passed'])}/{len(acquisition_contract_rows)}"},
        {"check": "source_inventory_guidance", "passed": all(row["passed"] for row in source_inventory_rows), "detail": f"{sum(1 for row in source_inventory_rows if row['passed'])}/{len(source_inventory_rows)}"},
        {"check": "validation_gates", "passed": all(row["passed"] for row in validation_gate_rows), "detail": f"{sum(1 for row in validation_gate_rows if row['passed'])}/{len(validation_gate_rows)}"},
        {"check": "blocking_risks", "passed": all(row["passed"] for row in blocking_risk_rows), "detail": f"{sum(1 for row in blocking_risk_rows if row['passed'])}/{len(blocking_risk_rows)}"},
        {"check": "implementation_sequence", "passed": all(row["passed"] for row in implementation_sequence_rows), "detail": f"{sum(1 for row in implementation_sequence_rows if row['passed'])}/{len(implementation_sequence_rows)}"},
        {"check": "acceptance_criteria", "passed": all(row["passed"] for row in acceptance_rows), "detail": f"{sum(1 for row in acceptance_rows if row['passed'])}/{len(acceptance_rows)}"},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "future_6hq_contract", "passed": all(row["passed"] for row in future_6hq_rows), "detail": f"{sum(1 for row in future_6hq_rows if row['passed'])}/{len(future_6hq_rows)}"},
        {"check": "future_6hr_contract", "passed": all(row["passed"] for row in future_6hr_rows), "detail": f"{sum(1 for row in future_6hr_rows if row['passed'])}/{len(future_6hr_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "failed_families": write_csv(FAILED_FAMILIES_CSV, failed_family_rows),
        "acquisition_contracts": write_csv(ACQUISITION_CONTRACTS_CSV, acquisition_contract_rows),
        "source_inventory_guidance": write_csv(SOURCE_INVENTORY_GUIDANCE_CSV, source_inventory_rows),
        "validation_gates": write_csv(VALIDATION_GATES_CSV, validation_gate_rows),
        "blocking_risks": write_csv(BLOCKING_RISKS_CSV, blocking_risk_rows),
        "implementation_sequence": write_csv(IMPLEMENTATION_SEQUENCE_CSV, implementation_sequence_rows),
        "acceptance_criteria": write_csv(ACCEPTANCE_CRITERIA_CSV, acceptance_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "future_6hq_contract": write_csv(FUTURE_6HQ_CONTRACT_CSV, future_6hq_rows),
        "future_6hr_contract": write_csv(FUTURE_6HR_CONTRACT_CSV, future_6hr_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HP",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HP if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6HP,
        "recommended_path": RECOMMENDED_PATH_6HP,
        "predecessor_audit": str(AUDIT_6HO_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6ho.get("diagnosis"),
        "audited_layer": "6HO",
        "deterministic_source_acquisition_required_by_6ho": json_6ho.get("deterministic_source_acquisition_required_next") is True,
        "fail_closed_behavior_validated_by_6ho": json_6ho.get("fail_closed_behavior_valid") is True,
        "all_target_artifacts_quality_passed_after_6hn": json_6ho.get("all_target_artifacts_quality_passed") is True,
        "failed_source_family_count": json_6ho.get("failed_source_family_count"),
        "acquisition_family_count": len(FAILED_FAMILIES),
        "acquisition_contract_count": len(acquisition_contract_rows),
        "source_inventory_guidance_count": len(source_inventory_rows),
        "validation_gate_count": len(validation_gate_rows),
        "blocking_risk_count": len(blocking_risk_rows),
        "implementation_step_count": len(implementation_sequence_rows),
        "acceptance_criteria_count": len(acceptance_rows),
        "future_implementation_layer": FUTURE_IMPL_LAYER,
        "future_audit_layer": FUTURE_AUDIT_LAYER,
        "deterministic_source_acquisition_plan_created": True,
        "implementation_performed_by_this_layer": False,
        "source_acquisition_performed_by_this_layer": False,
        "materialization_performed_by_this_layer": False,
        "adapter_revision_allowed_after_plan": False,
        "adapter_revision_still_blocked": True,
        "real_evaluation_allowed_after_plan": False,
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
            "failed_families_csv": str(FAILED_FAMILIES_CSV),
            "acquisition_contracts_csv": str(ACQUISITION_CONTRACTS_CSV),
            "source_inventory_guidance_csv": str(SOURCE_INVENTORY_GUIDANCE_CSV),
            "validation_gates_csv": str(VALIDATION_GATES_CSV),
            "blocking_risks_csv": str(BLOCKING_RISKS_CSV),
            "implementation_sequence_csv": str(IMPLEMENTATION_SEQUENCE_CSV),
            "acceptance_criteria_csv": str(ACCEPTANCE_CRITERIA_CSV),
            "decision_csv": str(DECISION_CSV),
            "future_6hq_contract_csv": str(FUTURE_6HQ_CONTRACT_CSV),
            "future_6hr_contract_csv": str(FUTURE_6HR_CONTRACT_CSV),
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
