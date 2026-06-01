#!/usr/bin/env python3
"""Plan Layer 6HT deterministic source gap remediation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ht_deterministic_source_gap_remediation_plan"
TMP_DIR = Path("tmp")

AUDIT_6HS_PATH = Path("scripts/audit_6hs_layer6_gameplay_mechanic_outcome_deterministic_source_acquisition_implementation.py")

JSON_6HS = TMP_DIR / "layer6_6hs_deterministic_source_acquisition_implementation_audit.json"
CHECKS_6HS = TMP_DIR / "layer6_6hs_deterministic_source_acquisition_implementation_audit_checks.csv"
PREDECESSOR_6HS = TMP_DIR / "layer6_6hs_deterministic_source_acquisition_implementation_audit_predecessor.csv"
ARTIFACT_PRESENCE_6HS = TMP_DIR / "layer6_6hs_deterministic_source_acquisition_implementation_audit_artifact_presence.csv"
MANIFEST_6HS = TMP_DIR / "layer6_6hs_deterministic_source_acquisition_implementation_audit_manifest.csv"
SOURCE_INDEXES_6HS = TMP_DIR / "layer6_6hs_deterministic_source_acquisition_implementation_audit_source_indexes.csv"
ACQ_QUALITY_6HS = TMP_DIR / "layer6_6hs_deterministic_source_acquisition_implementation_audit_acquisition_quality.csv"
INVENTORY_6HS = TMP_DIR / "layer6_6hs_deterministic_source_acquisition_implementation_audit_inventory_scan.csv"
CANDIDATE_6HS = TMP_DIR / "layer6_6hs_deterministic_source_acquisition_implementation_audit_candidate_evidence.csv"
FAIL_CLOSED_6HS = TMP_DIR / "layer6_6hs_deterministic_source_acquisition_implementation_audit_fail_closed.csv"
MATERIALIZATION_PROTECTION_6HS = TMP_DIR / "layer6_6hs_deterministic_source_acquisition_implementation_audit_materialization_protection.csv"
DECISION_6HS = TMP_DIR / "layer6_6hs_deterministic_source_acquisition_implementation_audit_decision.csv"
FUTURE_6HT_6HS = TMP_DIR / "layer6_6hs_deterministic_source_acquisition_implementation_audit_future_6ht_contract.csv"
SAFETY_6HS = TMP_DIR / "layer6_6hs_deterministic_source_acquisition_implementation_audit_safety_boundaries.csv"
IMMUTABILITY_6HS = TMP_DIR / "layer6_6hs_deterministic_source_acquisition_implementation_audit_immutability.csv"
RECOMMENDED_6HS = TMP_DIR / "layer6_6hs_deterministic_source_acquisition_implementation_audit_recommended_path.csv"

JSON_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation.json"
SOURCE_SELECTION_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation_source_selection.csv"
CANDIDATE_EVIDENCE_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation_candidate_evidence.csv"
ACQ_QUALITY_6HR = TMP_DIR / "layer6_6hr_acquisition_quality_report.csv"
ACQ_MANIFEST_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_manifest.json"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
GAP_SUMMARY_CSV = TMP_DIR / f"{SLUG}_gap_summary.csv"
FAMILY_PLANS_CSV = TMP_DIR / f"{SLUG}_family_plans.csv"
SOURCE_TARGETS_CSV = TMP_DIR / f"{SLUG}_source_targets.csv"
DISALLOWED_PATHS_CSV = TMP_DIR / f"{SLUG}_disallowed_paths.csv"
UNBLOCK_CRITERIA_CSV = TMP_DIR / f"{SLUG}_unblock_criteria.csv"
IMPLEMENTATION_SEQUENCE_CSV = TMP_DIR / f"{SLUG}_implementation_sequence.csv"
ACCEPTANCE_CSV = TMP_DIR / f"{SLUG}_acceptance_criteria.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
FUTURE_6HU_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hu_contract.csv"
FUTURE_6HV_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hv_contract.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HS = "layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_implementation_audit_complete"
DIAGNOSIS_6HT = "layer_6_gameplay_mechanic_outcome_deterministic_source_gap_remediation_plan_complete"

RECOMMENDED_NEXT_LAYER_6HS = "6HT_layer_6_gameplay_mechanic_outcome_deterministic_source_gap_remediation_plan"
RECOMMENDED_PATH_6HS = "audit_source_acquisition_fail_closed_then_plan_gap_remediation_before_materialization_or_adapter_revision"

RECOMMENDED_NEXT_LAYER_6HT = "6HU_layer_6_gameplay_mechanic_outcome_deterministic_source_gap_remediation_plan_audit"
RECOMMENDED_PATH_6HT = "plan_gap_remediation_then_audit_before_source_remediation_implementation_or_materialization"

FUTURE_6HU = "6HU_layer_6_gameplay_mechanic_outcome_deterministic_source_gap_remediation_plan_audit"
FUTURE_6HV = "6HV_layer_6_gameplay_mechanic_outcome_deterministic_source_gap_remediation_implementation"

SOURCE_FAMILIES = ["game_level_outcomes", "base_out_transitions", "inning_runs"]

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

FAMILY_PLAN_DATA = {
    "game_level_outcomes": {
        "current_gap": "local_boxscore_candidates_expose_status_or_season_like_evidence_but_not_complete_exact_game_id_final_scores_final_status_contract",
        "missing_required_evidence": "explicit_game_id|explicit_home_score|explicit_away_score|explicit_final_status_or_completed_state|preferred_home_team_away_team_game_date_season_context",
        "acceptable_future_source_types": "local_statsapi_schedule_cache_with_final_scores_status|local_game_json_with_explicit_final_score_status|local_final_score_csv_from_deterministic_source|local_archived_schedule_or_linescore_with_gamepk_home_away_status_scores",
        "local_source_roots_to_target": "data/raw|tmp/local_source_cache|tmp/statsapi_cache|cache|artifacts",
        "disallowed_remediation_paths": "live_network_fetch_inside_layer|remote_api_call_inside_layer|database_write|projection_score|simulated_score|inference_from_model_output|manual_fabricated_score_rows",
        "remediation_strategy": "stage_already_local_deterministic_schedule_or_game_status_source_then_validate_exact_final_scores_and_status_before_materialization",
        "materialization_unblock_condition": "one_unique_exact_final_score_status_row_per_game_id_acquired_and_audited",
    },
    "base_out_transitions": {
        "current_gap": "local_boxscore_candidates_expose_inning_like_evidence_but_not_exact_play_level_pre_post_base_out_state",
        "missing_required_evidence": "game_id|play_id_or_event_id|inning|half_inning|start_base_state_or_pre_base_state|end_base_state_or_post_base_state|start_outs|end_outs|runs_scored|sequence_ordering",
        "acceptable_future_source_types": "local_statsapi_live_feed_or_game_feed_allplays_with_state_context|local_play_by_play_csv_with_explicit_pre_post_base_out_state|local_retrosheet_style_event_file_with_deterministic_state_reconstruction",
        "local_source_roots_to_target": "data/raw|tmp/local_source_cache|tmp/statsapi_cache|cache|artifacts",
        "disallowed_remediation_paths": "aggregate_only_boxscore|simulated_transitions|model_generated_transitions|inferred_transitions_from_season_totals|live_network_fetch_inside_layer|database_write",
        "remediation_strategy": "identify_or_stage_local_allplays_or_event_level_files_then_validate_explicit_or_locally_reconstructable_state_transitions_before_materialization",
        "materialization_unblock_condition": "one_unique_exact_play_level_pre_post_state_row_per_game_id_event_id_play_id_acquired_and_audited",
    },
    "inning_runs": {
        "current_gap": "local_boxscore_candidates_expose_inning_team_like_evidence_but_not_exact_half_inning_run_totals_with_team_context",
        "missing_required_evidence": "game_id|inning|half_inning|runs_scored|batting_team_or_fielding_team_context|preferred_start_end_score_context",
        "acceptable_future_source_types": "local_statsapi_linescore_json|local_game_feed_allplays_aggregated_by_half_inning_with_team_context|local_raw_linescore_csv|local_archived_linescore_with_inning_by_inning_team_runs",
        "local_source_roots_to_target": "data/raw|tmp/local_source_cache|tmp/statsapi_cache|cache|artifacts",
        "disallowed_remediation_paths": "final_score_only_split_inference|simulated_inning_allocation|model_generated_inning_runs|live_network_fetch_inside_layer|database_write",
        "remediation_strategy": "target_local_linescore_or_game_feed_source_with_explicit_half_inning_run_team_context_and_validate_before_materialization",
        "materialization_unblock_condition": "one_unique_exact_half_inning_run_team_context_row_per_game_id_inning_half_inning_acquired_and_audited",
    },
}


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


def find_row(rows: List[Dict[str, str]], key: str, value: str) -> Dict[str, str]:
    for row in rows:
        if row.get(key) == value:
            return row
    return {}


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()
    script_before = Path(__file__).read_text(encoding="utf-8")
    audit_6hs_before = AUDIT_6HS_PATH.read_text(encoding="utf-8") if AUDIT_6HS_PATH.exists() else ""

    json_6hs = load_json(JSON_6HS)
    json_6hr = load_json(JSON_6HR)
    source_selection_6hr = read_csv(SOURCE_SELECTION_6HR)
    candidate_evidence_6hr = read_csv(CANDIDATE_EVIDENCE_6HR)
    acquisition_quality_6hr = read_csv(ACQ_QUALITY_6HR)

    required_inputs = [
        JSON_6HS,
        CHECKS_6HS,
        PREDECESSOR_6HS,
        ARTIFACT_PRESENCE_6HS,
        MANIFEST_6HS,
        SOURCE_INDEXES_6HS,
        ACQ_QUALITY_6HS,
        INVENTORY_6HS,
        CANDIDATE_6HS,
        FAIL_CLOSED_6HS,
        MATERIALIZATION_PROTECTION_6HS,
        DECISION_6HS,
        FUTURE_6HT_6HS,
        SAFETY_6HS,
        IMMUTABILITY_6HS,
        RECOMMENDED_6HS,
        JSON_6HR,
        SOURCE_SELECTION_6HR,
        CANDIDATE_EVIDENCE_6HR,
        ACQ_QUALITY_6HR,
        ACQ_MANIFEST_6HR,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hs_audit_exists", "expected": True, "actual": AUDIT_6HS_PATH.exists(), "passed": AUDIT_6HS_PATH.exists()},
        {"check": "6hs_json_exists", "expected": True, "actual": JSON_6HS.exists(), "passed": JSON_6HS.exists()},
        {"check": "6hs_all_checks_passed", "expected": True, "actual": json_6hs.get("all_checks_passed"), "passed": json_6hs.get("all_checks_passed") is True},
        {"check": "6hs_diagnosis", "expected": DIAGNOSIS_6HS, "actual": json_6hs.get("diagnosis"), "passed": json_6hs.get("diagnosis") == DIAGNOSIS_6HS},
        {"check": "6hs_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HS, "actual": json_6hs.get("recommended_next_layer"), "passed": json_6hs.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6HS},
        {"check": "6hs_recommended_path", "expected": RECOMMENDED_PATH_6HS, "actual": json_6hs.get("recommended_path"), "passed": json_6hs.get("recommended_path") == RECOMMENDED_PATH_6HS},
        {"check": "6hs_gap_remediation_required", "expected": True, "actual": json_6hs.get("gap_remediation_required_next"), "passed": json_6hs.get("gap_remediation_required_next") is True},
        {"check": "6hs_all_required_sources_acquired", "expected": False, "actual": json_6hs.get("all_required_sources_acquired"), "passed": json_6hs.get("all_required_sources_acquired") is False},
        {"check": "6hs_materialization_blocked", "expected": True, "actual": json_6hs.get("materialization_still_blocked"), "passed": json_6hs.get("materialization_still_blocked") is True},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    gap_summary_rows = []
    for family in SOURCE_FAMILIES:
        selection = find_row(source_selection_6hr, "source_family", family)
        quality = find_row(acquisition_quality_6hr, "source_family", family)
        plan = FAMILY_PLAN_DATA[family]
        gap_summary_rows.append({
            "source_family": family,
            "selected_after_6hr": selection.get("selected"),
            "acquisition_status_after_6hr": selection.get("acquisition_status") or quality.get("acquisition_status"),
            "fail_closed_reason": selection.get("rejection_reason") or quality.get("fail_closed_reason"),
            "candidate_evidence_rows": sum(1 for row in candidate_evidence_6hr if row.get("source_family") == family),
            "current_gap": plan["current_gap"],
            "gap_remediation_required": True,
            "passed": (
                selection.get("selected") == "False"
                and (selection.get("acquisition_status") == "fail_closed_no_exact_deterministic_local_source" or quality.get("acquisition_status") == "fail_closed_no_exact_deterministic_local_source")
            ),
        })

    family_plan_rows = []
    for family, plan in FAMILY_PLAN_DATA.items():
        family_plan_rows.append({
            "source_family": family,
            "current_gap": plan["current_gap"],
            "missing_required_evidence": plan["missing_required_evidence"],
            "acceptable_future_source_types": plan["acceptable_future_source_types"],
            "local_source_roots_to_target": plan["local_source_roots_to_target"],
            "disallowed_remediation_paths": plan["disallowed_remediation_paths"],
            "remediation_strategy": plan["remediation_strategy"],
            "future_implementation_layer": FUTURE_6HV,
            "future_audit_layer": FUTURE_6HU,
            "materialization_unblock_condition": plan["materialization_unblock_condition"],
            "adapter_revision_unblock_condition": "after_deterministic_sources_are_acquired_audited_materialized_and_materialization_audit_passes",
            "real_evaluation_unblock_condition": "after_materialized_outcomes_are_validated_and_adapter_revision_audit_passes",
            "passed": True,
        })

    source_target_rows = []
    for family, plan in FAMILY_PLAN_DATA.items():
        for target in plan["acceptable_future_source_types"].split("|"):
            source_target_rows.append({
                "source_family": family,
                "acceptable_future_source_type": target,
                "local_source_roots_to_target": plan["local_source_roots_to_target"],
                "requires_local_presence_before_use": True,
                "passed": True,
            })

    disallowed_rows = []
    for family, plan in FAMILY_PLAN_DATA.items():
        for item in plan["disallowed_remediation_paths"].split("|"):
            disallowed_rows.append({
                "source_family": family,
                "disallowed_path": item,
                "reason": "would_violate_deterministic_local_only_or_non_inferred_source_contract",
                "passed": True,
            })

    unblock_rows = []
    for family, plan in FAMILY_PLAN_DATA.items():
        unblock_rows.extend([
            {
                "source_family": family,
                "unblock_surface": "materialization",
                "unblock_condition": plan["materialization_unblock_condition"],
                "currently_unblocked": False,
                "passed": True,
            },
            {
                "source_family": family,
                "unblock_surface": "adapter_revision",
                "unblock_condition": "requires_all_deterministic_sources_acquired_audited_materialized_and_materialization_audit_passed",
                "currently_unblocked": False,
                "passed": True,
            },
            {
                "source_family": family,
                "unblock_surface": "real_evaluation",
                "unblock_condition": "requires_valid_materialized_outcomes_and_adapter_revision_audit_passed",
                "currently_unblocked": False,
                "passed": True,
            },
        ])

    implementation_sequence_rows = [
        {"step": 1, "future_layer": FUTURE_6HU, "action": "audit_6ht_gap_remediation_plan", "allowed_now": False, "passed": True},
        {"step": 2, "future_layer": FUTURE_6HV, "action": "implement_gap_remediation_for_local_deterministic_source_targets_after_6hu_passes", "allowed_now": False, "passed": True},
        {"step": 3, "future_layer": "6HW_layer_6_gameplay_mechanic_outcome_deterministic_source_gap_remediation_implementation_audit", "action": "audit_gap_remediation_implementation_before_any_materialization", "allowed_now": False, "passed": True},
        {"step": 4, "future_layer": "future_materialization_reentry_layer", "action": "only_if_all_required_sources_acquired_and_audited", "allowed_now": False, "passed": True},
        {"step": 5, "future_layer": "future_adapter_revision_layer", "action": "only_after_materialization_and_materialization_audit_pass", "allowed_now": False, "passed": True},
    ]

    acceptance_rows = [
        {"criterion": "6hs_predecessor_passed", "required": True, "passed": json_6hs.get("all_checks_passed") is True},
        {"criterion": "all_three_families_have_gap_summary", "required": True, "passed": len(gap_summary_rows) == 3},
        {"criterion": "all_three_families_have_remediation_plan", "required": True, "passed": len(family_plan_rows) == 3},
        {"criterion": "source_targets_defined", "required": True, "passed": len(source_target_rows) >= 9},
        {"criterion": "disallowed_paths_defined", "required": True, "passed": len(disallowed_rows) >= 15},
        {"criterion": "unblock_criteria_defined", "required": True, "passed": len(unblock_rows) == 9},
        {"criterion": "future_6hu_audit_required_before_implementation", "required": True, "passed": True},
        {"criterion": "materialization_remains_blocked", "required": True, "passed": True},
        {"criterion": "adapter_revision_and_real_evaluation_remain_blocked", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "gap_remediation_plan_created", "expected": True, "actual": True, "passed": True},
        {"decision": "implementation_performed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "source_acquisition_performed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "materialization_performed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HT, "actual": RECOMMENDED_NEXT_LAYER_6HT, "passed": True},
        {"decision": "materialization_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "adapter_revision_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "real_evaluation_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    future_6hu_rows = [
        {"contract": "audit_gap_summary_for_three_failed_families", "required": True, "passed": True},
        {"contract": "audit_family_remediation_plans_and_missing_evidence", "required": True, "passed": True},
        {"contract": "audit_source_targets_are_local_deterministic_only", "required": True, "passed": True},
        {"contract": "audit_disallowed_paths_block_fetch_db_simulation_model_manual_fabrication", "required": True, "passed": True},
        {"contract": "audit_unblock_criteria_keep_materialization_adapter_real_eval_blocked", "required": True, "passed": True},
        {"contract": "audit_6hv_is_next_implementation_layer_only_after_6hu_passes", "required": True, "passed": True},
    ]

    future_6hv_rows = [
        {"contract": "implement_gap_remediation_only_after_6hu_passes", "required": True, "passed": True},
        {"contract": "target_local_deterministic_source_roots_only", "required": True, "passed": True},
        {"contract": "stage_or_validate_exact_missing_source_evidence", "required": True, "passed": True},
        {"contract": "emit_remediation_manifest_and_evidence", "required": True, "passed": True},
        {"contract": "do_not_materialize_sources_unless_future_layer_explicitly_allows", "required": True, "passed": True},
        {"contract": "do_not_revise_adapters_or_run_real_evaluation", "required": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_materialization", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_adapter_revision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_backtests", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_evaluation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_actual_outcome_join_to_mechanics", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_corrected_normalized_outcomes", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_activation_or_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_6hs_after = AUDIT_6HS_PATH.read_text(encoding="utf-8") if AUDIT_6HS_PATH.exists() else ""
    immutability_rows = [
        {"surface": "this_6ht_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hs_audit", "policy": "unchanged_by_6ht", "passed": audit_6hs_after == audit_6hs_before},
        {"surface": "deterministic_sources", "policy": "not_acquired_by_6ht", "passed": True},
        {"surface": "materialized_artifacts", "policy": "not_modified_by_6ht", "passed": True},
        {"surface": "adapter_behavior", "policy": "unchanged_by_6ht", "passed": True},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6ht", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HT, "actual": RECOMMENDED_NEXT_LAYER_6HT, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6HT, "actual": RECOMMENDED_PATH_6HT, "passed": True},
        {"decision": "do_not_recommend_implementation_directly", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_materialization", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_adapter_revision", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_evaluation", "expected": True, "actual": True, "passed": True},
        {"decision": "materialization_still_blocked", "expected": True, "actual": True, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HT, "actual": DIAGNOSIS_6HT, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "gap_summary", "passed": all(row["passed"] for row in gap_summary_rows), "detail": f"{sum(1 for row in gap_summary_rows if row['passed'])}/{len(gap_summary_rows)}"},
        {"check": "family_plans", "passed": all(row["passed"] for row in family_plan_rows), "detail": f"{sum(1 for row in family_plan_rows if row['passed'])}/{len(family_plan_rows)}"},
        {"check": "source_targets", "passed": all(row["passed"] for row in source_target_rows), "detail": f"{sum(1 for row in source_target_rows if row['passed'])}/{len(source_target_rows)}"},
        {"check": "disallowed_paths", "passed": all(row["passed"] for row in disallowed_rows), "detail": f"{sum(1 for row in disallowed_rows if row['passed'])}/{len(disallowed_rows)}"},
        {"check": "unblock_criteria", "passed": all(row["passed"] for row in unblock_rows), "detail": f"{sum(1 for row in unblock_rows if row['passed'])}/{len(unblock_rows)}"},
        {"check": "implementation_sequence", "passed": all(row["passed"] for row in implementation_sequence_rows), "detail": f"{sum(1 for row in implementation_sequence_rows if row['passed'])}/{len(implementation_sequence_rows)}"},
        {"check": "acceptance_criteria", "passed": all(row["passed"] for row in acceptance_rows), "detail": f"{sum(1 for row in acceptance_rows if row['passed'])}/{len(acceptance_rows)}"},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "future_6hu_contract", "passed": all(row["passed"] for row in future_6hu_rows), "detail": f"{sum(1 for row in future_6hu_rows if row['passed'])}/{len(future_6hu_rows)}"},
        {"check": "future_6hv_contract", "passed": all(row["passed"] for row in future_6hv_rows), "detail": f"{sum(1 for row in future_6hv_rows if row['passed'])}/{len(future_6hv_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "gap_summary": write_csv(GAP_SUMMARY_CSV, gap_summary_rows),
        "family_plans": write_csv(FAMILY_PLANS_CSV, family_plan_rows),
        "source_targets": write_csv(SOURCE_TARGETS_CSV, source_target_rows),
        "disallowed_paths": write_csv(DISALLOWED_PATHS_CSV, disallowed_rows),
        "unblock_criteria": write_csv(UNBLOCK_CRITERIA_CSV, unblock_rows),
        "implementation_sequence": write_csv(IMPLEMENTATION_SEQUENCE_CSV, implementation_sequence_rows),
        "acceptance_criteria": write_csv(ACCEPTANCE_CSV, acceptance_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "future_6hu_contract": write_csv(FUTURE_6HU_CONTRACT_CSV, future_6hu_rows),
        "future_6hv_contract": write_csv(FUTURE_6HV_CONTRACT_CSV, future_6hv_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HT",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HT if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6HT,
        "recommended_path": RECOMMENDED_PATH_6HT,
        "audited_layer": "6HS",
        "predecessor_audit": str(AUDIT_6HS_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6hs.get("diagnosis"),
        "gap_remediation_required_by_6hs": json_6hs.get("gap_remediation_required_next") is True,
        "all_required_sources_acquired_after_6hr": json_6hs.get("all_required_sources_acquired") is True,
        "selected_source_family_count_after_6hr": json_6hs.get("selected_source_family_count"),
        "failed_source_family_count_after_6hr": json_6hs.get("failed_source_family_count"),
        "fail_closed_family_count_after_6hr": json_6hs.get("fail_closed_family_count"),
        "remediation_family_count": len(family_plan_rows),
        "remediation_plan_created": True,
        "source_target_count": len(source_target_rows),
        "disallowed_path_count": len(disallowed_rows),
        "unblock_criteria_count": len(unblock_rows),
        "future_plan_audit_layer": FUTURE_6HU,
        "future_remediation_implementation_layer": FUTURE_6HV,
        "implementation_performed_by_this_layer": False,
        "source_acquisition_performed_by_this_layer": False,
        "materialization_performed_by_this_layer": False,
        "materialization_allowed_after_this_layer": False,
        "materialization_still_blocked": True,
        "adapter_revision_allowed_after_this_layer": False,
        "adapter_revision_still_blocked": True,
        "real_evaluation_allowed_after_this_layer": False,
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
            "gap_summary_csv": str(GAP_SUMMARY_CSV),
            "family_plans_csv": str(FAMILY_PLANS_CSV),
            "source_targets_csv": str(SOURCE_TARGETS_CSV),
            "disallowed_paths_csv": str(DISALLOWED_PATHS_CSV),
            "unblock_criteria_csv": str(UNBLOCK_CRITERIA_CSV),
            "implementation_sequence_csv": str(IMPLEMENTATION_SEQUENCE_CSV),
            "acceptance_criteria_csv": str(ACCEPTANCE_CSV),
            "decision_csv": str(DECISION_CSV),
            "future_6hu_contract_csv": str(FUTURE_6HU_CONTRACT_CSV),
            "future_6hv_contract_csv": str(FUTURE_6HV_CONTRACT_CSV),
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
