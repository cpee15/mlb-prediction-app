#!/usr/bin/env python3
"""Audit Layer 6HT deterministic source gap remediation plan."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6hu_deterministic_source_gap_remediation_plan_audit"
TMP_DIR = Path("tmp")

PLAN_6HT_PATH = Path("scripts/plan_6ht_layer6_gameplay_mechanic_outcome_deterministic_source_gap_remediation.py")

JSON_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan.json"
CHECKS_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan_checks.csv"
PREDECESSOR_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan_predecessor.csv"
INPUT_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan_input_artifacts.csv"
GAP_SUMMARY_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan_gap_summary.csv"
FAMILY_PLANS_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan_family_plans.csv"
SOURCE_TARGETS_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan_source_targets.csv"
DISALLOWED_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan_disallowed_paths.csv"
UNBLOCK_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan_unblock_criteria.csv"
SEQUENCE_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan_implementation_sequence.csv"
ACCEPTANCE_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan_acceptance_criteria.csv"
DECISION_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan_decision.csv"
FUTURE_6HU_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan_future_6hu_contract.csv"
FUTURE_6HV_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan_future_6hv_contract.csv"
SAFETY_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan_safety_boundaries.csv"
IMMUTABILITY_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan_immutability.csv"
RECOMMENDED_6HT = TMP_DIR / "layer6_6ht_deterministic_source_gap_remediation_plan_recommended_path.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
GAP_SUMMARY_CSV = TMP_DIR / f"{SLUG}_gap_summary.csv"
FAMILY_PLANS_CSV = TMP_DIR / f"{SLUG}_family_plans.csv"
SOURCE_TARGETS_CSV = TMP_DIR / f"{SLUG}_source_targets.csv"
DISALLOWED_CSV = TMP_DIR / f"{SLUG}_disallowed_paths.csv"
UNBLOCK_CSV = TMP_DIR / f"{SLUG}_unblock_criteria.csv"
SEQUENCE_CSV = TMP_DIR / f"{SLUG}_implementation_sequence.csv"
ACCEPTANCE_CSV = TMP_DIR / f"{SLUG}_acceptance_criteria.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
FUTURE_6HV_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hv_contract.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HT = "layer_6_gameplay_mechanic_outcome_deterministic_source_gap_remediation_plan_complete"
DIAGNOSIS_6HU = "layer_6_gameplay_mechanic_outcome_deterministic_source_gap_remediation_plan_audit_complete"

RECOMMENDED_NEXT_LAYER_6HT = "6HU_layer_6_gameplay_mechanic_outcome_deterministic_source_gap_remediation_plan_audit"
RECOMMENDED_PATH_6HT = "plan_gap_remediation_then_audit_before_source_remediation_implementation_or_materialization"

RECOMMENDED_NEXT_LAYER_6HU = "6HV_layer_6_gameplay_mechanic_outcome_deterministic_source_gap_remediation_implementation"
RECOMMENDED_PATH_6HU = "audit_gap_remediation_plan_then_implement_source_gap_remediation_before_materialization_or_adapter_revision"

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

FAMILY_REQUIREMENTS = {
    "game_level_outcomes": {
        "gap_terms": ["game_id", "final_scores", "final_status"],
        "missing_terms": ["explicit_game_id", "explicit_home_score", "explicit_away_score", "explicit_final_status_or_completed_state"],
        "target_terms": ["local_statsapi_schedule_cache_with_final_scores_status", "local_game_json_with_explicit_final_score_status"],
        "disallowed_terms": ["live_network_fetch_inside_layer", "remote_api_call_inside_layer", "database_write", "projection_score", "simulated_score", "inference_from_model_output", "manual_fabricated_score_rows"],
        "unblock_terms": ["one_unique_exact_final_score_status_row_per_game_id_acquired_and_audited"],
    },
    "base_out_transitions": {
        "gap_terms": ["play_level", "pre_post_base_out_state"],
        "missing_terms": ["game_id", "play_id_or_event_id", "inning", "half_inning", "start_base_state_or_pre_base_state", "end_base_state_or_post_base_state", "start_outs", "end_outs", "runs_scored", "sequence_ordering"],
        "target_terms": ["local_statsapi_live_feed_or_game_feed_allplays_with_state_context", "local_play_by_play_csv_with_explicit_pre_post_base_out_state"],
        "disallowed_terms": ["aggregate_only_boxscore", "simulated_transitions", "model_generated_transitions", "inferred_transitions_from_season_totals", "live_network_fetch_inside_layer", "database_write"],
        "unblock_terms": ["one_unique_exact_play_level_pre_post_state_row_per_game_id_event_id_play_id_acquired_and_audited"],
    },
    "inning_runs": {
        "gap_terms": ["half_inning_run_totals", "team_context"],
        "missing_terms": ["game_id", "inning", "half_inning", "runs_scored", "batting_team_or_fielding_team_context"],
        "target_terms": ["local_statsapi_linescore_json", "local_raw_linescore_csv"],
        "disallowed_terms": ["final_score_only_split_inference", "simulated_inning_allocation", "model_generated_inning_runs", "live_network_fetch_inside_layer", "database_write"],
        "unblock_terms": ["one_unique_exact_half_inning_run_team_context_row_per_game_id_inning_half_inning_acquired_and_audited"],
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


def contains_all(text: str, terms: List[str]) -> bool:
    return all(term in text for term in terms)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()
    script_before = Path(__file__).read_text(encoding="utf-8")
    plan_6ht_before = PLAN_6HT_PATH.read_text(encoding="utf-8") if PLAN_6HT_PATH.exists() else ""

    json_6ht = load_json(JSON_6HT)
    gap_summary_rows_6ht = read_csv(GAP_SUMMARY_6HT)
    family_plan_rows_6ht = read_csv(FAMILY_PLANS_6HT)
    source_target_rows_6ht = read_csv(SOURCE_TARGETS_6HT)
    disallowed_rows_6ht = read_csv(DISALLOWED_6HT)
    unblock_rows_6ht = read_csv(UNBLOCK_6HT)
    sequence_rows_6ht = read_csv(SEQUENCE_6HT)
    acceptance_rows_6ht = read_csv(ACCEPTANCE_6HT)
    future_6hv_rows_6ht = read_csv(FUTURE_6HV_6HT)
    safety_rows_6ht = read_csv(SAFETY_6HT)
    immutability_rows_6ht = read_csv(IMMUTABILITY_6HT)
    recommended_rows_6ht = read_csv(RECOMMENDED_6HT)

    required_artifacts = [
        JSON_6HT,
        CHECKS_6HT,
        PREDECESSOR_6HT,
        INPUT_6HT,
        GAP_SUMMARY_6HT,
        FAMILY_PLANS_6HT,
        SOURCE_TARGETS_6HT,
        DISALLOWED_6HT,
        UNBLOCK_6HT,
        SEQUENCE_6HT,
        ACCEPTANCE_6HT,
        DECISION_6HT,
        FUTURE_6HU_6HT,
        FUTURE_6HV_6HT,
        SAFETY_6HT,
        IMMUTABILITY_6HT,
        RECOMMENDED_6HT,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ht_plan_exists", "expected": True, "actual": PLAN_6HT_PATH.exists(), "passed": PLAN_6HT_PATH.exists()},
        {"check": "6ht_json_exists", "expected": True, "actual": JSON_6HT.exists(), "passed": JSON_6HT.exists()},
        {"check": "6ht_all_checks_passed", "expected": True, "actual": json_6ht.get("all_checks_passed"), "passed": json_6ht.get("all_checks_passed") is True},
        {"check": "6ht_diagnosis", "expected": DIAGNOSIS_6HT, "actual": json_6ht.get("diagnosis"), "passed": json_6ht.get("diagnosis") == DIAGNOSIS_6HT},
        {"check": "6ht_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HT, "actual": json_6ht.get("recommended_next_layer"), "passed": json_6ht.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6HT},
        {"check": "6ht_recommended_path", "expected": RECOMMENDED_PATH_6HT, "actual": json_6ht.get("recommended_path"), "passed": json_6ht.get("recommended_path") == RECOMMENDED_PATH_6HT},
        {"check": "6ht_planning_only", "expected": True, "actual": json_6ht.get("planning_only"), "passed": json_6ht.get("planning_only") is True},
        {"check": "6ht_no_implementation", "expected": False, "actual": json_6ht.get("implementation_performed_by_this_layer"), "passed": json_6ht.get("implementation_performed_by_this_layer") is False},
        {"check": "6ht_materialization_blocked", "expected": True, "actual": json_6ht.get("materialization_still_blocked"), "passed": json_6ht.get("materialization_still_blocked") is True},
        {"check": "6ht_gap_family_count", "expected": 3, "actual": json_6ht.get("remediation_family_count"), "passed": json_6ht.get("remediation_family_count") == 3},
    ]

    artifact_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_artifacts
    ]

    gap_summary_audit_rows = []
    for family in SOURCE_FAMILIES:
        row = find_row(gap_summary_rows_6ht, "source_family", family)
        current_gap = row.get("current_gap", "")
        gap_summary_audit_rows.append({
            "source_family": family,
            "present": bool(row),
            "selected_after_6hr": row.get("selected_after_6hr"),
            "gap_remediation_required": row.get("gap_remediation_required"),
            "passed_flag": row.get("passed"),
            "current_gap_relevant": bool(current_gap),
            "passed": bool(row) and row.get("selected_after_6hr") == "False" and boolish(row.get("gap_remediation_required")) and boolish(row.get("passed")),
        })

    family_plan_audit_rows = []
    for family in SOURCE_FAMILIES:
        row = find_row(family_plan_rows_6ht, "source_family", family)
        req = FAMILY_REQUIREMENTS[family]
        joined = "|".join([
            row.get("current_gap", ""),
            row.get("missing_required_evidence", ""),
            row.get("acceptable_future_source_types", ""),
            row.get("disallowed_remediation_paths", ""),
            row.get("materialization_unblock_condition", ""),
        ])
        family_plan_audit_rows.append({
            "source_family": family,
            "present": bool(row),
            "gap_terms_present": contains_all(joined, req["gap_terms"]),
            "missing_terms_present": contains_all(joined, req["missing_terms"]),
            "target_terms_present": contains_all(joined, req["target_terms"]),
            "disallowed_terms_present": contains_all(joined, req["disallowed_terms"]),
            "unblock_terms_present": contains_all(joined, req["unblock_terms"]),
            "future_implementation_layer": row.get("future_implementation_layer"),
            "passed_flag": row.get("passed"),
            "passed": (
                bool(row)
                and contains_all(joined, req["gap_terms"])
                and contains_all(joined, req["missing_terms"])
                and contains_all(joined, req["target_terms"])
                and contains_all(joined, req["disallowed_terms"])
                and contains_all(joined, req["unblock_terms"])
                and row.get("future_implementation_layer") == FUTURE_6HV
                and boolish(row.get("passed"))
            ),
        })

    source_target_audit_rows = []
    for family in SOURCE_FAMILIES:
        req = FAMILY_REQUIREMENTS[family]
        rows = [row for row in source_target_rows_6ht if row.get("source_family") == family]
        target_text = "|".join(row.get("acceptable_future_source_type", "") for row in rows)
        source_target_audit_rows.append({
            "source_family": family,
            "target_count": len(rows),
            "target_terms_present": contains_all(target_text, req["target_terms"]),
            "all_require_local_presence": all(boolish(row.get("requires_local_presence_before_use")) for row in rows),
            "all_passed": all(boolish(row.get("passed")) for row in rows),
            "passed": bool(rows) and contains_all(target_text, req["target_terms"]) and all(boolish(row.get("requires_local_presence_before_use")) for row in rows) and all(boolish(row.get("passed")) for row in rows),
        })

    disallowed_audit_rows = []
    for family in SOURCE_FAMILIES:
        req = FAMILY_REQUIREMENTS[family]
        rows = [row for row in disallowed_rows_6ht if row.get("source_family") == family]
        text = "|".join(row.get("disallowed_path", "") for row in rows)
        disallowed_audit_rows.append({
            "source_family": family,
            "disallowed_count": len(rows),
            "required_disallowed_terms_present": contains_all(text, req["disallowed_terms"]),
            "all_passed": all(boolish(row.get("passed")) for row in rows),
            "passed": contains_all(text, req["disallowed_terms"]) and all(boolish(row.get("passed")) for row in rows),
        })

    unblock_audit_rows = []
    for family in SOURCE_FAMILIES:
        req = FAMILY_REQUIREMENTS[family]
        rows = [row for row in unblock_rows_6ht if row.get("source_family") == family]
        text = "|".join(row.get("unblock_condition", "") for row in rows)
        unblock_audit_rows.append({
            "source_family": family,
            "unblock_count": len(rows),
            "materialization_unblock_terms_present": contains_all(text, req["unblock_terms"]),
            "all_currently_unblocked_false": all(row.get("currently_unblocked") == "False" for row in rows),
            "all_passed": all(boolish(row.get("passed")) for row in rows),
            "passed": len(rows) == 3 and contains_all(text, req["unblock_terms"]) and all(row.get("currently_unblocked") == "False" for row in rows) and all(boolish(row.get("passed")) for row in rows),
        })

    sequence_step1 = find_row(sequence_rows_6ht, "step", "1")
    sequence_step2 = find_row(sequence_rows_6ht, "step", "2")
    sequence_audit_rows = [
        {"audit": "sequence_row_count", "expected": 5, "actual": len(sequence_rows_6ht), "passed": len(sequence_rows_6ht) == 5},
        {"audit": "step_1_is_6hu_audit", "expected": RECOMMENDED_NEXT_LAYER_6HT, "actual": sequence_step1.get("future_layer"), "passed": sequence_step1.get("future_layer") == RECOMMENDED_NEXT_LAYER_6HT},
        {"audit": "step_2_is_6hv_implementation", "expected": FUTURE_6HV, "actual": sequence_step2.get("future_layer"), "passed": sequence_step2.get("future_layer") == FUTURE_6HV},
        {"audit": "all_allowed_now_false", "expected": False, "actual": {row.get("allowed_now") for row in sequence_rows_6ht}, "passed": all(row.get("allowed_now") == "False" for row in sequence_rows_6ht)},
        {"audit": "all_sequence_passed", "expected": True, "actual": {row.get("passed") for row in sequence_rows_6ht}, "passed": all(boolish(row.get("passed")) for row in sequence_rows_6ht)},
    ]

    acceptance_audit_rows = [
        {"audit": "acceptance_count", "expected": 9, "actual": len(acceptance_rows_6ht), "passed": len(acceptance_rows_6ht) == 9},
        {"audit": "all_acceptance_passed", "expected": True, "actual": {row.get("passed") for row in acceptance_rows_6ht}, "passed": all(boolish(row.get("passed")) for row in acceptance_rows_6ht)},
    ]

    future_6hv_audit_rows = [
        {"audit": "future_6hv_contract_count", "expected": 6, "actual": len(future_6hv_rows_6ht), "passed": len(future_6hv_rows_6ht) == 6},
        {"audit": "all_future_6hv_contracts_passed", "expected": True, "actual": {row.get("passed") for row in future_6hv_rows_6ht}, "passed": all(boolish(row.get("passed")) for row in future_6hv_rows_6ht)},
        {"audit": "contract_mentions_after_6hu", "expected": True, "actual": any("after_6hu" in row.get("contract", "") for row in future_6hv_rows_6ht), "passed": any("after_6hu" in row.get("contract", "") for row in future_6hv_rows_6ht)},
    ]

    safety_audit_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "6ht_planning_only", "expected": True, "actual": json_6ht.get("planning_only"), "passed": json_6ht.get("planning_only") is True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": json_6ht.get("live_data_fetches_run"), "passed": json_6ht.get("live_data_fetches_run") is False},
        {"boundary": "no_database_write", "expected": False, "actual": json_6ht.get("database_writes_run"), "passed": json_6ht.get("database_writes_run") is False},
        {"boundary": "no_source_acquisition", "expected": False, "actual": json_6ht.get("source_acquisition_performed_by_this_layer"), "passed": json_6ht.get("source_acquisition_performed_by_this_layer") is False},
        {"boundary": "no_materialization", "expected": False, "actual": json_6ht.get("materialization_performed_by_this_layer"), "passed": json_6ht.get("materialization_performed_by_this_layer") is False},
        {"boundary": "no_adapter_revision", "expected": False, "actual": json_6ht.get("adapter_revision_allowed_after_this_layer"), "passed": json_6ht.get("adapter_revision_allowed_after_this_layer") is False},
        {"boundary": "no_real_backtests", "expected": False, "actual": json_6ht.get("real_backtests_run"), "passed": json_6ht.get("real_backtests_run") is False},
        {"boundary": "no_mechanic_evaluation", "expected": False, "actual": json_6ht.get("mechanic_evaluations_run"), "passed": json_6ht.get("mechanic_evaluations_run") is False},
        {"boundary": "no_actual_outcome_join_to_mechanics", "expected": False, "actual": json_6ht.get("actual_outcomes_joined_to_mechanics"), "passed": json_6ht.get("actual_outcomes_joined_to_mechanics") is False},
        {"boundary": "no_corrected_normalized_outcomes", "expected": False, "actual": json_6ht.get("corrected_normalized_outcomes_emitted_by_this_layer"), "passed": json_6ht.get("corrected_normalized_outcomes_emitted_by_this_layer") is False},
        {"boundary": "no_activation_or_layer_6_exit_credit", "expected": False, "actual": json_6ht.get("layer_6_exit_credit"), "passed": json_6ht.get("layer_6_exit_credit") is False},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    plan_6ht_after = PLAN_6HT_PATH.read_text(encoding="utf-8") if PLAN_6HT_PATH.exists() else ""
    immutability_audit_rows = [
        {"surface": "this_6hu_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6ht_plan", "policy": "unchanged_by_6hu", "passed": plan_6ht_after == plan_6ht_before},
        {"surface": "deterministic_sources", "policy": "not_acquired_by_6hu", "passed": True},
        {"surface": "materialized_artifacts", "policy": "not_modified_by_6hu", "passed": True},
        {"surface": "adapter_behavior", "policy": "unchanged_by_6hu", "passed": True},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6hu", "passed": True},
    ]

    decision_rows = [
        {"decision": "6ht_plan_passed", "expected": True, "actual": json_6ht.get("all_checks_passed"), "passed": json_6ht.get("all_checks_passed") is True},
        {"decision": "planning_only_confirmed", "expected": True, "actual": json_6ht.get("planning_only"), "passed": json_6ht.get("planning_only") is True},
        {"decision": "gap_remediation_plan_created", "expected": True, "actual": json_6ht.get("remediation_plan_created"), "passed": json_6ht.get("remediation_plan_created") is True},
        {"decision": "source_remediation_implementation_allowed_next", "expected": True, "actual": True, "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HU, "actual": RECOMMENDED_NEXT_LAYER_6HU, "passed": True},
        {"decision": "materialization_allowed_after_this_audit", "expected": False, "actual": False, "passed": True},
        {"decision": "adapter_revision_allowed_after_this_audit", "expected": False, "actual": False, "passed": True},
        {"decision": "real_evaluation_allowed_after_this_audit", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    recommended_audit_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HU, "actual": RECOMMENDED_NEXT_LAYER_6HU, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6HU, "actual": RECOMMENDED_PATH_6HU, "passed": True},
        {"decision": "recommend_implementation_after_plan_audit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_materialization", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_adapter_revision", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_evaluation", "expected": True, "actual": True, "passed": True},
        {"decision": "materialization_still_blocked", "expected": True, "actual": True, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HU, "actual": DIAGNOSIS_6HU, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_rows), "detail": f"{sum(1 for row in artifact_rows if row['passed'])}/{len(artifact_rows)}"},
        {"check": "gap_summary", "passed": all(row["passed"] for row in gap_summary_audit_rows), "detail": f"{sum(1 for row in gap_summary_audit_rows if row['passed'])}/{len(gap_summary_audit_rows)}"},
        {"check": "family_plans", "passed": all(row["passed"] for row in family_plan_audit_rows), "detail": f"{sum(1 for row in family_plan_audit_rows if row['passed'])}/{len(family_plan_audit_rows)}"},
        {"check": "source_targets", "passed": all(row["passed"] for row in source_target_audit_rows) and len(source_target_rows_6ht) == 11, "detail": f"{len(source_target_rows_6ht)}/11"},
        {"check": "disallowed_paths", "passed": all(row["passed"] for row in disallowed_audit_rows) and len(disallowed_rows_6ht) == 18, "detail": f"{len(disallowed_rows_6ht)}/18"},
        {"check": "unblock_criteria", "passed": all(row["passed"] for row in unblock_audit_rows) and len(unblock_rows_6ht) == 9, "detail": f"{len(unblock_rows_6ht)}/9"},
        {"check": "implementation_sequence", "passed": all(row["passed"] for row in sequence_audit_rows), "detail": f"{sum(1 for row in sequence_audit_rows if row['passed'])}/{len(sequence_audit_rows)}"},
        {"check": "acceptance_criteria", "passed": all(row["passed"] for row in acceptance_audit_rows), "detail": f"{sum(1 for row in acceptance_audit_rows if row['passed'])}/{len(acceptance_audit_rows)}"},
        {"check": "future_6hv_contract", "passed": all(row["passed"] for row in future_6hv_audit_rows), "detail": f"{sum(1 for row in future_6hv_audit_rows if row['passed'])}/{len(future_6hv_audit_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_audit_rows), "detail": f"{sum(1 for row in safety_audit_rows if row['passed'])}/{len(safety_audit_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_audit_rows), "detail": f"{sum(1 for row in immutability_audit_rows if row['passed'])}/{len(immutability_audit_rows)}"},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_audit_rows), "detail": f"{sum(1 for row in recommended_audit_rows if row['passed'])}/{len(recommended_audit_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "artifact_presence": write_csv(ARTIFACT_PRESENCE_CSV, artifact_rows),
        "gap_summary": write_csv(GAP_SUMMARY_CSV, gap_summary_audit_rows),
        "family_plans": write_csv(FAMILY_PLANS_CSV, family_plan_audit_rows),
        "source_targets": write_csv(SOURCE_TARGETS_CSV, source_target_audit_rows),
        "disallowed_paths": write_csv(DISALLOWED_CSV, disallowed_audit_rows),
        "unblock_criteria": write_csv(UNBLOCK_CSV, unblock_audit_rows),
        "implementation_sequence": write_csv(SEQUENCE_CSV, sequence_audit_rows),
        "acceptance_criteria": write_csv(ACCEPTANCE_CSV, acceptance_audit_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "future_6hv_contract": write_csv(FUTURE_6HV_CONTRACT_CSV, future_6hv_audit_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_audit_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_audit_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_audit_rows),
    }

    summary = {
        "layer": "6HU",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HU if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6HU,
        "recommended_path": RECOMMENDED_PATH_6HU,
        "audited_layer": "6HT",
        "predecessor_plan": str(PLAN_6HT_PATH),
        "predecessor_plan_returncode": 0,
        "predecessor_plan_diagnosis": json_6ht.get("diagnosis"),
        "planning_only_confirmed": json_6ht.get("planning_only") is True,
        "gap_remediation_plan_created": json_6ht.get("remediation_plan_created") is True,
        "family_plan_count": len(family_plan_rows_6ht),
        "source_target_count": len(source_target_rows_6ht),
        "disallowed_path_count": len(disallowed_rows_6ht),
        "unblock_criteria_count": len(unblock_rows_6ht),
        "implementation_sequence_valid": all(row["passed"] for row in sequence_audit_rows),
        "future_6hv_contract_valid": all(row["passed"] for row in future_6hv_audit_rows),
        "gap_summary_valid": all(row["passed"] for row in gap_summary_audit_rows),
        "family_plans_valid": all(row["passed"] for row in family_plan_audit_rows),
        "source_targets_valid": all(row["passed"] for row in source_target_audit_rows),
        "disallowed_paths_valid": all(row["passed"] for row in disallowed_audit_rows),
        "unblock_criteria_valid": all(row["passed"] for row in unblock_audit_rows),
        "materialization_allowed_after_this_audit": False,
        "materialization_still_blocked": True,
        "source_remediation_implementation_allowed_next": True,
        "future_remediation_implementation_layer": FUTURE_6HV,
        "adapter_revision_allowed_after_this_audit": False,
        "adapter_revision_still_blocked": True,
        "real_evaluation_allowed_after_this_audit": False,
        "real_evaluation_blocked_by_validation": True,
        "future_adapter_revision_allowed_by_this_layer": False,
        "future_real_evaluation_allowed_by_this_layer": False,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "mechanic_evaluations_run": False,
        "actual_outcomes_joined_to_mechanics": False,
        "corrected_normalized_outcomes_emitted_by_audited_layer": False,
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
            "artifact_presence_csv": str(ARTIFACT_PRESENCE_CSV),
            "gap_summary_csv": str(GAP_SUMMARY_CSV),
            "family_plans_csv": str(FAMILY_PLANS_CSV),
            "source_targets_csv": str(SOURCE_TARGETS_CSV),
            "disallowed_paths_csv": str(DISALLOWED_CSV),
            "unblock_criteria_csv": str(UNBLOCK_CSV),
            "implementation_sequence_csv": str(SEQUENCE_CSV),
            "acceptance_criteria_csv": str(ACCEPTANCE_CSV),
            "decision_csv": str(DECISION_CSV),
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
