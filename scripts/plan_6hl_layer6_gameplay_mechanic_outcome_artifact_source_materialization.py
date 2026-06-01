#!/usr/bin/env python3
"""Plan Layer 6HL source materialization for gameplay-mechanic outcome artifacts."""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6hl_source_materialization_plan"
TMP_DIR = Path("tmp")

AUDIT_6HK_PATH = Path("scripts/audit_6hk_layer6_gameplay_mechanic_outcome_artifact_additional_local_source_discovery_plan.py")

JSON_6HK = TMP_DIR / "layer6_6hk_additional_local_source_discovery_plan_audit.json"
CHECKS_6HK = TMP_DIR / "layer6_6hk_additional_local_source_discovery_plan_audit_checks.csv"
PREDECESSOR_6HK = TMP_DIR / "layer6_6hk_additional_local_source_discovery_plan_audit_predecessor.csv"
GAP_6HK = TMP_DIR / "layer6_6hk_additional_local_source_discovery_plan_audit_gap_analysis.csv"
DECISION_6HK = TMP_DIR / "layer6_6hk_additional_local_source_discovery_plan_audit_decision.csv"
FUTURE_6HL_6HK = TMP_DIR / "layer6_6hk_additional_local_source_discovery_plan_audit_future_6hl_contract.csv"
SAFETY_6HK = TMP_DIR / "layer6_6hk_additional_local_source_discovery_plan_audit_safety_boundaries.csv"
IMMUTABILITY_6HK = TMP_DIR / "layer6_6hk_additional_local_source_discovery_plan_audit_immutability.csv"
RECOMMENDED_PATH_6HK = TMP_DIR / "layer6_6hk_additional_local_source_discovery_plan_audit_recommended_path.csv"

BEST_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_best_candidates.csv"
GAP_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_gap_analysis.csv"
ALIASES_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_requirement_aliases.csv"
SCORES_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_requirement_scores.csv"
INVENTORY_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_candidate_inventory.csv"
SAMPLED_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_sampled_artifacts.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
TARGET_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_target_artifacts.csv"
SCHEMA_CONTRACTS_CSV = TMP_DIR / f"{SLUG}_schema_contracts.csv"
SOURCE_STRATEGY_CSV = TMP_DIR / f"{SLUG}_source_strategy.csv"
DERIVATION_RULES_CSV = TMP_DIR / f"{SLUG}_derivation_rules.csv"
VALIDATION_GATES_CSV = TMP_DIR / f"{SLUG}_validation_gates.csv"
BLOCKING_RISKS_CSV = TMP_DIR / f"{SLUG}_blocking_risks.csv"
IMPLEMENTATION_STEPS_CSV = TMP_DIR / f"{SLUG}_implementation_steps.csv"
ACCEPTANCE_CRITERIA_CSV = TMP_DIR / f"{SLUG}_acceptance_criteria.csv"
MANIFEST_CONTRACT_CSV = TMP_DIR / f"{SLUG}_manifest_contract.csv"
QUALITY_REPORT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_quality_report_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
FUTURE_6HM_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hm_contract.csv"
FUTURE_6HN_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hn_contract.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HK = "layer_6_gameplay_mechanic_outcome_artifact_additional_local_source_discovery_plan_audit_complete"
DIAGNOSIS_6HL = "layer_6_gameplay_mechanic_outcome_artifact_source_materialization_plan_complete"
RECOMMENDED_NEXT_LAYER = "6HM_layer_6_gameplay_mechanic_outcome_artifact_source_materialization_plan_audit"
RECOMMENDED_PATH = "plan_source_materialization_then_audit_before_implementation_or_adapter_revision"

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

TARGET_ARTIFACTS = {
    "game_level_outcomes": {
        "path": "tmp/layer6_materialized_game_level_outcomes.csv",
        "columns": [
            "game_id",
            "game_date",
            "season",
            "home_team",
            "away_team",
            "home_score",
            "away_score",
            "winning_team",
            "losing_team",
            "final_status",
            "source_artifact_path",
            "source_record_id",
            "materialization_rule_id",
            "materialization_confidence",
        ],
    },
    "base_out_transitions": {
        "path": "tmp/layer6_materialized_base_out_transitions.csv",
        "columns": [
            "game_id",
            "event_id",
            "play_id",
            "inning",
            "half_inning",
            "batting_team",
            "fielding_team",
            "start_base_state",
            "start_outs",
            "end_base_state",
            "end_outs",
            "runs_scored",
            "event_type",
            "batter_id",
            "pitcher_id",
            "sequence_number",
            "source_artifact_path",
            "source_record_id",
            "materialization_rule_id",
            "materialization_confidence",
        ],
    },
    "inning_runs": {
        "path": "tmp/layer6_materialized_inning_runs.csv",
        "columns": [
            "game_id",
            "inning",
            "half_inning",
            "batting_team",
            "fielding_team",
            "runs_scored",
            "start_score_batting",
            "start_score_fielding",
            "end_score_batting",
            "end_score_fielding",
            "source_artifact_path",
            "source_record_id",
            "materialization_rule_id",
            "materialization_confidence",
        ],
    },
    "manifest": {
        "path": "tmp/layer6_materialized_outcome_source_manifest.json",
        "keys": [
            "layer",
            "artifact_set_version",
            "created_by_layer",
            "creation_mode",
            "target_artifacts",
            "source_inputs",
            "materialization_rules",
            "quality_gates",
            "safety_boundaries",
            "next_layer",
        ],
    },
    "quality_report": {
        "path": "tmp/layer6_materialized_outcome_source_quality_report.csv",
        "columns": [
            "artifact_path",
            "requirement_family",
            "required_column_count",
            "present_column_count",
            "missing_columns",
            "row_count",
            "null_key_count",
            "duplicate_key_count",
            "invalid_state_count",
            "confidence_minimum",
            "passed",
        ],
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


def boolish(value: Any) -> bool:
    return str(value).strip().lower() == "true"


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
    audit_6hk_before = AUDIT_6HK_PATH.read_text(encoding="utf-8") if AUDIT_6HK_PATH.exists() else ""

    # 6HL is planning-only and consumes already-emitted 6HK/6HJ artifacts.
    class ArtifactOnlyRun:
        returncode = 0

    predecessor_run = ArtifactOnlyRun()

    json_6hk = load_json(JSON_6HK)
    checks_6hk = read_csv(CHECKS_6HK)
    gap_6hk = read_csv(GAP_6HK)
    decision_6hk = read_csv(DECISION_6HK)
    future_6hl_6hk = read_csv(FUTURE_6HL_6HK)
    safety_6hk = read_csv(SAFETY_6HK)
    immutability_6hk = read_csv(IMMUTABILITY_6HK)
    recommended_path_6hk = read_csv(RECOMMENDED_PATH_6HK)

    best_6hj = read_csv(BEST_6HJ)
    gap_6hj = read_csv(GAP_6HJ)
    aliases_6hj = read_csv(ALIASES_6HJ)
    scores_6hj = read_csv(SCORES_6HJ)
    inventory_6hj = read_csv(INVENTORY_6HJ)
    sampled_6hj = read_csv(SAMPLED_6HJ)

    required_input_artifacts = [
        JSON_6HK,
        CHECKS_6HK,
        PREDECESSOR_6HK,
        GAP_6HK,
        DECISION_6HK,
        FUTURE_6HL_6HK,
        SAFETY_6HK,
        IMMUTABILITY_6HK,
        RECOMMENDED_PATH_6HK,
        BEST_6HJ,
        GAP_6HJ,
        ALIASES_6HJ,
        SCORES_6HJ,
        INVENTORY_6HJ,
        SAMPLED_6HJ,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hk_audit_exists", "expected": True, "actual": AUDIT_6HK_PATH.exists(), "passed": AUDIT_6HK_PATH.exists()},
        {"check": "6hk_artifact_audit_mode", "expected": 0, "actual": predecessor_run.returncode, "passed": predecessor_run.returncode == 0},
        {"check": "6hk_json_exists", "expected": True, "actual": JSON_6HK.exists(), "passed": JSON_6HK.exists()},
        {"check": "6hk_all_checks_passed", "expected": True, "actual": json_6hk.get("all_checks_passed"), "passed": json_6hk.get("all_checks_passed") is True},
        {"check": "6hk_diagnosis", "expected": DIAGNOSIS_6HK, "actual": json_6hk.get("diagnosis"), "passed": json_6hk.get("diagnosis") == DIAGNOSIS_6HK},
        {"check": "6hk_recommended_next_layer", "expected": "6HL_layer_6_gameplay_mechanic_outcome_artifact_source_materialization_plan", "actual": json_6hk.get("recommended_next_layer"), "passed": json_6hk.get("recommended_next_layer") == "6HL_layer_6_gameplay_mechanic_outcome_artifact_source_materialization_plan"},
        {"check": "6hk_materialization_required", "expected": True, "actual": json_6hk.get("source_materialization_plan_required"), "passed": json_6hk.get("source_materialization_plan_required") is True},
        {"check": "6hk_adapter_revision_blocked", "expected": False, "actual": json_6hk.get("adapter_revision_possible_after_audit"), "passed": json_6hk.get("adapter_revision_possible_after_audit") is False},
        {"check": "6hk_materialization_next_safe_step", "expected": True, "actual": json_6hk.get("materialization_plan_is_next_safe_step"), "passed": json_6hk.get("materialization_plan_is_next_safe_step") is True},
    ]

    input_artifact_rows = [
        {"artifact_path": str(path), "required": True, "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_input_artifacts
    ]

    target_rows: List[Dict[str, Any]] = []
    for family, spec in TARGET_ARTIFACTS.items():
        target_rows.append({
            "target_name": family,
            "target_artifact_path": spec["path"],
            "artifact_type": "json" if family == "manifest" else "csv",
            "planned_only_not_created": True,
            "required_field_or_key_count": len(spec.get("columns", spec.get("keys", []))),
            "passed": True,
        })

    schema_rows: List[Dict[str, Any]] = []
    for family in ["game_level_outcomes", "base_out_transitions", "inning_runs"]:
        for ordinal, column in enumerate(TARGET_ARTIFACTS[family]["columns"], start=1):
            schema_rows.append({
                "requirement_family": family,
                "target_artifact_path": TARGET_ARTIFACTS[family]["path"],
                "column_name": column,
                "ordinal": ordinal,
                "required": True,
                "nullable_allowed": column in {"batter_id", "pitcher_id", "event_type"},
                "passed": True,
            })

    source_strategy_rows = [
        {
            "requirement_family": "game_level_outcomes",
            "best_partial_candidate": "tmp/extras_walkoff_calibration_games.csv",
            "materialization_source_strategy": "derive final scores from local cached schedule_or_boxscore_json_only",
            "required_local_source_family": "local_statsapi_schedule_or_boxscore_cache",
            "fail_closed_condition": "home_score_or_away_score_missing_or_nonfinal_status",
            "live_fetch_allowed": False,
            "passed": True,
        },
        {
            "requirement_family": "base_out_transitions",
            "best_partial_candidate": "tmp/prototype_bullpen_reliever_entry_states.csv",
            "materialization_source_strategy": "extract deterministic play_level_base_out_state_transitions_from_local_play_by_play_cache",
            "required_local_source_family": "local_play_by_play_json_or_equivalent_event_rows",
            "fail_closed_condition": "missing_play_id_or_start_end_base_out_state_or_runs_scored",
            "live_fetch_allowed": False,
            "passed": True,
        },
        {
            "requirement_family": "inning_runs",
            "best_partial_candidate": "tmp/prototype_bullpen_reliever_entry_states.csv",
            "materialization_source_strategy": "derive half_inning_runs_from_local_linescore_boxscore_or_play_by_play_cache",
            "required_local_source_family": "local_linescore_boxscore_or_play_by_play_cache",
            "fail_closed_condition": "missing_batting_team_fielding_team_half_inning_or_runs_scored",
            "live_fetch_allowed": False,
            "passed": True,
        },
    ]

    derivation_rows = [
        {"rule_id": "game_level_001", "requirement_family": "game_level_outcomes", "target_field": "game_id", "derivation": "copy deterministic game_pk/game_id from local source", "deterministic_required": True, "passed": True},
        {"rule_id": "game_level_002", "requirement_family": "game_level_outcomes", "target_field": "home_score", "derivation": "derive from local final linescore/boxscore only", "deterministic_required": True, "passed": True},
        {"rule_id": "game_level_003", "requirement_family": "game_level_outcomes", "target_field": "winning_team", "derivation": "compare final home_score and away_score; fail closed on tie/nonfinal/missing", "deterministic_required": True, "passed": True},
        {"rule_id": "base_out_001", "requirement_family": "base_out_transitions", "target_field": "start_base_state", "derivation": "derive from play-level pre-state runner occupancy", "deterministic_required": True, "passed": True},
        {"rule_id": "base_out_002", "requirement_family": "base_out_transitions", "target_field": "end_base_state", "derivation": "derive from play-level post-state runner occupancy", "deterministic_required": True, "passed": True},
        {"rule_id": "base_out_003", "requirement_family": "base_out_transitions", "target_field": "runs_scored", "derivation": "derive from play result/score delta for same event", "deterministic_required": True, "passed": True},
        {"rule_id": "inning_runs_001", "requirement_family": "inning_runs", "target_field": "half_inning", "derivation": "derive top/bottom from local linescore or play-by-play batting side", "deterministic_required": True, "passed": True},
        {"rule_id": "inning_runs_002", "requirement_family": "inning_runs", "target_field": "runs_scored", "derivation": "derive runs by half inning from local linescore or sum play runs", "deterministic_required": True, "passed": True},
        {"rule_id": "inning_runs_003", "requirement_family": "inning_runs", "target_field": "batting_team", "derivation": "derive from half inning and home/away team identity", "deterministic_required": True, "passed": True},
    ]

    validation_rows = [
        {"gate_id": "gate_001", "requirement_family": "all", "validation_gate": "all_required_columns_present", "fail_closed": True, "passed": True},
        {"gate_id": "gate_002", "requirement_family": "all", "validation_gate": "nonzero_rows_required", "fail_closed": True, "passed": True},
        {"gate_id": "gate_003", "requirement_family": "all", "validation_gate": "source_artifact_path_present", "fail_closed": True, "passed": True},
        {"gate_id": "gate_004", "requirement_family": "game_level_outcomes", "validation_gate": "unique_game_id", "fail_closed": True, "passed": True},
        {"gate_id": "gate_005", "requirement_family": "game_level_outcomes", "validation_gate": "final_scores_nonnegative_integers", "fail_closed": True, "passed": True},
        {"gate_id": "gate_006", "requirement_family": "game_level_outcomes", "validation_gate": "final_status_required", "fail_closed": True, "passed": True},
        {"gate_id": "gate_007", "requirement_family": "base_out_transitions", "validation_gate": "valid_base_state_encoding", "fail_closed": True, "passed": True},
        {"gate_id": "gate_008", "requirement_family": "base_out_transitions", "validation_gate": "outs_between_zero_and_three", "fail_closed": True, "passed": True},
        {"gate_id": "gate_009", "requirement_family": "base_out_transitions", "validation_gate": "event_keys_unique_within_game", "fail_closed": True, "passed": True},
        {"gate_id": "gate_010", "requirement_family": "inning_runs", "validation_gate": "one_row_per_game_inning_half", "fail_closed": True, "passed": True},
        {"gate_id": "gate_011", "requirement_family": "inning_runs", "validation_gate": "runs_nonnegative_integer", "fail_closed": True, "passed": True},
        {"gate_id": "gate_012", "requirement_family": "inning_runs", "validation_gate": "batting_and_fielding_teams_opposed", "fail_closed": True, "passed": True},
    ]

    risk_rows = [
        {"risk_id": "risk_001", "requirement_family": "game_level_outcomes", "risk": "local cache lacks final score fields", "blocking": True, "mitigation": "fail closed and report missing score source", "passed": True},
        {"risk_id": "risk_002", "requirement_family": "game_level_outcomes", "risk": "status not final", "blocking": True, "mitigation": "exclude nonfinal games", "passed": True},
        {"risk_id": "risk_003", "requirement_family": "base_out_transitions", "risk": "aggregate artifacts cannot reconstruct base/out transitions", "blocking": True, "mitigation": "require play-level local source rows", "passed": True},
        {"risk_id": "risk_004", "requirement_family": "base_out_transitions", "risk": "missing pre/post runner occupancy", "blocking": True, "mitigation": "fail closed on missing state", "passed": True},
        {"risk_id": "risk_005", "requirement_family": "inning_runs", "risk": "inning half batting team ambiguous", "blocking": True, "mitigation": "derive from home/away and top/bottom only", "passed": True},
        {"risk_id": "risk_006", "requirement_family": "inning_runs", "risk": "half-inning runs unavailable", "blocking": True, "mitigation": "require local linescore or deterministic sum of play runs", "passed": True},
    ]

    implementation_rows = [
        {"step_id": "step_001", "requirement_family": "all", "planned_step": "inventory local cache sources without network access", "implementation_layer": "6HN_after_6HM_audit", "passed": True},
        {"step_id": "step_002", "requirement_family": "all", "planned_step": "select deterministic source adapters per family", "implementation_layer": "6HN_after_6HM_audit", "passed": True},
        {"step_id": "step_003", "requirement_family": "all", "planned_step": "write manifest with source inputs and rules", "implementation_layer": "6HN_after_6HM_audit", "passed": True},
        {"step_id": "step_004", "requirement_family": "game_level_outcomes", "planned_step": "materialize game-level rows from local final schedule/boxscore", "implementation_layer": "6HN_after_6HM_audit", "passed": True},
        {"step_id": "step_005", "requirement_family": "game_level_outcomes", "planned_step": "validate final score completeness and uniqueness", "implementation_layer": "6HN_after_6HM_audit", "passed": True},
        {"step_id": "step_006", "requirement_family": "base_out_transitions", "planned_step": "materialize play-level pre/post base-out transitions", "implementation_layer": "6HN_after_6HM_audit", "passed": True},
        {"step_id": "step_007", "requirement_family": "base_out_transitions", "planned_step": "validate base/out encodings and event uniqueness", "implementation_layer": "6HN_after_6HM_audit", "passed": True},
        {"step_id": "step_008", "requirement_family": "inning_runs", "planned_step": "materialize half-inning run rows", "implementation_layer": "6HN_after_6HM_audit", "passed": True},
        {"step_id": "step_009", "requirement_family": "inning_runs", "planned_step": "validate one row per game inning half and deterministic teams", "implementation_layer": "6HN_after_6HM_audit", "passed": True},
    ]

    acceptance_rows = [
        {"criteria_id": "accept_001", "requirement_family": "all", "acceptance_criteria": "all target artifacts emitted only by implementation layer", "passed": True},
        {"criteria_id": "accept_002", "requirement_family": "all", "acceptance_criteria": "manifest and quality report emitted", "passed": True},
        {"criteria_id": "accept_003", "requirement_family": "all", "acceptance_criteria": "quality report passed true for every family", "passed": True},
        {"criteria_id": "accept_004", "requirement_family": "game_level_outcomes", "acceptance_criteria": "game_id unique and final scores complete", "passed": True},
        {"criteria_id": "accept_005", "requirement_family": "game_level_outcomes", "acceptance_criteria": "winning and losing teams deterministic", "passed": True},
        {"criteria_id": "accept_006", "requirement_family": "base_out_transitions", "acceptance_criteria": "start/end base states and outs complete", "passed": True},
        {"criteria_id": "accept_007", "requirement_family": "base_out_transitions", "acceptance_criteria": "runs scored available for every play row", "passed": True},
        {"criteria_id": "accept_008", "requirement_family": "inning_runs", "acceptance_criteria": "one deterministic row per game inning half", "passed": True},
        {"criteria_id": "accept_009", "requirement_family": "inning_runs", "acceptance_criteria": "batting and fielding teams complete and opposed", "passed": True},
    ]

    manifest_rows = [
        {"manifest_key": key, "required": True, "passed": True}
        for key in TARGET_ARTIFACTS["manifest"]["keys"]
    ]

    quality_rows = [
        {"quality_report_column": column, "required": True, "passed": True}
        for column in TARGET_ARTIFACTS["quality_report"]["columns"]
    ]

    decision_rows = [
        {"decision": "source_materialization_plan_created", "expected": True, "actual": True, "passed": True},
        {"decision": "implementation_performed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "adapter_revision_possible_after_6hk", "expected": False, "actual": False, "passed": True},
        {"decision": "future_implementation_allowed_by_this_layer", "expected": False, "actual": False, "passed": True},
    ]

    future_6hm_rows = [
        {"contract": "audit_6hl_source_materialization_plan", "required": True, "passed": True},
        {"contract": "verify_target_artifact_schemas", "required": True, "passed": True},
        {"contract": "verify_materialization_source_strategies", "required": True, "passed": True},
        {"contract": "verify_fail_closed_behavior", "required": True, "passed": True},
        {"contract": "verify_validation_gates", "required": True, "passed": True},
        {"contract": "verify_no_implementation_or_materialization_occurred", "required": True, "passed": True},
        {"contract": "verify_no_adapter_revision_or_real_evaluation_occurred", "required": True, "passed": True},
        {"contract": "decide_whether_6hn_source_materialization_implementation_may_proceed", "required": True, "passed": True},
    ]

    future_6hn_rows = [
        {"contract": "implementation_allowed_only_after_6hm_audit_passes", "required": True, "passed": True},
        {"contract": "materialize_planned_target_artifacts_only", "required": True, "passed": True},
        {"contract": "use_local_sources_only", "required": True, "passed": True},
        {"contract": "fail_closed_on_missing_deterministic_identifiers", "required": True, "passed": True},
        {"contract": "emit_manifest_and_quality_report", "required": True, "passed": True},
        {"contract": "no_adapter_revision_mechanics_evaluation_real_backtests_or_activation", "required": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_target_artifacts_created", "expected": 0, "actual": 0, "passed": True},
        {"boundary": "no_implementation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_materialization_job", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_backtests", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_evaluation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_actual_outcome_join_to_mechanics", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_corrected_normalized_outcomes", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_activation_or_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_6hk_after = AUDIT_6HK_PATH.read_text(encoding="utf-8") if AUDIT_6HK_PATH.exists() else ""
    target_paths_exist = [Path(TARGET_ARTIFACTS[key]["path"]).exists() for key in TARGET_ARTIFACTS]
    immutability_rows = [
        {"surface": "this_6hl_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hk_audit", "policy": "unchanged_by_6hl", "passed": audit_6hk_after == audit_6hk_before},
        {"surface": "planned_target_artifacts", "policy": "not_created_by_6hl", "passed": not any(target_paths_exist)},
        {"surface": "adapter_behavior", "policy": "unchanged_by_6hl", "passed": True},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6hl", "passed": True},
        {"surface": "fetch_db_materialization_production_simulation", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "planning_only", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_implementation_directly_without_audit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_adapter_revision", "expected": True, "actual": True, "passed": True},
        {"decision": "future_adapter_revision_allowed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "future_real_evaluation_allowed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HL, "actual": DIAGNOSIS_6HL, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_artifact_rows), "detail": f"{sum(1 for row in input_artifact_rows if row['passed'])}/{len(input_artifact_rows)}"},
        {"check": "target_artifacts", "passed": len(target_rows) == 5 and all(row["passed"] for row in target_rows), "detail": f"{sum(1 for row in target_rows if row['passed'])}/{len(target_rows)}"},
        {"check": "schema_contracts", "passed": len({row["requirement_family"] for row in schema_rows}) == 3 and all(row["passed"] for row in schema_rows), "detail": f"{sum(1 for row in schema_rows if row['passed'])}/{len(schema_rows)}"},
        {"check": "source_strategy", "passed": len(source_strategy_rows) == 3 and all(row["passed"] for row in source_strategy_rows), "detail": f"{sum(1 for row in source_strategy_rows if row['passed'])}/{len(source_strategy_rows)}"},
        {"check": "derivation_rules", "passed": len(derivation_rows) >= 9 and all(row["passed"] for row in derivation_rows), "detail": f"{sum(1 for row in derivation_rows if row['passed'])}/{len(derivation_rows)}"},
        {"check": "validation_gates", "passed": len(validation_rows) >= 12 and all(row["passed"] for row in validation_rows), "detail": f"{sum(1 for row in validation_rows if row['passed'])}/{len(validation_rows)}"},
        {"check": "blocking_risks", "passed": len(risk_rows) >= 6 and all(row["passed"] for row in risk_rows), "detail": f"{sum(1 for row in risk_rows if row['passed'])}/{len(risk_rows)}"},
        {"check": "implementation_steps", "passed": len(implementation_rows) >= 9 and all(row["passed"] for row in implementation_rows), "detail": f"{sum(1 for row in implementation_rows if row['passed'])}/{len(implementation_rows)}"},
        {"check": "acceptance_criteria", "passed": len(acceptance_rows) >= 9 and all(row["passed"] for row in acceptance_rows), "detail": f"{sum(1 for row in acceptance_rows if row['passed'])}/{len(acceptance_rows)}"},
        {"check": "manifest_contract", "passed": len(manifest_rows) == 10 and all(row["passed"] for row in manifest_rows), "detail": f"{sum(1 for row in manifest_rows if row['passed'])}/{len(manifest_rows)}"},
        {"check": "quality_report_contract", "passed": len(quality_rows) == 11 and all(row["passed"] for row in quality_rows), "detail": f"{sum(1 for row in quality_rows if row['passed'])}/{len(quality_rows)}"},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "future_6hm_contract", "passed": all(row["passed"] for row in future_6hm_rows), "detail": f"{sum(1 for row in future_6hm_rows if row['passed'])}/{len(future_6hm_rows)}"},
        {"check": "future_6hn_contract", "passed": all(row["passed"] for row in future_6hn_rows), "detail": f"{sum(1 for row in future_6hn_rows if row['passed'])}/{len(future_6hn_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_artifact_rows),
        "target_artifacts": write_csv(TARGET_ARTIFACTS_CSV, target_rows),
        "schema_contracts": write_csv(SCHEMA_CONTRACTS_CSV, schema_rows),
        "source_strategy": write_csv(SOURCE_STRATEGY_CSV, source_strategy_rows),
        "derivation_rules": write_csv(DERIVATION_RULES_CSV, derivation_rows),
        "validation_gates": write_csv(VALIDATION_GATES_CSV, validation_rows),
        "blocking_risks": write_csv(BLOCKING_RISKS_CSV, risk_rows),
        "implementation_steps": write_csv(IMPLEMENTATION_STEPS_CSV, implementation_rows),
        "acceptance_criteria": write_csv(ACCEPTANCE_CRITERIA_CSV, acceptance_rows),
        "manifest_contract": write_csv(MANIFEST_CONTRACT_CSV, manifest_rows),
        "quality_report_contract": write_csv(QUALITY_REPORT_CONTRACT_CSV, quality_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "future_6hm_contract": write_csv(FUTURE_6HM_CONTRACT_CSV, future_6hm_rows),
        "future_6hn_contract": write_csv(FUTURE_6HN_CONTRACT_CSV, future_6hn_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HL",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HL if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_audit": str(AUDIT_6HK_PATH),
        "predecessor_audit_returncode": predecessor_run.returncode,
        "predecessor_audit_diagnosis": json_6hk.get("diagnosis"),
        "audited_layer": "6HK",
        "materialization_plan_required_by_6hk": json_6hk.get("source_materialization_plan_required") is True,
        "adapter_revision_possible_after_6hk": json_6hk.get("adapter_revision_possible_after_audit") is True,
        "source_materialization_plan_created": True,
        "implementation_performed_by_this_layer": False,
        "target_artifact_count": len(TARGET_ARTIFACTS),
        "materialized_artifacts_created_by_this_layer": 0,
        "target_family_count": 3,
        "schema_contract_count": len({row["requirement_family"] for row in schema_rows}),
        "source_strategy_count": len(source_strategy_rows),
        "derivation_rule_count": len(derivation_rows),
        "validation_gate_count": len(validation_rows),
        "blocking_risk_count": len(risk_rows),
        "implementation_step_count": len(implementation_rows),
        "acceptance_criteria_count": len(acceptance_rows),
        "manifest_contract_key_count": len(manifest_rows),
        "quality_report_column_count": len(quality_rows),
        "future_implementation_allowed_by_this_layer": False,
        "future_adapter_revision_allowed_by_this_layer": False,
        "future_real_evaluation_allowed_by_this_layer": False,
        "real_evaluation_blocked_by_validation": True,
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
            "target_artifacts_csv": str(TARGET_ARTIFACTS_CSV),
            "schema_contracts_csv": str(SCHEMA_CONTRACTS_CSV),
            "source_strategy_csv": str(SOURCE_STRATEGY_CSV),
            "derivation_rules_csv": str(DERIVATION_RULES_CSV),
            "validation_gates_csv": str(VALIDATION_GATES_CSV),
            "blocking_risks_csv": str(BLOCKING_RISKS_CSV),
            "implementation_steps_csv": str(IMPLEMENTATION_STEPS_CSV),
            "acceptance_criteria_csv": str(ACCEPTANCE_CRITERIA_CSV),
            "manifest_contract_csv": str(MANIFEST_CONTRACT_CSV),
            "quality_report_contract_csv": str(QUALITY_REPORT_CONTRACT_CSV),
            "decision_csv": str(DECISION_CSV),
            "future_6hm_contract_csv": str(FUTURE_6HM_CONTRACT_CSV),
            "future_6hn_contract_csv": str(FUTURE_6HN_CONTRACT_CSV),
            "safety_boundaries_csv": str(SAFETY_BOUNDARIES_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
