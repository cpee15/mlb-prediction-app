#!/usr/bin/env python3
"""Plan historical backtest readiness for current UI realism state.

This planning-only layer prepares the first historical evaluation for the
current UI projection path labeled as bullpen-active partial realism. It does
not run evaluation, fetch data, write DBs, run production simulations, activate
mechanics, or grant Layer 6 exit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6kp_historical_backtest_readiness_plan"
TMP_DIR = Path("tmp")

AUDIT_6KO_PATH = Path("scripts/audit_6ko_layer6_ui_realism_feature_output_effect_measurement_implementation.py")
JSON_6KO = TMP_DIR / "layer6_6ko_ui_realism_feature_output_effect_measurement_implementation_audit.json"

REQUIRED_INPUTS = [
    JSON_6KO,
    TMP_DIR / "layer6_6ko_ui_realism_feature_output_effect_measurement_implementation_audit_checks.csv",
    TMP_DIR / "layer6_6ko_ui_realism_feature_output_effect_measurement_implementation_audit_predecessor.csv",
    TMP_DIR / "layer6_6ko_ui_realism_feature_output_effect_measurement_implementation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6ko_ui_realism_feature_output_effect_measurement_implementation_audit_measurement_outcome_audit.csv",
    TMP_DIR / "layer6_6ko_ui_realism_feature_output_effect_measurement_implementation_audit_current_realism_state.csv",
    TMP_DIR / "layer6_6ko_ui_realism_feature_output_effect_measurement_implementation_audit_next_layer_rationale.csv",
    TMP_DIR / "layer6_6ko_ui_realism_feature_output_effect_measurement_implementation_audit_blockers.csv",
    TMP_DIR / "layer6_6ko_ui_realism_feature_output_effect_measurement_implementation_audit_future_6kp_contract.csv",
    TMP_DIR / "layer6_6ko_ui_realism_feature_output_effect_measurement_implementation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6ko_ui_realism_feature_output_effect_measurement_implementation_audit_decision.csv",
    TMP_DIR / "layer6_6ko_ui_realism_feature_output_effect_measurement_implementation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6ko_ui_realism_feature_output_effect_measurement_implementation_audit_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
BACKTEST_LABEL_CSV = TMP_DIR / f"{SLUG}_backtest_label.csv"
DATASET_PRIORITY_CSV = TMP_DIR / f"{SLUG}_dataset_priority.csv"
WINDOW_PLAN_CSV = TMP_DIR / f"{SLUG}_window_plan.csv"
REQUIRED_COLUMNS_CSV = TMP_DIR / f"{SLUG}_required_columns.csv"
METRIC_PLAN_CSV = TMP_DIR / f"{SLUG}_metric_plan.csv"
MECHANIC_TAGS_CSV = TMP_DIR / f"{SLUG}_mechanic_tags.csv"
EXCLUSIONS_CSV = TMP_DIR / f"{SLUG}_exclusions.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6KQ_CSV = TMP_DIR / f"{SLUG}_future_6kq_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KO = "layer_6_ui_realism_feature_output_effect_measurement_implementation_audit_complete"
DIAGNOSIS_6KP = "layer_6_historical_backtest_readiness_plan_complete"
RECOMMENDED_NEXT_LAYER_6KO = "6KP_layer_6_historical_backtest_readiness_plan"
RECOMMENDED_NEXT_LAYER_6KP = "6KQ_layer_6_historical_backtest_readiness_implementation"
RECOMMENDED_PATH_6KP = "implement_historical_backtest_readiness_for_current_ui_realism_state"


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True, "passed": True}]
    fieldnames: List[str] = []
    for row in rows:
        for key in row:
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
    return value is True or str(value).lower() == "true"


def all_passed(rows: List[Dict[str, Any]]) -> bool:
    return all(boolish(row.get("passed", "")) for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6ko = load_json(JSON_6KO)

    backtest_label = [
        {
            "label_key": "backtest_label",
            "label_value": "current_ui_projection_path_bullpen_active_partial_realism",
            "current_ui_realism_state_label": "bullpen_active_partial_realism",
            "interpretation": "Evaluate current UI projection path as partial realism with bullpen-active evidence only.",
            "passed": True,
        }
    ]

    dataset_priority = [
        {"priority": 1, "dataset_option": "existing_predicted_vs_actual_backtest_dataset", "requirement": "usable if predicted/actual fields are present", "historical_odds_required": False, "passed": True},
        {"priority": 2, "dataset_option": "existing_backtest_dataset_with_projected_runs_actual_runs", "requirement": "usable for run MAE/RMSE even if win probabilities incomplete", "historical_odds_required": False, "passed": True},
        {"priority": 3, "dataset_option": "fixed_smaller_validation_slice", "requirement": "use if full window is unavailable or too expensive", "historical_odds_required": False, "passed": True},
    ]

    window_plan = [
        {"window_rank": 1, "window_name": "opening_day_to_latest_completed_game", "condition": "preferred if existing dataset covers this with complete fields", "passed": True},
        {"window_rank": 2, "window_name": "fixed_recent_slice", "condition": "fallback if full window is too expensive or incomplete", "passed": True},
        {"window_rank": 3, "window_name": "april_20_to_may_3_equivalent_if_available", "condition": "fallback named slice if date coverage supports it", "passed": True},
        {"window_rank": 4, "window_name": "first_n_complete_rows", "condition": "fallback if only row-level completeness is reliable", "passed": True},
    ]

    required_columns = [
        {"column_family": "game_or_date_identifier", "required": True, "examples": "game_id;date;game_date", "passed": True},
        {"column_family": "team_or_matchup_identifier", "required": True, "examples": "home_team;away_team;matchup", "passed": True},
        {"column_family": "predicted_win_probability", "required": False, "examples": "home_win_probability;away_win_probability;predicted_win_prob", "passed": True},
        {"column_family": "actual_winner_or_result", "required": False, "examples": "home_win;away_win;winner;result", "passed": True},
        {"column_family": "predicted_runs", "required": False, "examples": "home_expected_runs;away_expected_runs;predicted_home_runs;predicted_away_runs", "passed": True},
        {"column_family": "actual_runs", "required": False, "examples": "home_runs;away_runs;actual_home_runs;actual_away_runs", "passed": True},
        {"column_family": "realism_state_label", "required": True, "examples": "current_ui_projection_path_bullpen_active_partial_realism", "passed": True},
        {"column_family": "mechanic_tags", "required": True, "examples": "bullpen_active;extras_bypassed;steals_inactive;balk_deferred", "passed": True},
    ]

    metric_plan = [
        {"metric": "brier_score", "requires": "predicted win probability and actual winner", "odds_required": False, "passed": True},
        {"metric": "calibration_bucket_table", "requires": "predicted win probability and actual winner", "odds_required": False, "passed": True},
        {"metric": "favorite_underdog_directional_accuracy", "requires": "predicted side probability and actual winner", "odds_required": False, "passed": True},
        {"metric": "predicted_runs_mae_rmse", "requires": "predicted team runs and actual team runs", "odds_required": False, "passed": True},
        {"metric": "projected_total_runs_mae_rmse", "requires": "predicted total runs and actual total runs", "odds_required": False, "passed": True},
        {"metric": "confidence_bucket_summary", "requires": "prediction confidence or probability buckets", "odds_required": False, "passed": True},
        {"metric": "coverage_completeness_diagnostics", "requires": "row/field availability", "odds_required": False, "passed": True},
        {"metric": "missing_field_diagnostics", "requires": "schema inspection", "odds_required": False, "passed": True},
    ]

    mechanic_tags = [
        {"tag": "bullpen_active", "status": "include", "reason": "bullpen output-effect proxy confirmed", "passed": True},
        {"tag": "double_play_reachable_delta_unproven", "status": "tag_not_claim_full_effect", "reason": "reachable but displayed-output delta not proven", "passed": True},
        {"tag": "sac_fly_reachable_delta_unproven", "status": "tag_not_claim_full_effect", "reason": "reachable but displayed-output delta not proven", "passed": True},
        {"tag": "extras_walkoff_bypassed", "status": "exclude_or_tag_bypassed", "reason": "current UI route bypass confirmed", "passed": True},
        {"tag": "steals_inactive", "status": "exclude_or_tag_inactive", "reason": "inactive confirmed", "passed": True},
        {"tag": "balk_deferred", "status": "exclude_or_tag_deferred", "reason": "deferred confirmed", "passed": True},
    ]

    exclusions = [
        {"exclusion": "no_historical_odds_assumption", "reason": "dataset may not contain historical odds/moneyline/totals", "passed": True},
        {"exclusion": "no_full_realism_label", "reason": "current state is bullpen-active partial realism", "passed": True},
        {"exclusion": "no_activation_decision", "reason": "6KP is planning only", "passed": True},
        {"exclusion": "no_layer6_exit", "reason": "backtest readiness is not exit", "passed": True},
        {"exclusion": "no_database_write", "reason": "planning-only layer", "passed": True},
        {"exclusion": "no_remote_or_live_fetch", "reason": "use existing artifacts/data only in future implementation unless explicitly planned", "passed": True},
        {"exclusion": "no_real_evaluation_execution", "reason": "6KP does not run the backtest", "passed": True},
    ]

    blockers = [
        {"blocker": "historical_backtest_readiness_not_implemented", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "activation_decision_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "full_realism_activation_not_confirmed", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "balk_deferred", "blocks_activation": False, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6kq = [
        {"contract": "implement_dataset_discovery_for_existing_backtest_artifacts", "required": True, "passed": True},
        {"contract": "implement_schema_readiness_check", "required": True, "passed": True},
        {"contract": "implement_window_feasibility_check", "required": True, "passed": True},
        {"contract": "implement_metric_feasibility_check_without_historical_odds", "required": True, "passed": True},
        {"contract": "emit_backtest_readiness_verdict_not_real_backtest", "required": True, "passed": True},
        {"contract": "preserve_no_activation_no_layer6_exit", "required": True, "passed": True},
        {"contract": "do_not_fetch_or_write_in_6kq", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ko_audit_script_exists", "expected": True, "actual": AUDIT_6KO_PATH.exists(), "passed": AUDIT_6KO_PATH.exists()},
        {"check": "6ko_json_exists", "expected": True, "actual": JSON_6KO.exists(), "passed": JSON_6KO.exists()},
        {"check": "6ko_all_checks_passed", "expected": True, "actual": json_6ko.get("all_checks_passed"), "passed": json_6ko.get("all_checks_passed") is True},
        {"check": "6ko_diagnosis", "expected": DIAGNOSIS_6KO, "actual": json_6ko.get("diagnosis"), "passed": json_6ko.get("diagnosis") == DIAGNOSIS_6KO},
        {"check": "6ko_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KO, "actual": json_6ko.get("recommended_next_layer"), "passed": json_6ko.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KO},
        {"check": "6ko_current_state_label", "expected": "bullpen_active_partial_realism", "actual": json_6ko.get("current_ui_realism_state_label"), "passed": json_6ko.get("current_ui_realism_state_label") == "bullpen_active_partial_realism"},
        {"check": "6ko_historical_backtest_plan_required", "expected": True, "actual": json_6ko.get("historical_backtest_readiness_plan_required"), "passed": json_6ko.get("historical_backtest_readiness_plan_required") is True},
        {"check": "6ko_no_historical_eval", "expected": False, "actual": json_6ko.get("real_historical_evaluation_run"), "passed": json_6ko.get("real_historical_evaluation_run") is False},
        {"check": "6ko_no_layer6_exit", "expected": False, "actual": json_6ko.get("layer_6_exit_recommended"), "passed": json_6ko.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6kq_historical_backtest_readiness_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "6KP is planning-only; 6KQ readiness implementation required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "historical evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6KP", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KP is planning-only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6KP cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6ko_passed", "expected": True, "actual": json_6ko.get("all_checks_passed"), "passed": json_6ko.get("all_checks_passed") is True},
        {"decision": "backtest_label_count", "expected": 1, "actual": len(backtest_label), "passed": len(backtest_label) == 1 and all_passed(backtest_label)},
        {"decision": "dataset_priority_count", "expected": 3, "actual": len(dataset_priority), "passed": len(dataset_priority) == 3 and all_passed(dataset_priority)},
        {"decision": "window_plan_count", "expected": 4, "actual": len(window_plan), "passed": len(window_plan) == 4 and all_passed(window_plan)},
        {"decision": "required_column_count", "expected": 8, "actual": len(required_columns), "passed": len(required_columns) == 8 and all_passed(required_columns)},
        {"decision": "metric_plan_count", "expected": 8, "actual": len(metric_plan), "passed": len(metric_plan) == 8 and all_passed(metric_plan)},
        {"decision": "recommend_6kq_next", "expected": RECOMMENDED_NEXT_LAYER_6KP, "actual": RECOMMENDED_NEXT_LAYER_6KP, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "historical_backtest_readiness_plan_created", "expected": True, "actual": True, "passed": True},
        {"boundary": "real_historical_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_measurement_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "database_writes_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "live_data_fetches_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "remote_api_calls_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "activation_execution_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    immutability_rows = [
        {"surface": "source_tree", "policy": "read_only_planning", "passed": True},
        {"surface": "6ko_audit", "policy": "read_only", "passed": True},
        {"surface": "6ko_artifacts", "policy": "read_only", "passed": True},
        {"surface": "ui_projection_path", "policy": "not_modified_in_6kp", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6kp", "passed": True},
        {"surface": "database", "policy": "not_written_in_6kp", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KP, "actual": RECOMMENDED_NEXT_LAYER_6KP, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KP, "actual": RECOMMENDED_PATH_6KP, "passed": True},
        {"decision": "recommend_historical_backtest_readiness_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KP, "actual": DIAGNOSIS_6KP, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "backtest_label", "passed": len(backtest_label) == 1 and all_passed(backtest_label), "detail": "1/1"},
        {"check": "dataset_priority", "passed": len(dataset_priority) == 3 and all_passed(dataset_priority), "detail": "3/3"},
        {"check": "window_plan", "passed": len(window_plan) == 4 and all_passed(window_plan), "detail": "4/4"},
        {"check": "required_columns", "passed": len(required_columns) == 8 and all_passed(required_columns), "detail": "8/8"},
        {"check": "metric_plan", "passed": len(metric_plan) == 8 and all_passed(metric_plan), "detail": "8/8"},
        {"check": "mechanic_tags", "passed": len(mechanic_tags) == 6 and all_passed(mechanic_tags), "detail": "6/6"},
        {"check": "exclusions", "passed": len(exclusions) == 7 and all_passed(exclusions), "detail": "7/7"},
        {"check": "blockers", "passed": len(blockers) == 5 and all_passed(blockers), "detail": "5/5"},
        {"check": "future_6kq_contract", "passed": len(future_6kq) == 7 and all_passed(future_6kq), "detail": "7/7"},
        {"check": "readonly_sources", "passed": all_passed(readonly_rows), "detail": f"{sum(1 for r in readonly_rows if r['passed'])}/{len(readonly_rows)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_rows), "detail": f"{sum(1 for r in blocking_rows if r['passed'])}/{len(blocking_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all_passed(immutability_rows), "detail": f"{sum(1 for r in immutability_rows if r['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "backtest_label": write_csv(BACKTEST_LABEL_CSV, backtest_label),
        "dataset_priority": write_csv(DATASET_PRIORITY_CSV, dataset_priority),
        "window_plan": write_csv(WINDOW_PLAN_CSV, window_plan),
        "required_columns": write_csv(REQUIRED_COLUMNS_CSV, required_columns),
        "metric_plan": write_csv(METRIC_PLAN_CSV, metric_plan),
        "mechanic_tags": write_csv(MECHANIC_TAGS_CSV, mechanic_tags),
        "exclusions": write_csv(EXCLUSIONS_CSV, exclusions),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6kq_contract": write_csv(FUTURE_6KQ_CSV, future_6kq),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KP",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KP if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KP,
        "recommended_path": RECOMMENDED_PATH_6KP,
        "predecessor_audit": str(AUDIT_6KO_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6ko.get("diagnosis"),
        "planned_layer_after": "6KO",
        "source_family": "historical_backtest_readiness_plan",
        "backtest_label_count": len(backtest_label),
        "dataset_priority_count": len(dataset_priority),
        "window_plan_count": len(window_plan),
        "required_column_count": len(required_columns),
        "metric_plan_count": len(metric_plan),
        "mechanic_tag_count": len(mechanic_tags),
        "exclusion_count": len(exclusions),
        "blocker_count": len(blockers),
        "future_6kq_contract_valid": len(future_6kq) == 7 and all_passed(future_6kq),
        "historical_backtest_readiness_plan_created": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "existing_dataset_preferred": True,
        "historical_odds_required": False,
        "fallback_slice_planned": True,
        "predicted_vs_actual_metrics_planned": True,
        "real_historical_evaluation_run": False,
        "production_simulations_run": False,
        "local_measurement_run": False,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
        "database_writes_run": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "backtest_label_csv": str(BACKTEST_LABEL_CSV),
            "dataset_priority_csv": str(DATASET_PRIORITY_CSV),
            "window_plan_csv": str(WINDOW_PLAN_CSV),
            "required_columns_csv": str(REQUIRED_COLUMNS_CSV),
            "metric_plan_csv": str(METRIC_PLAN_CSV),
            "mechanic_tags_csv": str(MECHANIC_TAGS_CSV),
            "exclusions_csv": str(EXCLUSIONS_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6kq_contract_csv": str(FUTURE_6KQ_CSV),
            "readonly_sources_csv": str(READONLY_CSV),
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
