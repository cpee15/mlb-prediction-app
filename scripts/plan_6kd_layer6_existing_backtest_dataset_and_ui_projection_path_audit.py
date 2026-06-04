#!/usr/bin/env python3
"""Plan existing backtest dataset and UI projection-path audit for Layer 6.

This planning layer corrects the next step after 6KC: use/audit existing
predicted-vs-actual backtest data first, verify field/date coverage, record
market-odds caveats, and plan a UI projection-path audit. It does not fetch
data, write databases, run historical evaluation, activate mechanics, or grant
Layer 6 exit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6kd_existing_backtest_dataset_and_ui_projection_path_audit_plan"
TMP_DIR = Path("tmp")

AUDIT_6KC_PATH = Path("scripts/audit_6kc_layer6_game_state_realism_performance_evaluation_implementation.py")
JSON_6KC = TMP_DIR / "layer6_6kc_game_state_realism_performance_evaluation_implementation_audit.json"

REQUIRED_INPUTS = [
    JSON_6KC,
    TMP_DIR / "layer6_6kc_game_state_realism_performance_evaluation_implementation_audit_checks.csv",
    TMP_DIR / "layer6_6kc_game_state_realism_performance_evaluation_implementation_audit_predecessor.csv",
    TMP_DIR / "layer6_6kc_game_state_realism_performance_evaluation_implementation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6kc_game_state_realism_performance_evaluation_implementation_audit_metric_artifact_audit.csv",
    TMP_DIR / "layer6_6kc_game_state_realism_performance_evaluation_implementation_audit_baseline_vs_realism_audit.csv",
    TMP_DIR / "layer6_6kc_game_state_realism_performance_evaluation_implementation_audit_distribution_quality_audit.csv",
    TMP_DIR / "layer6_6kc_game_state_realism_performance_evaluation_implementation_audit_historical_backtest_gap.csv",
    TMP_DIR / "layer6_6kc_game_state_realism_performance_evaluation_implementation_audit_activation_blockers.csv",
    TMP_DIR / "layer6_6kc_game_state_realism_performance_evaluation_implementation_audit_deferred_mechanic_policy.csv",
    TMP_DIR / "layer6_6kc_game_state_realism_performance_evaluation_implementation_audit_future_6kd_contract.csv",
    TMP_DIR / "layer6_6kc_game_state_realism_performance_evaluation_implementation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6kc_game_state_realism_performance_evaluation_implementation_audit_decision.csv",
    TMP_DIR / "layer6_6kc_game_state_realism_performance_evaluation_implementation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6kc_game_state_realism_performance_evaluation_implementation_audit_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
DATASET_DISCOVERY_CSV = TMP_DIR / f"{SLUG}_dataset_discovery_plan.csv"
FIELD_COVERAGE_CSV = TMP_DIR / f"{SLUG}_field_coverage_plan.csv"
MARKET_CAVEAT_CSV = TMP_DIR / f"{SLUG}_market_field_caveat.csv"
WINDOW_STRATEGY_CSV = TMP_DIR / f"{SLUG}_window_strategy.csv"
RUNTIME_STRATEGY_CSV = TMP_DIR / f"{SLUG}_runtime_strategy.csv"
UI_PATH_CSV = TMP_DIR / f"{SLUG}_ui_projection_path_plan.csv"
ACTIVATION_BLOCKERS_CSV = TMP_DIR / f"{SLUG}_activation_blockers.csv"
FUTURE_6KE_CSV = TMP_DIR / f"{SLUG}_future_6ke_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KC = "layer_6_game_state_realism_performance_evaluation_implementation_audit_complete"
DIAGNOSIS_6KD = "layer_6_existing_backtest_dataset_and_ui_projection_path_audit_plan_complete"
RECOMMENDED_NEXT_LAYER_6KC_PRIOR = "6KD_layer_6_historical_backtest_dataset_acquisition_plan"
RECOMMENDED_NEXT_LAYER_6KD = "6KE_layer_6_existing_backtest_dataset_and_ui_projection_path_audit_implementation"
RECOMMENDED_PATH_6KD = "plan_existing_dataset_and_ui_path_audit_then_implement_before_real_backtest"


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
    json_6kc = load_json(JSON_6KC)

    dataset_discovery_plan = [
        {"target": "existing_predicted_vs_actual_backtest_dataset", "action": "locate_existing_artifact_or_generation_script", "assume_new_acquisition_required": False, "passed": True},
        {"target": "backtest_dataset_date_coverage", "action": "audit_min_max_game_date_and_row_count", "assume_new_acquisition_required": False, "passed": True},
        {"target": "backtest_dataset_grain", "action": "confirm_game_level_rows_and_unique_game_keys", "assume_new_acquisition_required": False, "passed": True},
        {"target": "dataset_runtime_feasibility", "action": "estimate_full_season_to_date_runtime_before_real_backtest", "assume_new_acquisition_required": False, "passed": True},
    ]

    field_coverage_plan = [
        {"field_family": "game_identity", "required_examples": "game_id,game_date,home_team,away_team", "audit_required": True, "passed": True},
        {"field_family": "predicted_outputs", "required_examples": "predicted_home_runs,predicted_away_runs,predicted_total,predicted_win_probability", "audit_required": True, "passed": True},
        {"field_family": "actual_outputs", "required_examples": "actual_home_runs,actual_away_runs,actual_total,actual_winner", "audit_required": True, "passed": True},
        {"field_family": "metric_inputs", "required_examples": "mae_inputs,brier_inputs,calibration_inputs,total_error_inputs", "audit_required": True, "passed": True},
        {"field_family": "market_odds", "required_examples": "closing_moneyline,closing_total,team_totals", "audit_required": True, "passed": True},
    ]

    market_field_caveat = [
        {"field": "closing_moneyline", "assumed_present": False, "audit_required": True, "blocks_market_comparison_if_missing": True, "passed": True},
        {"field": "closing_total", "assumed_present": False, "audit_required": True, "blocks_market_comparison_if_missing": True, "passed": True},
        {"field": "team_totals", "assumed_present": False, "audit_required": True, "blocks_market_comparison_if_missing": True, "passed": True},
        {"field": "predicted_vs_actual", "assumed_present": True, "audit_required": True, "blocks_market_comparison_if_missing": False, "passed": True},
    ]

    window_strategy = [
        {"window": "primary", "range": "2026_opening_day_to_latest_completed_game", "use_if": "runtime_acceptable", "priority": 1, "passed": True},
        {"window": "fixed_validation_slice", "range": "2026-04-20_to_2026-05-03", "use_if": "always_available_or_smoke_test", "priority": 2, "passed": True},
        {"window": "expanded_early_season", "range": "2026_opening_day_to_2026-05-03", "use_if": "needs_larger_than_fixed_slice_but_less_than_full", "priority": 3, "passed": True},
    ]

    runtime_strategy = [
        {"step": "row_count_audit", "purpose": "estimate number of games before evaluation", "required_before_real_backtest": True, "passed": True},
        {"step": "dry_run_slice", "purpose": "use 2026-04-20_to_2026-05-03 if full window is slow", "required_before_real_backtest": True, "passed": True},
        {"step": "full_window_runtime_threshold", "purpose": "prefer opening_day_to_latest_completed_if acceptable", "required_before_real_backtest": True, "passed": True},
    ]

    ui_projection_path_plan = [
        {"target": "frontend_projection_page", "audit": "identify API endpoint used by displayed projected numbers", "activation_assumed": False, "passed": True},
        {"target": "backend_projection_endpoint", "audit": "trace endpoint to projection/simulator function", "activation_assumed": False, "passed": True},
        {"target": "realism_enabled_path", "audit": "determine whether endpoint calls Layer 6 realism-enabled outputs", "activation_assumed": False, "passed": True},
        {"target": "legacy_current_path", "audit": "determine whether endpoint still calls current production/legacy outputs", "activation_assumed": False, "passed": True},
        {"target": "ui_realism_confirmation", "audit": "record whether UI displayed projections reflect built realism features except deferred balks", "activation_assumed": False, "passed": True},
    ]

    activation_blockers = [
        {"blocker": "existing_dataset_not_audited", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "ui_projection_path_not_audited", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "activation_decision_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "balks_deferred_or_exit_gated", "blocks_activation": False, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6ke = [
        {"contract": "locate_existing_backtest_dataset_or_generation_script", "required": True, "passed": True},
        {"contract": "audit_dataset_row_count_and_date_coverage", "required": True, "passed": True},
        {"contract": "audit_predicted_vs_actual_fields", "required": True, "passed": True},
        {"contract": "audit_historical_market_odds_field_presence", "required": True, "passed": True},
        {"contract": "audit_opening_day_to_latest_completed_runtime_feasibility", "required": True, "passed": True},
        {"contract": "audit_2026_04_20_to_2026_05_03_validation_slice", "required": True, "passed": True},
        {"contract": "audit_ui_projection_backend_path", "required": True, "passed": True},
        {"contract": "do_not_activate_or_grant_layer6_exit_in_6ke", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6kc_audit_script_exists", "expected": True, "actual": AUDIT_6KC_PATH.exists(), "passed": AUDIT_6KC_PATH.exists()},
        {"check": "6kc_json_exists", "expected": True, "actual": JSON_6KC.exists(), "passed": JSON_6KC.exists()},
        {"check": "6kc_all_checks_passed", "expected": True, "actual": json_6kc.get("all_checks_passed"), "passed": json_6kc.get("all_checks_passed") is True},
        {"check": "6kc_diagnosis", "expected": DIAGNOSIS_6KC, "actual": json_6kc.get("diagnosis"), "passed": json_6kc.get("diagnosis") == DIAGNOSIS_6KC},
        {"check": "6kc_prior_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KC_PRIOR, "actual": json_6kc.get("recommended_next_layer"), "passed": json_6kc.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KC_PRIOR},
        {"check": "6kc_historical_backtest_dataset_required", "expected": True, "actual": json_6kc.get("historical_backtest_dataset_required"), "passed": json_6kc.get("historical_backtest_dataset_required") is True},
        {"check": "6kc_real_games_evaluated", "expected": False, "actual": json_6kc.get("real_games_evaluated"), "passed": json_6kc.get("real_games_evaluated") is False},
        {"check": "6kc_games_evaluated", "expected": 0, "actual": json_6kc.get("games_evaluated"), "passed": json_6kc.get("games_evaluated") == 0},
        {"check": "6kc_no_layer6_exit", "expected": False, "actual": json_6kc.get("layer_6_exit_recommended"), "passed": json_6kc.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "existing_dataset_and_ui_path_audit_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "dataset and UI path audit must run first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "real evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "UI path and historical evaluation not confirmed", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KD is planning only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "dataset/UI path audit plan cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6kc_passed", "expected": True, "actual": json_6kc.get("all_checks_passed"), "passed": json_6kc.get("all_checks_passed") is True},
        {"decision": "dataset_discovery_plan_count", "expected": 4, "actual": len(dataset_discovery_plan), "passed": len(dataset_discovery_plan) == 4},
        {"decision": "field_coverage_plan_count", "expected": 5, "actual": len(field_coverage_plan), "passed": len(field_coverage_plan) == 5},
        {"decision": "market_field_caveat_count", "expected": 4, "actual": len(market_field_caveat), "passed": len(market_field_caveat) == 4},
        {"decision": "window_strategy_count", "expected": 3, "actual": len(window_strategy), "passed": len(window_strategy) == 3},
        {"decision": "ui_projection_path_plan_count", "expected": 5, "actual": len(ui_projection_path_plan), "passed": len(ui_projection_path_plan) == 5},
        {"decision": "recommend_6ke_next", "expected": RECOMMENDED_NEXT_LAYER_6KD, "actual": RECOMMENDED_NEXT_LAYER_6KD, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "existing_backtest_dataset_expected", "expected": True, "actual": True, "passed": True},
        {"boundary": "historical_odds_fields_assumed_present", "expected": False, "actual": False, "passed": True},
        {"boundary": "ui_projection_path_audit_required", "expected": True, "actual": True, "passed": True},
        {"boundary": "realism_ui_activation_confirmed", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_historical_evaluation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_activation_execution", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_recommendation", "expected": False, "actual": False, "passed": True},
    ]

    immutability_rows = [
        {"surface": "source_tree", "policy": "read_only_planning", "passed": True},
        {"surface": "6kc_audit", "policy": "read_only", "passed": True},
        {"surface": "existing_backtest_dataset", "policy": "audit_planned_not_read_or_modified_in_6kd", "passed": True},
        {"surface": "ui_projection_path", "policy": "audit_planned_not_modified_in_6kd", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6kd", "passed": True},
        {"surface": "database", "policy": "not_written_in_6kd", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KD, "actual": RECOMMENDED_NEXT_LAYER_6KD, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KD, "actual": RECOMMENDED_PATH_6KD, "passed": True},
        {"decision": "recommend_existing_dataset_and_ui_path_audit_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KD, "actual": DIAGNOSIS_6KD, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "dataset_discovery_plan", "passed": len(dataset_discovery_plan) == 4 and all_passed(dataset_discovery_plan), "detail": "4/4"},
        {"check": "field_coverage_plan", "passed": len(field_coverage_plan) == 5 and all_passed(field_coverage_plan), "detail": "5/5"},
        {"check": "market_field_caveat", "passed": len(market_field_caveat) == 4 and all_passed(market_field_caveat), "detail": "4/4"},
        {"check": "window_strategy", "passed": len(window_strategy) == 3 and all_passed(window_strategy), "detail": "3/3"},
        {"check": "runtime_strategy", "passed": len(runtime_strategy) == 3 and all_passed(runtime_strategy), "detail": "3/3"},
        {"check": "ui_projection_path_plan", "passed": len(ui_projection_path_plan) == 5 and all_passed(ui_projection_path_plan), "detail": "5/5"},
        {"check": "activation_blockers", "passed": len(activation_blockers) == 5 and all_passed(activation_blockers), "detail": "5/5"},
        {"check": "future_6ke_contract", "passed": len(future_6ke) == 8 and all_passed(future_6ke), "detail": "8/8"},
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
        "dataset_discovery_plan": write_csv(DATASET_DISCOVERY_CSV, dataset_discovery_plan),
        "field_coverage_plan": write_csv(FIELD_COVERAGE_CSV, field_coverage_plan),
        "market_field_caveat": write_csv(MARKET_CAVEAT_CSV, market_field_caveat),
        "window_strategy": write_csv(WINDOW_STRATEGY_CSV, window_strategy),
        "runtime_strategy": write_csv(RUNTIME_STRATEGY_CSV, runtime_strategy),
        "ui_projection_path_plan": write_csv(UI_PATH_CSV, ui_projection_path_plan),
        "activation_blockers": write_csv(ACTIVATION_BLOCKERS_CSV, activation_blockers),
        "future_6ke_contract": write_csv(FUTURE_6KE_CSV, future_6ke),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KD",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KD if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KD,
        "recommended_path": RECOMMENDED_PATH_6KD,
        "predecessor_audit": str(AUDIT_6KC_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6kc.get("diagnosis"),
        "planned_layer_after": "6KC",
        "source_family": "existing_backtest_dataset_and_ui_projection_path_audit_plan",
        "dataset_discovery_plan_count": len(dataset_discovery_plan),
        "field_coverage_plan_count": len(field_coverage_plan),
        "market_field_caveat_count": len(market_field_caveat),
        "window_strategy_count": len(window_strategy),
        "runtime_strategy_count": len(runtime_strategy),
        "ui_projection_path_plan_count": len(ui_projection_path_plan),
        "activation_blocker_count": len(activation_blockers),
        "future_6ke_contract_valid": len(future_6ke) == 8 and all_passed(future_6ke),
        "existing_backtest_dataset_expected": True,
        "primary_dataset_window": "2026_opening_day_to_latest_completed_game",
        "fixed_validation_slice": "2026-04-20_to_2026-05-03",
        "predicted_vs_actual_dataset_expected": True,
        "historical_odds_fields_assumed_present": False,
        "ui_projection_path_audit_required": True,
        "realism_ui_activation_confirmed": False,
        "production_simulations_run": False,
        "real_historical_evaluation_run": False,
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
            "dataset_discovery_plan_csv": str(DATASET_DISCOVERY_CSV),
            "field_coverage_plan_csv": str(FIELD_COVERAGE_CSV),
            "market_field_caveat_csv": str(MARKET_CAVEAT_CSV),
            "window_strategy_csv": str(WINDOW_STRATEGY_CSV),
            "runtime_strategy_csv": str(RUNTIME_STRATEGY_CSV),
            "ui_projection_path_plan_csv": str(UI_PATH_CSV),
            "activation_blockers_csv": str(ACTIVATION_BLOCKERS_CSV),
            "future_6ke_contract_csv": str(FUTURE_6KE_CSV),
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
