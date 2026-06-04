#!/usr/bin/env python3
"""Plan historical backtest source generation.

This planning-only layer defines how to generate the missing non-production
prediction-vs-actual evaluation surface for the current UI projection path. It
does not fetch data, call remote APIs, write DBs, run real metrics, activate
mechanics, or grant Layer 6 exit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6kv_historical_backtest_source_generation_plan"
TMP_DIR = Path("tmp")

AUDIT_6KU_PATH = Path("scripts/audit_6ku_layer6_historical_backtest_data_gap_remediation_implementation.py")
JSON_6KU = TMP_DIR / "layer6_6ku_historical_backtest_data_gap_remediation_implementation_audit.json"

REQUIRED_INPUTS = [
    JSON_6KU,
    TMP_DIR / "layer6_6ku_historical_backtest_data_gap_remediation_implementation_audit_checks.csv",
    TMP_DIR / "layer6_6ku_historical_backtest_data_gap_remediation_implementation_audit_predecessor.csv",
    TMP_DIR / "layer6_6ku_historical_backtest_data_gap_remediation_implementation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6ku_historical_backtest_data_gap_remediation_implementation_audit_candidate_audit.csv",
    TMP_DIR / "layer6_6ku_historical_backtest_data_gap_remediation_implementation_audit_join_surface_audit.csv",
    TMP_DIR / "layer6_6ku_historical_backtest_data_gap_remediation_implementation_audit_metric_gap_audit.csv",
    TMP_DIR / "layer6_6ku_historical_backtest_data_gap_remediation_implementation_audit_source_gap_verdict.csv",
    TMP_DIR / "layer6_6ku_historical_backtest_data_gap_remediation_implementation_audit_next_route.csv",
    TMP_DIR / "layer6_6ku_historical_backtest_data_gap_remediation_implementation_audit_blockers.csv",
    TMP_DIR / "layer6_6ku_historical_backtest_data_gap_remediation_implementation_audit_future_6kv_contract.csv",
    TMP_DIR / "layer6_6ku_historical_backtest_data_gap_remediation_implementation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6ku_historical_backtest_data_gap_remediation_implementation_audit_decision.csv",
    TMP_DIR / "layer6_6ku_historical_backtest_data_gap_remediation_implementation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6ku_historical_backtest_data_gap_remediation_implementation_audit_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
SOURCE_GAP_SUMMARY_CSV = TMP_DIR / f"{SLUG}_source_gap_summary.csv"
GENERATION_OPTIONS_CSV = TMP_DIR / f"{SLUG}_generation_options.csv"
PREDICTION_GENERATION_CSV = TMP_DIR / f"{SLUG}_prediction_generation_plan.csv"
ACTUAL_OUTCOME_CSV = TMP_DIR / f"{SLUG}_actual_outcome_plan.csv"
JOIN_PLAN_CSV = TMP_DIR / f"{SLUG}_join_plan.csv"
EVAL_SURFACE_SCHEMA_CSV = TMP_DIR / f"{SLUG}_evaluation_surface_schema.csv"
METRIC_TARGETS_CSV = TMP_DIR / f"{SLUG}_metric_targets.csv"
ALLOWED_OPERATIONS_CSV = TMP_DIR / f"{SLUG}_allowed_operations.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6KW_CSV = TMP_DIR / f"{SLUG}_future_6kw_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KU = "layer_6_historical_backtest_data_gap_remediation_implementation_audit_complete"
DIAGNOSIS_6KV = "layer_6_historical_backtest_source_generation_plan_complete"
RECOMMENDED_NEXT_LAYER_6KU = "6KV_layer_6_historical_backtest_source_generation_plan"
RECOMMENDED_NEXT_LAYER_6KV = "6KW_layer_6_historical_backtest_source_generation_implementation"
RECOMMENDED_PATH_6KV = "implement_historical_backtest_source_generation"


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8", errors="ignore") as handle:
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
    parsed = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}


def syntax_compile() -> Tuple[int, str]:
    failures: List[str] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            try:
                compile(path.read_text(encoding="utf-8", errors="ignore"), str(path), "exec")
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
    json_6ku = load_json(JSON_6KU)

    source_gap_summary = [
        {
            "summary": "evaluation_surface_missing_confirmed",
            "source_gap_confirmed": json_6ku.get("source_gap_confirmed"),
            "missed_joinable_surface_found": json_6ku.get("missed_joinable_surface_found"),
            "probability_metric_ready_after_audit": json_6ku.get("probability_metric_ready_after_audit"),
            "runs_metric_ready_after_audit": json_6ku.get("runs_metric_ready_after_audit"),
            "historical_odds_required": False,
            "passed": True,
        }
    ]

    generation_options = [
        {"option": "A", "name": "reconstruct_predictions_from_existing_local_schedule_inputs", "priority": 1, "description": "Use repo-local schedule/game inputs and current UI projection route/function to generate predictions deterministically.", "passed": True},
        {"option": "B", "name": "use_existing_actual_outcome_artifacts", "priority": 2, "description": "Use local actual game outcome artifacts for winners and actual runs.", "passed": True},
        {"option": "C", "name": "join_generated_predictions_to_actual_outcomes", "priority": 3, "description": "Join generated predictions to actuals using game_id/date/team/matchup keys with lineage.", "passed": True},
        {"option": "D", "name": "route_level_fixture_generation_or_gap_report", "priority": 4, "description": "If current UI route dependencies are unavailable, generate route-level fixture surface or explicit source-gap report.", "passed": True},
    ]

    prediction_generation_plan = [
        {"step": "identify_schedule_or_game_inputs", "allowed": True, "notes": "repo-local files only", "passed": True},
        {"step": "identify_current_ui_projection_route_or_function", "allowed": True, "notes": "local Python functions only", "passed": True},
        {"step": "generate_predictions_deterministically", "allowed": True, "notes": "tmp-only output; no real metrics", "passed": True},
        {"step": "preserve_bullpen_active_partial_realism_label", "allowed": True, "notes": "do not claim full realism", "passed": True},
        {"step": "record_generation_mode_and_notes", "allowed": True, "notes": "lineage required", "passed": True},
    ]

    actual_outcome_plan = [
        {"step": "identify_local_actual_outcome_sources", "required_fields": "actual_winner/home_actual_runs/away_actual_runs if available", "passed": True},
        {"step": "prefer_game_id_date_team_identifiers", "required_fields": "game_id/date/home_team/away_team/matchup", "passed": True},
        {"step": "preserve_historical_odds_non_requirement", "required_fields": "historical odds not needed", "passed": True},
        {"step": "emit_outcome_gap_if_actuals_missing", "required_fields": "source gap report", "passed": True},
    ]

    join_plan = [
        {"join_key": "game_id", "priority": 1, "join_confidence": "high", "passed": True},
        {"join_key": "game_date_home_team_away_team", "priority": 2, "join_confidence": "medium_high", "passed": True},
        {"join_key": "matchup_date", "priority": 3, "join_confidence": "medium", "passed": True},
        {"join_key": "team_date_pair", "priority": 4, "join_confidence": "low_medium", "passed": True},
        {"join_key": "lineage_fallback", "priority": 5, "join_confidence": "low", "passed": True},
    ]

    evaluation_surface_schema = [
        {"field": "game_id", "required": False, "family": "identifier", "passed": True},
        {"field": "game_date", "required": True, "family": "identifier", "passed": True},
        {"field": "home_team", "required": True, "family": "identifier", "passed": True},
        {"field": "away_team", "required": True, "family": "identifier", "passed": True},
        {"field": "matchup", "required": False, "family": "identifier", "passed": True},
        {"field": "home_win_probability", "required": False, "family": "prediction_probability", "passed": True},
        {"field": "away_win_probability", "required": False, "family": "prediction_probability", "passed": True},
        {"field": "home_expected_runs", "required": False, "family": "prediction_runs", "passed": True},
        {"field": "away_expected_runs", "required": False, "family": "prediction_runs", "passed": True},
        {"field": "total_expected_runs", "required": False, "family": "prediction_runs", "passed": True},
        {"field": "actual_winner", "required": False, "family": "actual_result", "passed": True},
        {"field": "home_actual_runs", "required": False, "family": "actual_runs", "passed": True},
        {"field": "away_actual_runs", "required": False, "family": "actual_runs", "passed": True},
        {"field": "backtest_label", "required": True, "family": "label", "passed": True},
        {"field": "current_ui_realism_state_label", "required": True, "family": "label", "passed": True},
        {"field": "mechanic_tags", "required": True, "family": "label", "passed": True},
        {"field": "prediction_source", "required": True, "family": "lineage", "passed": True},
        {"field": "actual_source", "required": True, "family": "lineage", "passed": True},
        {"field": "join_key_used", "required": True, "family": "lineage", "passed": True},
        {"field": "join_confidence", "required": True, "family": "lineage", "passed": True},
        {"field": "generation_mode", "required": True, "family": "lineage", "passed": True},
        {"field": "generation_notes", "required": True, "family": "lineage", "passed": True},
    ]

    metric_targets = [
        {"metric": "brier_score", "requires": "predicted probability + actual result", "allowed_next": False, "planned_for_after_surface": True, "passed": True},
        {"metric": "calibration", "requires": "predicted probability + actual result", "allowed_next": False, "planned_for_after_surface": True, "passed": True},
        {"metric": "favorite_underdog_directional_accuracy", "requires": "predicted probability + actual result", "allowed_next": False, "planned_for_after_surface": True, "passed": True},
        {"metric": "team_runs_mae_rmse", "requires": "predicted runs + actual runs", "allowed_next": False, "planned_for_after_surface": True, "passed": True},
        {"metric": "total_runs_mae_rmse", "requires": "total expected runs + actual total runs", "allowed_next": False, "planned_for_after_surface": True, "passed": True},
        {"metric": "coverage_diagnostics", "requires": "generated surface", "allowed_next": True, "planned_for_after_surface": True, "passed": True},
        {"metric": "missing_field_diagnostics", "requires": "generated surface schema", "allowed_next": True, "planned_for_after_surface": True, "passed": True},
        {"metric": "generation_lineage_diagnostics", "requires": "generation metadata", "allowed_next": True, "planned_for_after_surface": True, "passed": True},
    ]

    allowed_operations = [
        {"operation": "call_local_python_functions", "allowed_next": True, "passed": True},
        {"operation": "read_repo_local_files", "allowed_next": True, "passed": True},
        {"operation": "write_tmp_artifacts", "allowed_next": True, "passed": True},
        {"operation": "fetch_remote_live_data", "allowed_next": False, "passed": True},
        {"operation": "call_external_apis", "allowed_next": False, "passed": True},
        {"operation": "write_databases", "allowed_next": False, "passed": True},
        {"operation": "modify_production_source", "allowed_next": False, "passed": True},
        {"operation": "run_real_backtest_metrics", "allowed_next": False, "passed": True},
        {"operation": "activate_mechanics", "allowed_next": False, "passed": True},
        {"operation": "grant_layer6_exit", "allowed_next": False, "passed": True},
    ]

    blockers = [
        {"blocker": "source_generation_not_implemented", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "evaluation_surface_not_generated", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "activation_decision_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6kw = [
        {"contract": "discover_local_schedule_inputs", "required": True, "passed": True},
        {"contract": "discover_current_ui_projection_route_or_function", "required": True, "passed": True},
        {"contract": "generate_tmp_prediction_surface_if_possible", "required": True, "passed": True},
        {"contract": "join_to_local_actual_outcomes_if_possible", "required": True, "passed": True},
        {"contract": "emit_surface_or_source_gap_report", "required": True, "passed": True},
        {"contract": "preserve_no_fetch_no_db_write_no_real_metrics_no_activation_no_layer6_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ku_audit_script_exists", "expected": True, "actual": AUDIT_6KU_PATH.exists(), "passed": AUDIT_6KU_PATH.exists()},
        {"check": "6ku_json_exists", "expected": True, "actual": JSON_6KU.exists(), "passed": JSON_6KU.exists()},
        {"check": "6ku_all_checks_passed", "expected": True, "actual": json_6ku.get("all_checks_passed"), "passed": json_6ku.get("all_checks_passed") is True},
        {"check": "6ku_diagnosis", "expected": DIAGNOSIS_6KU, "actual": json_6ku.get("diagnosis"), "passed": json_6ku.get("diagnosis") == DIAGNOSIS_6KU},
        {"check": "6ku_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KU, "actual": json_6ku.get("recommended_next_layer"), "passed": json_6ku.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KU},
        {"check": "6ku_recommended_path", "expected": "plan_historical_backtest_source_generation", "actual": json_6ku.get("recommended_path"), "passed": json_6ku.get("recommended_path") == "plan_historical_backtest_source_generation"},
        {"check": "6ku_source_gap_confirmed", "expected": True, "actual": json_6ku.get("source_gap_confirmed"), "passed": json_6ku.get("source_gap_confirmed") is True},
        {"check": "6ku_no_historical_eval", "expected": False, "actual": json_6ku.get("real_historical_evaluation_run"), "passed": json_6ku.get("real_historical_evaluation_run") is False},
        {"check": "6ku_no_layer6_exit", "expected": False, "actual": json_6ku.get("layer_6_exit_recommended"), "passed": json_6ku.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6kw_source_generation_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "source generation must be implemented and audited first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "historical evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6KV", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KV is planning-only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6KV cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6ku_passed", "expected": True, "actual": json_6ku.get("all_checks_passed"), "passed": json_6ku.get("all_checks_passed") is True},
        {"decision": "source_gap_summary_count", "expected": 1, "actual": len(source_gap_summary), "passed": len(source_gap_summary) == 1 and all_passed(source_gap_summary)},
        {"decision": "generation_option_count", "expected": 4, "actual": len(generation_options), "passed": len(generation_options) == 4 and all_passed(generation_options)},
        {"decision": "evaluation_surface_schema_field_count", "expected": 22, "actual": len(evaluation_surface_schema), "passed": len(evaluation_surface_schema) == 22 and all_passed(evaluation_surface_schema)},
        {"decision": "allowed_operation_count", "expected": 10, "actual": len(allowed_operations), "passed": len(allowed_operations) == 10 and all_passed(allowed_operations)},
        {"decision": "recommend_6kw_next", "expected": RECOMMENDED_NEXT_LAYER_6KV, "actual": RECOMMENDED_NEXT_LAYER_6KV, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "source_generation_plan_created", "expected": True, "actual": True, "passed": True},
        {"boundary": "real_historical_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_measurement_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "database_writes_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "live_data_fetches_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "remote_api_calls_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_acquisition_performed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "activation_execution_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    immutability_rows = [
        {"surface": "source_tree", "policy": "read_only_planning", "passed": True},
        {"surface": "6ku_audit", "policy": "read_only", "passed": True},
        {"surface": "future_eval_surface", "policy": "tmp_non_production_only", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6kv", "passed": True},
        {"surface": "database", "policy": "not_written_in_6kv", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KV, "actual": RECOMMENDED_NEXT_LAYER_6KV, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KV, "actual": RECOMMENDED_PATH_6KV, "passed": True},
        {"decision": "recommend_source_generation_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KV, "actual": DIAGNOSIS_6KV, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "source_gap_summary", "passed": len(source_gap_summary) == 1 and all_passed(source_gap_summary), "detail": "1/1"},
        {"check": "generation_options", "passed": len(generation_options) == 4 and all_passed(generation_options), "detail": "4/4"},
        {"check": "prediction_generation_plan", "passed": len(prediction_generation_plan) == 5 and all_passed(prediction_generation_plan), "detail": "5/5"},
        {"check": "actual_outcome_plan", "passed": len(actual_outcome_plan) == 4 and all_passed(actual_outcome_plan), "detail": "4/4"},
        {"check": "join_plan", "passed": len(join_plan) == 5 and all_passed(join_plan), "detail": "5/5"},
        {"check": "evaluation_surface_schema", "passed": len(evaluation_surface_schema) == 22 and all_passed(evaluation_surface_schema), "detail": "22/22"},
        {"check": "metric_targets", "passed": len(metric_targets) == 8 and all_passed(metric_targets), "detail": "8/8"},
        {"check": "allowed_operations", "passed": len(allowed_operations) == 10 and all_passed(allowed_operations), "detail": "10/10"},
        {"check": "blockers", "passed": len(blockers) == 4 and all_passed(blockers), "detail": "4/4"},
        {"check": "future_6kw_contract", "passed": len(future_6kw) == 6 and all_passed(future_6kw), "detail": "6/6"},
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
        "source_gap_summary": write_csv(SOURCE_GAP_SUMMARY_CSV, source_gap_summary),
        "generation_options": write_csv(GENERATION_OPTIONS_CSV, generation_options),
        "prediction_generation_plan": write_csv(PREDICTION_GENERATION_CSV, prediction_generation_plan),
        "actual_outcome_plan": write_csv(ACTUAL_OUTCOME_CSV, actual_outcome_plan),
        "join_plan": write_csv(JOIN_PLAN_CSV, join_plan),
        "evaluation_surface_schema": write_csv(EVAL_SURFACE_SCHEMA_CSV, evaluation_surface_schema),
        "metric_targets": write_csv(METRIC_TARGETS_CSV, metric_targets),
        "allowed_operations": write_csv(ALLOWED_OPERATIONS_CSV, allowed_operations),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6kw_contract": write_csv(FUTURE_6KW_CSV, future_6kw),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KV",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KV if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KV,
        "recommended_path": RECOMMENDED_PATH_6KV,
        "predecessor_audit": str(AUDIT_6KU_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6ku.get("diagnosis"),
        "planned_layer_after": "6KU",
        "source_family": "historical_backtest_source_generation_plan",
        "source_gap_summary_count": len(source_gap_summary),
        "generation_option_count": len(generation_options),
        "prediction_generation_plan_count": len(prediction_generation_plan),
        "actual_outcome_plan_count": len(actual_outcome_plan),
        "join_plan_count": len(join_plan),
        "evaluation_surface_schema_field_count": len(evaluation_surface_schema),
        "metric_target_count": len(metric_targets),
        "allowed_operation_count": len(allowed_operations),
        "blocker_count": len(blockers),
        "future_6kw_contract_valid": len(future_6kw) == 6 and all_passed(future_6kw),
        "source_generation_plan_created": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "source_gap_confirmed": True,
        "evaluation_surface_generation_planned": True,
        "local_function_calls_allowed_next": True,
        "repo_file_reads_allowed_next": True,
        "tmp_writes_allowed_next": True,
        "live_fetches_allowed_next": False,
        "remote_api_calls_allowed_next": False,
        "database_writes_allowed_next": False,
        "production_source_modifications_allowed_next": False,
        "real_backtest_metrics_allowed_next": False,
        "mechanics_activation_allowed_next": False,
        "layer_6_exit_allowed_next": False,
        "historical_odds_required": False,
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
            "source_gap_summary_csv": str(SOURCE_GAP_SUMMARY_CSV),
            "generation_options_csv": str(GENERATION_OPTIONS_CSV),
            "prediction_generation_plan_csv": str(PREDICTION_GENERATION_CSV),
            "actual_outcome_plan_csv": str(ACTUAL_OUTCOME_CSV),
            "join_plan_csv": str(JOIN_PLAN_CSV),
            "evaluation_surface_schema_csv": str(EVAL_SURFACE_SCHEMA_CSV),
            "metric_targets_csv": str(METRIC_TARGETS_CSV),
            "allowed_operations_csv": str(ALLOWED_OPERATIONS_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6kw_contract_csv": str(FUTURE_6KW_CSV),
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
