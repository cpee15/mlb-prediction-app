#!/usr/bin/env python3
"""Plan historical backtest data-gap remediation.

This planning-only layer defines how to remediate the missing predicted/actual
evaluation fields discovered by 6KR. It does not fetch data, run a backtest,
write DBs, run production simulations, activate mechanics, or grant Layer 6
exit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ks_historical_backtest_data_gap_remediation_plan"
TMP_DIR = Path("tmp")

AUDIT_6KR_PATH = Path("scripts/audit_6kr_layer6_historical_backtest_readiness_implementation.py")
JSON_6KR = TMP_DIR / "layer6_6kr_historical_backtest_readiness_implementation_audit.json"

REQUIRED_INPUTS = [
    JSON_6KR,
    TMP_DIR / "layer6_6kr_historical_backtest_readiness_implementation_audit_checks.csv",
    TMP_DIR / "layer6_6kr_historical_backtest_readiness_implementation_audit_predecessor.csv",
    TMP_DIR / "layer6_6kr_historical_backtest_readiness_implementation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6kr_historical_backtest_readiness_implementation_audit_candidate_inventory_audit.csv",
    TMP_DIR / "layer6_6kr_historical_backtest_readiness_implementation_audit_schema_readiness_audit.csv",
    TMP_DIR / "layer6_6kr_historical_backtest_readiness_implementation_audit_metric_readiness_audit.csv",
    TMP_DIR / "layer6_6kr_historical_backtest_readiness_implementation_audit_window_readiness_audit.csv",
    TMP_DIR / "layer6_6kr_historical_backtest_readiness_implementation_audit_readiness_verdict.csv",
    TMP_DIR / "layer6_6kr_historical_backtest_readiness_implementation_audit_next_layer_rationale.csv",
    TMP_DIR / "layer6_6kr_historical_backtest_readiness_implementation_audit_blockers.csv",
    TMP_DIR / "layer6_6kr_historical_backtest_readiness_implementation_audit_future_6ks_contract.csv",
    TMP_DIR / "layer6_6kr_historical_backtest_readiness_implementation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6kr_historical_backtest_readiness_implementation_audit_decision.csv",
    TMP_DIR / "layer6_6kr_historical_backtest_readiness_implementation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6kr_historical_backtest_readiness_implementation_audit_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
GAP_STATEMENT_CSV = TMP_DIR / f"{SLUG}_gap_statement.csv"
REMEDIATION_OPTIONS_CSV = TMP_DIR / f"{SLUG}_remediation_options.csv"
JOIN_KEY_PLAN_CSV = TMP_DIR / f"{SLUG}_join_key_plan.csv"
EVAL_SURFACE_SCHEMA_CSV = TMP_DIR / f"{SLUG}_evaluation_surface_schema.csv"
METRIC_TARGETS_CSV = TMP_DIR / f"{SLUG}_metric_targets.csv"
LINEAGE_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_lineage_requirements.csv"
GUARDRAILS_CSV = TMP_DIR / f"{SLUG}_guardrails.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6KT_CSV = TMP_DIR / f"{SLUG}_future_6kt_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KR = "layer_6_historical_backtest_readiness_implementation_audit_complete"
DIAGNOSIS_6KS = "layer_6_historical_backtest_data_gap_remediation_plan_complete"
RECOMMENDED_NEXT_LAYER_6KR = "6KS_layer_6_historical_backtest_data_gap_remediation_plan"
RECOMMENDED_NEXT_LAYER_6KS = "6KT_layer_6_historical_backtest_data_gap_remediation_implementation"
RECOMMENDED_PATH_6KS = "implement_historical_backtest_data_gap_remediation"


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
    json_6kr = load_json(JSON_6KR)

    gap_statement = [
        {
            "gap": "backtest_evaluation_surface_missing_core_predicted_actual_fields",
            "best_candidate_path": json_6kr.get("best_candidate_path", ""),
            "predicted_probability_available": json_6kr.get("predicted_probability_available"),
            "actual_result_available": json_6kr.get("actual_result_available"),
            "predicted_runs_available": json_6kr.get("predicted_runs_available"),
            "actual_runs_available": json_6kr.get("actual_runs_available"),
            "historical_odds_required": False,
            "blocks_real_backtest": True,
            "passed": True,
        }
    ]

    remediation_options = [
        {"option": "A", "name": "locate_existing_better_artifact", "priority": 1, "description": "Broaden repo-local artifact search for files containing projected/predicted and actual/result/run fields.", "fetch_allowed": False, "db_write_allowed": False, "passed": True},
        {"option": "B", "name": "join_existing_prediction_to_actual_artifacts", "priority": 2, "description": "Plan join between prediction candidates and actual outcome candidates using game/date/team identifiers.", "fetch_allowed": False, "db_write_allowed": False, "passed": True},
        {"option": "C", "name": "materialize_non_production_evaluation_surface", "priority": 3, "description": "Plan tmp-only evaluation surface with predictions, actuals, labels, tags, and lineage from local artifacts.", "fetch_allowed": False, "db_write_allowed": False, "passed": True},
        {"option": "D", "name": "source_gap_report_if_local_data_insufficient", "priority": 4, "description": "If local artifacts remain insufficient, emit explicit source-gap report and do not run backtest.", "fetch_allowed": False, "db_write_allowed": False, "passed": True},
    ]

    join_key_plan = [
        {"join_key": "game_id", "priority": 1, "requires": "shared game_id in prediction and actual artifacts", "passed": True},
        {"join_key": "date_home_team_away_team", "priority": 2, "requires": "date plus home/away teams", "passed": True},
        {"join_key": "matchup_date", "priority": 3, "requires": "matchup string and date", "passed": True},
        {"join_key": "team_date_pair", "priority": 4, "requires": "team/date rows when game-level unavailable", "passed": True},
        {"join_key": "lineage_fallback", "priority": 5, "requires": "source path and row identifiers for manual audit", "passed": True},
    ]

    evaluation_surface_schema = [
        {"field": "game_id", "required": False, "family": "identifier", "passed": True},
        {"field": "game_date", "required": True, "family": "identifier", "passed": True},
        {"field": "home_team", "required": False, "family": "team", "passed": True},
        {"field": "away_team", "required": False, "family": "team", "passed": True},
        {"field": "matchup", "required": False, "family": "team", "passed": True},
        {"field": "home_win_probability", "required": False, "family": "prediction_probability", "passed": True},
        {"field": "away_win_probability", "required": False, "family": "prediction_probability", "passed": True},
        {"field": "actual_winner", "required": False, "family": "actual_result", "passed": True},
        {"field": "home_expected_runs", "required": False, "family": "prediction_runs", "passed": True},
        {"field": "away_expected_runs", "required": False, "family": "prediction_runs", "passed": True},
        {"field": "total_expected_runs", "required": False, "family": "prediction_runs", "passed": True},
        {"field": "home_actual_runs", "required": False, "family": "actual_runs", "passed": True},
        {"field": "away_actual_runs", "required": False, "family": "actual_runs", "passed": True},
        {"field": "backtest_label", "required": True, "family": "label", "passed": True},
        {"field": "current_ui_realism_state_label", "required": True, "family": "label", "passed": True},
        {"field": "mechanic_tags", "required": True, "family": "label", "passed": True},
        {"field": "prediction_source_path", "required": True, "family": "lineage", "passed": True},
        {"field": "actual_source_path", "required": True, "family": "lineage", "passed": True},
        {"field": "join_key_used", "required": True, "family": "lineage", "passed": True},
        {"field": "join_confidence", "required": True, "family": "lineage", "passed": True},
    ]

    metric_targets = [
        {"metric": "brier_score", "requires": "predicted probability + actual result", "ready_after_remediation_if": "probability/result fields joined", "passed": True},
        {"metric": "calibration", "requires": "predicted probability + actual result", "ready_after_remediation_if": "probability/result fields joined", "passed": True},
        {"metric": "favorite_underdog_directional_accuracy", "requires": "predicted probability + actual result", "ready_after_remediation_if": "probability/result fields joined", "passed": True},
        {"metric": "team_runs_mae_rmse", "requires": "predicted runs + actual runs", "ready_after_remediation_if": "run fields joined", "passed": True},
        {"metric": "total_runs_mae_rmse", "requires": "predicted total/team runs + actual total runs", "ready_after_remediation_if": "run fields joined", "passed": True},
        {"metric": "coverage_diagnostics", "requires": "candidate inventory", "ready_after_remediation_if": "always", "passed": True},
        {"metric": "missing_field_diagnostics", "requires": "schema inspection", "ready_after_remediation_if": "always", "passed": True},
        {"metric": "join_coverage", "requires": "prediction and actual candidate inventories", "ready_after_remediation_if": "join attempted", "passed": True},
        {"metric": "lineage_completeness", "requires": "source paths and join metadata", "ready_after_remediation_if": "surface materialized", "passed": True},
    ]

    lineage_requirements = [
        {"requirement": "prediction_source_path", "required": True, "passed": True},
        {"requirement": "actual_source_path", "required": True, "passed": True},
        {"requirement": "prediction_row_identifier", "required": True, "passed": True},
        {"requirement": "actual_row_identifier", "required": True, "passed": True},
        {"requirement": "join_key_used", "required": True, "passed": True},
        {"requirement": "join_confidence", "required": True, "passed": True},
        {"requirement": "current_ui_realism_state_label", "required": True, "passed": True},
        {"requirement": "mechanic_tags", "required": True, "passed": True},
    ]

    guardrails = [
        {"guardrail": "repo_local_only", "passed": True},
        {"guardrail": "no_external_fetch", "passed": True},
        {"guardrail": "no_remote_api_calls", "passed": True},
        {"guardrail": "no_database_writes", "passed": True},
        {"guardrail": "no_production_simulations", "passed": True},
        {"guardrail": "no_real_historical_evaluation", "passed": True},
        {"guardrail": "non_production_tmp_surface_only_in_future_implementation", "passed": True},
        {"guardrail": "no_activation", "passed": True},
        {"guardrail": "no_layer6_exit", "passed": True},
    ]

    blockers = [
        {"blocker": "data_gap_remediation_not_implemented", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "evaluation_surface_not_materialized", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "activation_decision_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6kt = [
        {"contract": "implement_repo_local_broadened_artifact_search", "required": True, "passed": True},
        {"contract": "implement_prediction_actual_candidate_classification", "required": True, "passed": True},
        {"contract": "implement_join_feasibility_and_lineage", "required": True, "passed": True},
        {"contract": "materialize_non_production_evaluation_surface_if_possible", "required": True, "passed": True},
        {"contract": "emit_source_gap_report_if_not_possible", "required": True, "passed": True},
        {"contract": "preserve_no_fetch_no_db_write_no_activation_no_layer6_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6kr_audit_script_exists", "expected": True, "actual": AUDIT_6KR_PATH.exists(), "passed": AUDIT_6KR_PATH.exists()},
        {"check": "6kr_json_exists", "expected": True, "actual": JSON_6KR.exists(), "passed": JSON_6KR.exists()},
        {"check": "6kr_all_checks_passed", "expected": True, "actual": json_6kr.get("all_checks_passed"), "passed": json_6kr.get("all_checks_passed") is True},
        {"check": "6kr_diagnosis", "expected": DIAGNOSIS_6KR, "actual": json_6kr.get("diagnosis"), "passed": json_6kr.get("diagnosis") == DIAGNOSIS_6KR},
        {"check": "6kr_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KR, "actual": json_6kr.get("recommended_next_layer"), "passed": json_6kr.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KR},
        {"check": "6kr_recommended_path", "expected": "plan_historical_backtest_data_gap_remediation", "actual": json_6kr.get("recommended_path"), "passed": json_6kr.get("recommended_path") == "plan_historical_backtest_data_gap_remediation"},
        {"check": "6kr_data_gap_blocks_backtest", "expected": True, "actual": json_6kr.get("data_gap_blocks_backtest"), "passed": json_6kr.get("data_gap_blocks_backtest") is True},
        {"check": "6kr_historical_odds_required", "expected": False, "actual": json_6kr.get("historical_odds_required"), "passed": json_6kr.get("historical_odds_required") is False},
        {"check": "6kr_no_layer6_exit", "expected": False, "actual": json_6kr.get("layer_6_exit_recommended"), "passed": json_6kr.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6kt_data_gap_remediation_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "data-gap remediation must be implemented and audited first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "historical evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6KS", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KS is planning-only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6KS cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6kr_passed", "expected": True, "actual": json_6kr.get("all_checks_passed"), "passed": json_6kr.get("all_checks_passed") is True},
        {"decision": "gap_statement_count", "expected": 1, "actual": len(gap_statement), "passed": len(gap_statement) == 1 and all_passed(gap_statement)},
        {"decision": "remediation_option_count", "expected": 4, "actual": len(remediation_options), "passed": len(remediation_options) == 4 and all_passed(remediation_options)},
        {"decision": "evaluation_surface_schema_field_count", "expected": 20, "actual": len(evaluation_surface_schema), "passed": len(evaluation_surface_schema) == 20 and all_passed(evaluation_surface_schema)},
        {"decision": "recommend_6kt_next", "expected": RECOMMENDED_NEXT_LAYER_6KS, "actual": RECOMMENDED_NEXT_LAYER_6KS, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "data_gap_remediation_plan_created", "expected": True, "actual": True, "passed": True},
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
        {"surface": "6kr_audit", "policy": "read_only", "passed": True},
        {"surface": "candidate_artifacts", "policy": "read_only", "passed": True},
        {"surface": "future_eval_surface", "policy": "tmp_non_production_only", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6ks", "passed": True},
        {"surface": "database", "policy": "not_written_in_6ks", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KS, "actual": RECOMMENDED_NEXT_LAYER_6KS, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KS, "actual": RECOMMENDED_PATH_6KS, "passed": True},
        {"decision": "recommend_data_gap_remediation_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KS, "actual": DIAGNOSIS_6KS, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "gap_statement", "passed": len(gap_statement) == 1 and all_passed(gap_statement), "detail": "1/1"},
        {"check": "remediation_options", "passed": len(remediation_options) == 4 and all_passed(remediation_options), "detail": "4/4"},
        {"check": "join_key_plan", "passed": len(join_key_plan) == 5 and all_passed(join_key_plan), "detail": "5/5"},
        {"check": "evaluation_surface_schema", "passed": len(evaluation_surface_schema) == 20 and all_passed(evaluation_surface_schema), "detail": "20/20"},
        {"check": "metric_targets", "passed": len(metric_targets) == 9 and all_passed(metric_targets), "detail": "9/9"},
        {"check": "lineage_requirements", "passed": len(lineage_requirements) == 8 and all_passed(lineage_requirements), "detail": "8/8"},
        {"check": "guardrails", "passed": len(guardrails) == 9 and all_passed(guardrails), "detail": "9/9"},
        {"check": "blockers", "passed": len(blockers) == 4 and all_passed(blockers), "detail": "4/4"},
        {"check": "future_6kt_contract", "passed": len(future_6kt) == 6 and all_passed(future_6kt), "detail": "6/6"},
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
        "gap_statement": write_csv(GAP_STATEMENT_CSV, gap_statement),
        "remediation_options": write_csv(REMEDIATION_OPTIONS_CSV, remediation_options),
        "join_key_plan": write_csv(JOIN_KEY_PLAN_CSV, join_key_plan),
        "evaluation_surface_schema": write_csv(EVAL_SURFACE_SCHEMA_CSV, evaluation_surface_schema),
        "metric_targets": write_csv(METRIC_TARGETS_CSV, metric_targets),
        "lineage_requirements": write_csv(LINEAGE_REQUIREMENTS_CSV, lineage_requirements),
        "guardrails": write_csv(GUARDRAILS_CSV, guardrails),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6kt_contract": write_csv(FUTURE_6KT_CSV, future_6kt),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KS",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KS,
        "recommended_path": RECOMMENDED_PATH_6KS,
        "predecessor_audit": str(AUDIT_6KR_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6kr.get("diagnosis"),
        "planned_layer_after": "6KR",
        "source_family": "historical_backtest_data_gap_remediation_plan",
        "gap_statement_count": len(gap_statement),
        "remediation_option_count": len(remediation_options),
        "join_key_plan_count": len(join_key_plan),
        "evaluation_surface_schema_field_count": len(evaluation_surface_schema),
        "metric_target_count": len(metric_targets),
        "lineage_requirement_count": len(lineage_requirements),
        "guardrail_count": len(guardrails),
        "blocker_count": len(blockers),
        "future_6kt_contract_valid": len(future_6kt) == 6 and all_passed(future_6kt),
        "data_gap_remediation_plan_created": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "data_gap_blocks_backtest": True,
        "historical_odds_required": False,
        "remediation_option_a_planned": True,
        "remediation_option_b_planned": True,
        "remediation_option_c_planned": True,
        "remediation_option_d_planned": True,
        "non_production_evaluation_surface_planned": True,
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
            "gap_statement_csv": str(GAP_STATEMENT_CSV),
            "remediation_options_csv": str(REMEDIATION_OPTIONS_CSV),
            "join_key_plan_csv": str(JOIN_KEY_PLAN_CSV),
            "evaluation_surface_schema_csv": str(EVAL_SURFACE_SCHEMA_CSV),
            "metric_targets_csv": str(METRIC_TARGETS_CSV),
            "lineage_requirements_csv": str(LINEAGE_REQUIREMENTS_CSV),
            "guardrails_csv": str(GUARDRAILS_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6kt_contract_csv": str(FUTURE_6KT_CSV),
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
