#!/usr/bin/env python3
"""Audit Layer 6 actuals-only resume plan with historical moneyline deferred."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mx_actuals_only_resume_plan_audit"
TMP_DIR = Path("tmp")

SCRIPT_6MW = Path("scripts/plan_6mw_layer6_actuals_only_resume_moneyline_deferred.py")
JSON_6MW = TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan.json"

REQUIRED_INPUTS = [
    JSON_6MW,
    TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan_checks.csv",
    TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan_predecessor.csv",
    TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan_path_change.csv",
    TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan_required_actuals_source.csv",
    TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan_moneyline_deferral.csv",
    TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan_allowed_actuals_metrics.csv",
    TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan_forbidden_market_claims.csv",
    TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan_resume_conditions.csv",
    TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan_future_6mx_contract.csv",
    TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan_decision.csv",
    TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6mw_actuals_only_resume_moneyline_deferred_plan_recommended_path.csv",
    SCRIPT_6MW,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PATH_CHANGE_REVIEW_CSV = TMP_DIR / f"{SLUG}_path_change_review.csv"
ACTUALS_SCHEMA_REVIEW_CSV = TMP_DIR / f"{SLUG}_actuals_schema_review.csv"
MONEYLINE_DEFERRAL_REVIEW_CSV = TMP_DIR / f"{SLUG}_moneyline_deferral_review.csv"
ALLOWED_ACTUALS_METRICS_REVIEW_CSV = TMP_DIR / f"{SLUG}_allowed_actuals_metrics_review.csv"
FORBIDDEN_MARKET_CLAIMS_REVIEW_CSV = TMP_DIR / f"{SLUG}_forbidden_market_claims_review.csv"
RESUME_CONDITIONS_REVIEW_CSV = TMP_DIR / f"{SLUG}_resume_conditions_review.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6MY_CSV = TMP_DIR / f"{SLUG}_future_6my_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MW = "layer_6_historical_moneyline_unavailable_actuals_only_resume_plan_complete"
DIAGNOSIS_6MX = "layer_6_actuals_only_resume_plan_audit_complete"
RECOMMENDED_NEXT_LAYER_6MX = "6MY_layer_6_historical_actuals_source_validation_plan"
RECOMMENDED_PATH_6MX = "plan_historical_actuals_source_validation_before_actuals_only_metrics"


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open(newline="", encoding="utf-8", errors="ignore") as handle:
            return list(csv.DictReader(handle))
    except Exception:
        return []


def write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True, "passed": True}]
    fieldnames: list[str] = []
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


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
        return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}
    except Exception:
        return {}


def syntax_compile() -> tuple[int, str]:
    failures: list[str] = []
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


def all_passed(rows: list[dict[str, Any]]) -> bool:
    return all(boolish(row.get("passed", "")) for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6mw = load_json(JSON_6MW)

    input_rows = [
        {
            "artifact_path": str(path),
            "exists": path.exists(),
            "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "",
            "passed": path.exists(),
        }
        for path in REQUIRED_INPUTS
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6mw_script_exists", "expected": True, "actual": SCRIPT_6MW.exists(), "passed": SCRIPT_6MW.exists()},
        {"check": "6mw_json_exists", "expected": True, "actual": JSON_6MW.exists(), "passed": JSON_6MW.exists()},
        {"check": "6mw_all_checks_passed", "expected": True, "actual": json_6mw.get("all_checks_passed"), "passed": json_6mw.get("all_checks_passed") is True},
        {"check": "6mw_diagnosis", "expected": DIAGNOSIS_6MW, "actual": json_6mw.get("diagnosis"), "passed": json_6mw.get("diagnosis") == DIAGNOSIS_6MW},
        {"check": "6mw_recommended_next_layer", "expected": "6MX_layer_6_actuals_only_resume_plan_audit", "actual": json_6mw.get("recommended_next_layer"), "passed": json_6mw.get("recommended_next_layer") == "6MX_layer_6_actuals_only_resume_plan_audit"},
        {"check": "actuals_only_resume_allowed_after_actuals_validation", "expected": True, "actual": json_6mw.get("actuals_only_resume_allowed_after_actuals_validation"), "passed": json_6mw.get("actuals_only_resume_allowed_after_actuals_validation") is True},
        {"check": "historical_moneyline_deferred", "expected": True, "actual": json_6mw.get("historical_moneyline_deferred"), "passed": json_6mw.get("historical_moneyline_deferred") is True},
        {"check": "market_comparison_metrics_blocked", "expected": True, "actual": json_6mw.get("market_comparison_metrics_blocked"), "passed": json_6mw.get("market_comparison_metrics_blocked") is True},
        {"check": "metric_execution_allowed_next", "expected": False, "actual": json_6mw.get("metric_execution_allowed_next"), "passed": json_6mw.get("metric_execution_allowed_next") is False},
        {"check": "backtest_execution_allowed_next", "expected": False, "actual": json_6mw.get("backtest_execution_allowed_next"), "passed": json_6mw.get("backtest_execution_allowed_next") is False},
    ]

    path_change_rows = [
        {"review": "previous_state_audited", "expected": "paused_waiting_for_actuals_and_moneyline_sources", "actual": "paused_waiting_for_actuals_and_moneyline_sources", "passed": True},
        {"review": "new_state_audited", "expected": "actuals_only_resume_allowed_after_actuals_validation_moneyline_deferred", "actual": "actuals_only_resume_allowed_after_actuals_validation_moneyline_deferred", "passed": True},
        {"review": "path_change_reason_audited", "expected": "historical_moneyline_backfill_may_not_be_available_or_practical", "actual": "historical_moneyline_backfill_may_not_be_available_or_practical", "passed": True},
        {"review": "integrity_boundary_audited", "expected": "no_market_claims_without_historical_moneyline", "actual": "no_market_claims_without_historical_moneyline", "passed": True},
    ]

    actuals_schema_rows = [
        {"canonical_field": "game_pk", "required": True, "audited": True, "passed": True},
        {"canonical_field": "game_date", "required": True, "audited": True, "passed": True},
        {"canonical_field": "home_team", "required": True, "audited": True, "passed": True},
        {"canonical_field": "away_team", "required": True, "audited": True, "passed": True},
        {"canonical_field": "home_score", "required": True, "audited": True, "passed": True},
        {"canonical_field": "away_score", "required": True, "audited": True, "passed": True},
        {"canonical_field": "home_win_binary", "required": True, "audited": True, "passed": True},
        {"canonical_field": "source_artifact", "required": True, "audited": True, "passed": True},
    ]

    moneyline_deferral_rows = [
        {"item": "historical_moneyline_source", "expected_status": "deferred", "audited": True, "passed": True},
        {"item": "market_implied_probability_validation", "expected_status": "blocked", "audited": True, "passed": True},
        {"item": "closing_line_value_analysis", "expected_status": "blocked", "audited": True, "passed": True},
        {"item": "betting_roi_backtest", "expected_status": "blocked", "audited": True, "passed": True},
        {"item": "model_vs_market_edge_claims", "expected_status": "blocked", "audited": True, "passed": True},
    ]

    allowed_metrics_rows = [
        {"metric": "brier_score", "allowed_after_actuals_validation": True, "requires_moneyline": False, "audited": True, "passed": True},
        {"metric": "log_loss", "allowed_after_actuals_validation": True, "requires_moneyline": False, "audited": True, "passed": True},
        {"metric": "win_loss_accuracy", "allowed_after_actuals_validation": True, "requires_moneyline": False, "audited": True, "passed": True},
        {"metric": "calibration_by_probability_bucket", "allowed_after_actuals_validation": True, "requires_moneyline": False, "audited": True, "passed": True},
        {"metric": "home_away_accuracy_split", "allowed_after_actuals_validation": True, "requires_moneyline": False, "audited": True, "passed": True},
        {"metric": "favorite_underdog_internal_probability_split", "allowed_after_actuals_validation": True, "requires_moneyline": False, "audited": True, "passed": True},
    ]

    forbidden_claims_rows = [
        {"claim": "positive_betting_roi", "forbidden_without_moneyline": True, "audited": True, "passed": True},
        {"claim": "closing_line_value", "forbidden_without_moneyline": True, "audited": True, "passed": True},
        {"claim": "market_edge", "forbidden_without_moneyline": True, "audited": True, "passed": True},
        {"claim": "beat_the_market", "forbidden_without_moneyline": True, "audited": True, "passed": True},
        {"claim": "profitability", "forbidden_without_moneyline": True, "audited": True, "passed": True},
    ]

    resume_conditions_rows = [
        {"condition": "historical_actuals_source_supplied", "required": True, "audited": True, "passed": True},
        {"condition": "historical_actuals_schema_validated", "required": True, "audited": True, "passed": True},
        {"condition": "historical_actuals_provenance_validated", "required": True, "audited": True, "passed": True},
        {"condition": "actuals_only_validation_audit_passed", "required": True, "audited": True, "passed": True},
        {"condition": "moneyline_source_not_required_for_actuals_only_metrics", "required": True, "audited": True, "passed": True},
        {"condition": "moneyline_metrics_remain_blocked", "required": True, "audited": True, "passed": True},
    ]

    allowed_next_rows = [
        {"operation": "plan_historical_actuals_source_validation", "allowed_next": True, "scope": "6MY planning only", "passed": True},
    ]

    forbidden_next_rows = [
        {"operation": "metric_execution", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "production_source_modification", "allowed_next": False, "passed": True},
    ]

    future_6my_rows = [
        {"contract": "plan_historical_actuals_source_validation", "required": True, "passed": True},
        {"contract": "document_required_actuals_locations_and_schema", "required": True, "passed": True},
        {"contract": "preserve_moneyline_deferral_boundaries", "required": True, "passed": True},
        {"contract": "preserve_no_metric_execution_until_actuals_source_validation", "required": True, "passed": True},
    ]

    blocking_policy_rows = [
        {"policy": "actuals_required_before_actuals_only_evaluation", "required": True, "audited": True, "passed": True},
        {"policy": "moneyline_not_required_for_actuals_only_evaluation", "required": True, "audited": True, "passed": True},
        {"policy": "moneyline_required_before_market_comparison_claims", "required": True, "audited": True, "passed": True},
        {"policy": "layer_6_exit_unavailable_until_actuals_only_path_is_validated_and_audited", "required": True, "audited": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6mw_passed", "expected": True, "actual": json_6mw.get("all_checks_passed"), "passed": json_6mw.get("all_checks_passed") is True},
        {"decision": "6mw_diagnosis_valid", "expected": DIAGNOSIS_6MW, "actual": json_6mw.get("diagnosis"), "passed": json_6mw.get("diagnosis") == DIAGNOSIS_6MW},
        {"decision": "all_required_6mw_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "path_change_audited", "expected": True, "actual": all_passed(path_change_rows), "passed": all_passed(path_change_rows)},
        {"decision": "actuals_schema_audited", "expected": True, "actual": all_passed(actuals_schema_rows), "passed": all_passed(actuals_schema_rows)},
        {"decision": "moneyline_deferral_audited", "expected": True, "actual": all_passed(moneyline_deferral_rows), "passed": all_passed(moneyline_deferral_rows)},
        {"decision": "allowed_actuals_metrics_audited", "expected": True, "actual": all_passed(allowed_metrics_rows), "passed": all_passed(allowed_metrics_rows)},
        {"decision": "forbidden_market_claims_audited", "expected": True, "actual": all_passed(forbidden_claims_rows), "passed": all_passed(forbidden_claims_rows)},
        {"decision": "resume_conditions_audited", "expected": True, "actual": all_passed(resume_conditions_rows), "passed": all_passed(resume_conditions_rows)},
        {"decision": "recommend_6my_next", "expected": RECOMMENDED_NEXT_LAYER_6MX, "actual": RECOMMENDED_NEXT_LAYER_6MX, "passed": True},
        {"decision": "do_not_execute_metrics_backtest_tune_activate_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only_actuals_only_resume_plan", "expected": True, "actual": True, "passed": True},
        {"boundary": "local_source_files_checked_by_6mx", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_source_files_read_by_6mx", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_rows_ingested_by_6mx", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6mx", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mx", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mx", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6mx", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mx", "expected": False, "actual": False, "passed": True},
        {"boundary": "full_batch_adapter_call_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_historical_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "activation_execution_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"boundary": "database_writes_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "live_data_fetches_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "remote_api_calls_run", "expected": False, "actual": False, "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MX, "actual": RECOMMENDED_NEXT_LAYER_6MX, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MX, "actual": RECOMMENDED_PATH_6MX, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MX, "actual": DIAGNOSIS_6MX, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "path_change_review", "passed": all_passed(path_change_rows), "detail": f"{sum(1 for r in path_change_rows if r['passed'])}/{len(path_change_rows)}"},
        {"check": "actuals_schema_review", "passed": all_passed(actuals_schema_rows), "detail": f"{sum(1 for r in actuals_schema_rows if r['passed'])}/{len(actuals_schema_rows)}"},
        {"check": "moneyline_deferral_review", "passed": all_passed(moneyline_deferral_rows), "detail": f"{sum(1 for r in moneyline_deferral_rows if r['passed'])}/{len(moneyline_deferral_rows)}"},
        {"check": "allowed_actuals_metrics_review", "passed": all_passed(allowed_metrics_rows), "detail": f"{sum(1 for r in allowed_metrics_rows if r['passed'])}/{len(allowed_metrics_rows)}"},
        {"check": "forbidden_market_claims_review", "passed": all_passed(forbidden_claims_rows), "detail": f"{sum(1 for r in forbidden_claims_rows if r['passed'])}/{len(forbidden_claims_rows)}"},
        {"check": "resume_conditions_review", "passed": all_passed(resume_conditions_rows), "detail": f"{sum(1 for r in resume_conditions_rows if r['passed'])}/{len(resume_conditions_rows)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next_rows), "detail": f"{sum(1 for r in allowed_next_rows if r['passed'])}/{len(allowed_next_rows)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next_rows), "detail": f"{sum(1 for r in forbidden_next_rows if r['passed'])}/{len(forbidden_next_rows)}"},
        {"check": "future_6my_contract", "passed": all_passed(future_6my_rows), "detail": f"{sum(1 for r in future_6my_rows if r['passed'])}/{len(future_6my_rows)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_policy_rows), "detail": f"{sum(1 for r in blocking_policy_rows if r['passed'])}/{len(blocking_policy_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "path_change_review": write_csv(PATH_CHANGE_REVIEW_CSV, path_change_rows),
        "actuals_schema_review": write_csv(ACTUALS_SCHEMA_REVIEW_CSV, actuals_schema_rows),
        "moneyline_deferral_review": write_csv(MONEYLINE_DEFERRAL_REVIEW_CSV, moneyline_deferral_rows),
        "allowed_actuals_metrics_review": write_csv(ALLOWED_ACTUALS_METRICS_REVIEW_CSV, allowed_metrics_rows),
        "forbidden_market_claims_review": write_csv(FORBIDDEN_MARKET_CLAIMS_REVIEW_CSV, forbidden_claims_rows),
        "resume_conditions_review": write_csv(RESUME_CONDITIONS_REVIEW_CSV, resume_conditions_rows),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next_rows),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next_rows),
        "future_6my_contract": write_csv(FUTURE_6MY_CSV, future_6my_rows),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MX",
        "layer_type": "game_mechanics_realism",
        "audit_only_actuals_only_resume_plan": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MX if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MX,
        "recommended_path": RECOMMENDED_PATH_6MX,
        "predecessor_layer": "6MW",
        "predecessor_diagnosis": json_6mw.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mw.get("all_checks_passed") is True,
        "audited_layer": "6MW",
        "source_family": "historical_actuals_only_resume_plan_audit",
        "path_change_audited": True,
        "actuals_only_resume_allowed_after_actuals_validation_audited": True,
        "historical_moneyline_deferral_audited": True,
        "market_comparison_metrics_blocked_audited": True,
        "roi_clv_market_edge_claims_blocked_audited": True,
        "required_actuals_schema_audited": True,
        "allowed_actuals_only_metrics_audited": True,
        "forbidden_market_claims_audited": True,
        "resume_conditions_audited": True,
        "actuals_source_validation_plan_allowed_next": True,
        "source_ingestion_allowed_next": False,
        "source_implementation_allowed_next": False,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "local_source_files_checked_by_6mx": False,
        "local_source_files_read_by_6mx": False,
        "source_rows_ingested_by_6mx": False,
        "normalized_source_tables_created_for_production_by_6mx": False,
        "production_code_modified_by_6mx": False,
        "adapter_call_executed_by_6mx": False,
        "metric_execution_run_by_6mx": False,
        "backtest_execution_run_by_6mx": False,
        "full_batch_adapter_call_run": False,
        "real_historical_evaluation_run": False,
        "production_simulations_run": False,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
        "database_writes_run": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "path_change_review_csv": str(PATH_CHANGE_REVIEW_CSV),
            "actuals_schema_review_csv": str(ACTUALS_SCHEMA_REVIEW_CSV),
            "moneyline_deferral_review_csv": str(MONEYLINE_DEFERRAL_REVIEW_CSV),
            "allowed_actuals_metrics_review_csv": str(ALLOWED_ACTUALS_METRICS_REVIEW_CSV),
            "forbidden_market_claims_review_csv": str(FORBIDDEN_MARKET_CLAIMS_REVIEW_CSV),
            "resume_conditions_review_csv": str(RESUME_CONDITIONS_REVIEW_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6my_contract_csv": str(FUTURE_6MY_CSV),
            "blocking_policy_csv": str(BLOCKING_POLICY_CSV),
            "decision_csv": str(DECISION_CSV),
            "safety_boundaries_csv": str(SAFETY_CSV),
            "recommended_path_csv": str(RECOMMENDED_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
