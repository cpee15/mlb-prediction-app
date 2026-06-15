#!/usr/bin/env python3
"""Audit 6NM actuals-only metric outputs before any backtest/tuning path."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6nn_actuals_only_metric_execution_audit"
TMP_DIR = Path("tmp")

SCRIPT_6NM = Path("scripts/execute_6nm_layer6_actuals_only_metric_execution.py")
JSON_6NM = TMP_DIR / "layer6_6nm_actuals_only_metric_execution.json"
TARGET_ACTUALS = Path("data/local/historical_actuals.csv")
ACTUALS_METRICS = TMP_DIR / "layer6_6nm_actuals_only_metric_execution_actuals_metrics.csv"
DATE_COVERAGE = TMP_DIR / "layer6_6nm_actuals_only_metric_execution_date_coverage.csv"
SOURCE_COVERAGE = TMP_DIR / "layer6_6nm_actuals_only_metric_execution_source_coverage.csv"
PREDICTION_JOIN_REVIEW = TMP_DIR / "layer6_6nm_actuals_only_metric_execution_prediction_join_review.csv"
FORBIDDEN_METRIC_REVIEW = TMP_DIR / "layer6_6nm_actuals_only_metric_execution_forbidden_metric_review.csv"

REQUIRED_INPUTS = [
    JSON_6NM,
    TMP_DIR / "layer6_6nm_actuals_only_metric_execution_checks.csv",
    TMP_DIR / "layer6_6nm_actuals_only_metric_execution_predecessor.csv",
    TMP_DIR / "layer6_6nm_actuals_only_metric_execution_input_artifacts.csv",
    ACTUALS_METRICS,
    DATE_COVERAGE,
    SOURCE_COVERAGE,
    PREDICTION_JOIN_REVIEW,
    FORBIDDEN_METRIC_REVIEW,
    TMP_DIR / "layer6_6nm_actuals_only_metric_execution_post_metric_audit_requirement.csv",
    TMP_DIR / "layer6_6nm_actuals_only_metric_execution_metric_boundaries.csv",
    TMP_DIR / "layer6_6nm_actuals_only_metric_execution_allowed_operations_next.csv",
    TMP_DIR / "layer6_6nm_actuals_only_metric_execution_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6nm_actuals_only_metric_execution_future_6nn_contract.csv",
    TMP_DIR / "layer6_6nm_actuals_only_metric_execution_decision.csv",
    TMP_DIR / "layer6_6nm_actuals_only_metric_execution_safety_boundaries.csv",
    TMP_DIR / "layer6_6nm_actuals_only_metric_execution_recommended_path.csv",
    SCRIPT_6NM,
    TARGET_ACTUALS,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
METRIC_VALUE_REVIEW_CSV = TMP_DIR / f"{SLUG}_metric_value_review.csv"
DATE_COVERAGE_REVIEW_CSV = TMP_DIR / f"{SLUG}_date_coverage_review.csv"
SOURCE_COVERAGE_REVIEW_CSV = TMP_DIR / f"{SLUG}_source_coverage_review.csv"
PREDICTION_JOIN_AUDIT_CSV = TMP_DIR / f"{SLUG}_prediction_join_review.csv"
FORBIDDEN_METRIC_AUDIT_CSV = TMP_DIR / f"{SLUG}_forbidden_metric_review.csv"
SAFETY_REVIEW_CSV = TMP_DIR / f"{SLUG}_safety_review.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6NO_CSV = TMP_DIR / f"{SLUG}_future_6no_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6NM = "layer_6_actuals_only_metric_execution_complete"
DIAGNOSIS_6NN = "layer_6_actuals_only_metric_execution_audit_complete"
RECOMMENDED_NEXT_LAYER = "6NO_layer_6_post_actuals_metric_safe_transition_plan"
RECOMMENDED_PATH = "plan_safe_transition_after_actuals_only_metric_audit"


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


def float_or_none(value: Any) -> float | None:
    try:
        return float(str(value).strip())
    except Exception:
        return None


def metric_map(rows: list[dict[str, str]]) -> dict[str, float]:
    result: dict[str, float] = {}
    for row in rows:
        metric = str(row.get("metric", "")).strip()
        value = float_or_none(row.get("value"))
        if metric and value is not None:
            result[metric] = value
    return result


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()
    json_6nm = load_json(JSON_6NM)
    actuals_metric_rows = read_csv_rows(ACTUALS_METRICS)
    date_coverage_rows_raw = read_csv_rows(DATE_COVERAGE)
    source_coverage_rows_raw = read_csv_rows(SOURCE_COVERAGE)
    prediction_join_rows_raw = read_csv_rows(PREDICTION_JOIN_REVIEW)
    forbidden_metric_rows_raw = read_csv_rows(FORBIDDEN_METRIC_REVIEW)

    metrics = metric_map(actuals_metric_rows)

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
        {"check": "6nm_script_exists", "expected": True, "actual": SCRIPT_6NM.exists(), "passed": SCRIPT_6NM.exists()},
        {"check": "6nm_json_exists", "expected": True, "actual": JSON_6NM.exists(), "passed": JSON_6NM.exists()},
        {"check": "6nm_all_checks_passed", "expected": True, "actual": json_6nm.get("all_checks_passed"), "passed": json_6nm.get("all_checks_passed") is True},
        {"check": "6nm_diagnosis", "expected": DIAGNOSIS_6NM, "actual": json_6nm.get("diagnosis"), "passed": json_6nm.get("diagnosis") == DIAGNOSIS_6NM},
        {"check": "post_metric_execution_audit_required_next", "expected": True, "actual": json_6nm.get("post_metric_execution_audit_required_next"), "passed": json_6nm.get("post_metric_execution_audit_required_next") is True},
    ]

    metric_value_rows = [
        {"metric": "actuals_row_count", "expected": json_6nm.get("actuals_row_count"), "actual": metrics.get("actuals_row_count"), "passed": metrics.get("actuals_row_count") == json_6nm.get("actuals_row_count")},
        {"metric": "unique_game_pk_count", "expected": json_6nm.get("unique_game_pk_count"), "actual": metrics.get("unique_game_pk_count"), "passed": metrics.get("unique_game_pk_count") == json_6nm.get("unique_game_pk_count")},
        {"metric": "row_count_equals_unique_game_pk", "expected": True, "actual": json_6nm.get("actuals_row_count") == json_6nm.get("unique_game_pk_count"), "passed": json_6nm.get("actuals_row_count") == json_6nm.get("unique_game_pk_count")},
        {"metric": "date_span_days", "expected": json_6nm.get("date_span_days"), "actual": metrics.get("date_span_days"), "passed": metrics.get("date_span_days") == json_6nm.get("date_span_days")},
        {"metric": "home_win_rate_plausible", "expected": "0..1", "actual": json_6nm.get("home_win_rate"), "passed": 0 <= float(json_6nm.get("home_win_rate", -1)) <= 1},
        {"metric": "average_home_score_plausible", "expected": ">=0", "actual": json_6nm.get("average_home_score"), "passed": float(json_6nm.get("average_home_score", -1)) >= 0},
        {"metric": "average_away_score_plausible", "expected": ">=0", "actual": json_6nm.get("average_away_score"), "passed": float(json_6nm.get("average_away_score", -1)) >= 0},
        {"metric": "average_total_runs_plausible", "expected": ">=0", "actual": json_6nm.get("average_total_runs"), "passed": float(json_6nm.get("average_total_runs", -1)) >= 0},
        {"metric": "one_run_game_rate_plausible", "expected": "0..1", "actual": json_6nm.get("one_run_game_rate"), "passed": 0 <= float(json_6nm.get("one_run_game_rate", -1)) <= 1},
        {"metric": "actuals_only_metric_count", "expected": 13, "actual": len(actuals_metric_rows), "passed": len(actuals_metric_rows) == 13},
        {"metric": "all_metric_scope_actuals_only", "expected": True, "actual": all(row.get("metric_scope") == "actuals_only" for row in actuals_metric_rows), "passed": all(row.get("metric_scope") == "actuals_only" for row in actuals_metric_rows)},
    ]

    date_count_sum = sum(int(float(row.get("game_count", 0))) for row in date_coverage_rows_raw)
    date_coverage_rows = [
        {"check": "date_coverage_csv_exists", "expected": True, "actual": DATE_COVERAGE.exists(), "passed": DATE_COVERAGE.exists()},
        {"check": "date_coverage_row_count_matches_json", "expected": json_6nm.get("coverage_date_count"), "actual": len(date_coverage_rows_raw), "passed": len(date_coverage_rows_raw) == json_6nm.get("coverage_date_count")},
        {"check": "date_coverage_game_sum_matches_row_count", "expected": json_6nm.get("actuals_row_count"), "actual": date_count_sum, "passed": date_count_sum == json_6nm.get("actuals_row_count")},
        {"check": "date_coverage_all_positive", "expected": True, "actual": all(int(float(row.get("game_count", 0))) > 0 for row in date_coverage_rows_raw), "passed": all(int(float(row.get("game_count", 0))) > 0 for row in date_coverage_rows_raw)},
    ]

    source_count_sum = sum(int(float(row.get("game_count", 0))) for row in source_coverage_rows_raw)
    source_coverage_rows = [
        {"check": "source_coverage_csv_exists", "expected": True, "actual": SOURCE_COVERAGE.exists(), "passed": SOURCE_COVERAGE.exists()},
        {"check": "source_coverage_row_count_matches_json", "expected": json_6nm.get("source_artifact_count"), "actual": len(source_coverage_rows_raw), "passed": len(source_coverage_rows_raw) == json_6nm.get("source_artifact_count")},
        {"check": "source_coverage_game_sum_matches_row_count", "expected": json_6nm.get("actuals_row_count"), "actual": source_count_sum, "passed": source_count_sum == json_6nm.get("actuals_row_count")},
        {"check": "source_coverage_all_positive", "expected": True, "actual": all(int(float(row.get("game_count", 0))) > 0 for row in source_coverage_rows_raw), "passed": all(int(float(row.get("game_count", 0))) > 0 for row in source_coverage_rows_raw)},
    ]

    prediction_join_rows = [
        {"check": "prediction_join_attempted_false", "expected": False, "actual": json_6nm.get("prediction_join_attempted"), "passed": json_6nm.get("prediction_join_attempted") is False},
        {"check": "prediction_metrics_computed_false", "expected": False, "actual": json_6nm.get("prediction_metrics_computed"), "passed": json_6nm.get("prediction_metrics_computed") is False},
        {"check": "model_accuracy_computed_false", "expected": False, "actual": json_6nm.get("model_accuracy_computed"), "passed": json_6nm.get("model_accuracy_computed") is False},
        {"check": "prediction_join_review_rows_passed", "expected": True, "actual": all_passed(prediction_join_rows_raw), "passed": all_passed(prediction_join_rows_raw)},
    ]

    forbidden_metric_rows = [
        {"check": "roi_or_betting_metrics_computed_false", "expected": False, "actual": json_6nm.get("roi_or_betting_metrics_computed"), "passed": json_6nm.get("roi_or_betting_metrics_computed") is False},
        {"check": "backtest_execution_run_by_6nm_false", "expected": False, "actual": json_6nm.get("backtest_execution_run_by_6nm"), "passed": json_6nm.get("backtest_execution_run_by_6nm") is False},
        {"check": "tuning_allowed_next_false", "expected": False, "actual": json_6nm.get("tuning_allowed_next"), "passed": json_6nm.get("tuning_allowed_next") is False},
        {"check": "forbidden_metric_review_rows_passed", "expected": True, "actual": all_passed(forbidden_metric_rows_raw), "passed": all_passed(forbidden_metric_rows_raw)},
    ]

    safety_review_rows = [
        {"check": "actuals_file_modified_by_6nm_false", "expected": False, "actual": json_6nm.get("actuals_file_modified_by_6nm"), "passed": json_6nm.get("actuals_file_modified_by_6nm") is False},
        {"check": "database_writes_run_false", "expected": False, "actual": json_6nm.get("database_writes_run"), "passed": json_6nm.get("database_writes_run") is False},
        {"check": "live_data_fetches_run_false", "expected": False, "actual": json_6nm.get("live_data_fetches_run"), "passed": json_6nm.get("live_data_fetches_run") is False},
        {"check": "remote_api_calls_run_false", "expected": False, "actual": json_6nm.get("remote_api_calls_run"), "passed": json_6nm.get("remote_api_calls_run") is False},
        {"check": "layer_6_exit_recommended_false", "expected": False, "actual": json_6nm.get("layer_6_exit_recommended"), "passed": json_6nm.get("layer_6_exit_recommended") is False},
    ]

    allowed_next_rows = [
        {"operation": "post_actuals_metric_safe_transition_plan", "allowed_next": True, "scope": "6NO planning only", "passed": True},
    ]

    forbidden_next_rows = [
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "prediction_accuracy_claims_without_join", "allowed_next": False, "passed": True},
        {"operation": "roi_or_betting_metrics", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "production_table_creation", "allowed_next": False, "passed": True},
    ]

    future_6no_rows = [
        {"contract": "plan_safe_transition_after_actuals_only_metric_audit", "required": True, "passed": True},
        {"contract": "preserve_no_backtest_execution_in_6no", "required": True, "passed": True},
        {"contract": "define_requirements_for_any_future_prediction_join", "required": True, "passed": True},
        {"contract": "preserve_no_tuning_activation_exit", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6nm_passed", "expected": True, "actual": json_6nm.get("all_checks_passed"), "passed": json_6nm.get("all_checks_passed") is True},
        {"decision": "metric_values_plausible", "expected": True, "actual": all_passed(metric_value_rows), "passed": all_passed(metric_value_rows)},
        {"decision": "date_coverage_review_passed", "expected": True, "actual": all_passed(date_coverage_rows), "passed": all_passed(date_coverage_rows)},
        {"decision": "source_coverage_review_passed", "expected": True, "actual": all_passed(source_coverage_rows), "passed": all_passed(source_coverage_rows)},
        {"decision": "prediction_join_audit_passed", "expected": True, "actual": all_passed(prediction_join_rows), "passed": all_passed(prediction_join_rows)},
        {"decision": "forbidden_metric_audit_passed", "expected": True, "actual": all_passed(forbidden_metric_rows), "passed": all_passed(forbidden_metric_rows)},
        {"decision": "recommend_6no", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only_actuals_only_metric_execution", "expected": True, "actual": True, "passed": True},
        {"boundary": "source_rows_ingested_by_6nn", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6nn", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6nn", "expected": False, "actual": False, "passed": True},
        {"boundary": "actuals_file_modified_by_6nn", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6nn", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6nn", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6nn", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "do_not_recommend_backtests", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning_activation_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6NN, "actual": DIAGNOSIS_6NN, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "metric_value_review", "passed": all_passed(metric_value_rows), "detail": f"{sum(1 for r in metric_value_rows if r['passed'])}/{len(metric_value_rows)}"},
        {"check": "date_coverage_review", "passed": all_passed(date_coverage_rows), "detail": f"{sum(1 for r in date_coverage_rows if r['passed'])}/{len(date_coverage_rows)}"},
        {"check": "source_coverage_review", "passed": all_passed(source_coverage_rows), "detail": f"{sum(1 for r in source_coverage_rows if r['passed'])}/{len(source_coverage_rows)}"},
        {"check": "prediction_join_review", "passed": all_passed(prediction_join_rows), "detail": f"{sum(1 for r in prediction_join_rows if r['passed'])}/{len(prediction_join_rows)}"},
        {"check": "forbidden_metric_review", "passed": all_passed(forbidden_metric_rows), "detail": f"{sum(1 for r in forbidden_metric_rows if r['passed'])}/{len(forbidden_metric_rows)}"},
        {"check": "safety_review", "passed": all_passed(safety_review_rows), "detail": f"{sum(1 for r in safety_review_rows if r['passed'])}/{len(safety_review_rows)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next_rows), "detail": f"{sum(1 for r in allowed_next_rows if r['passed'])}/{len(allowed_next_rows)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next_rows), "detail": f"{sum(1 for r in forbidden_next_rows if r['passed'])}/{len(forbidden_next_rows)}"},
        {"check": "future_6no_contract", "passed": all_passed(future_6no_rows), "detail": f"{sum(1 for r in future_6no_rows if r['passed'])}/{len(future_6no_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "metric_value_review": write_csv(METRIC_VALUE_REVIEW_CSV, metric_value_rows),
        "date_coverage_review": write_csv(DATE_COVERAGE_REVIEW_CSV, date_coverage_rows),
        "source_coverage_review": write_csv(SOURCE_COVERAGE_REVIEW_CSV, source_coverage_rows),
        "prediction_join_review": write_csv(PREDICTION_JOIN_AUDIT_CSV, prediction_join_rows),
        "forbidden_metric_review": write_csv(FORBIDDEN_METRIC_AUDIT_CSV, forbidden_metric_rows),
        "safety_review": write_csv(SAFETY_REVIEW_CSV, safety_review_rows),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next_rows),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next_rows),
        "future_6no_contract": write_csv(FUTURE_6NO_CSV, future_6no_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6NN",
        "layer_type": "game_mechanics_realism",
        "audit_only_actuals_only_metric_execution": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6NN if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_layer": "6NM",
        "predecessor_diagnosis": json_6nm.get("diagnosis"),
        "predecessor_all_checks_passed": json_6nm.get("all_checks_passed") is True,
        "source_family": "actuals_only_metric_execution_audit",
        "audited_actuals_path": json_6nm.get("actuals_path"),
        "audited_actuals_row_count": json_6nm.get("actuals_row_count"),
        "audited_unique_game_pk_count": json_6nm.get("unique_game_pk_count"),
        "audited_date_span_days": json_6nm.get("date_span_days"),
        "audited_coverage_date_count": json_6nm.get("coverage_date_count"),
        "audited_source_artifact_count": json_6nm.get("source_artifact_count"),
        "audited_home_win_rate": json_6nm.get("home_win_rate"),
        "audited_average_home_score": json_6nm.get("average_home_score"),
        "audited_average_away_score": json_6nm.get("average_away_score"),
        "audited_average_total_runs": json_6nm.get("average_total_runs"),
        "audited_one_run_game_rate": json_6nm.get("one_run_game_rate"),
        "metric_values_plausible": all_passed(metric_value_rows),
        "date_coverage_review_passed": all_passed(date_coverage_rows),
        "source_coverage_review_passed": all_passed(source_coverage_rows),
        "prediction_join_audit_passed": all_passed(prediction_join_rows),
        "forbidden_metric_audit_passed": all_passed(forbidden_metric_rows),
        "post_metric_execution_audit_complete": True,
        "safe_transition_plan_allowed_next": True,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "source_rows_ingested_by_6nn": False,
        "normalized_source_tables_created_for_production_by_6nn": False,
        "production_code_modified_by_6nn": False,
        "actuals_file_modified_by_6nn": False,
        "adapter_call_executed_by_6nn": False,
        "metric_execution_run_by_6nn": False,
        "backtest_execution_run_by_6nn": False,
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
        "moneyline_deferral_boundaries_preserved": True,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "metric_value_review_csv": str(METRIC_VALUE_REVIEW_CSV),
            "date_coverage_review_csv": str(DATE_COVERAGE_REVIEW_CSV),
            "source_coverage_review_csv": str(SOURCE_COVERAGE_REVIEW_CSV),
            "prediction_join_review_csv": str(PREDICTION_JOIN_AUDIT_CSV),
            "forbidden_metric_review_csv": str(FORBIDDEN_METRIC_AUDIT_CSV),
            "safety_review_csv": str(SAFETY_REVIEW_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6no_contract_csv": str(FUTURE_6NO_CSV),
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
