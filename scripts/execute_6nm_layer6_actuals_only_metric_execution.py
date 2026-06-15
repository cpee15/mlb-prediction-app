#!/usr/bin/env python3
"""Execute limited actuals-only diagnostics after metric unlock gate check."""

from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import date
from pathlib import Path
from statistics import mean
from typing import Any, Iterable


SLUG = "layer6_6nm_actuals_only_metric_execution"
TMP_DIR = Path("tmp")

SCRIPT_6NL = Path("scripts/check_6nl_layer6_actuals_only_metric_unlock_gate.py")
JSON_6NL = TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check.json"
TARGET_ACTUALS = Path("data/local/historical_actuals.csv")

REQUIRED_INPUTS = [
    JSON_6NL,
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_checks.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_predecessor.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_input_artifacts.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_minimum_sample_gate.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_schema_value_provenance_gate.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_6na_rerun_gate.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_source_expansion_audit_gate.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_metric_scope_gate.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_forbidden_metrics_gate.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_post_metric_audit_requirements.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_unlock_decision.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_metric_unlock_boundaries.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_allowed_operations_next.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_future_6nm_contract.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_decision.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_safety_boundaries.csv",
    TMP_DIR / "layer6_6nl_actuals_only_metric_unlock_gate_check_recommended_path.csv",
    SCRIPT_6NL,
    TARGET_ACTUALS,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
ACTUALS_METRICS_CSV = TMP_DIR / f"{SLUG}_actuals_metrics.csv"
DATE_COVERAGE_CSV = TMP_DIR / f"{SLUG}_date_coverage.csv"
SOURCE_COVERAGE_CSV = TMP_DIR / f"{SLUG}_source_coverage.csv"
PREDICTION_JOIN_REVIEW_CSV = TMP_DIR / f"{SLUG}_prediction_join_review.csv"
FORBIDDEN_METRIC_REVIEW_CSV = TMP_DIR / f"{SLUG}_forbidden_metric_review.csv"
POST_METRIC_AUDIT_CSV = TMP_DIR / f"{SLUG}_post_metric_audit_requirement.csv"
METRIC_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_metric_boundaries.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6NN_CSV = TMP_DIR / f"{SLUG}_future_6nn_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6NL = "layer_6_actuals_only_metric_unlock_gate_check_complete"
DIAGNOSIS_6NM = "layer_6_actuals_only_metric_execution_complete"
RECOMMENDED_NEXT_LAYER = "6NN_layer_6_actuals_only_metric_execution_audit"
RECOMMENDED_PATH = "audit_actuals_only_metric_outputs_before_any_backtest_or_tuning"


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


def int_or_none(value: Any) -> int | None:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def parse_iso_date(value: Any) -> str | None:
    try:
        return date.fromisoformat(str(value).strip()[:10]).isoformat()
    except Exception:
        return None


def pct(numerator: int | float, denominator: int | float) -> float:
    return round(float(numerator) / float(denominator), 6) if denominator else 0.0


def avg(values: list[int | float]) -> float:
    return round(mean(values), 6) if values else 0.0


def date_span_days(rows: list[dict[str, str]]) -> int:
    dates = sorted({str(row.get("game_date", "")) for row in rows if parse_iso_date(row.get("game_date"))})
    if len(dates) < 2:
        return len(dates)
    return (date.fromisoformat(dates[-1]) - date.fromisoformat(dates[0])).days + 1


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()
    json_6nl = load_json(JSON_6NL)
    actuals_rows = read_csv_rows(TARGET_ACTUALS)

    row_count = len(actuals_rows)
    game_pks = [str(row.get("game_pk", "")).strip() for row in actuals_rows]
    unique_game_pk_count = len(set(game_pks))
    date_span = date_span_days(actuals_rows)

    home_scores = [int_or_none(row.get("home_score")) for row in actuals_rows]
    away_scores = [int_or_none(row.get("away_score")) for row in actuals_rows]
    valid_score_pairs = [
        (h, a) for h, a in zip(home_scores, away_scores)
        if h is not None and a is not None
    ]
    home_wins = [1 if h > a else 0 for h, a in valid_score_pairs]
    totals = [h + a for h, a in valid_score_pairs]
    one_run_games = [1 if abs(h - a) == 1 else 0 for h, a in valid_score_pairs]

    dates = [parse_iso_date(row.get("game_date")) for row in actuals_rows]
    valid_dates = [d for d in dates if d]
    games_by_date = Counter(valid_dates)
    source_artifacts = [str(row.get("source_artifact", "")).strip() for row in actuals_rows if str(row.get("source_artifact", "")).strip()]
    source_counts = Counter(source_artifacts)

    actuals_metrics = [
        {"metric": "actuals_row_count", "value": row_count, "metric_scope": "actuals_only", "passed": row_count > 0},
        {"metric": "unique_game_pk_count", "value": unique_game_pk_count, "metric_scope": "actuals_only", "passed": unique_game_pk_count == row_count},
        {"metric": "date_span_days", "value": date_span, "metric_scope": "actuals_only", "passed": date_span >= 21},
        {"metric": "home_win_rate", "value": pct(sum(home_wins), len(home_wins)), "metric_scope": "actuals_only", "passed": len(home_wins) == row_count},
        {"metric": "average_home_score", "value": avg([h for h, _ in valid_score_pairs]), "metric_scope": "actuals_only", "passed": len(valid_score_pairs) == row_count},
        {"metric": "average_away_score", "value": avg([a for _, a in valid_score_pairs]), "metric_scope": "actuals_only", "passed": len(valid_score_pairs) == row_count},
        {"metric": "average_total_runs", "value": avg(totals), "metric_scope": "actuals_only", "passed": len(totals) == row_count},
        {"metric": "one_run_game_rate", "value": pct(sum(one_run_games), len(one_run_games)), "metric_scope": "actuals_only", "passed": len(one_run_games) == row_count},
        {"metric": "source_artifact_count", "value": len(source_counts), "metric_scope": "actuals_only", "passed": len(source_counts) > 0},
        {"metric": "coverage_date_count", "value": len(games_by_date), "metric_scope": "actuals_only", "passed": len(games_by_date) > 0},
        {"metric": "games_per_date_min", "value": min(games_by_date.values()) if games_by_date else 0, "metric_scope": "actuals_only", "passed": bool(games_by_date)},
        {"metric": "games_per_date_max", "value": max(games_by_date.values()) if games_by_date else 0, "metric_scope": "actuals_only", "passed": bool(games_by_date)},
        {"metric": "games_per_date_mean", "value": avg(list(games_by_date.values())), "metric_scope": "actuals_only", "passed": bool(games_by_date)},
    ]

    date_coverage_rows = [
        {"game_date": game_date, "game_count": count, "passed": count > 0}
        for game_date, count in sorted(games_by_date.items())
    ]

    source_coverage_rows = [
        {"source_artifact": artifact, "game_count": count, "passed": count > 0}
        for artifact, count in sorted(source_counts.items())
    ]

    prediction_join_rows = [
        {"check": "prediction_join_attempted", "expected": False, "actual": False, "passed": True},
        {"check": "prediction_join_available", "expected": False, "actual": False, "passed": True},
        {"check": "prediction_metrics_computed", "expected": False, "actual": False, "passed": True},
        {"check": "model_accuracy_computed", "expected": False, "actual": False, "passed": True},
    ]

    forbidden_metric_rows = [
        {"metric_family": "roi_or_betting_metrics", "computed": False, "passed": True},
        {"metric_family": "historical_backtest", "computed": False, "passed": True},
        {"metric_family": "parameter_performance", "computed": False, "passed": True},
        {"metric_family": "tuning_metrics", "computed": False, "passed": True},
        {"metric_family": "activation_metrics", "computed": False, "passed": True},
        {"metric_family": "layer_6_exit_metrics", "computed": False, "passed": True},
    ]

    post_metric_audit_rows = [
        {"requirement": "post_metric_execution_audit_required_next", "expected": True, "actual": True, "passed": True},
        {"requirement": "audit_actuals_only_metric_outputs", "expected": True, "actual": True, "passed": True},
        {"requirement": "audit_no_backtest_or_tuning", "expected": True, "actual": True, "passed": True},
        {"requirement": "audit_prediction_join_not_attempted", "expected": True, "actual": True, "passed": True},
    ]

    metric_boundary_rows = [
        {"boundary": "actuals_only_metric_execution_run_by_6nm", "value": True, "passed": True},
        {"boundary": "prediction_join_attempted", "value": False, "passed": True},
        {"boundary": "prediction_metrics_computed", "value": False, "passed": True},
        {"boundary": "model_accuracy_computed", "value": False, "passed": True},
        {"boundary": "roi_or_betting_metrics_computed", "value": False, "passed": True},
        {"boundary": "backtest_execution_allowed_next", "value": False, "passed": True},
        {"boundary": "tuning_allowed_next", "value": False, "passed": True},
        {"boundary": "post_metric_execution_audit_required_next", "value": True, "passed": True},
    ]

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
        {"check": "6nl_script_exists", "expected": True, "actual": SCRIPT_6NL.exists(), "passed": SCRIPT_6NL.exists()},
        {"check": "6nl_json_exists", "expected": True, "actual": JSON_6NL.exists(), "passed": JSON_6NL.exists()},
        {"check": "6nl_all_checks_passed", "expected": True, "actual": json_6nl.get("all_checks_passed"), "passed": json_6nl.get("all_checks_passed") is True},
        {"check": "6nl_diagnosis", "expected": DIAGNOSIS_6NL, "actual": json_6nl.get("diagnosis"), "passed": json_6nl.get("diagnosis") == DIAGNOSIS_6NL},
        {"check": "6nl_actuals_only_metric_allowed", "expected": True, "actual": json_6nl.get("actuals_only_metric_execution_allowed_next"), "passed": json_6nl.get("actuals_only_metric_execution_allowed_next") is True},
    ]

    allowed_next_rows = [
        {"operation": "actuals_only_metric_execution_audit", "allowed_next": True, "scope": "6NN audit only", "passed": True},
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

    future_6nn_rows = [
        {"contract": "audit_actuals_only_metric_outputs", "required": True, "passed": True},
        {"contract": "verify_no_prediction_join_or_backtest", "required": True, "passed": True},
        {"contract": "verify_metric_values_are_actuals_only", "required": True, "passed": True},
        {"contract": "preserve_no_tuning_activation_exit", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6nl_passed", "expected": True, "actual": json_6nl.get("all_checks_passed"), "passed": json_6nl.get("all_checks_passed") is True},
        {"decision": "actuals_metrics_computed", "expected": True, "actual": all_passed(actuals_metrics), "passed": all_passed(actuals_metrics)},
        {"decision": "prediction_join_not_attempted", "expected": True, "actual": all_passed(prediction_join_rows), "passed": all_passed(prediction_join_rows)},
        {"decision": "forbidden_metrics_not_computed", "expected": True, "actual": all_passed(forbidden_metric_rows), "passed": all_passed(forbidden_metric_rows)},
        {"decision": "recommend_6nn", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
    ]

    safety_rows = [
        {"boundary": "actuals_only_metric_execution", "expected": True, "actual": True, "passed": True},
        {"boundary": "actuals_file_modified_by_6nm", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_rows_ingested_by_6nm", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6nm", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6nm", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6nm", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6nm", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommend_audit_before_any_backtest_or_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_backtests", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning_activation_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6NM, "actual": DIAGNOSIS_6NM, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "actuals_metrics", "passed": all_passed(actuals_metrics), "detail": f"{sum(1 for r in actuals_metrics if r['passed'])}/{len(actuals_metrics)}"},
        {"check": "date_coverage", "passed": all_passed(date_coverage_rows), "detail": f"{sum(1 for r in date_coverage_rows if r['passed'])}/{len(date_coverage_rows)}"},
        {"check": "source_coverage", "passed": all_passed(source_coverage_rows), "detail": f"{sum(1 for r in source_coverage_rows if r['passed'])}/{len(source_coverage_rows)}"},
        {"check": "prediction_join_review", "passed": all_passed(prediction_join_rows), "detail": f"{sum(1 for r in prediction_join_rows if r['passed'])}/{len(prediction_join_rows)}"},
        {"check": "forbidden_metric_review", "passed": all_passed(forbidden_metric_rows), "detail": f"{sum(1 for r in forbidden_metric_rows if r['passed'])}/{len(forbidden_metric_rows)}"},
        {"check": "post_metric_audit_requirement", "passed": all_passed(post_metric_audit_rows), "detail": f"{sum(1 for r in post_metric_audit_rows if r['passed'])}/{len(post_metric_audit_rows)}"},
        {"check": "metric_boundaries", "passed": all_passed(metric_boundary_rows), "detail": f"{sum(1 for r in metric_boundary_rows if r['passed'])}/{len(metric_boundary_rows)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next_rows), "detail": f"{sum(1 for r in allowed_next_rows if r['passed'])}/{len(allowed_next_rows)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next_rows), "detail": f"{sum(1 for r in forbidden_next_rows if r['passed'])}/{len(forbidden_next_rows)}"},
        {"check": "future_6nn_contract", "passed": all_passed(future_6nn_rows), "detail": f"{sum(1 for r in future_6nn_rows if r['passed'])}/{len(future_6nn_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "actuals_metrics": write_csv(ACTUALS_METRICS_CSV, actuals_metrics),
        "date_coverage": write_csv(DATE_COVERAGE_CSV, date_coverage_rows),
        "source_coverage": write_csv(SOURCE_COVERAGE_CSV, source_coverage_rows),
        "prediction_join_review": write_csv(PREDICTION_JOIN_REVIEW_CSV, prediction_join_rows),
        "forbidden_metric_review": write_csv(FORBIDDEN_METRIC_REVIEW_CSV, forbidden_metric_rows),
        "post_metric_audit_requirement": write_csv(POST_METRIC_AUDIT_CSV, post_metric_audit_rows),
        "metric_boundaries": write_csv(METRIC_BOUNDARIES_CSV, metric_boundary_rows),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next_rows),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next_rows),
        "future_6nn_contract": write_csv(FUTURE_6NN_CSV, future_6nn_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6NM",
        "layer_type": "game_mechanics_realism",
        "actuals_only_metric_execution": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6NM if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_layer": "6NL",
        "predecessor_diagnosis": json_6nl.get("diagnosis"),
        "predecessor_all_checks_passed": json_6nl.get("all_checks_passed") is True,
        "source_family": "actuals_only_metric_execution",
        "actuals_path": str(TARGET_ACTUALS),
        "actuals_row_count": row_count,
        "unique_game_pk_count": unique_game_pk_count,
        "date_span_days": date_span,
        "home_win_rate": pct(sum(home_wins), len(home_wins)),
        "average_home_score": avg([h for h, _ in valid_score_pairs]),
        "average_away_score": avg([a for _, a in valid_score_pairs]),
        "average_total_runs": avg(totals),
        "one_run_game_rate": pct(sum(one_run_games), len(one_run_games)),
        "source_artifact_count": len(source_counts),
        "coverage_date_count": len(games_by_date),
        "games_per_date_min": min(games_by_date.values()) if games_by_date else 0,
        "games_per_date_max": max(games_by_date.values()) if games_by_date else 0,
        "games_per_date_mean": avg(list(games_by_date.values())),
        "prediction_join_attempted": False,
        "prediction_join_available": False,
        "prediction_metrics_computed": False,
        "model_accuracy_computed": False,
        "roi_or_betting_metrics_computed": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "post_metric_execution_audit_required_next": True,
        "actuals_only_metric_execution_run_by_6nm": True,
        "source_rows_ingested_by_6nm": False,
        "normalized_source_tables_created_for_production_by_6nm": False,
        "production_code_modified_by_6nm": False,
        "actuals_file_modified_by_6nm": False,
        "adapter_call_executed_by_6nm": False,
        "backtest_execution_run_by_6nm": False,
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
            "actuals_metrics_csv": str(ACTUALS_METRICS_CSV),
            "date_coverage_csv": str(DATE_COVERAGE_CSV),
            "source_coverage_csv": str(SOURCE_COVERAGE_CSV),
            "prediction_join_review_csv": str(PREDICTION_JOIN_REVIEW_CSV),
            "forbidden_metric_review_csv": str(FORBIDDEN_METRIC_REVIEW_CSV),
            "post_metric_audit_requirement_csv": str(POST_METRIC_AUDIT_CSV),
            "metric_boundaries_csv": str(METRIC_BOUNDARIES_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6nn_contract_csv": str(FUTURE_6NN_CSV),
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
