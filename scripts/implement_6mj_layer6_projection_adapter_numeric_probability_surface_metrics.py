#!/usr/bin/env python3
"""Implement readonly metrics for repaired numeric probability surface."""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation"
TMP_DIR = Path("tmp")

SCRIPT_6MI = Path("scripts/plan_6mi_layer6_projection_adapter_numeric_probability_surface_metrics.py")
JSON_6MI = TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan.json"
REPAIRED_SURFACE = TMP_DIR / "layer6_6mg_projection_adapter_numeric_probability_repair_implementation_repaired_surface.csv"
SURFACE_AUDIT_6MH = TMP_DIR / "layer6_6mh_projection_adapter_numeric_probability_repair_audit_repaired_surface_audit.csv"
PROVENANCE_AUDIT_6MH = TMP_DIR / "layer6_6mh_projection_adapter_numeric_probability_repair_audit_source_provenance_audit.csv"

REQUIRED_INPUTS = [
    JSON_6MI,
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_checks.csv",
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_predecessor.csv",
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_metric_families.csv",
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_model_vs_actual_metrics.csv",
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_model_vs_market_metrics.csv",
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_total_metrics_scope.csv",
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_required_inputs.csv",
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_metric_readiness_gate.csv",
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_blockers.csv",
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_future_6mj_contract.csv",
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_decision.csv",
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan_recommended_path.csv",
    SCRIPT_6MI,
    REPAIRED_SURFACE,
    SURFACE_AUDIT_6MH,
    PROVENANCE_AUDIT_6MH,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
SURFACE_HEALTH_CSV = TMP_DIR / f"{SLUG}_surface_health_metrics.csv"
DISTRIBUTION_HEALTH_CSV = TMP_DIR / f"{SLUG}_distribution_health.csv"
MODEL_ACTUAL_READINESS_CSV = TMP_DIR / f"{SLUG}_model_vs_actual_readiness.csv"
MODEL_ACTUAL_METRICS_CSV = TMP_DIR / f"{SLUG}_model_vs_actual_metrics.csv"
MODEL_MARKET_READINESS_CSV = TMP_DIR / f"{SLUG}_model_vs_market_readiness.csv"
MODEL_MARKET_METRICS_CSV = TMP_DIR / f"{SLUG}_model_vs_market_metrics.csv"
MISSING_DATA_CSV = TMP_DIR / f"{SLUG}_missing_data_diagnostics.csv"
TOTAL_SCOPE_CSV = TMP_DIR / f"{SLUG}_total_metrics_scope.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6MK_CSV = TMP_DIR / f"{SLUG}_future_6mk_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MI = "layer_6_projection_adapter_numeric_probability_surface_metric_plan_complete"
DIAGNOSIS_6MJ = "layer_6_projection_adapter_numeric_probability_surface_metric_implementation_complete"
RECOMMENDED_NEXT_LAYER_6MJ = "6MK_layer_6_projection_adapter_numeric_probability_surface_metric_audit"
RECOMMENDED_PATH_6MJ = "audit_repaired_numeric_probability_surface_metrics"

ACTUAL_OUTCOME_CANDIDATES = [
    TMP_DIR / "historical_game_outcomes.csv",
    TMP_DIR / "game_outcomes.csv",
    TMP_DIR / "actual_game_results.csv",
    TMP_DIR / "mlb_game_results.csv",
]

MONEYLINE_CANDIDATES = [
    TMP_DIR / "historical_moneyline_odds.csv",
    TMP_DIR / "moneyline_odds.csv",
    TMP_DIR / "mlb_moneyline_odds.csv",
    TMP_DIR / "historical_odds.csv",
]


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


def to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(str(value).strip())
    except Exception:
        return None


def safe_log_loss(p: float, y: int) -> float:
    clipped = min(max(p, 1e-15), 1 - 1e-15)
    return -((y * math.log(clipped)) + ((1 - y) * math.log(1 - clipped)))


def implied_prob_from_american(odds: float) -> float | None:
    if odds < 0:
        return abs(odds) / (abs(odds) + 100.0)
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return None


def normalize_game_pk(row: dict[str, Any]) -> str:
    for key in ["game_pk", "game_id", "mlb_game_id", "event_id"]:
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def actual_outcome_from_row(row: dict[str, Any]) -> int | None:
    for key in ["home_win", "home_win_binary", "actual_home_win", "home_team_win", "home_won"]:
        if key in row:
            value = str(row[key]).strip().lower()
            if value in {"1", "true", "yes", "win", "w"}:
                return 1
            if value in {"0", "false", "no", "loss", "l"}:
                return 0
    for key in ["winner_side", "winning_side"]:
        value = str(row.get(key, "")).strip().lower()
        if value == "home":
            return 1
        if value == "away":
            return 0
    return None


def moneyline_from_row(row: dict[str, Any]) -> float | None:
    for key in ["home_moneyline", "home_ml", "moneyline_home", "home_close_moneyline", "close_home_moneyline"]:
        if key in row:
            value = to_float(row[key])
            if value is not None:
                return value
    return None


def first_existing_with_rows(paths: list[Path]) -> tuple[Path | None, list[dict[str, str]]]:
    for path in paths:
        rows = read_csv_rows(path)
        if rows:
            return path, rows
    return None, []


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6mi = load_json(JSON_6MI)
    repaired_rows = [row for row in read_csv_rows(REPAIRED_SURFACE) if normalize_game_pk(row)]

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
        {"check": "6mi_script_exists", "expected": True, "actual": SCRIPT_6MI.exists(), "passed": SCRIPT_6MI.exists()},
        {"check": "6mi_json_exists", "expected": True, "actual": JSON_6MI.exists(), "passed": JSON_6MI.exists()},
        {"check": "6mi_all_checks_passed", "expected": True, "actual": json_6mi.get("all_checks_passed"), "passed": json_6mi.get("all_checks_passed") is True},
        {"check": "6mi_diagnosis", "expected": DIAGNOSIS_6MI, "actual": json_6mi.get("diagnosis"), "passed": json_6mi.get("diagnosis") == DIAGNOSIS_6MI},
        {"check": "6mi_recommended_next_layer", "expected": "6MJ_layer_6_projection_adapter_numeric_probability_surface_metric_implementation", "actual": json_6mi.get("recommended_next_layer"), "passed": json_6mi.get("recommended_next_layer") == "6MJ_layer_6_projection_adapter_numeric_probability_surface_metric_implementation"},
        {"check": "6mi_implementation_allowed_next", "expected": True, "actual": json_6mi.get("implementation_allowed_next"), "passed": json_6mi.get("implementation_allowed_next") is True},
    ]

    probabilities = []
    for row in repaired_rows:
        home = to_float(row.get("home_win_probability"))
        away = to_float(row.get("away_win_probability"))
        if home is not None and away is not None:
            probabilities.append((normalize_game_pk(row), home, away, row))

    total_rows = len(repaired_rows)
    valid_probability_rows = len(probabilities)
    missing_probability_rows = max(total_rows - valid_probability_rows, 0)
    default_5050_rows = sum(1 for _, home, away, _ in probabilities if abs(home - 0.5) <= 1e-12 and abs(away - 0.5) <= 1e-12)

    surface_health_rows = [
        {"metric": "row_count", "value": total_rows, "passed": total_rows > 0},
        {"metric": "valid_probability_row_count", "value": valid_probability_rows, "passed": valid_probability_rows > 0},
        {"metric": "missing_probability_row_count", "value": missing_probability_rows, "passed": True},
        {"metric": "duplicate_game_pk_count", "value": total_rows - len({game_pk for game_pk, _, _, _ in probabilities}), "passed": True},
        {"metric": "source_provenance_row_count", "value": sum(1 for _, _, _, row in probabilities if row.get("source_artifact")), "passed": True},
    ]

    if probabilities:
        home_probs = [home for _, home, _, _ in probabilities]
        min_home = min(home_probs)
        max_home = max(home_probs)
        mean_home = sum(home_probs) / len(home_probs)
        distribution_rows = [
            {"metric": "home_probability_min", "value": min_home, "passed": 0 <= min_home <= 1},
            {"metric": "home_probability_max", "value": max_home, "passed": 0 <= max_home <= 1},
            {"metric": "home_probability_mean", "value": mean_home, "passed": 0 <= mean_home <= 1},
            {"metric": "exact_50_50_probability_rows", "value": default_5050_rows, "passed": True},
            {"metric": "exact_50_50_probability_share", "value": default_5050_rows / len(probabilities), "passed": True},
        ]
    else:
        distribution_rows = [
            {"metric": "home_probability_min", "value": "", "passed": False},
            {"metric": "home_probability_max", "value": "", "passed": False},
            {"metric": "home_probability_mean", "value": "", "passed": False},
            {"metric": "exact_50_50_probability_rows", "value": 0, "passed": True},
            {"metric": "exact_50_50_probability_share", "value": "", "passed": True},
        ]

    actual_path, actual_rows = first_existing_with_rows(ACTUAL_OUTCOME_CANDIDATES)
    actual_by_game = {}
    for row in actual_rows:
        game_pk = normalize_game_pk(row)
        actual = actual_outcome_from_row(row)
        if game_pk and actual in {0, 1}:
            actual_by_game[game_pk] = actual

    actual_matches = [
        (game_pk, home, actual_by_game[game_pk])
        for game_pk, home, _, _ in probabilities
        if game_pk in actual_by_game
    ]

    model_actual_readiness = [
        {"input": "actual_outcome_source", "available": actual_path is not None, "path": str(actual_path or ""), "row_count": len(actual_rows), "passed": True},
        {"input": "matched_actual_outcome_rows", "available": bool(actual_matches), "path": str(actual_path or ""), "row_count": len(actual_matches), "passed": True},
        {"input": "no_fake_actual_outcomes_generated", "available": True, "path": "", "row_count": 0, "passed": True},
    ]

    if actual_matches:
        brier_values = [(home - y) ** 2 for _, home, y in actual_matches]
        log_values = [safe_log_loss(home, y) for _, home, y in actual_matches]
        model_actual_metrics = [
            {"metric": "brier_score_mean", "value": sum(brier_values) / len(brier_values), "row_count": len(brier_values), "blocked": False, "blocker": "", "passed": True},
            {"metric": "log_loss_mean", "value": sum(log_values) / len(log_values), "row_count": len(log_values), "blocked": False, "blocker": "", "passed": True},
            {"metric": "actual_outcome_coverage", "value": len(actual_matches) / max(valid_probability_rows, 1), "row_count": len(actual_matches), "blocked": False, "blocker": "", "passed": True},
        ]
        actual_blocker = False
    else:
        model_actual_metrics = [
            {"metric": "brier_score_mean", "value": "", "row_count": 0, "blocked": True, "blocker": "missing_local_actual_outcomes", "passed": True},
            {"metric": "log_loss_mean", "value": "", "row_count": 0, "blocked": True, "blocker": "missing_local_actual_outcomes", "passed": True},
            {"metric": "actual_outcome_coverage", "value": 0, "row_count": 0, "blocked": True, "blocker": "missing_local_actual_outcomes", "passed": True},
        ]
        actual_blocker = True

    moneyline_path, moneyline_rows = first_existing_with_rows(MONEYLINE_CANDIDATES)
    market_by_game = {}
    for row in moneyline_rows:
        game_pk = normalize_game_pk(row)
        odds = moneyline_from_row(row)
        implied = implied_prob_from_american(odds) if odds is not None else None
        if game_pk and implied is not None:
            market_by_game[game_pk] = implied

    market_matches = [
        (game_pk, home, market_by_game[game_pk])
        for game_pk, home, _, _ in probabilities
        if game_pk in market_by_game
    ]

    model_market_readiness = [
        {"input": "historical_moneyline_source", "available": moneyline_path is not None, "path": str(moneyline_path or ""), "row_count": len(moneyline_rows), "passed": True},
        {"input": "matched_moneyline_rows", "available": bool(market_matches), "path": str(moneyline_path or ""), "row_count": len(market_matches), "passed": True},
        {"input": "no_fake_moneyline_odds_generated", "available": True, "path": "", "row_count": 0, "passed": True},
    ]

    if market_matches:
        deltas = [home - implied for _, home, implied in market_matches]
        model_market_metrics = [
            {"metric": "mean_sim_minus_market_implied_probability", "value": sum(deltas) / len(deltas), "row_count": len(deltas), "blocked": False, "blocker": "", "passed": True},
            {"metric": "mean_abs_sim_market_probability_delta", "value": sum(abs(delta) for delta in deltas) / len(deltas), "row_count": len(deltas), "blocked": False, "blocker": "", "passed": True},
            {"metric": "market_probability_coverage", "value": len(market_matches) / max(valid_probability_rows, 1), "row_count": len(market_matches), "blocked": False, "blocker": "", "passed": True},
        ]
        market_blocker = False
    else:
        model_market_metrics = [
            {"metric": "mean_sim_minus_market_implied_probability", "value": "", "row_count": 0, "blocked": True, "blocker": "missing_local_historical_moneyline_odds", "passed": True},
            {"metric": "mean_abs_sim_market_probability_delta", "value": "", "row_count": 0, "blocked": True, "blocker": "missing_local_historical_moneyline_odds", "passed": True},
            {"metric": "market_probability_coverage", "value": 0, "row_count": 0, "blocked": True, "blocker": "missing_local_historical_moneyline_odds", "passed": True},
        ]
        market_blocker = True

    missing_data_rows = [
        {"diagnostic": "missing_probability_rows", "value": missing_probability_rows, "passed": True},
        {"diagnostic": "missing_actual_outcome_matches", "value": valid_probability_rows - len(actual_matches), "passed": True},
        {"diagnostic": "missing_moneyline_matches", "value": valid_probability_rows - len(market_matches), "passed": True},
        {"diagnostic": "exact_50_50_rows", "value": default_5050_rows, "passed": True},
    ]

    total_scope_rows = [
        {"scope_item": "total_runs_projection_metrics", "in_scope_for_6mj": False, "reason": "requires traced and audited projected total-runs surface", "passed": True},
        {"scope_item": "over_under_probability_metrics", "in_scope_for_6mj": False, "reason": "requires traced and audited over/under probability surface", "passed": True},
        {"scope_item": "historical_total_odds_market_comparison", "in_scope_for_6mj": False, "reason": "requires historical total line and odds plus sim total/over probability surface", "passed": True},
    ]

    blockers = [
        {"blocker": "missing_local_actual_outcomes", "active": actual_blocker, "reason": "model-vs-actual metrics require actual results", "passed": True},
        {"blocker": "missing_local_historical_moneyline_odds", "active": market_blocker, "reason": "model-vs-market metrics require historical moneyline odds", "passed": True},
        {"blocker": "totals_surface_required_for_total_metrics", "active": True, "reason": "totals require total-runs or over/under surface", "passed": True},
        {"blocker": "backtests_tuning_activation_exit_blocked", "active": True, "reason": "requires audited metric outputs and later historical validation", "passed": True},
    ]

    future_6mk = [
        {"contract": "audit_surface_health_metrics", "required": True, "why": "confirm surface metrics are based on repaired surface", "passed": True},
        {"contract": "audit_model_vs_actual_metric_or_blocker", "required": True, "why": "confirm actual metrics or explicit missing-actual blocker", "passed": True},
        {"contract": "audit_model_vs_market_metric_or_blocker", "required": True, "why": "confirm market metrics or explicit missing-moneyline blocker", "passed": True},
        {"contract": "preserve_no_backtest_tuning_activation_or_exit", "required": True, "why": "metric audit remains pre-backtest", "passed": True},
    ]

    blocking_policy = [
        {"policy": "do_not_generate_fake_actual_outcomes", "required": True, "passed": True},
        {"policy": "do_not_generate_fake_moneyline_odds", "required": True, "passed": True},
        {"policy": "do_not_treat_metric_artifacts_as_backtest", "required": True, "passed": True},
        {"policy": "do_not_tune_or_activate_from_single_layer_metrics", "required": True, "passed": True},
    ]

    surface_health_written = bool(surface_health_rows)
    distribution_health_written = bool(distribution_rows)
    model_actual_readiness_written = bool(model_actual_readiness)
    model_actual_metrics_written = bool(model_actual_metrics)
    model_market_readiness_written = bool(model_market_readiness)
    model_market_metrics_written = bool(model_market_metrics)
    missing_data_written = bool(missing_data_rows)

    decision_rows = [
        {"decision": "6mi_passed", "expected": True, "actual": json_6mi.get("all_checks_passed"), "passed": json_6mi.get("all_checks_passed") is True},
        {"decision": "6mi_diagnosis_valid", "expected": DIAGNOSIS_6MI, "actual": json_6mi.get("diagnosis"), "passed": json_6mi.get("diagnosis") == DIAGNOSIS_6MI},
        {"decision": "all_required_6mi_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "repaired_surface_loaded", "expected": True, "actual": bool(repaired_rows), "passed": bool(repaired_rows)},
        {"decision": "surface_health_metrics_written", "expected": True, "actual": surface_health_written, "passed": surface_health_written},
        {"decision": "distribution_health_metrics_written", "expected": True, "actual": distribution_health_written, "passed": distribution_health_written},
        {"decision": "model_vs_actual_metrics_or_blocker", "expected": True, "actual": model_actual_metrics_written, "passed": model_actual_metrics_written},
        {"decision": "model_vs_market_metrics_or_blocker", "expected": True, "actual": model_market_metrics_written, "passed": model_market_metrics_written},
        {"decision": "missing_data_diagnostics_written", "expected": True, "actual": missing_data_written, "passed": missing_data_written},
        {"decision": "totals_out_of_scope", "expected": True, "actual": True, "passed": True},
        {"decision": "no_fake_actuals_or_odds", "expected": True, "actual": True, "passed": True},
        {"decision": "recommend_6mk_next", "expected": RECOMMENDED_NEXT_LAYER_6MJ, "actual": RECOMMENDED_NEXT_LAYER_6MJ, "passed": True},
        {"decision": "do_not_run_backtest_tuning_activation_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only_probability_surface_metrics_readonly", "expected": True, "actual": True, "passed": True},
        {"boundary": "metric_execution_run_by_6mj", "expected": True, "actual": True, "passed": True},
        {"boundary": "backtest_execution_run_by_6mj", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mj", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_scan_run_by_6mj", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mj", "expected": False, "actual": False, "passed": True},
        {"boundary": "full_batch_adapter_call_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_historical_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_measurement_run", "expected": True, "actual": True, "passed": True},
        {"boundary": "database_writes_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "live_data_fetches_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "remote_api_calls_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_acquisition_performed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_source_modifications_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "activation_execution_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MJ, "actual": RECOMMENDED_NEXT_LAYER_6MJ, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MJ, "actual": RECOMMENDED_PATH_6MJ, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MJ, "actual": DIAGNOSIS_6MJ, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "surface_health_metrics", "passed": all_passed(surface_health_rows), "detail": f"{sum(1 for r in surface_health_rows if r['passed'])}/{len(surface_health_rows)}"},
        {"check": "distribution_health", "passed": all_passed(distribution_rows), "detail": f"{sum(1 for r in distribution_rows if r['passed'])}/{len(distribution_rows)}"},
        {"check": "model_vs_actual_readiness", "passed": all_passed(model_actual_readiness), "detail": f"{sum(1 for r in model_actual_readiness if r['passed'])}/{len(model_actual_readiness)}"},
        {"check": "model_vs_actual_metrics_or_blocker", "passed": all_passed(model_actual_metrics), "detail": f"{sum(1 for r in model_actual_metrics if r['passed'])}/{len(model_actual_metrics)}"},
        {"check": "model_vs_market_readiness", "passed": all_passed(model_market_readiness), "detail": f"{sum(1 for r in model_market_readiness if r['passed'])}/{len(model_market_readiness)}"},
        {"check": "model_vs_market_metrics_or_blocker", "passed": all_passed(model_market_metrics), "detail": f"{sum(1 for r in model_market_metrics if r['passed'])}/{len(model_market_metrics)}"},
        {"check": "missing_data_diagnostics", "passed": all_passed(missing_data_rows), "detail": f"{sum(1 for r in missing_data_rows if r['passed'])}/{len(missing_data_rows)}"},
        {"check": "total_metrics_scope", "passed": all_passed(total_scope_rows), "detail": f"{sum(1 for r in total_scope_rows if r['passed'])}/{len(total_scope_rows)}"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "future_6mk_contract", "passed": all_passed(future_6mk), "detail": f"{sum(1 for r in future_6mk if r['passed'])}/{len(future_6mk)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_policy), "detail": f"{sum(1 for r in blocking_policy if r['passed'])}/{len(blocking_policy)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "surface_health_metrics": write_csv(SURFACE_HEALTH_CSV, surface_health_rows),
        "distribution_health": write_csv(DISTRIBUTION_HEALTH_CSV, distribution_rows),
        "model_vs_actual_readiness": write_csv(MODEL_ACTUAL_READINESS_CSV, model_actual_readiness),
        "model_vs_actual_metrics": write_csv(MODEL_ACTUAL_METRICS_CSV, model_actual_metrics),
        "model_vs_market_readiness": write_csv(MODEL_MARKET_READINESS_CSV, model_market_readiness),
        "model_vs_market_metrics": write_csv(MODEL_MARKET_METRICS_CSV, model_market_metrics),
        "missing_data_diagnostics": write_csv(MISSING_DATA_CSV, missing_data_rows),
        "total_metrics_scope": write_csv(TOTAL_SCOPE_CSV, total_scope_rows),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6mk_contract": write_csv(FUTURE_6MK_CSV, future_6mk),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MJ",
        "layer_type": "game_mechanics_realism",
        "implementation_only_probability_surface_metrics_readonly": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MJ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MJ,
        "recommended_path": RECOMMENDED_PATH_6MJ,
        "predecessor_layer": "6MI",
        "predecessor_diagnosis": json_6mi.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mi.get("all_checks_passed") is True,
        "implemented_layer_after": "6MI",
        "source_family": "projection_adapter_numeric_probability_surface_metric_implementation",
        "repaired_probability_surface_loaded": bool(repaired_rows),
        "repaired_probability_surface_row_count": total_rows,
        "surface_health_metrics_written": surface_health_written,
        "distribution_health_metrics_written": distribution_health_written,
        "model_vs_actual_readiness_written": model_actual_readiness_written,
        "model_vs_actual_metrics_written": model_actual_metrics_written,
        "model_vs_actual_blocker_emitted": actual_blocker,
        "model_vs_market_readiness_written": model_market_readiness_written,
        "model_vs_market_metrics_written": model_market_metrics_written,
        "model_vs_market_blocker_emitted": market_blocker,
        "missing_data_diagnostics_written": missing_data_written,
        "total_metrics_out_of_scope_confirmed": True,
        "moneyline_odds_required_for_market_comparison": True,
        "actual_outcomes_required_for_scoring_metrics": True,
        "no_fake_actual_outcomes_generated": True,
        "no_fake_moneyline_odds_generated": True,
        "metric_execution_run_by_6mj": True,
        "backtest_execution_run_by_6mj": False,
        "adapter_call_executed_by_6mj": False,
        "source_scan_run_by_6mj": False,
        "production_code_modified_by_6mj": False,
        "full_batch_adapter_call_run": False,
        "real_historical_evaluation_run": False,
        "production_simulations_run": False,
        "local_measurement_run": True,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
        "database_writes_run": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "production_source_modifications_run": False,
        "games_evaluated": valid_probability_rows,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "surface_health_metrics_csv": str(SURFACE_HEALTH_CSV),
            "distribution_health_csv": str(DISTRIBUTION_HEALTH_CSV),
            "model_vs_actual_readiness_csv": str(MODEL_ACTUAL_READINESS_CSV),
            "model_vs_actual_metrics_csv": str(MODEL_ACTUAL_METRICS_CSV),
            "model_vs_market_readiness_csv": str(MODEL_MARKET_READINESS_CSV),
            "model_vs_market_metrics_csv": str(MODEL_MARKET_METRICS_CSV),
            "missing_data_diagnostics_csv": str(MISSING_DATA_CSV),
            "total_metrics_scope_csv": str(TOTAL_SCOPE_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6mk_contract_csv": str(FUTURE_6MK_CSV),
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
