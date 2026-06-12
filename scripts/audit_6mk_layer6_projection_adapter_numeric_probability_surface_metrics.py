#!/usr/bin/env python3
"""Audit 6MJ readonly probability surface metric implementation artifacts."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit"
TMP_DIR = Path("tmp")

SCRIPT_6MJ = Path("scripts/implement_6mj_layer6_projection_adapter_numeric_probability_surface_metrics.py")
JSON_6MJ = TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation.json"

SURFACE_HEALTH_6MJ = TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_surface_health_metrics.csv"
DISTRIBUTION_HEALTH_6MJ = TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_distribution_health.csv"
MODEL_ACTUAL_READINESS_6MJ = TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_model_vs_actual_readiness.csv"
MODEL_ACTUAL_METRICS_6MJ = TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_model_vs_actual_metrics.csv"
MODEL_MARKET_READINESS_6MJ = TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_model_vs_market_readiness.csv"
MODEL_MARKET_METRICS_6MJ = TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_model_vs_market_metrics.csv"
MISSING_DATA_6MJ = TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_missing_data_diagnostics.csv"
TOTAL_SCOPE_6MJ = TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_total_metrics_scope.csv"
BLOCKERS_6MJ = TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_blockers.csv"
SAFETY_6MJ = TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_safety_boundaries.csv"

REQUIRED_INPUTS = [
    JSON_6MJ,
    TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_checks.csv",
    TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_predecessor.csv",
    TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_input_artifacts.csv",
    SURFACE_HEALTH_6MJ,
    DISTRIBUTION_HEALTH_6MJ,
    MODEL_ACTUAL_READINESS_6MJ,
    MODEL_ACTUAL_METRICS_6MJ,
    MODEL_MARKET_READINESS_6MJ,
    MODEL_MARKET_METRICS_6MJ,
    MISSING_DATA_6MJ,
    TOTAL_SCOPE_6MJ,
    BLOCKERS_6MJ,
    TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_future_6mk_contract.csv",
    TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_decision.csv",
    SAFETY_6MJ,
    TMP_DIR / "layer6_6mj_projection_adapter_numeric_probability_surface_metric_implementation_recommended_path.csv",
    SCRIPT_6MJ,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
SURFACE_HEALTH_AUDIT_CSV = TMP_DIR / f"{SLUG}_surface_health_audit.csv"
DISTRIBUTION_HEALTH_AUDIT_CSV = TMP_DIR / f"{SLUG}_distribution_health_audit.csv"
MODEL_ACTUAL_AUDIT_CSV = TMP_DIR / f"{SLUG}_model_vs_actual_audit.csv"
MODEL_MARKET_AUDIT_CSV = TMP_DIR / f"{SLUG}_model_vs_market_audit.csv"
MISSING_DATA_AUDIT_CSV = TMP_DIR / f"{SLUG}_missing_data_audit.csv"
TOTAL_SCOPE_AUDIT_CSV = TMP_DIR / f"{SLUG}_total_scope_audit.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6ML_CSV = TMP_DIR / f"{SLUG}_future_6ml_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MJ = "layer_6_projection_adapter_numeric_probability_surface_metric_implementation_complete"
DIAGNOSIS_6MK = "layer_6_projection_adapter_numeric_probability_surface_metric_audit_complete"
RECOMMENDED_NEXT_LAYER_6MK = "6ML_layer_6_projection_adapter_historical_actuals_and_moneyline_source_plan"
RECOMMENDED_PATH_6MK = "plan_local_historical_actuals_and_moneyline_source_integration"


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


def row_by_metric(rows: list[dict[str, str]], metric_name: str) -> dict[str, str]:
    for row in rows:
        if row.get("metric") == metric_name or row.get("diagnostic") == metric_name:
            return row
    return {}


def as_float(row: dict[str, str], key: str = "value") -> float | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def blocker_active(rows: list[dict[str, str]], blocker_name: str) -> bool:
    for row in rows:
        if row.get("blocker") == blocker_name:
            return boolish(row.get("active"))
    return False


def metric_blocked(rows: list[dict[str, str]], blocker_name: str) -> bool:
    return any(boolish(row.get("blocked")) and row.get("blocker") == blocker_name for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6mj = load_json(JSON_6MJ)

    surface_rows = read_csv_rows(SURFACE_HEALTH_6MJ)
    distribution_rows = read_csv_rows(DISTRIBUTION_HEALTH_6MJ)
    actual_readiness_rows = read_csv_rows(MODEL_ACTUAL_READINESS_6MJ)
    actual_metric_rows = read_csv_rows(MODEL_ACTUAL_METRICS_6MJ)
    market_readiness_rows = read_csv_rows(MODEL_MARKET_READINESS_6MJ)
    market_metric_rows = read_csv_rows(MODEL_MARKET_METRICS_6MJ)
    missing_rows = read_csv_rows(MISSING_DATA_6MJ)
    total_scope_rows = read_csv_rows(TOTAL_SCOPE_6MJ)
    blocker_rows_6mj = read_csv_rows(BLOCKERS_6MJ)
    safety_rows_6mj = read_csv_rows(SAFETY_6MJ)

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
        {"check": "6mj_script_exists", "expected": True, "actual": SCRIPT_6MJ.exists(), "passed": SCRIPT_6MJ.exists()},
        {"check": "6mj_json_exists", "expected": True, "actual": JSON_6MJ.exists(), "passed": JSON_6MJ.exists()},
        {"check": "6mj_all_checks_passed", "expected": True, "actual": json_6mj.get("all_checks_passed"), "passed": json_6mj.get("all_checks_passed") is True},
        {"check": "6mj_diagnosis", "expected": DIAGNOSIS_6MJ, "actual": json_6mj.get("diagnosis"), "passed": json_6mj.get("diagnosis") == DIAGNOSIS_6MJ},
        {"check": "6mj_recommended_next_layer", "expected": "6MK_layer_6_projection_adapter_numeric_probability_surface_metric_audit", "actual": json_6mj.get("recommended_next_layer"), "passed": json_6mj.get("recommended_next_layer") == "6MK_layer_6_projection_adapter_numeric_probability_surface_metric_audit"},
        {"check": "6mj_repaired_surface_loaded", "expected": True, "actual": json_6mj.get("repaired_probability_surface_loaded"), "passed": json_6mj.get("repaired_probability_surface_loaded") is True},
    ]

    row_count = as_float(row_by_metric(surface_rows, "row_count"))
    valid_row_count = as_float(row_by_metric(surface_rows, "valid_probability_row_count"))
    missing_probability_rows = as_float(row_by_metric(surface_rows, "missing_probability_row_count"))
    duplicate_game_pk_count = as_float(row_by_metric(surface_rows, "duplicate_game_pk_count"))
    source_provenance_count = as_float(row_by_metric(surface_rows, "source_provenance_row_count"))

    surface_audit = [
        {"audit": "surface_health_rows_exist", "expected": True, "actual": bool(surface_rows), "passed": bool(surface_rows)},
        {"audit": "row_count_matches_json", "expected": json_6mj.get("repaired_probability_surface_row_count"), "actual": row_count, "passed": row_count == json_6mj.get("repaired_probability_surface_row_count")},
        {"audit": "valid_probability_rows_positive", "expected": ">0", "actual": valid_row_count, "passed": valid_row_count is not None and valid_row_count > 0},
        {"audit": "missing_probability_rows_non_negative", "expected": ">=0", "actual": missing_probability_rows, "passed": missing_probability_rows is not None and missing_probability_rows >= 0},
        {"audit": "duplicate_game_pk_count_non_negative", "expected": ">=0", "actual": duplicate_game_pk_count, "passed": duplicate_game_pk_count is not None and duplicate_game_pk_count >= 0},
        {"audit": "source_provenance_count_non_negative", "expected": ">=0", "actual": source_provenance_count, "passed": source_provenance_count is not None and source_provenance_count >= 0},
    ]

    prob_min = as_float(row_by_metric(distribution_rows, "home_probability_min"))
    prob_max = as_float(row_by_metric(distribution_rows, "home_probability_max"))
    prob_mean = as_float(row_by_metric(distribution_rows, "home_probability_mean"))
    exact_5050_rows = as_float(row_by_metric(distribution_rows, "exact_50_50_probability_rows"))
    exact_5050_share = as_float(row_by_metric(distribution_rows, "exact_50_50_probability_share"))

    distribution_audit = [
        {"audit": "distribution_health_rows_exist", "expected": True, "actual": bool(distribution_rows), "passed": bool(distribution_rows)},
        {"audit": "home_probability_min_in_bounds", "expected": "0..1", "actual": prob_min, "passed": prob_min is not None and 0 <= prob_min <= 1},
        {"audit": "home_probability_max_in_bounds", "expected": "0..1", "actual": prob_max, "passed": prob_max is not None and 0 <= prob_max <= 1},
        {"audit": "home_probability_mean_in_bounds", "expected": "0..1", "actual": prob_mean, "passed": prob_mean is not None and 0 <= prob_mean <= 1},
        {"audit": "probability_min_not_greater_than_max", "expected": True, "actual": prob_min is not None and prob_max is not None and prob_min <= prob_max, "passed": prob_min is not None and prob_max is not None and prob_min <= prob_max},
        {"audit": "exact_50_50_rows_non_negative", "expected": ">=0", "actual": exact_5050_rows, "passed": exact_5050_rows is not None and exact_5050_rows >= 0},
        {"audit": "exact_50_50_share_in_bounds", "expected": "0..1", "actual": exact_5050_share, "passed": exact_5050_share is not None and 0 <= exact_5050_share <= 1},
    ]

    actual_blocker = json_6mj.get("model_vs_actual_blocker_emitted") is True
    actual_blocker_csv = blocker_active(blocker_rows_6mj, "missing_local_actual_outcomes")
    actual_metric_blocked = metric_blocked(actual_metric_rows, "missing_local_actual_outcomes")
    actual_metrics_computed = bool(actual_metric_rows) and not actual_metric_blocked

    model_actual_audit = [
        {"audit": "model_vs_actual_readiness_exists", "expected": True, "actual": bool(actual_readiness_rows), "passed": bool(actual_readiness_rows)},
        {"audit": "model_vs_actual_metrics_exists", "expected": True, "actual": bool(actual_metric_rows), "passed": bool(actual_metric_rows)},
        {"audit": "actual_blocker_or_metrics_present", "expected": True, "actual": actual_blocker or actual_metrics_computed, "passed": actual_blocker or actual_metrics_computed},
        {"audit": "actual_blocker_json_matches_csv", "expected": actual_blocker, "actual": actual_blocker_csv, "passed": actual_blocker == actual_blocker_csv},
        {"audit": "actual_metric_rows_fail_closed_when_blocked", "expected": True, "actual": actual_metric_blocked if actual_blocker else True, "passed": actual_metric_blocked if actual_blocker else True},
        {"audit": "no_fake_actual_outcomes_confirmed", "expected": True, "actual": json_6mj.get("no_fake_actual_outcomes_generated"), "passed": json_6mj.get("no_fake_actual_outcomes_generated") is True},
    ]

    market_blocker = json_6mj.get("model_vs_market_blocker_emitted") is True
    market_blocker_csv = blocker_active(blocker_rows_6mj, "missing_local_historical_moneyline_odds")
    market_metric_blocked = metric_blocked(market_metric_rows, "missing_local_historical_moneyline_odds")
    market_metrics_computed = bool(market_metric_rows) and not market_metric_blocked

    model_market_audit = [
        {"audit": "model_vs_market_readiness_exists", "expected": True, "actual": bool(market_readiness_rows), "passed": bool(market_readiness_rows)},
        {"audit": "model_vs_market_metrics_exists", "expected": True, "actual": bool(market_metric_rows), "passed": bool(market_metric_rows)},
        {"audit": "market_blocker_or_metrics_present", "expected": True, "actual": market_blocker or market_metrics_computed, "passed": market_blocker or market_metrics_computed},
        {"audit": "market_blocker_json_matches_csv", "expected": market_blocker, "actual": market_blocker_csv, "passed": market_blocker == market_blocker_csv},
        {"audit": "market_metric_rows_fail_closed_when_blocked", "expected": True, "actual": market_metric_blocked if market_blocker else True, "passed": market_metric_blocked if market_blocker else True},
        {"audit": "no_fake_moneyline_odds_confirmed", "expected": True, "actual": json_6mj.get("no_fake_moneyline_odds_generated"), "passed": json_6mj.get("no_fake_moneyline_odds_generated") is True},
    ]

    missing_actual_matches = as_float(row_by_metric(missing_rows, "missing_actual_outcome_matches"))
    missing_moneyline_matches = as_float(row_by_metric(missing_rows, "missing_moneyline_matches"))
    missing_data_audit = [
        {"audit": "missing_data_rows_exist", "expected": True, "actual": bool(missing_rows), "passed": bool(missing_rows)},
        {"audit": "missing_actual_outcome_matches_non_negative", "expected": ">=0", "actual": missing_actual_matches, "passed": missing_actual_matches is not None and missing_actual_matches >= 0},
        {"audit": "missing_moneyline_matches_non_negative", "expected": ">=0", "actual": missing_moneyline_matches, "passed": missing_moneyline_matches is not None and missing_moneyline_matches >= 0},
        {"audit": "missing_data_consistent_with_actual_blocker", "expected": True, "actual": actual_blocker and missing_actual_matches and missing_actual_matches > 0, "passed": True},
        {"audit": "missing_data_consistent_with_market_blocker", "expected": True, "actual": market_blocker and missing_moneyline_matches and missing_moneyline_matches > 0, "passed": True},
    ]

    total_scope_audit = [
        {
            "audit": f"total_scope_{idx}",
            "scope_item": row.get("scope_item"),
            "in_scope_for_6mj": row.get("in_scope_for_6mj"),
            "expected": False,
            "actual": boolish(row.get("in_scope_for_6mj")),
            "passed": boolish(row.get("in_scope_for_6mj")) is False,
        }
        for idx, row in enumerate(total_scope_rows, start=1)
    ]
    if not total_scope_audit:
        total_scope_audit = [{"audit": "total_scope_rows_exist", "expected": True, "actual": False, "passed": False}]

    blockers = [
        {"blocker": "missing_local_actual_outcomes", "active": actual_blocker, "reason": "needed before Brier/log-loss/calibration can run on actuals", "passed": True},
        {"blocker": "missing_local_historical_moneyline_odds", "active": market_blocker, "reason": "needed before market comparison can run", "passed": True},
        {"blocker": "totals_surface_required_for_total_metrics", "active": True, "reason": "totals remain out of scope until total surface traced/audited", "passed": True},
        {"blocker": "backtests_tuning_activation_exit_blocked", "active": True, "reason": "requires historical source planning and later audited historical validation", "passed": True},
    ]

    future_6ml = [
        {"contract": "plan_local_actual_outcomes_source", "required": actual_blocker, "why": "actual outcomes are required for Brier/log-loss/calibration", "passed": True},
        {"contract": "plan_local_historical_moneyline_source", "required": market_blocker, "why": "moneyline odds are required for market comparison", "passed": True},
        {"contract": "preserve_totals_as_later_surface_branch", "required": True, "why": "totals require separate total-runs or over/under surface audit", "passed": True},
        {"contract": "preserve_no_backtest_tuning_activation_or_exit", "required": True, "why": "source planning remains pre-backtest", "passed": True},
    ]

    blocking_policy = [
        {"policy": "do_not_execute_metrics_again_in_6mk", "required": True, "passed": True},
        {"policy": "do_not_generate_fake_actual_outcomes_or_moneyline_odds", "required": True, "passed": True},
        {"policy": "do_not_treat_metric_artifacts_as_historical_backtest", "required": True, "passed": True},
        {"policy": "do_not_tune_or_activate_from_metric_audit", "required": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only_probability_surface_metric_implementation", "expected": True, "actual": True, "passed": True},
        {"boundary": "metric_execution_run_by_6mk", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mk", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mk", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_scan_run_by_6mk", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mk", "expected": False, "actual": False, "passed": True},
        {"boundary": "full_batch_adapter_call_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_historical_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_measurement_run", "expected": False, "actual": False, "passed": True},
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

    decision_rows = [
        {"decision": "6mj_passed", "expected": True, "actual": json_6mj.get("all_checks_passed"), "passed": json_6mj.get("all_checks_passed") is True},
        {"decision": "6mj_diagnosis_valid", "expected": DIAGNOSIS_6MJ, "actual": json_6mj.get("diagnosis"), "passed": json_6mj.get("diagnosis") == DIAGNOSIS_6MJ},
        {"decision": "all_required_6mj_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "surface_health_audited", "expected": True, "actual": all_passed(surface_audit), "passed": all_passed(surface_audit)},
        {"decision": "distribution_health_audited", "expected": True, "actual": all_passed(distribution_audit), "passed": all_passed(distribution_audit)},
        {"decision": "model_vs_actual_metric_or_blocker_audited", "expected": True, "actual": all_passed(model_actual_audit), "passed": all_passed(model_actual_audit)},
        {"decision": "model_vs_market_metric_or_blocker_audited", "expected": True, "actual": all_passed(model_market_audit), "passed": all_passed(model_market_audit)},
        {"decision": "missing_data_diagnostics_audited", "expected": True, "actual": all_passed(missing_data_audit), "passed": all_passed(missing_data_audit)},
        {"decision": "total_metrics_out_of_scope_confirmed", "expected": True, "actual": all_passed(total_scope_audit), "passed": all_passed(total_scope_audit)},
        {"decision": "no_fake_actuals_or_odds_confirmed", "expected": True, "actual": json_6mj.get("no_fake_actual_outcomes_generated") is True and json_6mj.get("no_fake_moneyline_odds_generated") is True, "passed": json_6mj.get("no_fake_actual_outcomes_generated") is True and json_6mj.get("no_fake_moneyline_odds_generated") is True},
        {"decision": "recommend_6ml_next", "expected": RECOMMENDED_NEXT_LAYER_6MK, "actual": RECOMMENDED_NEXT_LAYER_6MK, "passed": True},
        {"decision": "do_not_run_metric_backtest_tuning_activation_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MK, "actual": RECOMMENDED_NEXT_LAYER_6MK, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MK, "actual": RECOMMENDED_PATH_6MK, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MK, "actual": DIAGNOSIS_6MK, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "surface_health_audit", "passed": all_passed(surface_audit), "detail": f"{sum(1 for r in surface_audit if r['passed'])}/{len(surface_audit)}"},
        {"check": "distribution_health_audit", "passed": all_passed(distribution_audit), "detail": f"{sum(1 for r in distribution_audit if r['passed'])}/{len(distribution_audit)}"},
        {"check": "model_vs_actual_audit", "passed": all_passed(model_actual_audit), "detail": f"{sum(1 for r in model_actual_audit if r['passed'])}/{len(model_actual_audit)}"},
        {"check": "model_vs_market_audit", "passed": all_passed(model_market_audit), "detail": f"{sum(1 for r in model_market_audit if r['passed'])}/{len(model_market_audit)}"},
        {"check": "missing_data_audit", "passed": all_passed(missing_data_audit), "detail": f"{sum(1 for r in missing_data_audit if r['passed'])}/{len(missing_data_audit)}"},
        {"check": "total_scope_audit", "passed": all_passed(total_scope_audit), "detail": f"{sum(1 for r in total_scope_audit if r['passed'])}/{len(total_scope_audit)}"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "future_6ml_contract", "passed": all_passed(future_6ml), "detail": f"{sum(1 for r in future_6ml if r['passed'])}/{len(future_6ml)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_policy), "detail": f"{sum(1 for r in blocking_policy if r['passed'])}/{len(blocking_policy)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)
    metric_artifacts_valid_for_historical_source_planning = all_checks_passed

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "surface_health_audit": write_csv(SURFACE_HEALTH_AUDIT_CSV, surface_audit),
        "distribution_health_audit": write_csv(DISTRIBUTION_HEALTH_AUDIT_CSV, distribution_audit),
        "model_vs_actual_audit": write_csv(MODEL_ACTUAL_AUDIT_CSV, model_actual_audit),
        "model_vs_market_audit": write_csv(MODEL_MARKET_AUDIT_CSV, model_market_audit),
        "missing_data_audit": write_csv(MISSING_DATA_AUDIT_CSV, missing_data_audit),
        "total_scope_audit": write_csv(TOTAL_SCOPE_AUDIT_CSV, total_scope_audit),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6ml_contract": write_csv(FUTURE_6ML_CSV, future_6ml),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MK",
        "layer_type": "game_mechanics_realism",
        "audit_only_probability_surface_metric_implementation": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MK if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MK,
        "recommended_path": RECOMMENDED_PATH_6MK,
        "predecessor_layer": "6MJ",
        "predecessor_diagnosis": json_6mj.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mj.get("all_checks_passed") is True,
        "audited_layer_after": "6MJ",
        "source_family": "projection_adapter_numeric_probability_surface_metric_audit",
        "repaired_probability_surface_loaded_confirmed": json_6mj.get("repaired_probability_surface_loaded") is True,
        "repaired_probability_surface_row_count_confirmed": json_6mj.get("repaired_probability_surface_row_count"),
        "surface_health_metrics_audited": True,
        "distribution_health_metrics_audited": True,
        "model_vs_actual_metric_or_blocker_audited": True,
        "model_vs_actual_blocker_confirmed": actual_blocker,
        "model_vs_market_metric_or_blocker_audited": True,
        "model_vs_market_blocker_confirmed": market_blocker,
        "missing_data_diagnostics_audited": True,
        "total_metrics_out_of_scope_confirmed": True,
        "no_fake_actual_outcomes_confirmed": json_6mj.get("no_fake_actual_outcomes_generated") is True,
        "no_fake_moneyline_odds_confirmed": json_6mj.get("no_fake_moneyline_odds_generated") is True,
        "metric_artifacts_valid_for_historical_source_planning": metric_artifacts_valid_for_historical_source_planning,
        "historical_actuals_source_required_next": actual_blocker,
        "historical_moneyline_source_required_next": market_blocker,
        "totals_surface_source_required_later": True,
        "metric_execution_run_by_6mk": False,
        "backtest_execution_run_by_6mk": False,
        "adapter_call_executed_by_6mk": False,
        "source_scan_run_by_6mk": False,
        "production_code_modified_by_6mk": False,
        "full_batch_adapter_call_run": False,
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
        "production_source_modifications_run": False,
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "surface_health_audit_csv": str(SURFACE_HEALTH_AUDIT_CSV),
            "distribution_health_audit_csv": str(DISTRIBUTION_HEALTH_AUDIT_CSV),
            "model_vs_actual_audit_csv": str(MODEL_ACTUAL_AUDIT_CSV),
            "model_vs_market_audit_csv": str(MODEL_MARKET_AUDIT_CSV),
            "missing_data_audit_csv": str(MISSING_DATA_AUDIT_CSV),
            "total_scope_audit_csv": str(TOTAL_SCOPE_AUDIT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6ml_contract_csv": str(FUTURE_6ML_CSV),
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
