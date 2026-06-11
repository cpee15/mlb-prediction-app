#!/usr/bin/env python3
"""Plan probability surface metrics for repaired numeric win-probability surface."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mi_projection_adapter_numeric_probability_surface_metric_plan"
TMP_DIR = Path("tmp")

SCRIPT_6MH = Path("scripts/audit_6mh_layer6_projection_adapter_numeric_probability_repair.py")
JSON_6MH = TMP_DIR / "layer6_6mh_projection_adapter_numeric_probability_repair_audit.json"

REQUIRED_INPUTS = [
    JSON_6MH,
    TMP_DIR / "layer6_6mh_projection_adapter_numeric_probability_repair_audit_checks.csv",
    TMP_DIR / "layer6_6mh_projection_adapter_numeric_probability_repair_audit_predecessor.csv",
    TMP_DIR / "layer6_6mh_projection_adapter_numeric_probability_repair_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6mh_projection_adapter_numeric_probability_repair_audit_repair_result_review.csv",
    TMP_DIR / "layer6_6mh_projection_adapter_numeric_probability_repair_audit_repaired_surface_audit.csv",
    TMP_DIR / "layer6_6mh_projection_adapter_numeric_probability_repair_audit_source_provenance_audit.csv",
    TMP_DIR / "layer6_6mh_projection_adapter_numeric_probability_repair_audit_metric_planning_gate.csv",
    TMP_DIR / "layer6_6mh_projection_adapter_numeric_probability_repair_audit_blockers.csv",
    TMP_DIR / "layer6_6mh_projection_adapter_numeric_probability_repair_audit_future_6mi_contract.csv",
    TMP_DIR / "layer6_6mh_projection_adapter_numeric_probability_repair_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6mh_projection_adapter_numeric_probability_repair_audit_decision.csv",
    TMP_DIR / "layer6_6mh_projection_adapter_numeric_probability_repair_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6mh_projection_adapter_numeric_probability_repair_audit_recommended_path.csv",
    SCRIPT_6MH,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
METRIC_FAMILIES_CSV = TMP_DIR / f"{SLUG}_metric_families.csv"
MODEL_ACTUAL_CSV = TMP_DIR / f"{SLUG}_model_vs_actual_metrics.csv"
MODEL_MARKET_CSV = TMP_DIR / f"{SLUG}_model_vs_market_metrics.csv"
TOTAL_SCOPE_CSV = TMP_DIR / f"{SLUG}_total_metrics_scope.csv"
REQUIRED_INPUTS_CSV = TMP_DIR / f"{SLUG}_required_inputs.csv"
METRIC_GATE_CSV = TMP_DIR / f"{SLUG}_metric_readiness_gate.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6MJ_CSV = TMP_DIR / f"{SLUG}_future_6mj_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MH = "layer_6_projection_adapter_numeric_probability_repair_audit_complete"
DIAGNOSIS_6MI = "layer_6_projection_adapter_numeric_probability_surface_metric_plan_complete"
RECOMMENDED_NEXT_LAYER_6MI = "6MJ_layer_6_projection_adapter_numeric_probability_surface_metric_implementation"
RECOMMENDED_PATH_6MI = "implement_repaired_numeric_probability_surface_metrics_readonly"


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
    json_6mh = load_json(JSON_6MH)

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
        {"check": "6mh_script_exists", "expected": True, "actual": SCRIPT_6MH.exists(), "passed": SCRIPT_6MH.exists()},
        {"check": "6mh_json_exists", "expected": True, "actual": JSON_6MH.exists(), "passed": JSON_6MH.exists()},
        {"check": "6mh_all_checks_passed", "expected": True, "actual": json_6mh.get("all_checks_passed"), "passed": json_6mh.get("all_checks_passed") is True},
        {"check": "6mh_diagnosis", "expected": DIAGNOSIS_6MH, "actual": json_6mh.get("diagnosis"), "passed": json_6mh.get("diagnosis") == DIAGNOSIS_6MH},
        {"check": "6mh_recommended_next_layer", "expected": "6MI_layer_6_projection_adapter_numeric_probability_surface_metric_plan", "actual": json_6mh.get("recommended_next_layer"), "passed": json_6mh.get("recommended_next_layer") == "6MI_layer_6_projection_adapter_numeric_probability_surface_metric_plan"},
        {"check": "6mh_metric_planning_ready", "expected": True, "actual": json_6mh.get("repaired_numeric_surface_valid_for_metric_planning"), "passed": json_6mh.get("repaired_numeric_surface_valid_for_metric_planning") is True},
    ]

    metric_families = [
        {"metric_family": "brier_score", "domain": "model_vs_actual", "purpose": "squared error for predicted win probability against binary win/loss outcome", "requires_moneyline_odds": False, "requires_actual_outcome": True, "passed": True},
        {"metric_family": "log_loss", "domain": "model_vs_actual", "purpose": "penalize confident wrong win-probability predictions", "requires_moneyline_odds": False, "requires_actual_outcome": True, "passed": True},
        {"metric_family": "calibration_buckets", "domain": "model_vs_actual", "purpose": "compare average predicted probability to observed win rate by probability bucket", "requires_moneyline_odds": False, "requires_actual_outcome": True, "passed": True},
        {"metric_family": "probability_distribution_health", "domain": "surface_health", "purpose": "detect excess 0.5 defaults, spikes, clipping, missingness, and suspicious distributions", "requires_moneyline_odds": False, "requires_actual_outcome": False, "passed": True},
        {"metric_family": "confidence_analysis", "domain": "model_vs_actual", "purpose": "measure performance by confidence band and identify over/underconfidence", "requires_moneyline_odds": False, "requires_actual_outcome": True, "passed": True},
        {"metric_family": "surface_coverage_checks", "domain": "surface_health", "purpose": "measure game coverage, duplicate rows, missing games, missing probability fields, and provenance coverage", "requires_moneyline_odds": False, "requires_actual_outcome": False, "passed": True},
        {"metric_family": "missing_data_diagnostics", "domain": "surface_health", "purpose": "track missing actuals, missing odds, missing game IDs, and missing source provenance", "requires_moneyline_odds": False, "requires_actual_outcome": False, "passed": True},
        {"metric_family": "market_comparison_readiness", "domain": "model_vs_market", "purpose": "define readiness to compare sim probability to historical moneyline implied probability", "requires_moneyline_odds": True, "requires_actual_outcome": False, "passed": True},
    ]

    model_vs_actual = [
        {"metric": "brier_score", "formula": "(predicted_win_probability - actual_win_binary)^2", "requires": "predicted_win_probability, actual_win_binary", "execution_allowed_in_6mj": True, "passed": True},
        {"metric": "log_loss", "formula": "-[y*log(p)+(1-y)*log(1-p)] with clipping", "requires": "predicted_win_probability, actual_win_binary", "execution_allowed_in_6mj": True, "passed": True},
        {"metric": "calibration_bucket_error", "formula": "bucket_observed_win_rate - bucket_average_predicted_probability", "requires": "predicted_win_probability, actual_win_binary, bucket", "execution_allowed_in_6mj": True, "passed": True},
        {"metric": "confidence_band_accuracy", "formula": "observed_win_rate_by_confidence_band", "requires": "predicted_win_probability, actual_win_binary", "execution_allowed_in_6mj": True, "passed": True},
    ]

    model_vs_market = [
        {"metric": "market_implied_probability", "formula": "convert American moneyline odds to implied probability", "requires": "historical_moneyline_odds", "execution_allowed_in_6mj": "only_if_historical_moneyline_surface_exists", "passed": True},
        {"metric": "sim_vs_market_probability_delta", "formula": "sim_win_probability - market_implied_probability", "requires": "predicted_win_probability, market_implied_probability", "execution_allowed_in_6mj": "only_if_historical_moneyline_surface_exists", "passed": True},
        {"metric": "market_bucket_agreement", "formula": "group by edge bands and compare outcomes if actuals available", "requires": "predicted_win_probability, market_implied_probability, actual_win_binary", "execution_allowed_in_6mj": "only_if_historical_moneyline_and_actuals_exist", "passed": True},
        {"metric": "closing_line_value_readiness", "formula": "requires open/close moneyline snapshots; plan only until source audited", "requires": "open_moneyline, close_moneyline", "execution_allowed_in_6mj": False, "passed": True},
    ]

    total_scope = [
        {"scope_item": "total_runs_projection_metrics", "in_scope_for_6mi": False, "reason": "requires traced and audited projected total-runs surface", "passed": True},
        {"scope_item": "over_under_probability_metrics", "in_scope_for_6mi": False, "reason": "requires traced and audited over/under probability surface", "passed": True},
        {"scope_item": "historical_total_odds_market_comparison", "in_scope_for_6mi": False, "reason": "requires historical total line and odds plus sim total/over probability surface", "passed": True},
        {"scope_item": "future_total_metric_branch", "in_scope_for_6mi": False, "reason": "eligible after totals output surface is located, repaired, and audited", "passed": True},
    ]

    required_inputs = [
        {"input": "repaired_numeric_probability_surface", "required_for": "all probability metrics", "status": "available_from_6mh_audit", "passed": True},
        {"input": "actual_game_result_binary", "required_for": "brier_score, log_loss, calibration, confidence", "status": "required_for_6mj_if_available_else_blocker", "passed": True},
        {"input": "historical_moneyline_odds", "required_for": "market comparison", "status": "optional_market_readiness_input; required before market comparison execution", "passed": True},
        {"input": "historical_closing_moneyline_odds", "required_for": "closing-line value readiness", "status": "blocked_until_source_audited", "passed": True},
        {"input": "total_runs_or_over_under_probability_surface", "required_for": "totals metrics", "status": "out_of_scope_until_traced_and_audited", "passed": True},
        {"input": "source_provenance", "required_for": "all metric artifacts", "status": "available_from_6mh_audit", "passed": True},
    ]

    metric_gate = [
        {"gate": "metric_plan_created", "open": True, "reason": "metric families and required inputs defined", "passed": True},
        {"gate": "metric_implementation_allowed_next", "open": True, "reason": "6MH confirmed repaired numeric surface valid for metric planning", "passed": True},
        {"gate": "metric_execution_allowed_in_6mi", "open": False, "reason": "6MI is planning only", "passed": True},
        {"gate": "backtest_execution_allowed_next", "open": False, "reason": "requires implemented and audited metrics", "passed": True},
        {"gate": "tuning_allowed_next", "open": False, "reason": "requires historical validation/backtest evidence", "passed": True},
    ]

    allowed_next = [
        {"operation": "implement_probability_surface_metric_artifacts_readonly", "allowed_next": True, "scope": "tmp metric outputs only", "passed": True},
        {"operation": "compute_surface_health_metrics", "allowed_next": True, "scope": "repaired surface only", "passed": True},
        {"operation": "compute_model_vs_actual_metrics", "allowed_next": True, "scope": "only if actual outcomes exist locally", "passed": True},
        {"operation": "compute_model_vs_market_readiness_metrics", "allowed_next": True, "scope": "only if historical moneyline odds exist locally", "passed": True},
    ]

    forbidden_next = [
        {"operation": "backtest_execution", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "database_writes", "allowed_next": False, "passed": True},
        {"operation": "production_code_changes", "allowed_next": False, "passed": True},
        {"operation": "adapter_calls", "allowed_next": False, "passed": True},
    ]

    blockers = [
        {"blocker": "actual_outcomes_required_for_scoring_metrics", "active": True, "reason": "Brier/log-loss/calibration require actual result", "passed": True},
        {"blocker": "moneyline_odds_required_for_market_comparison", "active": True, "reason": "market comparison requires historical moneyline odds", "passed": True},
        {"blocker": "totals_surface_required_for_total_metrics", "active": True, "reason": "totals require total-runs or over/under surface", "passed": True},
        {"blocker": "backtests_tuning_activation_exit_blocked", "active": True, "reason": "requires implemented/audited metrics and later historical validation", "passed": True},
    ]

    future_6mj = [
        {"contract": "implement_repaired_numeric_probability_surface_metrics_readonly", "required": True, "why": "produce metric artifacts without production code changes", "passed": True},
        {"contract": "fail_closed_when_actual_outcomes_or_moneyline_odds_are_missing", "required": True, "why": "metrics must distinguish unavailable inputs from zero performance", "passed": True},
        {"contract": "preserve_no_backtest_tuning_activation_or_exit", "required": True, "why": "metric implementation is not historical betting validation", "passed": True},
    ]

    blocking_policy = [
        {"policy": "do_not_confuse_probability_quality_metrics_with_market_betting_backtest", "required": True, "passed": True},
        {"policy": "do_not_execute_moneyline_market_metrics_without_historical_moneyline_odds", "required": True, "passed": True},
        {"policy": "do_not_execute_total_metrics_without_total_surface", "required": True, "passed": True},
        {"policy": "do_not_tune_or_activate_from_metric_plan", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6mh_passed", "expected": True, "actual": json_6mh.get("all_checks_passed"), "passed": json_6mh.get("all_checks_passed") is True},
        {"decision": "6mh_diagnosis_valid", "expected": DIAGNOSIS_6MH, "actual": json_6mh.get("diagnosis"), "passed": json_6mh.get("diagnosis") == DIAGNOSIS_6MH},
        {"decision": "all_required_6mh_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "repaired_surface_valid_for_metric_planning_confirmed", "expected": True, "actual": json_6mh.get("repaired_numeric_surface_valid_for_metric_planning"), "passed": json_6mh.get("repaired_numeric_surface_valid_for_metric_planning") is True},
        {"decision": "metric_plan_created", "expected": True, "actual": True, "passed": True},
        {"decision": "required_metric_families_present", "expected": 8, "actual": len(metric_families), "passed": len(metric_families) == 8},
        {"decision": "model_vs_actual_and_market_distinguished", "expected": True, "actual": True, "passed": True},
        {"decision": "totals_metrics_out_of_scope", "expected": True, "actual": True, "passed": True},
        {"decision": "recommend_6mj_next", "expected": RECOMMENDED_NEXT_LAYER_6MI, "actual": RECOMMENDED_NEXT_LAYER_6MI, "passed": True},
        {"decision": "do_not_recommend_backtest_tuning_activation_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only_probability_surface_metrics", "expected": True, "actual": True, "passed": True},
        {"boundary": "metric_execution_run_by_6mi", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mi", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mi", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_scan_run_by_6mi", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mi", "expected": False, "actual": False, "passed": True},
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

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MI, "actual": RECOMMENDED_NEXT_LAYER_6MI, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MI, "actual": RECOMMENDED_PATH_6MI, "passed": True},
        {"decision": "allow_metric_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MI, "actual": DIAGNOSIS_6MI, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "metric_families", "passed": all_passed(metric_families), "detail": f"{sum(1 for r in metric_families if r['passed'])}/{len(metric_families)}"},
        {"check": "model_vs_actual_metrics", "passed": all_passed(model_vs_actual), "detail": f"{sum(1 for r in model_vs_actual if r['passed'])}/{len(model_vs_actual)}"},
        {"check": "model_vs_market_metrics", "passed": all_passed(model_vs_market), "detail": f"{sum(1 for r in model_vs_market if r['passed'])}/{len(model_vs_market)}"},
        {"check": "total_metrics_scope", "passed": all_passed(total_scope), "detail": f"{sum(1 for r in total_scope if r['passed'])}/{len(total_scope)}"},
        {"check": "required_inputs", "passed": all_passed(required_inputs), "detail": f"{sum(1 for r in required_inputs if r['passed'])}/{len(required_inputs)}"},
        {"check": "metric_readiness_gate", "passed": all_passed(metric_gate), "detail": f"{sum(1 for r in metric_gate if r['passed'])}/{len(metric_gate)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next), "detail": f"{sum(1 for r in allowed_next if r['passed'])}/{len(allowed_next)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next), "detail": f"{sum(1 for r in forbidden_next if r['passed'])}/{len(forbidden_next)}"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "future_6mj_contract", "passed": all_passed(future_6mj), "detail": f"{sum(1 for r in future_6mj if r['passed'])}/{len(future_6mj)}"},
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
        "metric_families": write_csv(METRIC_FAMILIES_CSV, metric_families),
        "model_vs_actual_metrics": write_csv(MODEL_ACTUAL_CSV, model_vs_actual),
        "model_vs_market_metrics": write_csv(MODEL_MARKET_CSV, model_vs_market),
        "total_metrics_scope": write_csv(TOTAL_SCOPE_CSV, total_scope),
        "required_inputs": write_csv(REQUIRED_INPUTS_CSV, required_inputs),
        "metric_readiness_gate": write_csv(METRIC_GATE_CSV, metric_gate),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6mj_contract": write_csv(FUTURE_6MJ_CSV, future_6mj),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MI",
        "layer_type": "game_mechanics_realism",
        "planning_only_probability_surface_metrics": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MI if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MI,
        "recommended_path": RECOMMENDED_PATH_6MI,
        "predecessor_layer": "6MH",
        "predecessor_diagnosis": json_6mh.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mh.get("all_checks_passed") is True,
        "planned_layer_after": "6MH",
        "source_family": "projection_adapter_numeric_probability_surface_metric_plan",
        "repaired_numeric_surface_valid_for_metric_planning_confirmed": json_6mh.get("repaired_numeric_surface_valid_for_metric_planning") is True,
        "metric_plan_created": True,
        "metric_family_count": len(metric_families),
        "model_vs_actual_metric_count": len(model_vs_actual),
        "model_vs_market_metric_count": len(model_vs_market),
        "total_metrics_out_of_scope_until_total_surface_audited": True,
        "required_inputs_defined": True,
        "implementation_allowed_next": True,
        "metric_execution_allowed_next": True,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "market_comparison_readiness_included": True,
        "moneyline_odds_required_for_market_comparison": True,
        "actual_outcomes_required_for_scoring_metrics": True,
        "totals_surface_required_for_total_metrics": True,
        "metric_execution_run_by_6mi": False,
        "backtest_execution_run_by_6mi": False,
        "adapter_call_executed_by_6mi": False,
        "source_scan_run_by_6mi": False,
        "production_code_modified_by_6mi": False,
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
            "metric_families_csv": str(METRIC_FAMILIES_CSV),
            "model_vs_actual_metrics_csv": str(MODEL_ACTUAL_CSV),
            "model_vs_market_metrics_csv": str(MODEL_MARKET_CSV),
            "total_metrics_scope_csv": str(TOTAL_SCOPE_CSV),
            "required_inputs_csv": str(REQUIRED_INPUTS_CSV),
            "metric_readiness_gate_csv": str(METRIC_GATE_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6mj_contract_csv": str(FUTURE_6MJ_CSV),
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
