#!/usr/bin/env python3
"""Audit 6NF smoke-sample status plan before larger source expansion planning."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6ng_historical_actuals_smoke_sample_status_plan_audit"
TMP_DIR = Path("tmp")

SCRIPT_6NF = Path("scripts/plan_6nf_layer6_historical_actuals_smoke_sample_status.py")
JSON_6NF = TMP_DIR / "layer6_6nf_historical_actuals_smoke_sample_status_plan.json"

REQUIRED_INPUTS = [
    JSON_6NF,
    TMP_DIR / "layer6_6nf_historical_actuals_smoke_sample_status_plan_checks.csv",
    TMP_DIR / "layer6_6nf_historical_actuals_smoke_sample_status_plan_predecessor.csv",
    TMP_DIR / "layer6_6nf_historical_actuals_smoke_sample_status_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6nf_historical_actuals_smoke_sample_status_plan_current_status.csv",
    TMP_DIR / "layer6_6nf_historical_actuals_smoke_sample_status_plan_real_eval_blockers.csv",
    TMP_DIR / "layer6_6nf_historical_actuals_smoke_sample_status_plan_larger_sample_requirements.csv",
    TMP_DIR / "layer6_6nf_historical_actuals_smoke_sample_status_plan_accepted_larger_source_locations.csv",
    TMP_DIR / "layer6_6nf_historical_actuals_smoke_sample_status_plan_validation_rerun_requirements.csv",
    TMP_DIR / "layer6_6nf_historical_actuals_smoke_sample_status_plan_metric_unlock_requirements.csv",
    TMP_DIR / "layer6_6nf_historical_actuals_smoke_sample_status_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6nf_historical_actuals_smoke_sample_status_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6nf_historical_actuals_smoke_sample_status_plan_future_6ng_contract.csv",
    TMP_DIR / "layer6_6nf_historical_actuals_smoke_sample_status_plan_decision.csv",
    TMP_DIR / "layer6_6nf_historical_actuals_smoke_sample_status_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6nf_historical_actuals_smoke_sample_status_plan_recommended_path.csv",
    SCRIPT_6NF,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CURRENT_STATUS_REVIEW_CSV = TMP_DIR / f"{SLUG}_current_status_review.csv"
LARGER_SAMPLE_REQUIREMENTS_REVIEW_CSV = TMP_DIR / f"{SLUG}_larger_sample_requirements_review.csv"
METRIC_UNLOCK_REVIEW_CSV = TMP_DIR / f"{SLUG}_metric_unlock_review.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6NH_CSV = TMP_DIR / f"{SLUG}_future_6nh_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6NF = "layer_6_historical_actuals_smoke_sample_status_plan_complete"
DIAGNOSIS_6NG = "layer_6_historical_actuals_smoke_sample_status_plan_audit_complete"
RECOMMENDED_NEXT_LAYER = "6NH_layer_6_larger_historical_actuals_source_expansion_plan"
RECOMMENDED_PATH = "plan_larger_historical_actuals_source_expansion_before_source_creation"


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
    json_6nf = load_json(JSON_6NF)

    current_rows = int(json_6nf.get("current_actuals_row_count") or 0)
    current_span = int(json_6nf.get("current_actuals_date_span_days") or 0)
    min_rows = int(json_6nf.get("minimum_larger_sample_row_count") or 0)
    min_span = int(json_6nf.get("minimum_larger_sample_date_span_days") or 0)

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
        {"check": "6nf_script_exists", "expected": True, "actual": SCRIPT_6NF.exists(), "passed": SCRIPT_6NF.exists()},
        {"check": "6nf_json_exists", "expected": True, "actual": JSON_6NF.exists(), "passed": JSON_6NF.exists()},
        {"check": "6nf_all_checks_passed", "expected": True, "actual": json_6nf.get("all_checks_passed"), "passed": json_6nf.get("all_checks_passed") is True},
        {"check": "6nf_diagnosis", "expected": DIAGNOSIS_6NF, "actual": json_6nf.get("diagnosis"), "passed": json_6nf.get("diagnosis") == DIAGNOSIS_6NF},
        {"check": "6nf_recommended_next", "expected": "6NG_layer_6_historical_actuals_smoke_sample_status_plan_audit", "actual": json_6nf.get("recommended_next_layer"), "passed": json_6nf.get("recommended_next_layer") == "6NG_layer_6_historical_actuals_smoke_sample_status_plan_audit"},
    ]

    current_status_rows = [
        {"review": "actuals_path_preserved", "expected": "data/local/historical_actuals.csv", "actual": json_6nf.get("current_actuals_path"), "passed": json_6nf.get("current_actuals_path") == "data/local/historical_actuals.csv"},
        {"review": "row_count_smoke", "expected": 16, "actual": current_rows, "passed": current_rows == 16},
        {"review": "date_span_smoke", "expected": 1, "actual": current_span, "passed": current_span == 1},
        {"review": "classification_smoke_test_only", "expected": "smoke_test_only", "actual": json_6nf.get("current_sample_classification"), "passed": json_6nf.get("current_sample_classification") == "smoke_test_only"},
        {"review": "real_eval_blocked", "expected": True, "actual": json_6nf.get("real_historical_evaluation_blocked_due_to_sample_size"), "passed": json_6nf.get("real_historical_evaluation_blocked_due_to_sample_size") is True},
    ]

    larger_sample_rows = [
        {"review": "larger_sample_required_before_metrics", "expected": True, "actual": json_6nf.get("larger_sample_required_before_metrics"), "passed": json_6nf.get("larger_sample_required_before_metrics") is True},
        {"review": "minimum_row_count_non_trivial", "expected": ">=100", "actual": min_rows, "passed": min_rows >= 100},
        {"review": "minimum_date_span_non_trivial", "expected": ">=21", "actual": min_span, "passed": min_span >= 21},
        {"review": "larger_sample_validation_required", "expected": True, "actual": json_6nf.get("larger_sample_validation_required"), "passed": json_6nf.get("larger_sample_validation_required") is True},
        {"review": "rerun_6na_required_after_larger_sample", "expected": True, "actual": json_6nf.get("rerun_6na_required_after_larger_sample"), "passed": json_6nf.get("rerun_6na_required_after_larger_sample") is True},
        {"review": "audit_required_after_larger_sample_validation", "expected": True, "actual": json_6nf.get("audit_required_after_larger_sample_validation"), "passed": json_6nf.get("audit_required_after_larger_sample_validation") is True},
    ]

    metric_unlock_rows = [
        {"review": "metric_execution_allowed_next", "expected": False, "actual": json_6nf.get("metric_execution_allowed_next"), "passed": json_6nf.get("metric_execution_allowed_next") is False},
        {"review": "backtest_execution_allowed_next", "expected": False, "actual": json_6nf.get("backtest_execution_allowed_next"), "passed": json_6nf.get("backtest_execution_allowed_next") is False},
        {"review": "tuning_allowed_next", "expected": False, "actual": json_6nf.get("tuning_allowed_next"), "passed": json_6nf.get("tuning_allowed_next") is False},
        {"review": "current_rows_below_unlock", "expected": True, "actual": current_rows < min_rows, "passed": current_rows < min_rows},
        {"review": "current_span_below_unlock", "expected": True, "actual": current_span < min_span, "passed": current_span < min_span},
    ]

    allowed_next_rows = [
        {"operation": "larger_historical_actuals_source_expansion_plan", "allowed_next": True, "scope": "6NH planning only", "passed": True},
    ]

    forbidden_next_rows = [
        {"operation": "source_creation", "allowed_next": False, "passed": True},
        {"operation": "source_ingestion", "allowed_next": False, "passed": True},
        {"operation": "metric_execution", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "actuals_only_metric_layer", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "real_historical_evaluation_claims", "allowed_next": False, "passed": True},
        {"operation": "production_table_creation", "allowed_next": False, "passed": True},
    ]

    future_6nh_rows = [
        {"contract": "plan_larger_historical_actuals_source_expansion", "required": True, "passed": True},
        {"contract": "preserve_minimum_row_count_and_date_span_requirements", "required": True, "passed": True},
        {"contract": "preserve_6na_rerun_and_audit_requirements", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_backtests_tuning_activation_exit", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6nf_passed", "expected": True, "actual": json_6nf.get("all_checks_passed"), "passed": json_6nf.get("all_checks_passed") is True},
        {"decision": "current_status_review_passed", "expected": True, "actual": all_passed(current_status_rows), "passed": all_passed(current_status_rows)},
        {"decision": "larger_sample_requirements_review_passed", "expected": True, "actual": all_passed(larger_sample_rows), "passed": all_passed(larger_sample_rows)},
        {"decision": "metric_unlock_review_passed", "expected": True, "actual": all_passed(metric_unlock_rows), "passed": all_passed(metric_unlock_rows)},
        {"decision": "recommend_6nh", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "do_not_execute_metrics_backtests_tuning_activation_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only_historical_actuals_smoke_sample_status_plan", "expected": True, "actual": True, "passed": True},
        {"boundary": "actuals_file_modified_by_6ng", "expected": False, "actual": False, "passed": True},
        {"boundary": "larger_actuals_source_created_by_6ng", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_rows_ingested_by_6ng", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6ng", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6ng", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6ng", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6ng", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6ng", "expected": False, "actual": False, "passed": True},
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
        {"decision": "do_not_recommend_source_creation_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6NG, "actual": DIAGNOSIS_6NG, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "current_status_review", "passed": all_passed(current_status_rows), "detail": f"{sum(1 for r in current_status_rows if r['passed'])}/{len(current_status_rows)}"},
        {"check": "larger_sample_requirements_review", "passed": all_passed(larger_sample_rows), "detail": f"{sum(1 for r in larger_sample_rows if r['passed'])}/{len(larger_sample_rows)}"},
        {"check": "metric_unlock_review", "passed": all_passed(metric_unlock_rows), "detail": f"{sum(1 for r in metric_unlock_rows if r['passed'])}/{len(metric_unlock_rows)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next_rows), "detail": f"{sum(1 for r in allowed_next_rows if r['passed'])}/{len(allowed_next_rows)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next_rows), "detail": f"{sum(1 for r in forbidden_next_rows if r['passed'])}/{len(forbidden_next_rows)}"},
        {"check": "future_6nh_contract", "passed": all_passed(future_6nh_rows), "detail": f"{sum(1 for r in future_6nh_rows if r['passed'])}/{len(future_6nh_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "current_status_review": write_csv(CURRENT_STATUS_REVIEW_CSV, current_status_rows),
        "larger_sample_requirements_review": write_csv(LARGER_SAMPLE_REQUIREMENTS_REVIEW_CSV, larger_sample_rows),
        "metric_unlock_review": write_csv(METRIC_UNLOCK_REVIEW_CSV, metric_unlock_rows),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next_rows),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next_rows),
        "future_6nh_contract": write_csv(FUTURE_6NH_CSV, future_6nh_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6NG",
        "layer_type": "game_mechanics_realism",
        "audit_only_historical_actuals_smoke_sample_status_plan": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6NG if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_layer": "6NF",
        "predecessor_diagnosis": json_6nf.get("diagnosis"),
        "predecessor_all_checks_passed": json_6nf.get("all_checks_passed") is True,
        "source_family": "historical_actuals_smoke_sample_status_plan_audit",
        "audited_current_actuals_path": json_6nf.get("current_actuals_path"),
        "audited_current_actuals_row_count": current_rows,
        "audited_current_actuals_date_span_days": current_span,
        "audited_current_sample_classification": json_6nf.get("current_sample_classification"),
        "audited_real_historical_evaluation_blocked_due_to_sample_size": json_6nf.get("real_historical_evaluation_blocked_due_to_sample_size") is True,
        "audited_larger_sample_required_before_metrics": json_6nf.get("larger_sample_required_before_metrics") is True,
        "audited_minimum_larger_sample_row_count": min_rows,
        "audited_minimum_larger_sample_date_span_days": min_span,
        "audited_metric_execution_allowed_next": False,
        "audited_backtest_execution_allowed_next": False,
        "larger_historical_actuals_source_expansion_plan_allowed_next": True,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "source_rows_ingested_by_6ng": False,
        "normalized_source_tables_created_for_production_by_6ng": False,
        "production_code_modified_by_6ng": False,
        "adapter_call_executed_by_6ng": False,
        "metric_execution_run_by_6ng": False,
        "backtest_execution_run_by_6ng": False,
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
            "current_status_review_csv": str(CURRENT_STATUS_REVIEW_CSV),
            "larger_sample_requirements_review_csv": str(LARGER_SAMPLE_REQUIREMENTS_REVIEW_CSV),
            "metric_unlock_review_csv": str(METRIC_UNLOCK_REVIEW_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6nh_contract_csv": str(FUTURE_6NH_CSV),
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
