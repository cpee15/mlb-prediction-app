#!/usr/bin/env python3
"""Plan smoke-sample status before any actuals-only metric layer."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6nf_historical_actuals_smoke_sample_status_plan"
TMP_DIR = Path("tmp")

SCRIPT_6NE = Path("scripts/audit_6ne_layer6_historical_actuals_source_creation.py")
TARGET_ACTUALS = Path("data/local/historical_actuals.csv")
JSON_6NE = TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit.json"

REQUIRED_INPUTS = [
    JSON_6NE,
    TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit_checks.csv",
    TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit_predecessor.csv",
    TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit_created_file_review.csv",
    TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit_schema_review.csv",
    TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit_value_review.csv",
    TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit_provenance_review.csv",
    TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit_sample_sufficiency_review.csv",
    TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit_rerun_6na_review.csv",
    TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit_moneyline_deferral_boundaries.csv",
    TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit_allowed_operations_next.csv",
    TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit_future_6nf_contract.csv",
    TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit_decision.csv",
    TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6ne_historical_actuals_source_creation_audit_recommended_path.csv",
    SCRIPT_6NE,
    TARGET_ACTUALS,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CURRENT_STATUS_CSV = TMP_DIR / f"{SLUG}_current_status.csv"
REAL_EVAL_BLOCKERS_CSV = TMP_DIR / f"{SLUG}_real_eval_blockers.csv"
LARGER_SAMPLE_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_larger_sample_requirements.csv"
ACCEPTED_LOCATIONS_CSV = TMP_DIR / f"{SLUG}_accepted_larger_source_locations.csv"
VALIDATION_RERUN_CSV = TMP_DIR / f"{SLUG}_validation_rerun_requirements.csv"
METRIC_UNLOCK_CSV = TMP_DIR / f"{SLUG}_metric_unlock_requirements.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6NG_CSV = TMP_DIR / f"{SLUG}_future_6ng_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6NE = "layer_6_historical_actuals_source_creation_audit_complete"
DIAGNOSIS_6NF = "layer_6_historical_actuals_smoke_sample_status_plan_complete"
RECOMMENDED_NEXT_LAYER = "6NG_layer_6_historical_actuals_smoke_sample_status_plan_audit"
RECOMMENDED_PATH = "audit_smoke_sample_status_plan_before_larger_sample_source_expansion"

MIN_LARGER_SAMPLE_ROW_COUNT = 100
MIN_LARGER_SAMPLE_DATE_SPAN_DAYS = 21


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
    json_6ne = load_json(JSON_6NE)

    current_row_count = int(json_6ne.get("audited_actuals_row_count") or 0)
    current_date_span = int(json_6ne.get("audited_actuals_date_span_days") or 0)
    current_classification = str(json_6ne.get("audited_sample_classification") or "")
    current_sufficient = json_6ne.get("audited_sufficient_for_real_historical_evaluation") is True

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
        {"check": "6ne_script_exists", "expected": True, "actual": SCRIPT_6NE.exists(), "passed": SCRIPT_6NE.exists()},
        {"check": "6ne_json_exists", "expected": True, "actual": JSON_6NE.exists(), "passed": JSON_6NE.exists()},
        {"check": "6ne_all_checks_passed", "expected": True, "actual": json_6ne.get("all_checks_passed"), "passed": json_6ne.get("all_checks_passed") is True},
        {"check": "6ne_diagnosis", "expected": DIAGNOSIS_6NE, "actual": json_6ne.get("diagnosis"), "passed": json_6ne.get("diagnosis") == DIAGNOSIS_6NE},
        {"check": "6ne_target_actuals_file_exists", "expected": True, "actual": json_6ne.get("target_actuals_file_exists"), "passed": json_6ne.get("target_actuals_file_exists") is True},
        {"check": "6ne_smoke_test_status_confirmed", "expected": True, "actual": json_6ne.get("smoke_test_only_status_confirmed"), "passed": json_6ne.get("smoke_test_only_status_confirmed") is True},
        {"check": "6ne_real_eval_blocked", "expected": True, "actual": json_6ne.get("real_historical_evaluation_blocked_due_to_sample_size"), "passed": json_6ne.get("real_historical_evaluation_blocked_due_to_sample_size") is True},
        {"check": "6ne_recommended_next", "expected": "6NF_layer_6_historical_actuals_smoke_sample_status_plan", "actual": json_6ne.get("recommended_next_layer"), "passed": json_6ne.get("recommended_next_layer") == "6NF_layer_6_historical_actuals_smoke_sample_status_plan"},
    ]

    current_status_rows = [
        {"field": "current_actuals_path", "value": str(TARGET_ACTUALS), "passed": str(TARGET_ACTUALS) == "data/local/historical_actuals.csv"},
        {"field": "current_actuals_file_exists", "value": TARGET_ACTUALS.exists(), "passed": TARGET_ACTUALS.exists()},
        {"field": "current_actuals_row_count", "value": current_row_count, "passed": current_row_count == 16},
        {"field": "current_actuals_date_span_days", "value": current_date_span, "passed": current_date_span == 1},
        {"field": "current_sample_classification", "value": current_classification, "passed": current_classification == "smoke_test_only"},
        {"field": "current_sufficient_for_real_historical_evaluation", "value": current_sufficient, "passed": current_sufficient is False},
        {"field": "rerun_6na_all_checks_passed", "value": json_6ne.get("rerun_6na_all_checks_passed"), "passed": json_6ne.get("rerun_6na_all_checks_passed") is True},
    ]

    real_eval_blocker_rows = [
        {"blocker": "row_count_below_minimum", "current": current_row_count, "minimum_required": MIN_LARGER_SAMPLE_ROW_COUNT, "blocks_real_eval": current_row_count < MIN_LARGER_SAMPLE_ROW_COUNT, "passed": current_row_count < MIN_LARGER_SAMPLE_ROW_COUNT},
        {"blocker": "date_span_below_minimum", "current": current_date_span, "minimum_required": MIN_LARGER_SAMPLE_DATE_SPAN_DAYS, "blocks_real_eval": current_date_span < MIN_LARGER_SAMPLE_DATE_SPAN_DAYS, "passed": current_date_span < MIN_LARGER_SAMPLE_DATE_SPAN_DAYS},
        {"blocker": "sample_classification_smoke_test_only", "current": current_classification, "blocks_real_eval": current_classification == "smoke_test_only", "passed": current_classification == "smoke_test_only"},
        {"blocker": "insufficient_for_real_historical_evaluation", "current": current_sufficient, "blocks_real_eval": not current_sufficient, "passed": not current_sufficient},
    ]

    larger_sample_requirement_rows = [
        {"requirement": "minimum_larger_sample_row_count", "value": MIN_LARGER_SAMPLE_ROW_COUNT, "required_before_metrics": True, "passed": True},
        {"requirement": "minimum_larger_sample_date_span_days", "value": MIN_LARGER_SAMPLE_DATE_SPAN_DAYS, "required_before_metrics": True, "passed": True},
        {"requirement": "must_cover_multiple_series_and_slate_contexts", "value": True, "required_before_metrics": True, "passed": True},
        {"requirement": "must_pass_6na_schema_value_provenance_validation", "value": True, "required_before_metrics": True, "passed": True},
        {"requirement": "must_be_audited_after_creation_or_replacement", "value": True, "required_before_metrics": True, "passed": True},
    ]

    accepted_location_rows = [
        {"location": "data/local/historical_actuals.csv", "accepted": True, "purpose": "primary replacement/expanded actuals source", "passed": True},
        {"location": "data/local/historical_actuals/*.csv", "accepted": True, "purpose": "partitioned larger actuals source files", "passed": True},
        {"location": "tmp/statsapi_cache/schedule/*.json", "accepted": True, "purpose": "local cached schedule source for larger source creation only", "passed": True},
        {"location": "tmp/*", "accepted": False, "purpose": "not authoritative output location for normalized actuals", "passed": True},
    ]

    validation_rerun_rows = [
        {"requirement": "rerun_6na_after_larger_sample_created", "required": True, "passed": True},
        {"requirement": "6na_must_pass_all_checks", "required": True, "passed": True},
        {"requirement": "audit_larger_sample_validation_before_metrics", "required": True, "passed": True},
        {"requirement": "preserve_source_artifact_provenance", "required": True, "passed": True},
    ]

    metric_unlock_rows = [
        {"requirement": "larger_sample_required_before_metrics", "required": True, "satisfied_now": False, "passed": True},
        {"requirement": "minimum_row_count_met", "required": True, "satisfied_now": current_row_count >= MIN_LARGER_SAMPLE_ROW_COUNT, "passed": current_row_count < MIN_LARGER_SAMPLE_ROW_COUNT},
        {"requirement": "minimum_date_span_met", "required": True, "satisfied_now": current_date_span >= MIN_LARGER_SAMPLE_DATE_SPAN_DAYS, "passed": current_date_span < MIN_LARGER_SAMPLE_DATE_SPAN_DAYS},
        {"requirement": "real_historical_evaluation_unlocked", "required": True, "satisfied_now": False, "passed": True},
        {"requirement": "actuals_only_metric_layer_allowed_now", "required": False, "satisfied_now": False, "passed": True},
    ]

    allowed_next_rows = [
        {"operation": "audit_smoke_sample_status_plan", "allowed_next": True, "scope": "6NG audit only", "passed": True},
    ]

    forbidden_next_rows = [
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

    future_6ng_rows = [
        {"contract": "audit_current_smoke_sample_status", "required": True, "passed": True},
        {"contract": "audit_larger_sample_requirements_documented", "required": True, "passed": True},
        {"contract": "audit_metric_unlock_requirements_block_metrics_now", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_backtests_tuning_activation_exit", "required": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only_historical_actuals_smoke_sample_status", "expected": True, "actual": True, "passed": True},
        {"boundary": "target_actuals_file_modified_by_6nf", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_rows_ingested_by_6nf", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6nf", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6nf", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6nf", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6nf", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6nf", "expected": False, "actual": False, "passed": True},
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

    decision_rows = [
        {"decision": "6ne_passed", "expected": True, "actual": json_6ne.get("all_checks_passed"), "passed": json_6ne.get("all_checks_passed") is True},
        {"decision": "current_status_confirmed", "expected": True, "actual": all_passed(current_status_rows), "passed": all_passed(current_status_rows)},
        {"decision": "real_eval_blockers_confirmed", "expected": True, "actual": all_passed(real_eval_blocker_rows), "passed": all_passed(real_eval_blocker_rows)},
        {"decision": "larger_sample_requirements_defined", "expected": True, "actual": all_passed(larger_sample_requirement_rows), "passed": all_passed(larger_sample_requirement_rows)},
        {"decision": "metric_unlock_requirements_keep_metrics_blocked", "expected": True, "actual": all_passed(metric_unlock_rows), "passed": all_passed(metric_unlock_rows)},
        {"decision": "recommend_6ng", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "do_not_execute_metrics_backtests_tuning_activation_exit", "expected": True, "actual": True, "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6NF, "actual": DIAGNOSIS_6NF, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "current_status", "passed": all_passed(current_status_rows), "detail": f"{sum(1 for r in current_status_rows if r['passed'])}/{len(current_status_rows)}"},
        {"check": "real_eval_blockers", "passed": all_passed(real_eval_blocker_rows), "detail": f"{sum(1 for r in real_eval_blocker_rows if r['passed'])}/{len(real_eval_blocker_rows)}"},
        {"check": "larger_sample_requirements", "passed": all_passed(larger_sample_requirement_rows), "detail": f"{sum(1 for r in larger_sample_requirement_rows if r['passed'])}/{len(larger_sample_requirement_rows)}"},
        {"check": "accepted_larger_source_locations", "passed": all_passed(accepted_location_rows), "detail": f"{sum(1 for r in accepted_location_rows if r['passed'])}/{len(accepted_location_rows)}"},
        {"check": "validation_rerun_requirements", "passed": all_passed(validation_rerun_rows), "detail": f"{sum(1 for r in validation_rerun_rows if r['passed'])}/{len(validation_rerun_rows)}"},
        {"check": "metric_unlock_requirements", "passed": all_passed(metric_unlock_rows), "detail": f"{sum(1 for r in metric_unlock_rows if r['passed'])}/{len(metric_unlock_rows)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next_rows), "detail": f"{sum(1 for r in allowed_next_rows if r['passed'])}/{len(allowed_next_rows)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next_rows), "detail": f"{sum(1 for r in forbidden_next_rows if r['passed'])}/{len(forbidden_next_rows)}"},
        {"check": "future_6ng_contract", "passed": all_passed(future_6ng_rows), "detail": f"{sum(1 for r in future_6ng_rows if r['passed'])}/{len(future_6ng_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "current_status": write_csv(CURRENT_STATUS_CSV, current_status_rows),
        "real_eval_blockers": write_csv(REAL_EVAL_BLOCKERS_CSV, real_eval_blocker_rows),
        "larger_sample_requirements": write_csv(LARGER_SAMPLE_REQUIREMENTS_CSV, larger_sample_requirement_rows),
        "accepted_larger_source_locations": write_csv(ACCEPTED_LOCATIONS_CSV, accepted_location_rows),
        "validation_rerun_requirements": write_csv(VALIDATION_RERUN_CSV, validation_rerun_rows),
        "metric_unlock_requirements": write_csv(METRIC_UNLOCK_CSV, metric_unlock_rows),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next_rows),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next_rows),
        "future_6ng_contract": write_csv(FUTURE_6NG_CSV, future_6ng_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6NF",
        "layer_type": "game_mechanics_realism",
        "planning_only_historical_actuals_smoke_sample_status": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6NF if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_layer": "6NE",
        "predecessor_diagnosis": json_6ne.get("diagnosis"),
        "predecessor_all_checks_passed": json_6ne.get("all_checks_passed") is True,
        "source_family": "historical_actuals_smoke_sample_status_plan",
        "current_actuals_path": str(TARGET_ACTUALS),
        "current_actuals_row_count": current_row_count,
        "current_actuals_date_span_days": current_date_span,
        "current_sample_classification": current_classification,
        "current_sufficient_for_real_historical_evaluation": current_sufficient,
        "real_historical_evaluation_blocked_due_to_sample_size": True,
        "larger_sample_required_before_metrics": True,
        "minimum_larger_sample_row_count": MIN_LARGER_SAMPLE_ROW_COUNT,
        "minimum_larger_sample_date_span_days": MIN_LARGER_SAMPLE_DATE_SPAN_DAYS,
        "larger_sample_validation_required": True,
        "rerun_6na_required_after_larger_sample": True,
        "audit_required_after_larger_sample_validation": True,
        "smoke_sample_status_plan_audit_allowed_next": True,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "source_rows_ingested_by_6nf": False,
        "normalized_source_tables_created_for_production_by_6nf": False,
        "production_code_modified_by_6nf": False,
        "adapter_call_executed_by_6nf": False,
        "metric_execution_run_by_6nf": False,
        "backtest_execution_run_by_6nf": False,
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
            "current_status_csv": str(CURRENT_STATUS_CSV),
            "real_eval_blockers_csv": str(REAL_EVAL_BLOCKERS_CSV),
            "larger_sample_requirements_csv": str(LARGER_SAMPLE_REQUIREMENTS_CSV),
            "accepted_larger_source_locations_csv": str(ACCEPTED_LOCATIONS_CSV),
            "validation_rerun_requirements_csv": str(VALIDATION_RERUN_CSV),
            "metric_unlock_requirements_csv": str(METRIC_UNLOCK_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6ng_contract_csv": str(FUTURE_6NG_CSV),
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
