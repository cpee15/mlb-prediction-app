#!/usr/bin/env python3
"""Audit larger historical actuals source expansion before metric unlock planning."""

from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6nj_larger_historical_actuals_source_expansion_audit"
TMP_DIR = Path("tmp")

SCRIPT_6NI = Path("scripts/implement_6ni_layer6_larger_historical_actuals_source_expansion.py")
JSON_6NI = TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation.json"
TARGET_ACTUALS = Path("data/local/historical_actuals.csv")

CANONICAL_SCHEMA = [
    "game_pk",
    "game_date",
    "home_team",
    "away_team",
    "home_score",
    "away_score",
    "home_win_binary",
    "source_artifact",
]
MIN_ROWS = 100
MIN_DATE_SPAN_DAYS = 21

REQUIRED_INPUTS = [
    JSON_6NI,
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_checks.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_predecessor.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_source_inventory.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_selected_sources.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_normalized_sample.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_output_summary.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_schema_value_review.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_provenance_review.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_sample_sufficiency.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_rerun_6na_summary.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_metric_unlock_boundaries.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_allowed_operations_next.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_future_6nj_contract.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_decision.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6ni_larger_historical_actuals_source_expansion_implementation_recommended_path.csv",
    SCRIPT_6NI,
    TARGET_ACTUALS,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
ACTUALS_FILE_REVIEW_CSV = TMP_DIR / f"{SLUG}_actuals_file_review.csv"
SCHEMA_VALUE_REVIEW_CSV = TMP_DIR / f"{SLUG}_schema_value_review.csv"
PROVENANCE_REVIEW_CSV = TMP_DIR / f"{SLUG}_provenance_review.csv"
SAMPLE_SUFFICIENCY_REVIEW_CSV = TMP_DIR / f"{SLUG}_sample_sufficiency_review.csv"
RERUN_6NA_REVIEW_CSV = TMP_DIR / f"{SLUG}_rerun_6na_review.csv"
METRIC_UNLOCK_CSV = TMP_DIR / f"{SLUG}_metric_unlock_boundaries.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6NK_CSV = TMP_DIR / f"{SLUG}_future_6nk_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6NI = "layer_6_larger_historical_actuals_source_expansion_implementation_complete"
DIAGNOSIS_6NJ = "layer_6_larger_historical_actuals_source_expansion_audit_complete"
RECOMMENDED_NEXT_LAYER = "6NK_layer_6_actuals_only_metric_unlock_gate_plan"
RECOMMENDED_PATH = "plan_actuals_only_metric_unlock_gates_before_metric_execution"


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


def parse_date(text: Any) -> str | None:
    try:
        return date.fromisoformat(str(text).strip()[:10]).isoformat()
    except Exception:
        return None


def int_or_none(value: Any) -> int | None:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def date_span_days(rows: list[dict[str, str]]) -> int:
    dates = sorted({str(row.get("game_date", "")) for row in rows if parse_date(row.get("game_date"))})
    if len(dates) < 2:
        return len(dates)
    return (date.fromisoformat(dates[-1]) - date.fromisoformat(dates[0])).days + 1


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()
    json_6ni = load_json(JSON_6NI)
    actuals_rows = read_csv_rows(TARGET_ACTUALS)
    actuals_header = []
    if TARGET_ACTUALS.exists():
        with TARGET_ACTUALS.open(newline="", encoding="utf-8", errors="ignore") as handle:
            reader = csv.reader(handle)
            actuals_header = next(reader, [])

    row_count = len(actuals_rows)
    date_span = date_span_days(actuals_rows)
    game_pks = [str(row.get("game_pk", "")).strip() for row in actuals_rows]
    selected_source_family = str(json_6ni.get("selected_source_family") or "")
    selected_source_count = int(json_6ni.get("selected_source_count") or 0)

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
        {"check": "6ni_script_exists", "expected": True, "actual": SCRIPT_6NI.exists(), "passed": SCRIPT_6NI.exists()},
        {"check": "6ni_json_exists", "expected": True, "actual": JSON_6NI.exists(), "passed": JSON_6NI.exists()},
        {"check": "6ni_all_checks_passed", "expected": True, "actual": json_6ni.get("all_checks_passed"), "passed": json_6ni.get("all_checks_passed") is True},
        {"check": "6ni_diagnosis", "expected": DIAGNOSIS_6NI, "actual": json_6ni.get("diagnosis"), "passed": json_6ni.get("diagnosis") == DIAGNOSIS_6NI},
        {"check": "6ni_recommended_next", "expected": "6NJ_layer_6_larger_historical_actuals_source_expansion_audit", "actual": json_6ni.get("recommended_next_layer"), "passed": json_6ni.get("recommended_next_layer") == "6NJ_layer_6_larger_historical_actuals_source_expansion_audit"},
    ]

    actuals_file_rows = [
        {"check": "target_actuals_exists", "expected": True, "actual": TARGET_ACTUALS.exists(), "passed": TARGET_ACTUALS.exists()},
        {"check": "target_actuals_path", "expected": "data/local/historical_actuals.csv", "actual": str(TARGET_ACTUALS), "passed": str(TARGET_ACTUALS) == "data/local/historical_actuals.csv"},
        {"check": "header_exact", "expected": "|".join(CANONICAL_SCHEMA), "actual": "|".join(actuals_header), "passed": actuals_header == CANONICAL_SCHEMA},
        {"check": "row_count_matches_6ni", "expected": json_6ni.get("output_row_count"), "actual": row_count, "passed": row_count == json_6ni.get("output_row_count")},
        {"check": "date_span_matches_6ni", "expected": json_6ni.get("output_date_span_days"), "actual": date_span, "passed": date_span == json_6ni.get("output_date_span_days")},
    ]

    schema_value_rows = [
        {"check": "row_count_minimum", "expected": MIN_ROWS, "actual": row_count, "passed": row_count >= MIN_ROWS},
        {"check": "date_span_minimum", "expected": MIN_DATE_SPAN_DAYS, "actual": date_span, "passed": date_span >= MIN_DATE_SPAN_DAYS},
        {"check": "unique_game_pk", "expected": row_count, "actual": len(set(game_pks)), "passed": len(set(game_pks)) == row_count},
        {"check": "game_pk_non_empty", "expected": row_count, "actual": sum(1 for pk in game_pks if pk), "passed": all(game_pks)},
        {"check": "game_date_parseable", "expected": row_count, "actual": sum(1 for row in actuals_rows if parse_date(row.get("game_date"))), "passed": all(parse_date(row.get("game_date")) for row in actuals_rows)},
        {"check": "teams_non_empty", "expected": True, "actual": all(row.get("home_team") and row.get("away_team") for row in actuals_rows), "passed": all(row.get("home_team") and row.get("away_team") for row in actuals_rows)},
        {"check": "scores_non_negative_ints", "expected": True, "actual": all((int_or_none(row.get("home_score")) is not None and int_or_none(row.get("away_score")) is not None and int_or_none(row.get("home_score")) >= 0 and int_or_none(row.get("away_score")) >= 0) for row in actuals_rows), "passed": all((int_or_none(row.get("home_score")) is not None and int_or_none(row.get("away_score")) is not None and int_or_none(row.get("home_score")) >= 0 and int_or_none(row.get("away_score")) >= 0) for row in actuals_rows)},
        {"check": "home_win_binary_correct", "expected": True, "actual": all(int_or_none(row.get("home_win_binary")) == int(int_or_none(row.get("home_score")) > int_or_none(row.get("away_score"))) for row in actuals_rows), "passed": all(int_or_none(row.get("home_win_binary")) == int(int_or_none(row.get("home_score")) > int_or_none(row.get("away_score"))) for row in actuals_rows)},
    ]

    provenance_rows = [
        {"check": "source_artifact_present_all_rows", "expected": row_count, "actual": sum(1 for row in actuals_rows if row.get("source_artifact")), "passed": all(row.get("source_artifact") for row in actuals_rows)},
        {"check": "selected_source_family_allowed", "expected": True, "actual": selected_source_family in {"partitioned_actuals_csv", "local_schedule_cache_json", "existing_actuals_fallback"}, "passed": selected_source_family in {"partitioned_actuals_csv", "local_schedule_cache_json", "existing_actuals_fallback"}},
        {"check": "selected_source_count_positive", "expected": ">0", "actual": selected_source_count, "passed": selected_source_count > 0},
        {"check": "selected_source_family_expected_for_larger_source", "expected": "local_schedule_cache_json", "actual": selected_source_family, "passed": selected_source_family == "local_schedule_cache_json"},
    ]

    sample_rows = [
        {"check": "minimum_row_count", "expected": MIN_ROWS, "actual": row_count, "passed": row_count >= MIN_ROWS},
        {"check": "minimum_date_span_days", "expected": MIN_DATE_SPAN_DAYS, "actual": date_span, "passed": date_span >= MIN_DATE_SPAN_DAYS},
        {"check": "sample_classification", "expected": "larger_sample", "actual": json_6ni.get("output_sample_classification"), "passed": json_6ni.get("output_sample_classification") == "larger_sample"},
        {"check": "sufficient_for_real_historical_evaluation", "expected": True, "actual": json_6ni.get("output_sufficient_for_real_historical_evaluation"), "passed": json_6ni.get("output_sufficient_for_real_historical_evaluation") is True},
    ]

    rerun_6na_rows = [
        {"check": "rerun_6na_all_checks_passed", "expected": True, "actual": json_6ni.get("rerun_6na_all_checks_passed"), "passed": json_6ni.get("rerun_6na_all_checks_passed") is True},
        {"check": "rerun_6na_after_larger_source_creation_run", "expected": True, "actual": json_6ni.get("rerun_6na_after_larger_source_creation_run"), "passed": json_6ni.get("rerun_6na_after_larger_source_creation_run") is True},
    ]

    metric_unlock_rows = [
        {"boundary": "actuals_only_metric_unlock_gate_plan_allowed_next", "value": True, "passed": True},
        {"boundary": "metric_execution_allowed_next", "value": False, "passed": True},
        {"boundary": "backtest_execution_allowed_next", "value": False, "passed": True},
        {"boundary": "tuning_allowed_next", "value": False, "passed": True},
        {"boundary": "activation_allowed_next", "value": False, "passed": True},
        {"boundary": "layer_6_exit_allowed_next", "value": False, "passed": True},
    ]

    allowed_next_rows = [
        {"operation": "actuals_only_metric_unlock_gate_plan", "allowed_next": True, "scope": "6NK planning only", "passed": True},
    ]

    forbidden_next_rows = [
        {"operation": "metric_execution", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "actuals_only_metric_layer", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "production_table_creation", "allowed_next": False, "passed": True},
    ]

    future_6nk_rows = [
        {"contract": "plan_actuals_only_metric_unlock_gates", "required": True, "passed": True},
        {"contract": "define_metrics_allowed_after_gate_only", "required": True, "passed": True},
        {"contract": "preserve_no_metric_execution_in_6nk", "required": True, "passed": True},
        {"contract": "preserve_no_backtests_tuning_activation_exit", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6ni_passed", "expected": True, "actual": json_6ni.get("all_checks_passed"), "passed": json_6ni.get("all_checks_passed") is True},
        {"decision": "actuals_file_review_passed", "expected": True, "actual": all_passed(actuals_file_rows), "passed": all_passed(actuals_file_rows)},
        {"decision": "schema_value_review_passed", "expected": True, "actual": all_passed(schema_value_rows), "passed": all_passed(schema_value_rows)},
        {"decision": "provenance_review_passed", "expected": True, "actual": all_passed(provenance_rows), "passed": all_passed(provenance_rows)},
        {"decision": "sample_sufficiency_review_passed", "expected": True, "actual": all_passed(sample_rows), "passed": all_passed(sample_rows)},
        {"decision": "rerun_6na_review_passed", "expected": True, "actual": all_passed(rerun_6na_rows), "passed": all_passed(rerun_6na_rows)},
        {"decision": "recommend_6nk", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only_larger_historical_actuals_source_expansion", "expected": True, "actual": True, "passed": True},
        {"boundary": "actuals_file_modified_by_6nj", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_rows_ingested_by_6nj", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6nj", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6nj", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6nj", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6nj", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6nj", "expected": False, "actual": False, "passed": True},
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
        {"decision": "do_not_recommend_metric_execution_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_backtests", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning_activation_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6NJ, "actual": DIAGNOSIS_6NJ, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "actuals_file_review", "passed": all_passed(actuals_file_rows), "detail": f"{sum(1 for r in actuals_file_rows if r['passed'])}/{len(actuals_file_rows)}"},
        {"check": "schema_value_review", "passed": all_passed(schema_value_rows), "detail": f"{sum(1 for r in schema_value_rows if r['passed'])}/{len(schema_value_rows)}"},
        {"check": "provenance_review", "passed": all_passed(provenance_rows), "detail": f"{sum(1 for r in provenance_rows if r['passed'])}/{len(provenance_rows)}"},
        {"check": "sample_sufficiency_review", "passed": all_passed(sample_rows), "detail": f"{sum(1 for r in sample_rows if r['passed'])}/{len(sample_rows)}"},
        {"check": "rerun_6na_review", "passed": all_passed(rerun_6na_rows), "detail": f"{sum(1 for r in rerun_6na_rows if r['passed'])}/{len(rerun_6na_rows)}"},
        {"check": "metric_unlock_boundaries", "passed": all_passed(metric_unlock_rows), "detail": f"{sum(1 for r in metric_unlock_rows if r['passed'])}/{len(metric_unlock_rows)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next_rows), "detail": f"{sum(1 for r in allowed_next_rows if r['passed'])}/{len(allowed_next_rows)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next_rows), "detail": f"{sum(1 for r in forbidden_next_rows if r['passed'])}/{len(forbidden_next_rows)}"},
        {"check": "future_6nk_contract", "passed": all_passed(future_6nk_rows), "detail": f"{sum(1 for r in future_6nk_rows if r['passed'])}/{len(future_6nk_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "actuals_file_review": write_csv(ACTUALS_FILE_REVIEW_CSV, actuals_file_rows),
        "schema_value_review": write_csv(SCHEMA_VALUE_REVIEW_CSV, schema_value_rows),
        "provenance_review": write_csv(PROVENANCE_REVIEW_CSV, provenance_rows),
        "sample_sufficiency_review": write_csv(SAMPLE_SUFFICIENCY_REVIEW_CSV, sample_rows),
        "rerun_6na_review": write_csv(RERUN_6NA_REVIEW_CSV, rerun_6na_rows),
        "metric_unlock_boundaries": write_csv(METRIC_UNLOCK_CSV, metric_unlock_rows),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next_rows),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next_rows),
        "future_6nk_contract": write_csv(FUTURE_6NK_CSV, future_6nk_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6NJ",
        "layer_type": "game_mechanics_realism",
        "audit_only_larger_historical_actuals_source_expansion": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6NJ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_layer": "6NI",
        "predecessor_diagnosis": json_6ni.get("diagnosis"),
        "predecessor_all_checks_passed": json_6ni.get("all_checks_passed") is True,
        "source_family": "larger_historical_actuals_source_expansion_audit",
        "audited_target_larger_actuals_path": str(TARGET_ACTUALS),
        "audited_output_file_exists": TARGET_ACTUALS.exists(),
        "audited_output_row_count": row_count,
        "audited_output_date_span_days": date_span,
        "audited_output_sample_classification": json_6ni.get("output_sample_classification"),
        "audited_output_sufficient_for_real_historical_evaluation": json_6ni.get("output_sufficient_for_real_historical_evaluation") is True,
        "audited_minimum_larger_sample_row_count": MIN_ROWS,
        "audited_minimum_larger_sample_date_span_days": MIN_DATE_SPAN_DAYS,
        "audited_selected_source_family": selected_source_family,
        "audited_selected_source_count": selected_source_count,
        "audited_rerun_6na_all_checks_passed": json_6ni.get("rerun_6na_all_checks_passed") is True,
        "actuals_only_metric_unlock_gate_plan_allowed_next": True,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "source_rows_ingested_by_6nj": False,
        "normalized_source_tables_created_for_production_by_6nj": False,
        "production_code_modified_by_6nj": False,
        "actuals_file_modified_by_6nj": False,
        "adapter_call_executed_by_6nj": False,
        "metric_execution_run_by_6nj": False,
        "backtest_execution_run_by_6nj": False,
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
            "actuals_file_review_csv": str(ACTUALS_FILE_REVIEW_CSV),
            "schema_value_review_csv": str(SCHEMA_VALUE_REVIEW_CSV),
            "provenance_review_csv": str(PROVENANCE_REVIEW_CSV),
            "sample_sufficiency_review_csv": str(SAMPLE_SUFFICIENCY_REVIEW_CSV),
            "rerun_6na_review_csv": str(RERUN_6NA_REVIEW_CSV),
            "metric_unlock_boundaries_csv": str(METRIC_UNLOCK_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6nk_contract_csv": str(FUTURE_6NK_CSV),
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
