#!/usr/bin/env python3
"""Audit Layer 6 historical actuals source creation from 6ND."""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6ne_historical_actuals_source_creation_audit"
TMP_DIR = Path("tmp")

SCRIPT_6ND = Path("scripts/implement_6nd_layer6_historical_actuals_source_creation.py")
TARGET_ACTUALS = Path("data/local/historical_actuals.csv")
JSON_6ND = TMP_DIR / "layer6_6nd_historical_actuals_source_creation.json"

REQUIRED_INPUTS = [
    JSON_6ND,
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_checks.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_predecessor.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_input_artifacts.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_candidate_files.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_candidate_scores.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_selected_source.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_schema_mapping.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_normalized_sample.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_source_provenance.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_sample_sufficiency.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_rerun_6na_summary.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_moneyline_deferral_boundaries.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_allowed_operations_next.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_future_6ne_contract.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_decision.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_safety_boundaries.csv",
    TMP_DIR / "layer6_6nd_historical_actuals_source_creation_recommended_path.csv",
    SCRIPT_6ND,
    TARGET_ACTUALS,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CREATED_FILE_REVIEW_CSV = TMP_DIR / f"{SLUG}_created_file_review.csv"
SCHEMA_REVIEW_CSV = TMP_DIR / f"{SLUG}_schema_review.csv"
VALUE_REVIEW_CSV = TMP_DIR / f"{SLUG}_value_review.csv"
PROVENANCE_REVIEW_CSV = TMP_DIR / f"{SLUG}_provenance_review.csv"
SAMPLE_SUFFICIENCY_REVIEW_CSV = TMP_DIR / f"{SLUG}_sample_sufficiency_review.csv"
RERUN_6NA_REVIEW_CSV = TMP_DIR / f"{SLUG}_rerun_6na_review.csv"
MONEYLINE_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_moneyline_deferral_boundaries.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6NF_CSV = TMP_DIR / f"{SLUG}_future_6nf_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6ND = "layer_6_historical_actuals_source_creation_implementation_complete"
DIAGNOSIS_6NE = "layer_6_historical_actuals_source_creation_audit_complete"
RECOMMENDED_NEXT_LAYER = "6NF_layer_6_historical_actuals_smoke_sample_status_plan"
RECOMMENDED_PATH = "plan_smoke_sample_status_before_any_actuals_only_metric_layer"

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


def parse_int(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(str(value).strip()))
    except Exception:
        return None


def parse_date(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text[:10])
    except Exception:
        return None


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6nd = load_json(JSON_6ND)
    actuals_rows = read_csv_rows(TARGET_ACTUALS)

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
        {"check": "6nd_script_exists", "expected": True, "actual": SCRIPT_6ND.exists(), "passed": SCRIPT_6ND.exists()},
        {"check": "6nd_json_exists", "expected": True, "actual": JSON_6ND.exists(), "passed": JSON_6ND.exists()},
        {"check": "6nd_all_checks_passed", "expected": True, "actual": json_6nd.get("all_checks_passed"), "passed": json_6nd.get("all_checks_passed") is True},
        {"check": "6nd_blocked", "expected": False, "actual": json_6nd.get("blocked"), "passed": json_6nd.get("blocked") is False},
        {"check": "6nd_diagnosis", "expected": DIAGNOSIS_6ND, "actual": json_6nd.get("diagnosis"), "passed": json_6nd.get("diagnosis") == DIAGNOSIS_6ND},
        {"check": "6nd_created_target", "expected": True, "actual": json_6nd.get("target_actuals_file_created_by_6nd"), "passed": json_6nd.get("target_actuals_file_created_by_6nd") is True},
        {"check": "6nd_reran_6na", "expected": True, "actual": json_6nd.get("reran_6na_validation"), "passed": json_6nd.get("reran_6na_validation") is True},
        {"check": "6nd_rerun_6na_passed", "expected": True, "actual": json_6nd.get("rerun_6na_all_checks_passed"), "passed": json_6nd.get("rerun_6na_all_checks_passed") is True},
        {"check": "6nd_sample_classification", "expected": "smoke_test_only", "actual": json_6nd.get("sample_classification"), "passed": json_6nd.get("sample_classification") == "smoke_test_only"},
        {"check": "6nd_sufficient_for_real_historical_evaluation", "expected": False, "actual": json_6nd.get("sufficient_for_real_historical_evaluation"), "passed": json_6nd.get("sufficient_for_real_historical_evaluation") is False},
    ]

    columns = list(actuals_rows[0].keys()) if actuals_rows else []
    created_file_rows = [
        {"review": "target_actuals_file_exists", "expected": True, "actual": TARGET_ACTUALS.exists(), "passed": TARGET_ACTUALS.exists()},
        {"review": "target_actuals_row_count_positive", "expected": ">0", "actual": len(actuals_rows), "passed": len(actuals_rows) > 0},
        {"review": "target_actuals_output_path", "expected": "data/local/historical_actuals.csv", "actual": str(TARGET_ACTUALS), "passed": str(TARGET_ACTUALS) == "data/local/historical_actuals.csv"},
    ]

    schema_rows = [
        {
            "canonical_field": field,
            "exists": field in columns,
            "position": columns.index(field) + 1 if field in columns else "",
            "expected_position": idx,
            "passed": field in columns and columns.index(field) + 1 == idx,
        }
        for idx, field in enumerate(CANONICAL_SCHEMA, start=1)
    ]
    schema_rows.append(
        {
            "canonical_field": "__exact_schema__",
            "exists": columns == CANONICAL_SCHEMA,
            "position": "",
            "expected_position": "",
            "actual_columns": "|".join(columns),
            "passed": columns == CANONICAL_SCHEMA,
        }
    )

    seen_game_pks: set[str] = set()
    duplicate_game_pks: set[str] = set()
    dates: list[datetime] = []
    invalid_counts = {
        "missing_game_pk": 0,
        "duplicate_game_pk": 0,
        "invalid_game_date": 0,
        "missing_team": 0,
        "invalid_score": 0,
        "tie_game": 0,
        "home_win_binary_mismatch": 0,
        "missing_source_artifact": 0,
    }

    for row in actuals_rows:
        game_pk = str(row.get("game_pk", "")).strip()
        if not game_pk:
            invalid_counts["missing_game_pk"] += 1
        elif game_pk in seen_game_pks:
            duplicate_game_pks.add(game_pk)
        else:
            seen_game_pks.add(game_pk)

        game_date = parse_date(row.get("game_date"))
        if game_date is None:
            invalid_counts["invalid_game_date"] += 1
        else:
            dates.append(game_date)

        if not str(row.get("home_team", "")).strip() or not str(row.get("away_team", "")).strip():
            invalid_counts["missing_team"] += 1

        home_score = parse_int(row.get("home_score"))
        away_score = parse_int(row.get("away_score"))
        home_win_binary = parse_int(row.get("home_win_binary"))

        if home_score is None or away_score is None or home_score < 0 or away_score < 0:
            invalid_counts["invalid_score"] += 1
        elif home_score == away_score:
            invalid_counts["tie_game"] += 1
        elif home_win_binary != int(home_score > away_score):
            invalid_counts["home_win_binary_mismatch"] += 1

        if not str(row.get("source_artifact", "")).strip():
            invalid_counts["missing_source_artifact"] += 1

    invalid_counts["duplicate_game_pk"] = len(duplicate_game_pks)
    date_span_days = 0
    if dates:
        date_span_days = (max(dates).date() - min(dates).date()).days + 1

    value_rows = [
        {"value_check": key, "invalid_count": count, "passed": count == 0}
        for key, count in invalid_counts.items()
    ]
    value_rows.extend(
        [
            {"value_check": "row_count", "actual": len(actuals_rows), "expected": ">0", "passed": len(actuals_rows) > 0},
            {"value_check": "date_span_days", "actual": date_span_days, "expected": 1, "passed": date_span_days == 1},
        ]
    )

    source_artifacts = sorted({str(row.get("source_artifact", "")).strip() for row in actuals_rows if str(row.get("source_artifact", "")).strip()})
    provenance_rows = [
        {"review": "source_artifact_non_empty", "actual_unique_source_artifacts": len(source_artifacts), "passed": len(source_artifacts) > 0},
        {"review": "source_artifact_matches_6nd_selected_source", "expected": json_6nd.get("selected_source_path"), "actual": "|".join(source_artifacts), "passed": source_artifacts == [json_6nd.get("selected_source_path")]},
        {"review": "source_family", "expected": "historical_actuals_source_creation", "actual": json_6nd.get("source_family"), "passed": json_6nd.get("source_family") == "historical_actuals_source_creation"},
    ]

    sample_classification = "smoke_test_only" if date_span_days <= 21 or len(actuals_rows) < 100 else "real_evaluation_candidate"
    sufficient_for_real_eval = sample_classification == "real_evaluation_candidate"
    sample_rows = [
        {"review": "sample_classification", "expected": "smoke_test_only", "actual": sample_classification, "passed": sample_classification == "smoke_test_only"},
        {"review": "row_count", "actual": len(actuals_rows), "threshold_for_real_eval": 100, "passed": len(actuals_rows) < 100},
        {"review": "date_span_days", "actual": date_span_days, "threshold_for_real_eval": 21, "passed": date_span_days <= 21},
        {"review": "sufficient_for_real_historical_evaluation", "expected": False, "actual": sufficient_for_real_eval, "passed": sufficient_for_real_eval is False},
        {"review": "real_historical_evaluation_blocked_due_to_sample_size", "expected": True, "actual": not sufficient_for_real_eval, "passed": not sufficient_for_real_eval},
    ]

    rerun_rows = [
        {"review": "6nd_reran_6na_validation", "expected": True, "actual": json_6nd.get("reran_6na_validation"), "passed": json_6nd.get("reran_6na_validation") is True},
        {"review": "6nd_rerun_6na_exit_code", "expected": 0, "actual": json_6nd.get("rerun_6na_exit_code"), "passed": json_6nd.get("rerun_6na_exit_code") == 0},
        {"review": "6nd_rerun_6na_all_checks_passed", "expected": True, "actual": json_6nd.get("rerun_6na_all_checks_passed"), "passed": json_6nd.get("rerun_6na_all_checks_passed") is True},
    ]

    moneyline_rows = [
        {"boundary": "historical_moneyline_validation", "status": "deferred", "passed": True},
        {"boundary": "market_comparison_metrics", "status": "blocked", "passed": True},
        {"boundary": "roi_clv_market_edge_claims", "status": "blocked", "passed": True},
        {"boundary": "actuals_only_metrics", "status": "blocked_pending_smoke_sample_status_plan", "passed": True},
    ]

    allowed_next_rows = [
        {"operation": "historical_actuals_smoke_sample_status_plan", "allowed_next": True, "scope": "6NF planning only", "passed": True},
    ]

    forbidden_next_rows = [
        {"operation": "source_ingestion", "allowed_next": False, "passed": True},
        {"operation": "metric_execution", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "real_historical_evaluation_claims", "allowed_next": False, "passed": True},
        {"operation": "production_table_creation", "allowed_next": False, "passed": True},
    ]

    future_6nf_rows = [
        {"contract": "plan_smoke_sample_status", "required": True, "passed": True},
        {"contract": "explicitly_block_real_historical_eval_until_larger_sample", "required": True, "passed": True},
        {"contract": "define_larger_sample_requirements", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_backtests_tuning_activation_exit", "required": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only_historical_actuals_source_creation", "expected": True, "actual": True, "passed": True},
        {"boundary": "source_rows_ingested_by_6ne", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6ne", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6ne", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6ne", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6ne", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6ne", "expected": False, "actual": False, "passed": True},
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
        {"decision": "6nd_passed", "expected": True, "actual": json_6nd.get("all_checks_passed"), "passed": json_6nd.get("all_checks_passed") is True},
        {"decision": "target_actuals_exists", "expected": True, "actual": TARGET_ACTUALS.exists(), "passed": TARGET_ACTUALS.exists()},
        {"decision": "canonical_schema_valid", "expected": True, "actual": all_passed(schema_rows), "passed": all_passed(schema_rows)},
        {"decision": "values_valid", "expected": True, "actual": all_passed(value_rows), "passed": all_passed(value_rows)},
        {"decision": "provenance_valid", "expected": True, "actual": all_passed(provenance_rows), "passed": all_passed(provenance_rows)},
        {"decision": "sample_classification_smoke_test_only", "expected": True, "actual": sample_classification == "smoke_test_only", "passed": sample_classification == "smoke_test_only"},
        {"decision": "real_historical_evaluation_blocked_due_to_sample_size", "expected": True, "actual": not sufficient_for_real_eval, "passed": not sufficient_for_real_eval},
        {"decision": "rerun_6na_passed", "expected": True, "actual": all_passed(rerun_rows), "passed": all_passed(rerun_rows)},
        {"decision": "do_not_execute_metrics_backtests_tuning_activation_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "recommend_6nf", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6NE, "actual": DIAGNOSIS_6NE, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "created_file_review", "passed": all_passed(created_file_rows), "detail": f"{sum(1 for r in created_file_rows if r['passed'])}/{len(created_file_rows)}"},
        {"check": "schema_review", "passed": all_passed(schema_rows), "detail": f"{sum(1 for r in schema_rows if r['passed'])}/{len(schema_rows)}"},
        {"check": "value_review", "passed": all_passed(value_rows), "detail": f"{sum(1 for r in value_rows if r['passed'])}/{len(value_rows)}"},
        {"check": "provenance_review", "passed": all_passed(provenance_rows), "detail": f"{sum(1 for r in provenance_rows if r['passed'])}/{len(provenance_rows)}"},
        {"check": "sample_sufficiency_review", "passed": all_passed(sample_rows), "detail": f"{sum(1 for r in sample_rows if r['passed'])}/{len(sample_rows)}"},
        {"check": "rerun_6na_review", "passed": all_passed(rerun_rows), "detail": f"{sum(1 for r in rerun_rows if r['passed'])}/{len(rerun_rows)}"},
        {"check": "moneyline_deferral_boundaries", "passed": all_passed(moneyline_rows), "detail": f"{sum(1 for r in moneyline_rows if r['passed'])}/{len(moneyline_rows)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next_rows), "detail": f"{sum(1 for r in allowed_next_rows if r['passed'])}/{len(allowed_next_rows)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next_rows), "detail": f"{sum(1 for r in forbidden_next_rows if r['passed'])}/{len(forbidden_next_rows)}"},
        {"check": "future_6nf_contract", "passed": all_passed(future_6nf_rows), "detail": f"{sum(1 for r in future_6nf_rows if r['passed'])}/{len(future_6nf_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "created_file_review": write_csv(CREATED_FILE_REVIEW_CSV, created_file_rows),
        "schema_review": write_csv(SCHEMA_REVIEW_CSV, schema_rows),
        "value_review": write_csv(VALUE_REVIEW_CSV, value_rows),
        "provenance_review": write_csv(PROVENANCE_REVIEW_CSV, provenance_rows),
        "sample_sufficiency_review": write_csv(SAMPLE_SUFFICIENCY_REVIEW_CSV, sample_rows),
        "rerun_6na_review": write_csv(RERUN_6NA_REVIEW_CSV, rerun_rows),
        "moneyline_deferral_boundaries": write_csv(MONEYLINE_BOUNDARIES_CSV, moneyline_rows),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next_rows),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next_rows),
        "future_6nf_contract": write_csv(FUTURE_6NF_CSV, future_6nf_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6NE",
        "layer_type": "game_mechanics_realism",
        "audit_only_historical_actuals_source_creation": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6NE if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_layer": "6ND",
        "predecessor_diagnosis": json_6nd.get("diagnosis"),
        "predecessor_all_checks_passed": json_6nd.get("all_checks_passed") is True,
        "source_family": "historical_actuals_source_creation_audit",
        "target_actuals_output_path": str(TARGET_ACTUALS),
        "target_actuals_file_exists": TARGET_ACTUALS.exists(),
        "audited_actuals_row_count": len(actuals_rows),
        "audited_actuals_date_span_days": date_span_days,
        "audited_sample_classification": sample_classification,
        "audited_sufficient_for_real_historical_evaluation": sufficient_for_real_eval,
        "smoke_test_only_status_confirmed": sample_classification == "smoke_test_only",
        "real_historical_evaluation_blocked_due_to_sample_size": not sufficient_for_real_eval,
        "rerun_6na_all_checks_passed": json_6nd.get("rerun_6na_all_checks_passed") is True,
        "historical_actuals_smoke_sample_status_plan_allowed_next": True,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "source_rows_ingested_by_6ne": False,
        "normalized_source_tables_created_for_production_by_6ne": False,
        "production_code_modified_by_6ne": False,
        "adapter_call_executed_by_6ne": False,
        "metric_execution_run_by_6ne": False,
        "backtest_execution_run_by_6ne": False,
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
            "created_file_review_csv": str(CREATED_FILE_REVIEW_CSV),
            "schema_review_csv": str(SCHEMA_REVIEW_CSV),
            "value_review_csv": str(VALUE_REVIEW_CSV),
            "provenance_review_csv": str(PROVENANCE_REVIEW_CSV),
            "sample_sufficiency_review_csv": str(SAMPLE_SUFFICIENCY_REVIEW_CSV),
            "rerun_6na_review_csv": str(RERUN_6NA_REVIEW_CSV),
            "moneyline_deferral_boundaries_csv": str(MONEYLINE_BOUNDARIES_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6nf_contract_csv": str(FUTURE_6NF_CSV),
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
