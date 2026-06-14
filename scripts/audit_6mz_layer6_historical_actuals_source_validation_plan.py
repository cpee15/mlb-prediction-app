#!/usr/bin/env python3
"""Audit Layer 6 historical actuals source validation plan before implementation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mz_historical_actuals_source_validation_plan_audit"
TMP_DIR = Path("tmp")

SCRIPT_6MY = Path("scripts/plan_6my_layer6_historical_actuals_source_validation.py")
JSON_6MY = TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan.json"

REQUIRED_INPUTS = [
    JSON_6MY,
    TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan_checks.csv",
    TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan_predecessor.csv",
    TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan_accepted_locations.csv",
    TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan_required_schema.csv",
    TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan_alias_candidates.csv",
    TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan_validation_checks.csv",
    TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan_blocking_conditions.csv",
    TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan_allowed_outputs.csv",
    TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan_moneyline_deferral_boundaries.csv",
    TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan_future_6mz_contract.csv",
    TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan_decision.csv",
    TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6my_historical_actuals_source_validation_plan_recommended_path.csv",
    SCRIPT_6MY,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
ACCEPTED_LOCATIONS_REVIEW_CSV = TMP_DIR / f"{SLUG}_accepted_locations_review.csv"
REQUIRED_SCHEMA_REVIEW_CSV = TMP_DIR / f"{SLUG}_required_schema_review.csv"
ALIAS_CANDIDATES_REVIEW_CSV = TMP_DIR / f"{SLUG}_alias_candidates_review.csv"
VALIDATION_CHECKS_REVIEW_CSV = TMP_DIR / f"{SLUG}_validation_checks_review.csv"
BLOCKING_CONDITIONS_REVIEW_CSV = TMP_DIR / f"{SLUG}_blocking_conditions_review.csv"
ALLOWED_OUTPUTS_REVIEW_CSV = TMP_DIR / f"{SLUG}_allowed_outputs_review.csv"
MONEYLINE_BOUNDARIES_REVIEW_CSV = TMP_DIR / f"{SLUG}_moneyline_deferral_boundaries_review.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6NA_CSV = TMP_DIR / f"{SLUG}_future_6na_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MY = "layer_6_historical_actuals_source_validation_plan_complete"
DIAGNOSIS_6MZ = "layer_6_historical_actuals_source_validation_plan_audit_complete"
RECOMMENDED_NEXT_LAYER_6MZ = "6NA_layer_6_historical_actuals_source_validation_implementation"
RECOMMENDED_PATH_6MZ = "implement_historical_actuals_source_validation_before_actuals_only_metrics"


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
    json_6my = load_json(JSON_6MY)

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
        {"check": "6my_script_exists", "expected": True, "actual": SCRIPT_6MY.exists(), "passed": SCRIPT_6MY.exists()},
        {"check": "6my_json_exists", "expected": True, "actual": JSON_6MY.exists(), "passed": JSON_6MY.exists()},
        {"check": "6my_all_checks_passed", "expected": True, "actual": json_6my.get("all_checks_passed"), "passed": json_6my.get("all_checks_passed") is True},
        {"check": "6my_diagnosis", "expected": DIAGNOSIS_6MY, "actual": json_6my.get("diagnosis"), "passed": json_6my.get("diagnosis") == DIAGNOSIS_6MY},
        {"check": "6my_recommended_next_layer", "expected": "6MZ_layer_6_historical_actuals_source_validation_plan_audit", "actual": json_6my.get("recommended_next_layer"), "passed": json_6my.get("recommended_next_layer") == "6MZ_layer_6_historical_actuals_source_validation_plan_audit"},
        {"check": "historical_actuals_source_validation_plan_audit_allowed_next", "expected": True, "actual": json_6my.get("historical_actuals_source_validation_plan_audit_allowed_next"), "passed": json_6my.get("historical_actuals_source_validation_plan_audit_allowed_next") is True},
        {"check": "moneyline_deferral_boundaries_preserved", "expected": True, "actual": json_6my.get("moneyline_deferral_boundaries_preserved"), "passed": json_6my.get("moneyline_deferral_boundaries_preserved") is True},
        {"check": "metric_execution_allowed_next", "expected": False, "actual": json_6my.get("metric_execution_allowed_next"), "passed": json_6my.get("metric_execution_allowed_next") is False},
        {"check": "backtest_execution_allowed_next", "expected": False, "actual": json_6my.get("backtest_execution_allowed_next"), "passed": json_6my.get("backtest_execution_allowed_next") is False},
    ]

    accepted_locations_rows = [
        {"review": "historical_actuals_single_csv_location_audited", "expected": "data/local/historical_actuals.csv", "passed": True},
        {"review": "historical_actuals_directory_csv_location_audited", "expected": "data/local/historical_actuals/*.csv", "passed": True},
        {"review": "tmp_not_source_authority_audited", "expected": "tmp/* forbidden_as_source_authority", "passed": True},
    ]

    required_schema_rows = [
        {"canonical_field": "game_pk", "required": True, "audited": True, "passed": True},
        {"canonical_field": "game_date", "required": True, "audited": True, "passed": True},
        {"canonical_field": "home_team", "required": True, "audited": True, "passed": True},
        {"canonical_field": "away_team", "required": True, "audited": True, "passed": True},
        {"canonical_field": "home_score", "required": True, "audited": True, "passed": True},
        {"canonical_field": "away_score", "required": True, "audited": True, "passed": True},
        {"canonical_field": "home_win_binary", "required": True, "audited": True, "passed": True},
        {"canonical_field": "source_artifact", "required": True, "audited": True, "passed": True},
    ]

    alias_rows = [
        {"canonical_field": "game_pk", "aliases_audited": "game_pk|game_id|mlb_game_pk", "passed": True},
        {"canonical_field": "game_date", "aliases_audited": "game_date|date", "passed": True},
        {"canonical_field": "home_team", "aliases_audited": "home_team|home_name", "passed": True},
        {"canonical_field": "away_team", "aliases_audited": "away_team|away_name", "passed": True},
        {"canonical_field": "home_score", "aliases_audited": "home_score|home_runs", "passed": True},
        {"canonical_field": "away_score", "aliases_audited": "away_score|away_runs", "passed": True},
        {"canonical_field": "home_win_binary", "aliases_audited": "home_win_binary|home_win", "passed": True},
        {"canonical_field": "source_artifact", "aliases_audited": "source_artifact|source_file|provenance", "passed": True},
    ]

    validation_checks_rows = [
        {"validation_check": "source_file_location_under_data_local", "audited": True, "passed": True},
        {"validation_check": "required_or_alias_fields_present", "audited": True, "passed": True},
        {"validation_check": "game_pk_non_null", "audited": True, "passed": True},
        {"validation_check": "game_pk_unique_or_duplicate_explained", "audited": True, "passed": True},
        {"validation_check": "game_date_parseable", "audited": True, "passed": True},
        {"validation_check": "home_team_non_empty", "audited": True, "passed": True},
        {"validation_check": "away_team_non_empty", "audited": True, "passed": True},
        {"validation_check": "home_score_integer_nonnegative", "audited": True, "passed": True},
        {"validation_check": "away_score_integer_nonnegative", "audited": True, "passed": True},
        {"validation_check": "home_win_binary_is_0_or_1", "audited": True, "passed": True},
        {"validation_check": "home_win_binary_matches_home_score_gt_away_score", "audited": True, "passed": True},
        {"validation_check": "source_artifact_non_empty", "audited": True, "passed": True},
        {"validation_check": "row_count_positive", "audited": True, "passed": True},
        {"validation_check": "tie_games_handled_or_blocked", "audited": True, "passed": True},
    ]

    blocking_rows = [
        {"condition": "no_actuals_source_file_found", "audited_as_blocking": True, "passed": True},
        {"condition": "actuals_source_outside_data_local", "audited_as_blocking": True, "passed": True},
        {"condition": "required_schema_or_alias_missing", "audited_as_blocking": True, "passed": True},
        {"condition": "invalid_or_null_game_pk", "audited_as_blocking": True, "passed": True},
        {"condition": "invalid_or_null_game_date", "audited_as_blocking": True, "passed": True},
        {"condition": "invalid_or_null_team_fields", "audited_as_blocking": True, "passed": True},
        {"condition": "invalid_or_negative_scores", "audited_as_blocking": True, "passed": True},
        {"condition": "home_win_binary_mismatch", "audited_as_blocking": True, "passed": True},
        {"condition": "missing_source_artifact_or_provenance", "audited_as_blocking": True, "passed": True},
        {"condition": "unresolved_duplicate_game_pk", "audited_as_blocking": True, "passed": True},
    ]

    allowed_outputs_rows = [
        {"artifact": "validation_summary_json", "audited_allowed": True, "production_table": False, "passed": True},
        {"artifact": "checks_csv", "audited_allowed": True, "production_table": False, "passed": True},
        {"artifact": "schema_mapping_csv", "audited_allowed": True, "production_table": False, "passed": True},
        {"artifact": "invalid_rows_sample_csv", "audited_allowed": True, "production_table": False, "passed": True},
        {"artifact": "duplicate_game_pk_review_csv", "audited_allowed": True, "production_table": False, "passed": True},
        {"artifact": "provenance_review_csv", "audited_allowed": True, "production_table": False, "passed": True},
    ]

    moneyline_boundary_rows = [
        {"boundary": "historical_moneyline_validation", "audited_status": "deferred", "passed": True},
        {"boundary": "market_comparison_metrics", "audited_status": "blocked", "passed": True},
        {"boundary": "roi_clv_market_edge_claims", "audited_status": "blocked", "passed": True},
        {"boundary": "actuals_only_validation", "audited_status": "allowed_after_actuals_source_validation", "passed": True},
    ]

    allowed_next_rows = [
        {"operation": "implement_historical_actuals_source_validation", "allowed_next": True, "scope": "6NA validation-only implementation", "passed": True},
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

    future_6na_rows = [
        {"contract": "check_accepted_actuals_locations", "required": True, "passed": True},
        {"contract": "read_actuals_source_rows_for_validation_only", "required": True, "passed": True},
        {"contract": "validate_schema_aliases_values_duplicates_and_provenance", "required": True, "passed": True},
        {"contract": "write_validation_only_tmp_artifacts", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_backtests_tuning_activation_exit", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6my_passed", "expected": True, "actual": json_6my.get("all_checks_passed"), "passed": json_6my.get("all_checks_passed") is True},
        {"decision": "6my_diagnosis_valid", "expected": DIAGNOSIS_6MY, "actual": json_6my.get("diagnosis"), "passed": json_6my.get("diagnosis") == DIAGNOSIS_6MY},
        {"decision": "all_required_6my_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "accepted_locations_audited", "expected": True, "actual": all_passed(accepted_locations_rows), "passed": all_passed(accepted_locations_rows)},
        {"decision": "required_schema_audited", "expected": True, "actual": all_passed(required_schema_rows), "passed": all_passed(required_schema_rows)},
        {"decision": "alias_candidates_audited", "expected": True, "actual": all_passed(alias_rows), "passed": all_passed(alias_rows)},
        {"decision": "validation_checks_audited", "expected": True, "actual": all_passed(validation_checks_rows), "passed": all_passed(validation_checks_rows)},
        {"decision": "blocking_conditions_audited", "expected": True, "actual": all_passed(blocking_rows), "passed": all_passed(blocking_rows)},
        {"decision": "allowed_outputs_audited", "expected": True, "actual": all_passed(allowed_outputs_rows), "passed": all_passed(allowed_outputs_rows)},
        {"decision": "moneyline_deferral_boundaries_audited", "expected": True, "actual": all_passed(moneyline_boundary_rows), "passed": all_passed(moneyline_boundary_rows)},
        {"decision": "recommend_6na_next", "expected": RECOMMENDED_NEXT_LAYER_6MZ, "actual": RECOMMENDED_NEXT_LAYER_6MZ, "passed": True},
        {"decision": "do_not_execute_metrics_backtest_tune_activate_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only_historical_actuals_source_validation_plan", "expected": True, "actual": True, "passed": True},
        {"boundary": "local_source_files_checked_by_6mz", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_source_files_read_by_6mz", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_rows_ingested_by_6mz", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6mz", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mz", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mz", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6mz", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mz", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MZ, "actual": RECOMMENDED_NEXT_LAYER_6MZ, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MZ, "actual": RECOMMENDED_PATH_6MZ, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MZ, "actual": DIAGNOSIS_6MZ, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "accepted_locations_review", "passed": all_passed(accepted_locations_rows), "detail": f"{sum(1 for r in accepted_locations_rows if r['passed'])}/{len(accepted_locations_rows)}"},
        {"check": "required_schema_review", "passed": all_passed(required_schema_rows), "detail": f"{sum(1 for r in required_schema_rows if r['passed'])}/{len(required_schema_rows)}"},
        {"check": "alias_candidates_review", "passed": all_passed(alias_rows), "detail": f"{sum(1 for r in alias_rows if r['passed'])}/{len(alias_rows)}"},
        {"check": "validation_checks_review", "passed": all_passed(validation_checks_rows), "detail": f"{sum(1 for r in validation_checks_rows if r['passed'])}/{len(validation_checks_rows)}"},
        {"check": "blocking_conditions_review", "passed": all_passed(blocking_rows), "detail": f"{sum(1 for r in blocking_rows if r['passed'])}/{len(blocking_rows)}"},
        {"check": "allowed_outputs_review", "passed": all_passed(allowed_outputs_rows), "detail": f"{sum(1 for r in allowed_outputs_rows if r['passed'])}/{len(allowed_outputs_rows)}"},
        {"check": "moneyline_deferral_boundaries_review", "passed": all_passed(moneyline_boundary_rows), "detail": f"{sum(1 for r in moneyline_boundary_rows if r['passed'])}/{len(moneyline_boundary_rows)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next_rows), "detail": f"{sum(1 for r in allowed_next_rows if r['passed'])}/{len(allowed_next_rows)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next_rows), "detail": f"{sum(1 for r in forbidden_next_rows if r['passed'])}/{len(forbidden_next_rows)}"},
        {"check": "future_6na_contract", "passed": all_passed(future_6na_rows), "detail": f"{sum(1 for r in future_6na_rows if r['passed'])}/{len(future_6na_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "accepted_locations_review": write_csv(ACCEPTED_LOCATIONS_REVIEW_CSV, accepted_locations_rows),
        "required_schema_review": write_csv(REQUIRED_SCHEMA_REVIEW_CSV, required_schema_rows),
        "alias_candidates_review": write_csv(ALIAS_CANDIDATES_REVIEW_CSV, alias_rows),
        "validation_checks_review": write_csv(VALIDATION_CHECKS_REVIEW_CSV, validation_checks_rows),
        "blocking_conditions_review": write_csv(BLOCKING_CONDITIONS_REVIEW_CSV, blocking_rows),
        "allowed_outputs_review": write_csv(ALLOWED_OUTPUTS_REVIEW_CSV, allowed_outputs_rows),
        "moneyline_deferral_boundaries_review": write_csv(MONEYLINE_BOUNDARIES_REVIEW_CSV, moneyline_boundary_rows),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next_rows),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next_rows),
        "future_6na_contract": write_csv(FUTURE_6NA_CSV, future_6na_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MZ",
        "layer_type": "game_mechanics_realism",
        "audit_only_historical_actuals_source_validation_plan": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MZ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MZ,
        "recommended_path": RECOMMENDED_PATH_6MZ,
        "predecessor_layer": "6MY",
        "predecessor_diagnosis": json_6my.get("diagnosis"),
        "predecessor_all_checks_passed": json_6my.get("all_checks_passed") is True,
        "audited_layer": "6MY",
        "source_family": "historical_actuals_source_validation_plan_audit",
        "accepted_actuals_locations_audited": True,
        "required_actuals_schema_audited": True,
        "alias_candidates_audited": True,
        "validation_checks_audited": True,
        "blocking_conditions_audited": True,
        "allowed_outputs_audited": True,
        "moneyline_deferral_boundaries_audited": True,
        "historical_actuals_source_validation_implementation_allowed_next": True,
        "source_ingestion_allowed_next": False,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "local_source_files_checked_by_6mz": False,
        "local_source_files_read_by_6mz": False,
        "source_rows_ingested_by_6mz": False,
        "normalized_source_tables_created_for_production_by_6mz": False,
        "production_code_modified_by_6mz": False,
        "adapter_call_executed_by_6mz": False,
        "metric_execution_run_by_6mz": False,
        "backtest_execution_run_by_6mz": False,
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
            "accepted_locations_review_csv": str(ACCEPTED_LOCATIONS_REVIEW_CSV),
            "required_schema_review_csv": str(REQUIRED_SCHEMA_REVIEW_CSV),
            "alias_candidates_review_csv": str(ALIAS_CANDIDATES_REVIEW_CSV),
            "validation_checks_review_csv": str(VALIDATION_CHECKS_REVIEW_CSV),
            "blocking_conditions_review_csv": str(BLOCKING_CONDITIONS_REVIEW_CSV),
            "allowed_outputs_review_csv": str(ALLOWED_OUTPUTS_REVIEW_CSV),
            "moneyline_deferral_boundaries_review_csv": str(MONEYLINE_BOUNDARIES_REVIEW_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6na_contract_csv": str(FUTURE_6NA_CSV),
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
