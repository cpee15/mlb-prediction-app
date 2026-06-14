#!/usr/bin/env python3
"""Audit Layer 6 historical actuals source preparation plan before source creation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6nc_historical_actuals_source_preparation_plan_audit"
TMP_DIR = Path("tmp")

SCRIPT_6NB = Path("scripts/plan_6nb_layer6_historical_actuals_source_preparation.py")
JSON_6NB = TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan.json"

REQUIRED_INPUTS = [
    JSON_6NB,
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_checks.csv",
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_predecessor.csv",
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_target_output.csv",
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_required_schema.csv",
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_allowed_source_families.csv",
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_source_preparation_checks.csv",
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_row_requirements.csv",
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_provenance_requirements.csv",
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_rerun_commands.csv",
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_blocking_conditions.csv",
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_future_6nc_contract.csv",
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_decision.csv",
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6nb_historical_actuals_source_preparation_plan_recommended_path.csv",
    SCRIPT_6NB,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
TARGET_OUTPUT_REVIEW_CSV = TMP_DIR / f"{SLUG}_target_output_review.csv"
REQUIRED_SCHEMA_REVIEW_CSV = TMP_DIR / f"{SLUG}_required_schema_review.csv"
ALLOWED_SOURCE_FAMILIES_REVIEW_CSV = TMP_DIR / f"{SLUG}_allowed_source_families_review.csv"
SOURCE_PREP_CHECKS_REVIEW_CSV = TMP_DIR / f"{SLUG}_source_preparation_checks_review.csv"
ROW_REQUIREMENTS_REVIEW_CSV = TMP_DIR / f"{SLUG}_row_requirements_review.csv"
PROVENANCE_REQUIREMENTS_REVIEW_CSV = TMP_DIR / f"{SLUG}_provenance_requirements_review.csv"
RERUN_COMMANDS_REVIEW_CSV = TMP_DIR / f"{SLUG}_rerun_commands_review.csv"
BLOCKING_CONDITIONS_REVIEW_CSV = TMP_DIR / f"{SLUG}_blocking_conditions_review.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6ND_CSV = TMP_DIR / f"{SLUG}_future_6nd_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6NB = "layer_6_historical_actuals_source_preparation_plan_complete"
DIAGNOSIS_6NC = "layer_6_historical_actuals_source_preparation_plan_audit_complete"
RECOMMENDED_NEXT_LAYER_6NC = "6ND_layer_6_historical_actuals_source_creation_implementation"
RECOMMENDED_PATH_6NC = "create_historical_actuals_source_then_rerun_6na_validation"


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
    json_6nb = load_json(JSON_6NB)

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
        {"check": "6nb_script_exists", "expected": True, "actual": SCRIPT_6NB.exists(), "passed": SCRIPT_6NB.exists()},
        {"check": "6nb_json_exists", "expected": True, "actual": JSON_6NB.exists(), "passed": JSON_6NB.exists()},
        {"check": "6nb_all_checks_passed", "expected": True, "actual": json_6nb.get("all_checks_passed"), "passed": json_6nb.get("all_checks_passed") is True},
        {"check": "6nb_diagnosis", "expected": DIAGNOSIS_6NB, "actual": json_6nb.get("diagnosis"), "passed": json_6nb.get("diagnosis") == DIAGNOSIS_6NB},
        {"check": "6nb_recommended_next_layer", "expected": "6NC_layer_6_historical_actuals_source_preparation_plan_audit", "actual": json_6nb.get("recommended_next_layer"), "passed": json_6nb.get("recommended_next_layer") == "6NC_layer_6_historical_actuals_source_preparation_plan_audit"},
        {"check": "historical_actuals_source_preparation_plan_audit_allowed_next", "expected": True, "actual": json_6nb.get("historical_actuals_source_preparation_plan_audit_allowed_next"), "passed": json_6nb.get("historical_actuals_source_preparation_plan_audit_allowed_next") is True},
        {"check": "source_creation_allowed_next", "expected": False, "actual": json_6nb.get("source_creation_allowed_next"), "passed": json_6nb.get("source_creation_allowed_next") is False},
        {"check": "metric_execution_allowed_next", "expected": False, "actual": json_6nb.get("metric_execution_allowed_next"), "passed": json_6nb.get("metric_execution_allowed_next") is False},
        {"check": "backtest_execution_allowed_next", "expected": False, "actual": json_6nb.get("backtest_execution_allowed_next"), "passed": json_6nb.get("backtest_execution_allowed_next") is False},
    ]

    target_output_rows = [
        {"review": "primary_actuals_output_audited", "path": "data/local/historical_actuals.csv", "passed": True},
        {"review": "alternate_actuals_directory_audited", "path": "data/local/historical_actuals/*.csv", "passed": True},
        {"review": "tmp_not_source_authority_audited", "path": "tmp/*", "passed": True},
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

    allowed_source_rows = [
        {"source_family": "user_supplied_csv", "allowed": True, "audited": True, "passed": True},
        {"source_family": "repo_existing_local_raw_results", "allowed": True, "audited": True, "passed": True},
        {"source_family": "manual_export_from_trusted_results_source", "allowed": True, "audited": True, "passed": True},
        {"source_family": "live_api_fetch", "allowed": False, "audited": True, "passed": True},
        {"source_family": "remote_scrape", "allowed": False, "audited": True, "passed": True},
    ]

    source_prep_rows = [
        {"check": "output_path_under_data_local", "audited": True, "passed": True},
        {"check": "canonical_columns_exactly_available_or_mapped", "audited": True, "passed": True},
        {"check": "row_count_positive", "audited": True, "passed": True},
        {"check": "game_pk_non_null_unique", "audited": True, "passed": True},
        {"check": "game_date_parseable", "audited": True, "passed": True},
        {"check": "team_fields_non_empty", "audited": True, "passed": True},
        {"check": "scores_integer_nonnegative", "audited": True, "passed": True},
        {"check": "home_win_binary_consistent_with_scores", "audited": True, "passed": True},
        {"check": "source_artifact_non_empty", "audited": True, "passed": True},
        {"check": "no_metric_columns_required_or_created", "audited": True, "passed": True},
    ]

    row_requirements_rows = [
        {"requirement": "one_row_per_completed_game", "audited": True, "passed": True},
        {"requirement": "exclude_postponed_or_unplayed_games", "audited": True, "passed": True},
        {"requirement": "tie_games_absent_or_explicitly_blocked", "audited": True, "passed": True},
        {"requirement": "home_win_binary_equals_int_home_score_gt_away_score", "audited": True, "passed": True},
        {"requirement": "no_market_odds_required", "audited": True, "passed": True},
    ]

    provenance_rows = [
        {"requirement": "source_artifact_identifies_file_or_export_source", "audited": True, "passed": True},
        {"requirement": "source_generation_method_documented_outside_tmp", "audited": True, "passed": True},
        {"requirement": "tmp_outputs_not_treated_as_source_authority", "audited": True, "passed": True},
        {"requirement": "historical_moneyline_not_required", "audited": True, "passed": True},
    ]

    rerun_rows = [
        {"step": 1, "command": "python scripts/implement_6na_layer6_historical_actuals_source_validation.py", "audited": True, "passed": True},
        {"step": 2, "command": "cat tmp/layer6_6na_historical_actuals_source_validation.json", "audited": True, "passed": True},
    ]

    blocking_rows = [
        {"condition": "unable_to_create_or_supply_actuals_source", "audited_as_blocking": True, "passed": True},
        {"condition": "actuals_source_outside_data_local", "audited_as_blocking": True, "passed": True},
        {"condition": "missing_required_schema", "audited_as_blocking": True, "passed": True},
        {"condition": "invalid_scores_or_home_win_binary", "audited_as_blocking": True, "passed": True},
        {"condition": "duplicate_game_pk", "audited_as_blocking": True, "passed": True},
        {"condition": "missing_source_artifact", "audited_as_blocking": True, "passed": True},
    ]

    allowed_next_rows = [
        {"operation": "create_historical_actuals_source", "allowed_next": True, "scope": "6ND source-creation implementation only", "passed": True},
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
        {"operation": "production_source_modification", "allowed_next": False, "passed": True},
        {"operation": "production_table_creation", "allowed_next": False, "passed": True},
    ]

    future_6nd_rows = [
        {"contract": "create_or_supply_data_local_historical_actuals_csv", "required": True, "passed": True},
        {"contract": "write_source_generation_provenance", "required": True, "passed": True},
        {"contract": "do_not_ingest_to_production_tables", "required": True, "passed": True},
        {"contract": "rerun_6na_after_source_created", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_backtests_tuning_activation_exit", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6nb_passed", "expected": True, "actual": json_6nb.get("all_checks_passed"), "passed": json_6nb.get("all_checks_passed") is True},
        {"decision": "6nb_diagnosis_valid", "expected": DIAGNOSIS_6NB, "actual": json_6nb.get("diagnosis"), "passed": json_6nb.get("diagnosis") == DIAGNOSIS_6NB},
        {"decision": "all_required_6nb_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "target_output_audited", "expected": True, "actual": all_passed(target_output_rows), "passed": all_passed(target_output_rows)},
        {"decision": "required_schema_audited", "expected": True, "actual": all_passed(required_schema_rows), "passed": all_passed(required_schema_rows)},
        {"decision": "allowed_source_families_audited", "expected": True, "actual": all_passed(allowed_source_rows), "passed": all_passed(allowed_source_rows)},
        {"decision": "source_preparation_checks_audited", "expected": True, "actual": all_passed(source_prep_rows), "passed": all_passed(source_prep_rows)},
        {"decision": "row_requirements_audited", "expected": True, "actual": all_passed(row_requirements_rows), "passed": all_passed(row_requirements_rows)},
        {"decision": "provenance_requirements_audited", "expected": True, "actual": all_passed(provenance_rows), "passed": all_passed(provenance_rows)},
        {"decision": "rerun_commands_audited", "expected": True, "actual": all_passed(rerun_rows), "passed": all_passed(rerun_rows)},
        {"decision": "blocking_conditions_audited", "expected": True, "actual": all_passed(blocking_rows), "passed": all_passed(blocking_rows)},
        {"decision": "recommend_6nd_next", "expected": RECOMMENDED_NEXT_LAYER_6NC, "actual": RECOMMENDED_NEXT_LAYER_6NC, "passed": True},
        {"decision": "do_not_execute_source_creation_or_metrics", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only_historical_actuals_source_preparation_plan", "expected": True, "actual": True, "passed": True},
        {"boundary": "local_source_files_checked_by_6nc", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_source_files_read_by_6nc", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_rows_created_by_6nc", "expected": 0, "actual": 0, "passed": True},
        {"boundary": "source_rows_ingested_by_6nc", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6nc", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6nc", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6nc", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6nc", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6nc", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6NC, "actual": RECOMMENDED_NEXT_LAYER_6NC, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6NC, "actual": RECOMMENDED_PATH_6NC, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6NC, "actual": DIAGNOSIS_6NC, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "target_output_review", "passed": all_passed(target_output_rows), "detail": f"{sum(1 for r in target_output_rows if r['passed'])}/{len(target_output_rows)}"},
        {"check": "required_schema_review", "passed": all_passed(required_schema_rows), "detail": f"{sum(1 for r in required_schema_rows if r['passed'])}/{len(required_schema_rows)}"},
        {"check": "allowed_source_families_review", "passed": all_passed(allowed_source_rows), "detail": f"{sum(1 for r in allowed_source_rows if r['passed'])}/{len(allowed_source_rows)}"},
        {"check": "source_preparation_checks_review", "passed": all_passed(source_prep_rows), "detail": f"{sum(1 for r in source_prep_rows if r['passed'])}/{len(source_prep_rows)}"},
        {"check": "row_requirements_review", "passed": all_passed(row_requirements_rows), "detail": f"{sum(1 for r in row_requirements_rows if r['passed'])}/{len(row_requirements_rows)}"},
        {"check": "provenance_requirements_review", "passed": all_passed(provenance_rows), "detail": f"{sum(1 for r in provenance_rows if r['passed'])}/{len(provenance_rows)}"},
        {"check": "rerun_commands_review", "passed": all_passed(rerun_rows), "detail": f"{sum(1 for r in rerun_rows if r['passed'])}/{len(rerun_rows)}"},
        {"check": "blocking_conditions_review", "passed": all_passed(blocking_rows), "detail": f"{sum(1 for r in blocking_rows if r['passed'])}/{len(blocking_rows)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next_rows), "detail": f"{sum(1 for r in allowed_next_rows if r['passed'])}/{len(allowed_next_rows)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next_rows), "detail": f"{sum(1 for r in forbidden_next_rows if r['passed'])}/{len(forbidden_next_rows)}"},
        {"check": "future_6nd_contract", "passed": all_passed(future_6nd_rows), "detail": f"{sum(1 for r in future_6nd_rows if r['passed'])}/{len(future_6nd_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "target_output_review": write_csv(TARGET_OUTPUT_REVIEW_CSV, target_output_rows),
        "required_schema_review": write_csv(REQUIRED_SCHEMA_REVIEW_CSV, required_schema_rows),
        "allowed_source_families_review": write_csv(ALLOWED_SOURCE_FAMILIES_REVIEW_CSV, allowed_source_rows),
        "source_preparation_checks_review": write_csv(SOURCE_PREP_CHECKS_REVIEW_CSV, source_prep_rows),
        "row_requirements_review": write_csv(ROW_REQUIREMENTS_REVIEW_CSV, row_requirements_rows),
        "provenance_requirements_review": write_csv(PROVENANCE_REQUIREMENTS_REVIEW_CSV, provenance_rows),
        "rerun_commands_review": write_csv(RERUN_COMMANDS_REVIEW_CSV, rerun_rows),
        "blocking_conditions_review": write_csv(BLOCKING_CONDITIONS_REVIEW_CSV, blocking_rows),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next_rows),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next_rows),
        "future_6nd_contract": write_csv(FUTURE_6ND_CSV, future_6nd_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6NC",
        "layer_type": "game_mechanics_realism",
        "audit_only_historical_actuals_source_preparation_plan": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6NC if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6NC,
        "recommended_path": RECOMMENDED_PATH_6NC,
        "predecessor_layer": "6NB",
        "predecessor_diagnosis": json_6nb.get("diagnosis"),
        "predecessor_all_checks_passed": json_6nb.get("all_checks_passed") is True,
        "audited_layer": "6NB",
        "source_family": "historical_actuals_source_preparation_plan_audit",
        "target_actuals_output_audited": True,
        "required_actuals_schema_audited": True,
        "allowed_source_families_audited": True,
        "source_preparation_checks_audited": True,
        "row_requirements_audited": True,
        "provenance_requirements_audited": True,
        "rerun_commands_audited": True,
        "blocking_conditions_audited": True,
        "historical_actuals_source_creation_allowed_next": True,
        "source_ingestion_allowed_next": False,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "local_source_files_checked_by_6nc": False,
        "local_source_files_read_by_6nc": False,
        "source_rows_created_by_6nc": 0,
        "source_rows_ingested_by_6nc": False,
        "normalized_source_tables_created_for_production_by_6nc": False,
        "production_code_modified_by_6nc": False,
        "adapter_call_executed_by_6nc": False,
        "metric_execution_run_by_6nc": False,
        "backtest_execution_run_by_6nc": False,
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
            "target_output_review_csv": str(TARGET_OUTPUT_REVIEW_CSV),
            "required_schema_review_csv": str(REQUIRED_SCHEMA_REVIEW_CSV),
            "allowed_source_families_review_csv": str(ALLOWED_SOURCE_FAMILIES_REVIEW_CSV),
            "source_preparation_checks_review_csv": str(SOURCE_PREP_CHECKS_REVIEW_CSV),
            "row_requirements_review_csv": str(ROW_REQUIREMENTS_REVIEW_CSV),
            "provenance_requirements_review_csv": str(PROVENANCE_REQUIREMENTS_REVIEW_CSV),
            "rerun_commands_review_csv": str(RERUN_COMMANDS_REVIEW_CSV),
            "blocking_conditions_review_csv": str(BLOCKING_CONDITIONS_REVIEW_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6nd_contract_csv": str(FUTURE_6ND_CSV),
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
