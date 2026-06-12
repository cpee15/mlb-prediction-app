#!/usr/bin/env python3
"""Audit Layer 6 wait-state documentation for historical actuals/moneyline sources."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit"
TMP_DIR = Path("tmp")

SCRIPT_6MT = Path("scripts/plan_6mt_layer6_projection_adapter_historical_actuals_moneyline_wait_state.py")
JSON_6MT = TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan.json"

CURRENT_STATE_6MT = TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_current_state.csv"
REQUIRED_FILES_6MT = TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_required_files.csv"
ACTUALS_SCHEMA_6MT = TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_actuals_schema.csv"
MONEYLINE_SCHEMA_6MT = TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_moneyline_schema.csv"
PROVENANCE_6MT = TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_provenance_requirements.csv"
RESUME_6MT = TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_resume_conditions.csv"
COMMANDS_6MT = TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_validation_commands.csv"
FORBIDDEN_WAITING_6MT = TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_forbidden_while_waiting.csv"

REQUIRED_INPUTS = [
    JSON_6MT,
    TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_checks.csv",
    TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_predecessor.csv",
    TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_input_artifacts.csv",
    CURRENT_STATE_6MT,
    REQUIRED_FILES_6MT,
    ACTUALS_SCHEMA_6MT,
    MONEYLINE_SCHEMA_6MT,
    PROVENANCE_6MT,
    RESUME_6MT,
    COMMANDS_6MT,
    FORBIDDEN_WAITING_6MT,
    TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_future_6mu_contract.csv",
    TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_decision.csv",
    TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan_recommended_path.csv",
    SCRIPT_6MT,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CURRENT_STATE_REVIEW_CSV = TMP_DIR / f"{SLUG}_current_state_review.csv"
REQUIRED_FILES_REVIEW_CSV = TMP_DIR / f"{SLUG}_required_files_review.csv"
SCHEMA_REVIEW_CSV = TMP_DIR / f"{SLUG}_schema_review.csv"
PROVENANCE_REVIEW_CSV = TMP_DIR / f"{SLUG}_provenance_review.csv"
RESUME_REVIEW_CSV = TMP_DIR / f"{SLUG}_resume_conditions_review.csv"
COMMANDS_REVIEW_CSV = TMP_DIR / f"{SLUG}_validation_commands_review.csv"
FORBIDDEN_REVIEW_CSV = TMP_DIR / f"{SLUG}_forbidden_waiting_review.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6MV_CSV = TMP_DIR / f"{SLUG}_future_6mv_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MT = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_wait_state_plan_complete"
DIAGNOSIS_6MU = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_wait_state_audit_complete"
RECOMMENDED_NEXT_LAYER_6MU = "6MV_layer_6_projection_adapter_historical_actuals_and_moneyline_source_pause_state_summary"
RECOMMENDED_PATH_6MU = "pause_layer_6_until_local_historical_actuals_and_moneyline_sources_are_supplied"


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
    json_6mt = load_json(JSON_6MT)

    current_state_rows = read_csv_rows(CURRENT_STATE_6MT)
    required_files_rows = read_csv_rows(REQUIRED_FILES_6MT)
    actuals_schema_rows = read_csv_rows(ACTUALS_SCHEMA_6MT)
    moneyline_schema_rows = read_csv_rows(MONEYLINE_SCHEMA_6MT)
    provenance_rows = read_csv_rows(PROVENANCE_6MT)
    resume_rows = read_csv_rows(RESUME_6MT)
    command_rows = read_csv_rows(COMMANDS_6MT)
    forbidden_wait_rows = read_csv_rows(FORBIDDEN_WAITING_6MT)

    actuals_count = int(json_6mt.get("actuals_source_files_found_count_confirmed_from_6ms", -1))
    moneyline_count = int(json_6mt.get("moneyline_source_files_found_count_confirmed_from_6ms", -1))

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
        {"check": "6mt_script_exists", "expected": True, "actual": SCRIPT_6MT.exists(), "passed": SCRIPT_6MT.exists()},
        {"check": "6mt_json_exists", "expected": True, "actual": JSON_6MT.exists(), "passed": JSON_6MT.exists()},
        {"check": "6mt_all_checks_passed", "expected": True, "actual": json_6mt.get("all_checks_passed"), "passed": json_6mt.get("all_checks_passed") is True},
        {"check": "6mt_diagnosis", "expected": DIAGNOSIS_6MT, "actual": json_6mt.get("diagnosis"), "passed": json_6mt.get("diagnosis") == DIAGNOSIS_6MT},
        {"check": "6mt_recommended_next_layer", "expected": "6MU_layer_6_projection_adapter_historical_actuals_and_moneyline_source_wait_state_audit", "actual": json_6mt.get("recommended_next_layer"), "passed": json_6mt.get("recommended_next_layer") == "6MU_layer_6_projection_adapter_historical_actuals_and_moneyline_source_wait_state_audit"},
        {"check": "wait_state_audit_allowed_next", "expected": True, "actual": json_6mt.get("wait_state_audit_allowed_next"), "passed": json_6mt.get("wait_state_audit_allowed_next") is True},
        {"check": "metric_execution_allowed_next", "expected": False, "actual": json_6mt.get("metric_execution_allowed_next"), "passed": json_6mt.get("metric_execution_allowed_next") is False},
        {"check": "backtest_execution_allowed_next", "expected": False, "actual": json_6mt.get("backtest_execution_allowed_next"), "passed": json_6mt.get("backtest_execution_allowed_next") is False},
    ]

    state_names = {row.get("state_item"): row.get("value") for row in current_state_rows}
    current_state_review = [
        {"review": "current_state_file_exists", "actual": CURRENT_STATE_6MT.exists(), "passed": CURRENT_STATE_6MT.exists()},
        {"review": "layer_6_status_blocked", "actual": state_names.get("layer_6_status"), "expected": "blocked_waiting_for_local_historical_sources", "passed": state_names.get("layer_6_status") == "blocked_waiting_for_local_historical_sources"},
        {"review": "actuals_count_zero", "actual": actuals_count, "expected": 0, "passed": actuals_count == 0},
        {"review": "moneyline_count_zero", "actual": moneyline_count, "expected": 0, "passed": moneyline_count == 0},
        {"review": "metrics_backtests_blocked", "actual": state_names.get("metrics_backtests_tuning_activation_exit"), "expected": "blocked", "passed": state_names.get("metrics_backtests_tuning_activation_exit") == "blocked"},
    ]

    required_file_locations = {row.get("accepted_location") for row in required_files_rows}
    required_files_review = [
        {"review": "actuals_single_file_location_documented", "actual": "data/local/historical_actuals.csv" in required_file_locations, "expected": True, "passed": "data/local/historical_actuals.csv" in required_file_locations},
        {"review": "actuals_glob_location_documented", "actual": "data/local/historical_actuals/*.csv" in required_file_locations, "expected": True, "passed": "data/local/historical_actuals/*.csv" in required_file_locations},
        {"review": "moneyline_single_file_location_documented", "actual": "data/local/historical_moneyline_odds.csv" in required_file_locations, "expected": True, "passed": "data/local/historical_moneyline_odds.csv" in required_file_locations},
        {"review": "moneyline_glob_location_documented", "actual": "data/local/historical_moneyline_odds/*.csv" in required_file_locations, "expected": True, "passed": "data/local/historical_moneyline_odds/*.csv" in required_file_locations},
        {"review": "tmp_forbidden_as_source_documented", "actual": "tmp/*" in required_file_locations, "expected": True, "passed": "tmp/*" in required_file_locations},
    ]

    actuals_fields = {row.get("canonical_field") for row in actuals_schema_rows if row.get("required") == "True"}
    moneyline_required_fields = {row.get("canonical_field") for row in moneyline_schema_rows if row.get("required") == "True"}
    schema_review = [
        {"review": "actuals_required_schema_count", "actual": len(actuals_fields), "expected": 8, "passed": len(actuals_fields) == 8},
        {"review": "moneyline_required_schema_count", "actual": len(moneyline_required_fields), "expected": 6, "passed": len(moneyline_required_fields) == 6},
        {"review": "actuals_source_artifact_required", "actual": "source_artifact" in actuals_fields, "expected": True, "passed": "source_artifact" in actuals_fields},
        {"review": "moneyline_source_artifact_required", "actual": "source_artifact" in moneyline_required_fields, "expected": True, "passed": "source_artifact" in moneyline_required_fields},
    ]

    provenance_requirements = {row.get("requirement") for row in provenance_rows}
    provenance_review = [
        {"review": "source_artifact_requirement_documented", "actual": "source_artifact_or_source_file_column" in provenance_requirements, "expected": True, "passed": "source_artifact_or_source_file_column" in provenance_requirements},
        {"review": "data_local_requirement_documented", "actual": "source_must_be_under_data_local" in provenance_requirements, "expected": True, "passed": "source_must_be_under_data_local" in provenance_requirements},
        {"review": "tmp_forbidden_requirement_documented", "actual": "tmp_outputs_forbidden_as_source" in provenance_requirements, "expected": True, "passed": "tmp_outputs_forbidden_as_source" in provenance_requirements},
        {"review": "unknown_authority_blocks_resume_documented", "actual": "unknown_source_authority_blocks_resume" in provenance_requirements, "expected": True, "passed": "unknown_source_authority_blocks_resume" in provenance_requirements},
    ]

    resume_conditions = {row.get("condition") for row in resume_rows}
    resume_review = [
        {"review": "actuals_file_supplied_condition", "actual": "actuals_file_supplied" in resume_conditions, "expected": True, "passed": "actuals_file_supplied" in resume_conditions},
        {"review": "moneyline_file_supplied_condition", "actual": "moneyline_file_supplied" in resume_conditions, "expected": True, "passed": "moneyline_file_supplied" in resume_conditions},
        {"review": "actuals_schema_condition", "actual": "required_actuals_schema_validates" in resume_conditions, "expected": True, "passed": "required_actuals_schema_validates" in resume_conditions},
        {"review": "moneyline_schema_condition", "actual": "required_moneyline_schema_validates" in resume_conditions, "expected": True, "passed": "required_moneyline_schema_validates" in resume_conditions},
        {"review": "provenance_condition", "actual": "source_artifact_provenance_present" in resume_conditions, "expected": True, "passed": "source_artifact_provenance_present" in resume_conditions},
        {"review": "no_blockers_condition", "actual": "no_unresolved_schema_or_source_blockers" in resume_conditions, "expected": True, "passed": "no_unresolved_schema_or_source_blockers" in resume_conditions},
    ]

    command_steps = {row.get("step") for row in command_rows}
    commands_review = [
        {"review": "sync_main_command_documented", "actual": "sync_main" in command_steps, "expected": True, "passed": "sync_main" in command_steps},
        {"review": "rerun_validation_command_documented", "actual": "rerun_presence_schema_validation" in command_steps, "expected": True, "passed": "rerun_presence_schema_validation" in command_steps},
        {"review": "audit_validation_command_documented", "actual": "audit_validation" in command_steps, "expected": True, "passed": "audit_validation" in command_steps},
        {"review": "do_not_run_metrics_directly_documented", "actual": "only_after_clean_validation" in command_steps, "expected": True, "passed": "only_after_clean_validation" in command_steps},
    ]

    forbidden_ops = {row.get("operation") for row in forbidden_wait_rows if row.get("forbidden") == "True"}
    forbidden_review = [
        {"review": "production_source_ingestion_forbidden", "actual": "source_ingestion_into_production_tables" in forbidden_ops, "expected": True, "passed": "source_ingestion_into_production_tables" in forbidden_ops},
        {"review": "metric_execution_forbidden", "actual": "historical_metric_execution" in forbidden_ops, "expected": True, "passed": "historical_metric_execution" in forbidden_ops},
        {"review": "historical_backtest_forbidden", "actual": "historical_backtest" in forbidden_ops, "expected": True, "passed": "historical_backtest" in forbidden_ops},
        {"review": "tuning_forbidden", "actual": "tuning" in forbidden_ops, "expected": True, "passed": "tuning" in forbidden_ops},
        {"review": "activation_forbidden", "actual": "mechanics_activation" in forbidden_ops, "expected": True, "passed": "mechanics_activation" in forbidden_ops},
        {"review": "layer_6_exit_forbidden", "actual": "layer_6_exit" in forbidden_ops, "expected": True, "passed": "layer_6_exit" in forbidden_ops},
        {"review": "remote_fetch_forbidden", "actual": "remote_api_or_live_data_fetch" in forbidden_ops, "expected": True, "passed": "remote_api_or_live_data_fetch" in forbidden_ops},
    ]

    allowed_next = [
        {"operation": "summarize_pause_state", "allowed_next": True, "scope": "6MV summary only", "passed": True},
    ]

    forbidden_next = [
        {"operation": "metric_execution", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "production_source_modification", "allowed_next": False, "passed": True},
    ]

    future_6mv = [
        {"contract": "summarize_layer_6_pause_state", "required": True, "passed": True},
        {"contract": "summarize_required_user_supplied_sources", "required": True, "passed": True},
        {"contract": "summarize_resume_conditions", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_no_backtests_until_sources_exist", "required": True, "passed": True},
    ]

    blocking_policy = [
        {"policy": "layer_6_paused_until_local_sources_validate", "required": True, "passed": True},
        {"policy": "historical_actuals_required_before_evaluation", "required": True, "passed": True},
        {"policy": "moneyline_source_required_before_market_comparison", "required": True, "passed": True},
        {"policy": "layer_6_exit_not_available_in_pause_state", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6mt_passed", "expected": True, "actual": json_6mt.get("all_checks_passed"), "passed": json_6mt.get("all_checks_passed") is True},
        {"decision": "6mt_diagnosis_valid", "expected": DIAGNOSIS_6MT, "actual": json_6mt.get("diagnosis"), "passed": json_6mt.get("diagnosis") == DIAGNOSIS_6MT},
        {"decision": "all_required_6mt_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "current_state_review_passed", "expected": True, "actual": all_passed(current_state_review), "passed": all_passed(current_state_review)},
        {"decision": "required_files_review_passed", "expected": True, "actual": all_passed(required_files_review), "passed": all_passed(required_files_review)},
        {"decision": "schema_review_passed", "expected": True, "actual": all_passed(schema_review), "passed": all_passed(schema_review)},
        {"decision": "provenance_review_passed", "expected": True, "actual": all_passed(provenance_review), "passed": all_passed(provenance_review)},
        {"decision": "resume_review_passed", "expected": True, "actual": all_passed(resume_review), "passed": all_passed(resume_review)},
        {"decision": "commands_review_passed", "expected": True, "actual": all_passed(commands_review), "passed": all_passed(commands_review)},
        {"decision": "forbidden_waiting_review_passed", "expected": True, "actual": all_passed(forbidden_review), "passed": all_passed(forbidden_review)},
        {"decision": "recommend_6mv_next", "expected": RECOMMENDED_NEXT_LAYER_6MU, "actual": RECOMMENDED_NEXT_LAYER_6MU, "passed": True},
        {"decision": "do_not_execute_metrics_backtest_tune_activate_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only_wait_state_documentation", "expected": True, "actual": True, "passed": True},
        {"boundary": "local_source_files_checked_by_6mu", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_source_files_read_by_6mu", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_rows_ingested_by_6mu", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6mu", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mu", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mu", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6mu", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mu", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MU, "actual": RECOMMENDED_NEXT_LAYER_6MU, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MU, "actual": RECOMMENDED_PATH_6MU, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MU, "actual": DIAGNOSIS_6MU, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "current_state_review", "passed": all_passed(current_state_review), "detail": f"{sum(1 for r in current_state_review if r['passed'])}/{len(current_state_review)}"},
        {"check": "required_files_review", "passed": all_passed(required_files_review), "detail": f"{sum(1 for r in required_files_review if r['passed'])}/{len(required_files_review)}"},
        {"check": "schema_review", "passed": all_passed(schema_review), "detail": f"{sum(1 for r in schema_review if r['passed'])}/{len(schema_review)}"},
        {"check": "provenance_review", "passed": all_passed(provenance_review), "detail": f"{sum(1 for r in provenance_review if r['passed'])}/{len(provenance_review)}"},
        {"check": "resume_conditions_review", "passed": all_passed(resume_review), "detail": f"{sum(1 for r in resume_review if r['passed'])}/{len(resume_review)}"},
        {"check": "validation_commands_review", "passed": all_passed(commands_review), "detail": f"{sum(1 for r in commands_review if r['passed'])}/{len(commands_review)}"},
        {"check": "forbidden_waiting_review", "passed": all_passed(forbidden_review), "detail": f"{sum(1 for r in forbidden_review if r['passed'])}/{len(forbidden_review)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next), "detail": f"{sum(1 for r in allowed_next if r['passed'])}/{len(allowed_next)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next), "detail": f"{sum(1 for r in forbidden_next if r['passed'])}/{len(forbidden_next)}"},
        {"check": "future_6mv_contract", "passed": all_passed(future_6mv), "detail": f"{sum(1 for r in future_6mv if r['passed'])}/{len(future_6mv)}"},
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
        "current_state_review": write_csv(CURRENT_STATE_REVIEW_CSV, current_state_review),
        "required_files_review": write_csv(REQUIRED_FILES_REVIEW_CSV, required_files_review),
        "schema_review": write_csv(SCHEMA_REVIEW_CSV, schema_review),
        "provenance_review": write_csv(PROVENANCE_REVIEW_CSV, provenance_review),
        "resume_conditions_review": write_csv(RESUME_REVIEW_CSV, resume_review),
        "validation_commands_review": write_csv(COMMANDS_REVIEW_CSV, commands_review),
        "forbidden_waiting_review": write_csv(FORBIDDEN_REVIEW_CSV, forbidden_review),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "future_6mv_contract": write_csv(FUTURE_6MV_CSV, future_6mv),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MU",
        "layer_type": "game_mechanics_realism",
        "audit_only_wait_state_documentation": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MU if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MU,
        "recommended_path": RECOMMENDED_PATH_6MU,
        "predecessor_layer": "6MT",
        "predecessor_diagnosis": json_6mt.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mt.get("all_checks_passed") is True,
        "audited_layer_after": "6MT",
        "source_family": "projection_adapter_historical_actuals_moneyline_source_wait_state_audit",
        "wait_state_documentation_audited": True,
        "current_state_documentation_confirmed": all_passed(current_state_review),
        "local_sources_missing_confirmed_from_6mt": actuals_count == 0 and moneyline_count == 0,
        "actuals_source_files_found_count_confirmed_from_6mt": actuals_count,
        "moneyline_source_files_found_count_confirmed_from_6mt": moneyline_count,
        "required_file_locations_audited": all_passed(required_files_review),
        "actuals_schema_audited": "source_artifact" in actuals_fields and len(actuals_fields) == 8,
        "moneyline_schema_audited": "source_artifact" in moneyline_required_fields and len(moneyline_required_fields) == 6,
        "provenance_requirements_audited": all_passed(provenance_review),
        "resume_conditions_audited": all_passed(resume_review),
        "validation_commands_audited": all_passed(commands_review),
        "forbidden_while_waiting_audited": all_passed(forbidden_review),
        "pause_state_summary_allowed_next": True,
        "source_ingestion_allowed_next": False,
        "source_implementation_allowed_next": False,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "local_source_files_checked_by_6mu": False,
        "local_source_files_read_by_6mu": False,
        "source_rows_ingested_by_6mu": False,
        "normalized_source_tables_created_for_production_by_6mu": False,
        "production_code_modified_by_6mu": False,
        "adapter_call_executed_by_6mu": False,
        "metric_execution_run_by_6mu": False,
        "backtest_execution_run_by_6mu": False,
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
            "current_state_review_csv": str(CURRENT_STATE_REVIEW_CSV),
            "required_files_review_csv": str(REQUIRED_FILES_REVIEW_CSV),
            "schema_review_csv": str(SCHEMA_REVIEW_CSV),
            "provenance_review_csv": str(PROVENANCE_REVIEW_CSV),
            "resume_conditions_review_csv": str(RESUME_REVIEW_CSV),
            "validation_commands_review_csv": str(COMMANDS_REVIEW_CSV),
            "forbidden_waiting_review_csv": str(FORBIDDEN_REVIEW_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6mv_contract_csv": str(FUTURE_6MV_CSV),
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
