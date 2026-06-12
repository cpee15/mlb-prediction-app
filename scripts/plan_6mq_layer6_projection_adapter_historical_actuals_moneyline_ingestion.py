#!/usr/bin/env python3
"""Plan local historical actuals and moneyline ingestion validation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan"
TMP_DIR = Path("tmp")

SCRIPT_6MP = Path("scripts/audit_6mp_layer6_projection_adapter_historical_actuals_moneyline_remediation.py")
JSON_6MP = TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit.json"

REQUIRED_INPUTS = [
    JSON_6MP,
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_checks.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_predecessor.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_contract_review.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_file_drop_review.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_schema_review.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_provenance_review.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_validation_quality_review.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_policy_review.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_precondition_review.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_allowed_operations_next.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_blockers.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_future_6mq_contract.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_decision.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit_recommended_path.csv",
    SCRIPT_6MP,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
FILE_PRESENCE_PLAN_CSV = TMP_DIR / f"{SLUG}_file_presence_plan.csv"
SCHEMA_VALIDATION_PLAN_CSV = TMP_DIR / f"{SLUG}_schema_validation_plan.csv"
ALIAS_NORMALIZATION_PLAN_CSV = TMP_DIR / f"{SLUG}_alias_normalization_plan.csv"
PROVENANCE_VALIDATION_PLAN_CSV = TMP_DIR / f"{SLUG}_provenance_validation_plan.csv"
SOURCE_AUTHORITY_VALIDATION_PLAN_CSV = TMP_DIR / f"{SLUG}_source_authority_validation_plan.csv"
DUPLICATE_RESOLUTION_PLAN_CSV = TMP_DIR / f"{SLUG}_duplicate_resolution_plan.csv"
MISSING_FIELD_VALIDATION_PLAN_CSV = TMP_DIR / f"{SLUG}_missing_field_validation_plan.csv"
OUTPUT_ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_output_artifact_contract.csv"
FAIL_CLOSED_POLICY_CSV = TMP_DIR / f"{SLUG}_fail_closed_policy.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6MR_CSV = TMP_DIR / f"{SLUG}_future_6mr_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MP = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_remediation_audit_complete"
DIAGNOSIS_6MQ = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_ingestion_plan_complete"
RECOMMENDED_NEXT_LAYER_6MQ = "6MR_layer_6_projection_adapter_historical_actuals_and_moneyline_source_ingestion_implementation"
RECOMMENDED_PATH_6MQ = "implement_local_source_presence_and_schema_validation_without_metric_execution"


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
    json_6mp = load_json(JSON_6MP)

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
        {"check": "6mp_script_exists", "expected": True, "actual": SCRIPT_6MP.exists(), "passed": SCRIPT_6MP.exists()},
        {"check": "6mp_json_exists", "expected": True, "actual": JSON_6MP.exists(), "passed": JSON_6MP.exists()},
        {"check": "6mp_all_checks_passed", "expected": True, "actual": json_6mp.get("all_checks_passed"), "passed": json_6mp.get("all_checks_passed") is True},
        {"check": "6mp_diagnosis", "expected": DIAGNOSIS_6MP, "actual": json_6mp.get("diagnosis"), "passed": json_6mp.get("diagnosis") == DIAGNOSIS_6MP},
        {"check": "6mp_recommended_next_layer", "expected": "6MQ_layer_6_projection_adapter_historical_actuals_and_moneyline_source_ingestion_plan", "actual": json_6mp.get("recommended_next_layer"), "passed": json_6mp.get("recommended_next_layer") == "6MQ_layer_6_projection_adapter_historical_actuals_and_moneyline_source_ingestion_plan"},
        {"check": "contract_valid_for_ingestion_planning", "expected": True, "actual": json_6mp.get("contract_valid_for_ingestion_planning"), "passed": json_6mp.get("contract_valid_for_ingestion_planning") is True},
        {"check": "source_ingestion_allowed_next", "expected": False, "actual": json_6mp.get("source_ingestion_allowed_next"), "passed": json_6mp.get("source_ingestion_allowed_next") is False},
        {"check": "metric_execution_allowed_next", "expected": False, "actual": json_6mp.get("metric_execution_allowed_next"), "passed": json_6mp.get("metric_execution_allowed_next") is False},
    ]

    file_presence_plan = [
        {"source_type": "actuals", "candidate_location": "data/local/historical_actuals.csv", "future_check": "path_exists", "required_before_ingestion": True, "run_by_6mq": False, "passed": True},
        {"source_type": "actuals", "candidate_location": "data/local/historical_actuals/*.csv", "future_check": "glob_non_empty", "required_before_ingestion": True, "run_by_6mq": False, "passed": True},
        {"source_type": "moneyline", "candidate_location": "data/local/historical_moneyline_odds.csv", "future_check": "path_exists", "required_before_ingestion": True, "run_by_6mq": False, "passed": True},
        {"source_type": "moneyline", "candidate_location": "data/local/historical_moneyline_odds/*.csv", "future_check": "glob_non_empty", "required_before_ingestion": True, "run_by_6mq": False, "passed": True},
        {"source_type": "generated_tmp", "candidate_location": "tmp/*", "future_check": "excluded_as_source", "required_before_ingestion": False, "run_by_6mq": False, "passed": True},
    ]

    schema_validation_plan = [
        {"source_type": "actuals", "canonical_field": "game_pk", "required": True, "validation": "non_empty", "blocks_if_missing": True, "passed": True},
        {"source_type": "actuals", "canonical_field": "game_date", "required": True, "validation": "parseable_date", "blocks_if_missing": True, "passed": True},
        {"source_type": "actuals", "canonical_field": "home_team", "required": True, "validation": "non_empty", "blocks_if_missing": True, "passed": True},
        {"source_type": "actuals", "canonical_field": "away_team", "required": True, "validation": "non_empty", "blocks_if_missing": True, "passed": True},
        {"source_type": "actuals", "canonical_field": "home_score", "required": True, "validation": "integer_ge_0", "blocks_if_missing": True, "passed": True},
        {"source_type": "actuals", "canonical_field": "away_score", "required": True, "validation": "integer_ge_0", "blocks_if_missing": True, "passed": True},
        {"source_type": "actuals", "canonical_field": "home_win_binary", "required": True, "validation": "0_or_1_and_score_consistent", "blocks_if_missing": True, "passed": True},
        {"source_type": "actuals", "canonical_field": "source_artifact", "required": True, "validation": "non_empty", "blocks_if_missing": True, "passed": True},
        {"source_type": "moneyline", "canonical_field": "game_pk", "required": True, "validation": "non_empty", "blocks_if_missing": True, "passed": True},
        {"source_type": "moneyline", "canonical_field": "game_date", "required": True, "validation": "parseable_date", "blocks_if_missing": True, "passed": True},
        {"source_type": "moneyline", "canonical_field": "home_team", "required": True, "validation": "non_empty", "blocks_if_missing": True, "passed": True},
        {"source_type": "moneyline", "canonical_field": "away_team", "required": True, "validation": "non_empty", "blocks_if_missing": True, "passed": True},
        {"source_type": "moneyline", "canonical_field": "home_moneyline", "required": True, "validation": "numeric_non_zero_american_odds", "blocks_if_missing": True, "passed": True},
        {"source_type": "moneyline", "canonical_field": "source_artifact", "required": True, "validation": "non_empty", "blocks_if_missing": True, "passed": True},
        {"source_type": "moneyline", "canonical_field": "away_moneyline", "required": False, "validation": "numeric_non_zero_if_present", "blocks_if_missing": False, "passed": True},
        {"source_type": "moneyline", "canonical_field": "odds_timestamp_or_type", "required": False, "validation": "preserve_if_present", "blocks_if_missing": False, "passed": True},
        {"source_type": "moneyline", "canonical_field": "sportsbook_or_source", "required": False, "validation": "preserve_if_present", "blocks_if_missing": False, "passed": True},
    ]

    alias_normalization_plan = [
        {"source_type": "actuals", "canonical_field": "game_pk", "aliases": "game_id,mlb_game_id,event_id", "future_action": "normalize_to_game_pk", "passed": True},
        {"source_type": "actuals", "canonical_field": "game_date", "aliases": "date,official_date", "future_action": "normalize_to_game_date", "passed": True},
        {"source_type": "actuals", "canonical_field": "home_score", "aliases": "home_runs,home_final_score", "future_action": "normalize_to_home_score", "passed": True},
        {"source_type": "actuals", "canonical_field": "away_score", "aliases": "away_runs,away_final_score", "future_action": "normalize_to_away_score", "passed": True},
        {"source_type": "moneyline", "canonical_field": "home_moneyline", "aliases": "home_ml,moneyline_home,home_close_moneyline", "future_action": "normalize_to_home_moneyline", "passed": True},
        {"source_type": "moneyline", "canonical_field": "away_moneyline", "aliases": "away_ml,moneyline_away,away_close_moneyline", "future_action": "normalize_to_away_moneyline_if_present", "passed": True},
        {"source_type": "both", "canonical_field": "source_artifact", "aliases": "source_file,provenance", "future_action": "normalize_to_source_artifact", "passed": True},
    ]

    provenance_validation_plan = [
        {"check": "source_artifact_present", "future_behavior": "block_if_missing", "run_by_6mq": False, "passed": True},
        {"check": "source_artifact_not_tmp_generated", "future_behavior": "block_if_tmp_generated_source", "run_by_6mq": False, "passed": True},
        {"check": "source_rows_carry_original_file_path", "future_behavior": "preserve_in_output_artifact", "run_by_6mq": False, "passed": True},
    ]

    source_authority_validation_plan = [
        {"check": "local_user_provided_path_under_data_local", "future_behavior": "allow_if_schema_valid", "run_by_6mq": False, "passed": True},
        {"check": "tmp_artifact_source", "future_behavior": "block_as_source_authority", "run_by_6mq": False, "passed": True},
        {"check": "unknown_source_authority", "future_behavior": "block_until_declared", "run_by_6mq": False, "passed": True},
    ]

    duplicate_resolution_plan = [
        {"source_type": "actuals", "key": "game_pk", "future_behavior": "allow_identical_duplicates_else_block", "run_by_6mq": False, "passed": True},
        {"source_type": "moneyline", "key": "game_pk+sportsbook_or_source+odds_timestamp_or_type", "future_behavior": "preserve_distinct_snapshots", "run_by_6mq": False, "passed": True},
        {"source_type": "moneyline", "key": "game_pk without snapshot/source", "future_behavior": "block_multiple_rows_until_selection_policy", "run_by_6mq": False, "passed": True},
    ]

    missing_field_validation_plan = [
        {"source_type": "actuals", "missing_field_type": "required", "future_behavior": "block_ingestion", "run_by_6mq": False, "passed": True},
        {"source_type": "moneyline", "missing_field_type": "required", "future_behavior": "block_ingestion", "run_by_6mq": False, "passed": True},
        {"source_type": "moneyline", "missing_field_type": "away_moneyline_optional", "future_behavior": "allow_raw_home_market_only_block_devig", "run_by_6mq": False, "passed": True},
        {"source_type": "both", "missing_field_type": "source_artifact", "future_behavior": "block_ingestion", "run_by_6mq": False, "passed": True},
    ]

    output_artifact_contract = [
        {"artifact": "actuals_presence_report_csv", "future_path": "tmp/layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_actuals_presence.csv", "required": True, "created_by_6mq": False, "passed": True},
        {"artifact": "moneyline_presence_report_csv", "future_path": "tmp/layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_moneyline_presence.csv", "required": True, "created_by_6mq": False, "passed": True},
        {"artifact": "actuals_schema_validation_csv", "future_path": "tmp/layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_actuals_schema_validation.csv", "required": True, "created_by_6mq": False, "passed": True},
        {"artifact": "moneyline_schema_validation_csv", "future_path": "tmp/layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_moneyline_schema_validation.csv", "required": True, "created_by_6mq": False, "passed": True},
        {"artifact": "normalized_source_preview_csv", "future_path": "tmp/layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_normalized_preview.csv", "required": False, "created_by_6mq": False, "passed": True},
        {"artifact": "source_blockers_csv", "future_path": "tmp/layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_blockers.csv", "required": True, "created_by_6mq": False, "passed": True},
    ]

    fail_closed_policy = [
        {"condition": "no_local_actuals_source_file", "future_behavior": "emit_blocker_and_do_not_ingest_actuals", "passed": True},
        {"condition": "no_local_moneyline_source_file", "future_behavior": "emit_blocker_and_do_not_ingest_moneyline", "passed": True},
        {"condition": "schema_missing_required_fields", "future_behavior": "emit_blocker_and_do_not_normalize", "passed": True},
        {"condition": "tmp_generated_path_used_as_source", "future_behavior": "emit_blocker_and_do_not_ingest", "passed": True},
        {"condition": "duplicate_unresolved", "future_behavior": "emit_blocker_and_do_not_use_for_metrics", "passed": True},
        {"condition": "source_present_but_no_provenance", "future_behavior": "emit_blocker_and_do_not_ingest", "passed": True},
    ]

    allowed_next = [
        {"operation": "implement_local_file_presence_checks", "allowed_next": True, "scope": "6MR only; no metrics", "passed": True},
        {"operation": "implement_schema_validation_checks", "allowed_next": True, "scope": "6MR only; no metrics", "passed": True},
        {"operation": "implement_alias_normalization_validation", "allowed_next": True, "scope": "6MR only; no metrics", "passed": True},
    ]

    forbidden_next = [
        {"operation": "metric_execution", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "external_source_scan", "allowed_next": False, "passed": True},
        {"operation": "production_source_modification", "allowed_next": False, "passed": True},
    ]

    blockers = [
        {"blocker": "local_source_files_not_checked", "active": True, "reason": "6MQ only plans checks", "passed": True},
        {"blocker": "local_source_rows_not_ingested", "active": True, "reason": "6MQ does not read files", "passed": True},
        {"blocker": "source_validation_not_implemented", "active": True, "reason": "6MR must implement validation", "passed": True},
        {"blocker": "metrics_backtests_tuning_activation_exit_blocked", "active": True, "reason": "requires implemented and audited source validation", "passed": True},
    ]

    future_6mr = [
        {"contract": "implement_presence_check_for_actuals_and_moneyline_locations", "required": True, "passed": True},
        {"contract": "implement_schema_validation_without_metrics", "required": True, "passed": True},
        {"contract": "emit_blockers_when_sources_missing_or_invalid", "required": True, "passed": True},
        {"contract": "do_not_run_backtest_or_metrics", "required": True, "passed": True},
    ]

    blocking_policy = [
        {"policy": "do_not_read_local_source_files_in_6mq", "required": True, "passed": True},
        {"policy": "do_not_ingest_source_rows_in_6mq", "required": True, "passed": True},
        {"policy": "do_not_create_normalized_source_tables_in_6mq", "required": True, "passed": True},
        {"policy": "do_not_generate_fake_actual_outcomes", "required": True, "passed": True},
        {"policy": "do_not_generate_fake_moneyline_odds", "required": True, "passed": True},
        {"policy": "do_not_execute_metrics_without_implemented_and_audited_sources", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6mp_passed", "expected": True, "actual": json_6mp.get("all_checks_passed"), "passed": json_6mp.get("all_checks_passed") is True},
        {"decision": "6mp_diagnosis_valid", "expected": DIAGNOSIS_6MP, "actual": json_6mp.get("diagnosis"), "passed": json_6mp.get("diagnosis") == DIAGNOSIS_6MP},
        {"decision": "all_required_6mp_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "contract_valid_for_ingestion_planning_confirmed", "expected": True, "actual": json_6mp.get("contract_valid_for_ingestion_planning"), "passed": json_6mp.get("contract_valid_for_ingestion_planning") is True},
        {"decision": "file_presence_plan_created", "expected": True, "actual": True, "passed": all_passed(file_presence_plan)},
        {"decision": "schema_validation_plan_created", "expected": True, "actual": True, "passed": all_passed(schema_validation_plan)},
        {"decision": "alias_normalization_plan_created", "expected": True, "actual": True, "passed": all_passed(alias_normalization_plan)},
        {"decision": "provenance_validation_plan_created", "expected": True, "actual": True, "passed": all_passed(provenance_validation_plan)},
        {"decision": "source_authority_validation_plan_created", "expected": True, "actual": True, "passed": all_passed(source_authority_validation_plan)},
        {"decision": "duplicate_resolution_plan_created", "expected": True, "actual": True, "passed": all_passed(duplicate_resolution_plan)},
        {"decision": "missing_field_validation_plan_created", "expected": True, "actual": True, "passed": all_passed(missing_field_validation_plan)},
        {"decision": "output_artifact_contract_created", "expected": True, "actual": True, "passed": all_passed(output_artifact_contract)},
        {"decision": "fail_closed_policy_created", "expected": True, "actual": True, "passed": all_passed(fail_closed_policy)},
        {"decision": "recommend_6mr_next", "expected": RECOMMENDED_NEXT_LAYER_6MQ, "actual": RECOMMENDED_NEXT_LAYER_6MQ, "passed": True},
        {"decision": "do_not_read_ingest_implement_metrics_backtest_tune_activate_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only_local_source_ingestion_validation", "expected": True, "actual": True, "passed": True},
        {"boundary": "local_source_files_checked_by_6mq", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_source_files_read_by_6mq", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_rows_ingested_by_6mq", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_by_6mq", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_data_created_by_6mq", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_acquisition_performed_by_6mq", "expected": False, "actual": False, "passed": True},
        {"boundary": "external_source_scan_run_by_6mq", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_source_scan_run_by_6mq", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_ingestion_run_by_6mq", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_implementation_run_by_6mq", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6mq", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mq", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mq", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mq", "expected": False, "actual": False, "passed": True},
        {"boundary": "full_batch_adapter_call_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_historical_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_measurement_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "database_writes_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "live_data_fetches_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "remote_api_calls_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_source_modifications_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "activation_execution_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MQ, "actual": RECOMMENDED_NEXT_LAYER_6MQ, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MQ, "actual": RECOMMENDED_PATH_6MQ, "passed": True},
        {"decision": "allow_ingestion_validation_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MQ, "actual": DIAGNOSIS_6MQ, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "file_presence_plan", "passed": all_passed(file_presence_plan), "detail": f"{sum(1 for r in file_presence_plan if r['passed'])}/{len(file_presence_plan)}"},
        {"check": "schema_validation_plan", "passed": all_passed(schema_validation_plan), "detail": f"{sum(1 for r in schema_validation_plan if r['passed'])}/{len(schema_validation_plan)}"},
        {"check": "alias_normalization_plan", "passed": all_passed(alias_normalization_plan), "detail": f"{sum(1 for r in alias_normalization_plan if r['passed'])}/{len(alias_normalization_plan)}"},
        {"check": "provenance_validation_plan", "passed": all_passed(provenance_validation_plan), "detail": f"{sum(1 for r in provenance_validation_plan if r['passed'])}/{len(provenance_validation_plan)}"},
        {"check": "source_authority_validation_plan", "passed": all_passed(source_authority_validation_plan), "detail": f"{sum(1 for r in source_authority_validation_plan if r['passed'])}/{len(source_authority_validation_plan)}"},
        {"check": "duplicate_resolution_plan", "passed": all_passed(duplicate_resolution_plan), "detail": f"{sum(1 for r in duplicate_resolution_plan if r['passed'])}/{len(duplicate_resolution_plan)}"},
        {"check": "missing_field_validation_plan", "passed": all_passed(missing_field_validation_plan), "detail": f"{sum(1 for r in missing_field_validation_plan if r['passed'])}/{len(missing_field_validation_plan)}"},
        {"check": "output_artifact_contract", "passed": all_passed(output_artifact_contract), "detail": f"{sum(1 for r in output_artifact_contract if r['passed'])}/{len(output_artifact_contract)}"},
        {"check": "fail_closed_policy", "passed": all_passed(fail_closed_policy), "detail": f"{sum(1 for r in fail_closed_policy if r['passed'])}/{len(fail_closed_policy)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next), "detail": f"{sum(1 for r in allowed_next if r['passed'])}/{len(allowed_next)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next), "detail": f"{sum(1 for r in forbidden_next if r['passed'])}/{len(forbidden_next)}"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "future_6mr_contract", "passed": all_passed(future_6mr), "detail": f"{sum(1 for r in future_6mr if r['passed'])}/{len(future_6mr)}"},
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
        "file_presence_plan": write_csv(FILE_PRESENCE_PLAN_CSV, file_presence_plan),
        "schema_validation_plan": write_csv(SCHEMA_VALIDATION_PLAN_CSV, schema_validation_plan),
        "alias_normalization_plan": write_csv(ALIAS_NORMALIZATION_PLAN_CSV, alias_normalization_plan),
        "provenance_validation_plan": write_csv(PROVENANCE_VALIDATION_PLAN_CSV, provenance_validation_plan),
        "source_authority_validation_plan": write_csv(SOURCE_AUTHORITY_VALIDATION_PLAN_CSV, source_authority_validation_plan),
        "duplicate_resolution_plan": write_csv(DUPLICATE_RESOLUTION_PLAN_CSV, duplicate_resolution_plan),
        "missing_field_validation_plan": write_csv(MISSING_FIELD_VALIDATION_PLAN_CSV, missing_field_validation_plan),
        "output_artifact_contract": write_csv(OUTPUT_ARTIFACT_CONTRACT_CSV, output_artifact_contract),
        "fail_closed_policy": write_csv(FAIL_CLOSED_POLICY_CSV, fail_closed_policy),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6mr_contract": write_csv(FUTURE_6MR_CSV, future_6mr),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MQ",
        "layer_type": "game_mechanics_realism",
        "planning_only_local_source_ingestion_validation": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MQ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MQ,
        "recommended_path": RECOMMENDED_PATH_6MQ,
        "predecessor_layer": "6MP",
        "predecessor_diagnosis": json_6mp.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mp.get("all_checks_passed") is True,
        "planned_layer_after": "6MP",
        "source_family": "projection_adapter_historical_actuals_moneyline_source_ingestion_plan",
        "contract_valid_for_ingestion_planning_confirmed": json_6mp.get("contract_valid_for_ingestion_planning") is True,
        "file_presence_plan_created": True,
        "schema_validation_plan_created": True,
        "alias_normalization_plan_created": True,
        "provenance_validation_plan_created": True,
        "source_authority_validation_plan_created": True,
        "duplicate_resolution_plan_created": True,
        "missing_field_validation_plan_created": True,
        "output_artifact_contract_created": True,
        "fail_closed_policy_created": True,
        "ingestion_implementation_allowed_next": True,
        "source_ingestion_allowed_next": False,
        "source_implementation_allowed_next": False,
        "data_acquisition_allowed_next": False,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "local_source_files_checked_by_6mq": False,
        "local_source_files_read_by_6mq": False,
        "source_rows_ingested_by_6mq": False,
        "normalized_source_tables_created_by_6mq": False,
        "source_data_created_by_6mq": False,
        "source_acquisition_performed_by_6mq": False,
        "external_source_scan_run_by_6mq": False,
        "local_source_scan_run_by_6mq": False,
        "source_ingestion_run_by_6mq": False,
        "source_implementation_run_by_6mq": False,
        "metric_execution_run_by_6mq": False,
        "backtest_execution_run_by_6mq": False,
        "adapter_call_executed_by_6mq": False,
        "production_code_modified_by_6mq": False,
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
        "production_source_modifications_run": False,
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "file_presence_plan_csv": str(FILE_PRESENCE_PLAN_CSV),
            "schema_validation_plan_csv": str(SCHEMA_VALIDATION_PLAN_CSV),
            "alias_normalization_plan_csv": str(ALIAS_NORMALIZATION_PLAN_CSV),
            "provenance_validation_plan_csv": str(PROVENANCE_VALIDATION_PLAN_CSV),
            "source_authority_validation_plan_csv": str(SOURCE_AUTHORITY_VALIDATION_PLAN_CSV),
            "duplicate_resolution_plan_csv": str(DUPLICATE_RESOLUTION_PLAN_CSV),
            "missing_field_validation_plan_csv": str(MISSING_FIELD_VALIDATION_PLAN_CSV),
            "output_artifact_contract_csv": str(OUTPUT_ARTIFACT_CONTRACT_CSV),
            "fail_closed_policy_csv": str(FAIL_CLOSED_POLICY_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6mr_contract_csv": str(FUTURE_6MR_CSV),
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
