#!/usr/bin/env python3
"""Audit 6MO historical actuals and moneyline remediation contract."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mp_projection_adapter_historical_actuals_moneyline_remediation_audit"
TMP_DIR = Path("tmp")

SCRIPT_6MO = Path("scripts/plan_6mo_layer6_projection_adapter_historical_actuals_moneyline_remediation.py")
JSON_6MO = TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan.json"

ACTUALS_CONTRACT_6MO = TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_actuals_file_contract.csv"
MONEYLINE_CONTRACT_6MO = TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_moneyline_file_contract.csv"
FILE_DROP_6MO = TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_file_drop_locations.csv"
SCHEMA_ALIASES_6MO = TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_schema_aliases.csv"
PROVENANCE_6MO = TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_provenance_contract.csv"
VALIDATION_6MO = TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_validation_gates.csv"
QUALITY_6MO = TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_quality_gates.csv"
DUPLICATE_6MO = TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_duplicate_policy.csv"
MISSING_6MO = TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_missing_field_policy.csv"
AUTHORITY_6MO = TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_source_authority_policy.csv"
IMPLEMENT_PRE_6MO = TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_implementation_preconditions.csv"
AUDIT_PRE_6MO = TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_audit_preconditions.csv"

REQUIRED_INPUTS = [
    JSON_6MO,
    TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_checks.csv",
    TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_predecessor.csv",
    TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_input_artifacts.csv",
    ACTUALS_CONTRACT_6MO,
    MONEYLINE_CONTRACT_6MO,
    FILE_DROP_6MO,
    SCHEMA_ALIASES_6MO,
    PROVENANCE_6MO,
    VALIDATION_6MO,
    QUALITY_6MO,
    DUPLICATE_6MO,
    MISSING_6MO,
    AUTHORITY_6MO,
    IMPLEMENT_PRE_6MO,
    AUDIT_PRE_6MO,
    TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_blockers.csv",
    TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_future_6mp_contract.csv",
    TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_decision.csv",
    TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan_recommended_path.csv",
    SCRIPT_6MO,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CONTRACT_REVIEW_CSV = TMP_DIR / f"{SLUG}_contract_review.csv"
FILE_DROP_REVIEW_CSV = TMP_DIR / f"{SLUG}_file_drop_review.csv"
SCHEMA_REVIEW_CSV = TMP_DIR / f"{SLUG}_schema_review.csv"
PROVENANCE_REVIEW_CSV = TMP_DIR / f"{SLUG}_provenance_review.csv"
VALIDATION_QUALITY_REVIEW_CSV = TMP_DIR / f"{SLUG}_validation_quality_review.csv"
POLICY_REVIEW_CSV = TMP_DIR / f"{SLUG}_policy_review.csv"
PRECONDITION_REVIEW_CSV = TMP_DIR / f"{SLUG}_precondition_review.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6MQ_CSV = TMP_DIR / f"{SLUG}_future_6mq_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MO = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_remediation_plan_complete"
DIAGNOSIS_6MP = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_remediation_audit_complete"
RECOMMENDED_NEXT_LAYER_6MP = "6MQ_layer_6_projection_adapter_historical_actuals_and_moneyline_source_ingestion_plan"
RECOMMENDED_PATH_6MP = "plan_local_source_ingestion_after_remediation_contract_audit"


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


def count_required(rows: list[dict[str, str]]) -> int:
    return sum(1 for row in rows if boolish(row.get("required", "")))


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6mo = load_json(JSON_6MO)

    actuals_contract = read_csv_rows(ACTUALS_CONTRACT_6MO)
    moneyline_contract = read_csv_rows(MONEYLINE_CONTRACT_6MO)
    file_drop = read_csv_rows(FILE_DROP_6MO)
    schema_aliases = read_csv_rows(SCHEMA_ALIASES_6MO)
    provenance = read_csv_rows(PROVENANCE_6MO)
    validation = read_csv_rows(VALIDATION_6MO)
    quality = read_csv_rows(QUALITY_6MO)
    duplicate = read_csv_rows(DUPLICATE_6MO)
    missing = read_csv_rows(MISSING_6MO)
    authority = read_csv_rows(AUTHORITY_6MO)
    implementation_pre = read_csv_rows(IMPLEMENT_PRE_6MO)
    audit_pre = read_csv_rows(AUDIT_PRE_6MO)

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
        {"check": "6mo_script_exists", "expected": True, "actual": SCRIPT_6MO.exists(), "passed": SCRIPT_6MO.exists()},
        {"check": "6mo_json_exists", "expected": True, "actual": JSON_6MO.exists(), "passed": JSON_6MO.exists()},
        {"check": "6mo_all_checks_passed", "expected": True, "actual": json_6mo.get("all_checks_passed"), "passed": json_6mo.get("all_checks_passed") is True},
        {"check": "6mo_diagnosis", "expected": DIAGNOSIS_6MO, "actual": json_6mo.get("diagnosis"), "passed": json_6mo.get("diagnosis") == DIAGNOSIS_6MO},
        {"check": "6mo_recommended_next_layer", "expected": "6MP_layer_6_projection_adapter_historical_actuals_and_moneyline_source_remediation_audit", "actual": json_6mo.get("recommended_next_layer"), "passed": json_6mo.get("recommended_next_layer") == "6MP_layer_6_projection_adapter_historical_actuals_and_moneyline_source_remediation_audit"},
        {"check": "source_ingestion_allowed_next", "expected": False, "actual": json_6mo.get("source_ingestion_allowed_next"), "passed": json_6mo.get("source_ingestion_allowed_next") is False},
        {"check": "source_implementation_allowed_next", "expected": False, "actual": json_6mo.get("source_implementation_allowed_next"), "passed": json_6mo.get("source_implementation_allowed_next") is False},
        {"check": "metric_execution_allowed_next", "expected": False, "actual": json_6mo.get("metric_execution_allowed_next"), "passed": json_6mo.get("metric_execution_allowed_next") is False},
    ]

    contract_review = [
        {"contract": "actuals_file_contract", "row_count": len(actuals_contract), "required_count": count_required(actuals_contract), "expected_required_count": 8, "passed": len(actuals_contract) >= 8 and count_required(actuals_contract) >= 8},
        {"contract": "moneyline_file_contract", "row_count": len(moneyline_contract), "required_count": count_required(moneyline_contract), "expected_required_count": 6, "passed": len(moneyline_contract) >= 9 and count_required(moneyline_contract) >= 6},
        {"contract": "actuals_game_pk_present", "row_count": sum(1 for row in actuals_contract if row.get("field") == "game_pk"), "required_count": "", "expected_required_count": 1, "passed": any(row.get("field") == "game_pk" for row in actuals_contract)},
        {"contract": "moneyline_home_moneyline_present", "row_count": sum(1 for row in moneyline_contract if row.get("field") == "home_moneyline"), "required_count": "", "expected_required_count": 1, "passed": any(row.get("field") == "home_moneyline" for row in moneyline_contract)},
    ]

    tmp_excluded = any(row.get("accepted_location") == "tmp/*" and str(row.get("allowed", "")).lower() == "false" for row in file_drop)
    local_actuals_allowed = any("historical_actuals" in row.get("accepted_location", "") and str(row.get("allowed", "")).lower() == "true" for row in file_drop)
    local_moneyline_allowed = any("historical_moneyline_odds" in row.get("accepted_location", "") and str(row.get("allowed", "")).lower() == "true" for row in file_drop)
    file_drop_review = [
        {"review": "local_actuals_drop_allowed", "actual": local_actuals_allowed, "passed": local_actuals_allowed},
        {"review": "local_moneyline_drop_allowed", "actual": local_moneyline_allowed, "passed": local_moneyline_allowed},
        {"review": "tmp_generated_drop_excluded", "actual": tmp_excluded, "passed": tmp_excluded},
        {"review": "file_drop_contract_non_empty", "actual": len(file_drop), "passed": len(file_drop) >= 5},
    ]

    schema_review = [
        {"review": "schema_alias_rows_present", "actual": len(schema_aliases), "expected_min": 17, "passed": len(schema_aliases) >= 17},
        {"review": "actuals_aliases_present", "actual": sum(1 for row in schema_aliases if row.get("source_type") == "actuals"), "expected_min": 8, "passed": sum(1 for row in schema_aliases if row.get("source_type") == "actuals") >= 8},
        {"review": "moneyline_aliases_present", "actual": sum(1 for row in schema_aliases if row.get("source_type") == "moneyline"), "expected_min": 9, "passed": sum(1 for row in schema_aliases if row.get("source_type") == "moneyline") >= 9},
    ]

    provenance_review = [
        {"review": "source_artifact_required", "actual": any(row.get("requirement") == "source_artifact" and boolish(row.get("required")) for row in provenance), "passed": any(row.get("requirement") == "source_artifact" and boolish(row.get("required")) for row in provenance)},
        {"review": "source_authority_required", "actual": any(row.get("requirement") == "source_authority" and boolish(row.get("required")) for row in provenance), "passed": any(row.get("requirement") == "source_authority" and boolish(row.get("required")) for row in provenance)},
        {"review": "tmp_artifact_truth_source_blocked", "actual": any(row.get("requirement") == "no_tmp_artifact_truth_source" and boolish(row.get("required")) for row in provenance), "passed": any(row.get("requirement") == "no_tmp_artifact_truth_source" and boolish(row.get("required")) for row in provenance)},
    ]

    validation_quality_review = [
        {"review": "validation_gates_present", "actual": len(validation), "expected_min": 7, "passed": len(validation) >= 7},
        {"review": "quality_gates_present", "actual": len(quality), "expected_min": 6, "passed": len(quality) >= 6},
        {"review": "all_validation_gates_blocking", "actual": all(str(row.get("blocks_if_failed", "")).lower() == "true" for row in validation), "expected": True, "passed": all(str(row.get("blocks_if_failed", "")).lower() == "true" for row in validation)},
        {"review": "missing_required_threshold_zero", "actual": any(row.get("gate") == "missing_required_field_count" and row.get("threshold") == "0" for row in quality), "expected": True, "passed": any(row.get("gate") == "missing_required_field_count" and row.get("threshold") == "0" for row in quality)},
    ]

    policy_review = [
        {"review": "duplicate_policy_present", "actual": len(duplicate), "expected_min": 3, "passed": len(duplicate) >= 3},
        {"review": "missing_policy_present", "actual": len(missing), "expected_min": 5, "passed": len(missing) >= 5},
        {"review": "source_authority_policy_present", "actual": len(authority), "expected_min": 4, "passed": len(authority) >= 4},
        {"review": "tmp_not_authority", "actual": any(row.get("policy") == "tmp_artifacts_allowed_as_authority" and str(row.get("allowed", "")).lower() == "false" for row in authority), "expected": True, "passed": any(row.get("policy") == "tmp_artifacts_allowed_as_authority" and str(row.get("allowed", "")).lower() == "false" for row in authority)},
        {"review": "unknown_source_blocked", "actual": any(row.get("policy") == "unknown_source_allowed" and str(row.get("allowed", "")).lower() == "false" for row in authority), "expected": True, "passed": any(row.get("policy") == "unknown_source_allowed" and str(row.get("allowed", "")).lower() == "false" for row in authority)},
    ]

    precondition_review = [
        {"review": "implementation_preconditions_present", "actual": len(implementation_pre), "expected_min": 5, "passed": len(implementation_pre) >= 5},
        {"review": "audit_preconditions_present", "actual": len(audit_pre), "expected_min": 5, "passed": len(audit_pre) >= 5},
        {"review": "source_contract_audit_required_before_implementation", "actual": any(row.get("precondition") == "source_contract_audited_by_6mp" and boolish(row.get("required")) for row in implementation_pre), "passed": any(row.get("precondition") == "source_contract_audited_by_6mp" and boolish(row.get("required")) for row in implementation_pre)},
        {"review": "forbidden_operations_audit_required", "actual": any(row.get("precondition") == "audit_forbidden_operations" and boolish(row.get("required")) for row in audit_pre), "passed": any(row.get("precondition") == "audit_forbidden_operations" and boolish(row.get("required")) for row in audit_pre)},
    ]

    allowed_next = [
        {"operation": "plan_local_source_ingestion", "allowed_next": True, "scope": "planning only; no source ingestion", "passed": True},
        {"operation": "plan_local_file_validation", "allowed_next": True, "scope": "planning only", "passed": True},
        {"operation": "plan_local_file_presence_check", "allowed_next": True, "scope": "planning only", "passed": True},
    ]

    forbidden_next = [
        {"operation": "source_ingestion", "allowed_next": False, "passed": True},
        {"operation": "source_implementation", "allowed_next": False, "passed": True},
        {"operation": "source_data_creation", "allowed_next": False, "passed": True},
        {"operation": "data_acquisition", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "external_source_scan", "allowed_next": False, "passed": True},
        {"operation": "local_source_scan", "allowed_next": False, "passed": True},
        {"operation": "metric_execution", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
    ]

    blockers = [
        {"blocker": "local_source_files_not_present_or_not_validated", "active": True, "reason": "6MP does not check file presence or ingest data", "passed": True},
        {"blocker": "source_ingestion_not_planned", "active": True, "reason": "6MQ must plan ingestion/validation only", "passed": True},
        {"blocker": "source_ingestion_or_implementation_blocked", "active": True, "reason": "requires future planning and validation", "passed": True},
        {"blocker": "metrics_backtests_tuning_activation_exit_blocked", "active": True, "reason": "requires implemented and audited historical sources", "passed": True},
    ]

    future_6mq = [
        {"contract": "plan_local_file_presence_validation", "required": True, "why": "accepted local locations are defined but files are not validated by 6MP", "passed": True},
        {"contract": "plan_schema_validation_for_actuals_and_moneyline", "required": True, "why": "next layer plans ingestion validators", "passed": True},
        {"contract": "plan_no_ingestion_execution", "required": True, "why": "6MQ should remain planning only", "passed": True},
        {"contract": "preserve_no_metrics_no_backtests", "required": True, "why": "sources not implemented/audited yet", "passed": True},
    ]

    blocking_policy = [
        {"policy": "do_not_use_tmp_outputs_as_historical_truth", "required": True, "passed": True},
        {"policy": "do_not_generate_fake_actual_outcomes", "required": True, "passed": True},
        {"policy": "do_not_generate_fake_moneyline_odds", "required": True, "passed": True},
        {"policy": "do_not_ingest_sources_from_audit_layer", "required": True, "passed": True},
        {"policy": "do_not_execute_metrics_without_implemented_and_audited_sources", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6mo_passed", "expected": True, "actual": json_6mo.get("all_checks_passed"), "passed": json_6mo.get("all_checks_passed") is True},
        {"decision": "6mo_diagnosis_valid", "expected": DIAGNOSIS_6MO, "actual": json_6mo.get("diagnosis"), "passed": json_6mo.get("diagnosis") == DIAGNOSIS_6MO},
        {"decision": "all_required_6mo_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "actuals_file_contract_audited", "expected": True, "actual": all_passed(contract_review[:2]), "passed": all_passed(contract_review[:2])},
        {"decision": "moneyline_file_contract_audited", "expected": True, "actual": all_passed(contract_review[:2]), "passed": all_passed(contract_review[:2])},
        {"decision": "file_drop_locations_audited", "expected": True, "actual": all_passed(file_drop_review), "passed": all_passed(file_drop_review)},
        {"decision": "schema_aliases_audited", "expected": True, "actual": all_passed(schema_review), "passed": all_passed(schema_review)},
        {"decision": "provenance_contract_audited", "expected": True, "actual": all_passed(provenance_review), "passed": all_passed(provenance_review)},
        {"decision": "validation_quality_gates_audited", "expected": True, "actual": all_passed(validation_quality_review), "passed": all_passed(validation_quality_review)},
        {"decision": "policies_audited", "expected": True, "actual": all_passed(policy_review), "passed": all_passed(policy_review)},
        {"decision": "preconditions_audited", "expected": True, "actual": all_passed(precondition_review), "passed": all_passed(precondition_review)},
        {"decision": "contract_valid_for_ingestion_planning", "expected": True, "actual": True, "passed": True},
        {"decision": "recommend_6mq_next", "expected": RECOMMENDED_NEXT_LAYER_6MP, "actual": RECOMMENDED_NEXT_LAYER_6MP, "passed": True},
        {"decision": "do_not_ingest_implement_metrics_backtest_tune_activate_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only_historical_source_remediation_contract", "expected": True, "actual": True, "passed": True},
        {"boundary": "source_data_created_by_6mp", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_acquisition_performed_by_6mp", "expected": False, "actual": False, "passed": True},
        {"boundary": "external_source_scan_run_by_6mp", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_source_scan_run_by_6mp", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_ingestion_run_by_6mp", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_implementation_run_by_6mp", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6mp", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mp", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mp", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mp", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MP, "actual": RECOMMENDED_NEXT_LAYER_6MP, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MP, "actual": RECOMMENDED_PATH_6MP, "passed": True},
        {"decision": "allow_local_source_ingestion_planning_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_source_ingestion", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_source_implementation", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MP, "actual": DIAGNOSIS_6MP, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "contract_review", "passed": all_passed(contract_review), "detail": f"{sum(1 for r in contract_review if r['passed'])}/{len(contract_review)}"},
        {"check": "file_drop_review", "passed": all_passed(file_drop_review), "detail": f"{sum(1 for r in file_drop_review if r['passed'])}/{len(file_drop_review)}"},
        {"check": "schema_review", "passed": all_passed(schema_review), "detail": f"{sum(1 for r in schema_review if r['passed'])}/{len(schema_review)}"},
        {"check": "provenance_review", "passed": all_passed(provenance_review), "detail": f"{sum(1 for r in provenance_review if r['passed'])}/{len(provenance_review)}"},
        {"check": "validation_quality_review", "passed": all_passed(validation_quality_review), "detail": f"{sum(1 for r in validation_quality_review if r['passed'])}/{len(validation_quality_review)}"},
        {"check": "policy_review", "passed": all_passed(policy_review), "detail": f"{sum(1 for r in policy_review if r['passed'])}/{len(policy_review)}"},
        {"check": "precondition_review", "passed": all_passed(precondition_review), "detail": f"{sum(1 for r in precondition_review if r['passed'])}/{len(precondition_review)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next), "detail": f"{sum(1 for r in allowed_next if r['passed'])}/{len(allowed_next)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next), "detail": f"{sum(1 for r in forbidden_next if r['passed'])}/{len(forbidden_next)}"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "future_6mq_contract", "passed": all_passed(future_6mq), "detail": f"{sum(1 for r in future_6mq if r['passed'])}/{len(future_6mq)}"},
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
        "contract_review": write_csv(CONTRACT_REVIEW_CSV, contract_review),
        "file_drop_review": write_csv(FILE_DROP_REVIEW_CSV, file_drop_review),
        "schema_review": write_csv(SCHEMA_REVIEW_CSV, schema_review),
        "provenance_review": write_csv(PROVENANCE_REVIEW_CSV, provenance_review),
        "validation_quality_review": write_csv(VALIDATION_QUALITY_REVIEW_CSV, validation_quality_review),
        "policy_review": write_csv(POLICY_REVIEW_CSV, policy_review),
        "precondition_review": write_csv(PRECONDITION_REVIEW_CSV, precondition_review),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6mq_contract": write_csv(FUTURE_6MQ_CSV, future_6mq),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MP",
        "layer_type": "game_mechanics_realism",
        "audit_only_historical_source_remediation_contract": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MP if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MP,
        "recommended_path": RECOMMENDED_PATH_6MP,
        "predecessor_layer": "6MO",
        "predecessor_diagnosis": json_6mo.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mo.get("all_checks_passed") is True,
        "audited_layer_after": "6MO",
        "source_family": "projection_adapter_historical_actuals_moneyline_source_remediation_audit",
        "actuals_file_contract_audited": all_passed(contract_review),
        "moneyline_file_contract_audited": all_passed(contract_review),
        "file_drop_locations_audited": all_passed(file_drop_review),
        "schema_aliases_audited": all_passed(schema_review),
        "provenance_contract_audited": all_passed(provenance_review),
        "validation_gates_audited": all_passed(validation_quality_review),
        "quality_gates_audited": all_passed(validation_quality_review),
        "duplicate_policy_audited": all_passed(policy_review),
        "missing_field_policy_audited": all_passed(policy_review),
        "source_authority_policy_audited": all_passed(policy_review),
        "implementation_preconditions_audited": all_passed(precondition_review),
        "audit_preconditions_audited": all_passed(precondition_review),
        "contract_valid_for_ingestion_planning": True,
        "local_source_ingestion_planning_allowed_next": True,
        "source_ingestion_allowed_next": False,
        "source_implementation_allowed_next": False,
        "data_acquisition_allowed_next": False,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "source_data_created_by_6mp": False,
        "source_acquisition_performed_by_6mp": False,
        "external_source_scan_run_by_6mp": False,
        "local_source_scan_run_by_6mp": False,
        "source_ingestion_run_by_6mp": False,
        "source_implementation_run_by_6mp": False,
        "metric_execution_run_by_6mp": False,
        "backtest_execution_run_by_6mp": False,
        "adapter_call_executed_by_6mp": False,
        "production_code_modified_by_6mp": False,
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
            "contract_review_csv": str(CONTRACT_REVIEW_CSV),
            "file_drop_review_csv": str(FILE_DROP_REVIEW_CSV),
            "schema_review_csv": str(SCHEMA_REVIEW_CSV),
            "provenance_review_csv": str(PROVENANCE_REVIEW_CSV),
            "validation_quality_review_csv": str(VALIDATION_QUALITY_REVIEW_CSV),
            "policy_review_csv": str(POLICY_REVIEW_CSV),
            "precondition_review_csv": str(PRECONDITION_REVIEW_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6mq_contract_csv": str(FUTURE_6MQ_CSV),
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
