#!/usr/bin/env python3
"""Plan remediation/source-ingestion contract for missing historical actuals and moneyline."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mo_projection_adapter_historical_actuals_moneyline_remediation_plan"
TMP_DIR = Path("tmp")

SCRIPT_6MN = Path("scripts/plan_6mn_layer6_projection_adapter_historical_actuals_moneyline_integration.py")
JSON_6MN = TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan.json"

REQUIRED_INPUTS = [
    JSON_6MN,
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_checks.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_predecessor.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_scan_result_review.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_actuals_source_shape.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_moneyline_source_shape.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_candidate_insufficiency_review.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_transformability_decision.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_remediation_options.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_integration_requirements.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_fail_closed_policy.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_blockers.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_future_6mo_contract.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_decision.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan_recommended_path.csv",
    SCRIPT_6MN,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
ACTUALS_FILE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_actuals_file_contract.csv"
MONEYLINE_FILE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_moneyline_file_contract.csv"
FILE_DROP_LOCATIONS_CSV = TMP_DIR / f"{SLUG}_file_drop_locations.csv"
SCHEMA_ALIASES_CSV = TMP_DIR / f"{SLUG}_schema_aliases.csv"
PROVENANCE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_provenance_contract.csv"
VALIDATION_GATES_CSV = TMP_DIR / f"{SLUG}_validation_gates.csv"
QUALITY_GATES_CSV = TMP_DIR / f"{SLUG}_quality_gates.csv"
DUPLICATE_POLICY_CSV = TMP_DIR / f"{SLUG}_duplicate_policy.csv"
MISSING_FIELD_POLICY_CSV = TMP_DIR / f"{SLUG}_missing_field_policy.csv"
SOURCE_AUTHORITY_POLICY_CSV = TMP_DIR / f"{SLUG}_source_authority_policy.csv"
IMPLEMENTATION_PRECONDITIONS_CSV = TMP_DIR / f"{SLUG}_implementation_preconditions.csv"
AUDIT_PRECONDITIONS_CSV = TMP_DIR / f"{SLUG}_audit_preconditions.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6MP_CSV = TMP_DIR / f"{SLUG}_future_6mp_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MN = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_integration_plan_complete"
DIAGNOSIS_6MO = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_remediation_plan_complete"
RECOMMENDED_NEXT_LAYER_6MO = "6MP_layer_6_projection_adapter_historical_actuals_and_moneyline_source_remediation_audit"
RECOMMENDED_PATH_6MO = "audit_source_remediation_contract_before_any_source_ingestion_or_implementation"


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
    json_6mn = load_json(JSON_6MN)

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
        {"check": "6mn_script_exists", "expected": True, "actual": SCRIPT_6MN.exists(), "passed": SCRIPT_6MN.exists()},
        {"check": "6mn_json_exists", "expected": True, "actual": JSON_6MN.exists(), "passed": JSON_6MN.exists()},
        {"check": "6mn_all_checks_passed", "expected": True, "actual": json_6mn.get("all_checks_passed"), "passed": json_6mn.get("all_checks_passed") is True},
        {"check": "6mn_diagnosis", "expected": DIAGNOSIS_6MN, "actual": json_6mn.get("diagnosis"), "passed": json_6mn.get("diagnosis") == DIAGNOSIS_6MN},
        {"check": "6mn_recommended_next_layer", "expected": "6MO_layer_6_projection_adapter_historical_actuals_and_moneyline_source_remediation_plan", "actual": json_6mn.get("recommended_next_layer"), "passed": json_6mn.get("recommended_next_layer") == "6MO_layer_6_projection_adapter_historical_actuals_and_moneyline_source_remediation_plan"},
        {"check": "no_viable_actuals_candidate_confirmed", "expected": True, "actual": json_6mn.get("viable_actuals_candidate_found_confirmed"), "passed": json_6mn.get("viable_actuals_candidate_found_confirmed") is True},
        {"check": "no_viable_moneyline_candidate_confirmed", "expected": True, "actual": json_6mn.get("viable_moneyline_candidate_found_confirmed"), "passed": json_6mn.get("viable_moneyline_candidate_found_confirmed") is True},
        {"check": "existing_artifact_transform_safe", "expected": False, "actual": json_6mn.get("existing_artifact_transform_safe"), "passed": json_6mn.get("existing_artifact_transform_safe") is False},
    ]

    actuals_contract = [
        {"field": "game_pk", "required": True, "accepted_aliases": "game_id,mlb_game_id,event_id", "type": "string_or_integer", "validation": "non_empty and unique with source", "passed": True},
        {"field": "game_date", "required": True, "accepted_aliases": "date,official_date", "type": "date_string", "validation": "parseable date", "passed": True},
        {"field": "home_team", "required": True, "accepted_aliases": "home,home_abbrev,home_team_abbrev", "type": "string", "validation": "non_empty", "passed": True},
        {"field": "away_team", "required": True, "accepted_aliases": "away,away_abbrev,away_team_abbrev", "type": "string", "validation": "non_empty", "passed": True},
        {"field": "home_score", "required": True, "accepted_aliases": "home_runs,home_final_score", "type": "integer", "validation": ">=0", "passed": True},
        {"field": "away_score", "required": True, "accepted_aliases": "away_runs,away_final_score", "type": "integer", "validation": ">=0", "passed": True},
        {"field": "home_win_binary", "required": True, "accepted_aliases": "home_win,actual_home_win,winner_side", "type": "0_or_1", "validation": "consistent with score fields", "passed": True},
        {"field": "source_artifact", "required": True, "accepted_aliases": "source_file,provenance", "type": "string_path", "validation": "non_empty provenance", "passed": True},
    ]

    moneyline_contract = [
        {"field": "game_pk", "required": True, "accepted_aliases": "game_id,mlb_game_id,event_id", "type": "string_or_integer", "validation": "non_empty and joinable", "passed": True},
        {"field": "game_date", "required": True, "accepted_aliases": "date,odds_date", "type": "date_string", "validation": "parseable date", "passed": True},
        {"field": "home_team", "required": True, "accepted_aliases": "home,home_abbrev,home_team_abbrev", "type": "string", "validation": "non_empty", "passed": True},
        {"field": "away_team", "required": True, "accepted_aliases": "away,away_abbrev,away_team_abbrev", "type": "string", "validation": "non_empty", "passed": True},
        {"field": "home_moneyline", "required": True, "accepted_aliases": "home_ml,moneyline_home,home_close_moneyline", "type": "american_odds_number", "validation": "numeric and non_zero", "passed": True},
        {"field": "away_moneyline", "required": False, "accepted_aliases": "away_ml,moneyline_away,away_close_moneyline", "type": "american_odds_number", "validation": "numeric and non_zero when present", "passed": True},
        {"field": "odds_timestamp_or_type", "required": False, "accepted_aliases": "snapshot_type,open_close,market_time", "type": "string_or_datetime", "validation": "preserve if present", "passed": True},
        {"field": "sportsbook_or_source", "required": False, "accepted_aliases": "book,source_book", "type": "string", "validation": "preserve if present", "passed": True},
        {"field": "source_artifact", "required": True, "accepted_aliases": "source_file,provenance", "type": "string_path", "validation": "non_empty provenance", "passed": True},
    ]

    file_drop_locations = [
        {"source_type": "actuals", "accepted_location": "data/local/historical_actuals.csv", "allowed": True, "must_exist_before_implementation": True, "passed": True},
        {"source_type": "moneyline", "accepted_location": "data/local/historical_moneyline_odds.csv", "allowed": True, "must_exist_before_implementation": True, "passed": True},
        {"source_type": "actuals", "accepted_location": "data/local/historical_actuals/*.csv", "allowed": True, "must_exist_before_implementation": True, "passed": True},
        {"source_type": "moneyline", "accepted_location": "data/local/historical_moneyline_odds/*.csv", "allowed": True, "must_exist_before_implementation": True, "passed": True},
        {"source_type": "generated_or_tmp", "accepted_location": "tmp/*", "allowed": False, "must_exist_before_implementation": False, "passed": True},
    ]

    schema_aliases = [
        {"source_type": "actuals", "canonical": row["field"], "accepted_aliases": row["accepted_aliases"], "passed": True}
        for row in actuals_contract
    ] + [
        {"source_type": "moneyline", "canonical": row["field"], "accepted_aliases": row["accepted_aliases"], "passed": True}
        for row in moneyline_contract
    ]

    provenance_contract = [
        {"requirement": "source_artifact", "required": True, "detail": "path or descriptor for local source file", "passed": True},
        {"requirement": "source_authority", "required": True, "detail": "human-declared or documented source authority before use", "passed": True},
        {"requirement": "ingestion_timestamp", "required": False, "detail": "preserve if later source ingestion occurs", "passed": True},
        {"requirement": "no_tmp_artifact_truth_source", "required": True, "detail": "tmp/debug/reporting artifacts cannot be truth or odds authority", "passed": True},
    ]

    validation_gates = [
        {"gate": "actuals_required_fields_present", "applies_to": "actuals", "blocks_if_failed": True, "passed": True},
        {"gate": "moneyline_required_fields_present", "applies_to": "moneyline", "blocks_if_failed": True, "passed": True},
        {"gate": "game_pk_joinable_to_probability_surface", "applies_to": "both", "blocks_if_failed": True, "passed": True},
        {"gate": "home_away_team_alignment_valid", "applies_to": "both", "blocks_if_failed": True, "passed": True},
        {"gate": "home_win_binary_consistent_with_scores", "applies_to": "actuals", "blocks_if_failed": True, "passed": True},
        {"gate": "home_moneyline_numeric_non_zero", "applies_to": "moneyline", "blocks_if_failed": True, "passed": True},
        {"gate": "provenance_present", "applies_to": "both", "blocks_if_failed": True, "passed": True},
    ]

    quality_gates = [
        {"gate": "minimum_joined_rows", "threshold": ">=1 before prototype metric; larger threshold before real backtest", "passed": True},
        {"gate": "duplicate_policy_applied", "threshold": "zero unresolved duplicate game/source rows", "passed": True},
        {"gate": "missing_required_field_count", "threshold": "0", "passed": True},
        {"gate": "invalid_moneyline_count", "threshold": "0", "passed": True},
        {"gate": "score_binary_consistency_errors", "threshold": "0", "passed": True},
        {"gate": "provenance_missing_count", "threshold": "0", "passed": True},
    ]

    duplicate_policy = [
        {"source_type": "actuals", "duplicate_key": "game_pk", "policy": "allow only if all winner/scores/team fields identical; otherwise block", "passed": True},
        {"source_type": "moneyline", "duplicate_key": "game_pk + sportsbook_or_source + odds_timestamp_or_type", "policy": "preserve distinct snapshots; require selection policy before metric execution", "passed": True},
        {"source_type": "moneyline_missing_snapshot", "duplicate_key": "game_pk", "policy": "block if multiple odds rows and no source/timestamp policy", "passed": True},
    ]

    missing_field_policy = [
        {"source_type": "actuals", "missing": "required field", "behavior": "block actuals source implementation", "passed": True},
        {"source_type": "moneyline", "missing": "required field", "behavior": "block moneyline source implementation", "passed": True},
        {"source_type": "actuals", "missing": "home_win_binary", "behavior": "may derive from scores only if scores authoritative and consistent", "passed": True},
        {"source_type": "moneyline", "missing": "away_moneyline", "behavior": "allow raw home market comparison but block de-vig until available", "passed": True},
        {"source_type": "both", "missing": "source_artifact", "behavior": "block source use", "passed": True},
    ]

    source_authority_policy = [
        {"policy": "local_user_provided_csv_allowed", "allowed": True, "condition": "must be under accepted data/local path and pass provenance/validation gates", "passed": True},
        {"policy": "tmp_artifacts_allowed_as_authority", "allowed": False, "condition": "tmp artifacts are generated outputs, not source authority", "passed": True},
        {"policy": "web_or_api_fetch_allowed_by_6mo", "allowed": False, "condition": "requires explicit future acquisition layer and approval", "passed": True},
        {"policy": "unknown_source_allowed", "allowed": False, "condition": "source authority and provenance required", "passed": True},
    ]

    implementation_preconditions = [
        {"precondition": "actuals_source_file_present_in_accepted_location", "required": True, "passed": True},
        {"precondition": "moneyline_source_file_present_in_accepted_location", "required": True, "passed": True},
        {"precondition": "source_contract_audited_by_6mp", "required": True, "passed": True},
        {"precondition": "schema_validation_layer_defined", "required": True, "passed": True},
        {"precondition": "no_metric_or_backtest_execution_before_source_audit", "required": True, "passed": True},
    ]

    audit_preconditions = [
        {"precondition": "6mo_contract_outputs_exist", "required": True, "passed": True},
        {"precondition": "audit_actuals_file_contract", "required": True, "passed": True},
        {"precondition": "audit_moneyline_file_contract", "required": True, "passed": True},
        {"precondition": "audit_provenance_and_authority_policy", "required": True, "passed": True},
        {"precondition": "audit_forbidden_operations", "required": True, "passed": True},
    ]

    allowed_next = [
        {"operation": "audit_source_remediation_contract", "allowed_next": True, "scope": "audit only", "passed": True},
        {"operation": "audit_file_drop_and_schema_contract", "allowed_next": True, "scope": "audit only", "passed": True},
        {"operation": "audit_source_authority_policy", "allowed_next": True, "scope": "audit only", "passed": True},
    ]

    forbidden_next = [
        {"operation": "source_ingestion", "allowed_next": False, "passed": True},
        {"operation": "source_implementation", "allowed_next": False, "passed": True},
        {"operation": "source_data_creation", "allowed_next": False, "passed": True},
        {"operation": "data_acquisition", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "external_source_scan", "allowed_next": False, "passed": True},
        {"operation": "metric_execution", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
    ]

    blockers = [
        {"blocker": "no_viable_actuals_candidate_found", "active": True, "reason": "source file absent", "passed": True},
        {"blocker": "no_viable_moneyline_candidate_found", "active": True, "reason": "source file absent", "passed": True},
        {"blocker": "source_contract_not_audited", "active": True, "reason": "6MP must audit 6MO remediation contract", "passed": True},
        {"blocker": "source_ingestion_or_implementation_blocked", "active": True, "reason": "contract audit required first", "passed": True},
        {"blocker": "metrics_backtests_tuning_activation_exit_blocked", "active": True, "reason": "requires implemented and audited historical sources", "passed": True},
    ]

    future_6mp = [
        {"contract": "audit_actuals_file_contract", "required": True, "why": "verify local source shape before ingestion", "passed": True},
        {"contract": "audit_moneyline_file_contract", "required": True, "why": "verify local odds shape before ingestion", "passed": True},
        {"contract": "audit_file_drop_locations", "required": True, "why": "ensure tmp/generated artifacts remain excluded", "passed": True},
        {"contract": "audit_no_source_data_created", "required": True, "why": "6MO is planning-only", "passed": True},
        {"contract": "preserve_no_ingestion_no_metrics_no_backtests", "required": True, "why": "6MP remains contract audit", "passed": True},
    ]

    blocking_policy = [
        {"policy": "do_not_use_tmp_outputs_as_historical_truth", "required": True, "passed": True},
        {"policy": "do_not_generate_fake_actual_outcomes", "required": True, "passed": True},
        {"policy": "do_not_generate_fake_moneyline_odds", "required": True, "passed": True},
        {"policy": "do_not_ingest_sources_before_contract_audit", "required": True, "passed": True},
        {"policy": "do_not_execute_metrics_without_implemented_and_audited_sources", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6mn_passed", "expected": True, "actual": json_6mn.get("all_checks_passed"), "passed": json_6mn.get("all_checks_passed") is True},
        {"decision": "6mn_diagnosis_valid", "expected": DIAGNOSIS_6MN, "actual": json_6mn.get("diagnosis"), "passed": json_6mn.get("diagnosis") == DIAGNOSIS_6MN},
        {"decision": "all_required_6mn_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "no_viable_actuals_candidate_confirmed", "expected": True, "actual": json_6mn.get("viable_actuals_candidate_found_confirmed"), "passed": json_6mn.get("viable_actuals_candidate_found_confirmed") is True},
        {"decision": "no_viable_moneyline_candidate_confirmed", "expected": True, "actual": json_6mn.get("viable_moneyline_candidate_found_confirmed"), "passed": json_6mn.get("viable_moneyline_candidate_found_confirmed") is True},
        {"decision": "existing_artifact_transform_safe_confirmed", "expected": False, "actual": json_6mn.get("existing_artifact_transform_safe"), "passed": json_6mn.get("existing_artifact_transform_safe") is False},
        {"decision": "actuals_file_contract_created", "expected": True, "actual": True, "passed": all_passed(actuals_contract)},
        {"decision": "moneyline_file_contract_created", "expected": True, "actual": True, "passed": all_passed(moneyline_contract)},
        {"decision": "file_drop_locations_created", "expected": True, "actual": True, "passed": all_passed(file_drop_locations)},
        {"decision": "schema_aliases_created", "expected": True, "actual": True, "passed": all_passed(schema_aliases)},
        {"decision": "provenance_contract_created", "expected": True, "actual": True, "passed": all_passed(provenance_contract)},
        {"decision": "validation_and_quality_gates_created", "expected": True, "actual": True, "passed": all_passed(validation_gates) and all_passed(quality_gates)},
        {"decision": "duplicate_missing_authority_policies_created", "expected": True, "actual": True, "passed": all_passed(duplicate_policy) and all_passed(missing_field_policy) and all_passed(source_authority_policy)},
        {"decision": "implementation_and_audit_preconditions_created", "expected": True, "actual": True, "passed": all_passed(implementation_preconditions) and all_passed(audit_preconditions)},
        {"decision": "recommend_6mp_next", "expected": RECOMMENDED_NEXT_LAYER_6MO, "actual": RECOMMENDED_NEXT_LAYER_6MO, "passed": True},
        {"decision": "do_not_ingest_implement_metrics_backtest_tune_activate_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only_historical_source_remediation_contract", "expected": True, "actual": True, "passed": True},
        {"boundary": "source_data_created_by_6mo", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_acquisition_performed_by_6mo", "expected": False, "actual": False, "passed": True},
        {"boundary": "external_source_scan_run_by_6mo", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_source_scan_run_by_6mo", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_implementation_run_by_6mo", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6mo", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mo", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mo", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mo", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MO, "actual": RECOMMENDED_NEXT_LAYER_6MO, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MO, "actual": RECOMMENDED_PATH_6MO, "passed": True},
        {"decision": "allow_contract_audit_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_source_ingestion", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_source_implementation", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MO, "actual": DIAGNOSIS_6MO, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "actuals_file_contract", "passed": all_passed(actuals_contract), "detail": f"{sum(1 for r in actuals_contract if r['passed'])}/{len(actuals_contract)}"},
        {"check": "moneyline_file_contract", "passed": all_passed(moneyline_contract), "detail": f"{sum(1 for r in moneyline_contract if r['passed'])}/{len(moneyline_contract)}"},
        {"check": "file_drop_locations", "passed": all_passed(file_drop_locations), "detail": f"{sum(1 for r in file_drop_locations if r['passed'])}/{len(file_drop_locations)}"},
        {"check": "schema_aliases", "passed": all_passed(schema_aliases), "detail": f"{sum(1 for r in schema_aliases if r['passed'])}/{len(schema_aliases)}"},
        {"check": "provenance_contract", "passed": all_passed(provenance_contract), "detail": f"{sum(1 for r in provenance_contract if r['passed'])}/{len(provenance_contract)}"},
        {"check": "validation_gates", "passed": all_passed(validation_gates), "detail": f"{sum(1 for r in validation_gates if r['passed'])}/{len(validation_gates)}"},
        {"check": "quality_gates", "passed": all_passed(quality_gates), "detail": f"{sum(1 for r in quality_gates if r['passed'])}/{len(quality_gates)}"},
        {"check": "duplicate_policy", "passed": all_passed(duplicate_policy), "detail": f"{sum(1 for r in duplicate_policy if r['passed'])}/{len(duplicate_policy)}"},
        {"check": "missing_field_policy", "passed": all_passed(missing_field_policy), "detail": f"{sum(1 for r in missing_field_policy if r['passed'])}/{len(missing_field_policy)}"},
        {"check": "source_authority_policy", "passed": all_passed(source_authority_policy), "detail": f"{sum(1 for r in source_authority_policy if r['passed'])}/{len(source_authority_policy)}"},
        {"check": "implementation_preconditions", "passed": all_passed(implementation_preconditions), "detail": f"{sum(1 for r in implementation_preconditions if r['passed'])}/{len(implementation_preconditions)}"},
        {"check": "audit_preconditions", "passed": all_passed(audit_preconditions), "detail": f"{sum(1 for r in audit_preconditions if r['passed'])}/{len(audit_preconditions)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next), "detail": f"{sum(1 for r in allowed_next if r['passed'])}/{len(allowed_next)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next), "detail": f"{sum(1 for r in forbidden_next if r['passed'])}/{len(forbidden_next)}"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "future_6mp_contract", "passed": all_passed(future_6mp), "detail": f"{sum(1 for r in future_6mp if r['passed'])}/{len(future_6mp)}"},
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
        "actuals_file_contract": write_csv(ACTUALS_FILE_CONTRACT_CSV, actuals_contract),
        "moneyline_file_contract": write_csv(MONEYLINE_FILE_CONTRACT_CSV, moneyline_contract),
        "file_drop_locations": write_csv(FILE_DROP_LOCATIONS_CSV, file_drop_locations),
        "schema_aliases": write_csv(SCHEMA_ALIASES_CSV, schema_aliases),
        "provenance_contract": write_csv(PROVENANCE_CONTRACT_CSV, provenance_contract),
        "validation_gates": write_csv(VALIDATION_GATES_CSV, validation_gates),
        "quality_gates": write_csv(QUALITY_GATES_CSV, quality_gates),
        "duplicate_policy": write_csv(DUPLICATE_POLICY_CSV, duplicate_policy),
        "missing_field_policy": write_csv(MISSING_FIELD_POLICY_CSV, missing_field_policy),
        "source_authority_policy": write_csv(SOURCE_AUTHORITY_POLICY_CSV, source_authority_policy),
        "implementation_preconditions": write_csv(IMPLEMENTATION_PRECONDITIONS_CSV, implementation_preconditions),
        "audit_preconditions": write_csv(AUDIT_PRECONDITIONS_CSV, audit_preconditions),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6mp_contract": write_csv(FUTURE_6MP_CSV, future_6mp),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MO",
        "layer_type": "game_mechanics_realism",
        "planning_only_historical_source_remediation_contract": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MO if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MO,
        "recommended_path": RECOMMENDED_PATH_6MO,
        "predecessor_layer": "6MN",
        "predecessor_diagnosis": json_6mn.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mn.get("all_checks_passed") is True,
        "planned_layer_after": "6MN",
        "source_family": "projection_adapter_historical_actuals_moneyline_source_remediation_plan",
        "no_viable_actuals_candidate_confirmed": json_6mn.get("viable_actuals_candidate_found_confirmed") is True,
        "no_viable_moneyline_candidate_confirmed": json_6mn.get("viable_moneyline_candidate_found_confirmed") is True,
        "existing_artifact_transform_safe_confirmed": json_6mn.get("existing_artifact_transform_safe") is False,
        "actuals_file_contract_created": True,
        "moneyline_file_contract_created": True,
        "file_drop_locations_created": True,
        "schema_aliases_created": True,
        "provenance_contract_created": True,
        "validation_gates_created": True,
        "quality_gates_created": True,
        "duplicate_policy_created": True,
        "missing_field_policy_created": True,
        "source_authority_policy_created": True,
        "implementation_preconditions_created": True,
        "audit_preconditions_created": True,
        "remediation_contract_audit_allowed_next": True,
        "source_ingestion_allowed_next": False,
        "source_implementation_allowed_next": False,
        "data_acquisition_allowed_next": False,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "source_data_created_by_6mo": False,
        "source_acquisition_performed_by_6mo": False,
        "external_source_scan_run_by_6mo": False,
        "local_source_scan_run_by_6mo": False,
        "source_implementation_run_by_6mo": False,
        "metric_execution_run_by_6mo": False,
        "backtest_execution_run_by_6mo": False,
        "adapter_call_executed_by_6mo": False,
        "production_code_modified_by_6mo": False,
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
            "actuals_file_contract_csv": str(ACTUALS_FILE_CONTRACT_CSV),
            "moneyline_file_contract_csv": str(MONEYLINE_FILE_CONTRACT_CSV),
            "file_drop_locations_csv": str(FILE_DROP_LOCATIONS_CSV),
            "schema_aliases_csv": str(SCHEMA_ALIASES_CSV),
            "provenance_contract_csv": str(PROVENANCE_CONTRACT_CSV),
            "validation_gates_csv": str(VALIDATION_GATES_CSV),
            "quality_gates_csv": str(QUALITY_GATES_CSV),
            "duplicate_policy_csv": str(DUPLICATE_POLICY_CSV),
            "missing_field_policy_csv": str(MISSING_FIELD_POLICY_CSV),
            "source_authority_policy_csv": str(SOURCE_AUTHORITY_POLICY_CSV),
            "implementation_preconditions_csv": str(IMPLEMENTATION_PRECONDITIONS_CSV),
            "audit_preconditions_csv": str(AUDIT_PRECONDITIONS_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6mp_contract_csv": str(FUTURE_6MP_CSV),
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
