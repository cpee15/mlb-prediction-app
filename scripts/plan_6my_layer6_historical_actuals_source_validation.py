#!/usr/bin/env python3
"""Plan Layer 6 historical actuals source validation before actuals-only metrics."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6my_historical_actuals_source_validation_plan"
TMP_DIR = Path("tmp")

SCRIPT_6MX = Path("scripts/audit_6mx_layer6_actuals_only_resume_plan.py")
JSON_6MX = TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit.json"

REQUIRED_INPUTS = [
    JSON_6MX,
    TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit_checks.csv",
    TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit_predecessor.csv",
    TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit_path_change_review.csv",
    TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit_actuals_schema_review.csv",
    TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit_moneyline_deferral_review.csv",
    TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit_allowed_actuals_metrics_review.csv",
    TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit_forbidden_market_claims_review.csv",
    TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit_resume_conditions_review.csv",
    TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit_allowed_operations_next.csv",
    TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit_future_6my_contract.csv",
    TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit_decision.csv",
    TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6mx_actuals_only_resume_plan_audit_recommended_path.csv",
    SCRIPT_6MX,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
ACCEPTED_LOCATIONS_CSV = TMP_DIR / f"{SLUG}_accepted_locations.csv"
REQUIRED_SCHEMA_CSV = TMP_DIR / f"{SLUG}_required_schema.csv"
ALIAS_CANDIDATES_CSV = TMP_DIR / f"{SLUG}_alias_candidates.csv"
VALIDATION_CHECKS_CSV = TMP_DIR / f"{SLUG}_validation_checks.csv"
BLOCKING_CONDITIONS_CSV = TMP_DIR / f"{SLUG}_blocking_conditions.csv"
ALLOWED_OUTPUTS_CSV = TMP_DIR / f"{SLUG}_allowed_outputs.csv"
MONEYLINE_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_moneyline_deferral_boundaries.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6MZ_CSV = TMP_DIR / f"{SLUG}_future_6mz_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MX = "layer_6_actuals_only_resume_plan_audit_complete"
DIAGNOSIS_6MY = "layer_6_historical_actuals_source_validation_plan_complete"
RECOMMENDED_NEXT_LAYER_6MY = "6MZ_layer_6_historical_actuals_source_validation_plan_audit"
RECOMMENDED_PATH_6MY = "audit_historical_actuals_source_validation_plan_before_implementation"


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
    json_6mx = load_json(JSON_6MX)

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
        {"check": "6mx_script_exists", "expected": True, "actual": SCRIPT_6MX.exists(), "passed": SCRIPT_6MX.exists()},
        {"check": "6mx_json_exists", "expected": True, "actual": JSON_6MX.exists(), "passed": JSON_6MX.exists()},
        {"check": "6mx_all_checks_passed", "expected": True, "actual": json_6mx.get("all_checks_passed"), "passed": json_6mx.get("all_checks_passed") is True},
        {"check": "6mx_diagnosis", "expected": DIAGNOSIS_6MX, "actual": json_6mx.get("diagnosis"), "passed": json_6mx.get("diagnosis") == DIAGNOSIS_6MX},
        {"check": "6mx_recommended_next_layer", "expected": "6MY_layer_6_historical_actuals_source_validation_plan", "actual": json_6mx.get("recommended_next_layer"), "passed": json_6mx.get("recommended_next_layer") == "6MY_layer_6_historical_actuals_source_validation_plan"},
        {"check": "actuals_source_validation_plan_allowed_next", "expected": True, "actual": json_6mx.get("actuals_source_validation_plan_allowed_next"), "passed": json_6mx.get("actuals_source_validation_plan_allowed_next") is True},
        {"check": "historical_moneyline_deferral_audited", "expected": True, "actual": json_6mx.get("historical_moneyline_deferral_audited"), "passed": json_6mx.get("historical_moneyline_deferral_audited") is True},
        {"check": "metric_execution_allowed_next", "expected": False, "actual": json_6mx.get("metric_execution_allowed_next"), "passed": json_6mx.get("metric_execution_allowed_next") is False},
        {"check": "backtest_execution_allowed_next", "expected": False, "actual": json_6mx.get("backtest_execution_allowed_next"), "passed": json_6mx.get("backtest_execution_allowed_next") is False},
    ]

    accepted_locations_rows = [
        {"source_type": "historical_actuals", "accepted_location": "data/local/historical_actuals.csv", "required": False, "passed": True},
        {"source_type": "historical_actuals", "accepted_location": "data/local/historical_actuals/*.csv", "required": False, "passed": True},
        {"source_type": "forbidden_as_source_authority", "accepted_location": "tmp/*", "required": False, "passed": True},
    ]

    required_schema_rows = [
        {"canonical_field": "game_pk", "required": True, "type_expectation": "string_or_integer_game_identifier", "passed": True},
        {"canonical_field": "game_date", "required": True, "type_expectation": "date_or_iso_date_string", "passed": True},
        {"canonical_field": "home_team", "required": True, "type_expectation": "non_empty_string", "passed": True},
        {"canonical_field": "away_team", "required": True, "type_expectation": "non_empty_string", "passed": True},
        {"canonical_field": "home_score", "required": True, "type_expectation": "integer_nonnegative", "passed": True},
        {"canonical_field": "away_score", "required": True, "type_expectation": "integer_nonnegative", "passed": True},
        {"canonical_field": "home_win_binary", "required": True, "type_expectation": "0_or_1_consistent_with_home_score_gt_away_score", "passed": True},
        {"canonical_field": "source_artifact", "required": True, "type_expectation": "non_empty_source_or_provenance_identifier", "passed": True},
    ]

    alias_candidates_rows = [
        {"canonical_field": "game_pk", "alias_candidate": "game_pk", "priority": 1, "passed": True},
        {"canonical_field": "game_pk", "alias_candidate": "game_id", "priority": 2, "passed": True},
        {"canonical_field": "game_pk", "alias_candidate": "mlb_game_pk", "priority": 3, "passed": True},
        {"canonical_field": "game_date", "alias_candidate": "game_date", "priority": 1, "passed": True},
        {"canonical_field": "game_date", "alias_candidate": "date", "priority": 2, "passed": True},
        {"canonical_field": "home_team", "alias_candidate": "home_team", "priority": 1, "passed": True},
        {"canonical_field": "home_team", "alias_candidate": "home_name", "priority": 2, "passed": True},
        {"canonical_field": "away_team", "alias_candidate": "away_team", "priority": 1, "passed": True},
        {"canonical_field": "away_team", "alias_candidate": "away_name", "priority": 2, "passed": True},
        {"canonical_field": "home_score", "alias_candidate": "home_score", "priority": 1, "passed": True},
        {"canonical_field": "home_score", "alias_candidate": "home_runs", "priority": 2, "passed": True},
        {"canonical_field": "away_score", "alias_candidate": "away_score", "priority": 1, "passed": True},
        {"canonical_field": "away_score", "alias_candidate": "away_runs", "priority": 2, "passed": True},
        {"canonical_field": "home_win_binary", "alias_candidate": "home_win_binary", "priority": 1, "passed": True},
        {"canonical_field": "home_win_binary", "alias_candidate": "home_win", "priority": 2, "passed": True},
        {"canonical_field": "source_artifact", "alias_candidate": "source_artifact", "priority": 1, "passed": True},
        {"canonical_field": "source_artifact", "alias_candidate": "source_file", "priority": 2, "passed": True},
        {"canonical_field": "source_artifact", "alias_candidate": "provenance", "priority": 3, "passed": True},
    ]

    validation_checks_rows = [
        {"validation_check": "source_file_location_under_data_local", "blocks_on_failure": True, "passed": True},
        {"validation_check": "required_or_alias_fields_present", "blocks_on_failure": True, "passed": True},
        {"validation_check": "game_pk_non_null", "blocks_on_failure": True, "passed": True},
        {"validation_check": "game_pk_unique_or_duplicate_explained", "blocks_on_failure": True, "passed": True},
        {"validation_check": "game_date_parseable", "blocks_on_failure": True, "passed": True},
        {"validation_check": "home_team_non_empty", "blocks_on_failure": True, "passed": True},
        {"validation_check": "away_team_non_empty", "blocks_on_failure": True, "passed": True},
        {"validation_check": "home_score_integer_nonnegative", "blocks_on_failure": True, "passed": True},
        {"validation_check": "away_score_integer_nonnegative", "blocks_on_failure": True, "passed": True},
        {"validation_check": "home_win_binary_is_0_or_1", "blocks_on_failure": True, "passed": True},
        {"validation_check": "home_win_binary_matches_home_score_gt_away_score", "blocks_on_failure": True, "passed": True},
        {"validation_check": "source_artifact_non_empty", "blocks_on_failure": True, "passed": True},
        {"validation_check": "row_count_positive", "blocks_on_failure": True, "passed": True},
        {"validation_check": "tie_games_handled_or_blocked", "blocks_on_failure": True, "passed": True},
    ]

    blocking_conditions_rows = [
        {"condition": "no_actuals_source_file_found", "blocks": True, "passed": True},
        {"condition": "actuals_source_outside_data_local", "blocks": True, "passed": True},
        {"condition": "required_schema_or_alias_missing", "blocks": True, "passed": True},
        {"condition": "invalid_or_null_game_pk", "blocks": True, "passed": True},
        {"condition": "invalid_or_null_game_date", "blocks": True, "passed": True},
        {"condition": "invalid_or_null_team_fields", "blocks": True, "passed": True},
        {"condition": "invalid_or_negative_scores", "blocks": True, "passed": True},
        {"condition": "home_win_binary_mismatch", "blocks": True, "passed": True},
        {"condition": "missing_source_artifact_or_provenance", "blocks": True, "passed": True},
        {"condition": "unresolved_duplicate_game_pk", "blocks": True, "passed": True},
    ]

    allowed_outputs_rows = [
        {"artifact": "validation_summary_json", "allowed": True, "production_table": False, "passed": True},
        {"artifact": "checks_csv", "allowed": True, "production_table": False, "passed": True},
        {"artifact": "schema_mapping_csv", "allowed": True, "production_table": False, "passed": True},
        {"artifact": "invalid_rows_sample_csv", "allowed": True, "production_table": False, "passed": True},
        {"artifact": "duplicate_game_pk_review_csv", "allowed": True, "production_table": False, "passed": True},
        {"artifact": "provenance_review_csv", "allowed": True, "production_table": False, "passed": True},
    ]

    moneyline_boundary_rows = [
        {"boundary": "historical_moneyline_validation", "status": "deferred", "passed": True},
        {"boundary": "market_comparison_metrics", "status": "blocked", "passed": True},
        {"boundary": "roi_clv_market_edge_claims", "status": "blocked", "passed": True},
        {"boundary": "actuals_only_validation", "status": "allowed_after_actuals_source_validation", "passed": True},
    ]

    allowed_next_rows = [
        {"operation": "audit_historical_actuals_source_validation_plan", "allowed_next": True, "scope": "6MZ audit only", "passed": True},
    ]

    forbidden_next_rows = [
        {"operation": "actuals_source_validation_implementation", "allowed_next": False, "passed": True},
        {"operation": "metric_execution", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
    ]

    future_6mz_rows = [
        {"contract": "audit_accepted_actuals_locations", "required": True, "passed": True},
        {"contract": "audit_required_actuals_schema_and_aliases", "required": True, "passed": True},
        {"contract": "audit_validation_checks_and_blockers", "required": True, "passed": True},
        {"contract": "preserve_moneyline_deferral_and_no_metric_execution", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6mx_passed", "expected": True, "actual": json_6mx.get("all_checks_passed"), "passed": json_6mx.get("all_checks_passed") is True},
        {"decision": "6mx_diagnosis_valid", "expected": DIAGNOSIS_6MX, "actual": json_6mx.get("diagnosis"), "passed": json_6mx.get("diagnosis") == DIAGNOSIS_6MX},
        {"decision": "all_required_6mx_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "accepted_actuals_locations_documented", "expected": True, "actual": all_passed(accepted_locations_rows), "passed": all_passed(accepted_locations_rows)},
        {"decision": "required_actuals_schema_documented", "expected": True, "actual": all_passed(required_schema_rows), "passed": all_passed(required_schema_rows)},
        {"decision": "alias_candidates_documented", "expected": True, "actual": all_passed(alias_candidates_rows), "passed": all_passed(alias_candidates_rows)},
        {"decision": "validation_checks_documented", "expected": True, "actual": all_passed(validation_checks_rows), "passed": all_passed(validation_checks_rows)},
        {"decision": "blocking_conditions_documented", "expected": True, "actual": all_passed(blocking_conditions_rows), "passed": all_passed(blocking_conditions_rows)},
        {"decision": "allowed_outputs_documented", "expected": True, "actual": all_passed(allowed_outputs_rows), "passed": all_passed(allowed_outputs_rows)},
        {"decision": "moneyline_deferral_boundaries_preserved", "expected": True, "actual": all_passed(moneyline_boundary_rows), "passed": all_passed(moneyline_boundary_rows)},
        {"decision": "recommend_6mz_next", "expected": RECOMMENDED_NEXT_LAYER_6MY, "actual": RECOMMENDED_NEXT_LAYER_6MY, "passed": True},
        {"decision": "do_not_execute_metrics_backtest_tune_activate_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only_historical_actuals_source_validation", "expected": True, "actual": True, "passed": True},
        {"boundary": "local_source_files_checked_by_6my", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_source_files_read_by_6my", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_rows_ingested_by_6my", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6my", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6my", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6my", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6my", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6my", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MY, "actual": RECOMMENDED_NEXT_LAYER_6MY, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MY, "actual": RECOMMENDED_PATH_6MY, "passed": True},
        {"decision": "do_not_recommend_source_validation_implementation_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MY, "actual": DIAGNOSIS_6MY, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "accepted_locations", "passed": all_passed(accepted_locations_rows), "detail": f"{sum(1 for r in accepted_locations_rows if r['passed'])}/{len(accepted_locations_rows)}"},
        {"check": "required_schema", "passed": all_passed(required_schema_rows), "detail": f"{sum(1 for r in required_schema_rows if r['passed'])}/{len(required_schema_rows)}"},
        {"check": "alias_candidates", "passed": all_passed(alias_candidates_rows), "detail": f"{sum(1 for r in alias_candidates_rows if r['passed'])}/{len(alias_candidates_rows)}"},
        {"check": "validation_checks", "passed": all_passed(validation_checks_rows), "detail": f"{sum(1 for r in validation_checks_rows if r['passed'])}/{len(validation_checks_rows)}"},
        {"check": "blocking_conditions", "passed": all_passed(blocking_conditions_rows), "detail": f"{sum(1 for r in blocking_conditions_rows if r['passed'])}/{len(blocking_conditions_rows)}"},
        {"check": "allowed_outputs", "passed": all_passed(allowed_outputs_rows), "detail": f"{sum(1 for r in allowed_outputs_rows if r['passed'])}/{len(allowed_outputs_rows)}"},
        {"check": "moneyline_deferral_boundaries", "passed": all_passed(moneyline_boundary_rows), "detail": f"{sum(1 for r in moneyline_boundary_rows if r['passed'])}/{len(moneyline_boundary_rows)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next_rows), "detail": f"{sum(1 for r in allowed_next_rows if r['passed'])}/{len(allowed_next_rows)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next_rows), "detail": f"{sum(1 for r in forbidden_next_rows if r['passed'])}/{len(forbidden_next_rows)}"},
        {"check": "future_6mz_contract", "passed": all_passed(future_6mz_rows), "detail": f"{sum(1 for r in future_6mz_rows if r['passed'])}/{len(future_6mz_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "accepted_locations": write_csv(ACCEPTED_LOCATIONS_CSV, accepted_locations_rows),
        "required_schema": write_csv(REQUIRED_SCHEMA_CSV, required_schema_rows),
        "alias_candidates": write_csv(ALIAS_CANDIDATES_CSV, alias_candidates_rows),
        "validation_checks": write_csv(VALIDATION_CHECKS_CSV, validation_checks_rows),
        "blocking_conditions": write_csv(BLOCKING_CONDITIONS_CSV, blocking_conditions_rows),
        "allowed_outputs": write_csv(ALLOWED_OUTPUTS_CSV, allowed_outputs_rows),
        "moneyline_deferral_boundaries": write_csv(MONEYLINE_BOUNDARIES_CSV, moneyline_boundary_rows),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next_rows),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next_rows),
        "future_6mz_contract": write_csv(FUTURE_6MZ_CSV, future_6mz_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MY",
        "layer_type": "game_mechanics_realism",
        "planning_only_historical_actuals_source_validation": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MY if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MY,
        "recommended_path": RECOMMENDED_PATH_6MY,
        "predecessor_layer": "6MX",
        "predecessor_diagnosis": json_6mx.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mx.get("all_checks_passed") is True,
        "planned_layer_after": "6MX",
        "source_family": "historical_actuals_source_validation_plan",
        "accepted_actuals_locations_documented": True,
        "required_actuals_schema_documented": True,
        "alias_candidates_documented": True,
        "validation_checks_documented": True,
        "blocking_conditions_documented": True,
        "allowed_outputs_documented": True,
        "moneyline_deferral_boundaries_preserved": True,
        "historical_actuals_source_validation_plan_audit_allowed_next": True,
        "source_ingestion_allowed_next": False,
        "source_implementation_allowed_next": False,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "local_source_files_checked_by_6my": False,
        "local_source_files_read_by_6my": False,
        "source_rows_ingested_by_6my": False,
        "normalized_source_tables_created_for_production_by_6my": False,
        "production_code_modified_by_6my": False,
        "adapter_call_executed_by_6my": False,
        "metric_execution_run_by_6my": False,
        "backtest_execution_run_by_6my": False,
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
            "accepted_locations_csv": str(ACCEPTED_LOCATIONS_CSV),
            "required_schema_csv": str(REQUIRED_SCHEMA_CSV),
            "alias_candidates_csv": str(ALIAS_CANDIDATES_CSV),
            "validation_checks_csv": str(VALIDATION_CHECKS_CSV),
            "blocking_conditions_csv": str(BLOCKING_CONDITIONS_CSV),
            "allowed_outputs_csv": str(ALLOWED_OUTPUTS_CSV),
            "moneyline_deferral_boundaries_csv": str(MONEYLINE_BOUNDARIES_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6mz_contract_csv": str(FUTURE_6MZ_CSV),
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
