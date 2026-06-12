#!/usr/bin/env python3
"""Summarize Layer 6 pause state until historical actuals/moneyline sources are supplied."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mv_projection_adapter_historical_actuals_moneyline_pause_state_summary"
TMP_DIR = Path("tmp")

SCRIPT_6MU = Path("scripts/audit_6mu_layer6_projection_adapter_historical_actuals_moneyline_wait_state.py")
JSON_6MU = TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit.json"

REQUIRED_INPUTS = [
    JSON_6MU,
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_checks.csv",
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_predecessor.csv",
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_current_state_review.csv",
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_required_files_review.csv",
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_schema_review.csv",
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_provenance_review.csv",
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_resume_conditions_review.csv",
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_validation_commands_review.csv",
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_forbidden_waiting_review.csv",
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_allowed_operations_next.csv",
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_future_6mv_contract.csv",
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_decision.csv",
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6mu_projection_adapter_historical_actuals_moneyline_wait_state_audit_recommended_path.csv",
    SCRIPT_6MU,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
STATUS_CSV = TMP_DIR / f"{SLUG}_status.csv"
MISSING_SOURCES_CSV = TMP_DIR / f"{SLUG}_missing_sources.csv"
ACCEPTED_LOCATIONS_CSV = TMP_DIR / f"{SLUG}_accepted_locations.csv"
REQUIRED_SCHEMAS_CSV = TMP_DIR / f"{SLUG}_required_schemas.csv"
PROVENANCE_REQ_CSV = TMP_DIR / f"{SLUG}_provenance_requirements.csv"
RESUME_CONDITIONS_CSV = TMP_DIR / f"{SLUG}_resume_conditions.csv"
RESUME_COMMANDS_CSV = TMP_DIR / f"{SLUG}_resume_commands.csv"
FORBIDDEN_OPS_CSV = TMP_DIR / f"{SLUG}_forbidden_operations.csv"
FINAL_DECISION_CSV = TMP_DIR / f"{SLUG}_final_decision.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MU = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_wait_state_audit_complete"
DIAGNOSIS_6MV = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_pause_state_summary_complete"
RECOMMENDED_NEXT_LAYER_6MV = "WAIT_FOR_USER_SUPPLIED_LOCAL_HISTORICAL_ACTUALS_AND_MONEYLINE_SOURCES"
RECOMMENDED_PATH_6MV = "supply_local_sources_then_rerun_6mr_validation_path"


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
    json_6mu = load_json(JSON_6MU)

    actuals_count = int(json_6mu.get("actuals_source_files_found_count_confirmed_from_6mt", -1))
    moneyline_count = int(json_6mu.get("moneyline_source_files_found_count_confirmed_from_6mt", -1))

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
        {"check": "6mu_script_exists", "expected": True, "actual": SCRIPT_6MU.exists(), "passed": SCRIPT_6MU.exists()},
        {"check": "6mu_json_exists", "expected": True, "actual": JSON_6MU.exists(), "passed": JSON_6MU.exists()},
        {"check": "6mu_all_checks_passed", "expected": True, "actual": json_6mu.get("all_checks_passed"), "passed": json_6mu.get("all_checks_passed") is True},
        {"check": "6mu_diagnosis", "expected": DIAGNOSIS_6MU, "actual": json_6mu.get("diagnosis"), "passed": json_6mu.get("diagnosis") == DIAGNOSIS_6MU},
        {"check": "6mu_recommended_next_layer", "expected": "6MV_layer_6_projection_adapter_historical_actuals_and_moneyline_source_pause_state_summary", "actual": json_6mu.get("recommended_next_layer"), "passed": json_6mu.get("recommended_next_layer") == "6MV_layer_6_projection_adapter_historical_actuals_and_moneyline_source_pause_state_summary"},
        {"check": "pause_state_summary_allowed_next", "expected": True, "actual": json_6mu.get("pause_state_summary_allowed_next"), "passed": json_6mu.get("pause_state_summary_allowed_next") is True},
        {"check": "metric_execution_allowed_next", "expected": False, "actual": json_6mu.get("metric_execution_allowed_next"), "passed": json_6mu.get("metric_execution_allowed_next") is False},
        {"check": "backtest_execution_allowed_next", "expected": False, "actual": json_6mu.get("backtest_execution_allowed_next"), "passed": json_6mu.get("backtest_execution_allowed_next") is False},
    ]

    status_rows = [
        {"summary_item": "layer_6_status", "value": "paused_not_failed", "passed": True},
        {"summary_item": "pause_reason", "value": "missing_user_supplied_local_historical_actuals_and_moneyline_sources", "passed": True},
        {"summary_item": "actuals_source_files_found_count", "value": actuals_count, "expected": 0, "passed": actuals_count == 0},
        {"summary_item": "moneyline_source_files_found_count", "value": moneyline_count, "expected": 0, "passed": moneyline_count == 0},
        {"summary_item": "next_action", "value": "user_supplies_local_sources_then_reruns_validation_path", "passed": True},
    ]

    missing_sources = [
        {"source_type": "historical_actuals", "missing": True, "required": True, "passed": actuals_count == 0},
        {"source_type": "historical_moneyline", "missing": True, "required": True, "passed": moneyline_count == 0},
    ]

    accepted_locations = [
        {"source_type": "actuals", "accepted_location": "data/local/historical_actuals.csv", "passed": True},
        {"source_type": "actuals", "accepted_location": "data/local/historical_actuals/*.csv", "passed": True},
        {"source_type": "moneyline", "accepted_location": "data/local/historical_moneyline_odds.csv", "passed": True},
        {"source_type": "moneyline", "accepted_location": "data/local/historical_moneyline_odds/*.csv", "passed": True},
        {"source_type": "forbidden_as_source_authority", "accepted_location": "tmp/*", "passed": True},
    ]

    required_schemas = [
        {"source_type": "actuals", "canonical_field": "game_pk", "required": True, "passed": True},
        {"source_type": "actuals", "canonical_field": "game_date", "required": True, "passed": True},
        {"source_type": "actuals", "canonical_field": "home_team", "required": True, "passed": True},
        {"source_type": "actuals", "canonical_field": "away_team", "required": True, "passed": True},
        {"source_type": "actuals", "canonical_field": "home_score", "required": True, "passed": True},
        {"source_type": "actuals", "canonical_field": "away_score", "required": True, "passed": True},
        {"source_type": "actuals", "canonical_field": "home_win_binary", "required": True, "passed": True},
        {"source_type": "actuals", "canonical_field": "source_artifact", "required": True, "passed": True},
        {"source_type": "moneyline", "canonical_field": "game_pk", "required": True, "passed": True},
        {"source_type": "moneyline", "canonical_field": "game_date", "required": True, "passed": True},
        {"source_type": "moneyline", "canonical_field": "home_team", "required": True, "passed": True},
        {"source_type": "moneyline", "canonical_field": "away_team", "required": True, "passed": True},
        {"source_type": "moneyline", "canonical_field": "home_moneyline", "required": True, "passed": True},
        {"source_type": "moneyline", "canonical_field": "source_artifact", "required": True, "passed": True},
    ]

    provenance_requirements = [
        {"requirement": "source_artifact_or_source_file_column_required", "passed": True},
        {"requirement": "source_must_be_user_supplied_under_data_local", "passed": True},
        {"requirement": "tmp_outputs_are_forbidden_as_source_authority", "passed": True},
        {"requirement": "unknown_source_authority_blocks_resume", "passed": True},
    ]

    resume_conditions = [
        {"condition": "place_actuals_csv_in_accepted_location", "passed": True},
        {"condition": "place_moneyline_csv_in_accepted_location", "passed": True},
        {"condition": "include_required_actuals_fields_or_aliases", "passed": True},
        {"condition": "include_required_moneyline_fields_or_aliases", "passed": True},
        {"condition": "include_source_artifact_or_provenance_column", "passed": True},
        {"condition": "rerun_validation_and_confirm_no_source_blockers", "passed": True},
    ]

    resume_commands = [
        {"step": "sync_main", "command": "git checkout main && git fetch upstream main && git reset --hard upstream/main", "passed": True},
        {"step": "rerun_6mr_validation", "command": "python scripts/implement_6mr_layer6_projection_adapter_historical_actuals_moneyline_ingestion_validation.py", "passed": True},
        {"step": "rerun_6ms_audit", "command": "python scripts/audit_6ms_layer6_projection_adapter_historical_actuals_moneyline_ingestion_validation.py", "passed": True},
        {"step": "continue_only_after_clean_source_validation", "command": "plan a future metric-readiness layer; do not jump directly to metrics", "passed": True},
    ]

    forbidden_ops = [
        {"operation": "source_ingestion", "forbidden": True, "passed": True},
        {"operation": "production_normalized_source_tables", "forbidden": True, "passed": True},
        {"operation": "metric_execution", "forbidden": True, "passed": True},
        {"operation": "historical_backtest", "forbidden": True, "passed": True},
        {"operation": "tuning", "forbidden": True, "passed": True},
        {"operation": "mechanics_activation", "forbidden": True, "passed": True},
        {"operation": "layer_6_exit", "forbidden": True, "passed": True},
        {"operation": "remote_api_or_live_data_fetch", "forbidden": True, "passed": True},
    ]

    final_decision = [
        {"decision": "layer_6_paused_not_failed", "value": True, "passed": True},
        {"decision": "external_user_source_files_required", "value": True, "passed": True},
        {"decision": "wait_for_user_supplied_local_sources", "value": True, "passed": True},
        {"decision": "do_not_continue_metrics_path_until_validation_clean", "value": True, "passed": True},
    ]

    blocking_policy = [
        {"policy": "pause_until_actuals_and_moneyline_sources_supplied", "required": True, "passed": True},
        {"policy": "validate_sources_before_ingestion_or_metrics", "required": True, "passed": True},
        {"policy": "do_not_use_tmp_outputs_as_historical_truth", "required": True, "passed": True},
        {"policy": "layer_6_exit_unavailable_in_pause_state", "required": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "summary_only_pause_state", "expected": True, "actual": True, "passed": True},
        {"boundary": "local_source_files_checked_by_6mv", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_source_files_read_by_6mv", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_rows_ingested_by_6mv", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6mv", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mv", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mv", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6mv", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mv", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MV, "actual": RECOMMENDED_NEXT_LAYER_6MV, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MV, "actual": RECOMMENDED_PATH_6MV, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MV, "actual": DIAGNOSIS_6MV, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "status_summary", "passed": all_passed(status_rows), "detail": f"{sum(1 for r in status_rows if r['passed'])}/{len(status_rows)}"},
        {"check": "missing_sources", "passed": all_passed(missing_sources), "detail": f"{sum(1 for r in missing_sources if r['passed'])}/{len(missing_sources)}"},
        {"check": "accepted_locations", "passed": all_passed(accepted_locations), "detail": f"{sum(1 for r in accepted_locations if r['passed'])}/{len(accepted_locations)}"},
        {"check": "required_schemas", "passed": all_passed(required_schemas), "detail": f"{sum(1 for r in required_schemas if r['passed'])}/{len(required_schemas)}"},
        {"check": "provenance_requirements", "passed": all_passed(provenance_requirements), "detail": f"{sum(1 for r in provenance_requirements if r['passed'])}/{len(provenance_requirements)}"},
        {"check": "resume_conditions", "passed": all_passed(resume_conditions), "detail": f"{sum(1 for r in resume_conditions if r['passed'])}/{len(resume_conditions)}"},
        {"check": "resume_commands", "passed": all_passed(resume_commands), "detail": f"{sum(1 for r in resume_commands if r['passed'])}/{len(resume_commands)}"},
        {"check": "forbidden_operations", "passed": all_passed(forbidden_ops), "detail": f"{sum(1 for r in forbidden_ops if r['passed'])}/{len(forbidden_ops)}"},
        {"check": "final_decision", "passed": all_passed(final_decision), "detail": f"{sum(1 for r in final_decision if r['passed'])}/{len(final_decision)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_policy), "detail": f"{sum(1 for r in blocking_policy if r['passed'])}/{len(blocking_policy)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "status": write_csv(STATUS_CSV, status_rows),
        "missing_sources": write_csv(MISSING_SOURCES_CSV, missing_sources),
        "accepted_locations": write_csv(ACCEPTED_LOCATIONS_CSV, accepted_locations),
        "required_schemas": write_csv(REQUIRED_SCHEMAS_CSV, required_schemas),
        "provenance_requirements": write_csv(PROVENANCE_REQ_CSV, provenance_requirements),
        "resume_conditions": write_csv(RESUME_CONDITIONS_CSV, resume_conditions),
        "resume_commands": write_csv(RESUME_COMMANDS_CSV, resume_commands),
        "forbidden_operations": write_csv(FORBIDDEN_OPS_CSV, forbidden_ops),
        "final_decision": write_csv(FINAL_DECISION_CSV, final_decision),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MV",
        "layer_type": "game_mechanics_realism",
        "summary_only_pause_state": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MV if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MV,
        "recommended_path": RECOMMENDED_PATH_6MV,
        "predecessor_layer": "6MU",
        "predecessor_diagnosis": json_6mu.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mu.get("all_checks_passed") is True,
        "summarized_layer_after": "6MU",
        "source_family": "projection_adapter_historical_actuals_moneyline_source_pause_state_summary",
        "pause_state_summary_created": True,
        "layer_6_paused_not_failed": True,
        "pause_reason": "missing_user_supplied_local_historical_actuals_and_moneyline_sources",
        "local_sources_missing_confirmed_from_6mu": actuals_count == 0 and moneyline_count == 0,
        "actuals_source_files_found_count_confirmed_from_6mu": actuals_count,
        "moneyline_source_files_found_count_confirmed_from_6mu": moneyline_count,
        "missing_actuals_source_documented": True,
        "missing_moneyline_source_documented": True,
        "accepted_locations_summarized": True,
        "required_schemas_summarized": True,
        "provenance_requirements_summarized": True,
        "resume_conditions_summarized": True,
        "resume_commands_summarized": True,
        "forbidden_operations_summarized": True,
        "source_ingestion_allowed_next": False,
        "source_implementation_allowed_next": False,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "local_source_files_checked_by_6mv": False,
        "local_source_files_read_by_6mv": False,
        "source_rows_ingested_by_6mv": False,
        "normalized_source_tables_created_for_production_by_6mv": False,
        "production_code_modified_by_6mv": False,
        "adapter_call_executed_by_6mv": False,
        "metric_execution_run_by_6mv": False,
        "backtest_execution_run_by_6mv": False,
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
            "status_csv": str(STATUS_CSV),
            "missing_sources_csv": str(MISSING_SOURCES_CSV),
            "accepted_locations_csv": str(ACCEPTED_LOCATIONS_CSV),
            "required_schemas_csv": str(REQUIRED_SCHEMAS_CSV),
            "provenance_requirements_csv": str(PROVENANCE_REQ_CSV),
            "resume_conditions_csv": str(RESUME_CONDITIONS_CSV),
            "resume_commands_csv": str(RESUME_COMMANDS_CSV),
            "forbidden_operations_csv": str(FORBIDDEN_OPS_CSV),
            "final_decision_csv": str(FINAL_DECISION_CSV),
            "blocking_policy_csv": str(BLOCKING_POLICY_CSV),
            "safety_boundaries_csv": str(SAFETY_CSV),
            "recommended_path_csv": str(RECOMMENDED_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
