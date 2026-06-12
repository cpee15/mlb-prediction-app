#!/usr/bin/env python3
"""Plan Layer 6 wait state until historical actuals and moneyline source files are supplied."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mt_projection_adapter_historical_actuals_moneyline_wait_state_plan"
TMP_DIR = Path("tmp")

SCRIPT_6MS = Path("scripts/audit_6ms_layer6_projection_adapter_historical_actuals_moneyline_ingestion_validation.py")
JSON_6MS = TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit.json"

REQUIRED_INPUTS = [
    JSON_6MS,
    TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit_checks.csv",
    TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit_predecessor.csv",
    TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit_presence_review.csv",
    TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit_schema_review.csv",
    TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit_provenance_authority_review.csv",
    TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit_missing_duplicate_review.csv",
    TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit_preview_review.csv",
    TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit_blocker_review.csv",
    TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit_allowed_operations_next.csv",
    TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit_future_6mt_contract.csv",
    TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit_decision.csv",
    TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit_recommended_path.csv",
    SCRIPT_6MS,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CURRENT_STATE_CSV = TMP_DIR / f"{SLUG}_current_state.csv"
REQUIRED_FILES_CSV = TMP_DIR / f"{SLUG}_required_files.csv"
ACTUALS_SCHEMA_CSV = TMP_DIR / f"{SLUG}_actuals_schema.csv"
MONEYLINE_SCHEMA_CSV = TMP_DIR / f"{SLUG}_moneyline_schema.csv"
PROVENANCE_REQ_CSV = TMP_DIR / f"{SLUG}_provenance_requirements.csv"
RESUME_CONDITIONS_CSV = TMP_DIR / f"{SLUG}_resume_conditions.csv"
VALIDATION_COMMANDS_CSV = TMP_DIR / f"{SLUG}_validation_commands.csv"
FORBIDDEN_WAITING_CSV = TMP_DIR / f"{SLUG}_forbidden_while_waiting.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6MU_CSV = TMP_DIR / f"{SLUG}_future_6mu_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MS = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_ingestion_audit_complete"
DIAGNOSIS_6MT = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_wait_state_plan_complete"
RECOMMENDED_NEXT_LAYER_6MT = "6MU_layer_6_projection_adapter_historical_actuals_and_moneyline_source_wait_state_audit"
RECOMMENDED_PATH_6MT = "audit_wait_state_before_pausing_layer_6_until_sources_are_supplied"


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
    json_6ms = load_json(JSON_6MS)

    actuals_count = int(json_6ms.get("actuals_source_files_found_count_confirmed", -1))
    moneyline_count = int(json_6ms.get("moneyline_source_files_found_count_confirmed", -1))

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
        {"check": "6ms_script_exists", "expected": True, "actual": SCRIPT_6MS.exists(), "passed": SCRIPT_6MS.exists()},
        {"check": "6ms_json_exists", "expected": True, "actual": JSON_6MS.exists(), "passed": JSON_6MS.exists()},
        {"check": "6ms_all_checks_passed", "expected": True, "actual": json_6ms.get("all_checks_passed"), "passed": json_6ms.get("all_checks_passed") is True},
        {"check": "6ms_diagnosis", "expected": DIAGNOSIS_6MS, "actual": json_6ms.get("diagnosis"), "passed": json_6ms.get("diagnosis") == DIAGNOSIS_6MS},
        {"check": "6ms_recommended_next_layer", "expected": "6MT_layer_6_projection_adapter_historical_actuals_and_moneyline_source_wait_state_plan", "actual": json_6ms.get("recommended_next_layer"), "passed": json_6ms.get("recommended_next_layer") == "6MT_layer_6_projection_adapter_historical_actuals_and_moneyline_source_wait_state_plan"},
        {"check": "wait_state_planning_allowed_next", "expected": True, "actual": json_6ms.get("wait_state_planning_allowed_next"), "passed": json_6ms.get("wait_state_planning_allowed_next") is True},
        {"check": "metric_execution_allowed_next", "expected": False, "actual": json_6ms.get("metric_execution_allowed_next"), "passed": json_6ms.get("metric_execution_allowed_next") is False},
        {"check": "backtest_execution_allowed_next", "expected": False, "actual": json_6ms.get("backtest_execution_allowed_next"), "passed": json_6ms.get("backtest_execution_allowed_next") is False},
    ]

    current_state = [
        {"state_item": "layer_6_status", "value": "blocked_waiting_for_local_historical_sources", "passed": True},
        {"state_item": "actuals_source_files_found_count", "value": actuals_count, "expected": 0, "passed": actuals_count == 0},
        {"state_item": "moneyline_source_files_found_count", "value": moneyline_count, "expected": 0, "passed": moneyline_count == 0},
        {"state_item": "expected_missing_source_blockers", "value": json_6ms.get("expected_missing_source_blockers_confirmed"), "expected": True, "passed": json_6ms.get("expected_missing_source_blockers_confirmed") is True},
        {"state_item": "metrics_backtests_tuning_activation_exit", "value": "blocked", "passed": True},
    ]

    required_files = [
        {"source_type": "actuals", "accepted_location": "data/local/historical_actuals.csv", "required_before_resume": True, "passed": True},
        {"source_type": "actuals", "accepted_location": "data/local/historical_actuals/*.csv", "required_before_resume": True, "passed": True},
        {"source_type": "moneyline", "accepted_location": "data/local/historical_moneyline_odds.csv", "required_before_resume": True, "passed": True},
        {"source_type": "moneyline", "accepted_location": "data/local/historical_moneyline_odds/*.csv", "required_before_resume": True, "passed": True},
        {"source_type": "forbidden", "accepted_location": "tmp/*", "required_before_resume": False, "passed": True},
    ]

    actuals_schema = [
        {"canonical_field": "game_pk", "aliases": "game_pk,game_id,mlb_game_id,event_id", "required": True, "passed": True},
        {"canonical_field": "game_date", "aliases": "game_date,date,official_date", "required": True, "passed": True},
        {"canonical_field": "home_team", "aliases": "home_team,home,home_abbrev,home_team_abbrev", "required": True, "passed": True},
        {"canonical_field": "away_team", "aliases": "away_team,away,away_abbrev,away_team_abbrev", "required": True, "passed": True},
        {"canonical_field": "home_score", "aliases": "home_score,home_runs,home_final_score", "required": True, "passed": True},
        {"canonical_field": "away_score", "aliases": "away_score,away_runs,away_final_score", "required": True, "passed": True},
        {"canonical_field": "home_win_binary", "aliases": "home_win_binary,home_win,actual_home_win,winner_side", "required": True, "passed": True},
        {"canonical_field": "source_artifact", "aliases": "source_artifact,source_file,provenance", "required": True, "passed": True},
    ]

    moneyline_schema = [
        {"canonical_field": "game_pk", "aliases": "game_pk,game_id,mlb_game_id,event_id", "required": True, "passed": True},
        {"canonical_field": "game_date", "aliases": "game_date,date,odds_date", "required": True, "passed": True},
        {"canonical_field": "home_team", "aliases": "home_team,home,home_abbrev,home_team_abbrev", "required": True, "passed": True},
        {"canonical_field": "away_team", "aliases": "away_team,away,away_abbrev,away_team_abbrev", "required": True, "passed": True},
        {"canonical_field": "home_moneyline", "aliases": "home_moneyline,home_ml,moneyline_home,home_close_moneyline", "required": True, "passed": True},
        {"canonical_field": "source_artifact", "aliases": "source_artifact,source_file,provenance", "required": True, "passed": True},
        {"canonical_field": "away_moneyline", "aliases": "away_moneyline,away_ml,moneyline_away,away_close_moneyline", "required": False, "passed": True},
        {"canonical_field": "odds_timestamp_or_type", "aliases": "odds_timestamp_or_type,snapshot_type,open_close,market_time", "required": False, "passed": True},
        {"canonical_field": "sportsbook_or_source", "aliases": "sportsbook_or_source,book,source_book", "required": False, "passed": True},
    ]

    provenance_requirements = [
        {"requirement": "source_artifact_or_source_file_column", "required": True, "why": "trace every row to user-supplied local source", "passed": True},
        {"requirement": "source_must_be_under_data_local", "required": True, "why": "tmp artifacts are not source authority", "passed": True},
        {"requirement": "tmp_outputs_forbidden_as_source", "required": True, "why": "generated artifacts cannot become historical truth", "passed": True},
        {"requirement": "unknown_source_authority_blocks_resume", "required": True, "why": "avoid untrusted or synthetic truth source", "passed": True},
    ]

    resume_conditions = [
        {"condition": "actuals_file_supplied", "required": True, "how_to_satisfy": "place CSV at accepted actuals location", "passed": True},
        {"condition": "moneyline_file_supplied", "required": True, "how_to_satisfy": "place CSV at accepted moneyline location", "passed": True},
        {"condition": "required_actuals_schema_validates", "required": True, "how_to_satisfy": "include required canonical fields or aliases", "passed": True},
        {"condition": "required_moneyline_schema_validates", "required": True, "how_to_satisfy": "include required canonical fields or aliases", "passed": True},
        {"condition": "source_artifact_provenance_present", "required": True, "how_to_satisfy": "include source_artifact/source_file/provenance column", "passed": True},
        {"condition": "no_unresolved_schema_or_source_blockers", "required": True, "how_to_satisfy": "rerun 6MR-style validation and confirm blocker-free state", "passed": True},
    ]

    validation_commands = [
        {"step": "sync_main", "command": "git checkout main && git fetch upstream main && git reset --hard upstream/main", "passed": True},
        {"step": "rerun_presence_schema_validation", "command": "python scripts/implement_6mr_layer6_projection_adapter_historical_actuals_moneyline_ingestion_validation.py", "passed": True},
        {"step": "audit_validation", "command": "python scripts/audit_6ms_layer6_projection_adapter_historical_actuals_moneyline_ingestion_validation.py", "passed": True},
        {"step": "only_after_clean_validation", "command": "plan future metric-readiness layer; do not run metrics directly from wait state", "passed": True},
    ]

    forbidden_waiting = [
        {"operation": "source_ingestion_into_production_tables", "forbidden": True, "passed": True},
        {"operation": "historical_metric_execution", "forbidden": True, "passed": True},
        {"operation": "historical_backtest", "forbidden": True, "passed": True},
        {"operation": "tuning", "forbidden": True, "passed": True},
        {"operation": "mechanics_activation", "forbidden": True, "passed": True},
        {"operation": "layer_6_exit", "forbidden": True, "passed": True},
        {"operation": "remote_api_or_live_data_fetch", "forbidden": True, "passed": True},
    ]

    allowed_next = [
        {"operation": "audit_wait_state_documentation", "allowed_next": True, "scope": "6MU audit only", "passed": True},
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

    future_6mu = [
        {"contract": "audit_wait_state_current_status", "required": True, "passed": True},
        {"contract": "audit_required_files_and_schema_documentation", "required": True, "passed": True},
        {"contract": "audit_resume_conditions", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_no_backtests_until_sources_exist", "required": True, "passed": True},
    ]

    blocking_policy = [
        {"policy": "layer_6_remains_blocked_until_actuals_and_moneyline_files_validate", "required": True, "passed": True},
        {"policy": "do_not_use_tmp_outputs_as_historical_truth", "required": True, "passed": True},
        {"policy": "do_not_execute_metrics_until_actuals_and_moneyline_sources_validate", "required": True, "passed": True},
        {"policy": "do_not_grant_layer_6_exit_without_historical_source_validation", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6ms_passed", "expected": True, "actual": json_6ms.get("all_checks_passed"), "passed": json_6ms.get("all_checks_passed") is True},
        {"decision": "6ms_diagnosis_valid", "expected": DIAGNOSIS_6MS, "actual": json_6ms.get("diagnosis"), "passed": json_6ms.get("diagnosis") == DIAGNOSIS_6MS},
        {"decision": "all_required_6ms_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "actuals_source_count_zero_confirmed", "expected": 0, "actual": actuals_count, "passed": actuals_count == 0},
        {"decision": "moneyline_source_count_zero_confirmed", "expected": 0, "actual": moneyline_count, "passed": moneyline_count == 0},
        {"decision": "current_wait_state_documented", "expected": True, "actual": all_passed(current_state), "passed": all_passed(current_state)},
        {"decision": "required_files_documented", "expected": True, "actual": all_passed(required_files), "passed": all_passed(required_files)},
        {"decision": "schemas_documented", "expected": True, "actual": all_passed(actuals_schema) and all_passed(moneyline_schema), "passed": all_passed(actuals_schema) and all_passed(moneyline_schema)},
        {"decision": "resume_conditions_documented", "expected": True, "actual": all_passed(resume_conditions), "passed": all_passed(resume_conditions)},
        {"decision": "forbidden_while_waiting_documented", "expected": True, "actual": all_passed(forbidden_waiting), "passed": all_passed(forbidden_waiting)},
        {"decision": "recommend_6mu_next", "expected": RECOMMENDED_NEXT_LAYER_6MT, "actual": RECOMMENDED_NEXT_LAYER_6MT, "passed": True},
        {"decision": "do_not_execute_metrics_backtest_tune_activate_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only_wait_state_documentation", "expected": True, "actual": True, "passed": True},
        {"boundary": "local_source_files_checked_by_6mt", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_source_files_read_by_6mt", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_rows_ingested_by_6mt", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6mt", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mt", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mt", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6mt", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mt", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MT, "actual": RECOMMENDED_NEXT_LAYER_6MT, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MT, "actual": RECOMMENDED_PATH_6MT, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MT, "actual": DIAGNOSIS_6MT, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "current_state", "passed": all_passed(current_state), "detail": f"{sum(1 for r in current_state if r['passed'])}/{len(current_state)}"},
        {"check": "required_files", "passed": all_passed(required_files), "detail": f"{sum(1 for r in required_files if r['passed'])}/{len(required_files)}"},
        {"check": "actuals_schema", "passed": all_passed(actuals_schema), "detail": f"{sum(1 for r in actuals_schema if r['passed'])}/{len(actuals_schema)}"},
        {"check": "moneyline_schema", "passed": all_passed(moneyline_schema), "detail": f"{sum(1 for r in moneyline_schema if r['passed'])}/{len(moneyline_schema)}"},
        {"check": "provenance_requirements", "passed": all_passed(provenance_requirements), "detail": f"{sum(1 for r in provenance_requirements if r['passed'])}/{len(provenance_requirements)}"},
        {"check": "resume_conditions", "passed": all_passed(resume_conditions), "detail": f"{sum(1 for r in resume_conditions if r['passed'])}/{len(resume_conditions)}"},
        {"check": "validation_commands", "passed": all_passed(validation_commands), "detail": f"{sum(1 for r in validation_commands if r['passed'])}/{len(validation_commands)}"},
        {"check": "forbidden_while_waiting", "passed": all_passed(forbidden_waiting), "detail": f"{sum(1 for r in forbidden_waiting if r['passed'])}/{len(forbidden_waiting)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next), "detail": f"{sum(1 for r in allowed_next if r['passed'])}/{len(allowed_next)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next), "detail": f"{sum(1 for r in forbidden_next if r['passed'])}/{len(forbidden_next)}"},
        {"check": "future_6mu_contract", "passed": all_passed(future_6mu), "detail": f"{sum(1 for r in future_6mu if r['passed'])}/{len(future_6mu)}"},
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
        "current_state": write_csv(CURRENT_STATE_CSV, current_state),
        "required_files": write_csv(REQUIRED_FILES_CSV, required_files),
        "actuals_schema": write_csv(ACTUALS_SCHEMA_CSV, actuals_schema),
        "moneyline_schema": write_csv(MONEYLINE_SCHEMA_CSV, moneyline_schema),
        "provenance_requirements": write_csv(PROVENANCE_REQ_CSV, provenance_requirements),
        "resume_conditions": write_csv(RESUME_CONDITIONS_CSV, resume_conditions),
        "validation_commands": write_csv(VALIDATION_COMMANDS_CSV, validation_commands),
        "forbidden_while_waiting": write_csv(FORBIDDEN_WAITING_CSV, forbidden_waiting),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "future_6mu_contract": write_csv(FUTURE_6MU_CSV, future_6mu),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MT",
        "layer_type": "game_mechanics_realism",
        "planning_only_wait_state_documentation": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MT if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MT,
        "recommended_path": RECOMMENDED_PATH_6MT,
        "predecessor_layer": "6MS",
        "predecessor_diagnosis": json_6ms.get("diagnosis"),
        "predecessor_all_checks_passed": json_6ms.get("all_checks_passed") is True,
        "planned_layer_after": "6MS",
        "source_family": "projection_adapter_historical_actuals_moneyline_source_wait_state_plan",
        "wait_state_documented": True,
        "local_sources_missing_confirmed_from_6ms": actuals_count == 0 and moneyline_count == 0,
        "actuals_source_files_found_count_confirmed_from_6ms": actuals_count,
        "moneyline_source_files_found_count_confirmed_from_6ms": moneyline_count,
        "required_file_locations_documented": True,
        "actuals_schema_documented": True,
        "moneyline_schema_documented": True,
        "provenance_requirements_documented": True,
        "resume_conditions_documented": True,
        "validation_commands_documented": True,
        "forbidden_while_waiting_documented": True,
        "wait_state_audit_allowed_next": True,
        "source_ingestion_allowed_next": False,
        "source_implementation_allowed_next": False,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "local_source_files_checked_by_6mt": False,
        "local_source_files_read_by_6mt": False,
        "source_rows_ingested_by_6mt": False,
        "normalized_source_tables_created_for_production_by_6mt": False,
        "production_code_modified_by_6mt": False,
        "adapter_call_executed_by_6mt": False,
        "metric_execution_run_by_6mt": False,
        "backtest_execution_run_by_6mt": False,
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
            "current_state_csv": str(CURRENT_STATE_CSV),
            "required_files_csv": str(REQUIRED_FILES_CSV),
            "actuals_schema_csv": str(ACTUALS_SCHEMA_CSV),
            "moneyline_schema_csv": str(MONEYLINE_SCHEMA_CSV),
            "provenance_requirements_csv": str(PROVENANCE_REQ_CSV),
            "resume_conditions_csv": str(RESUME_CONDITIONS_CSV),
            "validation_commands_csv": str(VALIDATION_COMMANDS_CSV),
            "forbidden_while_waiting_csv": str(FORBIDDEN_WAITING_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6mu_contract_csv": str(FUTURE_6MU_CSV),
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
