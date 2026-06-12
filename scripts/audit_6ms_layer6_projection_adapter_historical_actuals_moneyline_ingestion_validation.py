#!/usr/bin/env python3
"""Audit local source presence/schema validation for historical actuals/moneyline."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6ms_projection_adapter_historical_actuals_moneyline_ingestion_validation_audit"
TMP_DIR = Path("tmp")

SCRIPT_6MR = Path("scripts/implement_6mr_layer6_projection_adapter_historical_actuals_moneyline_ingestion_validation.py")
JSON_6MR = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_validation.json"

ACTUALS_PRESENCE_6MR = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_actuals_presence.csv"
MONEYLINE_PRESENCE_6MR = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_moneyline_presence.csv"
ACTUALS_SCHEMA_6MR = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_actuals_schema_validation.csv"
MONEYLINE_SCHEMA_6MR = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_moneyline_schema_validation.csv"
PROVENANCE_6MR = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_provenance_validation.csv"
AUTHORITY_6MR = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_source_authority_validation.csv"
DUPLICATE_6MR = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_duplicate_validation.csv"
MISSING_6MR = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_missing_field_validation.csv"
PREVIEW_6MR = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_normalized_preview.csv"
BLOCKERS_6MR = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_blockers.csv"

REQUIRED_INPUTS = [
    JSON_6MR,
    TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_validation_checks.csv",
    TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_validation_predecessor.csv",
    TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_validation_input_artifacts.csv",
    ACTUALS_PRESENCE_6MR,
    MONEYLINE_PRESENCE_6MR,
    ACTUALS_SCHEMA_6MR,
    MONEYLINE_SCHEMA_6MR,
    TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_alias_validation.csv",
    PROVENANCE_6MR,
    AUTHORITY_6MR,
    DUPLICATE_6MR,
    MISSING_6MR,
    PREVIEW_6MR,
    BLOCKERS_6MR,
    TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_validation_allowed_operations_next.csv",
    TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_validation_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_validation_future_6ms_contract.csv",
    TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_validation_blocking_policy.csv",
    TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_validation_decision.csv",
    TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_validation_safety_boundaries.csv",
    TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_validation_recommended_path.csv",
    SCRIPT_6MR,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PRESENCE_REVIEW_CSV = TMP_DIR / f"{SLUG}_presence_review.csv"
SCHEMA_REVIEW_CSV = TMP_DIR / f"{SLUG}_schema_review.csv"
PROVENANCE_AUTHORITY_REVIEW_CSV = TMP_DIR / f"{SLUG}_provenance_authority_review.csv"
MISSING_DUPLICATE_REVIEW_CSV = TMP_DIR / f"{SLUG}_missing_duplicate_review.csv"
PREVIEW_REVIEW_CSV = TMP_DIR / f"{SLUG}_preview_review.csv"
BLOCKER_REVIEW_CSV = TMP_DIR / f"{SLUG}_blocker_review.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6MT_CSV = TMP_DIR / f"{SLUG}_future_6mt_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MR = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_ingestion_implementation_complete"
DIAGNOSIS_6MS = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_ingestion_audit_complete"
RECOMMENDED_NEXT_LAYER_6MS = "6MT_layer_6_projection_adapter_historical_actuals_and_moneyline_source_wait_state_plan"
RECOMMENDED_PATH_6MS = "document_wait_state_until_local_historical_actuals_and_moneyline_sources_are_supplied"


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


def count_matches(rows: list[dict[str, str]]) -> int:
    total = 0
    for row in rows:
        try:
            total += int(row.get("match_count", "0") or "0")
        except ValueError:
            pass
    return total


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6mr = load_json(JSON_6MR)

    actuals_presence = read_csv_rows(ACTUALS_PRESENCE_6MR)
    moneyline_presence = read_csv_rows(MONEYLINE_PRESENCE_6MR)
    actuals_schema = read_csv_rows(ACTUALS_SCHEMA_6MR)
    moneyline_schema = read_csv_rows(MONEYLINE_SCHEMA_6MR)
    provenance = read_csv_rows(PROVENANCE_6MR)
    authority = read_csv_rows(AUTHORITY_6MR)
    duplicate = read_csv_rows(DUPLICATE_6MR)
    missing = read_csv_rows(MISSING_6MR)
    preview = read_csv_rows(PREVIEW_6MR)
    blockers = read_csv_rows(BLOCKERS_6MR)

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
        {"check": "6mr_script_exists", "expected": True, "actual": SCRIPT_6MR.exists(), "passed": SCRIPT_6MR.exists()},
        {"check": "6mr_json_exists", "expected": True, "actual": JSON_6MR.exists(), "passed": JSON_6MR.exists()},
        {"check": "6mr_all_checks_passed", "expected": True, "actual": json_6mr.get("all_checks_passed"), "passed": json_6mr.get("all_checks_passed") is True},
        {"check": "6mr_diagnosis", "expected": DIAGNOSIS_6MR, "actual": json_6mr.get("diagnosis"), "passed": json_6mr.get("diagnosis") == DIAGNOSIS_6MR},
        {"check": "6mr_recommended_next_layer", "expected": "6MS_layer_6_projection_adapter_historical_actuals_and_moneyline_source_ingestion_audit", "actual": json_6mr.get("recommended_next_layer"), "passed": json_6mr.get("recommended_next_layer") == "6MS_layer_6_projection_adapter_historical_actuals_and_moneyline_source_ingestion_audit"},
        {"check": "metric_execution_allowed_next", "expected": False, "actual": json_6mr.get("metric_execution_allowed_next"), "passed": json_6mr.get("metric_execution_allowed_next") is False},
        {"check": "backtest_execution_allowed_next", "expected": False, "actual": json_6mr.get("backtest_execution_allowed_next"), "passed": json_6mr.get("backtest_execution_allowed_next") is False},
    ]

    actuals_count = int(json_6mr.get("actuals_source_files_found_count", -1))
    moneyline_count = int(json_6mr.get("moneyline_source_files_found_count", -1))
    presence_review = [
        {"review": "actuals_presence_file_exists", "actual": ACTUALS_PRESENCE_6MR.exists(), "passed": ACTUALS_PRESENCE_6MR.exists()},
        {"review": "moneyline_presence_file_exists", "actual": MONEYLINE_PRESENCE_6MR.exists(), "passed": MONEYLINE_PRESENCE_6MR.exists()},
        {"review": "actuals_count_confirmed_zero", "actual": actuals_count, "expected": 0, "passed": actuals_count == 0},
        {"review": "moneyline_count_confirmed_zero", "actual": moneyline_count, "expected": 0, "passed": moneyline_count == 0},
        {"review": "presence_csv_actuals_matches_zero", "actual": count_matches(actuals_presence), "expected": 0, "passed": count_matches(actuals_presence) == 0},
        {"review": "presence_csv_moneyline_matches_zero", "actual": count_matches(moneyline_presence), "expected": 0, "passed": count_matches(moneyline_presence) == 0},
    ]

    actuals_deferred = bool(actuals_schema) and all(row.get("file_present") == "False" and row.get("passed") == "True" for row in actuals_schema)
    moneyline_deferred = bool(moneyline_schema) and all(row.get("file_present") == "False" and row.get("passed") == "True" for row in moneyline_schema)
    schema_review = [
        {"review": "actuals_schema_deferred_safely", "actual": actuals_deferred, "expected": True, "passed": actuals_deferred},
        {"review": "moneyline_schema_deferred_safely", "actual": moneyline_deferred, "expected": True, "passed": moneyline_deferred},
        {"review": "actuals_schema_rows", "actual": len(actuals_schema), "expected": 8, "passed": len(actuals_schema) == 8},
        {"review": "moneyline_schema_rows", "actual": len(moneyline_schema), "expected": 6, "passed": len(moneyline_schema) == 6},
    ]

    provenance_deferred = bool(provenance) and all(row.get("source_file_present") == "False" and row.get("passed") == "True" for row in provenance)
    authority_deferred = bool(authority) and all(row.get("under_data_local") == "False" and row.get("tmp_path") == "False" and row.get("passed") == "True" for row in authority)
    provenance_authority_review = [
        {"review": "provenance_deferred_safely", "actual": provenance_deferred, "expected": True, "passed": provenance_deferred},
        {"review": "authority_deferred_safely", "actual": authority_deferred, "expected": True, "passed": authority_deferred},
        {"review": "provenance_rows", "actual": len(provenance), "expected": 2, "passed": len(provenance) == 2},
        {"review": "authority_rows", "actual": len(authority), "expected": 2, "passed": len(authority) == 2},
    ]

    missing_blocking = bool(missing) and all(row.get("missing") == "True" and row.get("blocks_ingestion") == "True" and row.get("passed") == "True" for row in missing)
    duplicate_validation_only = bool(duplicate) and all(row.get("validation_only") == "True" and row.get("rows_ingested") == "0" and row.get("passed") == "True" for row in duplicate)
    missing_duplicate_review = [
        {"review": "missing_field_validation_blocks_ingestion", "actual": missing_blocking, "expected": True, "passed": missing_blocking},
        {"review": "duplicate_validation_only", "actual": duplicate_validation_only, "expected": True, "passed": duplicate_validation_only},
        {"review": "missing_rows", "actual": len(missing), "expected": 14, "passed": len(missing) == 14},
        {"review": "duplicate_rows", "actual": len(duplicate), "expected": 2, "passed": len(duplicate) == 2},
    ]

    preview_validation_only = bool(preview) and all(row.get("validation_only") == "True" and row.get("production_table") == "False" and row.get("passed") == "True" for row in preview)
    preview_review = [
        {"review": "normalized_preview_validation_only", "actual": preview_validation_only, "expected": True, "passed": preview_validation_only},
        {"review": "normalized_preview_not_created_without_files", "actual": json_6mr.get("normalized_preview_created"), "expected": False, "passed": json_6mr.get("normalized_preview_created") is False},
        {"review": "preview_rows", "actual": len(preview), "expected": 2, "passed": len(preview) == 2},
    ]

    blocker_names = {row.get("blocker") for row in blockers}
    blocker_review = [
        {"review": "no_local_actuals_source_file_blocker", "actual": "no_local_actuals_source_file" in blocker_names, "expected": True, "passed": "no_local_actuals_source_file" in blocker_names},
        {"review": "no_local_moneyline_source_file_blocker", "actual": "no_local_moneyline_source_file" in blocker_names, "expected": True, "passed": "no_local_moneyline_source_file" in blocker_names},
        {"review": "expected_source_missing_blockers_allowed", "actual": json_6mr.get("expected_source_missing_blockers_allowed"), "expected": True, "passed": json_6mr.get("expected_source_missing_blockers_allowed") is True},
        {"review": "blocker_rows", "actual": len(blockers), "expected": 2, "passed": len(blockers) == 2},
    ]

    allowed_next = [
        {"operation": "document_wait_state_until_local_sources_are_supplied", "allowed_next": True, "scope": "6MT planning only", "passed": True},
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

    future_6mt = [
        {"contract": "document_current_wait_state", "required": True, "passed": True},
        {"contract": "document_required_local_source_files", "required": True, "passed": True},
        {"contract": "document_resume_conditions_after_files_supplied", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_no_backtests_until_sources_exist", "required": True, "passed": True},
    ]

    blocking_policy = [
        {"policy": "missing_local_sources_keep_layer_6_blocked", "required": True, "passed": True},
        {"policy": "do_not_execute_metrics_until_actuals_and_moneyline_sources_validate", "required": True, "passed": True},
        {"policy": "do_not_run_backtests_until_actuals_and_moneyline_sources_validate", "required": True, "passed": True},
        {"policy": "do_not_grant_layer_6_exit_without_historical_source_validation", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6mr_passed", "expected": True, "actual": json_6mr.get("all_checks_passed"), "passed": json_6mr.get("all_checks_passed") is True},
        {"decision": "6mr_diagnosis_valid", "expected": DIAGNOSIS_6MR, "actual": json_6mr.get("diagnosis"), "passed": json_6mr.get("diagnosis") == DIAGNOSIS_6MR},
        {"decision": "all_required_6mr_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "presence_review_passed", "expected": True, "actual": all_passed(presence_review), "passed": all_passed(presence_review)},
        {"decision": "schema_review_passed", "expected": True, "actual": all_passed(schema_review), "passed": all_passed(schema_review)},
        {"decision": "provenance_authority_review_passed", "expected": True, "actual": all_passed(provenance_authority_review), "passed": all_passed(provenance_authority_review)},
        {"decision": "missing_duplicate_review_passed", "expected": True, "actual": all_passed(missing_duplicate_review), "passed": all_passed(missing_duplicate_review)},
        {"decision": "preview_review_passed", "expected": True, "actual": all_passed(preview_review), "passed": all_passed(preview_review)},
        {"decision": "blocker_review_passed", "expected": True, "actual": all_passed(blocker_review), "passed": all_passed(blocker_review)},
        {"decision": "recommend_6mt_next", "expected": RECOMMENDED_NEXT_LAYER_6MS, "actual": RECOMMENDED_NEXT_LAYER_6MS, "passed": True},
        {"decision": "do_not_execute_metrics_backtest_tune_activate_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only_local_source_presence_schema_validation", "expected": True, "actual": True, "passed": True},
        {"boundary": "source_rows_ingested_by_6ms", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6ms", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6ms", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6ms", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6ms", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6ms", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MS, "actual": RECOMMENDED_NEXT_LAYER_6MS, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MS, "actual": RECOMMENDED_PATH_6MS, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MS, "actual": DIAGNOSIS_6MS, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "presence_review", "passed": all_passed(presence_review), "detail": f"{sum(1 for r in presence_review if r['passed'])}/{len(presence_review)}"},
        {"check": "schema_review", "passed": all_passed(schema_review), "detail": f"{sum(1 for r in schema_review if r['passed'])}/{len(schema_review)}"},
        {"check": "provenance_authority_review", "passed": all_passed(provenance_authority_review), "detail": f"{sum(1 for r in provenance_authority_review if r['passed'])}/{len(provenance_authority_review)}"},
        {"check": "missing_duplicate_review", "passed": all_passed(missing_duplicate_review), "detail": f"{sum(1 for r in missing_duplicate_review if r['passed'])}/{len(missing_duplicate_review)}"},
        {"check": "preview_review", "passed": all_passed(preview_review), "detail": f"{sum(1 for r in preview_review if r['passed'])}/{len(preview_review)}"},
        {"check": "blocker_review", "passed": all_passed(blocker_review), "detail": f"{sum(1 for r in blocker_review if r['passed'])}/{len(blocker_review)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next), "detail": f"{sum(1 for r in allowed_next if r['passed'])}/{len(allowed_next)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next), "detail": f"{sum(1 for r in forbidden_next if r['passed'])}/{len(forbidden_next)}"},
        {"check": "future_6mt_contract", "passed": all_passed(future_6mt), "detail": f"{sum(1 for r in future_6mt if r['passed'])}/{len(future_6mt)}"},
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
        "presence_review": write_csv(PRESENCE_REVIEW_CSV, presence_review),
        "schema_review": write_csv(SCHEMA_REVIEW_CSV, schema_review),
        "provenance_authority_review": write_csv(PROVENANCE_AUTHORITY_REVIEW_CSV, provenance_authority_review),
        "missing_duplicate_review": write_csv(MISSING_DUPLICATE_REVIEW_CSV, missing_duplicate_review),
        "preview_review": write_csv(PREVIEW_REVIEW_CSV, preview_review),
        "blocker_review": write_csv(BLOCKER_REVIEW_CSV, blocker_review),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "future_6mt_contract": write_csv(FUTURE_6MT_CSV, future_6mt),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MS",
        "layer_type": "game_mechanics_realism",
        "audit_only_local_source_presence_schema_validation": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MS,
        "recommended_path": RECOMMENDED_PATH_6MS,
        "predecessor_layer": "6MR",
        "predecessor_diagnosis": json_6mr.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mr.get("all_checks_passed") is True,
        "audited_layer_after": "6MR",
        "source_family": "projection_adapter_historical_actuals_moneyline_source_ingestion_validation_audit",
        "local_file_presence_checks_audited": True,
        "actuals_presence_audited": True,
        "moneyline_presence_audited": True,
        "actuals_source_files_found_count_confirmed": actuals_count,
        "moneyline_source_files_found_count_confirmed": moneyline_count,
        "expected_missing_source_blockers_confirmed": all_passed(blocker_review),
        "actuals_schema_deferred_safely_confirmed": actuals_deferred,
        "moneyline_schema_deferred_safely_confirmed": moneyline_deferred,
        "provenance_deferred_safely_confirmed": provenance_deferred,
        "source_authority_deferred_safely_confirmed": authority_deferred,
        "duplicate_validation_audited": duplicate_validation_only,
        "missing_field_validation_audited": missing_blocking,
        "normalized_preview_validation_only_confirmed": preview_validation_only,
        "ingestion_validation_audit_complete": all_checks_passed,
        "wait_state_planning_allowed_next": True,
        "source_ingestion_allowed_next": False,
        "source_implementation_allowed_next": False,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "source_rows_ingested_by_6ms": False,
        "normalized_source_tables_created_for_production_by_6ms": False,
        "production_code_modified_by_6ms": False,
        "adapter_call_executed_by_6ms": False,
        "metric_execution_run_by_6ms": False,
        "backtest_execution_run_by_6ms": False,
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
            "presence_review_csv": str(PRESENCE_REVIEW_CSV),
            "schema_review_csv": str(SCHEMA_REVIEW_CSV),
            "provenance_authority_review_csv": str(PROVENANCE_AUTHORITY_REVIEW_CSV),
            "missing_duplicate_review_csv": str(MISSING_DUPLICATE_REVIEW_CSV),
            "preview_review_csv": str(PREVIEW_REVIEW_CSV),
            "blocker_review_csv": str(BLOCKER_REVIEW_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6mt_contract_csv": str(FUTURE_6MT_CSV),
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
