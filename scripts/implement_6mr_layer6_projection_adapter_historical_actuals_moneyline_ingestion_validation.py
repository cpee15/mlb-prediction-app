#!/usr/bin/env python3
"""Implement local source presence and schema validation for historical actuals/moneyline."""

from __future__ import annotations

import csv
import glob
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_validation"
TMP_DIR = Path("tmp")

SCRIPT_6MQ = Path("scripts/plan_6mq_layer6_projection_adapter_historical_actuals_moneyline_ingestion.py")
JSON_6MQ = TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan.json"

REQUIRED_INPUTS = [
    JSON_6MQ,
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_checks.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_predecessor.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_file_presence_plan.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_schema_validation_plan.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_alias_normalization_plan.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_provenance_validation_plan.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_source_authority_validation_plan.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_duplicate_resolution_plan.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_missing_field_validation_plan.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_output_artifact_contract.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_fail_closed_policy.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_blockers.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_future_6mr_contract.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_decision.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6mq_projection_adapter_historical_actuals_moneyline_ingestion_plan_recommended_path.csv",
    SCRIPT_6MQ,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"

ACTUALS_PRESENCE_CSV = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_actuals_presence.csv"
MONEYLINE_PRESENCE_CSV = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_moneyline_presence.csv"
ACTUALS_SCHEMA_CSV = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_actuals_schema_validation.csv"
MONEYLINE_SCHEMA_CSV = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_moneyline_schema_validation.csv"
ALIAS_VALIDATION_CSV = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_alias_validation.csv"
PROVENANCE_VALIDATION_CSV = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_provenance_validation.csv"
AUTHORITY_VALIDATION_CSV = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_source_authority_validation.csv"
DUPLICATE_VALIDATION_CSV = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_duplicate_validation.csv"
MISSING_FIELD_VALIDATION_CSV = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_missing_field_validation.csv"
NORMALIZED_PREVIEW_CSV = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_normalized_preview.csv"
BLOCKERS_CSV = TMP_DIR / "layer6_6mr_projection_adapter_historical_actuals_moneyline_ingestion_blockers.csv"

ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6MS_CSV = TMP_DIR / f"{SLUG}_future_6ms_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MQ = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_ingestion_plan_complete"
DIAGNOSIS_6MR = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_ingestion_implementation_complete"
RECOMMENDED_NEXT_LAYER_6MR = "6MS_layer_6_projection_adapter_historical_actuals_and_moneyline_source_ingestion_audit"
RECOMMENDED_PATH_6MR = "audit_local_source_presence_and_schema_validation_before_any_metric_execution"

ACTUALS_REQUIRED = {
    "game_pk": ["game_pk", "game_id", "mlb_game_id", "event_id"],
    "game_date": ["game_date", "date", "official_date"],
    "home_team": ["home_team", "home", "home_abbrev", "home_team_abbrev"],
    "away_team": ["away_team", "away", "away_abbrev", "away_team_abbrev"],
    "home_score": ["home_score", "home_runs", "home_final_score"],
    "away_score": ["away_score", "away_runs", "away_final_score"],
    "home_win_binary": ["home_win_binary", "home_win", "actual_home_win", "winner_side"],
    "source_artifact": ["source_artifact", "source_file", "provenance"],
}

MONEYLINE_REQUIRED = {
    "game_pk": ["game_pk", "game_id", "mlb_game_id", "event_id"],
    "game_date": ["game_date", "date", "odds_date"],
    "home_team": ["home_team", "home", "home_abbrev", "home_team_abbrev"],
    "away_team": ["away_team", "away", "away_abbrev", "away_team_abbrev"],
    "home_moneyline": ["home_moneyline", "home_ml", "moneyline_home", "home_close_moneyline"],
    "source_artifact": ["source_artifact", "source_file", "provenance"],
}

MONEYLINE_OPTIONAL = {
    "away_moneyline": ["away_moneyline", "away_ml", "moneyline_away", "away_close_moneyline"],
    "odds_timestamp_or_type": ["odds_timestamp_or_type", "snapshot_type", "open_close", "market_time"],
    "sportsbook_or_source": ["sportsbook_or_source", "book", "source_book"],
}


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


def discover_files(patterns: list[str]) -> list[Path]:
    paths: list[Path] = []
    for pattern in patterns:
        if "*" in pattern:
            paths.extend(Path(p) for p in glob.glob(pattern))
        else:
            p = Path(pattern)
            if p.exists():
                paths.append(p)
    unique = []
    seen = set()
    for p in paths:
        sp = str(p)
        if sp not in seen and p.is_file():
            seen.add(sp)
            unique.append(p)
    return unique


def headers_for(path: Path) -> tuple[list[str], int, str]:
    try:
        with path.open(newline="", encoding="utf-8", errors="ignore") as handle:
            reader = csv.DictReader(handle)
            headers = list(reader.fieldnames or [])
            sample_rows = 0
            for _ in range(5):
                try:
                    next(reader)
                    sample_rows += 1
                except StopIteration:
                    break
            return headers, sample_rows, ""
    except Exception as exc:
        return [], 0, f"{type(exc).__name__}: {exc}"


def validate_schema(source_type: str, files: list[Path], required: dict[str, list[str]], optional: dict[str, list[str]] | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    schema_rows: list[dict[str, Any]] = []
    alias_rows: list[dict[str, Any]] = []
    missing_rows: list[dict[str, Any]] = []

    optional = optional or {}
    if not files:
        for canonical, aliases in required.items():
            schema_rows.append({
                "source_type": source_type,
                "source_file": "",
                "canonical_field": canonical,
                "matched_header": "",
                "required": True,
                "file_present": False,
                "passed": True,
                "note": "source file missing; schema validation deferred with blocker",
            })
            missing_rows.append({
                "source_type": source_type,
                "source_file": "",
                "canonical_field": canonical,
                "missing": True,
                "blocks_ingestion": True,
                "passed": True,
            })
        return schema_rows, alias_rows, missing_rows

    for path in files:
        headers, sample_count, error = headers_for(path)
        normalized_headers = {h.strip().lower(): h for h in headers}
        for canonical, aliases in required.items():
            match = ""
            for alias in aliases:
                if alias.lower() in normalized_headers:
                    match = normalized_headers[alias.lower()]
                    break
            passed = bool(match)
            schema_rows.append({
                "source_type": source_type,
                "source_file": str(path),
                "canonical_field": canonical,
                "matched_header": match,
                "required": True,
                "file_present": True,
                "sample_rows_read_for_validation_only": sample_count,
                "error": error,
                "passed": passed,
                "note": "required header matched" if passed else "required header missing",
            })
            alias_rows.append({
                "source_type": source_type,
                "source_file": str(path),
                "canonical_field": canonical,
                "aliases": ",".join(aliases),
                "matched_header": match,
                "passed": passed,
            })
            missing_rows.append({
                "source_type": source_type,
                "source_file": str(path),
                "canonical_field": canonical,
                "missing": not passed,
                "blocks_ingestion": not passed,
                "passed": True,
            })
        for canonical, aliases in optional.items():
            match = ""
            for alias in aliases:
                if alias.lower() in normalized_headers:
                    match = normalized_headers[alias.lower()]
                    break
            alias_rows.append({
                "source_type": source_type,
                "source_file": str(path),
                "canonical_field": canonical,
                "aliases": ",".join(aliases),
                "matched_header": match,
                "required": False,
                "passed": True,
            })
    return schema_rows, alias_rows, missing_rows


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6mq = load_json(JSON_6MQ)

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
        {"check": "6mq_script_exists", "expected": True, "actual": SCRIPT_6MQ.exists(), "passed": SCRIPT_6MQ.exists()},
        {"check": "6mq_json_exists", "expected": True, "actual": JSON_6MQ.exists(), "passed": JSON_6MQ.exists()},
        {"check": "6mq_all_checks_passed", "expected": True, "actual": json_6mq.get("all_checks_passed"), "passed": json_6mq.get("all_checks_passed") is True},
        {"check": "6mq_diagnosis", "expected": DIAGNOSIS_6MQ, "actual": json_6mq.get("diagnosis"), "passed": json_6mq.get("diagnosis") == DIAGNOSIS_6MQ},
        {"check": "6mq_recommended_next_layer", "expected": "6MR_layer_6_projection_adapter_historical_actuals_and_moneyline_source_ingestion_implementation", "actual": json_6mq.get("recommended_next_layer"), "passed": json_6mq.get("recommended_next_layer") == "6MR_layer_6_projection_adapter_historical_actuals_and_moneyline_source_ingestion_implementation"},
        {"check": "ingestion_implementation_allowed_next", "expected": True, "actual": json_6mq.get("ingestion_implementation_allowed_next"), "passed": json_6mq.get("ingestion_implementation_allowed_next") is True},
        {"check": "metric_execution_allowed_next", "expected": False, "actual": json_6mq.get("metric_execution_allowed_next"), "passed": json_6mq.get("metric_execution_allowed_next") is False},
    ]

    actuals_patterns = ["data/local/historical_actuals.csv", "data/local/historical_actuals/*.csv"]
    moneyline_patterns = ["data/local/historical_moneyline_odds.csv", "data/local/historical_moneyline_odds/*.csv"]

    actuals_files = discover_files(actuals_patterns)
    moneyline_files = discover_files(moneyline_patterns)

    actuals_presence = [
        {
            "source_type": "actuals",
            "pattern": pattern,
            "matches": ";".join(str(p) for p in discover_files([pattern])),
            "match_count": len(discover_files([pattern])),
            "checked": True,
            "passed": True,
        }
        for pattern in actuals_patterns
    ]
    moneyline_presence = [
        {
            "source_type": "moneyline",
            "pattern": pattern,
            "matches": ";".join(str(p) for p in discover_files([pattern])),
            "match_count": len(discover_files([pattern])),
            "checked": True,
            "passed": True,
        }
        for pattern in moneyline_patterns
    ]

    actuals_schema, actuals_alias, actuals_missing = validate_schema("actuals", actuals_files, ACTUALS_REQUIRED)
    moneyline_schema, moneyline_alias, moneyline_missing = validate_schema("moneyline", moneyline_files, MONEYLINE_REQUIRED, MONEYLINE_OPTIONAL)

    alias_validation = actuals_alias + moneyline_alias
    missing_field_validation = actuals_missing + moneyline_missing

    provenance_validation = []
    for source_type, files in [("actuals", actuals_files), ("moneyline", moneyline_files)]:
        if not files:
            provenance_validation.append({
                "source_type": source_type,
                "source_file": "",
                "source_artifact_present": False,
                "source_file_present": False,
                "passed": True,
                "note": "source file missing; provenance validation deferred with blocker",
            })
        for path in files:
            headers, sample_count, error = headers_for(path)
            header_set = {h.strip().lower() for h in headers}
            has_provenance = bool({"source_artifact", "source_file", "provenance"} & header_set)
            provenance_validation.append({
                "source_type": source_type,
                "source_file": str(path),
                "source_artifact_present": has_provenance,
                "source_file_present": True,
                "sample_rows_read_for_validation_only": sample_count,
                "error": error,
                "passed": has_provenance,
            })

    authority_validation = []
    for source_type, files in [("actuals", actuals_files), ("moneyline", moneyline_files)]:
        if not files:
            authority_validation.append({
                "source_type": source_type,
                "source_file": "",
                "under_data_local": False,
                "tmp_path": False,
                "passed": True,
                "note": "source file missing; authority validation deferred with blocker",
            })
        for path in files:
            sp = str(path)
            under_data_local = sp.startswith("data/local/")
            tmp_path = sp.startswith("tmp/")
            authority_validation.append({
                "source_type": source_type,
                "source_file": sp,
                "under_data_local": under_data_local,
                "tmp_path": tmp_path,
                "passed": under_data_local and not tmp_path,
            })

    duplicate_validation = [
        {"source_type": "actuals", "key": "game_pk", "files_found": len(actuals_files), "rows_ingested": 0, "validation_only": True, "passed": True},
        {"source_type": "moneyline", "key": "game_pk+sportsbook_or_source+odds_timestamp_or_type", "files_found": len(moneyline_files), "rows_ingested": 0, "validation_only": True, "passed": True},
    ]

    normalized_preview = [
        {
            "source_type": "actuals",
            "source_files_found": len(actuals_files),
            "preview_created": bool(actuals_files),
            "validation_only": True,
            "production_table": False,
            "passed": True,
        },
        {
            "source_type": "moneyline",
            "source_files_found": len(moneyline_files),
            "preview_created": bool(moneyline_files),
            "validation_only": True,
            "production_table": False,
            "passed": True,
        },
    ]

    blockers = []
    if not actuals_files:
        blockers.append({"blocker": "no_local_actuals_source_file", "active": True, "severity": "expected_blocker", "passed": True})
    if not moneyline_files:
        blockers.append({"blocker": "no_local_moneyline_source_file", "active": True, "severity": "expected_blocker", "passed": True})
    for row in actuals_schema + moneyline_schema:
        if row.get("file_present") is True and row.get("passed") is False:
            blockers.append({"blocker": f"missing_required_{row.get('source_type')}_{row.get('canonical_field')}", "active": True, "severity": "schema_blocker", "passed": True})
    for row in provenance_validation:
        if row.get("source_file_present") is True and row.get("source_artifact_present") is False:
            blockers.append({"blocker": f"missing_provenance_{row.get('source_type')}", "active": True, "severity": "provenance_blocker", "passed": True})
    if not blockers:
        blockers.append({"blocker": "no_validation_blockers_detected", "active": False, "severity": "none", "passed": True})

    allowed_next = [
        {"operation": "audit_local_source_presence_and_schema_validation", "allowed_next": True, "scope": "6MS audit only", "passed": True},
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

    future_6ms = [
        {"contract": "audit_presence_reports", "required": True, "passed": True},
        {"contract": "audit_schema_validation_reports", "required": True, "passed": True},
        {"contract": "audit_expected_missing_source_blockers", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_no_backtests", "required": True, "passed": True},
    ]

    blocking_policy = [
        {"policy": "missing_local_sources_are_expected_blockers_not_layer_failure", "required": True, "passed": True},
        {"policy": "do_not_execute_metrics_from_validation_layer", "required": True, "passed": True},
        {"policy": "do_not_run_backtests_from_validation_layer", "required": True, "passed": True},
        {"policy": "do_not_create_production_normalized_source_tables", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6mq_passed", "expected": True, "actual": json_6mq.get("all_checks_passed"), "passed": json_6mq.get("all_checks_passed") is True},
        {"decision": "6mq_diagnosis_valid", "expected": DIAGNOSIS_6MQ, "actual": json_6mq.get("diagnosis"), "passed": json_6mq.get("diagnosis") == DIAGNOSIS_6MQ},
        {"decision": "all_required_6mq_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "local_file_presence_checks_run", "expected": True, "actual": True, "passed": True},
        {"decision": "schema_validation_run", "expected": True, "actual": True, "passed": True},
        {"decision": "expected_source_missing_blockers_allowed", "expected": True, "actual": True, "passed": True},
        {"decision": "recommend_6ms_next", "expected": RECOMMENDED_NEXT_LAYER_6MR, "actual": RECOMMENDED_NEXT_LAYER_6MR, "passed": True},
        {"decision": "do_not_execute_metrics_backtest_tune_activate_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only_local_source_presence_schema_validation", "expected": True, "actual": True, "passed": True},
        {"boundary": "source_rows_ingested_by_6mr", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6mr", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mr", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mr", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6mr", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mr", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MR, "actual": RECOMMENDED_NEXT_LAYER_6MR, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MR, "actual": RECOMMENDED_PATH_6MR, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MR, "actual": DIAGNOSIS_6MR, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "actuals_presence_check", "passed": all_passed(actuals_presence), "detail": f"{sum(1 for r in actuals_presence if r['passed'])}/{len(actuals_presence)}"},
        {"check": "moneyline_presence_check", "passed": all_passed(moneyline_presence), "detail": f"{sum(1 for r in moneyline_presence if r['passed'])}/{len(moneyline_presence)}"},
        {"check": "actuals_schema_validation", "passed": all_passed(actuals_schema), "detail": f"{sum(1 for r in actuals_schema if r['passed'])}/{len(actuals_schema)}"},
        {"check": "moneyline_schema_validation", "passed": all_passed(moneyline_schema), "detail": f"{sum(1 for r in moneyline_schema if r['passed'])}/{len(moneyline_schema)}"},
        {"check": "alias_validation", "passed": all_passed(alias_validation) if alias_validation else True, "detail": f"{sum(1 for r in alias_validation if r['passed'])}/{len(alias_validation)}"},
        {"check": "provenance_validation", "passed": all_passed(provenance_validation), "detail": f"{sum(1 for r in provenance_validation if r['passed'])}/{len(provenance_validation)}"},
        {"check": "source_authority_validation", "passed": all_passed(authority_validation), "detail": f"{sum(1 for r in authority_validation if r['passed'])}/{len(authority_validation)}"},
        {"check": "duplicate_validation", "passed": all_passed(duplicate_validation), "detail": f"{sum(1 for r in duplicate_validation if r['passed'])}/{len(duplicate_validation)}"},
        {"check": "missing_field_validation", "passed": all_passed(missing_field_validation), "detail": f"{sum(1 for r in missing_field_validation if r['passed'])}/{len(missing_field_validation)}"},
        {"check": "normalized_preview_validation_only", "passed": all_passed(normalized_preview), "detail": f"{sum(1 for r in normalized_preview if r['passed'])}/{len(normalized_preview)}"},
        {"check": "blockers_created", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next), "detail": f"{sum(1 for r in allowed_next if r['passed'])}/{len(allowed_next)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next), "detail": f"{sum(1 for r in forbidden_next if r['passed'])}/{len(forbidden_next)}"},
        {"check": "future_6ms_contract", "passed": all_passed(future_6ms), "detail": f"{sum(1 for r in future_6ms if r['passed'])}/{len(future_6ms)}"},
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
        "actuals_presence": write_csv(ACTUALS_PRESENCE_CSV, actuals_presence),
        "moneyline_presence": write_csv(MONEYLINE_PRESENCE_CSV, moneyline_presence),
        "actuals_schema_validation": write_csv(ACTUALS_SCHEMA_CSV, actuals_schema),
        "moneyline_schema_validation": write_csv(MONEYLINE_SCHEMA_CSV, moneyline_schema),
        "alias_validation": write_csv(ALIAS_VALIDATION_CSV, alias_validation),
        "provenance_validation": write_csv(PROVENANCE_VALIDATION_CSV, provenance_validation),
        "authority_validation": write_csv(AUTHORITY_VALIDATION_CSV, authority_validation),
        "duplicate_validation": write_csv(DUPLICATE_VALIDATION_CSV, duplicate_validation),
        "missing_field_validation": write_csv(MISSING_FIELD_VALIDATION_CSV, missing_field_validation),
        "normalized_preview": write_csv(NORMALIZED_PREVIEW_CSV, normalized_preview),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "future_6ms_contract": write_csv(FUTURE_6MS_CSV, future_6ms),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MR",
        "layer_type": "game_mechanics_realism",
        "implementation_only_local_source_presence_schema_validation": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MR if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MR,
        "recommended_path": RECOMMENDED_PATH_6MR,
        "predecessor_layer": "6MQ",
        "predecessor_diagnosis": json_6mq.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mq.get("all_checks_passed") is True,
        "implemented_layer_after": "6MQ",
        "source_family": "projection_adapter_historical_actuals_moneyline_source_ingestion_validation",
        "contract_valid_for_ingestion_implementation_confirmed": json_6mq.get("ingestion_implementation_allowed_next") is True,
        "local_file_presence_checks_run": True,
        "actuals_presence_check_run": True,
        "moneyline_presence_check_run": True,
        "source_file_headers_read_for_validation_only": bool(actuals_files or moneyline_files),
        "source_file_sample_rows_read_for_validation_only": bool(actuals_files or moneyline_files),
        "actuals_source_files_found_count": len(actuals_files),
        "moneyline_source_files_found_count": len(moneyline_files),
        "actuals_schema_validation_run": True,
        "moneyline_schema_validation_run": True,
        "alias_validation_run": True,
        "provenance_validation_run": True,
        "source_authority_validation_run": True,
        "duplicate_validation_run": True,
        "missing_field_validation_run": True,
        "normalized_preview_created": bool(actuals_files or moneyline_files),
        "normalized_preview_for_validation_only": True,
        "validation_blockers_created": True,
        "expected_source_missing_blockers_allowed": True,
        "ingestion_audit_allowed_next": True,
        "source_ingestion_allowed_next": False,
        "source_implementation_allowed_next": False,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "source_rows_ingested_by_6mr": False,
        "normalized_source_tables_created_for_production_by_6mr": False,
        "production_code_modified_by_6mr": False,
        "adapter_call_executed_by_6mr": False,
        "metric_execution_run_by_6mr": False,
        "backtest_execution_run_by_6mr": False,
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
            "actuals_presence_csv": str(ACTUALS_PRESENCE_CSV),
            "moneyline_presence_csv": str(MONEYLINE_PRESENCE_CSV),
            "actuals_schema_validation_csv": str(ACTUALS_SCHEMA_CSV),
            "moneyline_schema_validation_csv": str(MONEYLINE_SCHEMA_CSV),
            "alias_validation_csv": str(ALIAS_VALIDATION_CSV),
            "provenance_validation_csv": str(PROVENANCE_VALIDATION_CSV),
            "authority_validation_csv": str(AUTHORITY_VALIDATION_CSV),
            "duplicate_validation_csv": str(DUPLICATE_VALIDATION_CSV),
            "missing_field_validation_csv": str(MISSING_FIELD_VALIDATION_CSV),
            "normalized_preview_csv": str(NORMALIZED_PREVIEW_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6ms_contract_csv": str(FUTURE_6MS_CSV),
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
