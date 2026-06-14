#!/usr/bin/env python3
"""Validate local historical actuals source files for Layer 6 actuals-only evaluation."""

from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6na_historical_actuals_source_validation"
TMP_DIR = Path("tmp")

SCRIPT_6MZ = Path("scripts/audit_6mz_layer6_historical_actuals_source_validation_plan.py")
JSON_6MZ = TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit.json"

ACCEPTED_SINGLE = Path("data/local/historical_actuals.csv")
ACCEPTED_DIR = Path("data/local/historical_actuals")

REQUIRED_INPUTS = [
    JSON_6MZ,
    TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit_checks.csv",
    TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit_predecessor.csv",
    TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit_accepted_locations_review.csv",
    TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit_required_schema_review.csv",
    TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit_alias_candidates_review.csv",
    TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit_validation_checks_review.csv",
    TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit_blocking_conditions_review.csv",
    TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit_allowed_outputs_review.csv",
    TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit_moneyline_deferral_boundaries_review.csv",
    TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit_allowed_operations_next.csv",
    TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit_future_6na_contract.csv",
    TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit_decision.csv",
    TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6mz_historical_actuals_source_validation_plan_audit_recommended_path.csv",
    SCRIPT_6MZ,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
SOURCE_DISCOVERY_CSV = TMP_DIR / f"{SLUG}_source_discovery.csv"
SCHEMA_MAPPING_CSV = TMP_DIR / f"{SLUG}_schema_mapping.csv"
SCHEMA_CHECKS_CSV = TMP_DIR / f"{SLUG}_schema_checks.csv"
VALUE_CHECKS_CSV = TMP_DIR / f"{SLUG}_value_checks.csv"
DUPLICATES_CSV = TMP_DIR / f"{SLUG}_duplicate_game_pk_review.csv"
INVALID_ROWS_CSV = TMP_DIR / f"{SLUG}_invalid_rows_sample.csv"
PROVENANCE_CSV = TMP_DIR / f"{SLUG}_provenance_review.csv"
MONEYLINE_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_moneyline_deferral_boundaries.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6NB_CSV = TMP_DIR / f"{SLUG}_future_6nb_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MZ = "layer_6_historical_actuals_source_validation_plan_audit_complete"
DIAGNOSIS_COMPLETE = "layer_6_historical_actuals_source_validation_implementation_complete"
DIAGNOSIS_BLOCKED = "layer_6_historical_actuals_source_validation_blocked"
RECOMMENDED_NEXT_COMPLETE = "6NB_layer_6_historical_actuals_source_validation_audit"
RECOMMENDED_PATH_COMPLETE = "audit_historical_actuals_source_validation_before_actuals_only_metrics"
RECOMMENDED_PATH_BLOCKED = "supply_or_repair_historical_actuals_source_then_rerun_6na"

ALIASES = {
    "game_pk": ["game_pk", "game_id", "mlb_game_pk"],
    "game_date": ["game_date", "date"],
    "home_team": ["home_team", "home_name"],
    "away_team": ["away_team", "away_name"],
    "home_score": ["home_score", "home_runs"],
    "away_score": ["away_score", "away_runs"],
    "home_win_binary": ["home_win_binary", "home_win"],
    "source_artifact": ["source_artifact", "source_file", "provenance"],
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
        rows = [{"empty": True}]
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


def discover_sources() -> list[Path]:
    files: list[Path] = []
    if ACCEPTED_SINGLE.exists() and ACCEPTED_SINGLE.is_file():
        files.append(ACCEPTED_SINGLE)
    if ACCEPTED_DIR.exists() and ACCEPTED_DIR.is_dir():
        files.extend(sorted(path for path in ACCEPTED_DIR.glob("*.csv") if path.is_file()))
    seen: set[str] = set()
    unique_files: list[Path] = []
    for path in files:
        key = str(path.resolve())
        if key not in seen:
            seen.add(key)
            unique_files.append(path)
    return unique_files


def map_schema(fieldnames: list[str]) -> tuple[dict[str, str | None], list[dict[str, Any]]]:
    normalized = {name.strip().lower(): name for name in fieldnames if name is not None}
    mapping: dict[str, str | None] = {}
    rows: list[dict[str, Any]] = []
    for canonical, aliases in ALIASES.items():
        matched = None
        for alias in aliases:
            if alias.lower() in normalized:
                matched = normalized[alias.lower()]
                break
        mapping[canonical] = matched
        rows.append(
            {
                "canonical_field": canonical,
                "matched_source_column": matched or "",
                "aliases": "|".join(aliases),
                "mapped": matched is not None,
                "passed": matched is not None,
            }
        )
    return mapping, rows


def parse_int(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(str(value).strip()))
    except Exception:
        return None


def parse_date_ok(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%Y%m%d"):
        try:
            datetime.strptime(text[:10] if fmt == "%Y-%m-%d" else text, fmt)
            return True
        except Exception:
            pass
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
        return True
    except Exception:
        return False


def clean(row: dict[str, Any], source_column: str | None) -> str:
    if not source_column:
        return ""
    return str(row.get(source_column, "")).strip()


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6mz = load_json(JSON_6MZ)

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
        {"check": "6mz_script_exists", "expected": True, "actual": SCRIPT_6MZ.exists(), "passed": SCRIPT_6MZ.exists()},
        {"check": "6mz_json_exists", "expected": True, "actual": JSON_6MZ.exists(), "passed": JSON_6MZ.exists()},
        {"check": "6mz_all_checks_passed", "expected": True, "actual": json_6mz.get("all_checks_passed"), "passed": json_6mz.get("all_checks_passed") is True},
        {"check": "6mz_diagnosis", "expected": DIAGNOSIS_6MZ, "actual": json_6mz.get("diagnosis"), "passed": json_6mz.get("diagnosis") == DIAGNOSIS_6MZ},
        {"check": "6mz_recommended_next_layer", "expected": "6NA_layer_6_historical_actuals_source_validation_implementation", "actual": json_6mz.get("recommended_next_layer"), "passed": json_6mz.get("recommended_next_layer") == "6NA_layer_6_historical_actuals_source_validation_implementation"},
        {"check": "historical_actuals_source_validation_implementation_allowed_next", "expected": True, "actual": json_6mz.get("historical_actuals_source_validation_implementation_allowed_next"), "passed": json_6mz.get("historical_actuals_source_validation_implementation_allowed_next") is True},
        {"check": "metric_execution_allowed_next", "expected": False, "actual": json_6mz.get("metric_execution_allowed_next"), "passed": json_6mz.get("metric_execution_allowed_next") is False},
        {"check": "backtest_execution_allowed_next", "expected": False, "actual": json_6mz.get("backtest_execution_allowed_next"), "passed": json_6mz.get("backtest_execution_allowed_next") is False},
    ]

    source_files = discover_sources()
    source_discovery_rows = [
        {"location": str(ACCEPTED_SINGLE), "exists": ACCEPTED_SINGLE.exists(), "type": "single_csv", "passed": True},
        {"location": str(ACCEPTED_DIR), "exists": ACCEPTED_DIR.exists(), "type": "csv_directory", "passed": True},
    ]
    for path in source_files:
        source_discovery_rows.append({"location": str(path), "exists": True, "type": "discovered_source_file", "passed": True})

    combined_rows: list[dict[str, Any]] = []
    schema_mapping_rows: list[dict[str, Any]] = []
    source_read_errors: list[str] = []

    for source_path in source_files:
        try:
            rows = read_csv_rows(source_path)
            fieldnames = list(rows[0].keys()) if rows else []
            if not fieldnames:
                with source_path.open(newline="", encoding="utf-8", errors="ignore") as handle:
                    reader = csv.DictReader(handle)
                    fieldnames = list(reader.fieldnames or [])
            mapping, mapping_rows = map_schema(fieldnames)
            for mapping_row in mapping_rows:
                mapping_row["source_file"] = str(source_path)
            schema_mapping_rows.extend(mapping_rows)

            for idx, raw_row in enumerate(rows, start=2):
                canonical_row = {"source_file": str(source_path), "source_row_number": idx}
                for canonical, source_column in mapping.items():
                    canonical_row[canonical] = clean(raw_row, source_column)
                combined_rows.append(canonical_row)
        except Exception as exc:
            source_read_errors.append(f"{source_path}: {type(exc).__name__}: {exc}")

    if not source_files:
        for canonical, aliases in ALIASES.items():
            schema_mapping_rows.append(
                {
                    "source_file": "",
                    "canonical_field": canonical,
                    "matched_source_column": "",
                    "aliases": "|".join(aliases),
                    "mapped": False,
                    "passed": False,
                }
            )

    row_count = len(combined_rows)
    game_pk_values = [str(row.get("game_pk", "")).strip() for row in combined_rows]
    duplicate_counts = {key: count for key, count in Counter(game_pk_values).items() if key and count > 1}
    duplicate_rows = [
        {"game_pk": game_pk, "duplicate_count": count, "passed": False}
        for game_pk, count in sorted(duplicate_counts.items())
    ]

    invalid_rows: list[dict[str, Any]] = []
    provenance_values: Counter[str] = Counter()

    for row in combined_rows:
        reasons: list[str] = []
        game_pk = str(row.get("game_pk", "")).strip()
        game_date = str(row.get("game_date", "")).strip()
        home_team = str(row.get("home_team", "")).strip()
        away_team = str(row.get("away_team", "")).strip()
        source_artifact = str(row.get("source_artifact", "")).strip()
        home_score = parse_int(row.get("home_score"))
        away_score = parse_int(row.get("away_score"))
        home_win_binary = parse_int(row.get("home_win_binary"))

        if not game_pk:
            reasons.append("missing_game_pk")
        if not parse_date_ok(game_date):
            reasons.append("invalid_game_date")
        if not home_team:
            reasons.append("missing_home_team")
        if not away_team:
            reasons.append("missing_away_team")
        if home_score is None or home_score < 0:
            reasons.append("invalid_home_score")
        if away_score is None or away_score < 0:
            reasons.append("invalid_away_score")
        if home_win_binary not in (0, 1):
            reasons.append("invalid_home_win_binary")
        if home_score is not None and away_score is not None and home_win_binary in (0, 1):
            expected = int(home_score > away_score)
            if home_win_binary != expected:
                reasons.append("home_win_binary_mismatch")
        if home_score is not None and away_score is not None and home_score == away_score:
            reasons.append("tie_game_blocked")
        if not source_artifact:
            reasons.append("missing_source_artifact")

        if source_artifact:
            provenance_values[source_artifact] += 1

        if reasons:
            invalid_rows.append(
                {
                    "source_file": row.get("source_file", ""),
                    "source_row_number": row.get("source_row_number", ""),
                    "game_pk": game_pk,
                    "reasons": "|".join(reasons),
                    "passed": False,
                }
            )

    schema_valid = bool(source_files) and bool(schema_mapping_rows) and all_passed(schema_mapping_rows)
    values_valid = row_count > 0 and not invalid_rows and not duplicate_rows and not source_read_errors
    provenance_valid = row_count > 0 and all(str(row.get("source_artifact", "")).strip() for row in combined_rows)
    home_win_binary_consistency_valid = not any("home_win_binary_mismatch" in row.get("reasons", "") for row in invalid_rows)
    source_files_found_count = len(source_files)
    invalid_row_count = len(invalid_rows)
    duplicate_game_pk_count = sum(duplicate_counts.values()) if duplicate_counts else 0

    schema_check_rows = [
        {"check": "source_files_found", "expected": ">0", "actual": source_files_found_count, "passed": source_files_found_count > 0},
        {"check": "source_read_errors", "expected": 0, "actual": len(source_read_errors), "passed": len(source_read_errors) == 0},
        {"check": "required_schema_or_aliases_mapped", "expected": True, "actual": schema_valid, "passed": schema_valid},
        {"check": "row_count_positive", "expected": True, "actual": row_count > 0, "passed": row_count > 0},
    ]

    value_check_rows = [
        {"check": "game_pk_non_null", "passed": row_count > 0 and all(bool(str(row.get("game_pk", "")).strip()) for row in combined_rows)},
        {"check": "game_date_parseable", "passed": row_count > 0 and all(parse_date_ok(row.get("game_date", "")) for row in combined_rows)},
        {"check": "home_team_non_empty", "passed": row_count > 0 and all(bool(str(row.get("home_team", "")).strip()) for row in combined_rows)},
        {"check": "away_team_non_empty", "passed": row_count > 0 and all(bool(str(row.get("away_team", "")).strip()) for row in combined_rows)},
        {"check": "home_score_integer_nonnegative", "passed": row_count > 0 and all((parse_int(row.get("home_score")) is not None and parse_int(row.get("home_score")) >= 0) for row in combined_rows)},
        {"check": "away_score_integer_nonnegative", "passed": row_count > 0 and all((parse_int(row.get("away_score")) is not None and parse_int(row.get("away_score")) >= 0) for row in combined_rows)},
        {"check": "home_win_binary_is_0_or_1", "passed": row_count > 0 and all(parse_int(row.get("home_win_binary")) in (0, 1) for row in combined_rows)},
        {"check": "home_win_binary_matches_home_score_gt_away_score", "passed": home_win_binary_consistency_valid and row_count > 0},
        {"check": "source_artifact_non_empty", "passed": provenance_valid},
        {"check": "duplicate_game_pk_count_zero", "passed": duplicate_game_pk_count == 0},
        {"check": "invalid_row_count_zero", "passed": invalid_row_count == 0},
    ]

    provenance_rows = [
        {"source_artifact": source, "row_count": count, "passed": True}
        for source, count in sorted(provenance_values.items())
    ]
    if not provenance_rows:
        provenance_rows = [{"source_artifact": "", "row_count": 0, "passed": False}]

    moneyline_boundary_rows = [
        {"boundary": "historical_moneyline_validation", "status": "deferred", "passed": True},
        {"boundary": "market_comparison_metrics", "status": "blocked", "passed": True},
        {"boundary": "roi_clv_market_edge_claims", "status": "blocked", "passed": True},
        {"boundary": "actuals_only_metrics", "status": "blocked_until_6nb_audit_passes", "passed": True},
    ]

    complete = (
        all_passed(predecessor_rows)
        and all_passed(input_rows)
        and source_files_found_count > 0
        and schema_valid
        and values_valid
        and provenance_valid
        and home_win_binary_consistency_valid
    )

    blocked = not complete
    diagnosis = DIAGNOSIS_COMPLETE if complete else DIAGNOSIS_BLOCKED
    recommended_next_layer = RECOMMENDED_NEXT_COMPLETE if complete else ""
    recommended_path = RECOMMENDED_PATH_COMPLETE if complete else RECOMMENDED_PATH_BLOCKED

    allowed_next_rows = [
        {
            "operation": "audit_historical_actuals_source_validation",
            "allowed_next": complete,
            "scope": "6NB audit only" if complete else "blocked_until_actuals_source_repaired",
            "passed": True,
        },
    ]

    forbidden_next_rows = [
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

    future_6nb_rows = [
        {"contract": "audit_6na_validation_results", "required_if_complete": True, "passed": True},
        {"contract": "verify_no_metric_execution", "required_if_complete": True, "passed": True},
        {"contract": "verify_moneyline_deferral_boundaries", "required_if_complete": True, "passed": True},
        {"contract": "allow_actuals_only_metrics_plan_after_audit", "required_if_complete": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6mz_passed", "expected": True, "actual": json_6mz.get("all_checks_passed"), "passed": json_6mz.get("all_checks_passed") is True},
        {"decision": "6mz_diagnosis_valid", "expected": DIAGNOSIS_6MZ, "actual": json_6mz.get("diagnosis"), "passed": json_6mz.get("diagnosis") == DIAGNOSIS_6MZ},
        {"decision": "all_required_6mz_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "source_files_found", "expected": ">0", "actual": source_files_found_count, "passed": source_files_found_count > 0},
        {"decision": "schema_valid", "expected": True, "actual": schema_valid, "passed": schema_valid},
        {"decision": "values_valid", "expected": True, "actual": values_valid, "passed": values_valid},
        {"decision": "provenance_valid", "expected": True, "actual": provenance_valid, "passed": provenance_valid},
        {"decision": "home_win_binary_consistency_valid", "expected": True, "actual": home_win_binary_consistency_valid, "passed": home_win_binary_consistency_valid},
        {"decision": "no_duplicate_game_pk", "expected": True, "actual": duplicate_game_pk_count == 0, "passed": duplicate_game_pk_count == 0},
        {"decision": "complete", "expected": True, "actual": complete, "passed": complete},
        {"decision": "blocked", "expected": False, "actual": blocked, "passed": not blocked},
        {"decision": "do_not_execute_metrics_backtest_tune_activate_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "validation_only_historical_actuals_source", "expected": True, "actual": True, "passed": True},
        {"boundary": "local_source_files_checked_by_6na", "expected": True, "actual": True, "passed": True},
        {"boundary": "local_source_files_read_by_6na", "expected": source_files_found_count > 0, "actual": source_files_found_count > 0, "passed": True},
        {"boundary": "source_rows_ingested_by_6na", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6na", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6na", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6na", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6na", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6na", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_COMPLETE, "actual": recommended_next_layer, "passed": complete},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_COMPLETE if complete else RECOMMENDED_PATH_BLOCKED, "actual": recommended_path, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_COMPLETE if complete else DIAGNOSIS_BLOCKED, "actual": diagnosis, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "source_discovery", "passed": source_files_found_count > 0, "detail": f"source_files_found={source_files_found_count}"},
        {"check": "schema_mapping", "passed": schema_valid, "detail": f"{sum(1 for r in schema_mapping_rows if boolish(r.get('passed')) )}/{len(schema_mapping_rows)}"},
        {"check": "schema_checks", "passed": all_passed(schema_check_rows), "detail": f"{sum(1 for r in schema_check_rows if r['passed'])}/{len(schema_check_rows)}"},
        {"check": "value_checks", "passed": all_passed(value_check_rows), "detail": f"{sum(1 for r in value_check_rows if r['passed'])}/{len(value_check_rows)}"},
        {"check": "duplicate_game_pk_review", "passed": duplicate_game_pk_count == 0, "detail": f"duplicate_game_pk_count={duplicate_game_pk_count}"},
        {"check": "invalid_rows_sample", "passed": invalid_row_count == 0, "detail": f"invalid_row_count={invalid_row_count}"},
        {"check": "provenance_review", "passed": provenance_valid, "detail": f"source_artifacts={len(provenance_values)}"},
        {"check": "moneyline_deferral_boundaries", "passed": all_passed(moneyline_boundary_rows), "detail": f"{sum(1 for r in moneyline_boundary_rows if r['passed'])}/{len(moneyline_boundary_rows)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next_rows), "detail": f"{sum(1 for r in allowed_next_rows if r['passed'])}/{len(allowed_next_rows)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next_rows), "detail": f"{sum(1 for r in forbidden_next_rows if r['passed'])}/{len(forbidden_next_rows)}"},
        {"check": "future_6nb_contract", "passed": all_passed(future_6nb_rows), "detail": f"{sum(1 for r in future_6nb_rows if r['passed'])}/{len(future_6nb_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = complete

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "source_discovery": write_csv(SOURCE_DISCOVERY_CSV, source_discovery_rows),
        "schema_mapping": write_csv(SCHEMA_MAPPING_CSV, schema_mapping_rows),
        "schema_checks": write_csv(SCHEMA_CHECKS_CSV, schema_check_rows),
        "value_checks": write_csv(VALUE_CHECKS_CSV, value_check_rows),
        "duplicate_game_pk_review": write_csv(DUPLICATES_CSV, duplicate_rows),
        "invalid_rows_sample": write_csv(INVALID_ROWS_CSV, invalid_rows[:100]),
        "provenance_review": write_csv(PROVENANCE_CSV, provenance_rows),
        "moneyline_deferral_boundaries": write_csv(MONEYLINE_BOUNDARIES_CSV, moneyline_boundary_rows),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next_rows),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next_rows),
        "future_6nb_contract": write_csv(FUTURE_6NB_CSV, future_6nb_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6NA",
        "layer_type": "game_mechanics_realism",
        "validation_only_historical_actuals_source": True,
        "all_checks_passed": all_checks_passed,
        "blocked": blocked,
        "diagnosis": diagnosis,
        "recommended_next_layer": recommended_next_layer,
        "recommended_path": recommended_path,
        "predecessor_layer": "6MZ",
        "predecessor_diagnosis": json_6mz.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mz.get("all_checks_passed") is True,
        "source_family": "historical_actuals_source_validation",
        "local_source_files_checked_by_6na": True,
        "local_source_files_read_by_6na": source_files_found_count > 0,
        "source_rows_validated_by_6na": row_count,
        "source_rows_ingested_by_6na": False,
        "normalized_source_tables_created_for_production_by_6na": False,
        "production_code_modified_by_6na": False,
        "adapter_call_executed_by_6na": False,
        "metric_execution_allowed_next": False,
        "metric_execution_run_by_6na": False,
        "backtest_execution_allowed_next": False,
        "backtest_execution_run_by_6na": False,
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
        "accepted_source_locations_checked": True,
        "source_files_found_count": source_files_found_count,
        "row_count": row_count,
        "invalid_row_count": invalid_row_count,
        "duplicate_game_pk_count": duplicate_game_pk_count,
        "schema_valid": schema_valid,
        "values_valid": values_valid,
        "provenance_valid": provenance_valid,
        "home_win_binary_consistency_valid": home_win_binary_consistency_valid,
        "moneyline_deferral_boundaries_preserved": True,
        "historical_actuals_source_validation_audit_allowed_next": complete,
        "source_read_errors": source_read_errors,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "source_discovery_csv": str(SOURCE_DISCOVERY_CSV),
            "schema_mapping_csv": str(SCHEMA_MAPPING_CSV),
            "schema_checks_csv": str(SCHEMA_CHECKS_CSV),
            "value_checks_csv": str(VALUE_CHECKS_CSV),
            "duplicate_game_pk_review_csv": str(DUPLICATES_CSV),
            "invalid_rows_sample_csv": str(INVALID_ROWS_CSV),
            "provenance_review_csv": str(PROVENANCE_CSV),
            "moneyline_deferral_boundaries_csv": str(MONEYLINE_BOUNDARIES_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6nb_contract_csv": str(FUTURE_6NB_CSV),
            "decision_csv": str(DECISION_CSV),
            "safety_boundaries_csv": str(SAFETY_CSV),
            "recommended_path_csv": str(RECOMMENDED_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
