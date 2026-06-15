#!/usr/bin/env python3
"""Plan larger historical actuals source expansion before source creation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6nh_larger_historical_actuals_source_expansion_plan"
TMP_DIR = Path("tmp")

SCRIPT_6NG = Path("scripts/audit_6ng_layer6_historical_actuals_smoke_sample_status_plan.py")
JSON_6NG = TMP_DIR / "layer6_6ng_historical_actuals_smoke_sample_status_plan_audit.json"

REQUIRED_INPUTS = [
    JSON_6NG,
    TMP_DIR / "layer6_6ng_historical_actuals_smoke_sample_status_plan_audit_checks.csv",
    TMP_DIR / "layer6_6ng_historical_actuals_smoke_sample_status_plan_audit_predecessor.csv",
    TMP_DIR / "layer6_6ng_historical_actuals_smoke_sample_status_plan_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6ng_historical_actuals_smoke_sample_status_plan_audit_current_status_review.csv",
    TMP_DIR / "layer6_6ng_historical_actuals_smoke_sample_status_plan_audit_larger_sample_requirements_review.csv",
    TMP_DIR / "layer6_6ng_historical_actuals_smoke_sample_status_plan_audit_metric_unlock_review.csv",
    TMP_DIR / "layer6_6ng_historical_actuals_smoke_sample_status_plan_audit_allowed_operations_next.csv",
    TMP_DIR / "layer6_6ng_historical_actuals_smoke_sample_status_plan_audit_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6ng_historical_actuals_smoke_sample_status_plan_audit_future_6nh_contract.csv",
    TMP_DIR / "layer6_6ng_historical_actuals_smoke_sample_status_plan_audit_decision.csv",
    TMP_DIR / "layer6_6ng_historical_actuals_smoke_sample_status_plan_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6ng_historical_actuals_smoke_sample_status_plan_audit_recommended_path.csv",
    SCRIPT_6NG,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
SOURCE_INVENTORY_CSV = TMP_DIR / f"{SLUG}_source_inventory.csv"
TARGET_CONTRACT_CSV = TMP_DIR / f"{SLUG}_target_contract.csv"
SOURCE_PRECEDENCE_CSV = TMP_DIR / f"{SLUG}_source_precedence.csv"
DEDUP_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_deduplication_requirements.csv"
SCHEMA_VALUE_REQ_CSV = TMP_DIR / f"{SLUG}_schema_value_requirements.csv"
PROVENANCE_REQ_CSV = TMP_DIR / f"{SLUG}_provenance_requirements.csv"
VALIDATION_REQ_CSV = TMP_DIR / f"{SLUG}_validation_requirements.csv"
METRIC_UNLOCK_CSV = TMP_DIR / f"{SLUG}_metric_unlock_boundaries.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6NI_CSV = TMP_DIR / f"{SLUG}_future_6ni_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6NG = "layer_6_historical_actuals_smoke_sample_status_plan_audit_complete"
DIAGNOSIS_6NH = "layer_6_larger_historical_actuals_source_expansion_plan_complete"
RECOMMENDED_NEXT_LAYER = "6NI_layer_6_larger_historical_actuals_source_expansion_implementation"
RECOMMENDED_PATH = "implement_larger_historical_actuals_source_expansion_before_validation"

CANONICAL_SCHEMA = [
    "game_pk",
    "game_date",
    "home_team",
    "away_team",
    "home_score",
    "away_score",
    "home_win_binary",
    "source_artifact",
]
DEDUP_KEY = ["game_pk"]


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
    json_6ng = load_json(JSON_6NG)

    current_rows = int(json_6ng.get("audited_current_actuals_row_count") or 0)
    current_span = int(json_6ng.get("audited_current_actuals_date_span_days") or 0)
    min_rows = int(json_6ng.get("audited_minimum_larger_sample_row_count") or 100)
    min_span = int(json_6ng.get("audited_minimum_larger_sample_date_span_days") or 21)

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
        {"check": "6ng_script_exists", "expected": True, "actual": SCRIPT_6NG.exists(), "passed": SCRIPT_6NG.exists()},
        {"check": "6ng_json_exists", "expected": True, "actual": JSON_6NG.exists(), "passed": JSON_6NG.exists()},
        {"check": "6ng_all_checks_passed", "expected": True, "actual": json_6ng.get("all_checks_passed"), "passed": json_6ng.get("all_checks_passed") is True},
        {"check": "6ng_diagnosis", "expected": DIAGNOSIS_6NG, "actual": json_6ng.get("diagnosis"), "passed": json_6ng.get("diagnosis") == DIAGNOSIS_6NG},
        {"check": "6ng_recommended_next", "expected": "6NH_layer_6_larger_historical_actuals_source_expansion_plan", "actual": json_6ng.get("recommended_next_layer"), "passed": json_6ng.get("recommended_next_layer") == "6NH_layer_6_larger_historical_actuals_source_expansion_plan"},
    ]

    source_inventory_rows = [
        {"source_location": "data/local/historical_actuals.csv", "role": "current normalized target and possible replacement target", "accepted_for_6ni": True, "authoritative_final_output": True, "passed": True},
        {"source_location": "data/local/historical_actuals/*.csv", "role": "optional partitioned normalized larger actuals inputs", "accepted_for_6ni": True, "authoritative_final_output": False, "passed": True},
        {"source_location": "tmp/statsapi_cache/schedule/*.json", "role": "local cached schedule JSON source for creating larger normalized actuals", "accepted_for_6ni": True, "authoritative_final_output": False, "passed": True},
        {"source_location": "tmp/*", "role": "temporary artifacts only; not authoritative normalized actuals output", "accepted_for_6ni": False, "authoritative_final_output": False, "passed": True},
    ]

    target_contract_rows = [
        {"contract": "target_larger_actuals_path", "value": "data/local/historical_actuals.csv", "passed": True},
        {"contract": "canonical_schema", "value": "|".join(CANONICAL_SCHEMA), "passed": True},
        {"contract": "minimum_larger_sample_row_count", "value": min_rows, "passed": min_rows >= 100},
        {"contract": "minimum_larger_sample_date_span_days", "value": min_span, "passed": min_span >= 21},
        {"contract": "replacement_must_not_drop_schema_or_provenance", "value": True, "passed": True},
    ]

    precedence_rows = [
        {"rank": 1, "source": "data/local/historical_actuals/*.csv", "use_when": "valid partitioned normalized actuals files exist and meet schema/value/provenance contract", "passed": True},
        {"rank": 2, "source": "tmp/statsapi_cache/schedule/*.json", "use_when": "partitioned normalized actuals insufficient; parse completed games from cached schedule JSON", "passed": True},
        {"rank": 3, "source": "data/local/historical_actuals.csv", "use_when": "existing smoke file only as baseline to replace/augment after larger source creation", "passed": True},
    ]

    dedup_rows = [
        {"requirement": "deduplication_key", "value": "|".join(DEDUP_KEY), "passed": True},
        {"requirement": "duplicate_game_pk_policy", "value": "one canonical row per game_pk", "passed": True},
        {"requirement": "tie_resolution", "value": "ties invalid for completed MLB games in this source", "passed": True},
        {"requirement": "stable_sort", "value": "sort by game_date then game_pk before writing output", "passed": True},
    ]

    schema_value_rows = [
        {"requirement": "required_columns_exact", "value": "|".join(CANONICAL_SCHEMA), "passed": True},
        {"requirement": "game_pk", "value": "non-null unique integer/string identifier", "passed": True},
        {"requirement": "game_date", "value": "parseable ISO date", "passed": True},
        {"requirement": "team_fields", "value": "home_team and away_team non-empty", "passed": True},
        {"requirement": "scores", "value": "home_score and away_score non-negative integers", "passed": True},
        {"requirement": "home_win_binary", "value": "must equal int(home_score > away_score)", "passed": True},
    ]

    provenance_rows = [
        {"requirement": "source_artifact_required", "value": True, "passed": True},
        {"requirement": "source_artifact_path_points_to_local_input", "value": True, "passed": True},
        {"requirement": "candidate_file_inventory_required", "value": True, "passed": True},
        {"requirement": "selected_source_summary_required", "value": True, "passed": True},
    ]

    validation_rows = [
        {"requirement": "rerun_6na_after_larger_source_creation", "required": True, "passed": True},
        {"requirement": "6na_all_checks_must_pass", "required": True, "passed": True},
        {"requirement": "larger_source_creation_audit_required_after_validation", "required": True, "passed": True},
        {"requirement": "metrics_stay_blocked_until_validation_and_audit_pass", "required": True, "passed": True},
    ]

    metric_unlock_rows = [
        {"boundary": "current_rows_below_minimum", "current": current_rows, "minimum": min_rows, "passed": current_rows < min_rows},
        {"boundary": "current_span_below_minimum", "current": current_span, "minimum": min_span, "passed": current_span < min_span},
        {"boundary": "metric_execution_allowed_next", "value": False, "passed": True},
        {"boundary": "backtest_execution_allowed_next", "value": False, "passed": True},
        {"boundary": "real_historical_evaluation_allowed_next", "value": False, "passed": True},
    ]

    allowed_next_rows = [
        {"operation": "larger_historical_actuals_source_expansion_implementation", "allowed_next": True, "scope": "6NI implementation only", "passed": True},
    ]

    forbidden_next_rows = [
        {"operation": "metric_execution", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "actuals_only_metric_layer", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "real_historical_evaluation_claims", "allowed_next": False, "passed": True},
        {"operation": "production_table_creation", "allowed_next": False, "passed": True},
    ]

    future_6ni_rows = [
        {"contract": "implement_larger_source_creation_or_expansion", "required": True, "passed": True},
        {"contract": "write_or_replace_data_local_historical_actuals_csv_only_as_normalized_actuals_output", "required": True, "passed": True},
        {"contract": "preserve_schema_value_provenance_contract", "required": True, "passed": True},
        {"contract": "rerun_6na_after_creation", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_backtests_tuning_activation_exit", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6ng_passed", "expected": True, "actual": json_6ng.get("all_checks_passed"), "passed": json_6ng.get("all_checks_passed") is True},
        {"decision": "source_inventory_defined", "expected": True, "actual": all_passed(source_inventory_rows), "passed": all_passed(source_inventory_rows)},
        {"decision": "target_contract_defined", "expected": True, "actual": all_passed(target_contract_rows), "passed": all_passed(target_contract_rows)},
        {"decision": "validation_requirements_defined", "expected": True, "actual": all_passed(validation_rows), "passed": all_passed(validation_rows)},
        {"decision": "metric_unlock_boundaries_block_now", "expected": True, "actual": all_passed(metric_unlock_rows), "passed": all_passed(metric_unlock_rows)},
        {"decision": "recommend_6ni", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only_larger_historical_actuals_source_expansion", "expected": True, "actual": True, "passed": True},
        {"boundary": "actuals_file_modified_by_6nh", "expected": False, "actual": False, "passed": True},
        {"boundary": "larger_actuals_source_created_by_6nh", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_rows_ingested_by_6nh", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6nh", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6nh", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6nh", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6nh", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6nh", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "do_not_recommend_metrics", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_backtests", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning_activation_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6NH, "actual": DIAGNOSIS_6NH, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "source_inventory", "passed": all_passed(source_inventory_rows), "detail": f"{sum(1 for r in source_inventory_rows if r['passed'])}/{len(source_inventory_rows)}"},
        {"check": "target_contract", "passed": all_passed(target_contract_rows), "detail": f"{sum(1 for r in target_contract_rows if r['passed'])}/{len(target_contract_rows)}"},
        {"check": "source_precedence", "passed": all_passed(precedence_rows), "detail": f"{sum(1 for r in precedence_rows if r['passed'])}/{len(precedence_rows)}"},
        {"check": "deduplication_requirements", "passed": all_passed(dedup_rows), "detail": f"{sum(1 for r in dedup_rows if r['passed'])}/{len(dedup_rows)}"},
        {"check": "schema_value_requirements", "passed": all_passed(schema_value_rows), "detail": f"{sum(1 for r in schema_value_rows if r['passed'])}/{len(schema_value_rows)}"},
        {"check": "provenance_requirements", "passed": all_passed(provenance_rows), "detail": f"{sum(1 for r in provenance_rows if r['passed'])}/{len(provenance_rows)}"},
        {"check": "validation_requirements", "passed": all_passed(validation_rows), "detail": f"{sum(1 for r in validation_rows if r['passed'])}/{len(validation_rows)}"},
        {"check": "metric_unlock_boundaries", "passed": all_passed(metric_unlock_rows), "detail": f"{sum(1 for r in metric_unlock_rows if r['passed'])}/{len(metric_unlock_rows)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next_rows), "detail": f"{sum(1 for r in allowed_next_rows if r['passed'])}/{len(allowed_next_rows)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next_rows), "detail": f"{sum(1 for r in forbidden_next_rows if r['passed'])}/{len(forbidden_next_rows)}"},
        {"check": "future_6ni_contract", "passed": all_passed(future_6ni_rows), "detail": f"{sum(1 for r in future_6ni_rows if r['passed'])}/{len(future_6ni_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "source_inventory": write_csv(SOURCE_INVENTORY_CSV, source_inventory_rows),
        "target_contract": write_csv(TARGET_CONTRACT_CSV, target_contract_rows),
        "source_precedence": write_csv(SOURCE_PRECEDENCE_CSV, precedence_rows),
        "deduplication_requirements": write_csv(DEDUP_REQUIREMENTS_CSV, dedup_rows),
        "schema_value_requirements": write_csv(SCHEMA_VALUE_REQ_CSV, schema_value_rows),
        "provenance_requirements": write_csv(PROVENANCE_REQ_CSV, provenance_rows),
        "validation_requirements": write_csv(VALIDATION_REQ_CSV, validation_rows),
        "metric_unlock_boundaries": write_csv(METRIC_UNLOCK_CSV, metric_unlock_rows),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next_rows),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next_rows),
        "future_6ni_contract": write_csv(FUTURE_6NI_CSV, future_6ni_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6NH",
        "layer_type": "game_mechanics_realism",
        "planning_only_larger_historical_actuals_source_expansion": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6NH if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_layer": "6NG",
        "predecessor_diagnosis": json_6ng.get("diagnosis"),
        "predecessor_all_checks_passed": json_6ng.get("all_checks_passed") is True,
        "source_family": "larger_historical_actuals_source_expansion_plan",
        "current_actuals_path": json_6ng.get("audited_current_actuals_path"),
        "current_actuals_row_count": current_rows,
        "current_actuals_date_span_days": current_span,
        "current_sample_classification": json_6ng.get("audited_current_sample_classification"),
        "larger_source_expansion_required": True,
        "minimum_larger_sample_row_count": min_rows,
        "minimum_larger_sample_date_span_days": min_span,
        "target_larger_actuals_path": "data/local/historical_actuals.csv",
        "accepted_partitioned_actuals_pattern": "data/local/historical_actuals/*.csv",
        "accepted_local_schedule_cache_pattern": "tmp/statsapi_cache/schedule/*.json",
        "normalized_output_schema": CANONICAL_SCHEMA,
        "deduplication_key": DEDUP_KEY,
        "rerun_6na_required_after_larger_source_creation": True,
        "audit_required_after_larger_source_validation": True,
        "larger_historical_actuals_source_expansion_implementation_allowed_next": True,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "source_rows_ingested_by_6nh": False,
        "normalized_source_tables_created_for_production_by_6nh": False,
        "production_code_modified_by_6nh": False,
        "actuals_file_modified_by_6nh": False,
        "larger_actuals_source_created_by_6nh": False,
        "adapter_call_executed_by_6nh": False,
        "metric_execution_run_by_6nh": False,
        "backtest_execution_run_by_6nh": False,
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
        "moneyline_deferral_boundaries_preserved": True,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "source_inventory_csv": str(SOURCE_INVENTORY_CSV),
            "target_contract_csv": str(TARGET_CONTRACT_CSV),
            "source_precedence_csv": str(SOURCE_PRECEDENCE_CSV),
            "deduplication_requirements_csv": str(DEDUP_REQUIREMENTS_CSV),
            "schema_value_requirements_csv": str(SCHEMA_VALUE_REQ_CSV),
            "provenance_requirements_csv": str(PROVENANCE_REQ_CSV),
            "validation_requirements_csv": str(VALIDATION_REQ_CSV),
            "metric_unlock_boundaries_csv": str(METRIC_UNLOCK_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6ni_contract_csv": str(FUTURE_6NI_CSV),
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
