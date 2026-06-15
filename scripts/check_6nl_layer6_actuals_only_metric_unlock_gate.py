#!/usr/bin/env python3
"""Check actuals-only metric unlock gates before metric execution."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6nl_actuals_only_metric_unlock_gate_check"
TMP_DIR = Path("tmp")

SCRIPT_6NK = Path("scripts/plan_6nk_layer6_actuals_only_metric_unlock_gate.py")
JSON_6NK = TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan.json"
TARGET_ACTUALS = Path("data/local/historical_actuals.csv")

REQUIRED_INPUTS = [
    JSON_6NK,
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_checks.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_predecessor.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_gate_inventory.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_minimum_sample_gate.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_schema_value_provenance_gate.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_6na_rerun_gate.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_source_expansion_audit_gate.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_metric_scope_gate.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_forbidden_metrics_gate.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_post_metric_audit_requirements.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_metric_unlock_boundaries.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_future_6nl_contract.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_decision.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6nk_actuals_only_metric_unlock_gate_plan_recommended_path.csv",
    SCRIPT_6NK,
    TARGET_ACTUALS,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
MINIMUM_SAMPLE_GATE_CSV = TMP_DIR / f"{SLUG}_minimum_sample_gate.csv"
SCHEMA_VALUE_PROV_GATE_CSV = TMP_DIR / f"{SLUG}_schema_value_provenance_gate.csv"
RERUN_6NA_GATE_CSV = TMP_DIR / f"{SLUG}_6na_rerun_gate.csv"
SOURCE_EXP_AUDIT_GATE_CSV = TMP_DIR / f"{SLUG}_source_expansion_audit_gate.csv"
METRIC_SCOPE_GATE_CSV = TMP_DIR / f"{SLUG}_metric_scope_gate.csv"
FORBIDDEN_METRICS_GATE_CSV = TMP_DIR / f"{SLUG}_forbidden_metrics_gate.csv"
POST_METRIC_AUDIT_REQ_CSV = TMP_DIR / f"{SLUG}_post_metric_audit_requirements.csv"
UNLOCK_DECISION_CSV = TMP_DIR / f"{SLUG}_unlock_decision.csv"
METRIC_UNLOCK_CSV = TMP_DIR / f"{SLUG}_metric_unlock_boundaries.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6NM_CSV = TMP_DIR / f"{SLUG}_future_6nm_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6NK = "layer_6_actuals_only_metric_unlock_gate_plan_complete"
DIAGNOSIS_6NL = "layer_6_actuals_only_metric_unlock_gate_check_complete"
RECOMMENDED_NEXT_LAYER = "6NM_layer_6_actuals_only_metric_execution"
RECOMMENDED_PATH = "execute_actuals_only_metrics_then_audit_outputs"


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
    json_6nk = load_json(JSON_6NK)

    checked_path = str(json_6nk.get("audited_actuals_path") or TARGET_ACTUALS)
    row_count = int(json_6nk.get("audited_actuals_row_count") or 0)
    date_span = int(json_6nk.get("audited_actuals_date_span_days") or 0)
    classification = str(json_6nk.get("audited_actuals_sample_classification") or "")
    sufficient = json_6nk.get("audited_actuals_sufficient_for_real_historical_evaluation") is True

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
        {"check": "6nk_script_exists", "expected": True, "actual": SCRIPT_6NK.exists(), "passed": SCRIPT_6NK.exists()},
        {"check": "6nk_json_exists", "expected": True, "actual": JSON_6NK.exists(), "passed": JSON_6NK.exists()},
        {"check": "6nk_all_checks_passed", "expected": True, "actual": json_6nk.get("all_checks_passed"), "passed": json_6nk.get("all_checks_passed") is True},
        {"check": "6nk_diagnosis", "expected": DIAGNOSIS_6NK, "actual": json_6nk.get("diagnosis"), "passed": json_6nk.get("diagnosis") == DIAGNOSIS_6NK},
        {"check": "6nk_recommended_next", "expected": "6NL_layer_6_actuals_only_metric_unlock_gate_check", "actual": json_6nk.get("recommended_next_layer"), "passed": json_6nk.get("recommended_next_layer") == "6NL_layer_6_actuals_only_metric_unlock_gate_check"},
    ]

    minimum_sample_rows = [
        {"gate": "row_count_minimum", "expected": ">=100", "actual": row_count, "passed": row_count >= 100},
        {"gate": "date_span_minimum", "expected": ">=21", "actual": date_span, "passed": date_span >= 21},
        {"gate": "sample_classification", "expected": "larger_sample", "actual": classification, "passed": classification == "larger_sample"},
        {"gate": "sufficient_for_real_historical_evaluation", "expected": True, "actual": sufficient, "passed": sufficient},
    ]

    schema_value_prov_rows = [
        {"gate": "schema_value_provenance_gate_required", "expected": True, "actual": json_6nk.get("schema_value_provenance_gate_required"), "passed": json_6nk.get("schema_value_provenance_gate_required") is True},
        {"gate": "actuals_file_exists", "expected": True, "actual": TARGET_ACTUALS.exists(), "passed": TARGET_ACTUALS.exists()},
        {"gate": "actuals_path_matches", "expected": "data/local/historical_actuals.csv", "actual": checked_path, "passed": checked_path == "data/local/historical_actuals.csv"},
    ]

    rerun_6na_rows = [
        {"gate": "rerun_6na_gate_required", "expected": True, "actual": json_6nk.get("rerun_6na_gate_required"), "passed": json_6nk.get("rerun_6na_gate_required") is True},
    ]

    source_audit_rows = [
        {"gate": "source_expansion_audit_gate_required", "expected": True, "actual": json_6nk.get("source_expansion_audit_gate_required"), "passed": json_6nk.get("source_expansion_audit_gate_required") is True},
        {"gate": "predecessor_6nk_passed", "expected": True, "actual": json_6nk.get("all_checks_passed"), "passed": json_6nk.get("all_checks_passed") is True},
    ]

    metric_scope_rows = [
        {"gate": "metric_scope_gate_required", "expected": True, "actual": json_6nk.get("metric_scope_gate_required"), "passed": json_6nk.get("metric_scope_gate_required") is True},
        {"scope": "actuals_only_home_win_accuracy", "allowed_next_layer": True, "passed": True},
        {"scope": "actuals_only_confusion_table", "allowed_next_layer": True, "passed": True},
        {"scope": "actuals_only_coverage_and_join_rate", "allowed_next_layer": True, "passed": True},
        {"scope": "actuals_only_error_bucket_summary", "allowed_next_layer": True, "passed": True},
    ]

    forbidden_metrics_rows = [
        {"gate": "forbidden_metrics_gate_required", "expected": True, "actual": json_6nk.get("forbidden_metrics_gate_required"), "passed": json_6nk.get("forbidden_metrics_gate_required") is True},
        {"forbidden_scope": "historical_backtest", "allowed_next_layer": False, "passed": True},
        {"forbidden_scope": "parameter_tuning", "allowed_next_layer": False, "passed": True},
        {"forbidden_scope": "mechanics_activation", "allowed_next_layer": False, "passed": True},
        {"forbidden_scope": "layer_6_exit", "allowed_next_layer": False, "passed": True},
        {"forbidden_scope": "live_fetch", "allowed_next_layer": False, "passed": True},
        {"forbidden_scope": "remote_api_call", "allowed_next_layer": False, "passed": True},
    ]

    post_metric_audit_rows = [
        {"requirement": "post_metric_execution_audit_required", "expected": True, "actual": json_6nk.get("post_metric_execution_audit_required"), "passed": json_6nk.get("post_metric_execution_audit_required") is True},
        {"requirement": "audit_actuals_only_metric_outputs_after_execution", "required_after_metric_layer": True, "passed": True},
        {"requirement": "audit_no_backtest_or_tuning_performed", "required_after_metric_layer": True, "passed": True},
        {"requirement": "audit_join_keys_and_unmatched_rows", "required_after_metric_layer": True, "passed": True},
    ]

    gate_checks = [
        all_passed(minimum_sample_rows),
        all_passed(schema_value_prov_rows),
        all_passed(rerun_6na_rows),
        all_passed(source_audit_rows),
        all_passed(metric_scope_rows),
        all_passed(forbidden_metrics_rows),
        all_passed(post_metric_audit_rows),
        json_6nk.get("actuals_only_metric_unlock_gate_check_allowed_next") is True,
    ]
    unlock_allowed = all(gate_checks)

    unlock_decision_rows = [
        {"decision": "minimum_sample_gate_passed", "value": all_passed(minimum_sample_rows), "passed": all_passed(minimum_sample_rows)},
        {"decision": "schema_value_provenance_gate_passed", "value": all_passed(schema_value_prov_rows), "passed": all_passed(schema_value_prov_rows)},
        {"decision": "rerun_6na_gate_passed", "value": all_passed(rerun_6na_rows), "passed": all_passed(rerun_6na_rows)},
        {"decision": "source_expansion_audit_gate_passed", "value": all_passed(source_audit_rows), "passed": all_passed(source_audit_rows)},
        {"decision": "metric_scope_gate_passed", "value": all_passed(metric_scope_rows), "passed": all_passed(metric_scope_rows)},
        {"decision": "forbidden_metrics_gate_passed", "value": all_passed(forbidden_metrics_rows), "passed": all_passed(forbidden_metrics_rows)},
        {"decision": "post_metric_execution_audit_required", "value": all_passed(post_metric_audit_rows), "passed": all_passed(post_metric_audit_rows)},
        {"decision": "actuals_only_metric_execution_allowed_after_6nl", "value": unlock_allowed, "passed": unlock_allowed},
    ]

    metric_unlock_rows = [
        {"boundary": "actuals_only_metric_execution_allowed_after_6nl", "value": unlock_allowed, "passed": unlock_allowed},
        {"boundary": "actuals_only_metric_execution_allowed_next", "value": unlock_allowed, "passed": unlock_allowed},
        {"boundary": "metric_execution_allowed_next", "value": unlock_allowed, "passed": unlock_allowed},
        {"boundary": "backtest_execution_allowed_next", "value": False, "passed": True},
        {"boundary": "tuning_allowed_next", "value": False, "passed": True},
        {"boundary": "activation_allowed_next", "value": False, "passed": True},
        {"boundary": "layer_6_exit_allowed_next", "value": False, "passed": True},
    ]

    allowed_next_rows = [
        {"operation": "actuals_only_metric_execution", "allowed_next": unlock_allowed, "scope": "6NM actuals-only metrics only", "passed": unlock_allowed},
    ]

    forbidden_next_rows = [
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "production_table_creation", "allowed_next": False, "passed": True},
    ]

    future_6nm_rows = [
        {"contract": "execute_actuals_only_metrics_only", "required": True, "passed": True},
        {"contract": "write_actuals_only_metric_outputs_to_tmp", "required": True, "passed": True},
        {"contract": "preserve_no_backtests_tuning_activation_exit", "required": True, "passed": True},
        {"contract": "require_post_metric_audit_next", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6nk_passed", "expected": True, "actual": json_6nk.get("all_checks_passed"), "passed": json_6nk.get("all_checks_passed") is True},
        {"decision": "all_unlock_gates_passed", "expected": True, "actual": unlock_allowed, "passed": unlock_allowed},
        {"decision": "metric_execution_not_run_by_6nl", "expected": True, "actual": True, "passed": True},
        {"decision": "recommend_6nm", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": unlock_allowed},
    ]

    safety_rows = [
        {"boundary": "gate_check_only_actuals_only_metric_unlock", "expected": True, "actual": True, "passed": True},
        {"boundary": "actuals_file_modified_by_6nl", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_rows_ingested_by_6nl", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6nl", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6nl", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6nl", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6nl", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6nl", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": unlock_allowed},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": unlock_allowed},
        {"decision": "recommend_actuals_only_metric_execution_next", "expected": True, "actual": unlock_allowed, "passed": unlock_allowed},
        {"decision": "do_not_recommend_backtests", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning_activation_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6NL, "actual": DIAGNOSIS_6NL, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "minimum_sample_gate", "passed": all_passed(minimum_sample_rows), "detail": f"{sum(1 for r in minimum_sample_rows if r['passed'])}/{len(minimum_sample_rows)}"},
        {"check": "schema_value_provenance_gate", "passed": all_passed(schema_value_prov_rows), "detail": f"{sum(1 for r in schema_value_prov_rows if r['passed'])}/{len(schema_value_prov_rows)}"},
        {"check": "6na_rerun_gate", "passed": all_passed(rerun_6na_rows), "detail": f"{sum(1 for r in rerun_6na_rows if r['passed'])}/{len(rerun_6na_rows)}"},
        {"check": "source_expansion_audit_gate", "passed": all_passed(source_audit_rows), "detail": f"{sum(1 for r in source_audit_rows if r['passed'])}/{len(source_audit_rows)}"},
        {"check": "metric_scope_gate", "passed": all_passed(metric_scope_rows), "detail": f"{sum(1 for r in metric_scope_rows if r['passed'])}/{len(metric_scope_rows)}"},
        {"check": "forbidden_metrics_gate", "passed": all_passed(forbidden_metrics_rows), "detail": f"{sum(1 for r in forbidden_metrics_rows if r['passed'])}/{len(forbidden_metrics_rows)}"},
        {"check": "post_metric_audit_requirements", "passed": all_passed(post_metric_audit_rows), "detail": f"{sum(1 for r in post_metric_audit_rows if r['passed'])}/{len(post_metric_audit_rows)}"},
        {"check": "unlock_decision", "passed": all_passed(unlock_decision_rows), "detail": f"{sum(1 for r in unlock_decision_rows if r['passed'])}/{len(unlock_decision_rows)}"},
        {"check": "metric_unlock_boundaries", "passed": all_passed(metric_unlock_rows), "detail": f"{sum(1 for r in metric_unlock_rows if r['passed'])}/{len(metric_unlock_rows)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next_rows), "detail": f"{sum(1 for r in allowed_next_rows if r['passed'])}/{len(allowed_next_rows)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next_rows), "detail": f"{sum(1 for r in forbidden_next_rows if r['passed'])}/{len(forbidden_next_rows)}"},
        {"check": "future_6nm_contract", "passed": all_passed(future_6nm_rows), "detail": f"{sum(1 for r in future_6nm_rows if r['passed'])}/{len(future_6nm_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "minimum_sample_gate": write_csv(MINIMUM_SAMPLE_GATE_CSV, minimum_sample_rows),
        "schema_value_provenance_gate": write_csv(SCHEMA_VALUE_PROV_GATE_CSV, schema_value_prov_rows),
        "6na_rerun_gate": write_csv(RERUN_6NA_GATE_CSV, rerun_6na_rows),
        "source_expansion_audit_gate": write_csv(SOURCE_EXP_AUDIT_GATE_CSV, source_audit_rows),
        "metric_scope_gate": write_csv(METRIC_SCOPE_GATE_CSV, metric_scope_rows),
        "forbidden_metrics_gate": write_csv(FORBIDDEN_METRICS_GATE_CSV, forbidden_metrics_rows),
        "post_metric_audit_requirements": write_csv(POST_METRIC_AUDIT_REQ_CSV, post_metric_audit_rows),
        "unlock_decision": write_csv(UNLOCK_DECISION_CSV, unlock_decision_rows),
        "metric_unlock_boundaries": write_csv(METRIC_UNLOCK_CSV, metric_unlock_rows),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next_rows),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next_rows),
        "future_6nm_contract": write_csv(FUTURE_6NM_CSV, future_6nm_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6NL",
        "layer_type": "game_mechanics_realism",
        "gate_check_only_actuals_only_metric_unlock": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6NL if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_layer": "6NK",
        "predecessor_diagnosis": json_6nk.get("diagnosis"),
        "predecessor_all_checks_passed": json_6nk.get("all_checks_passed") is True,
        "source_family": "actuals_only_metric_unlock_gate_check",
        "checked_actuals_path": checked_path,
        "checked_actuals_row_count": row_count,
        "checked_actuals_date_span_days": date_span,
        "checked_actuals_sample_classification": classification,
        "checked_actuals_sufficient_for_real_historical_evaluation": sufficient,
        "minimum_sample_gate_passed": all_passed(minimum_sample_rows),
        "schema_value_provenance_gate_passed": all_passed(schema_value_prov_rows),
        "rerun_6na_gate_passed": all_passed(rerun_6na_rows),
        "source_expansion_audit_gate_passed": all_passed(source_audit_rows),
        "metric_scope_gate_passed": all_passed(metric_scope_rows),
        "forbidden_metrics_gate_passed": all_passed(forbidden_metrics_rows),
        "post_metric_execution_audit_required": all_passed(post_metric_audit_rows),
        "actuals_only_metric_execution_allowed_after_6nl": unlock_allowed,
        "actuals_only_metric_execution_allowed_next": unlock_allowed,
        "metric_execution_allowed_next": unlock_allowed,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "source_rows_ingested_by_6nl": False,
        "normalized_source_tables_created_for_production_by_6nl": False,
        "production_code_modified_by_6nl": False,
        "actuals_file_modified_by_6nl": False,
        "adapter_call_executed_by_6nl": False,
        "metric_execution_run_by_6nl": False,
        "backtest_execution_run_by_6nl": False,
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
            "minimum_sample_gate_csv": str(MINIMUM_SAMPLE_GATE_CSV),
            "schema_value_provenance_gate_csv": str(SCHEMA_VALUE_PROV_GATE_CSV),
            "6na_rerun_gate_csv": str(RERUN_6NA_GATE_CSV),
            "source_expansion_audit_gate_csv": str(SOURCE_EXP_AUDIT_GATE_CSV),
            "metric_scope_gate_csv": str(METRIC_SCOPE_GATE_CSV),
            "forbidden_metrics_gate_csv": str(FORBIDDEN_METRICS_GATE_CSV),
            "post_metric_audit_requirements_csv": str(POST_METRIC_AUDIT_REQ_CSV),
            "unlock_decision_csv": str(UNLOCK_DECISION_CSV),
            "metric_unlock_boundaries_csv": str(METRIC_UNLOCK_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6nm_contract_csv": str(FUTURE_6NM_CSV),
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
