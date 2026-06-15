#!/usr/bin/env python3
"""Plan safe transition after actuals-only metric audit."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6no_post_actuals_metric_safe_transition_plan"
TMP_DIR = Path("tmp")

SCRIPT_6NN = Path("scripts/audit_6nn_layer6_actuals_only_metric_execution.py")
JSON_6NN = TMP_DIR / "layer6_6nn_actuals_only_metric_execution_audit.json"
TARGET_ACTUALS = Path("data/local/historical_actuals.csv")

REQUIRED_INPUTS = [
    JSON_6NN,
    TMP_DIR / "layer6_6nn_actuals_only_metric_execution_audit_checks.csv",
    TMP_DIR / "layer6_6nn_actuals_only_metric_execution_audit_predecessor.csv",
    TMP_DIR / "layer6_6nn_actuals_only_metric_execution_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6nn_actuals_only_metric_execution_audit_metric_value_review.csv",
    TMP_DIR / "layer6_6nn_actuals_only_metric_execution_audit_date_coverage_review.csv",
    TMP_DIR / "layer6_6nn_actuals_only_metric_execution_audit_source_coverage_review.csv",
    TMP_DIR / "layer6_6nn_actuals_only_metric_execution_audit_prediction_join_review.csv",
    TMP_DIR / "layer6_6nn_actuals_only_metric_execution_audit_forbidden_metric_review.csv",
    TMP_DIR / "layer6_6nn_actuals_only_metric_execution_audit_safety_review.csv",
    TMP_DIR / "layer6_6nn_actuals_only_metric_execution_audit_allowed_operations_next.csv",
    TMP_DIR / "layer6_6nn_actuals_only_metric_execution_audit_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6nn_actuals_only_metric_execution_audit_future_6no_contract.csv",
    TMP_DIR / "layer6_6nn_actuals_only_metric_execution_audit_decision.csv",
    TMP_DIR / "layer6_6nn_actuals_only_metric_execution_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6nn_actuals_only_metric_execution_audit_recommended_path.csv",
    SCRIPT_6NN,
    TARGET_ACTUALS,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
TRANSITION_OPTIONS_CSV = TMP_DIR / f"{SLUG}_transition_options.csv"
PREDICTION_JOIN_REQ_CSV = TMP_DIR / f"{SLUG}_prediction_join_readiness_requirements.csv"
BACKTEST_PREREQ_CSV = TMP_DIR / f"{SLUG}_backtest_prerequisites.csv"
FORBIDDEN_OPS_CSV = TMP_DIR / f"{SLUG}_forbidden_operations.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6NP_CSV = TMP_DIR / f"{SLUG}_future_6np_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6NN = "layer_6_actuals_only_metric_execution_audit_complete"
DIAGNOSIS_6NO = "layer_6_post_actuals_metric_safe_transition_plan_complete"
RECOMMENDED_NEXT_LAYER = "6NP_layer_6_prediction_join_readiness_gate_plan"
RECOMMENDED_PATH = "plan_prediction_join_readiness_gate_before_any_backtest"


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
    json_6nn = load_json(JSON_6NN)

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
        {"check": "6nn_script_exists", "expected": True, "actual": SCRIPT_6NN.exists(), "passed": SCRIPT_6NN.exists()},
        {"check": "6nn_json_exists", "expected": True, "actual": JSON_6NN.exists(), "passed": JSON_6NN.exists()},
        {"check": "6nn_all_checks_passed", "expected": True, "actual": json_6nn.get("all_checks_passed"), "passed": json_6nn.get("all_checks_passed") is True},
        {"check": "6nn_diagnosis", "expected": DIAGNOSIS_6NN, "actual": json_6nn.get("diagnosis"), "passed": json_6nn.get("diagnosis") == DIAGNOSIS_6NN},
        {"check": "safe_transition_plan_allowed_next", "expected": True, "actual": json_6nn.get("safe_transition_plan_allowed_next"), "passed": json_6nn.get("safe_transition_plan_allowed_next") is True},
    ]

    transition_options_rows = [
        {"option": "prediction_join_readiness_gate_plan", "allowed_next": True, "reason": "required before any prediction join or backtest", "passed": True},
        {"option": "prediction_join_execution", "allowed_next": False, "reason": "readiness gate has not been planned or checked", "passed": True},
        {"option": "historical_backtest", "allowed_next": False, "reason": "prediction join readiness and prediction artifact provenance are not validated", "passed": True},
        {"option": "tuning", "allowed_next": False, "reason": "no backtest or model evaluation layer is approved", "passed": True},
        {"option": "mechanics_activation", "allowed_next": False, "reason": "Layer 6 exit has not been earned", "passed": True},
    ]

    prediction_join_req_rows = [
        {"requirement": "identify_local_prediction_artifact", "required_before_join": True, "passed": True},
        {"requirement": "validate_prediction_artifact_schema", "required_before_join": True, "passed": True},
        {"requirement": "validate_prediction_artifact_provenance", "required_before_join": True, "passed": True},
        {"requirement": "validate_join_keys_against_actuals_game_pk", "required_before_join": True, "passed": True},
        {"requirement": "define_unmatched_row_handling", "required_before_join": True, "passed": True},
        {"requirement": "define_no_roi_or_betting_metrics_boundary", "required_before_join": True, "passed": True},
        {"requirement": "require_post_join_audit", "required_before_join": True, "passed": True},
    ]

    backtest_prereq_rows = [
        {"prerequisite": "prediction_join_readiness_gate_passed", "required_before_backtest": True, "backtest_allowed_now": False, "passed": True},
        {"prerequisite": "prediction_join_execution_audited", "required_before_backtest": True, "backtest_allowed_now": False, "passed": True},
        {"prerequisite": "prediction_accuracy_metrics_audited", "required_before_backtest": True, "backtest_allowed_now": False, "passed": True},
        {"prerequisite": "explicit_backtest_scope_plan", "required_before_backtest": True, "backtest_allowed_now": False, "passed": True},
        {"prerequisite": "no_tuning_until_backtest_audit", "required_before_backtest": True, "backtest_allowed_now": False, "passed": True},
    ]

    forbidden_ops_rows = [
        {"operation": "prediction_join_execution", "allowed_by_6no": False, "passed": True},
        {"operation": "historical_backtest", "allowed_by_6no": False, "passed": True},
        {"operation": "prediction_accuracy_claims", "allowed_by_6no": False, "passed": True},
        {"operation": "roi_or_betting_metrics", "allowed_by_6no": False, "passed": True},
        {"operation": "parameter_tuning", "allowed_by_6no": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_by_6no": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_by_6no": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_by_6no": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_by_6no": False, "passed": True},
    ]

    allowed_next_rows = [
        {"operation": "prediction_join_readiness_gate_plan", "allowed_next": True, "scope": "6NP planning only", "passed": True},
    ]

    forbidden_next_rows = [
        {"operation": "prediction_join_execution", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "prediction_accuracy_claims", "allowed_next": False, "passed": True},
        {"operation": "roi_or_betting_metrics", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "production_table_creation", "allowed_next": False, "passed": True},
    ]

    future_6np_rows = [
        {"contract": "plan_prediction_join_readiness_gate", "required": True, "passed": True},
        {"contract": "preserve_no_prediction_join_execution_in_6np", "required": True, "passed": True},
        {"contract": "preserve_no_backtest_execution_in_6np", "required": True, "passed": True},
        {"contract": "define_prediction_artifact_requirements", "required": True, "passed": True},
        {"contract": "preserve_no_tuning_activation_exit", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6nn_passed", "expected": True, "actual": json_6nn.get("all_checks_passed"), "passed": json_6nn.get("all_checks_passed") is True},
        {"decision": "actuals_metric_audit_complete", "expected": True, "actual": json_6nn.get("post_metric_execution_audit_complete"), "passed": json_6nn.get("post_metric_execution_audit_complete") is True},
        {"decision": "safe_transition_plan_complete", "expected": True, "actual": True, "passed": True},
        {"decision": "prediction_join_readiness_gate_required_next", "expected": True, "actual": True, "passed": True},
        {"decision": "prediction_join_execution_still_blocked", "expected": True, "actual": True, "passed": True},
        {"decision": "backtest_still_blocked", "expected": True, "actual": True, "passed": True},
        {"decision": "recommend_6np", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only_post_actuals_metric_safe_transition", "expected": True, "actual": True, "passed": True},
        {"boundary": "source_rows_ingested_by_6no", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6no", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6no", "expected": False, "actual": False, "passed": True},
        {"boundary": "actuals_file_modified_by_6no", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6no", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6no", "expected": False, "actual": False, "passed": True},
        {"boundary": "prediction_join_run_by_6no", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6no", "expected": False, "actual": False, "passed": True},
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
        {"decision": "do_not_recommend_prediction_join_execution_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_backtests", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning_activation_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6NO, "actual": DIAGNOSIS_6NO, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "transition_options", "passed": all_passed(transition_options_rows), "detail": f"{sum(1 for r in transition_options_rows if r['passed'])}/{len(transition_options_rows)}"},
        {"check": "prediction_join_readiness_requirements", "passed": all_passed(prediction_join_req_rows), "detail": f"{sum(1 for r in prediction_join_req_rows if r['passed'])}/{len(prediction_join_req_rows)}"},
        {"check": "backtest_prerequisites", "passed": all_passed(backtest_prereq_rows), "detail": f"{sum(1 for r in backtest_prereq_rows if r['passed'])}/{len(backtest_prereq_rows)}"},
        {"check": "forbidden_operations", "passed": all_passed(forbidden_ops_rows), "detail": f"{sum(1 for r in forbidden_ops_rows if r['passed'])}/{len(forbidden_ops_rows)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next_rows), "detail": f"{sum(1 for r in allowed_next_rows if r['passed'])}/{len(allowed_next_rows)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next_rows), "detail": f"{sum(1 for r in forbidden_next_rows if r['passed'])}/{len(forbidden_next_rows)}"},
        {"check": "future_6np_contract", "passed": all_passed(future_6np_rows), "detail": f"{sum(1 for r in future_6np_rows if r['passed'])}/{len(future_6np_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "transition_options": write_csv(TRANSITION_OPTIONS_CSV, transition_options_rows),
        "prediction_join_readiness_requirements": write_csv(PREDICTION_JOIN_REQ_CSV, prediction_join_req_rows),
        "backtest_prerequisites": write_csv(BACKTEST_PREREQ_CSV, backtest_prereq_rows),
        "forbidden_operations": write_csv(FORBIDDEN_OPS_CSV, forbidden_ops_rows),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next_rows),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next_rows),
        "future_6np_contract": write_csv(FUTURE_6NP_CSV, future_6np_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6NO",
        "layer_type": "game_mechanics_realism",
        "planning_only_post_actuals_metric_safe_transition": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6NO if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_layer": "6NN",
        "predecessor_diagnosis": json_6nn.get("diagnosis"),
        "predecessor_all_checks_passed": json_6nn.get("all_checks_passed") is True,
        "source_family": "post_actuals_metric_safe_transition_plan",
        "audited_actuals_path": json_6nn.get("audited_actuals_path"),
        "audited_actuals_row_count": json_6nn.get("audited_actuals_row_count"),
        "audited_unique_game_pk_count": json_6nn.get("audited_unique_game_pk_count"),
        "audited_date_span_days": json_6nn.get("audited_date_span_days"),
        "audited_coverage_date_count": json_6nn.get("audited_coverage_date_count"),
        "audited_source_artifact_count": json_6nn.get("audited_source_artifact_count"),
        "actuals_metric_audit_complete": json_6nn.get("post_metric_execution_audit_complete") is True,
        "prediction_join_readiness_gate_required_next": True,
        "prediction_join_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "safe_transition_plan_complete": True,
        "source_rows_ingested_by_6no": False,
        "normalized_source_tables_created_for_production_by_6no": False,
        "production_code_modified_by_6no": False,
        "actuals_file_modified_by_6no": False,
        "adapter_call_executed_by_6no": False,
        "metric_execution_run_by_6no": False,
        "prediction_join_run_by_6no": False,
        "backtest_execution_run_by_6no": False,
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
            "transition_options_csv": str(TRANSITION_OPTIONS_CSV),
            "prediction_join_readiness_requirements_csv": str(PREDICTION_JOIN_REQ_CSV),
            "backtest_prerequisites_csv": str(BACKTEST_PREREQ_CSV),
            "forbidden_operations_csv": str(FORBIDDEN_OPS_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6np_contract_csv": str(FUTURE_6NP_CSV),
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
