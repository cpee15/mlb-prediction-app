#!/usr/bin/env python3
"""Plan prediction-join readiness gate before any join execution."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6np_prediction_join_readiness_gate_plan"
TMP_DIR = Path("tmp")

SCRIPT_6NO = Path("scripts/plan_6no_layer6_post_actuals_metric_safe_transition.py")
JSON_6NO = TMP_DIR / "layer6_6no_post_actuals_metric_safe_transition_plan.json"
TARGET_ACTUALS = Path("data/local/historical_actuals.csv")

REQUIRED_INPUTS = [
    JSON_6NO,
    TMP_DIR / "layer6_6no_post_actuals_metric_safe_transition_plan_checks.csv",
    TMP_DIR / "layer6_6no_post_actuals_metric_safe_transition_plan_predecessor.csv",
    TMP_DIR / "layer6_6no_post_actuals_metric_safe_transition_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6no_post_actuals_metric_safe_transition_plan_transition_options.csv",
    TMP_DIR / "layer6_6no_post_actuals_metric_safe_transition_plan_prediction_join_readiness_requirements.csv",
    TMP_DIR / "layer6_6no_post_actuals_metric_safe_transition_plan_backtest_prerequisites.csv",
    TMP_DIR / "layer6_6no_post_actuals_metric_safe_transition_plan_forbidden_operations.csv",
    TMP_DIR / "layer6_6no_post_actuals_metric_safe_transition_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6no_post_actuals_metric_safe_transition_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6no_post_actuals_metric_safe_transition_plan_future_6np_contract.csv",
    TMP_DIR / "layer6_6no_post_actuals_metric_safe_transition_plan_decision.csv",
    TMP_DIR / "layer6_6no_post_actuals_metric_safe_transition_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6no_post_actuals_metric_safe_transition_plan_recommended_path.csv",
    SCRIPT_6NO,
    TARGET_ACTUALS,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PRED_ARTIFACT_REQ_CSV = TMP_DIR / f"{SLUG}_prediction_artifact_requirements.csv"
SCHEMA_REQ_CSV = TMP_DIR / f"{SLUG}_schema_requirements.csv"
PROVENANCE_REQ_CSV = TMP_DIR / f"{SLUG}_provenance_requirements.csv"
JOIN_KEY_REQ_CSV = TMP_DIR / f"{SLUG}_join_key_requirements.csv"
UNMATCHED_POLICY_CSV = TMP_DIR / f"{SLUG}_unmatched_row_policy.csv"
ALLOWED_POST_JOIN_METRICS_CSV = TMP_DIR / f"{SLUG}_allowed_post_join_metrics.csv"
FORBIDDEN_POST_JOIN_METRICS_CSV = TMP_DIR / f"{SLUG}_forbidden_post_join_metrics.csv"
POST_JOIN_AUDIT_REQ_CSV = TMP_DIR / f"{SLUG}_post_join_audit_requirements.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6NQ_CSV = TMP_DIR / f"{SLUG}_future_6nq_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6NO = "layer_6_post_actuals_metric_safe_transition_plan_complete"
DIAGNOSIS_6NP = "layer_6_prediction_join_readiness_gate_plan_complete"
RECOMMENDED_NEXT_LAYER = "6NQ_layer_6_prediction_join_readiness_gate_check"
RECOMMENDED_PATH = "check_prediction_join_readiness_before_join_execution"


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
    json_6no = load_json(JSON_6NO)
    actuals_row_count = json_6no.get("audited_actuals_row_count") or 0

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
        {"check": "6no_script_exists", "expected": True, "actual": SCRIPT_6NO.exists(), "passed": SCRIPT_6NO.exists()},
        {"check": "6no_json_exists", "expected": True, "actual": JSON_6NO.exists(), "passed": JSON_6NO.exists()},
        {"check": "6no_all_checks_passed", "expected": True, "actual": json_6no.get("all_checks_passed"), "passed": json_6no.get("all_checks_passed") is True},
        {"check": "6no_diagnosis", "expected": DIAGNOSIS_6NO, "actual": json_6no.get("diagnosis"), "passed": json_6no.get("diagnosis") == DIAGNOSIS_6NO},
        {"check": "prediction_join_readiness_required", "expected": True, "actual": json_6no.get("prediction_join_readiness_gate_required_next"), "passed": json_6no.get("prediction_join_readiness_gate_required_next") is True},
    ]

    pred_artifact_req_rows = [
        {"requirement": "artifact_must_be_local", "required": True, "passed": True},
        {"requirement": "artifact_path_must_be_explicit", "required": True, "passed": True},
        {"requirement": "artifact_must_not_require_live_fetch", "required": True, "passed": True},
        {"requirement": "artifact_must_include_game_level_rows", "required": True, "passed": True},
        {"requirement": "artifact_must_have_stable_identifier", "required": True, "passed": True},
    ]

    schema_req_rows = [
        {"column": "game_pk", "required": True, "purpose": "join key to actuals", "passed": True},
        {"column": "game_date", "required": True, "purpose": "date validation", "passed": True},
        {"column": "home_team", "required": True, "purpose": "human audit", "passed": True},
        {"column": "away_team", "required": True, "purpose": "human audit", "passed": True},
        {"column": "predicted_home_win_probability", "required": True, "purpose": "prediction metric input", "passed": True},
        {"column": "prediction_generated_at_or_source", "required": True, "purpose": "provenance", "passed": True},
    ]

    provenance_req_rows = [
        {"requirement": "source_artifact_recorded", "required": True, "passed": True},
        {"requirement": "generation_time_or_snapshot_recorded", "required": True, "passed": True},
        {"requirement": "model_or_rule_version_recorded", "required": True, "passed": True},
        {"requirement": "no_live_fetch_needed_for_validation", "required": True, "passed": True},
        {"requirement": "reproducible_local_artifact", "required": True, "passed": True},
    ]

    join_key_req_rows = [
        {"requirement": "join_on_game_pk", "required": True, "passed": True},
        {"requirement": "actuals_game_pk_unique", "required": True, "actual": json_6no.get("audited_actuals_row_count") == json_6no.get("audited_unique_game_pk_count"), "passed": json_6no.get("audited_actuals_row_count") == json_6no.get("audited_unique_game_pk_count")},
        {"requirement": "prediction_game_pk_unique_required", "required": True, "passed": True},
        {"requirement": "date_crosscheck_required", "required": True, "passed": True},
        {"requirement": "team_name_crosscheck_required", "required": True, "passed": True},
    ]

    unmatched_policy_rows = [
        {"policy": "count_unmatched_actuals", "required": True, "passed": True},
        {"policy": "count_unmatched_predictions", "required": True, "passed": True},
        {"policy": "report_join_coverage_rate", "required": True, "passed": True},
        {"policy": "block_accuracy_if_join_coverage_below_threshold", "required": True, "passed": True},
        {"policy": "do_not_impute_missing_predictions", "required": True, "passed": True},
    ]

    allowed_post_join_metrics_rows = [
        {"metric": "join_coverage_rate", "allowed_after_join_audit": True, "requires_backtest": False, "passed": True},
        {"metric": "matched_row_count", "allowed_after_join_audit": True, "requires_backtest": False, "passed": True},
        {"metric": "unmatched_actuals_count", "allowed_after_join_audit": True, "requires_backtest": False, "passed": True},
        {"metric": "unmatched_predictions_count", "allowed_after_join_audit": True, "requires_backtest": False, "passed": True},
        {"metric": "basic_home_win_accuracy", "allowed_after_join_audit": True, "requires_backtest": False, "passed": True},
        {"metric": "probability_bucket_calibration_preview", "allowed_after_join_audit": True, "requires_backtest": False, "passed": True},
    ]

    forbidden_post_join_metrics_rows = [
        {"metric": "roi_or_betting_results", "allowed": False, "passed": True},
        {"metric": "historical_backtest", "allowed": False, "passed": True},
        {"metric": "parameter_tuning", "allowed": False, "passed": True},
        {"metric": "production_simulation", "allowed": False, "passed": True},
        {"metric": "mechanics_activation", "allowed": False, "passed": True},
        {"metric": "layer_6_exit", "allowed": False, "passed": True},
    ]

    post_join_audit_req_rows = [
        {"requirement": "audit_prediction_artifact_schema", "required_after_join": True, "passed": True},
        {"requirement": "audit_prediction_artifact_provenance", "required_after_join": True, "passed": True},
        {"requirement": "audit_join_key_uniqueness", "required_after_join": True, "passed": True},
        {"requirement": "audit_unmatched_rows", "required_after_join": True, "passed": True},
        {"requirement": "audit_no_backtest_or_tuning", "required_after_join": True, "passed": True},
    ]

    allowed_next_rows = [
        {"operation": "prediction_join_readiness_gate_check", "allowed_next": True, "scope": "6NQ check only", "passed": True},
    ]

    forbidden_next_rows = [
        {"operation": "prediction_join_execution", "allowed_next": False, "passed": True},
        {"operation": "prediction_accuracy_calculation", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "roi_or_betting_metrics", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "production_table_creation", "allowed_next": False, "passed": True},
    ]

    future_6nq_rows = [
        {"contract": "check_prediction_join_readiness", "required": True, "passed": True},
        {"contract": "preserve_no_prediction_join_execution_in_6nq", "required": True, "passed": True},
        {"contract": "emit_boolean_prediction_join_execution_allowed_after_6nq", "required": True, "passed": True},
        {"contract": "preserve_no_backtest_tuning_activation_exit", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6no_passed", "expected": True, "actual": json_6no.get("all_checks_passed"), "passed": json_6no.get("all_checks_passed") is True},
        {"decision": "prediction_join_readiness_gate_planned", "expected": True, "actual": True, "passed": True},
        {"decision": "prediction_join_execution_still_blocked", "expected": True, "actual": True, "passed": True},
        {"decision": "backtest_still_blocked", "expected": True, "actual": True, "passed": True},
        {"decision": "recommend_6nq", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only_prediction_join_readiness_gate", "expected": True, "actual": True, "passed": True},
        {"boundary": "source_rows_ingested_by_6np", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6np", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6np", "expected": False, "actual": False, "passed": True},
        {"boundary": "actuals_file_modified_by_6np", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6np", "expected": False, "actual": False, "passed": True},
        {"boundary": "prediction_join_run_by_6np", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6np", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6np", "expected": False, "actual": False, "passed": True},
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
        {"decision": "do_not_recommend_prediction_accuracy_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_backtests", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning_activation_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6NP, "actual": DIAGNOSIS_6NP, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "prediction_artifact_requirements", "passed": all_passed(pred_artifact_req_rows), "detail": f"{sum(1 for r in pred_artifact_req_rows if r['passed'])}/{len(pred_artifact_req_rows)}"},
        {"check": "schema_requirements", "passed": all_passed(schema_req_rows), "detail": f"{sum(1 for r in schema_req_rows if r['passed'])}/{len(schema_req_rows)}"},
        {"check": "provenance_requirements", "passed": all_passed(provenance_req_rows), "detail": f"{sum(1 for r in provenance_req_rows if r['passed'])}/{len(provenance_req_rows)}"},
        {"check": "join_key_requirements", "passed": all_passed(join_key_req_rows), "detail": f"{sum(1 for r in join_key_req_rows if r['passed'])}/{len(join_key_req_rows)}"},
        {"check": "unmatched_row_policy", "passed": all_passed(unmatched_policy_rows), "detail": f"{sum(1 for r in unmatched_policy_rows if r['passed'])}/{len(unmatched_policy_rows)}"},
        {"check": "allowed_post_join_metrics", "passed": all_passed(allowed_post_join_metrics_rows), "detail": f"{sum(1 for r in allowed_post_join_metrics_rows if r['passed'])}/{len(allowed_post_join_metrics_rows)}"},
        {"check": "forbidden_post_join_metrics", "passed": all_passed(forbidden_post_join_metrics_rows), "detail": f"{sum(1 for r in forbidden_post_join_metrics_rows if r['passed'])}/{len(forbidden_post_join_metrics_rows)}"},
        {"check": "post_join_audit_requirements", "passed": all_passed(post_join_audit_req_rows), "detail": f"{sum(1 for r in post_join_audit_req_rows if r['passed'])}/{len(post_join_audit_req_rows)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next_rows), "detail": f"{sum(1 for r in allowed_next_rows if r['passed'])}/{len(allowed_next_rows)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next_rows), "detail": f"{sum(1 for r in forbidden_next_rows if r['passed'])}/{len(forbidden_next_rows)}"},
        {"check": "future_6nq_contract", "passed": all_passed(future_6nq_rows), "detail": f"{sum(1 for r in future_6nq_rows if r['passed'])}/{len(future_6nq_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "prediction_artifact_requirements": write_csv(PRED_ARTIFACT_REQ_CSV, pred_artifact_req_rows),
        "schema_requirements": write_csv(SCHEMA_REQ_CSV, schema_req_rows),
        "provenance_requirements": write_csv(PROVENANCE_REQ_CSV, provenance_req_rows),
        "join_key_requirements": write_csv(JOIN_KEY_REQ_CSV, join_key_req_rows),
        "unmatched_row_policy": write_csv(UNMATCHED_POLICY_CSV, unmatched_policy_rows),
        "allowed_post_join_metrics": write_csv(ALLOWED_POST_JOIN_METRICS_CSV, allowed_post_join_metrics_rows),
        "forbidden_post_join_metrics": write_csv(FORBIDDEN_POST_JOIN_METRICS_CSV, forbidden_post_join_metrics_rows),
        "post_join_audit_requirements": write_csv(POST_JOIN_AUDIT_REQ_CSV, post_join_audit_req_rows),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next_rows),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next_rows),
        "future_6nq_contract": write_csv(FUTURE_6NQ_CSV, future_6nq_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6NP",
        "layer_type": "game_mechanics_realism",
        "planning_only_prediction_join_readiness_gate": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6NP if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_layer": "6NO",
        "predecessor_diagnosis": json_6no.get("diagnosis"),
        "predecessor_all_checks_passed": json_6no.get("all_checks_passed") is True,
        "source_family": "prediction_join_readiness_gate_plan",
        "actuals_path": str(TARGET_ACTUALS),
        "actuals_row_count": actuals_row_count,
        "prediction_artifact_identification_required": True,
        "prediction_schema_validation_required": True,
        "prediction_provenance_validation_required": True,
        "join_key_validation_required": True,
        "unmatched_row_policy_required": True,
        "post_join_audit_required": True,
        "prediction_join_readiness_check_allowed_next": True,
        "prediction_join_execution_allowed_next": False,
        "prediction_accuracy_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "source_rows_ingested_by_6np": False,
        "normalized_source_tables_created_for_production_by_6np": False,
        "production_code_modified_by_6np": False,
        "actuals_file_modified_by_6np": False,
        "adapter_call_executed_by_6np": False,
        "prediction_join_run_by_6np": False,
        "metric_execution_run_by_6np": False,
        "backtest_execution_run_by_6np": False,
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
            "prediction_artifact_requirements_csv": str(PRED_ARTIFACT_REQ_CSV),
            "schema_requirements_csv": str(SCHEMA_REQ_CSV),
            "provenance_requirements_csv": str(PROVENANCE_REQ_CSV),
            "join_key_requirements_csv": str(JOIN_KEY_REQ_CSV),
            "unmatched_row_policy_csv": str(UNMATCHED_POLICY_CSV),
            "allowed_post_join_metrics_csv": str(ALLOWED_POST_JOIN_METRICS_CSV),
            "forbidden_post_join_metrics_csv": str(FORBIDDEN_POST_JOIN_METRICS_CSV),
            "post_join_audit_requirements_csv": str(POST_JOIN_AUDIT_REQ_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6nq_contract_csv": str(FUTURE_6NQ_CSV),
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
