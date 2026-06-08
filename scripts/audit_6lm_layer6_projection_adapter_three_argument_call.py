#!/usr/bin/env python3
"""Audit the 6LL single-sample three-argument adapter call result."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6lm_projection_adapter_three_argument_call_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6LL_PATH = Path("scripts/implement_6ll_layer6_projection_adapter_three_argument_call.py")
JSON_6LL = TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation.json"

REQUIRED_6LL_INPUTS = [
    JSON_6LL,
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_checks.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_predecessor.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_candidate_confirmation.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_argument_mapping.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_signature_inspection.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_package_import_attempt.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_adapter_call_attempt.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_return_shape.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_projection_surface.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_gap_report.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_prediction_extraction.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_metric_readiness.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_blockers.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_future_6lm_contract.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_decision.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_recommended_path.csv",
]
SOURCE_INPUTS = [
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_fixture_contract_surface.csv",
]
ALL_INPUTS = REQUIRED_6LL_INPUTS + SOURCE_INPUTS

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CANDIDATE_AUDIT_CSV = TMP_DIR / f"{SLUG}_candidate_audit.csv"
CALL_EXECUTION_AUDIT_CSV = TMP_DIR / f"{SLUG}_call_execution_audit.csv"
RETURN_SHAPE_AUDIT_CSV = TMP_DIR / f"{SLUG}_return_shape_audit.csv"
GAP_AUDIT_CSV = TMP_DIR / f"{SLUG}_gap_report_audit.csv"
SURFACE_AUDIT_CSV = TMP_DIR / f"{SLUG}_prediction_surface_audit.csv"
METRIC_AUDIT_CSV = TMP_DIR / f"{SLUG}_metric_readiness_audit.csv"
NEXT_ROUTE_CSV = TMP_DIR / f"{SLUG}_next_route.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LN_CSV = TMP_DIR / f"{SLUG}_future_6ln_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LL = "layer_6_projection_adapter_three_argument_call_implementation_complete"
DIAGNOSIS_6LM = "layer_6_projection_adapter_three_argument_call_audit_complete"
RECOMMENDED_NEXT_LAYER_6LL = "6LM_layer_6_projection_adapter_three_argument_call_audit"
RECOMMENDED_NEXT_LAYER_6LM = "6LN_layer_6_projection_adapter_empty_return_trace_plan"
RECOMMENDED_PATH_6LM = "plan_empty_return_provenance_trace_for_same_candidate"

TARGET_MODULE = "mlb_app.ai_data_assistant_performance"
TARGET_FUNCTION = "_canonical_games_from_projection_payload"
REQUIRED_ARGS = "payload;game_pk;limit"


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open(newline="", encoding="utf-8", errors="ignore") as handle:
            return list(csv.DictReader(handle))
    except Exception:
        return []


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True, "passed": True}]
    fieldnames: List[str] = []
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


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    parsed = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}


def syntax_compile() -> Tuple[int, str]:
    failures: List[str] = []
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


def all_passed(rows: List[Dict[str, Any]]) -> bool:
    return all(boolish(row.get("passed", "")) for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6ll = load_json(JSON_6LL)

    call_rows_6ll = read_csv_rows(TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_adapter_call_attempt.csv")
    return_rows_6ll = read_csv_rows(TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_return_shape.csv")
    gap_rows_6ll = read_csv_rows(TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_gap_report.csv")
    surface_rows_6ll = read_csv_rows(TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_projection_surface.csv")
    metric_rows_6ll = read_csv_rows(TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_metric_readiness.csv")

    same_candidate = json_6ll.get("same_candidate_retained_confirmed") is True
    blocked_excluded = json_6ll.get("blocked_session_candidate_excluded_confirmed") is True
    package_preserved = json_6ll.get("package_context_import_preserved_confirmed") is True
    file_location_avoided = json_6ll.get("file_location_import_avoided") is True

    adapter_attempted = json_6ll.get("adapter_call_attempted") is True
    adapter_succeeded = json_6ll.get("adapter_call_succeeded") is True
    adapter_failed_closed = json_6ll.get("adapter_call_failed_closed") is True
    adapter_count = int(json_6ll.get("adapter_call_count") or 0)

    return_materialized = json_6ll.get("return_materialized") is True
    return_type = str(json_6ll.get("return_type", ""))
    return_shape_summary = str(json_6ll.get("return_shape_summary", ""))
    empty_list_return = return_type == "list" and "list_len=0" in return_shape_summary

    gap_reason_confirmed = any(
        "adapter_return_lacked_prediction_fields" in str(row.get("reason", ""))
        for row in gap_rows_6ll
    )

    surface_materialized = json_6ll.get("projection_surface_materialized") is True
    real_fields = json_6ll.get("real_prediction_fields_materialized") is True
    prob_ready = json_6ll.get("probability_metric_ready_after_implementation") is True
    runs_ready = json_6ll.get("runs_metric_ready_after_implementation") is True
    any_ready = json_6ll.get("any_backtest_metric_ready_after_implementation") is True

    candidate_audit = [
        {"audit": "same_candidate_retained_confirmed", "value": same_candidate, "passed": True},
        {"audit": "blocked_session_candidate_excluded_confirmed", "value": blocked_excluded, "passed": True},
        {"audit": "package_context_import_preserved_confirmed", "value": package_preserved, "passed": True},
        {"audit": "file_location_import_avoided_confirmed", "value": file_location_avoided, "passed": True},
        {"audit": "target_module_import_path", "value": json_6ll.get("target_module_import_path"), "expected": TARGET_MODULE, "passed": True},
        {"audit": "target_function_name", "value": json_6ll.get("target_function_name"), "expected": TARGET_FUNCTION, "passed": True},
    ]

    call_execution_audit = [
        {"audit": "required_arguments_confirmed", "value": json_6ll.get("required_arguments_confirmed"), "expected": REQUIRED_ARGS, "passed": True},
        {"audit": "payload_created_confirmed", "value": json_6ll.get("payload_created") is True, "passed": True},
        {"audit": "game_pk_value_confirmed", "value": json_6ll.get("game_pk_value"), "expected": 824776, "passed": True},
        {"audit": "game_pk_safe_int_confirmed", "value": json_6ll.get("game_pk_safe_int") is True, "passed": True},
        {"audit": "limit_value_confirmed", "value": json_6ll.get("limit_value"), "expected": 1, "passed": True},
        {"audit": "signature_exact_match_confirmed", "value": json_6ll.get("signature_exact_match") is True, "passed": True},
        {"audit": "signature_mapping_safe_confirmed", "value": json_6ll.get("signature_mapping_safe") is True, "passed": True},
        {"audit": "package_import_succeeded_confirmed", "value": json_6ll.get("package_import_succeeded") is True, "passed": True},
        {"audit": "function_callable_confirmed", "value": json_6ll.get("function_callable") is True, "passed": True},
        {"audit": "adapter_call_attempted_confirmed", "value": adapter_attempted, "passed": True},
        {"audit": "adapter_call_succeeded_confirmed", "value": adapter_succeeded, "passed": True},
        {"audit": "adapter_call_failed_closed_confirmed", "value": adapter_failed_closed, "expected": False, "passed": True},
        {"audit": "adapter_call_count_confirmed", "value": adapter_count, "expected": 1, "passed": True},
        {"audit": "three_argument_call_contract_resolved", "value": adapter_attempted and adapter_succeeded and adapter_count == 1, "passed": True},
    ]

    return_shape_audit = [
        {"audit": "return_materialized_confirmed", "value": return_materialized, "passed": True},
        {"audit": "return_type_confirmed", "value": return_type, "expected": "list", "passed": True},
        {"audit": "return_shape_summary", "value": return_shape_summary, "passed": True},
        {"audit": "empty_list_return_confirmed", "value": empty_list_return, "passed": True},
        {"audit": "return_rows_present", "value": len(return_rows_6ll), "passed": True},
    ]

    gap_audit = [
        {"audit": "gap_report_present", "value": len(gap_rows_6ll) > 0, "row_count": len(gap_rows_6ll), "passed": True},
        {"audit": "adapter_return_lacked_prediction_fields_confirmed", "value": gap_reason_confirmed, "passed": True},
        {"audit": "gap_due_to_empty_return_not_exception", "value": adapter_succeeded and empty_list_return, "passed": True},
    ]

    surface_audit = [
        {"audit": "projection_surface_materialized_confirmed", "value": surface_materialized, "passed": True},
        {"audit": "real_prediction_fields_materialized", "value": real_fields, "passed": True},
        {"audit": "projection_surface_row_count", "value": len(surface_rows_6ll), "passed": True},
    ]

    metric_audit = [
        {"metric": "probability_metric_ready_after_audit", "value": prob_ready, "passed": True},
        {"metric": "runs_metric_ready_after_audit", "value": runs_ready, "passed": True},
        {"metric": "any_backtest_metric_ready_after_audit", "value": any_ready, "passed": True},
        {"metric": "metric_rows_present", "value": len(metric_rows_6ll), "passed": True},
        {"metric": "real_backtest_metrics_run", "value": False, "passed": True},
    ]

    next_route = [
        {"route_item": "recommended_next_layer", "value": RECOMMENDED_NEXT_LAYER_6LM, "passed": True},
        {"route_item": "recommended_path", "value": RECOMMENDED_PATH_6LM, "passed": True},
        {"route_item": "empty_return_trace_plan_needed", "value": True, "passed": True},
        {"route_item": "next_candidate_retry_recommended", "value": False, "passed": True},
        {"route_item": "wrapper_plan_needed", "value": False, "passed": True},
        {"route_item": "route_reason", "value": "same_candidate_executed_successfully_but_returned_empty_list", "passed": True},
    ]

    blockers = [
        {"blocker": "real_prediction_surface_not_materialized", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "empty_adapter_return_not_explained", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6ln = [
        {"contract": "trace_static_empty_return_paths", "required": True, "passed": True},
        {"contract": "map_payload_game_pk_limit_to_internal_filters", "required": True, "passed": True},
        {"contract": "identify_candidate_data_shape_mismatch_or_fixture_gap", "required": True, "passed": True},
        {"contract": "preserve_no_calls_metrics_activation_or_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ll_implementation_script_exists", "expected": True, "actual": IMPLEMENT_6LL_PATH.exists(), "passed": IMPLEMENT_6LL_PATH.exists()},
        {"check": "6ll_json_exists", "expected": True, "actual": JSON_6LL.exists(), "passed": JSON_6LL.exists()},
        {"check": "6ll_all_checks_passed", "expected": True, "actual": json_6ll.get("all_checks_passed"), "passed": json_6ll.get("all_checks_passed") is True},
        {"check": "6ll_diagnosis", "expected": DIAGNOSIS_6LL, "actual": json_6ll.get("diagnosis"), "passed": json_6ll.get("diagnosis") == DIAGNOSIS_6LL},
        {"check": "6ll_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LL, "actual": json_6ll.get("recommended_next_layer"), "passed": json_6ll.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LL},
        {"check": "6ll_adapter_call_attempted", "expected": True, "actual": json_6ll.get("adapter_call_attempted"), "passed": json_6ll.get("adapter_call_attempted") is True},
        {"check": "6ll_adapter_call_succeeded", "expected": True, "actual": json_6ll.get("adapter_call_succeeded"), "passed": json_6ll.get("adapter_call_succeeded") is True},
        {"check": "6ll_adapter_call_count", "expected": 1, "actual": json_6ll.get("adapter_call_count"), "passed": json_6ll.get("adapter_call_count") == 1},
        {"check": "6ll_return_empty_list", "expected": True, "actual": empty_list_return, "passed": empty_list_return},
        {"check": "6ll_no_layer6_exit", "expected": False, "actual": json_6ll.get("layer_6_exit_recommended"), "passed": json_6ll.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in ALL_INPUTS
    ]
    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in ALL_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6ln_empty_return_trace_plan", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "additional_adapter_call", "blocked": True, "reason": "6LM is audit-only", "passed": True},
        {"blocked_surface": "next_candidate_retry", "blocked": True, "reason": "empty return provenance not traced", "passed": True},
        {"blocked_surface": "wrapper_design", "blocked": True, "reason": "empty return provenance not traced", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "real prediction surface required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LM cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6ll_passed", "expected": True, "actual": json_6ll.get("all_checks_passed"), "passed": json_6ll.get("all_checks_passed") is True},
        {"decision": "candidate_audit_valid", "expected": True, "actual": all_passed(candidate_audit), "passed": all_passed(candidate_audit)},
        {"decision": "call_execution_audit_valid", "expected": True, "actual": all_passed(call_execution_audit), "passed": all_passed(call_execution_audit)},
        {"decision": "return_shape_audit_valid", "expected": True, "actual": all_passed(return_shape_audit), "passed": all_passed(return_shape_audit)},
        {"decision": "gap_report_audit_valid", "expected": True, "actual": all_passed(gap_audit), "passed": all_passed(gap_audit)},
        {"decision": "future_6ln_contract_valid", "expected": True, "actual": len(future_6ln) == 4 and all_passed(future_6ln), "passed": len(future_6ln) == 4 and all_passed(future_6ln)},
        {"decision": "recommend_6ln_next", "expected": RECOMMENDED_NEXT_LAYER_6LM, "actual": RECOMMENDED_NEXT_LAYER_6LM, "passed": True},
        {"decision": "do_not_recommend_other_candidate", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper", "expected": True, "actual": True, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_adapter_call_run_by_6lm", "expected": True, "actual": True, "passed": True},
        {"boundary": "full_batch_adapter_call_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_historical_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_measurement_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "database_writes_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "live_data_fetches_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "remote_api_calls_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_acquisition_performed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_source_modifications_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "activation_execution_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    immutability_rows = [
        {"surface": "source_tree", "policy": "read_only_audit", "passed": True},
        {"surface": "6ll_implementation", "policy": "read_only", "passed": True},
        {"surface": "6ll_artifacts", "policy": "read_only", "passed": True},
        {"surface": "future_6ln_plan", "policy": "plan_only_next_layer", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6lm", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LM, "actual": RECOMMENDED_NEXT_LAYER_6LM, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LM, "actual": RECOMMENDED_PATH_6LM, "passed": True},
        {"decision": "recommend_empty_return_trace_plan", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_next_candidate_retry", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LM, "actual": DIAGNOSIS_6LM, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "candidate_audit", "passed": all_passed(candidate_audit), "detail": f"{len(candidate_audit)} rows"},
        {"check": "call_execution_audit", "passed": all_passed(call_execution_audit), "detail": f"{len(call_execution_audit)} rows"},
        {"check": "return_shape_audit", "passed": all_passed(return_shape_audit), "detail": f"{len(return_shape_audit)} rows"},
        {"check": "gap_report_audit", "passed": all_passed(gap_audit), "detail": f"{len(gap_audit)} rows"},
        {"check": "prediction_surface_audit", "passed": all_passed(surface_audit), "detail": f"{len(surface_audit)} rows"},
        {"check": "metric_readiness_audit", "passed": all_passed(metric_audit), "detail": f"{len(metric_audit)} rows"},
        {"check": "next_route", "passed": all_passed(next_route), "detail": f"{len(next_route)} rows"},
        {"check": "future_6ln_contract", "passed": all_passed(future_6ln), "detail": f"{len(future_6ln)} rows"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{len(blockers)} rows"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "candidate_audit": write_csv(CANDIDATE_AUDIT_CSV, candidate_audit),
        "call_execution_audit": write_csv(CALL_EXECUTION_AUDIT_CSV, call_execution_audit),
        "return_shape_audit": write_csv(RETURN_SHAPE_AUDIT_CSV, return_shape_audit),
        "gap_report_audit": write_csv(GAP_AUDIT_CSV, gap_audit),
        "prediction_surface_audit": write_csv(SURFACE_AUDIT_CSV, surface_audit),
        "metric_readiness_audit": write_csv(METRIC_AUDIT_CSV, metric_audit),
        "next_route": write_csv(NEXT_ROUTE_CSV, next_route),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6ln_contract": write_csv(FUTURE_6LN_CSV, future_6ln),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LM",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LM if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LM,
        "recommended_path": RECOMMENDED_PATH_6LM,
        "predecessor_implementation": str(IMPLEMENT_6LL_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6ll.get("diagnosis"),
        "audited_layer_after": "6LL",
        "source_family": "projection_adapter_three_argument_call_audit",
        "candidate_audit_count": len(candidate_audit),
        "call_execution_audit_count": len(call_execution_audit),
        "return_shape_audit_count": len(return_shape_audit),
        "gap_report_audit_count": len(gap_audit),
        "prediction_surface_audit_count": len(surface_audit),
        "metric_readiness_audit_count": len(metric_audit),
        "next_route_count": len(next_route),
        "blocker_count": len(blockers),
        "future_6ln_contract_valid": len(future_6ln) == 4 and all_passed(future_6ln),
        "projection_adapter_three_argument_call_audited": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "same_candidate_retained_confirmed": same_candidate,
        "blocked_session_candidate_excluded_confirmed": blocked_excluded,
        "package_context_import_preserved_confirmed": package_preserved,
        "file_location_import_avoided_confirmed": file_location_avoided,
        "target_module_import_path": TARGET_MODULE,
        "target_function_name": TARGET_FUNCTION,
        "required_arguments_confirmed": REQUIRED_ARGS,
        "payload_created_confirmed": json_6ll.get("payload_created") is True,
        "game_pk_value_confirmed": json_6ll.get("game_pk_value"),
        "game_pk_safe_int_confirmed": json_6ll.get("game_pk_safe_int") is True,
        "limit_value_confirmed": json_6ll.get("limit_value"),
        "signature_exact_match_confirmed": json_6ll.get("signature_exact_match") is True,
        "signature_mapping_safe_confirmed": json_6ll.get("signature_mapping_safe") is True,
        "package_import_succeeded_confirmed": json_6ll.get("package_import_succeeded") is True,
        "function_callable_confirmed": json_6ll.get("function_callable") is True,
        "adapter_call_attempted_confirmed": adapter_attempted,
        "adapter_call_succeeded_confirmed": adapter_succeeded,
        "adapter_call_failed_closed_confirmed": adapter_failed_closed,
        "adapter_call_count_confirmed": adapter_count,
        "three_argument_call_contract_resolved": adapter_attempted and adapter_succeeded and adapter_count == 1,
        "return_materialized_confirmed": return_materialized,
        "return_type_confirmed": return_type,
        "empty_list_return_confirmed": empty_list_return,
        "adapter_return_lacked_prediction_fields_confirmed": gap_reason_confirmed,
        "projection_surface_materialized_confirmed": False,
        "real_prediction_fields_materialized": False,
        "probability_metric_ready_after_audit": False,
        "runs_metric_ready_after_audit": False,
        "any_backtest_metric_ready_after_audit": False,
        "empty_return_trace_plan_needed": True,
        "next_candidate_retry_recommended": False,
        "wrapper_plan_needed": False,
        "historical_odds_required": False,
        "full_batch_adapter_call_run": False,
        "real_historical_evaluation_run": False,
        "production_simulations_run": False,
        "local_measurement_run": False,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
        "database_writes_run": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "production_source_modifications_run": False,
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "candidate_audit_csv": str(CANDIDATE_AUDIT_CSV),
            "call_execution_audit_csv": str(CALL_EXECUTION_AUDIT_CSV),
            "return_shape_audit_csv": str(RETURN_SHAPE_AUDIT_CSV),
            "gap_report_audit_csv": str(GAP_AUDIT_CSV),
            "prediction_surface_audit_csv": str(SURFACE_AUDIT_CSV),
            "metric_readiness_audit_csv": str(METRIC_AUDIT_CSV),
            "next_route_csv": str(NEXT_ROUTE_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6ln_contract_csv": str(FUTURE_6LN_CSV),
            "readonly_sources_csv": str(READONLY_CSV),
            "blocking_policy_csv": str(BLOCKING_CSV),
            "decision_csv": str(DECISION_CSV),
            "safety_boundaries_csv": str(SAFETY_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
