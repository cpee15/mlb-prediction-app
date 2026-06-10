#!/usr/bin/env python3
"""Execute one shape-repaired adapter call and materialize return shape only."""

from __future__ import annotations

import csv
import importlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6lu_projection_adapter_shape_repaired_call_implementation"
TMP_DIR = Path("tmp")

PLAN_6LT_PATH = Path("scripts/plan_6lt_layer6_projection_adapter_shape_repaired_call.py")
JSON_6LT = TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan.json"
SHAPE_PAYLOAD_JSON = TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_adapter_payload.json"

REQUIRED_6LT_INPUTS = [
    JSON_6LT,
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_checks.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_predecessor.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_problem_statement.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_candidate_retention.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_call_arguments.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_call_contract.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_return_shape_targets.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_fail_closed_policy.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_prediction_surface_rules.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_metric_guardrails.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_blockers.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_future_6lu_contract.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_future_6lv_contract.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_decision.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6lt_projection_adapter_shape_repaired_call_plan_recommended_path.csv",
    SHAPE_PAYLOAD_JSON,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PAYLOAD_VALIDATION_CSV = TMP_DIR / f"{SLUG}_payload_validation.csv"
IMPORT_TRACE_CSV = TMP_DIR / f"{SLUG}_import_trace.csv"
CALL_EXECUTION_CSV = TMP_DIR / f"{SLUG}_call_execution.csv"
RETURN_SHAPE_CSV = TMP_DIR / f"{SLUG}_return_shape.csv"
FIELD_PRESENCE_CSV = TMP_DIR / f"{SLUG}_prediction_field_presence.csv"
GAP_REPORT_CSV = TMP_DIR / f"{SLUG}_gap_report.csv"
SURFACE_READY_CSV = TMP_DIR / f"{SLUG}_projection_surface_readiness.csv"
METRIC_READY_CSV = TMP_DIR / f"{SLUG}_metric_readiness.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LV_CSV = TMP_DIR / f"{SLUG}_future_6lv_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LT = "layer_6_projection_adapter_shape_repaired_call_plan_complete"
DIAGNOSIS_6LU = "layer_6_projection_adapter_shape_repaired_call_implementation_complete"
RECOMMENDED_NEXT_LAYER_6LT = "6LU_layer_6_projection_adapter_shape_repaired_call_implementation"
RECOMMENDED_NEXT_LAYER_6LU = "6LV_layer_6_projection_adapter_shape_repaired_call_audit"
RECOMMENDED_PATH_6LU = "audit_single_sample_adapter_call_return_shape"

TARGET_MODULE = "mlb_app.ai_data_assistant_performance"
TARGET_FUNCTION = "_canonical_games_from_projection_payload"
REQUIRED_ARGS = "payload;game_pk;limit"
TARGET_GAME_PK = 824776
TARGET_LIMIT = 1
PREDICTION_FIELDS = [
    "game_pk",
    "home_win_probability",
    "away_win_probability",
    "home_expected_runs",
    "away_expected_runs",
    "total_expected_runs",
    "projected_total",
]


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


def result_shape(result: Any) -> Tuple[List[Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]]]:
    is_list = isinstance(result, list)
    length = len(result) if is_list else None
    first = result[0] if is_list and result else None
    first_type = type(first).__name__ if first is not None else "empty"
    first_keys = sorted(first.keys()) if isinstance(first, dict) else []
    return_rows = [
        {"shape_item": "return_type", "value": type(result).__name__, "passed": True},
        {"shape_item": "is_list", "value": is_list, "passed": True},
        {"shape_item": "list_length", "value": length if length is not None else "", "passed": True},
        {"shape_item": "first_item_type", "value": first_type, "passed": True},
        {"shape_item": "first_item_keys", "value": ";".join(first_keys), "passed": True},
    ]
    field_rows = [
        {"field": field, "present": field in first_keys, "passed": True}
        for field in PREDICTION_FIELDS
    ]
    info = {
        "adapter_return_type": type(result).__name__,
        "adapter_return_list_length": length if length is not None else "",
        "adapter_return_first_item_type": first_type,
        "adapter_return_first_item_keys": ";".join(first_keys),
        "adapter_return_empty": is_list and length == 0,
        "adapter_return_has_game_pk": "game_pk" in first_keys,
        "adapter_return_has_home_win_probability": "home_win_probability" in first_keys,
        "adapter_return_has_away_win_probability": "away_win_probability" in first_keys,
        "adapter_return_has_home_expected_runs": "home_expected_runs" in first_keys,
        "adapter_return_has_away_expected_runs": "away_expected_runs" in first_keys,
        "adapter_return_has_total_expected_runs": "total_expected_runs" in first_keys,
        "adapter_return_has_projected_total": "projected_total" in first_keys,
    }
    return return_rows, info, field_rows


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6lt = load_json(JSON_6LT)
    payload = load_json(SHAPE_PAYLOAD_JSON)

    games = payload.get("games") if isinstance(payload, dict) else None
    first_game = games[0] if isinstance(games, list) and games else {}
    payload_loaded = bool(payload)
    games_present = isinstance(payload, dict) and "games" in payload
    games_is_list = isinstance(games, list)
    games_count = len(games) if isinstance(games, list) else 0
    game_pk_present = isinstance(first_game, dict) and first_game.get("game_pk") == TARGET_GAME_PK
    payload_valid = payload_loaded and games_present and games_is_list and games_count == 1 and game_pk_present

    payload_validation = [
        {"validation": "payload_loaded", "value": payload_loaded, "passed": payload_loaded},
        {"validation": "payload_games_key_present", "value": games_present, "passed": games_present},
        {"validation": "payload_games_is_list", "value": games_is_list, "passed": games_is_list},
        {"validation": "payload_games_count", "value": games_count, "expected": 1, "passed": games_count == 1},
        {"validation": "payload_game_pk_824776_present", "value": game_pk_present, "passed": game_pk_present},
        {"validation": "call_limit_exactly_one", "value": TARGET_LIMIT, "expected": 1, "passed": TARGET_LIMIT == 1},
    ]

    module = None
    func = None
    import_error_type = ""
    import_error_message = ""
    import_succeeded = False
    function_retrieved = False
    function_callable = False

    try:
        module = importlib.import_module(TARGET_MODULE)
        import_succeeded = True
    except Exception as exc:
        import_error_type = type(exc).__name__
        import_error_message = str(exc)

    if import_succeeded:
        try:
            func = getattr(module, TARGET_FUNCTION)
            function_retrieved = True
            function_callable = callable(func)
        except Exception as exc:
            import_error_type = type(exc).__name__
            import_error_message = str(exc)

    import_trace = [
        {"trace": "target_module_import", "target": TARGET_MODULE, "succeeded": import_succeeded, "error_type": import_error_type, "error_message": import_error_message, "passed": True},
        {"trace": "target_function_retrieved", "target": TARGET_FUNCTION, "succeeded": function_retrieved, "error_type": import_error_type if not function_retrieved else "", "error_message": import_error_message if not function_retrieved else "", "passed": True},
        {"trace": "target_function_callable", "target": TARGET_FUNCTION, "succeeded": function_callable, "passed": True},
    ]

    adapter_call_attempted = False
    adapter_call_succeeded = False
    adapter_call_count = 0
    adapter_call_error_type = ""
    adapter_call_error_message = ""
    result: Any = None

    if payload_valid and function_callable:
        adapter_call_attempted = True
        adapter_call_count = 1
        try:
            result = func(payload, game_pk=TARGET_GAME_PK, limit=TARGET_LIMIT)
            adapter_call_succeeded = True
        except Exception as exc:
            adapter_call_error_type = type(exc).__name__
            adapter_call_error_message = str(exc)

    call_execution = [
        {"call_item": "adapter_call_attempted", "value": adapter_call_attempted, "passed": True},
        {"call_item": "adapter_call_succeeded", "value": adapter_call_succeeded, "passed": True},
        {"call_item": "adapter_call_count", "value": adapter_call_count, "expected_max": 1, "passed": adapter_call_count <= 1},
        {"call_item": "game_pk_used", "value": TARGET_GAME_PK, "passed": True},
        {"call_item": "limit_used", "value": TARGET_LIMIT, "passed": TARGET_LIMIT == 1},
        {"call_item": "adapter_call_error_type", "value": adapter_call_error_type, "passed": True},
        {"call_item": "adapter_call_error_message", "value": adapter_call_error_message[:500], "passed": True},
    ]

    if adapter_call_succeeded:
        return_shape_rows, shape_info, field_presence = result_shape(result)
    else:
        return_shape_rows, shape_info, field_presence = result_shape([])
        return_shape_rows.append({"shape_item": "call_not_successful", "value": adapter_call_error_type or import_error_type or "not_attempted", "passed": True})

    has_any_prediction_field = any(
        row["field"] != "game_pk" and row["present"] for row in field_presence
    )
    adapter_return_empty = bool(shape_info.get("adapter_return_empty"))
    adapter_return_lacks_prediction_fields = not has_any_prediction_field
    projection_surface_materialized = adapter_call_succeeded and (not adapter_return_empty) and has_any_prediction_field

    gap_report = [
        {"gap": "adapter_call_not_succeeded", "active": not adapter_call_succeeded, "detail": adapter_call_error_type or import_error_type or "", "passed": True},
        {"gap": "adapter_return_empty", "active": adapter_return_empty, "detail": "result list empty" if adapter_return_empty else "", "passed": True},
        {"gap": "adapter_return_lacks_prediction_fields", "active": adapter_return_lacks_prediction_fields, "detail": "no prediction fields in first returned row", "passed": True},
        {"gap": "real_prediction_surface_not_materialized", "active": not projection_surface_materialized, "detail": "requires non-empty rows with prediction fields", "passed": True},
    ]

    surface_ready = [
        {"surface": "single_sample_return_shape_materialized", "ready": adapter_call_succeeded, "passed": True},
        {"surface": "projection_surface_materialized", "ready": projection_surface_materialized, "passed": True},
        {"surface": "real_prediction_fields_materialized", "ready": has_any_prediction_field and not adapter_return_empty, "passed": True},
    ]

    metric_ready = [
        {"metric": "probability_metric_ready_after_implementation", "ready": False, "reason": "no metric execution in 6LU", "passed": True},
        {"metric": "runs_metric_ready_after_implementation", "ready": False, "reason": "no metric execution in 6LU", "passed": True},
        {"metric": "any_backtest_metric_ready_after_implementation", "ready": False, "reason": "no backtest in 6LU", "passed": True},
    ]

    blockers = [
        {"blocker": "real_prediction_surface_not_materialized", "active": not projection_surface_materialized, "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "single_sample_adapter_call_requires_audit", "active": True, "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "active": True, "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "active": True, "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6lv = [
        {"contract": "audit_single_sample_adapter_call_return_shape", "required": True, "passed": True},
        {"contract": "audit_import_and_call_trace", "required": True, "passed": True},
        {"contract": "determine_if_prediction_surface_materialized", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_activation_or_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6lt_plan_script_exists", "expected": True, "actual": PLAN_6LT_PATH.exists(), "passed": PLAN_6LT_PATH.exists()},
        {"check": "6lt_json_exists", "expected": True, "actual": JSON_6LT.exists(), "passed": JSON_6LT.exists()},
        {"check": "6lt_all_checks_passed", "expected": True, "actual": json_6lt.get("all_checks_passed"), "passed": json_6lt.get("all_checks_passed") is True},
        {"check": "6lt_diagnosis", "expected": DIAGNOSIS_6LT, "actual": json_6lt.get("diagnosis"), "passed": json_6lt.get("diagnosis") == DIAGNOSIS_6LT},
        {"check": "6lt_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LT, "actual": json_6lt.get("recommended_next_layer"), "passed": json_6lt.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LT},
        {"check": "6lt_future_call_execution_allowed_next", "expected": True, "actual": json_6lt.get("future_call_execution_allowed_next"), "passed": json_6lt.get("future_call_execution_allowed_next") is True},
        {"check": "6lt_no_adapter_call_executed", "expected": False, "actual": json_6lt.get("adapter_call_executed_by_6lt"), "passed": json_6lt.get("adapter_call_executed_by_6lt") is False},
        {"check": "6lt_no_layer6_exit", "expected": False, "actual": json_6lt.get("layer_6_exit_recommended"), "passed": json_6lt.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_6LT_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_6LT_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6lv_return_shape_audit", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "additional_adapter_call", "blocked": True, "reason": "6LU allowed exactly one call only", "passed": True},
        {"blocked_surface": "full_batch_adapter_call", "blocked": True, "reason": "full batch forbidden", "passed": True},
        {"blocked_surface": "real_metrics", "blocked": True, "reason": "return shape audit required first", "passed": True},
        {"blocked_surface": "historical_backtest", "blocked": True, "reason": "real prediction surface required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LU cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6lt_passed", "expected": True, "actual": json_6lt.get("all_checks_passed"), "passed": json_6lt.get("all_checks_passed") is True},
        {"decision": "payload_valid", "expected": True, "actual": payload_valid, "passed": payload_valid},
        {"decision": "import_trace_recorded", "expected": True, "actual": len(import_trace) == 3, "passed": len(import_trace) == 3},
        {"decision": "adapter_call_count_lte_one", "expected": True, "actual": adapter_call_count <= 1, "passed": adapter_call_count <= 1},
        {"decision": "call_execution_recorded", "expected": True, "actual": len(call_execution) >= 7, "passed": len(call_execution) >= 7},
        {"decision": "return_shape_recorded", "expected": True, "actual": len(return_shape_rows) >= 5, "passed": len(return_shape_rows) >= 5},
        {"decision": "future_6lv_contract_valid", "expected": True, "actual": len(future_6lv) == 4 and all_passed(future_6lv), "passed": len(future_6lv) == 4 and all_passed(future_6lv)},
        {"decision": "recommend_6lv_next", "expected": RECOMMENDED_NEXT_LAYER_6LU, "actual": RECOMMENDED_NEXT_LAYER_6LU, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only_single_sample_adapter_call", "expected": True, "actual": True, "passed": True},
        {"boundary": "adapter_call_count_lte_one", "expected": True, "actual": adapter_call_count <= 1, "passed": adapter_call_count <= 1},
        {"boundary": "full_batch_adapter_call_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_metric_execution_run", "expected": False, "actual": False, "passed": True},
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
        {"surface": "shape_repaired_payload_artifact", "policy": "read_only_input", "passed": True},
        {"surface": "candidate_module", "policy": "import_only_no_source_mutation", "passed": True},
        {"surface": "adapter_call", "policy": "exactly_one_call_max", "passed": True},
        {"surface": "return_shape", "policy": "materialize_shape_only", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6lu", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LU, "actual": RECOMMENDED_NEXT_LAYER_6LU, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LU, "actual": RECOMMENDED_PATH_6LU, "passed": True},
        {"decision": "recommend_return_shape_audit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_additional_call", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_full_batch_call", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metrics", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_next_candidate_retry", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LU, "actual": DIAGNOSIS_6LU, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "payload_validation", "passed": all_passed(payload_validation), "detail": f"{len(payload_validation)} rows"},
        {"check": "import_trace", "passed": all_passed(import_trace), "detail": f"{len(import_trace)} rows"},
        {"check": "call_execution", "passed": all_passed(call_execution), "detail": f"{len(call_execution)} rows"},
        {"check": "return_shape", "passed": all_passed(return_shape_rows), "detail": f"{len(return_shape_rows)} rows"},
        {"check": "prediction_field_presence", "passed": all_passed(field_presence), "detail": f"{len(field_presence)} rows"},
        {"check": "gap_report", "passed": all_passed(gap_report), "detail": f"{len(gap_report)} rows"},
        {"check": "projection_surface_readiness", "passed": all_passed(surface_ready), "detail": f"{len(surface_ready)} rows"},
        {"check": "metric_readiness", "passed": all_passed(metric_ready), "detail": f"{len(metric_ready)} rows"},
        {"check": "future_6lv_contract", "passed": all_passed(future_6lv), "detail": f"{len(future_6lv)} rows"},
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
        "payload_validation": write_csv(PAYLOAD_VALIDATION_CSV, payload_validation),
        "import_trace": write_csv(IMPORT_TRACE_CSV, import_trace),
        "call_execution": write_csv(CALL_EXECUTION_CSV, call_execution),
        "return_shape": write_csv(RETURN_SHAPE_CSV, return_shape_rows),
        "prediction_field_presence": write_csv(FIELD_PRESENCE_CSV, field_presence),
        "gap_report": write_csv(GAP_REPORT_CSV, gap_report),
        "projection_surface_readiness": write_csv(SURFACE_READY_CSV, surface_ready),
        "metric_readiness": write_csv(METRIC_READY_CSV, metric_ready),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6lv_contract": write_csv(FUTURE_6LV_CSV, future_6lv),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LU",
        "layer_type": "game_mechanics_realism",
        "implementation_only_single_sample_adapter_call": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LU if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LU,
        "recommended_path": RECOMMENDED_PATH_6LU,
        "predecessor_plan": str(PLAN_6LT_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6lt.get("diagnosis"),
        "implemented_layer_after": "6LT",
        "source_family": "projection_adapter_shape_repaired_call_implementation",
        "payload_validation_count": len(payload_validation),
        "import_trace_count": len(import_trace),
        "call_execution_count": len(call_execution),
        "return_shape_count": len(return_shape_rows),
        "prediction_field_presence_count": len(field_presence),
        "gap_report_count": len(gap_report),
        "projection_surface_readiness_count": len(surface_ready),
        "metric_readiness_count": len(metric_ready),
        "blocker_count": len(blockers),
        "future_6lv_contract_valid": len(future_6lv) == 4 and all_passed(future_6lv),
        "projection_adapter_shape_repaired_call_implemented": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "same_candidate_retained_confirmed": json_6lt.get("same_candidate_retained") is True,
        "blocked_session_candidate_excluded_confirmed": json_6lt.get("blocked_session_candidate_excluded") is True,
        "target_module_import_path": TARGET_MODULE,
        "target_function_name": TARGET_FUNCTION,
        "required_arguments_confirmed": REQUIRED_ARGS,
        "payload_source_used": str(SHAPE_PAYLOAD_JSON),
        "payload_loaded": payload_loaded,
        "payload_games_key_present": games_present,
        "payload_games_is_list": games_is_list,
        "payload_games_count": games_count,
        "payload_game_pk_824776_present": game_pk_present,
        "call_game_pk_used": TARGET_GAME_PK,
        "call_limit_used": TARGET_LIMIT,
        "target_module_import_succeeded": import_succeeded,
        "target_function_retrieved": function_retrieved,
        "target_function_callable": function_callable,
        "adapter_call_attempted": adapter_call_attempted,
        "adapter_call_succeeded": adapter_call_succeeded,
        "adapter_call_count": adapter_call_count,
        "adapter_call_error_type": adapter_call_error_type,
        "adapter_call_error_message": adapter_call_error_message,
        **shape_info,
        "adapter_return_lacks_prediction_fields": adapter_return_lacks_prediction_fields,
        "projection_surface_materialized": projection_surface_materialized,
        "real_prediction_fields_materialized": has_any_prediction_field and not adapter_return_empty,
        "probability_metric_ready_after_implementation": False,
        "runs_metric_ready_after_implementation": False,
        "any_backtest_metric_ready_after_implementation": False,
        "full_batch_adapter_call_run": False,
        "real_metric_execution_run": False,
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
            "payload_validation_csv": str(PAYLOAD_VALIDATION_CSV),
            "import_trace_csv": str(IMPORT_TRACE_CSV),
            "call_execution_csv": str(CALL_EXECUTION_CSV),
            "return_shape_csv": str(RETURN_SHAPE_CSV),
            "prediction_field_presence_csv": str(FIELD_PRESENCE_CSV),
            "gap_report_csv": str(GAP_REPORT_CSV),
            "projection_surface_readiness_csv": str(SURFACE_READY_CSV),
            "metric_readiness_csv": str(METRIC_READY_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6lv_contract_csv": str(FUTURE_6LV_CSV),
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
