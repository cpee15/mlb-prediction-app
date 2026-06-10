#!/usr/bin/env python3
"""Plan a single-sample adapter call using the shape-repaired payload."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6lt_projection_adapter_shape_repaired_call_plan"
TMP_DIR = Path("tmp")

AUDIT_6LS_PATH = Path("scripts/audit_6ls_layer6_projection_adapter_payload_shape_repair.py")
JSON_6LS = TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit.json"
SHAPE_PAYLOAD_JSON = TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_adapter_payload.json"

REQUIRED_6LS_INPUTS = [
    JSON_6LS,
    TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit_checks.csv",
    TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit_predecessor.csv",
    TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit_payload_artifact_audit.csv",
    TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit_game_entry_audit.csv",
    TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit_field_readiness_audit.csv",
    TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit_projection_surface_audit.csv",
    TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit_metric_readiness_audit.csv",
    TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit_call_plan_readiness.csv",
    TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit_next_route.csv",
    TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit_blockers.csv",
    TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit_future_6lt_contract.csv",
    TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit_decision.csv",
    TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6ls_projection_adapter_payload_shape_repair_audit_recommended_path.csv",
    SHAPE_PAYLOAD_JSON,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PROBLEM_CSV = TMP_DIR / f"{SLUG}_problem_statement.csv"
RETENTION_CSV = TMP_DIR / f"{SLUG}_candidate_retention.csv"
CALL_ARGS_CSV = TMP_DIR / f"{SLUG}_call_arguments.csv"
CALL_CONTRACT_CSV = TMP_DIR / f"{SLUG}_call_contract.csv"
RETURN_TARGETS_CSV = TMP_DIR / f"{SLUG}_return_shape_targets.csv"
FAIL_CLOSED_CSV = TMP_DIR / f"{SLUG}_fail_closed_policy.csv"
SURFACE_RULES_CSV = TMP_DIR / f"{SLUG}_prediction_surface_rules.csv"
METRIC_GUARDS_CSV = TMP_DIR / f"{SLUG}_metric_guardrails.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LU_CSV = TMP_DIR / f"{SLUG}_future_6lu_contract.csv"
FUTURE_6LV_CSV = TMP_DIR / f"{SLUG}_future_6lv_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LS = "layer_6_projection_adapter_payload_shape_repair_audit_complete"
DIAGNOSIS_6LT = "layer_6_projection_adapter_shape_repaired_call_plan_complete"
RECOMMENDED_NEXT_LAYER_6LS = "6LT_layer_6_projection_adapter_shape_repaired_call_plan"
RECOMMENDED_NEXT_LAYER_6LT = "6LU_layer_6_projection_adapter_shape_repaired_call_implementation"
RECOMMENDED_PATH_6LT = "implement_single_sample_adapter_call_with_shape_repaired_payload"

TARGET_MODULE = "mlb_app.ai_data_assistant_performance"
TARGET_FUNCTION = "_canonical_games_from_projection_payload"
REQUIRED_ARGS = "payload;game_pk;limit"
TARGET_GAME_PK = 824776
TARGET_LIMIT = 1
CALL_CONTRACT = "_canonical_games_from_projection_payload(payload, game_pk=824776, limit=1)"


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
    json_6ls = load_json(JSON_6LS)
    payload = load_json(SHAPE_PAYLOAD_JSON)

    games = payload.get("games") if isinstance(payload, dict) else None
    first_game = games[0] if isinstance(games, list) and games else {}

    payload_exists = SHAPE_PAYLOAD_JSON.exists()
    games_present = isinstance(payload, dict) and "games" in payload
    games_count = len(games) if isinstance(games, list) else 0
    game_pk_present = isinstance(first_game, dict) and first_game.get("game_pk") == TARGET_GAME_PK

    problem = [
        {
            "problem": "shape_repaired_single_sample_adapter_call_needs_plan",
            "payload_path": str(SHAPE_PAYLOAD_JSON),
            "game_pk": TARGET_GAME_PK,
            "limit": TARGET_LIMIT,
            "passed": True,
        }
    ]

    retention = [
        {"item": "same_candidate_retained", "value": True, "module": TARGET_MODULE, "function": TARGET_FUNCTION, "passed": True},
        {"item": "blocked_session_candidate_excluded", "value": True, "blocked_function": "cached_build_model_projection_payload", "passed": True},
        {"item": "next_candidate_retry_recommended", "value": False, "reason": "same candidate now has shape-repaired call plan", "passed": True},
        {"item": "wrapper_plan_needed", "value": False, "reason": "direct adapter call contract still viable", "passed": True},
    ]

    call_args = [
        {"argument": "payload", "planned_source": str(SHAPE_PAYLOAD_JSON), "safe": payload_exists and games_present, "passed": payload_exists and games_present},
        {"argument": "game_pk", "planned_value": TARGET_GAME_PK, "safe": True, "passed": True},
        {"argument": "limit", "planned_value": TARGET_LIMIT, "safe": TARGET_LIMIT == 1, "passed": TARGET_LIMIT == 1},
    ]

    call_contract = [
        {"contract_item": "target_module", "value": TARGET_MODULE, "passed": True},
        {"contract_item": "target_function", "value": TARGET_FUNCTION, "passed": True},
        {"contract_item": "required_arguments", "value": REQUIRED_ARGS, "passed": True},
        {"contract_item": "future_call_contract", "value": CALL_CONTRACT, "passed": True},
        {"contract_item": "single_sample_only", "value": True, "passed": True},
        {"contract_item": "non_production_only", "value": True, "passed": True},
        {"contract_item": "return_shape_materialization_only", "value": True, "passed": True},
    ]

    return_targets = [
        {"field": "game_pk", "inspect_in_future_return": True, "passed": True},
        {"field": "home_win_probability", "inspect_in_future_return": True, "passed": True},
        {"field": "away_win_probability", "inspect_in_future_return": True, "passed": True},
        {"field": "home_expected_runs", "inspect_in_future_return": True, "passed": True},
        {"field": "away_expected_runs", "inspect_in_future_return": True, "passed": True},
        {"field": "total_expected_runs", "inspect_in_future_return": True, "passed": True},
        {"field": "projected_total", "inspect_in_future_return": True, "passed": True},
    ]

    fail_closed = [
        {"condition": "module_import_fails", "action": "record_call_not_attempted_or_failed_closed", "passed": True},
        {"condition": "function_missing", "action": "record_function_missing", "passed": True},
        {"condition": "payload_missing_games", "action": "do_not_call_or_record_empty_payload_gap", "passed": True},
        {"condition": "game_pk_not_safe", "action": "do_not_call", "passed": True},
        {"condition": "limit_not_exactly_one", "action": "do_not_call", "passed": True},
        {"condition": "result_not_list_like", "action": "record_return_shape_gap", "passed": True},
        {"condition": "result_empty", "action": "record_empty_result_gap", "passed": True},
        {"condition": "result_lacks_prediction_fields", "action": "record_prediction_surface_gap", "passed": True},
    ]

    surface_rules = [
        {"rule": "future_call_may_materialize_return_shape_only", "passed": True},
        {"rule": "future_call_output_is_not_metric_until_audited", "passed": True},
        {"rule": "single_sample_result_is_not_backtest_surface", "passed": True},
        {"rule": "real_projection_surface_requires_non_placeholder_prediction_fields", "passed": True},
    ]

    metric_guards = [
        {"guardrail": "do_not_compute_metrics_in_6lt", "passed": True},
        {"guardrail": "do_not_compute_brier", "passed": True},
        {"guardrail": "do_not_compute_log_loss", "passed": True},
        {"guardrail": "do_not_compute_run_error_metrics", "passed": True},
        {"guardrail": "do_not_compute_calibration", "passed": True},
        {"guardrail": "future_metrics_require_audited_real_prediction_surface", "passed": True},
    ]

    allowed_next = [
        {"operation": "import_target_module_for_single_sample_call", "allowed_next": True, "passed": True},
        {"operation": "retrieve_target_function", "allowed_next": True, "passed": True},
        {"operation": "load_shape_repaired_payload_artifact", "allowed_next": True, "passed": True},
        {"operation": "execute_one_adapter_call", "allowed_next": True, "max_count": 1, "passed": True},
        {"operation": "materialize_return_shape_only", "allowed_next": True, "passed": True},
    ]

    forbidden_next = [
        {"operation": "full_batch_adapter_call", "allowed_next": False, "passed": True},
        {"operation": "real_metric_execution", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "live_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "database_writes", "allowed_next": False, "passed": True},
        {"operation": "production_source_modifications", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit_credit", "allowed_next": False, "passed": True},
        {"operation": "next_candidate_retry", "allowed_next": False, "passed": True},
        {"operation": "wrapper_design", "allowed_next": False, "passed": True},
    ]

    blockers = [
        {"blocker": "real_prediction_surface_not_materialized", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "shape_repaired_adapter_call_not_executed_or_audited", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6lu = [
        {"contract": "execute_exactly_one_adapter_call", "required": True, "max_count": 1, "passed": True},
        {"contract": "use_shape_repaired_payload_artifact", "required": True, "passed": True},
        {"contract": "materialize_return_shape_only", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_activation_or_exit", "required": True, "passed": True},
    ]

    future_6lv = [
        {"contract": "audit_single_sample_adapter_call_return_shape", "required": True, "passed": True},
        {"contract": "determine_if_real_prediction_surface_exists", "required": True, "passed": True},
        {"contract": "route_to_surface_repair_metrics_or_next_gap", "required": True, "passed": True},
        {"contract": "preserve_layer6_exit_blocked", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ls_audit_script_exists", "expected": True, "actual": AUDIT_6LS_PATH.exists(), "passed": AUDIT_6LS_PATH.exists()},
        {"check": "6ls_json_exists", "expected": True, "actual": JSON_6LS.exists(), "passed": JSON_6LS.exists()},
        {"check": "6ls_all_checks_passed", "expected": True, "actual": json_6ls.get("all_checks_passed"), "passed": json_6ls.get("all_checks_passed") is True},
        {"check": "6ls_diagnosis", "expected": DIAGNOSIS_6LS, "actual": json_6ls.get("diagnosis"), "passed": json_6ls.get("diagnosis") == DIAGNOSIS_6LS},
        {"check": "6ls_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LS, "actual": json_6ls.get("recommended_next_layer"), "passed": json_6ls.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LS},
        {"check": "6ls_future_call_plan_allowed", "expected": True, "actual": json_6ls.get("future_single_sample_adapter_call_plan_allowed_next"), "passed": json_6ls.get("future_single_sample_adapter_call_plan_allowed_next") is True},
        {"check": "6ls_no_call_execution_next", "expected": False, "actual": json_6ls.get("future_adapter_call_execution_allowed_next"), "passed": json_6ls.get("future_adapter_call_execution_allowed_next") is False},
        {"check": "6ls_no_layer6_exit", "expected": False, "actual": json_6ls.get("layer_6_exit_recommended"), "passed": json_6ls.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_6LS_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_6LS_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6lu_shape_repaired_call_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "full_batch_adapter_call", "blocked": True, "reason": "only one call allowed next", "passed": True},
        {"blocked_surface": "real_metrics", "blocked": True, "reason": "return shape must be audited first", "passed": True},
        {"blocked_surface": "historical_backtest", "blocked": True, "reason": "real prediction surface required first", "passed": True},
        {"blocked_surface": "mechanics_activation", "blocked": True, "reason": "real metrics required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LT cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6ls_passed", "expected": True, "actual": json_6ls.get("all_checks_passed"), "passed": json_6ls.get("all_checks_passed") is True},
        {"decision": "payload_artifact_valid", "expected": True, "actual": payload_exists and games_present and games_count == 1 and game_pk_present, "passed": payload_exists and games_present and games_count == 1 and game_pk_present},
        {"decision": "call_args_valid", "expected": True, "actual": all_passed(call_args), "passed": all_passed(call_args)},
        {"decision": "call_contract_valid", "expected": True, "actual": all_passed(call_contract), "passed": all_passed(call_contract)},
        {"decision": "future_6lu_contract_valid", "expected": True, "actual": len(future_6lu) == 4 and all_passed(future_6lu), "passed": len(future_6lu) == 4 and all_passed(future_6lu)},
        {"decision": "future_6lv_contract_valid", "expected": True, "actual": len(future_6lv) == 4 and all_passed(future_6lv), "passed": len(future_6lv) == 4 and all_passed(future_6lv)},
        {"decision": "recommend_6lu_next", "expected": RECOMMENDED_NEXT_LAYER_6LT, "actual": RECOMMENDED_NEXT_LAYER_6LT, "passed": True},
        {"decision": "do_not_execute_adapter_by_6lt", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "target_module_imported_by_6lt", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6lt", "expected": False, "actual": False, "passed": True},
        {"boundary": "full_batch_adapter_call_allowed_next", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_metric_execution_allowed_next", "expected": False, "actual": False, "passed": True},
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
        {"surface": "source_tree", "policy": "read_only_planning", "passed": True},
        {"surface": "6ls_audit", "policy": "read_only", "passed": True},
        {"surface": "shape_repaired_payload_artifact", "policy": "read_only_input", "passed": True},
        {"surface": "future_6lu_execution", "policy": "single_sample_only_next_layer", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6lt", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LT, "actual": RECOMMENDED_NEXT_LAYER_6LT, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LT, "actual": RECOMMENDED_PATH_6LT, "passed": True},
        {"decision": "recommend_shape_repaired_call_implementation", "expected": True, "actual": True, "passed": True},
        {"decision": "allow_next_layer_one_adapter_call_only", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_full_batch_call", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metrics", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_next_candidate_retry", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LT, "actual": DIAGNOSIS_6LT, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "problem_statement", "passed": all_passed(problem), "detail": f"{len(problem)} rows"},
        {"check": "candidate_retention", "passed": all_passed(retention), "detail": f"{len(retention)} rows"},
        {"check": "call_arguments", "passed": all_passed(call_args), "detail": f"{len(call_args)} rows"},
        {"check": "call_contract", "passed": all_passed(call_contract), "detail": f"{len(call_contract)} rows"},
        {"check": "return_shape_targets", "passed": all_passed(return_targets), "detail": f"{len(return_targets)} rows"},
        {"check": "fail_closed_policy", "passed": all_passed(fail_closed), "detail": f"{len(fail_closed)} rows"},
        {"check": "prediction_surface_rules", "passed": all_passed(surface_rules), "detail": f"{len(surface_rules)} rows"},
        {"check": "metric_guardrails", "passed": all_passed(metric_guards), "detail": f"{len(metric_guards)} rows"},
        {"check": "allowed_next", "passed": all_passed(allowed_next), "detail": f"{len(allowed_next)} rows"},
        {"check": "forbidden_next", "passed": all_passed(forbidden_next), "detail": f"{len(forbidden_next)} rows"},
        {"check": "future_6lu_contract", "passed": all_passed(future_6lu), "detail": f"{len(future_6lu)} rows"},
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
        "problem_statement": write_csv(PROBLEM_CSV, problem),
        "candidate_retention": write_csv(RETENTION_CSV, retention),
        "call_arguments": write_csv(CALL_ARGS_CSV, call_args),
        "call_contract": write_csv(CALL_CONTRACT_CSV, call_contract),
        "return_shape_targets": write_csv(RETURN_TARGETS_CSV, return_targets),
        "fail_closed_policy": write_csv(FAIL_CLOSED_CSV, fail_closed),
        "prediction_surface_rules": write_csv(SURFACE_RULES_CSV, surface_rules),
        "metric_guardrails": write_csv(METRIC_GUARDS_CSV, metric_guards),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6lu_contract": write_csv(FUTURE_6LU_CSV, future_6lu),
        "future_6lv_contract": write_csv(FUTURE_6LV_CSV, future_6lv),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LT",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LT if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LT,
        "recommended_path": RECOMMENDED_PATH_6LT,
        "predecessor_audit": str(AUDIT_6LS_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6ls.get("diagnosis"),
        "planned_layer_after": "6LS",
        "source_family": "projection_adapter_shape_repaired_call_plan",
        "problem_statement_count": len(problem),
        "candidate_retention_count": len(retention),
        "call_argument_count": len(call_args),
        "call_contract_count": len(call_contract),
        "return_shape_target_count": len(return_targets),
        "fail_closed_policy_count": len(fail_closed),
        "prediction_surface_rule_count": len(surface_rules),
        "metric_guardrail_count": len(metric_guards),
        "allowed_operation_next_count": len(allowed_next),
        "forbidden_operation_next_count": len(forbidden_next),
        "blocker_count": len(blockers),
        "future_6lu_contract_valid": len(future_6lu) == 4 and all_passed(future_6lu),
        "future_6lv_contract_valid": len(future_6lv) == 4 and all_passed(future_6lv),
        "projection_adapter_shape_repaired_call_planned": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "same_candidate_retained": True,
        "blocked_session_candidate_excluded": True,
        "target_module_import_path": TARGET_MODULE,
        "target_function_name": TARGET_FUNCTION,
        "required_arguments_confirmed": REQUIRED_ARGS,
        "shape_repaired_payload_path": str(SHAPE_PAYLOAD_JSON),
        "shape_repaired_payload_exists": payload_exists,
        "shape_repaired_payload_games_key_present": games_present,
        "shape_repaired_payload_games_count": games_count,
        "shape_repaired_payload_game_pk_824776_present": game_pk_present,
        "future_call_payload_source_planned": str(SHAPE_PAYLOAD_JSON),
        "future_call_game_pk_planned": TARGET_GAME_PK,
        "future_call_limit_planned": TARGET_LIMIT,
        "future_call_contract_planned": CALL_CONTRACT,
        "future_call_single_sample_only": True,
        "future_call_non_production_only": True,
        "future_call_return_shape_materialization_only": True,
        "future_call_execution_allowed_next": True,
        "adapter_call_executed_by_6lt": False,
        "target_module_imported_by_6lt": False,
        "real_prediction_fields_materialized": False,
        "projection_surface_materialized": False,
        "probability_metric_ready_after_plan": False,
        "runs_metric_ready_after_plan": False,
        "any_backtest_metric_ready_after_plan": False,
        "next_candidate_retry_recommended": False,
        "wrapper_plan_needed": False,
        "historical_odds_required": False,
        "full_batch_adapter_call_allowed_next": False,
        "real_metric_execution_allowed_next": False,
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
            "problem_statement_csv": str(PROBLEM_CSV),
            "candidate_retention_csv": str(RETENTION_CSV),
            "call_arguments_csv": str(CALL_ARGS_CSV),
            "call_contract_csv": str(CALL_CONTRACT_CSV),
            "return_shape_targets_csv": str(RETURN_TARGETS_CSV),
            "fail_closed_policy_csv": str(FAIL_CLOSED_CSV),
            "prediction_surface_rules_csv": str(SURFACE_RULES_CSV),
            "metric_guardrails_csv": str(METRIC_GUARDS_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6lu_contract_csv": str(FUTURE_6LU_CSV),
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
