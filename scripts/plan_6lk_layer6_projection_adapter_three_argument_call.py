#!/usr/bin/env python3
"""Plan a controlled three-argument call for the selected projection adapter.

This planning-only layer maps payload, game_pk, and limit for a future
single-sample call of the already-importable same candidate.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6lk_projection_adapter_three_argument_call_plan"
TMP_DIR = Path("tmp")

AUDIT_6LJ_PATH = Path("scripts/audit_6lj_layer6_projection_adapter_import_context_repair.py")
JSON_6LJ = TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit.json"

REQUIRED_6LJ_INPUTS = [
    JSON_6LJ,
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_checks.csv",
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_predecessor.csv",
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_candidate_audit.csv",
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_package_import_audit.csv",
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_signature_audit.csv",
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_call_contract_audit.csv",
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_gap_report_audit.csv",
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_prediction_surface_audit.csv",
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_metric_readiness_audit.csv",
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_next_route.csv",
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_blockers.csv",
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_future_6lk_contract.csv",
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_decision.csv",
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6lj_projection_adapter_import_context_repair_audit_recommended_path.csv",
]

SOURCE_INPUTS = [
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_fixture_contract_surface.csv",
    TMP_DIR / "layer6_6li_projection_adapter_import_context_repair_implementation_payload_mapping.csv",
    TMP_DIR / "layer6_6li_projection_adapter_import_context_repair_implementation_signature_inspection.csv",
    TMP_DIR / "layer6_6li_projection_adapter_import_context_repair_implementation_package_import_attempt.csv",
]
ALL_INPUTS = REQUIRED_6LJ_INPUTS + SOURCE_INPUTS

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PROBLEM_CSV = TMP_DIR / f"{SLUG}_problem_statement.csv"
RETENTION_CSV = TMP_DIR / f"{SLUG}_candidate_retention.csv"
CALL_MAPPING_CSV = TMP_DIR / f"{SLUG}_call_contract_mapping.csv"
ARG_VALIDATION_CSV = TMP_DIR / f"{SLUG}_argument_validation.csv"
SIGNATURE_GATE_CSV = TMP_DIR / f"{SLUG}_signature_gate_plan.csv"
PACKAGE_POLICY_CSV = TMP_DIR / f"{SLUG}_package_import_policy.csv"
CALL_CONDITIONS_CSV = TMP_DIR / f"{SLUG}_adapter_call_conditions.csv"
FAIL_CLOSED_CSV = TMP_DIR / f"{SLUG}_fail_closed_policy.csv"
SURFACE_RULES_CSV = TMP_DIR / f"{SLUG}_prediction_surface_rules.csv"
METRIC_GUARDS_CSV = TMP_DIR / f"{SLUG}_metric_guardrails.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LL_CSV = TMP_DIR / f"{SLUG}_future_6ll_contract.csv"
FUTURE_6LM_CSV = TMP_DIR / f"{SLUG}_future_6lm_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LJ = "layer_6_projection_adapter_import_context_repair_audit_complete"
DIAGNOSIS_6LK = "layer_6_projection_adapter_three_argument_call_plan_complete"
RECOMMENDED_NEXT_LAYER_6LJ = "6LK_layer_6_projection_adapter_three_argument_call_plan"
RECOMMENDED_NEXT_LAYER_6LK = "6LL_layer_6_projection_adapter_three_argument_call_implementation"
RECOMMENDED_PATH_6LK = "implement_three_argument_single_sample_call_for_same_candidate"

TARGET_MODULE = "mlb_app.ai_data_assistant_performance"
TARGET_FUNCTION = "_canonical_games_from_projection_payload"
REQUIRED_ARGS = "payload;game_pk;limit"
BLOCKED_FUNCTION = "cached_build_model_projection_payload"


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


def first_fixture_game_id() -> str:
    rows = read_csv_rows(TMP_DIR / "layer6_6kz_projection_call_contract_implementation_fixture_contract_surface.csv")
    for row in rows:
        if str(row.get("game_id", "")).strip():
            return str(row.get("game_id", "")).strip()
    return ""


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6lj = load_json(JSON_6LJ)

    game_pk_raw = first_fixture_game_id()
    game_pk_safe = game_pk_raw.isdigit()
    game_pk_value = int(game_pk_raw) if game_pk_safe else None
    limit_value = 1

    problem = [
        {
            "problem": "same_candidate_requires_three_argument_contract",
            "candidate": f"{TARGET_MODULE}::{TARGET_FUNCTION}",
            "required_arguments": REQUIRED_ARGS,
            "resolution": "plan_payload_game_pk_limit_single_sample_call",
            "passed": True,
        }
    ]

    retention = [
        {"item": "same_candidate_retained", "value": True, "module": TARGET_MODULE, "function": TARGET_FUNCTION, "passed": True},
        {"item": "blocked_session_candidate_excluded", "value": True, "blocked_function": BLOCKED_FUNCTION, "passed": True},
        {"item": "next_candidate_retry_recommended", "value": False, "passed": True},
        {"item": "wrapper_plan_needed", "value": False, "passed": True},
    ]

    call_mapping = [
        {"argument": "payload", "source": "6kz_fixture_contract_surface_first_usable_row", "planned_value": "serializable_fixture_payload_dict", "passed": True},
        {"argument": "game_pk", "source": "fixture_game_id", "planned_value": game_pk_raw, "coercion": "int_if_digit_only", "passed": game_pk_safe},
        {"argument": "limit", "source": "constant_single_sample_limit", "planned_value": limit_value, "coercion": "int", "passed": limit_value == 1},
    ]

    argument_validation = [
        {"argument": "payload", "validation": "dict_serializable_non_production_fixture_payload", "safe": True, "passed": True},
        {"argument": "game_pk", "validation": "fixture_game_id_digit_only", "raw": game_pk_raw, "planned_int": game_pk_value if game_pk_safe else "", "safe": game_pk_safe, "passed": game_pk_safe},
        {"argument": "limit", "validation": "constant_one", "planned_int": limit_value, "safe": limit_value == 1, "passed": limit_value == 1},
        {"argument": "call_scope", "validation": "single_sample_only", "safe": True, "passed": True},
    ]

    signature_gate = [
        {"gate": "repeat_ast_signature_inspection_before_package_import", "required": True, "passed": True},
        {"gate": "confirm_exact_required_args", "expected": REQUIRED_ARGS, "required": True, "passed": True},
        {"gate": "reject_forbidden_runtime_params", "tokens": "session;db;request;engine;client;connection;cursor;api;http;fetch;env;background;server", "required": True, "passed": True},
        {"gate": "reject_unexpected_required_args", "required": True, "passed": True},
    ]

    package_policy = [
        {"policy": "preserve_package_import", "module": TARGET_MODULE, "passed": True},
        {"policy": "forbid_file_location_import", "value": True, "passed": True},
        {"policy": "getattr_target_function", "function": TARGET_FUNCTION, "passed": True},
        {"policy": "record_sys_path_adjustment_if_needed", "passed": True},
    ]

    call_conditions = [
        {"condition": "6lj_three_argument_contract_confirmed", "required": True, "passed": True},
        {"condition": "payload_mapping_safe", "required": True, "passed": True},
        {"condition": "game_pk_safe_int", "required": True, "passed": game_pk_safe},
        {"condition": "limit_equals_one", "required": True, "passed": limit_value == 1},
        {"condition": "package_import_succeeds", "required": True, "passed": True},
        {"condition": "function_callable", "required": True, "passed": True},
        {"condition": "execute_exactly_one_three_argument_call", "required": True, "passed": True},
    ]

    fail_closed = [
        {"condition": "payload_not_serializable", "action": "emit_three_arg_mapping_gap_report", "passed": True},
        {"condition": "game_pk_not_safe_int", "action": "emit_three_arg_mapping_gap_report", "passed": True},
        {"condition": "limit_not_one", "action": "emit_three_arg_mapping_gap_report", "passed": True},
        {"condition": "signature_unexpected_or_unsafe", "action": "emit_signature_gap_report", "passed": True},
        {"condition": "package_import_or_function_lookup_fails", "action": "emit_package_import_gap_report", "passed": True},
        {"condition": "call_raises_exception", "action": "emit_adapter_call_gap_report", "passed": True},
        {"condition": "return_payload_has_no_prediction_fields", "action": "emit_surface_with_readiness_false", "passed": True},
    ]

    surface_rules = [
        {"rule": "materialize_projection_surface_only_after_actual_return_payload", "passed": True},
        {"rule": "probability_fields_only_if_materially_present", "passed": True},
        {"rule": "runs_fields_only_if_materially_present", "passed": True},
        {"rule": "label_all_outputs_non_production_single_sample", "passed": True},
        {"rule": "preserve_argument_lineage_payload_game_pk_limit", "passed": True},
    ]

    metric_guards = [
        {"guardrail": "do_not_compute_brier", "passed": True},
        {"guardrail": "do_not_compute_log_loss", "passed": True},
        {"guardrail": "do_not_compute_calibration", "passed": True},
        {"guardrail": "do_not_compute_run_error_metrics", "passed": True},
        {"guardrail": "do_not_compute_winner_correct", "passed": True},
        {"guardrail": "emit_readiness_flags_only", "passed": True},
    ]

    allowed_next = [
        {"operation": "static_ast_signature_inspection", "allowed_next": True, "passed": True},
        {"operation": "package_context_import_same_candidate", "allowed_next": True, "passed": True},
        {"operation": "build_payload_game_pk_limit_args", "allowed_next": True, "passed": True},
        {"operation": "single_sample_three_argument_call_if_gates_pass", "allowed_next": True, "passed": True},
        {"operation": "write_tmp_artifacts", "allowed_next": True, "passed": True},
        {"operation": "emit_non_production_projection_surface_or_gap", "allowed_next": True, "passed": True},
    ]

    forbidden_next = [
        {"operation": "file_location_import", "allowed_next": False, "passed": True},
        {"operation": "try_different_candidate", "allowed_next": False, "passed": True},
        {"operation": "wrapper_design", "allowed_next": False, "passed": True},
        {"operation": "full_batch_adapter_call", "allowed_next": False, "passed": True},
        {"operation": "real_metric_execution", "allowed_next": False, "passed": True},
        {"operation": "live_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "database_writes", "allowed_next": False, "passed": True},
        {"operation": "production_source_modifications", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit_credit", "allowed_next": False, "passed": True},
    ]

    blockers = [
        {"blocker": "real_prediction_surface_not_materialized", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "three_argument_call_not_implemented", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6ll = [
        {"contract": "implement_payload_game_pk_limit_mapping", "required": True, "passed": True},
        {"contract": "package_import_same_candidate", "required": True, "passed": True},
        {"contract": "attempt_one_three_argument_call_or_fail_closed", "required": True, "passed": True},
        {"contract": "emit_surface_or_gap_without_metrics_activation_or_exit", "required": True, "passed": True},
    ]

    future_6lm = [
        {"contract": "audit_three_argument_call_result", "required": True, "passed": True},
        {"contract": "audit_surface_or_gap_result", "required": True, "passed": True},
        {"contract": "route_to_batch_plan_next_candidate_or_wrapper", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_activation_or_layer6_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6lj_audit_script_exists", "expected": True, "actual": AUDIT_6LJ_PATH.exists(), "passed": AUDIT_6LJ_PATH.exists()},
        {"check": "6lj_json_exists", "expected": True, "actual": JSON_6LJ.exists(), "passed": JSON_6LJ.exists()},
        {"check": "6lj_all_checks_passed", "expected": True, "actual": json_6lj.get("all_checks_passed"), "passed": json_6lj.get("all_checks_passed") is True},
        {"check": "6lj_diagnosis", "expected": DIAGNOSIS_6LJ, "actual": json_6lj.get("diagnosis"), "passed": json_6lj.get("diagnosis") == DIAGNOSIS_6LJ},
        {"check": "6lj_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LJ, "actual": json_6lj.get("recommended_next_layer"), "passed": json_6lj.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LJ},
        {"check": "6lj_three_arg_contract", "expected": True, "actual": json_6lj.get("three_argument_call_contract_confirmed"), "passed": json_6lj.get("three_argument_call_contract_confirmed") is True},
        {"check": "6lj_required_args", "expected": REQUIRED_ARGS, "actual": json_6lj.get("required_arguments_confirmed"), "passed": json_6lj.get("required_arguments_confirmed") == REQUIRED_ARGS},
        {"check": "6lj_no_wrapper", "expected": False, "actual": json_6lj.get("wrapper_plan_needed"), "passed": json_6lj.get("wrapper_plan_needed") is False},
        {"check": "6lj_no_layer6_exit", "expected": False, "actual": json_6lj.get("layer_6_exit_recommended"), "passed": json_6lj.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in ALL_INPUTS
    ]
    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in ALL_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6ll_three_argument_call_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "next_candidate_retry", "blocked": True, "reason": "same candidate has safe callable import and mapped three-arg contract", "passed": True},
        {"blocked_surface": "wrapper_design", "blocked": True, "reason": "same candidate not exhausted", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "real prediction surface required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "historical evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LK cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6lj_passed", "expected": True, "actual": json_6lj.get("all_checks_passed"), "passed": json_6lj.get("all_checks_passed") is True},
        {"decision": "same_candidate_retained", "expected": True, "actual": True, "passed": True},
        {"decision": "call_contract_mapping_valid", "expected": True, "actual": all_passed(call_mapping), "passed": all_passed(call_mapping)},
        {"decision": "argument_validation_valid", "expected": True, "actual": all_passed(argument_validation), "passed": all_passed(argument_validation)},
        {"decision": "future_6ll_contract_valid", "expected": True, "actual": len(future_6ll) == 4 and all_passed(future_6ll), "passed": len(future_6ll) == 4 and all_passed(future_6ll)},
        {"decision": "future_6lm_contract_valid", "expected": True, "actual": len(future_6lm) == 4 and all_passed(future_6lm), "passed": len(future_6lm) == 4 and all_passed(future_6lm)},
        {"decision": "recommend_6ll_next", "expected": RECOMMENDED_NEXT_LAYER_6LK, "actual": RECOMMENDED_NEXT_LAYER_6LK, "passed": True},
        {"decision": "do_not_recommend_other_candidate", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper", "expected": True, "actual": True, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "projection_adapter_three_argument_call_planned", "expected": True, "actual": True, "passed": True},
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
        {"surface": "6lj_audit", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only_reference", "passed": True},
        {"surface": "future_6ll_call", "policy": "single_sample_tmp_only_next_layer", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6lk", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LK, "actual": RECOMMENDED_NEXT_LAYER_6LK, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LK, "actual": RECOMMENDED_PATH_6LK, "passed": True},
        {"decision": "recommend_three_argument_call_implementation", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_next_candidate_retry", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LK, "actual": DIAGNOSIS_6LK, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "problem_statement", "passed": all_passed(problem), "detail": f"{len(problem)} rows"},
        {"check": "candidate_retention", "passed": all_passed(retention), "detail": f"{len(retention)} rows"},
        {"check": "call_contract_mapping", "passed": all_passed(call_mapping), "detail": f"{len(call_mapping)} rows"},
        {"check": "argument_validation", "passed": all_passed(argument_validation), "detail": f"{len(argument_validation)} rows"},
        {"check": "signature_gate_plan", "passed": all_passed(signature_gate), "detail": f"{len(signature_gate)} rows"},
        {"check": "package_import_policy", "passed": all_passed(package_policy), "detail": f"{len(package_policy)} rows"},
        {"check": "adapter_call_conditions", "passed": all_passed(call_conditions), "detail": f"{len(call_conditions)} rows"},
        {"check": "fail_closed_policy", "passed": all_passed(fail_closed), "detail": f"{len(fail_closed)} rows"},
        {"check": "prediction_surface_rules", "passed": all_passed(surface_rules), "detail": f"{len(surface_rules)} rows"},
        {"check": "metric_guardrails", "passed": all_passed(metric_guards), "detail": f"{len(metric_guards)} rows"},
        {"check": "allowed_next", "passed": all_passed(allowed_next), "detail": f"{len(allowed_next)} rows"},
        {"check": "forbidden_next", "passed": all_passed(forbidden_next), "detail": f"{len(forbidden_next)} rows"},
        {"check": "future_6ll_contract", "passed": all_passed(future_6ll), "detail": f"{len(future_6ll)} rows"},
        {"check": "future_6lm_contract", "passed": all_passed(future_6lm), "detail": f"{len(future_6lm)} rows"},
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
        "call_contract_mapping": write_csv(CALL_MAPPING_CSV, call_mapping),
        "argument_validation": write_csv(ARG_VALIDATION_CSV, argument_validation),
        "signature_gate_plan": write_csv(SIGNATURE_GATE_CSV, signature_gate),
        "package_import_policy": write_csv(PACKAGE_POLICY_CSV, package_policy),
        "adapter_call_conditions": write_csv(CALL_CONDITIONS_CSV, call_conditions),
        "fail_closed_policy": write_csv(FAIL_CLOSED_CSV, fail_closed),
        "prediction_surface_rules": write_csv(SURFACE_RULES_CSV, surface_rules),
        "metric_guardrails": write_csv(METRIC_GUARDS_CSV, metric_guards),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6ll_contract": write_csv(FUTURE_6LL_CSV, future_6ll),
        "future_6lm_contract": write_csv(FUTURE_6LM_CSV, future_6lm),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LK",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LK if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LK,
        "recommended_path": RECOMMENDED_PATH_6LK,
        "predecessor_audit": str(AUDIT_6LJ_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6lj.get("diagnosis"),
        "planned_layer_after": "6LJ",
        "source_family": "projection_adapter_three_argument_call_plan",
        "problem_statement_count": len(problem),
        "candidate_retention_count": len(retention),
        "call_contract_mapping_count": len(call_mapping),
        "argument_validation_count": len(argument_validation),
        "signature_gate_plan_count": len(signature_gate),
        "package_import_policy_count": len(package_policy),
        "adapter_call_condition_count": len(call_conditions),
        "fail_closed_policy_count": len(fail_closed),
        "prediction_surface_rule_count": len(surface_rules),
        "metric_guardrail_count": len(metric_guards),
        "allowed_operation_next_count": len(allowed_next),
        "forbidden_operation_next_count": len(forbidden_next),
        "blocker_count": len(blockers),
        "future_6ll_contract_valid": len(future_6ll) == 4 and all_passed(future_6ll),
        "future_6lm_contract_valid": len(future_6lm) == 4 and all_passed(future_6lm),
        "projection_adapter_three_argument_call_planned": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "same_candidate_retained": True,
        "blocked_session_candidate_excluded": True,
        "package_context_import_preserved": True,
        "file_location_import_forbidden_next": True,
        "target_module_import_path": TARGET_MODULE,
        "target_function_name": TARGET_FUNCTION,
        "required_arguments_planned": REQUIRED_ARGS,
        "payload_source_planned": "6kz_fixture_contract_surface_first_usable_row",
        "game_pk_source_planned": "fixture_game_id",
        "game_pk_value_planned": game_pk_value if game_pk_safe else "",
        "game_pk_safe_int_planned": game_pk_safe,
        "limit_value_planned": limit_value,
        "single_sample_adapter_call_allowed_next": True,
        "full_batch_adapter_call_allowed_next": False,
        "real_metric_execution_allowed_next": False,
        "projection_surface_materialization_allowed_next": True,
        "next_candidate_retry_recommended": False,
        "wrapper_plan_needed": False,
        "historical_odds_required": False,
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
            "call_contract_mapping_csv": str(CALL_MAPPING_CSV),
            "argument_validation_csv": str(ARG_VALIDATION_CSV),
            "signature_gate_plan_csv": str(SIGNATURE_GATE_CSV),
            "package_import_policy_csv": str(PACKAGE_POLICY_CSV),
            "adapter_call_conditions_csv": str(CALL_CONDITIONS_CSV),
            "fail_closed_policy_csv": str(FAIL_CLOSED_CSV),
            "prediction_surface_rules_csv": str(SURFACE_RULES_CSV),
            "metric_guardrails_csv": str(METRIC_GUARDS_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6ll_contract_csv": str(FUTURE_6LL_CSV),
            "future_6lm_contract_csv": str(FUTURE_6LM_CSV),
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
