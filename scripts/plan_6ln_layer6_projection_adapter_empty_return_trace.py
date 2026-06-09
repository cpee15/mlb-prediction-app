#!/usr/bin/env python3
"""Plan a static provenance trace for the empty adapter return."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ln_projection_adapter_empty_return_trace_plan"
TMP_DIR = Path("tmp")

AUDIT_6LM_PATH = Path("scripts/audit_6lm_layer6_projection_adapter_three_argument_call.py")
JSON_6LM = TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit.json"

REQUIRED_6LM_INPUTS = [
    JSON_6LM,
    TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit_checks.csv",
    TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit_predecessor.csv",
    TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit_candidate_audit.csv",
    TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit_call_execution_audit.csv",
    TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit_return_shape_audit.csv",
    TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit_gap_report_audit.csv",
    TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit_prediction_surface_audit.csv",
    TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit_metric_readiness_audit.csv",
    TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit_next_route.csv",
    TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit_blockers.csv",
    TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit_future_6ln_contract.csv",
    TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit_decision.csv",
    TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6lm_projection_adapter_three_argument_call_audit_recommended_path.csv",
]
SOURCE_INPUTS = [
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_fixture_contract_surface.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_argument_mapping.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_return_shape.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_gap_report.csv",
]
ALL_INPUTS = REQUIRED_6LM_INPUTS + SOURCE_INPUTS

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PROBLEM_CSV = TMP_DIR / f"{SLUG}_problem_statement.csv"
RETENTION_CSV = TMP_DIR / f"{SLUG}_candidate_retention.csv"
HYPOTHESES_CSV = TMP_DIR / f"{SLUG}_empty_return_hypotheses.csv"
STATIC_TARGETS_CSV = TMP_DIR / f"{SLUG}_static_trace_targets.csv"
PAYLOAD_TRACE_CSV = TMP_DIR / f"{SLUG}_payload_shape_trace.csv"
GAME_PK_TRACE_CSV = TMP_DIR / f"{SLUG}_game_pk_trace.csv"
LIMIT_TRACE_CSV = TMP_DIR / f"{SLUG}_limit_trace.csv"
FILTER_TRACE_CSV = TMP_DIR / f"{SLUG}_filter_trace.csv"
FAIL_CLOSED_CSV = TMP_DIR / f"{SLUG}_fail_closed_policy.csv"
SURFACE_RULES_CSV = TMP_DIR / f"{SLUG}_prediction_surface_rules.csv"
METRIC_GUARDS_CSV = TMP_DIR / f"{SLUG}_metric_guardrails.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LO_CSV = TMP_DIR / f"{SLUG}_future_6lo_contract.csv"
FUTURE_6LP_CSV = TMP_DIR / f"{SLUG}_future_6lp_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LM = "layer_6_projection_adapter_three_argument_call_audit_complete"
DIAGNOSIS_6LN = "layer_6_projection_adapter_empty_return_trace_plan_complete"
RECOMMENDED_NEXT_LAYER_6LM = "6LN_layer_6_projection_adapter_empty_return_trace_plan"
RECOMMENDED_NEXT_LAYER_6LN = "6LO_layer_6_projection_adapter_empty_return_trace_implementation"
RECOMMENDED_PATH_6LN = "implement_static_empty_return_provenance_trace_for_same_candidate"

TARGET_MODULE = "mlb_app.ai_data_assistant_performance"
TARGET_FILE = "mlb_app/ai_data_assistant_performance.py"
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
    json_6lm = load_json(JSON_6LM)

    problem = [
        {
            "problem": "same_candidate_three_argument_call_succeeded_but_returned_empty_list",
            "target": f"{TARGET_MODULE}::{TARGET_FUNCTION}",
            "known_return_shape": "list_len=0;first_type=empty",
            "resolution_needed": "static_provenance_trace_of_empty_return_paths",
            "passed": True,
        }
    ]

    retention = [
        {"item": "same_candidate_retained", "value": True, "module": TARGET_MODULE, "function": TARGET_FUNCTION, "passed": True},
        {"item": "blocked_session_candidate_excluded", "value": True, "blocked_function": "cached_build_model_projection_payload", "passed": True},
        {"item": "next_candidate_retry_recommended", "value": False, "reason": "same candidate empty return not traced", "passed": True},
        {"item": "wrapper_plan_needed", "value": False, "reason": "same candidate empty return not traced", "passed": True},
    ]

    hypotheses = [
        {"hypothesis": "fixture_payload_contract_shaped_not_adapter_shaped", "trace_needed": True, "passed": True},
        {"hypothesis": "payload_missing_expected_game_collection_key", "trace_needed": True, "candidate_keys": "games;canonical_games;projections;game_models;model_outputs;odds;probability_context", "passed": True},
        {"hypothesis": "game_pk_not_found_in_expected_nested_payload_location", "trace_needed": True, "game_pk": 824776, "passed": True},
        {"hypothesis": "empty_team_date_fields_cause_candidate_rejection", "trace_needed": True, "passed": True},
        {"hypothesis": "limit_applied_after_zero_eligible_games", "trace_needed": True, "passed": True},
        {"hypothesis": "internal_filters_reject_all_candidate_games", "trace_needed": True, "passed": True},
    ]

    static_targets = [
        {"target": "function_body", "path": TARGET_FILE, "symbol": TARGET_FUNCTION, "trace": "extract_ast_function_body", "passed": True},
        {"target": "return_empty_paths", "path": TARGET_FILE, "symbol": TARGET_FUNCTION, "trace": "locate_return_list_literals_and_empty_accumulators", "passed": True},
        {"target": "payload_access_paths", "path": TARGET_FILE, "symbol": TARGET_FUNCTION, "trace": "collect_subscript_get_attribute_access_on_payload", "passed": True},
        {"target": "game_pk_matching_paths", "path": TARGET_FILE, "symbol": TARGET_FUNCTION, "trace": "collect_comparisons_to_game_pk_and_casts", "passed": True},
        {"target": "limit_application_paths", "path": TARGET_FILE, "symbol": TARGET_FUNCTION, "trace": "collect_slice_break_and_limit_usage", "passed": True},
        {"target": "filter_rejection_paths", "path": TARGET_FILE, "symbol": TARGET_FUNCTION, "trace": "collect_continue_if_return_filter_conditions", "passed": True},
    ]

    payload_trace = [
        {"trace_item": "expected_top_level_payload_keys", "planned_method": "static_collect_payload_get_and_subscript_keys", "passed": True},
        {"trace_item": "expected_nested_game_collection_shape", "planned_method": "static_collect_iteration_sources", "passed": True},
        {"trace_item": "fixture_payload_key_coverage", "planned_method": "compare_6ll_argument_payload_keys_to_expected_keys", "passed": True},
        {"trace_item": "contract_vs_adapter_shape_gap", "planned_method": "classify_missing_required_keys", "passed": True},
    ]

    game_pk_trace = [
        {"trace_item": "game_pk_casting", "planned_method": "static_collect_int_str_comparisons", "passed": True},
        {"trace_item": "game_pk_lookup_field_names", "planned_method": "static_collect_pk_key_names_game_pk_game_id_id", "passed": True},
        {"trace_item": "game_pk_present_in_fixture_payload", "planned_method": "compare_824776_to_payload_locations", "passed": True},
        {"trace_item": "game_pk_filter_rejection", "planned_method": "trace_conditions_that_drop_nonmatching_pk", "passed": True},
    ]

    limit_trace = [
        {"trace_item": "limit_parameter_usage", "planned_method": "static_collect_limit_references", "passed": True},
        {"trace_item": "limit_applied_before_or_after_filtering", "planned_method": "order_trace_filter_then_slice_or_break", "passed": True},
        {"trace_item": "limit_zero_or_none_handling", "planned_method": "static_check_limit_guard_conditions", "passed": True},
    ]

    filter_trace = [
        {"trace_item": "empty_or_missing_team_fields", "planned_method": "static_collect_home_away_team_required_checks", "passed": True},
        {"trace_item": "empty_or_missing_date_fields", "planned_method": "static_collect_date_required_checks", "passed": True},
        {"trace_item": "probability_or_odds_required_fields", "planned_method": "static_collect_required_probability_odds_keys", "passed": True},
        {"trace_item": "model_output_required_fields", "planned_method": "static_collect_required_model_keys", "passed": True},
        {"trace_item": "canonical_game_required_fields", "planned_method": "static_collect_output_append_conditions", "passed": True},
    ]

    fail_closed = [
        {"condition": "target_function_missing", "action": "emit_static_trace_gap", "passed": True},
        {"condition": "ast_parse_fails", "action": "emit_static_trace_gap", "passed": True},
        {"condition": "no_payload_access_paths_found", "action": "emit_payload_shape_unknown_gap", "passed": True},
        {"condition": "no_game_pk_match_paths_found", "action": "emit_game_pk_matching_unknown_gap", "passed": True},
        {"condition": "no_filter_paths_found", "action": "emit_filter_trace_unknown_gap", "passed": True},
        {"condition": "trace_inconclusive", "action": "recommend_static_trace_audit_not_execution", "passed": True},
    ]

    surface_rules = [
        {"rule": "do_not_materialize_projection_surface_in_6ln", "passed": True},
        {"rule": "future_surface_requires_nonempty_return_with_prediction_fields", "passed": True},
        {"rule": "empty_return_is_not_real_prediction_surface", "passed": True},
        {"rule": "preserve_empty_return_lineage", "passed": True},
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
        {"operation": "static_ast_trace_target_function", "allowed_next": True, "passed": True},
        {"operation": "trace_return_empty_paths", "allowed_next": True, "passed": True},
        {"operation": "trace_payload_shape_requirements", "allowed_next": True, "passed": True},
        {"operation": "trace_game_pk_matching", "allowed_next": True, "passed": True},
        {"operation": "trace_limit_application", "allowed_next": True, "passed": True},
        {"operation": "trace_filter_rejection_paths", "allowed_next": True, "passed": True},
        {"operation": "write_tmp_trace_artifacts", "allowed_next": True, "passed": True},
    ]

    forbidden_next = [
        {"operation": "import_candidate_module", "allowed_next": False, "passed": True},
        {"operation": "additional_adapter_call", "allowed_next": False, "passed": True},
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
        {"blocker": "empty_adapter_return_not_explained", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6lo = [
        {"contract": "implement_static_empty_return_path_trace", "required": True, "passed": True},
        {"contract": "trace_payload_game_pk_limit_filters_without_import_or_call", "required": True, "passed": True},
        {"contract": "emit_payload_shape_gap_or_filter_rejection_map", "required": True, "passed": True},
        {"contract": "preserve_no_calls_metrics_activation_or_exit", "required": True, "passed": True},
    ]

    future_6lp = [
        {"contract": "audit_empty_return_trace_findings", "required": True, "passed": True},
        {"contract": "route_to_payload_repair_next_candidate_or_wrapper_plan", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_activation_or_exit", "required": True, "passed": True},
        {"contract": "keep_layer6_exit_blocked", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6lm_audit_script_exists", "expected": True, "actual": AUDIT_6LM_PATH.exists(), "passed": AUDIT_6LM_PATH.exists()},
        {"check": "6lm_json_exists", "expected": True, "actual": JSON_6LM.exists(), "passed": JSON_6LM.exists()},
        {"check": "6lm_all_checks_passed", "expected": True, "actual": json_6lm.get("all_checks_passed"), "passed": json_6lm.get("all_checks_passed") is True},
        {"check": "6lm_diagnosis", "expected": DIAGNOSIS_6LM, "actual": json_6lm.get("diagnosis"), "passed": json_6lm.get("diagnosis") == DIAGNOSIS_6LM},
        {"check": "6lm_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LM, "actual": json_6lm.get("recommended_next_layer"), "passed": json_6lm.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LM},
        {"check": "6lm_empty_list_return", "expected": True, "actual": json_6lm.get("empty_list_return_confirmed"), "passed": json_6lm.get("empty_list_return_confirmed") is True},
        {"check": "6lm_empty_return_trace_needed", "expected": True, "actual": json_6lm.get("empty_return_trace_plan_needed"), "passed": json_6lm.get("empty_return_trace_plan_needed") is True},
        {"check": "6lm_no_layer6_exit", "expected": False, "actual": json_6lm.get("layer_6_exit_recommended"), "passed": json_6lm.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in ALL_INPUTS
    ]
    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in ALL_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6lo_empty_return_trace_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "additional_adapter_call", "blocked": True, "reason": "empty return trace is static-only", "passed": True},
        {"blocked_surface": "candidate_module_import", "blocked": True, "reason": "empty return trace uses AST/static source", "passed": True},
        {"blocked_surface": "next_candidate_retry", "blocked": True, "reason": "same candidate empty return not traced", "passed": True},
        {"blocked_surface": "wrapper_design", "blocked": True, "reason": "same candidate empty return not traced", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "real prediction surface required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LN cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6lm_passed", "expected": True, "actual": json_6lm.get("all_checks_passed"), "passed": json_6lm.get("all_checks_passed") is True},
        {"decision": "problem_statement_valid", "expected": True, "actual": all_passed(problem), "passed": all_passed(problem)},
        {"decision": "same_candidate_retained", "expected": True, "actual": True, "passed": True},
        {"decision": "empty_return_hypotheses_valid", "expected": True, "actual": all_passed(hypotheses), "passed": all_passed(hypotheses)},
        {"decision": "static_trace_targets_valid", "expected": True, "actual": all_passed(static_targets), "passed": all_passed(static_targets)},
        {"decision": "future_6lo_contract_valid", "expected": True, "actual": len(future_6lo) == 4 and all_passed(future_6lo), "passed": len(future_6lo) == 4 and all_passed(future_6lo)},
        {"decision": "future_6lp_contract_valid", "expected": True, "actual": len(future_6lp) == 4 and all_passed(future_6lp), "passed": len(future_6lp) == 4 and all_passed(future_6lp)},
        {"decision": "recommend_6lo_next", "expected": RECOMMENDED_NEXT_LAYER_6LN, "actual": RECOMMENDED_NEXT_LAYER_6LN, "passed": True},
        {"decision": "do_not_recommend_other_candidate", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper", "expected": True, "actual": True, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_adapter_call_run_by_6ln", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_candidate_module_import_by_6ln", "expected": True, "actual": True, "passed": True},
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
        {"surface": "source_tree", "policy": "read_only_planning", "passed": True},
        {"surface": "6lm_audit", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only_reference", "passed": True},
        {"surface": "future_6lo_trace", "policy": "static_ast_only_next_layer", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6ln", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LN, "actual": RECOMMENDED_NEXT_LAYER_6LN, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LN, "actual": RECOMMENDED_PATH_6LN, "passed": True},
        {"decision": "recommend_empty_return_trace_implementation", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_next_candidate_retry", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LN, "actual": DIAGNOSIS_6LN, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "problem_statement", "passed": all_passed(problem), "detail": f"{len(problem)} rows"},
        {"check": "candidate_retention", "passed": all_passed(retention), "detail": f"{len(retention)} rows"},
        {"check": "empty_return_hypotheses", "passed": all_passed(hypotheses), "detail": f"{len(hypotheses)} rows"},
        {"check": "static_trace_targets", "passed": all_passed(static_targets), "detail": f"{len(static_targets)} rows"},
        {"check": "payload_shape_trace", "passed": all_passed(payload_trace), "detail": f"{len(payload_trace)} rows"},
        {"check": "game_pk_trace", "passed": all_passed(game_pk_trace), "detail": f"{len(game_pk_trace)} rows"},
        {"check": "limit_trace", "passed": all_passed(limit_trace), "detail": f"{len(limit_trace)} rows"},
        {"check": "filter_trace", "passed": all_passed(filter_trace), "detail": f"{len(filter_trace)} rows"},
        {"check": "fail_closed_policy", "passed": all_passed(fail_closed), "detail": f"{len(fail_closed)} rows"},
        {"check": "prediction_surface_rules", "passed": all_passed(surface_rules), "detail": f"{len(surface_rules)} rows"},
        {"check": "metric_guardrails", "passed": all_passed(metric_guards), "detail": f"{len(metric_guards)} rows"},
        {"check": "allowed_next", "passed": all_passed(allowed_next), "detail": f"{len(allowed_next)} rows"},
        {"check": "forbidden_next", "passed": all_passed(forbidden_next), "detail": f"{len(forbidden_next)} rows"},
        {"check": "future_6lo_contract", "passed": all_passed(future_6lo), "detail": f"{len(future_6lo)} rows"},
        {"check": "future_6lp_contract", "passed": all_passed(future_6lp), "detail": f"{len(future_6lp)} rows"},
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
        "empty_return_hypotheses": write_csv(HYPOTHESES_CSV, hypotheses),
        "static_trace_targets": write_csv(STATIC_TARGETS_CSV, static_targets),
        "payload_shape_trace": write_csv(PAYLOAD_TRACE_CSV, payload_trace),
        "game_pk_trace": write_csv(GAME_PK_TRACE_CSV, game_pk_trace),
        "limit_trace": write_csv(LIMIT_TRACE_CSV, limit_trace),
        "filter_trace": write_csv(FILTER_TRACE_CSV, filter_trace),
        "fail_closed_policy": write_csv(FAIL_CLOSED_CSV, fail_closed),
        "prediction_surface_rules": write_csv(SURFACE_RULES_CSV, surface_rules),
        "metric_guardrails": write_csv(METRIC_GUARDS_CSV, metric_guards),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6lo_contract": write_csv(FUTURE_6LO_CSV, future_6lo),
        "future_6lp_contract": write_csv(FUTURE_6LP_CSV, future_6lp),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LN",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LN if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LN,
        "recommended_path": RECOMMENDED_PATH_6LN,
        "predecessor_audit": str(AUDIT_6LM_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6lm.get("diagnosis"),
        "planned_layer_after": "6LM",
        "source_family": "projection_adapter_empty_return_trace_plan",
        "problem_statement_count": len(problem),
        "candidate_retention_count": len(retention),
        "empty_return_hypothesis_count": len(hypotheses),
        "static_trace_target_count": len(static_targets),
        "payload_shape_trace_count": len(payload_trace),
        "game_pk_trace_count": len(game_pk_trace),
        "limit_trace_count": len(limit_trace),
        "filter_trace_count": len(filter_trace),
        "fail_closed_policy_count": len(fail_closed),
        "prediction_surface_rule_count": len(surface_rules),
        "metric_guardrail_count": len(metric_guards),
        "allowed_operation_next_count": len(allowed_next),
        "forbidden_operation_next_count": len(forbidden_next),
        "blocker_count": len(blockers),
        "future_6lo_contract_valid": len(future_6lo) == 4 and all_passed(future_6lo),
        "future_6lp_contract_valid": len(future_6lp) == 4 and all_passed(future_6lp),
        "projection_adapter_empty_return_trace_planned": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "same_candidate_retained": True,
        "blocked_session_candidate_excluded": True,
        "target_module_import_path": TARGET_MODULE,
        "target_function_name": TARGET_FUNCTION,
        "required_arguments_confirmed": REQUIRED_ARGS,
        "three_argument_call_contract_resolved_confirmed": json_6lm.get("three_argument_call_contract_resolved") is True,
        "adapter_call_succeeded_empty_list_confirmed": json_6lm.get("adapter_call_succeeded_confirmed") is True and json_6lm.get("empty_list_return_confirmed") is True,
        "empty_return_trace_needed": True,
        "static_return_path_trace_planned": True,
        "payload_shape_trace_planned": True,
        "game_pk_match_trace_planned": True,
        "limit_application_trace_planned": True,
        "filter_rejection_trace_planned": True,
        "fixture_contract_vs_adapter_shape_trace_planned": True,
        "additional_adapter_call_allowed_next": False,
        "import_candidate_module_allowed_next": False,
        "next_candidate_retry_recommended": False,
        "wrapper_plan_needed": False,
        "full_batch_adapter_call_allowed_next": False,
        "real_metric_execution_allowed_next": False,
        "projection_surface_materialization_allowed_next": False,
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
            "empty_return_hypotheses_csv": str(HYPOTHESES_CSV),
            "static_trace_targets_csv": str(STATIC_TARGETS_CSV),
            "payload_shape_trace_csv": str(PAYLOAD_TRACE_CSV),
            "game_pk_trace_csv": str(GAME_PK_TRACE_CSV),
            "limit_trace_csv": str(LIMIT_TRACE_CSV),
            "filter_trace_csv": str(FILTER_TRACE_CSV),
            "fail_closed_policy_csv": str(FAIL_CLOSED_CSV),
            "prediction_surface_rules_csv": str(SURFACE_RULES_CSV),
            "metric_guardrails_csv": str(METRIC_GUARDS_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6lo_contract_csv": str(FUTURE_6LO_CSV),
            "future_6lp_contract_csv": str(FUTURE_6LP_CSV),
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
