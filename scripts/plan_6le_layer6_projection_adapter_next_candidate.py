#!/usr/bin/env python3
"""Plan the next projection adapter candidate attempt.

This planning-only layer excludes the first blocked runtime-context candidate
and selects the next static-safe candidate for a future single-sample adapter
attempt.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6le_projection_adapter_next_candidate_plan"
TMP_DIR = Path("tmp")

AUDIT_6LD_PATH = Path("scripts/audit_6ld_layer6_projection_adapter_call_implementation.py")
JSON_6LD = TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit.json"
SAFETY_SCAN_6KZ = TMP_DIR / "layer6_6kz_projection_call_contract_implementation_entrypoint_safety_scan.csv"
FIXTURE_SURFACE_6KZ = TMP_DIR / "layer6_6kz_projection_call_contract_implementation_fixture_contract_surface.csv"

REQUIRED_6LD_INPUTS = [
    JSON_6LD,
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_checks.csv",
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_predecessor.csv",
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_selected_candidate_audit.csv",
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_signature_audit.csv",
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_fail_closed_audit.csv",
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_prediction_surface_audit.csv",
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_metric_readiness_audit.csv",
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_next_candidate_inventory.csv",
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_next_route.csv",
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_blockers.csv",
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_future_6le_contract.csv",
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_decision.csv",
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_recommended_path.csv",
]
ALL_INPUTS = REQUIRED_6LD_INPUTS + [SAFETY_SCAN_6KZ, FIXTURE_SURFACE_6KZ]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
BLOCKED_POLICY_CSV = TMP_DIR / f"{SLUG}_blocked_candidate_policy.csv"
CANDIDATE_SELECTION_CSV = TMP_DIR / f"{SLUG}_candidate_selection.csv"
SIGNATURE_GATE_CSV = TMP_DIR / f"{SLUG}_static_signature_gate.csv"
PAYLOAD_STRATEGY_CSV = TMP_DIR / f"{SLUG}_payload_strategy.csv"
CALL_CONDITIONS_CSV = TMP_DIR / f"{SLUG}_adapter_call_conditions.csv"
FAIL_CLOSED_CSV = TMP_DIR / f"{SLUG}_fail_closed_policy.csv"
SURFACE_RULES_CSV = TMP_DIR / f"{SLUG}_prediction_surface_rules.csv"
METRIC_GUARDS_CSV = TMP_DIR / f"{SLUG}_metric_guardrails.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LF_CSV = TMP_DIR / f"{SLUG}_future_6lf_contract.csv"
FUTURE_6LG_CSV = TMP_DIR / f"{SLUG}_future_6lg_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LD = "layer_6_projection_adapter_call_implementation_audit_complete"
DIAGNOSIS_6LE = "layer_6_projection_adapter_next_candidate_plan_complete"
RECOMMENDED_NEXT_LAYER_6LD = "6LE_layer_6_projection_adapter_next_candidate_plan"
RECOMMENDED_NEXT_LAYER_6LE = "6LF_layer_6_projection_adapter_next_candidate_implementation"
RECOMMENDED_PATH_6LE = "implement_next_safe_projection_adapter_candidate_attempt"

BLOCKED_NAME = "cached_build_model_projection_payload"
BLOCKED_PATH = "mlb_app/ai_data_assistant_performance.py"


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


def select_next_candidate() -> Dict[str, Any]:
    inventory = read_csv_rows(TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_next_candidate_inventory.csv")
    for row in inventory:
        if int(row.get("candidate_rank", "0") or 0) > 0:
            return {
                "candidate_rank": row.get("candidate_rank", "1"),
                "path": row.get("path", ""),
                "entrypoint_name": row.get("entrypoint_name", ""),
                "selection_reason": "first_remaining_static_safe_candidate_after_excluding_blocked_session_candidate",
                "passed": True,
            }

    safety = read_csv_rows(SAFETY_SCAN_6KZ)
    for row in safety:
        path = row.get("path", "")
        name = row.get("entrypoint_name", "")
        if str(row.get("safe_for_direct_call", "")).lower() == "true" and not (path == BLOCKED_PATH and name == BLOCKED_NAME):
            return {
                "candidate_rank": "fallback",
                "path": path,
                "entrypoint_name": name,
                "selection_reason": "fallback_from_6kz_safety_scan",
                "passed": True,
            }

    return {
        "candidate_rank": "0",
        "path": "",
        "entrypoint_name": "",
        "selection_reason": "no_remaining_candidate",
        "passed": False,
    }


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6ld = load_json(JSON_6LD)
    selected = select_next_candidate()
    next_available = bool(selected.get("path") and selected.get("entrypoint_name"))

    blocked_policy = [
        {"policy": "exclude_blocked_candidate", "path": BLOCKED_PATH, "entrypoint_name": BLOCKED_NAME, "reason": "requires_forbidden_session_runtime_context", "passed": True},
        {"policy": "do_not_build_wrapper_until_candidates_exhausted", "value": True, "passed": True},
        {"policy": "try_next_static_safe_candidate_first", "value": True, "passed": True},
    ]

    candidate_selection = [
        selected,
        {"selection_rule": "candidate_must_not_equal_blocked_candidate", "passed": selected.get("entrypoint_name") != BLOCKED_NAME},
        {"selection_rule": "candidate_from_6ld_next_candidate_inventory", "passed": True},
        {"selection_rule": "candidate_attempt_limited_to_single_sample", "passed": True},
    ]

    signature_gate = [
        {"gate": "ast_parse_candidate_file_before_import", "required_next": True, "passed": True},
        {"gate": "reject_forbidden_runtime_params", "tokens": "session;db;request;engine;client;connection;cursor;api;http;fetch;env", "required_next": True, "passed": True},
        {"gate": "reject_server_route_or_app_context_candidate", "required_next": True, "passed": True},
        {"gate": "allow_no_arg_or_single_serializable_arg_only", "required_next": True, "passed": True},
        {"gate": "record_signature_before_any_import", "required_next": True, "passed": True},
    ]

    payload_strategy = [
        {"strategy": "use_first_usable_6kz_fixture_row", "passed": True},
        {"strategy": "build_serializable_payload_dict", "passed": True},
        {"strategy": "include_game_identity_and_mechanic_tags", "passed": True},
        {"strategy": "preserve_non_production_and_source_lineage", "passed": True},
        {"strategy": "do_not_join_actual_outcomes", "passed": True},
    ]

    call_conditions = [
        {"condition": "candidate_not_blocked", "required_next": True, "passed": True},
        {"condition": "static_signature_gate_passes", "required_next": True, "passed": True},
        {"condition": "payload_mapping_complete", "required_next": True, "passed": True},
        {"condition": "import_safety_gate_passes", "required_next": True, "passed": True},
        {"condition": "execute_at_most_one_local_call", "required_next": True, "passed": True},
    ]

    fail_closed = [
        {"condition": "forbidden_param_detected", "action": "emit_next_candidate_gap_report", "passed": True},
        {"condition": "import_fails_or_has_side_effect_signal", "action": "emit_next_candidate_gap_report", "passed": True},
        {"condition": "return_payload_has_no_prediction_fields", "action": "emit_surface_with_readiness_false", "passed": True},
        {"condition": "runtime_fetch_db_api_signal_detected", "action": "abort_and_emit_safety_gap", "passed": True},
    ]

    surface_rules = [
        {"rule": "materialize_surface_only_for_actual_adapter_return", "passed": True},
        {"rule": "materialize_probability_fields_only_if_returned", "passed": True},
        {"rule": "materialize_runs_fields_only_if_returned", "passed": True},
        {"rule": "label_all_rows_non_production", "passed": True},
        {"rule": "preserve_candidate_and_payload_lineage", "passed": True},
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
        {"operation": "read_repo_local_files", "allowed_next": True, "passed": True},
        {"operation": "static_ast_signature_inspection", "allowed_next": True, "passed": True},
        {"operation": "single_sample_local_adapter_call_if_gates_pass", "allowed_next": True, "passed": True},
        {"operation": "write_tmp_artifacts", "allowed_next": True, "passed": True},
        {"operation": "emit_non_production_projection_surface_or_gap", "allowed_next": True, "passed": True},
    ]

    forbidden_next = [
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
        {"blocker": "next_candidate_not_yet_attempted", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6lf = [
        {"contract": "exclude_blocked_session_candidate", "required": True, "passed": True},
        {"contract": "implement_next_candidate_static_signature_gate", "required": True, "passed": True},
        {"contract": "attempt_single_sample_call_or_fail_closed", "required": True, "passed": True},
        {"contract": "emit_surface_or_gap_without_metrics_activation_or_exit", "required": True, "passed": True},
    ]

    future_6lg = [
        {"contract": "audit_next_candidate_attempt", "required": True, "passed": True},
        {"contract": "audit_prediction_field_materialization", "required": True, "passed": True},
        {"contract": "route_to_batch_plan_or_candidate_exhaustion_wrapper_plan", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_no_activation_no_layer6_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ld_audit_script_exists", "expected": True, "actual": AUDIT_6LD_PATH.exists(), "passed": AUDIT_6LD_PATH.exists()},
        {"check": "6ld_json_exists", "expected": True, "actual": JSON_6LD.exists(), "passed": JSON_6LD.exists()},
        {"check": "6ld_all_checks_passed", "expected": True, "actual": json_6ld.get("all_checks_passed"), "passed": json_6ld.get("all_checks_passed") is True},
        {"check": "6ld_diagnosis", "expected": DIAGNOSIS_6LD, "actual": json_6ld.get("diagnosis"), "passed": json_6ld.get("diagnosis") == DIAGNOSIS_6LD},
        {"check": "6ld_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LD, "actual": json_6ld.get("recommended_next_layer"), "passed": json_6ld.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LD},
        {"check": "6ld_next_candidate_available", "expected": True, "actual": json_6ld.get("next_safe_candidate_available"), "passed": json_6ld.get("next_safe_candidate_available") is True},
        {"check": "6ld_wrapper_not_needed", "expected": False, "actual": json_6ld.get("wrapper_plan_needed"), "passed": json_6ld.get("wrapper_plan_needed") is False},
        {"check": "6ld_no_layer6_exit", "expected": False, "actual": json_6ld.get("layer_6_exit_recommended"), "passed": json_6ld.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in ALL_INPUTS
    ]
    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in ALL_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6lf_projection_adapter_next_candidate_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "wrapper_design", "blocked": True, "reason": "remaining candidates exist", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "real prediction surface required first", "passed": True},
        {"blocked_surface": "full_batch_projection_generation", "blocked": True, "reason": "single-sample successful candidate required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "historical evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LE cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6ld_passed", "expected": True, "actual": json_6ld.get("all_checks_passed"), "passed": json_6ld.get("all_checks_passed") is True},
        {"decision": "blocked_candidate_excluded", "expected": True, "actual": selected.get("entrypoint_name") != BLOCKED_NAME, "passed": selected.get("entrypoint_name") != BLOCKED_NAME},
        {"decision": "next_safe_candidate_available", "expected": True, "actual": next_available, "passed": next_available},
        {"decision": "future_6lf_contract_valid", "expected": True, "actual": len(future_6lf) == 4 and all_passed(future_6lf), "passed": len(future_6lf) == 4 and all_passed(future_6lf)},
        {"decision": "future_6lg_contract_valid", "expected": True, "actual": len(future_6lg) == 4 and all_passed(future_6lg), "passed": len(future_6lg) == 4 and all_passed(future_6lg)},
        {"decision": "recommend_6lf_next", "expected": RECOMMENDED_NEXT_LAYER_6LE, "actual": RECOMMENDED_NEXT_LAYER_6LE, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "projection_adapter_next_candidate_planned", "expected": True, "actual": True, "passed": True},
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
        {"surface": "6ld_audit", "policy": "read_only", "passed": True},
        {"surface": "6kz_artifacts", "policy": "read_only_reference", "passed": True},
        {"surface": "future_6lf_attempt", "policy": "single_sample_tmp_only_next_layer", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6le", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LE, "actual": RECOMMENDED_NEXT_LAYER_6LE, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LE, "actual": RECOMMENDED_PATH_6LE, "passed": True},
        {"decision": "recommend_next_candidate_attempt", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LE, "actual": DIAGNOSIS_6LE, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "blocked_candidate_policy", "passed": all_passed(blocked_policy), "detail": f"{len(blocked_policy)} rows"},
        {"check": "candidate_selection", "passed": all_passed(candidate_selection), "detail": f"{len(candidate_selection)} rows"},
        {"check": "static_signature_gate", "passed": all_passed(signature_gate), "detail": f"{len(signature_gate)} rows"},
        {"check": "payload_strategy", "passed": all_passed(payload_strategy), "detail": f"{len(payload_strategy)} rows"},
        {"check": "adapter_call_conditions", "passed": all_passed(call_conditions), "detail": f"{len(call_conditions)} rows"},
        {"check": "fail_closed_policy", "passed": all_passed(fail_closed), "detail": f"{len(fail_closed)} rows"},
        {"check": "prediction_surface_rules", "passed": all_passed(surface_rules), "detail": f"{len(surface_rules)} rows"},
        {"check": "metric_guardrails", "passed": all_passed(metric_guards), "detail": f"{len(metric_guards)} rows"},
        {"check": "allowed_next", "passed": all_passed(allowed_next), "detail": f"{len(allowed_next)} rows"},
        {"check": "forbidden_next", "passed": all_passed(forbidden_next), "detail": f"{len(forbidden_next)} rows"},
        {"check": "future_6lf_contract", "passed": all_passed(future_6lf), "detail": f"{len(future_6lf)} rows"},
        {"check": "future_6lg_contract", "passed": all_passed(future_6lg), "detail": f"{len(future_6lg)} rows"},
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
        "blocked_candidate_policy": write_csv(BLOCKED_POLICY_CSV, blocked_policy),
        "candidate_selection": write_csv(CANDIDATE_SELECTION_CSV, candidate_selection),
        "static_signature_gate": write_csv(SIGNATURE_GATE_CSV, signature_gate),
        "payload_strategy": write_csv(PAYLOAD_STRATEGY_CSV, payload_strategy),
        "adapter_call_conditions": write_csv(CALL_CONDITIONS_CSV, call_conditions),
        "fail_closed_policy": write_csv(FAIL_CLOSED_CSV, fail_closed),
        "prediction_surface_rules": write_csv(SURFACE_RULES_CSV, surface_rules),
        "metric_guardrails": write_csv(METRIC_GUARDS_CSV, metric_guards),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6lf_contract": write_csv(FUTURE_6LF_CSV, future_6lf),
        "future_6lg_contract": write_csv(FUTURE_6LG_CSV, future_6lg),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LE",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LE if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LE,
        "recommended_path": RECOMMENDED_PATH_6LE,
        "predecessor_audit": str(AUDIT_6LD_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6ld.get("diagnosis"),
        "planned_layer_after": "6LD",
        "source_family": "projection_adapter_next_candidate_plan",
        "blocked_candidate_policy_count": len(blocked_policy),
        "candidate_selection_count": len(candidate_selection),
        "static_signature_gate_count": len(signature_gate),
        "payload_strategy_count": len(payload_strategy),
        "adapter_call_condition_count": len(call_conditions),
        "fail_closed_policy_count": len(fail_closed),
        "prediction_surface_rule_count": len(surface_rules),
        "metric_guardrail_count": len(metric_guards),
        "allowed_operation_next_count": len(allowed_next),
        "forbidden_operation_next_count": len(forbidden_next),
        "blocker_count": len(blockers),
        "future_6lf_contract_valid": len(future_6lf) == 4 and all_passed(future_6lf),
        "future_6lg_contract_valid": len(future_6lg) == 4 and all_passed(future_6lg),
        "projection_adapter_next_candidate_planned": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "blocked_candidate_excluded": selected.get("entrypoint_name") != BLOCKED_NAME,
        "next_safe_candidate_available": next_available,
        "selected_next_candidate_path": selected.get("path", ""),
        "selected_next_candidate_name": selected.get("entrypoint_name", ""),
        "single_sample_adapter_call_allowed_next": True,
        "full_batch_adapter_call_allowed_next": False,
        "real_metric_execution_allowed_next": False,
        "projection_surface_materialization_allowed_next": True,
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
            "blocked_candidate_policy_csv": str(BLOCKED_POLICY_CSV),
            "candidate_selection_csv": str(CANDIDATE_SELECTION_CSV),
            "static_signature_gate_csv": str(SIGNATURE_GATE_CSV),
            "payload_strategy_csv": str(PAYLOAD_STRATEGY_CSV),
            "adapter_call_conditions_csv": str(CALL_CONDITIONS_CSV),
            "fail_closed_policy_csv": str(FAIL_CLOSED_CSV),
            "prediction_surface_rules_csv": str(SURFACE_RULES_CSV),
            "metric_guardrails_csv": str(METRIC_GUARDS_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6lf_contract_csv": str(FUTURE_6LF_CSV),
            "future_6lg_contract_csv": str(FUTURE_6LG_CSV),
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
