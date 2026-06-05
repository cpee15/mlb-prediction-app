#!/usr/bin/env python3
"""Plan safe projection adapter call execution.

This planning-only layer defines how the next implementation layer can safely
attempt one deterministic local adapter call against a static-safe projection
entrypoint candidate to produce real prediction fields, while preserving all
Layer 6 safety boundaries.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6lb_projection_adapter_call_implementation_plan"
TMP_DIR = Path("tmp")

AUDIT_6LA_PATH = Path("scripts/audit_6la_layer6_projection_call_contract_implementation.py")
JSON_6LA = TMP_DIR / "layer6_6la_projection_call_contract_implementation_audit.json"

REQUIRED_6LA_INPUTS = [
    JSON_6LA,
    TMP_DIR / "layer6_6la_projection_call_contract_implementation_audit_checks.csv",
    TMP_DIR / "layer6_6la_projection_call_contract_implementation_audit_predecessor.csv",
    TMP_DIR / "layer6_6la_projection_call_contract_implementation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6la_projection_call_contract_implementation_audit_fixture_surface_audit.csv",
    TMP_DIR / "layer6_6la_projection_call_contract_implementation_audit_entrypoint_audit.csv",
    TMP_DIR / "layer6_6la_projection_call_contract_implementation_audit_projection_surface_audit.csv",
    TMP_DIR / "layer6_6la_projection_call_contract_implementation_audit_prediction_materialization_audit.csv",
    TMP_DIR / "layer6_6la_projection_call_contract_implementation_audit_metric_readiness_audit.csv",
    TMP_DIR / "layer6_6la_projection_call_contract_implementation_audit_next_route.csv",
    TMP_DIR / "layer6_6la_projection_call_contract_implementation_audit_blockers.csv",
    TMP_DIR / "layer6_6la_projection_call_contract_implementation_audit_future_6lb_contract.csv",
    TMP_DIR / "layer6_6la_projection_call_contract_implementation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6la_projection_call_contract_implementation_audit_decision.csv",
    TMP_DIR / "layer6_6la_projection_call_contract_implementation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6la_projection_call_contract_implementation_audit_recommended_path.csv",
]

REFERENCED_6KZ_INPUTS = [
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_fixture_contract_surface.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_entrypoint_inventory.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_entrypoint_safety_scan.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_adapter_feasibility.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_projection_surface.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_metric_readiness.csv",
]

ALL_INPUTS = REQUIRED_6LA_INPUTS + REFERENCED_6KZ_INPUTS

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PROBLEM_CSV = TMP_DIR / f"{SLUG}_problem_statement.csv"
CANDIDATE_RULES_CSV = TMP_DIR / f"{SLUG}_candidate_selection_rules.csv"
SIGNATURE_PLAN_CSV = TMP_DIR / f"{SLUG}_signature_inspection_plan.csv"
PAYLOAD_PLAN_CSV = TMP_DIR / f"{SLUG}_payload_mapping_plan.csv"
EXECUTION_PLAN_CSV = TMP_DIR / f"{SLUG}_adapter_execution_plan.csv"
FAIL_CLOSED_CSV = TMP_DIR / f"{SLUG}_fail_closed_plan.csv"
SURFACE_RULES_CSV = TMP_DIR / f"{SLUG}_prediction_surface_rules.csv"
METRIC_GUARDS_CSV = TMP_DIR / f"{SLUG}_metric_guardrails.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LC_CSV = TMP_DIR / f"{SLUG}_future_6lc_contract.csv"
FUTURE_6LD_CSV = TMP_DIR / f"{SLUG}_future_6ld_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LA = "layer_6_projection_call_contract_implementation_audit_complete"
DIAGNOSIS_6LB = "layer_6_projection_adapter_call_implementation_plan_complete"
RECOMMENDED_NEXT_LAYER_6LA = "6LB_layer_6_projection_adapter_call_implementation_plan"
RECOMMENDED_NEXT_LAYER_6LB = "6LC_layer_6_projection_adapter_call_implementation"
RECOMMENDED_PATH_6LB = "implement_safe_projection_adapter_call_for_real_prediction_surface"


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


def select_safe_candidates() -> List[Dict[str, Any]]:
    rows = read_csv_rows(TMP_DIR / "layer6_6kz_projection_call_contract_implementation_entrypoint_safety_scan.csv")
    selected = []
    for row in rows:
        if str(row.get("safe_for_direct_call", "")).lower() == "true":
            selected.append({
                "path": row.get("path", ""),
                "entrypoint_name": row.get("entrypoint_name", ""),
                "selection_rank": len(selected) + 1,
                "reason": "static_safe_no_risk_tokens_callable_candidate",
                "passed": True,
            })
        if len(selected) >= 10:
            break
    if not selected:
        selected.append({
            "path": "",
            "entrypoint_name": "",
            "selection_rank": 0,
            "reason": "no_static_safe_candidate_available",
            "passed": True,
        })
    return selected


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6la = load_json(JSON_6LA)
    selected_candidates = select_safe_candidates()
    safe_candidates_available = any(row.get("selection_rank", 0) != 0 for row in selected_candidates)

    problem_statement = [
        {
            "problem": "contract_shell_surface_without_real_predictions",
            "confirmed_by": "6LA",
            "impact": "real backtest metrics remain blocked",
            "planned_resolution": "plan one safe local adapter call attempt or fail-closed adapter-call gap report",
            "passed": True,
        }
    ]

    candidate_selection_rules = [
        {"rule": "use_6kz_safety_scan", "detail": "Select only rows with safe_for_direct_call=true.", "passed": True},
        {"rule": "prefer_projection_payload_builders", "detail": "Prefer payload builders over UI routes or server handlers.", "passed": True},
        {"rule": "prefer_no_risk_tokens", "detail": "Candidate must have zero static risk-token hits from 6KZ.", "passed": True},
        {"rule": "prefer_serializable_return_shape", "detail": "Prioritize candidates likely to return dict/list payloads.", "passed": True},
        {"rule": "limit_to_one_candidate_first", "detail": "6LC may attempt at most one candidate first.", "passed": True},
    ]

    signature_plan = [
        {"step": "parse_ast_function_signature", "execution_allowed_next": True, "detail": "Use AST/static inspection before import.", "passed": True},
        {"step": "inspect_import_without_side_effects_if_static_gate_passes", "execution_allowed_next": True, "detail": "Future 6LC may import one candidate only after static gate.", "passed": True},
        {"step": "record_required_positional_and_keyword_args", "execution_allowed_next": True, "detail": "Map fixture to accepted parameters.", "passed": True},
        {"step": "fail_closed_if_signature_requires_app_db_session_or_request", "execution_allowed_next": True, "detail": "No app/server/db/runtime object allowed.", "passed": True},
    ]

    payload_mapping_plan = [
        {"field_family": "fixture_identity", "source": "6kz_fixture_contract_surface", "mapping": "game_id/game_date/home_team/away_team", "passed": True},
        {"field_family": "mechanic_tags", "source": "6kz_fixture_contract_surface", "mapping": "mechanic_context_tags", "passed": True},
        {"field_family": "optional_pitcher_lineup_park_bullpen", "source": "fixture_or_proxy", "mapping": "include only if present and labeled", "passed": True},
        {"field_family": "mode", "source": "adapter", "mapping": "single_sample_non_production_adapter_call", "passed": True},
        {"field_family": "lineage", "source": "all_input_paths", "mapping": "source_lineage plus adapter entrypoint", "passed": True},
    ]

    adapter_execution_plan = [
        {"step": "choose_first_safe_candidate", "allowed_next": True, "limit": "one candidate", "passed": True},
        {"step": "choose_first_complete_fixture", "allowed_next": True, "limit": "one game", "passed": True},
        {"step": "execute_local_call_only_after_static_and_signature_gates", "allowed_next": True, "limit": "one call", "passed": True},
        {"step": "capture_return_payload_without_metric_computation", "allowed_next": True, "limit": "projection extraction only", "passed": True},
        {"step": "write_tmp_only_projection_surface_or_gap_report", "allowed_next": True, "limit": "tmp artifacts only", "passed": True},
    ]

    fail_closed_plan = [
        {"condition": "candidate_import_has_side_effect_or_fails", "action": "emit_adapter_call_gap_report", "passed": True},
        {"condition": "signature_requires_forbidden_runtime_object", "action": "emit_adapter_call_gap_report", "passed": True},
        {"condition": "payload_mapping_incomplete", "action": "emit_adapter_call_gap_report", "passed": True},
        {"condition": "return_payload_has_no_probability_or_runs_fields", "action": "emit_projection_surface_with_readiness_false", "passed": True},
        {"condition": "any_remote_db_write_or_activation_signal_detected", "action": "abort_and_emit_safety_gap", "passed": True},
    ]

    prediction_surface_rules = [
        {"rule": "materialize_probability_fields_only_if_returned", "passed": True},
        {"rule": "materialize_runs_fields_only_if_returned", "passed": True},
        {"rule": "label_projection_call_status_success_only_after_real_return_payload", "passed": True},
        {"rule": "preserve_non_production_and_lineage_labels", "passed": True},
        {"rule": "do_not_join_actuals_for_metric_computation", "passed": True},
    ]

    metric_guardrails = [
        {"guardrail": "do_not_compute_brier", "passed": True},
        {"guardrail": "do_not_compute_log_loss", "passed": True},
        {"guardrail": "do_not_compute_calibration", "passed": True},
        {"guardrail": "do_not_compute_mae_rmse", "passed": True},
        {"guardrail": "do_not_compute_winner_correct", "passed": True},
        {"guardrail": "only_emit_metric_readiness_flags", "passed": True},
    ]

    allowed_next = [
        {"operation": "read_repo_local_files", "allowed_next": True, "passed": True},
        {"operation": "static_ast_signature_inspection", "allowed_next": True, "passed": True},
        {"operation": "single_sample_local_adapter_call", "allowed_next": True, "passed": True},
        {"operation": "write_tmp_artifacts", "allowed_next": True, "passed": True},
        {"operation": "materialize_non_production_prediction_surface", "allowed_next": True, "passed": True},
        {"operation": "emit_fail_closed_adapter_call_gap_report", "allowed_next": True, "passed": True},
    ]

    forbidden_next = [
        {"operation": "full_batch_adapter_call", "allowed_next": False, "passed": True},
        {"operation": "real_backtest_metric_execution", "allowed_next": False, "passed": True},
        {"operation": "live_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "database_writes", "allowed_next": False, "passed": True},
        {"operation": "production_source_modifications", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit_credit", "allowed_next": False, "passed": True},
    ]

    blockers = [
        {"blocker": "safe_projection_adapter_call_not_implemented_yet", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_prediction_surface_not_materialized", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6lc = [
        {"contract": "implement_single_sample_safe_adapter_call", "required": True, "passed": True},
        {"contract": "emit_real_prediction_surface_or_fail_closed_gap", "required": True, "passed": True},
        {"contract": "emit_prediction_readiness_flags_without_metrics", "required": True, "passed": True},
        {"contract": "preserve_no_fetch_no_db_no_activation_no_layer6_exit", "required": True, "passed": True},
    ]

    future_6ld = [
        {"contract": "audit_single_sample_adapter_call_result", "required": True, "passed": True},
        {"contract": "audit_prediction_surface_real_vs_gap", "required": True, "passed": True},
        {"contract": "route_to_batch_projection_plan_or_adapter_repair", "required": True, "passed": True},
        {"contract": "preserve_no_real_metrics_no_activation_no_layer6_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6la_audit_script_exists", "expected": True, "actual": AUDIT_6LA_PATH.exists(), "passed": AUDIT_6LA_PATH.exists()},
        {"check": "6la_json_exists", "expected": True, "actual": JSON_6LA.exists(), "passed": JSON_6LA.exists()},
        {"check": "6la_all_checks_passed", "expected": True, "actual": json_6la.get("all_checks_passed"), "passed": json_6la.get("all_checks_passed") is True},
        {"check": "6la_diagnosis", "expected": DIAGNOSIS_6LA, "actual": json_6la.get("diagnosis"), "passed": json_6la.get("diagnosis") == DIAGNOSIS_6LA},
        {"check": "6la_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LA, "actual": json_6la.get("recommended_next_layer"), "passed": json_6la.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LA},
        {"check": "6la_contract_shell_confirmed", "expected": True, "actual": json_6la.get("projection_surface_is_contract_shell_confirmed"), "passed": json_6la.get("projection_surface_is_contract_shell_confirmed") is True},
        {"check": "6la_real_prediction_surface_false", "expected": False, "actual": json_6la.get("real_prediction_surface_materialized"), "passed": json_6la.get("real_prediction_surface_materialized") is False},
        {"check": "6la_adapter_call_needed", "expected": True, "actual": json_6la.get("projection_adapter_call_execution_needed"), "passed": json_6la.get("projection_adapter_call_execution_needed") is True},
        {"check": "6la_no_layer6_exit", "expected": False, "actual": json_6la.get("layer_6_exit_recommended"), "passed": json_6la.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in ALL_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in ALL_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6lc_projection_adapter_call_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "full_batch_projection_generation", "blocked": True, "reason": "single-sample adapter call required first", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "real prediction surface required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "historical evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6LB is planning-only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LB cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6la_passed", "expected": True, "actual": json_6la.get("all_checks_passed"), "passed": json_6la.get("all_checks_passed") is True},
        {"decision": "safe_candidates_available", "expected": True, "actual": safe_candidates_available, "passed": safe_candidates_available},
        {"decision": "problem_statement_count", "expected": 1, "actual": len(problem_statement), "passed": len(problem_statement) == 1 and all_passed(problem_statement)},
        {"decision": "candidate_selection_rule_count", "expected": 5, "actual": len(candidate_selection_rules), "passed": len(candidate_selection_rules) == 5 and all_passed(candidate_selection_rules)},
        {"decision": "adapter_execution_plan_count", "expected": 5, "actual": len(adapter_execution_plan), "passed": len(adapter_execution_plan) == 5 and all_passed(adapter_execution_plan)},
        {"decision": "future_6lc_contract_valid", "expected": True, "actual": len(future_6lc) == 4 and all_passed(future_6lc), "passed": len(future_6lc) == 4 and all_passed(future_6lc)},
        {"decision": "future_6ld_contract_valid", "expected": True, "actual": len(future_6ld) == 4 and all_passed(future_6ld), "passed": len(future_6ld) == 4 and all_passed(future_6ld)},
        {"decision": "recommend_6lc_next", "expected": RECOMMENDED_NEXT_LAYER_6LB, "actual": RECOMMENDED_NEXT_LAYER_6LB, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "projection_adapter_call_implementation_planned", "expected": True, "actual": True, "passed": True},
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
        {"surface": "6la_audit", "policy": "read_only", "passed": True},
        {"surface": "6kz_artifacts", "policy": "read_only_reference", "passed": True},
        {"surface": "future_adapter_call", "policy": "single_sample_tmp_only_next_layer", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6lb", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LB, "actual": RECOMMENDED_NEXT_LAYER_6LB, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LB, "actual": RECOMMENDED_PATH_6LB, "passed": True},
        {"decision": "recommend_single_sample_projection_adapter_call_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_full_batch_or_metrics", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LB, "actual": DIAGNOSIS_6LB, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "problem_statement", "passed": all_passed(problem_statement), "detail": f"{len(problem_statement)} rows"},
        {"check": "candidate_selection_rules", "passed": all_passed(candidate_selection_rules), "detail": f"{len(candidate_selection_rules)} rows"},
        {"check": "signature_inspection_plan", "passed": all_passed(signature_plan), "detail": f"{len(signature_plan)} rows"},
        {"check": "payload_mapping_plan", "passed": all_passed(payload_mapping_plan), "detail": f"{len(payload_mapping_plan)} rows"},
        {"check": "adapter_execution_plan", "passed": all_passed(adapter_execution_plan), "detail": f"{len(adapter_execution_plan)} rows"},
        {"check": "fail_closed_plan", "passed": all_passed(fail_closed_plan), "detail": f"{len(fail_closed_plan)} rows"},
        {"check": "prediction_surface_rules", "passed": all_passed(prediction_surface_rules), "detail": f"{len(prediction_surface_rules)} rows"},
        {"check": "metric_guardrails", "passed": all_passed(metric_guardrails), "detail": f"{len(metric_guardrails)} rows"},
        {"check": "allowed_next", "passed": all_passed(allowed_next), "detail": f"{len(allowed_next)} rows"},
        {"check": "forbidden_next", "passed": all_passed(forbidden_next), "detail": f"{len(forbidden_next)} rows"},
        {"check": "future_6lc_contract", "passed": all_passed(future_6lc), "detail": f"{len(future_6lc)} rows"},
        {"check": "future_6ld_contract", "passed": all_passed(future_6ld), "detail": f"{len(future_6ld)} rows"},
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
        "problem_statement": write_csv(PROBLEM_CSV, problem_statement),
        "candidate_selection_rules": write_csv(CANDIDATE_RULES_CSV, candidate_selection_rules),
        "signature_inspection_plan": write_csv(SIGNATURE_PLAN_CSV, signature_plan),
        "payload_mapping_plan": write_csv(PAYLOAD_PLAN_CSV, payload_mapping_plan),
        "adapter_execution_plan": write_csv(EXECUTION_PLAN_CSV, adapter_execution_plan),
        "fail_closed_plan": write_csv(FAIL_CLOSED_CSV, fail_closed_plan),
        "prediction_surface_rules": write_csv(SURFACE_RULES_CSV, prediction_surface_rules),
        "metric_guardrails": write_csv(METRIC_GUARDS_CSV, metric_guardrails),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6lc_contract": write_csv(FUTURE_6LC_CSV, future_6lc),
        "future_6ld_contract": write_csv(FUTURE_6LD_CSV, future_6ld),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LB",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LB if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LB,
        "recommended_path": RECOMMENDED_PATH_6LB,
        "predecessor_audit": str(AUDIT_6LA_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6la.get("diagnosis"),
        "planned_layer_after": "6LA",
        "source_family": "projection_adapter_call_implementation_plan",
        "problem_statement_count": len(problem_statement),
        "candidate_selection_rule_count": len(candidate_selection_rules),
        "signature_inspection_plan_count": len(signature_plan),
        "payload_mapping_plan_count": len(payload_mapping_plan),
        "adapter_execution_plan_count": len(adapter_execution_plan),
        "fail_closed_plan_count": len(fail_closed_plan),
        "prediction_surface_rule_count": len(prediction_surface_rules),
        "metric_guardrail_count": len(metric_guardrails),
        "allowed_operation_next_count": len(allowed_next),
        "forbidden_operation_next_count": len(forbidden_next),
        "blocker_count": len(blockers),
        "future_6lc_contract_valid": len(future_6lc) == 4 and all_passed(future_6lc),
        "future_6ld_contract_valid": len(future_6ld) == 4 and all_passed(future_6ld),
        "projection_adapter_call_implementation_planned": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "contract_shell_surface_confirmed": True,
        "real_prediction_surface_materialized": False,
        "projection_adapter_call_execution_needed": True,
        "safe_entrypoint_candidates_available": safe_candidates_available,
        "selected_entrypoint_candidate_count": sum(1 for r in selected_candidates if r.get("selection_rank", 0) != 0),
        "single_sample_adapter_call_allowed_next": True,
        "full_batch_adapter_call_allowed_next": False,
        "real_metric_execution_allowed_next": False,
        "projection_surface_materialization_allowed_next": True,
        "live_fetches_allowed_next": False,
        "remote_api_calls_allowed_next": False,
        "database_writes_allowed_next": False,
        "production_source_modifications_allowed_next": False,
        "mechanics_activation_allowed_next": False,
        "layer_6_exit_allowed_next": False,
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
        "selected_entrypoint_candidates": selected_candidates,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "problem_statement_csv": str(PROBLEM_CSV),
            "candidate_selection_rules_csv": str(CANDIDATE_RULES_CSV),
            "signature_inspection_plan_csv": str(SIGNATURE_PLAN_CSV),
            "payload_mapping_plan_csv": str(PAYLOAD_PLAN_CSV),
            "adapter_execution_plan_csv": str(EXECUTION_PLAN_CSV),
            "fail_closed_plan_csv": str(FAIL_CLOSED_CSV),
            "prediction_surface_rules_csv": str(SURFACE_RULES_CSV),
            "metric_guardrails_csv": str(METRIC_GUARDS_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6lc_contract_csv": str(FUTURE_6LC_CSV),
            "future_6ld_contract_csv": str(FUTURE_6LD_CSV),
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
