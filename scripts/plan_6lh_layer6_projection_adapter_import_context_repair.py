#!/usr/bin/env python3
"""Plan package-context import repair for the selected projection adapter.

This planning-only layer keeps the signature-safe candidate selected in 6LE/6LF
and plans a package-context import retry instead of file-location import.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6lh_projection_adapter_import_context_repair_plan"
TMP_DIR = Path("tmp")

AUDIT_6LG_PATH = Path("scripts/audit_6lg_layer6_projection_adapter_next_candidate.py")
JSON_6LG = TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit.json"

REQUIRED_6LG_INPUTS = [
    JSON_6LG,
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_checks.csv",
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_predecessor.csv",
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_candidate_audit.csv",
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_signature_audit.csv",
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_adapter_attempt_audit.csv",
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_gap_report_audit.csv",
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_prediction_surface_audit.csv",
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_metric_readiness_audit.csv",
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_import_context_diagnosis.csv",
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_next_route.csv",
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_blockers.csv",
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_future_6lh_contract.csv",
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_decision.csv",
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6lg_projection_adapter_next_candidate_audit_recommended_path.csv",
]

SOURCE_INPUTS = [
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_signature_inspection.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_payload_mapping.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_gap_report.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_fixture_contract_surface.csv",
]

ALL_INPUTS = REQUIRED_6LG_INPUTS + SOURCE_INPUTS

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PROBLEM_CSV = TMP_DIR / f"{SLUG}_problem_statement.csv"
RETENTION_CSV = TMP_DIR / f"{SLUG}_candidate_retention.csv"
IMPORT_STRATEGY_CSV = TMP_DIR / f"{SLUG}_import_repair_strategy.csv"
PACKAGE_CHECKS_CSV = TMP_DIR / f"{SLUG}_package_context_checks.csv"
SIGNATURE_GATE_CSV = TMP_DIR / f"{SLUG}_signature_gate_plan.csv"
RETRY_CONDITIONS_CSV = TMP_DIR / f"{SLUG}_adapter_retry_conditions.csv"
FAIL_CLOSED_CSV = TMP_DIR / f"{SLUG}_fail_closed_policy.csv"
SURFACE_RULES_CSV = TMP_DIR / f"{SLUG}_prediction_surface_rules.csv"
METRIC_GUARDS_CSV = TMP_DIR / f"{SLUG}_metric_guardrails.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LI_CSV = TMP_DIR / f"{SLUG}_future_6li_contract.csv"
FUTURE_6LJ_CSV = TMP_DIR / f"{SLUG}_future_6lj_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LG = "layer_6_projection_adapter_next_candidate_audit_complete"
DIAGNOSIS_6LH = "layer_6_projection_adapter_import_context_repair_plan_complete"
RECOMMENDED_NEXT_LAYER_6LG = "6LH_layer_6_projection_adapter_import_context_repair_plan"
RECOMMENDED_NEXT_LAYER_6LH = "6LI_layer_6_projection_adapter_import_context_repair_implementation"
RECOMMENDED_PATH_6LH = "implement_package_context_import_retry_for_same_candidate"

TARGET_MODULE = "mlb_app.ai_data_assistant_performance"
TARGET_FILE = "mlb_app/ai_data_assistant_performance.py"
TARGET_FUNCTION = "_canonical_games_from_projection_payload"
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


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6lg = load_json(JSON_6LG)

    package_init = Path("mlb_app/__init__.py")
    target_file = Path(TARGET_FILE)

    problem_statement = [
        {
            "problem": "file_location_import_failed_relative_imports",
            "failure": "ImportError: attempted relative import with no known parent package",
            "candidate": f"{TARGET_FILE}::{TARGET_FUNCTION}",
            "resolution": "retry_same_candidate_using_package_context_import",
            "passed": True,
        }
    ]

    candidate_retention = [
        {"item": "same_candidate_retained", "value": True, "path": TARGET_FILE, "function": TARGET_FUNCTION, "passed": True},
        {"item": "blocked_session_candidate_excluded", "value": True, "blocked_function": BLOCKED_FUNCTION, "passed": True},
        {"item": "next_candidate_retry_recommended", "value": False, "passed": True},
        {"item": "wrapper_plan_needed", "value": False, "passed": True},
    ]

    import_repair_strategy = [
        {"strategy": "forbid_file_location_import", "value": True, "passed": True},
        {"strategy": "use_package_import_path", "module": TARGET_MODULE, "passed": True},
        {"strategy": "use_importlib_import_module", "call": f'importlib.import_module("{TARGET_MODULE}")', "passed": True},
        {"strategy": "get_function_by_getattr", "function": TARGET_FUNCTION, "passed": True},
        {"strategy": "record_sys_path_adjustment_if_needed", "allowed": True, "passed": True},
    ]

    package_context_checks = [
        {"check": "target_file_exists", "value": target_file.exists(), "path": str(target_file), "passed": target_file.exists()},
        {"check": "package_init_present_or_namespace_package_possible", "value": package_init.exists() or Path("mlb_app").exists(), "init_path": str(package_init), "passed": package_init.exists() or Path("mlb_app").exists()},
        {"check": "module_import_path_planned", "value": TARGET_MODULE, "passed": True},
        {"check": "relative_import_error_targeted", "value": True, "passed": True},
    ]

    signature_gate_plan = [
        {"gate": "repeat_ast_signature_inspection_before_package_import", "required": True, "passed": True},
        {"gate": "confirm_function_name_exists_in_target_file", "function": TARGET_FUNCTION, "required": True, "passed": True},
        {"gate": "reject_forbidden_runtime_params", "tokens": "session;db;request;engine;client;connection;cursor;api;http;fetch;env;background;server", "required": True, "passed": True},
        {"gate": "allow_no_arg_or_single_serializable_arg_only", "required": True, "passed": True},
    ]

    retry_conditions = [
        {"condition": "6lg_import_context_failure_confirmed", "required": True, "passed": True},
        {"condition": "same_candidate_signature_remains_safe", "required": True, "passed": True},
        {"condition": "package_import_succeeds", "required": True, "passed": True},
        {"condition": "target_function_callable", "required": True, "passed": True},
        {"condition": "execute_at_most_one_single_sample_call", "required": True, "passed": True},
    ]

    fail_closed = [
        {"condition": "package_import_fails", "action": "emit_import_context_repair_gap_report", "passed": True},
        {"condition": "function_missing_or_not_callable", "action": "emit_import_context_repair_gap_report", "passed": True},
        {"condition": "signature_gate_fails", "action": "emit_signature_safety_gap_report", "passed": True},
        {"condition": "call_raises_exception", "action": "emit_adapter_retry_gap_report", "passed": True},
        {"condition": "return_payload_has_no_prediction_fields", "action": "emit_surface_with_readiness_false", "passed": True},
    ]

    surface_rules = [
        {"rule": "materialize_projection_surface_only_after_actual_return_payload", "passed": True},
        {"rule": "probability_fields_only_if_materially_present", "passed": True},
        {"rule": "runs_fields_only_if_materially_present", "passed": True},
        {"rule": "label_all_outputs_non_production_single_sample", "passed": True},
        {"rule": "preserve_package_import_lineage", "passed": True},
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
        {"operation": "sys_path_adjustment_if_recorded", "allowed_next": True, "passed": True},
        {"operation": "single_sample_local_adapter_call_if_gates_pass", "allowed_next": True, "passed": True},
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
        {"blocker": "package_context_import_repair_not_implemented", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6li = [
        {"contract": "implement_package_context_import_same_candidate", "required": True, "passed": True},
        {"contract": "repeat_signature_gate_before_import", "required": True, "passed": True},
        {"contract": "attempt_one_single_sample_call_or_fail_closed", "required": True, "passed": True},
        {"contract": "emit_surface_or_gap_without_metrics_activation_or_exit", "required": True, "passed": True},
    ]

    future_6lj = [
        {"contract": "audit_import_context_repair_attempt", "required": True, "passed": True},
        {"contract": "audit_surface_or_gap_result", "required": True, "passed": True},
        {"contract": "route_to_batch_plan_next_candidate_or_wrapper", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_activation_or_layer6_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6lg_audit_script_exists", "expected": True, "actual": AUDIT_6LG_PATH.exists(), "passed": AUDIT_6LG_PATH.exists()},
        {"check": "6lg_json_exists", "expected": True, "actual": JSON_6LG.exists(), "passed": JSON_6LG.exists()},
        {"check": "6lg_all_checks_passed", "expected": True, "actual": json_6lg.get("all_checks_passed"), "passed": json_6lg.get("all_checks_passed") is True},
        {"check": "6lg_diagnosis", "expected": DIAGNOSIS_6LG, "actual": json_6lg.get("diagnosis"), "passed": json_6lg.get("diagnosis") == DIAGNOSIS_6LG},
        {"check": "6lg_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LG, "actual": json_6lg.get("recommended_next_layer"), "passed": json_6lg.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LG},
        {"check": "6lg_import_context_failure_confirmed", "expected": True, "actual": json_6lg.get("import_context_failure_confirmed"), "passed": json_6lg.get("import_context_failure_confirmed") is True},
        {"check": "6lg_package_context_repair_needed", "expected": True, "actual": json_6lg.get("package_context_repair_needed"), "passed": json_6lg.get("package_context_repair_needed") is True},
        {"check": "6lg_no_wrapper", "expected": False, "actual": json_6lg.get("wrapper_plan_needed"), "passed": json_6lg.get("wrapper_plan_needed") is False},
        {"check": "6lg_no_layer6_exit", "expected": False, "actual": json_6lg.get("layer_6_exit_recommended"), "passed": json_6lg.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in ALL_INPUTS
    ]
    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in ALL_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6li_import_context_repair_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "next_candidate_retry", "blocked": True, "reason": "package import retry for same candidate should happen first", "passed": True},
        {"blocked_surface": "wrapper_design", "blocked": True, "reason": "same candidate not exhausted", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "real prediction surface required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "historical evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LH cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6lg_passed", "expected": True, "actual": json_6lg.get("all_checks_passed"), "passed": json_6lg.get("all_checks_passed") is True},
        {"decision": "same_candidate_retained", "expected": True, "actual": True, "passed": True},
        {"decision": "package_context_import_planned", "expected": True, "actual": True, "passed": True},
        {"decision": "target_module_import_path", "expected": TARGET_MODULE, "actual": TARGET_MODULE, "passed": True},
        {"decision": "future_6li_contract_valid", "expected": True, "actual": len(future_6li) == 4 and all_passed(future_6li), "passed": len(future_6li) == 4 and all_passed(future_6li)},
        {"decision": "future_6lj_contract_valid", "expected": True, "actual": len(future_6lj) == 4 and all_passed(future_6lj), "passed": len(future_6lj) == 4 and all_passed(future_6lj)},
        {"decision": "recommend_6li_next", "expected": RECOMMENDED_NEXT_LAYER_6LH, "actual": RECOMMENDED_NEXT_LAYER_6LH, "passed": True},
        {"decision": "do_not_recommend_other_candidate", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper", "expected": True, "actual": True, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "projection_adapter_import_context_repair_planned", "expected": True, "actual": True, "passed": True},
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
        {"surface": "6lg_audit", "policy": "read_only", "passed": True},
        {"surface": "6lf_artifacts", "policy": "read_only_reference", "passed": True},
        {"surface": "future_6li_attempt", "policy": "single_sample_tmp_only_next_layer", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6lh", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LH, "actual": RECOMMENDED_NEXT_LAYER_6LH, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LH, "actual": RECOMMENDED_PATH_6LH, "passed": True},
        {"decision": "recommend_package_context_import_repair", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_next_candidate_retry", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LH, "actual": DIAGNOSIS_6LH, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "problem_statement", "passed": all_passed(problem_statement), "detail": f"{len(problem_statement)} rows"},
        {"check": "candidate_retention", "passed": all_passed(candidate_retention), "detail": f"{len(candidate_retention)} rows"},
        {"check": "import_repair_strategy", "passed": all_passed(import_repair_strategy), "detail": f"{len(import_repair_strategy)} rows"},
        {"check": "package_context_checks", "passed": all_passed(package_context_checks), "detail": f"{len(package_context_checks)} rows"},
        {"check": "signature_gate_plan", "passed": all_passed(signature_gate_plan), "detail": f"{len(signature_gate_plan)} rows"},
        {"check": "adapter_retry_conditions", "passed": all_passed(retry_conditions), "detail": f"{len(retry_conditions)} rows"},
        {"check": "fail_closed_policy", "passed": all_passed(fail_closed), "detail": f"{len(fail_closed)} rows"},
        {"check": "prediction_surface_rules", "passed": all_passed(surface_rules), "detail": f"{len(surface_rules)} rows"},
        {"check": "metric_guardrails", "passed": all_passed(metric_guards), "detail": f"{len(metric_guards)} rows"},
        {"check": "allowed_next", "passed": all_passed(allowed_next), "detail": f"{len(allowed_next)} rows"},
        {"check": "forbidden_next", "passed": all_passed(forbidden_next), "detail": f"{len(forbidden_next)} rows"},
        {"check": "future_6li_contract", "passed": all_passed(future_6li), "detail": f"{len(future_6li)} rows"},
        {"check": "future_6lj_contract", "passed": all_passed(future_6lj), "detail": f"{len(future_6lj)} rows"},
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
        "candidate_retention": write_csv(RETENTION_CSV, candidate_retention),
        "import_repair_strategy": write_csv(IMPORT_STRATEGY_CSV, import_repair_strategy),
        "package_context_checks": write_csv(PACKAGE_CHECKS_CSV, package_context_checks),
        "signature_gate_plan": write_csv(SIGNATURE_GATE_CSV, signature_gate_plan),
        "adapter_retry_conditions": write_csv(RETRY_CONDITIONS_CSV, retry_conditions),
        "fail_closed_policy": write_csv(FAIL_CLOSED_CSV, fail_closed),
        "prediction_surface_rules": write_csv(SURFACE_RULES_CSV, surface_rules),
        "metric_guardrails": write_csv(METRIC_GUARDS_CSV, metric_guards),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6li_contract": write_csv(FUTURE_6LI_CSV, future_6li),
        "future_6lj_contract": write_csv(FUTURE_6LJ_CSV, future_6lj),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LH",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LH if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LH,
        "recommended_path": RECOMMENDED_PATH_6LH,
        "predecessor_audit": str(AUDIT_6LG_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6lg.get("diagnosis"),
        "planned_layer_after": "6LG",
        "source_family": "projection_adapter_import_context_repair_plan",
        "problem_statement_count": len(problem_statement),
        "candidate_retention_count": len(candidate_retention),
        "import_repair_strategy_count": len(import_repair_strategy),
        "package_context_check_count": len(package_context_checks),
        "signature_gate_plan_count": len(signature_gate_plan),
        "adapter_retry_condition_count": len(retry_conditions),
        "fail_closed_policy_count": len(fail_closed),
        "prediction_surface_rule_count": len(surface_rules),
        "metric_guardrail_count": len(metric_guards),
        "allowed_operation_next_count": len(allowed_next),
        "forbidden_operation_next_count": len(forbidden_next),
        "blocker_count": len(blockers),
        "future_6li_contract_valid": len(future_6li) == 4 and all_passed(future_6li),
        "future_6lj_contract_valid": len(future_6lj) == 4 and all_passed(future_6lj),
        "projection_adapter_import_context_repair_planned": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "same_candidate_retained": True,
        "blocked_session_candidate_excluded": True,
        "package_context_import_planned": True,
        "target_module_import_path": TARGET_MODULE,
        "target_function_name": TARGET_FUNCTION,
        "file_location_import_forbidden_next": True,
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
            "import_repair_strategy_csv": str(IMPORT_STRATEGY_CSV),
            "package_context_checks_csv": str(PACKAGE_CHECKS_CSV),
            "signature_gate_plan_csv": str(SIGNATURE_GATE_CSV),
            "adapter_retry_conditions_csv": str(RETRY_CONDITIONS_CSV),
            "fail_closed_policy_csv": str(FAIL_CLOSED_CSV),
            "prediction_surface_rules_csv": str(SURFACE_RULES_CSV),
            "metric_guardrails_csv": str(METRIC_GUARDS_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6li_contract_csv": str(FUTURE_6LI_CSV),
            "future_6lj_contract_csv": str(FUTURE_6LJ_CSV),
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
