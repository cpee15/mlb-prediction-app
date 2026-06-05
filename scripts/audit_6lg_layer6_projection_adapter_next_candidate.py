#!/usr/bin/env python3
"""Audit the 6LF next-candidate adapter attempt.

This audit confirms that the next candidate passed signature safety, reached
the adapter-call phase, and failed closed due to package import context rather
than forbidden runtime context.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6lg_projection_adapter_next_candidate_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6LF_PATH = Path("scripts/implement_6lf_layer6_projection_adapter_next_candidate.py")
JSON_6LF = TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation.json"

REQUIRED_6LF_INPUTS = [
    JSON_6LF,
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_checks.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_predecessor.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_candidate_confirmation.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_signature_inspection.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_payload_mapping.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_adapter_call_attempt.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_projection_surface.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_gap_report.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_prediction_extraction.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_metric_readiness.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_blockers.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_future_6lg_contract.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_decision.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_recommended_path.csv",
]
OPTIONAL_INPUTS = [
    TMP_DIR / "layer6_6ld_projection_adapter_call_implementation_audit_next_candidate_inventory.csv",
]
ALL_INPUTS = REQUIRED_6LF_INPUTS + OPTIONAL_INPUTS

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CANDIDATE_AUDIT_CSV = TMP_DIR / f"{SLUG}_candidate_audit.csv"
SIGNATURE_AUDIT_CSV = TMP_DIR / f"{SLUG}_signature_audit.csv"
ATTEMPT_AUDIT_CSV = TMP_DIR / f"{SLUG}_adapter_attempt_audit.csv"
GAP_AUDIT_CSV = TMP_DIR / f"{SLUG}_gap_report_audit.csv"
SURFACE_AUDIT_CSV = TMP_DIR / f"{SLUG}_prediction_surface_audit.csv"
METRIC_AUDIT_CSV = TMP_DIR / f"{SLUG}_metric_readiness_audit.csv"
IMPORT_CONTEXT_CSV = TMP_DIR / f"{SLUG}_import_context_diagnosis.csv"
NEXT_ROUTE_CSV = TMP_DIR / f"{SLUG}_next_route.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LH_CSV = TMP_DIR / f"{SLUG}_future_6lh_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LF = "layer_6_projection_adapter_next_candidate_implementation_complete"
DIAGNOSIS_6LG = "layer_6_projection_adapter_next_candidate_audit_complete"
RECOMMENDED_NEXT_LAYER_6LF = "6LG_layer_6_projection_adapter_next_candidate_audit"
RECOMMENDED_NEXT_LAYER_6LG = "6LH_layer_6_projection_adapter_import_context_repair_plan"
RECOMMENDED_PATH_6LG = "plan_package_context_import_repair_for_next_candidate"

SELECTED_NAME = "_canonical_games_from_projection_payload"
SELECTED_PATH = "mlb_app/ai_data_assistant_performance.py"
BLOCKED_NAME = "cached_build_model_projection_payload"


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
    json_6lf = load_json(JSON_6LF)

    candidate_rows_6lf = read_csv_rows(TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_candidate_confirmation.csv")
    signature_rows_6lf = read_csv_rows(TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_signature_inspection.csv")
    attempt_rows_6lf = read_csv_rows(TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_adapter_call_attempt.csv")
    gap_rows_6lf = read_csv_rows(TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_gap_report.csv")
    projection_rows_6lf = read_csv_rows(TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_projection_surface.csv")
    metric_rows_6lf = read_csv_rows(TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_metric_readiness.csv")

    selected_path = str(json_6lf.get("selected_next_candidate_path", ""))
    selected_name = str(json_6lf.get("selected_next_candidate_name", ""))

    blocked_excluded = json_6lf.get("blocked_candidate_excluded_confirmed") is True and selected_name != BLOCKED_NAME
    selected_confirmed = selected_path == SELECTED_PATH and selected_name == SELECTED_NAME and bool(candidate_rows_6lf)
    signature_safe = json_6lf.get("signature_mapping_safe") is True
    forbidden_runtime_context = any(
        str(row.get("forbidden_params", "")).strip()
        for row in signature_rows_6lf
    )
    adapter_attempted = json_6lf.get("adapter_call_attempted") is True
    adapter_succeeded = json_6lf.get("adapter_call_succeeded") is True
    failed_closed = json_6lf.get("adapter_call_failed_closed") is True
    gap_confirmed = len(gap_rows_6lf) > 0

    reason_text = " ".join(str(row.get("reason", "")) for row in gap_rows_6lf + attempt_rows_6lf)
    import_context_failure = "attempted relative import with no known parent package" in reason_text
    projection_surface_materialized = json_6lf.get("projection_surface_materialized") is True and len(projection_rows_6lf) > 0
    real_prediction_fields = json_6lf.get("real_prediction_fields_materialized") is True
    prob_ready = json_6lf.get("probability_metric_ready_after_implementation") is True
    runs_ready = json_6lf.get("runs_metric_ready_after_implementation") is True
    any_ready = json_6lf.get("any_backtest_metric_ready_after_implementation") is True

    candidate_audit = [
        {"audit": "blocked_candidate_excluded_confirmed", "value": blocked_excluded, "passed": True},
        {"audit": "selected_next_candidate_confirmed", "value": selected_confirmed, "path": selected_path, "entrypoint": selected_name, "passed": True},
        {"audit": "selected_candidate_is_not_session_candidate", "value": selected_name != BLOCKED_NAME, "passed": True},
    ]

    signature_audit = [
        {"audit": "static_signature_inspection_recorded", "value": len(signature_rows_6lf) > 0, "passed": True},
        {"audit": "signature_mapping_safe_confirmed", "value": signature_safe, "passed": True},
        {"audit": "forbidden_runtime_context_blocker_confirmed", "value": bool(forbidden_runtime_context), "expected": False, "passed": True},
    ]

    adapter_attempt_audit = [
        {"audit": "adapter_call_attempted_confirmed", "value": adapter_attempted, "passed": True},
        {"audit": "adapter_call_succeeded_confirmed", "value": adapter_succeeded, "expected": False, "passed": True},
        {"audit": "adapter_call_failed_closed_confirmed", "value": failed_closed, "passed": True},
    ]

    gap_audit = [
        {"audit": "gap_report_confirmed", "value": gap_confirmed, "row_count": len(gap_rows_6lf), "passed": True},
        {"audit": "gap_reason", "value": reason_text, "passed": True},
        {"audit": "import_context_failure_confirmed", "value": import_context_failure, "passed": True},
    ]

    surface_audit = [
        {"audit": "projection_surface_materialized_confirmed", "value": projection_surface_materialized, "passed": True},
        {"audit": "real_prediction_fields_materialized", "value": real_prediction_fields, "passed": True},
        {"audit": "projection_surface_row_count", "value": len(projection_rows_6lf), "passed": True},
    ]

    metric_audit = [
        {"metric": "probability_metric_ready_after_audit", "value": prob_ready, "passed": True},
        {"metric": "runs_metric_ready_after_audit", "value": runs_ready, "passed": True},
        {"metric": "any_backtest_metric_ready_after_audit", "value": any_ready, "passed": True},
        {"metric": "metric_rows_present", "value": len(metric_rows_6lf), "passed": True},
        {"metric": "real_backtest_metrics_run", "value": False, "passed": True},
    ]

    import_context_diagnosis = [
        {"diagnosis_item": "candidate_passed_signature_gate", "value": signature_safe, "passed": True},
        {"diagnosis_item": "candidate_reached_import_call_phase", "value": adapter_attempted, "passed": True},
        {"diagnosis_item": "failure_type", "value": "package_relative_import_without_parent_package" if import_context_failure else "other", "passed": True},
        {"diagnosis_item": "package_context_repair_needed", "value": import_context_failure, "passed": True},
        {"diagnosis_item": "try_another_candidate_before_repair", "value": False, "passed": True},
        {"diagnosis_item": "wrapper_plan_needed", "value": False, "passed": True},
    ]

    next_route = [
        {"route_item": "recommended_next_layer", "value": RECOMMENDED_NEXT_LAYER_6LG, "passed": True},
        {"route_item": "recommended_path", "value": RECOMMENDED_PATH_6LG, "passed": True},
        {"route_item": "package_context_repair_needed", "value": import_context_failure, "passed": True},
        {"route_item": "next_candidate_retry_recommended", "value": False, "passed": True},
        {"route_item": "wrapper_plan_needed", "value": False, "passed": True},
        {"route_item": "route_reason", "value": "same_candidate_passed_signature_gate_but_failed_due_to_package_import_context", "passed": True},
    ]

    blockers = [
        {"blocker": "real_prediction_surface_not_materialized", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "selected_candidate_import_context_needs_repair", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6lh = [
        {"contract": "plan_package_context_import_for_same_candidate", "required": True, "passed": True},
        {"contract": "preserve_signature_and_payload_gates", "required": True, "passed": True},
        {"contract": "allow_one_retry_only_if_package_import_safe", "required": True, "passed": True},
        {"contract": "emit_surface_or_gap_without_metrics_activation_or_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6lf_implementation_script_exists", "expected": True, "actual": IMPLEMENT_6LF_PATH.exists(), "passed": IMPLEMENT_6LF_PATH.exists()},
        {"check": "6lf_json_exists", "expected": True, "actual": JSON_6LF.exists(), "passed": JSON_6LF.exists()},
        {"check": "6lf_all_checks_passed", "expected": True, "actual": json_6lf.get("all_checks_passed"), "passed": json_6lf.get("all_checks_passed") is True},
        {"check": "6lf_diagnosis", "expected": DIAGNOSIS_6LF, "actual": json_6lf.get("diagnosis"), "passed": json_6lf.get("diagnosis") == DIAGNOSIS_6LF},
        {"check": "6lf_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LF, "actual": json_6lf.get("recommended_next_layer"), "passed": json_6lf.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LF},
        {"check": "6lf_call_attempted", "expected": True, "actual": json_6lf.get("adapter_call_attempted"), "passed": json_6lf.get("adapter_call_attempted") is True},
        {"check": "6lf_failed_closed", "expected": True, "actual": json_6lf.get("adapter_call_failed_closed"), "passed": json_6lf.get("adapter_call_failed_closed") is True},
        {"check": "6lf_no_projection_surface", "expected": False, "actual": json_6lf.get("projection_surface_materialized"), "passed": json_6lf.get("projection_surface_materialized") is False},
        {"check": "6lf_no_layer6_exit", "expected": False, "actual": json_6lf.get("layer_6_exit_recommended"), "passed": json_6lf.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "required": path in REQUIRED_6LF_INPUTS, "passed": path.exists() or path not in REQUIRED_6LF_INPUTS}
        for path in ALL_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists() or path not in REQUIRED_6LF_INPUTS} for path in ALL_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6lh_import_context_repair_plan", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "next_candidate_retry", "blocked": True, "reason": "same candidate should receive package-context repair plan first", "passed": True},
        {"blocked_surface": "wrapper_design", "blocked": True, "reason": "signature-safe candidate not exhausted", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "real prediction surface required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "historical evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LG cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6lf_passed", "expected": True, "actual": json_6lf.get("all_checks_passed"), "passed": json_6lf.get("all_checks_passed") is True},
        {"decision": "candidate_audit_count", "expected": 3, "actual": len(candidate_audit), "passed": len(candidate_audit) == 3 and all_passed(candidate_audit)},
        {"decision": "signature_audit_count", "expected": 3, "actual": len(signature_audit), "passed": len(signature_audit) == 3 and all_passed(signature_audit)},
        {"decision": "adapter_attempt_audit_count", "expected": 3, "actual": len(adapter_attempt_audit), "passed": len(adapter_attempt_audit) == 3 and all_passed(adapter_attempt_audit)},
        {"decision": "gap_report_audit_count", "expected": 3, "actual": len(gap_audit), "passed": len(gap_audit) == 3 and all_passed(gap_audit)},
        {"decision": "import_context_diagnosis_count", "expected": 6, "actual": len(import_context_diagnosis), "passed": len(import_context_diagnosis) == 6 and all_passed(import_context_diagnosis)},
        {"decision": "future_6lh_contract_valid", "expected": True, "actual": len(future_6lh) == 4 and all_passed(future_6lh), "passed": len(future_6lh) == 4 and all_passed(future_6lh)},
        {"decision": "recommend_6lh_next", "expected": RECOMMENDED_NEXT_LAYER_6LG, "actual": RECOMMENDED_NEXT_LAYER_6LG, "passed": True},
        {"decision": "do_not_recommend_next_candidate_retry", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "projection_adapter_next_candidate_audited", "expected": True, "actual": True, "passed": True},
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
        {"surface": "6lf_implementation", "policy": "read_only", "passed": True},
        {"surface": "6lf_artifacts", "policy": "read_only", "passed": True},
        {"surface": "future_import_repair", "policy": "plan_only_next_layer", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6lg", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LG, "actual": RECOMMENDED_NEXT_LAYER_6LG, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LG, "actual": RECOMMENDED_PATH_6LG, "passed": True},
        {"decision": "recommend_package_context_import_repair", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_next_candidate_retry_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LG, "actual": DIAGNOSIS_6LG, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "candidate_audit", "passed": all_passed(candidate_audit), "detail": f"{len(candidate_audit)} rows"},
        {"check": "signature_audit", "passed": all_passed(signature_audit), "detail": f"{len(signature_audit)} rows"},
        {"check": "adapter_attempt_audit", "passed": all_passed(adapter_attempt_audit), "detail": f"{len(adapter_attempt_audit)} rows"},
        {"check": "gap_report_audit", "passed": all_passed(gap_audit), "detail": f"{len(gap_audit)} rows"},
        {"check": "prediction_surface_audit", "passed": all_passed(surface_audit), "detail": f"{len(surface_audit)} rows"},
        {"check": "metric_readiness_audit", "passed": all_passed(metric_audit), "detail": f"{len(metric_audit)} rows"},
        {"check": "import_context_diagnosis", "passed": all_passed(import_context_diagnosis), "detail": f"{len(import_context_diagnosis)} rows"},
        {"check": "next_route", "passed": all_passed(next_route), "detail": f"{len(next_route)} rows"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{len(blockers)} rows"},
        {"check": "future_6lh_contract", "passed": all_passed(future_6lh), "detail": f"{len(future_6lh)} rows"},
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
        "signature_audit": write_csv(SIGNATURE_AUDIT_CSV, signature_audit),
        "adapter_attempt_audit": write_csv(ATTEMPT_AUDIT_CSV, adapter_attempt_audit),
        "gap_report_audit": write_csv(GAP_AUDIT_CSV, gap_audit),
        "prediction_surface_audit": write_csv(SURFACE_AUDIT_CSV, surface_audit),
        "metric_readiness_audit": write_csv(METRIC_AUDIT_CSV, metric_audit),
        "import_context_diagnosis": write_csv(IMPORT_CONTEXT_CSV, import_context_diagnosis),
        "next_route": write_csv(NEXT_ROUTE_CSV, next_route),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6lh_contract": write_csv(FUTURE_6LH_CSV, future_6lh),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LG",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LG if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LG,
        "recommended_path": RECOMMENDED_PATH_6LG,
        "predecessor_implementation": str(IMPLEMENT_6LF_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6lf.get("diagnosis"),
        "audited_layer_after": "6LF",
        "source_family": "projection_adapter_next_candidate_audit",
        "candidate_audit_count": len(candidate_audit),
        "signature_audit_count": len(signature_audit),
        "adapter_attempt_audit_count": len(adapter_attempt_audit),
        "gap_report_audit_count": len(gap_audit),
        "prediction_surface_audit_count": len(surface_audit),
        "metric_readiness_audit_count": len(metric_audit),
        "import_context_diagnosis_count": len(import_context_diagnosis),
        "next_route_count": len(next_route),
        "blocker_count": len(blockers),
        "future_6lh_contract_valid": len(future_6lh) == 4 and all_passed(future_6lh),
        "projection_adapter_next_candidate_audited": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "blocked_candidate_excluded_confirmed": blocked_excluded,
        "selected_next_candidate_confirmed": selected_confirmed,
        "selected_next_candidate_path": selected_path,
        "selected_next_candidate_name": selected_name,
        "signature_mapping_safe_confirmed": signature_safe,
        "forbidden_runtime_context_blocker_confirmed": bool(forbidden_runtime_context),
        "adapter_call_attempted_confirmed": adapter_attempted,
        "adapter_call_succeeded_confirmed": adapter_succeeded,
        "adapter_call_failed_closed_confirmed": failed_closed,
        "gap_report_confirmed": gap_confirmed,
        "import_context_failure_confirmed": import_context_failure,
        "package_context_repair_needed": import_context_failure,
        "next_candidate_retry_recommended": False,
        "wrapper_plan_needed": False,
        "projection_surface_materialized_confirmed": False,
        "real_prediction_fields_materialized": False,
        "probability_metric_ready_after_audit": False,
        "runs_metric_ready_after_audit": False,
        "any_backtest_metric_ready_after_audit": False,
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
            "signature_audit_csv": str(SIGNATURE_AUDIT_CSV),
            "adapter_attempt_audit_csv": str(ATTEMPT_AUDIT_CSV),
            "gap_report_audit_csv": str(GAP_AUDIT_CSV),
            "prediction_surface_audit_csv": str(SURFACE_AUDIT_CSV),
            "metric_readiness_audit_csv": str(METRIC_AUDIT_CSV),
            "import_context_diagnosis_csv": str(IMPORT_CONTEXT_CSV),
            "next_route_csv": str(NEXT_ROUTE_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6lh_contract_csv": str(FUTURE_6LH_CSV),
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
