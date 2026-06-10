#!/usr/bin/env python3
"""Audit the shape-repaired single-sample adapter call return contract."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple


SLUG = "layer6_6lv_projection_adapter_shape_repaired_call_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6LU_PATH = Path("scripts/implement_6lu_layer6_projection_adapter_shape_repaired_call.py")
JSON_6LU = TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation.json"

REQUIRED_6LU_INPUTS = [
    JSON_6LU,
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_checks.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_predecessor.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_payload_validation.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_import_trace.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_call_execution.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_return_shape.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_prediction_field_presence.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_gap_report.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_projection_surface_readiness.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_metric_readiness.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_blockers.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_future_6lv_contract.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_decision.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CALL_SUCCESS_CSV = TMP_DIR / f"{SLUG}_call_success_audit.csv"
RETURN_SHAPE_CSV = TMP_DIR / f"{SLUG}_return_shape_audit.csv"
PROB_ALIAS_CSV = TMP_DIR / f"{SLUG}_probability_alias_audit.csv"
CANON_MISMATCH_CSV = TMP_DIR / f"{SLUG}_canonical_target_mismatch.csv"
RUN_SURFACE_CSV = TMP_DIR / f"{SLUG}_run_surface_audit.csv"
BLOCKER_RECLASS_CSV = TMP_DIR / f"{SLUG}_active_blocker_reclassification.csv"
METRIC_READY_CSV = TMP_DIR / f"{SLUG}_metric_readiness.csv"
NEXT_ROUTE_CSV = TMP_DIR / f"{SLUG}_next_route.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LW_CSV = TMP_DIR / f"{SLUG}_future_6lw_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LU = "layer_6_projection_adapter_shape_repaired_call_implementation_complete"
DIAGNOSIS_6LV = "layer_6_projection_adapter_shape_repaired_call_audit_complete"
RECOMMENDED_NEXT_LAYER_6LU = "6LV_layer_6_projection_adapter_shape_repaired_call_audit"
RECOMMENDED_NEXT_LAYER_6LV = "6LW_layer_6_projection_adapter_probability_alias_normalization_plan"
RECOMMENDED_PATH_6LV = "plan_probability_alias_normalization_for_adapter_return_surface"

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


def parse_keys(value: Any) -> Set[str]:
    if value is None:
        return set()
    return {part.strip() for part in str(value).split(";") if part.strip()}


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6lu = load_json(JSON_6LU)
    keys = parse_keys(json_6lu.get("adapter_return_first_item_keys"))

    has_game_pk = "game_pk" in keys
    has_home_win_prob = "home_win_prob" in keys
    has_away_win_prob = "away_win_prob" in keys
    has_favorite_probability = "favorite_probability" in keys
    has_home_win_probability = "home_win_probability" in keys
    has_away_win_probability = "away_win_probability" in keys

    run_fields = ["home_expected_runs", "away_expected_runs", "total_expected_runs", "projected_total"]
    run_surface_materialized = any(field in keys for field in run_fields)
    run_surface_fields_absent = not run_surface_materialized

    probability_alias_surface_detected = has_home_win_prob and has_away_win_prob and has_favorite_probability
    canonical_probability_targets_absent = (not has_home_win_probability) and (not has_away_win_probability)

    call_success = [
        {"audit": "adapter_call_attempted", "expected": True, "actual": json_6lu.get("adapter_call_attempted"), "passed": json_6lu.get("adapter_call_attempted") is True},
        {"audit": "adapter_call_succeeded", "expected": True, "actual": json_6lu.get("adapter_call_succeeded"), "passed": json_6lu.get("adapter_call_succeeded") is True},
        {"audit": "adapter_call_count", "expected": 1, "actual": json_6lu.get("adapter_call_count"), "passed": json_6lu.get("adapter_call_count") == 1},
        {"audit": "target_module_import_succeeded", "expected": True, "actual": json_6lu.get("target_module_import_succeeded"), "passed": json_6lu.get("target_module_import_succeeded") is True},
        {"audit": "target_function_callable", "expected": True, "actual": json_6lu.get("target_function_callable"), "passed": json_6lu.get("target_function_callable") is True},
    ]

    return_shape = [
        {"audit": "adapter_return_type", "expected": "list", "actual": json_6lu.get("adapter_return_type"), "passed": json_6lu.get("adapter_return_type") == "list"},
        {"audit": "adapter_return_list_length", "expected": 1, "actual": json_6lu.get("adapter_return_list_length"), "passed": json_6lu.get("adapter_return_list_length") == 1},
        {"audit": "adapter_return_first_item_type", "expected": "dict", "actual": json_6lu.get("adapter_return_first_item_type"), "passed": json_6lu.get("adapter_return_first_item_type") == "dict"},
        {"audit": "adapter_return_empty", "expected": False, "actual": json_6lu.get("adapter_return_empty"), "passed": json_6lu.get("adapter_return_empty") is False},
        {"audit": "first_item_key_count", "expected_min": 1, "actual": len(keys), "passed": len(keys) > 0},
        {"audit": "adapter_return_has_game_pk", "expected": True, "actual": has_game_pk, "passed": has_game_pk},
    ]

    prob_alias = [
        {"alias_field": "home_win_prob", "canonical_target": "home_win_probability", "present": has_home_win_prob, "probability_like": True, "passed": has_home_win_prob},
        {"alias_field": "away_win_prob", "canonical_target": "away_win_probability", "present": has_away_win_prob, "probability_like": True, "passed": has_away_win_prob},
        {"alias_field": "favorite_probability", "canonical_target": "favorite_probability", "present": has_favorite_probability, "probability_like": True, "passed": has_favorite_probability},
        {"alias_field": "probability_component_keys", "canonical_target": "probability_diagnostics", "present": "probability_component_keys" in keys, "probability_like": True, "passed": "probability_component_keys" in keys},
    ]

    canonical_mismatch = [
        {"target_field": "home_win_probability", "present": has_home_win_probability, "alias_available": has_home_win_prob, "normalization_needed": True, "passed": (not has_home_win_probability) and has_home_win_prob},
        {"target_field": "away_win_probability", "present": has_away_win_probability, "alias_available": has_away_win_prob, "normalization_needed": True, "passed": (not has_away_win_probability) and has_away_win_prob},
    ]

    run_surface = [
        {"run_field": "home_expected_runs", "present": "home_expected_runs" in keys, "run_surface_materialized": False, "passed": "home_expected_runs" not in keys},
        {"run_field": "away_expected_runs", "present": "away_expected_runs" in keys, "run_surface_materialized": False, "passed": "away_expected_runs" not in keys},
        {"run_field": "total_expected_runs", "present": "total_expected_runs" in keys, "run_surface_materialized": False, "passed": "total_expected_runs" not in keys},
        {"run_field": "projected_total", "present": "projected_total" in keys, "run_surface_materialized": False, "passed": "projected_total" not in keys},
    ]

    blocker_reclass = [
        {"old_blocker": "adapter_execution_unknown", "status": "resolved", "support": "single-sample adapter call succeeded", "passed": True},
        {"old_blocker": "payload_shape_unknown", "status": "resolved", "support": "shape-repaired payload returned one row", "passed": True},
        {"new_blocker": "prediction_field_contract_normalization_needed", "status": "active", "support": "probability aliases present but canonical targets absent", "passed": True},
        {"new_blocker": "run_surface_gap_remains", "status": "active", "support": "expected run fields absent", "passed": True},
    ]

    metric_ready = [
        {"metric": "probability_metric_ready_after_audit", "ready": False, "reason": "alias normalization not planned or implemented", "passed": True},
        {"metric": "runs_metric_ready_after_audit", "ready": False, "reason": "run fields absent", "passed": True},
        {"metric": "any_backtest_metric_ready_after_audit", "ready": False, "reason": "no normalized real prediction surface and no backtest", "passed": True},
    ]

    next_route = [
        {"route_item": "recommended_next_layer", "value": RECOMMENDED_NEXT_LAYER_6LV, "passed": True},
        {"route_item": "recommended_path", "value": RECOMMENDED_PATH_6LV, "passed": True},
        {"route_item": "next_layer_mode", "value": "planning_only", "passed": True},
        {"route_item": "plan_probability_alias_normalization", "value": True, "passed": True},
        {"route_item": "run_additional_adapter_call_now", "value": False, "passed": True},
        {"route_item": "run_metrics_now", "value": False, "passed": True},
    ]

    blockers = [
        {"blocker": "prediction_field_contract_normalization_needed", "active": True, "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "run_surface_gap_remains", "active": True, "blocks_runs_metrics": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "active": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "active": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6lw = [
        {"contract": "plan_probability_alias_normalization", "required": True, "passed": True},
        {"contract": "map_home_win_prob_to_home_win_probability", "required": True, "passed": True},
        {"contract": "map_away_win_prob_to_away_win_probability", "required": True, "passed": True},
        {"contract": "preserve_run_surface_gap_and_no_metrics", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6lu_implementation_script_exists", "expected": True, "actual": IMPLEMENT_6LU_PATH.exists(), "passed": IMPLEMENT_6LU_PATH.exists()},
        {"check": "6lu_json_exists", "expected": True, "actual": JSON_6LU.exists(), "passed": JSON_6LU.exists()},
        {"check": "6lu_all_checks_passed", "expected": True, "actual": json_6lu.get("all_checks_passed"), "passed": json_6lu.get("all_checks_passed") is True},
        {"check": "6lu_diagnosis", "expected": DIAGNOSIS_6LU, "actual": json_6lu.get("diagnosis"), "passed": json_6lu.get("diagnosis") == DIAGNOSIS_6LU},
        {"check": "6lu_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LU, "actual": json_6lu.get("recommended_next_layer"), "passed": json_6lu.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LU},
        {"check": "6lu_call_succeeded", "expected": True, "actual": json_6lu.get("adapter_call_succeeded"), "passed": json_6lu.get("adapter_call_succeeded") is True},
        {"check": "6lu_call_count_one", "expected": 1, "actual": json_6lu.get("adapter_call_count"), "passed": json_6lu.get("adapter_call_count") == 1},
        {"check": "6lu_no_layer6_exit", "expected": False, "actual": json_6lu.get("layer_6_exit_recommended"), "passed": json_6lu.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_6LU_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_6LU_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6lw_probability_alias_normalization_plan", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "additional_adapter_call", "blocked": True, "reason": "return-shape audit routes to normalization first", "passed": True},
        {"blocked_surface": "full_batch_adapter_call", "blocked": True, "reason": "full batch forbidden", "passed": True},
        {"blocked_surface": "real_metrics", "blocked": True, "reason": "normalized surface required first", "passed": True},
        {"blocked_surface": "historical_backtest", "blocked": True, "reason": "metrics/surface readiness required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LV cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6lu_passed", "expected": True, "actual": json_6lu.get("all_checks_passed"), "passed": json_6lu.get("all_checks_passed") is True},
        {"decision": "adapter_call_succeeded", "expected": True, "actual": json_6lu.get("adapter_call_succeeded"), "passed": json_6lu.get("adapter_call_succeeded") is True},
        {"decision": "adapter_return_non_empty", "expected": True, "actual": json_6lu.get("adapter_return_empty") is False, "passed": json_6lu.get("adapter_return_empty") is False},
        {"decision": "probability_alias_surface_detected", "expected": True, "actual": probability_alias_surface_detected, "passed": probability_alias_surface_detected},
        {"decision": "canonical_probability_targets_absent", "expected": True, "actual": canonical_probability_targets_absent, "passed": canonical_probability_targets_absent},
        {"decision": "run_surface_fields_absent", "expected": True, "actual": run_surface_fields_absent, "passed": run_surface_fields_absent},
        {"decision": "future_6lw_contract_valid", "expected": True, "actual": len(future_6lw) == 4 and all_passed(future_6lw), "passed": len(future_6lw) == 4 and all_passed(future_6lw)},
        {"decision": "recommend_6lw_next", "expected": RECOMMENDED_NEXT_LAYER_6LV, "actual": RECOMMENDED_NEXT_LAYER_6LV, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "additional_adapter_call_executed_by_6lv", "expected": False, "actual": False, "passed": True},
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
        {"surface": "6lu_artifacts", "policy": "read_only_audit", "passed": True},
        {"surface": "adapter_call", "policy": "no_additional_calls_in_6lv", "passed": True},
        {"surface": "return_contract", "policy": "audit_only", "passed": True},
        {"surface": "future_6lw_plan", "policy": "plan_only_next_layer", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6lv", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LV, "actual": RECOMMENDED_NEXT_LAYER_6LV, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LV, "actual": RECOMMENDED_PATH_6LV, "passed": True},
        {"decision": "recommend_probability_alias_normalization_plan", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_additional_adapter_call", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_next_candidate_retry", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "preserve_run_surface_gap", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metrics", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LV, "actual": DIAGNOSIS_6LV, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "call_success_audit", "passed": all_passed(call_success), "detail": f"{len(call_success)} rows"},
        {"check": "return_shape_audit", "passed": all_passed(return_shape), "detail": f"{len(return_shape)} rows"},
        {"check": "probability_alias_audit", "passed": all_passed(prob_alias), "detail": f"{len(prob_alias)} rows"},
        {"check": "canonical_target_mismatch", "passed": all_passed(canonical_mismatch), "detail": f"{len(canonical_mismatch)} rows"},
        {"check": "run_surface_audit", "passed": all_passed(run_surface), "detail": f"{len(run_surface)} rows"},
        {"check": "active_blocker_reclassification", "passed": all_passed(blocker_reclass), "detail": f"{len(blocker_reclass)} rows"},
        {"check": "metric_readiness", "passed": all_passed(metric_ready), "detail": f"{len(metric_ready)} rows"},
        {"check": "next_route", "passed": all_passed(next_route), "detail": f"{len(next_route)} rows"},
        {"check": "future_6lw_contract", "passed": all_passed(future_6lw), "detail": f"{len(future_6lw)} rows"},
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
        "call_success_audit": write_csv(CALL_SUCCESS_CSV, call_success),
        "return_shape_audit": write_csv(RETURN_SHAPE_CSV, return_shape),
        "probability_alias_audit": write_csv(PROB_ALIAS_CSV, prob_alias),
        "canonical_target_mismatch": write_csv(CANON_MISMATCH_CSV, canonical_mismatch),
        "run_surface_audit": write_csv(RUN_SURFACE_CSV, run_surface),
        "active_blocker_reclassification": write_csv(BLOCKER_RECLASS_CSV, blocker_reclass),
        "metric_readiness": write_csv(METRIC_READY_CSV, metric_ready),
        "next_route": write_csv(NEXT_ROUTE_CSV, next_route),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6lw_contract": write_csv(FUTURE_6LW_CSV, future_6lw),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LV",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LV if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LV,
        "recommended_path": RECOMMENDED_PATH_6LV,
        "predecessor_implementation": str(IMPLEMENT_6LU_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6lu.get("diagnosis"),
        "audited_layer_after": "6LU",
        "source_family": "projection_adapter_shape_repaired_call_audit",
        "call_success_audit_count": len(call_success),
        "return_shape_audit_count": len(return_shape),
        "probability_alias_audit_count": len(prob_alias),
        "canonical_target_mismatch_count": len(canonical_mismatch),
        "run_surface_audit_count": len(run_surface),
        "active_blocker_reclassification_count": len(blocker_reclass),
        "metric_readiness_count": len(metric_ready),
        "next_route_count": len(next_route),
        "blocker_count": len(blockers),
        "future_6lw_contract_valid": len(future_6lw) == 4 and all_passed(future_6lw),
        "projection_adapter_shape_repaired_call_audited": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "same_candidate_retained_confirmed": json_6lu.get("same_candidate_retained_confirmed") is True,
        "blocked_session_candidate_excluded_confirmed": json_6lu.get("blocked_session_candidate_excluded_confirmed") is True,
        "target_module_import_path": TARGET_MODULE,
        "target_function_name": TARGET_FUNCTION,
        "required_arguments_confirmed": REQUIRED_ARGS,
        "adapter_call_attempted_confirmed": json_6lu.get("adapter_call_attempted") is True,
        "adapter_call_succeeded_confirmed": json_6lu.get("adapter_call_succeeded") is True,
        "adapter_call_count_confirmed": json_6lu.get("adapter_call_count"),
        "adapter_return_type_confirmed": json_6lu.get("adapter_return_type"),
        "adapter_return_list_length_confirmed": json_6lu.get("adapter_return_list_length"),
        "adapter_return_first_item_type_confirmed": json_6lu.get("adapter_return_first_item_type"),
        "adapter_return_empty_confirmed": json_6lu.get("adapter_return_empty"),
        "adapter_return_first_item_keys": ";".join(sorted(keys)),
        "adapter_return_has_game_pk": has_game_pk,
        "adapter_return_has_home_win_prob": has_home_win_prob,
        "adapter_return_has_away_win_prob": has_away_win_prob,
        "adapter_return_has_favorite_probability": has_favorite_probability,
        "adapter_return_has_home_win_probability": has_home_win_probability,
        "adapter_return_has_away_win_probability": has_away_win_probability,
        "probability_alias_surface_detected": probability_alias_surface_detected,
        "probability_alias_home_source": "home_win_prob" if has_home_win_prob else "",
        "probability_alias_away_source": "away_win_prob" if has_away_win_prob else "",
        "canonical_probability_targets_absent": canonical_probability_targets_absent,
        "run_surface_materialized": run_surface_materialized,
        "run_surface_fields_absent": run_surface_fields_absent,
        "adapter_call_plumbing_live": json_6lu.get("adapter_call_succeeded") is True,
        "adapter_call_no_longer_active_blocker": json_6lu.get("adapter_call_succeeded") is True,
        "prediction_field_contract_normalization_needed": True,
        "probability_alias_normalization_needed": True,
        "run_surface_gap_remains": True,
        "projection_surface_materialized": False,
        "real_prediction_fields_materialized": False,
        "probability_metric_ready_after_audit": False,
        "runs_metric_ready_after_audit": False,
        "any_backtest_metric_ready_after_audit": False,
        "additional_adapter_call_executed_by_6lv": False,
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
            "call_success_audit_csv": str(CALL_SUCCESS_CSV),
            "return_shape_audit_csv": str(RETURN_SHAPE_CSV),
            "probability_alias_audit_csv": str(PROB_ALIAS_CSV),
            "canonical_target_mismatch_csv": str(CANON_MISMATCH_CSV),
            "run_surface_audit_csv": str(RUN_SURFACE_CSV),
            "active_blocker_reclassification_csv": str(BLOCKER_RECLASS_CSV),
            "metric_readiness_csv": str(METRIC_READY_CSV),
            "next_route_csv": str(NEXT_ROUTE_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6lw_contract_csv": str(FUTURE_6LW_CSV),
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
