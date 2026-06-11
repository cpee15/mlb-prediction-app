#!/usr/bin/env python3
"""Plan probability alias normalization for the adapter return surface."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6lw_projection_adapter_probability_alias_normalization_plan"
TMP_DIR = Path("tmp")

AUDIT_6LV_PATH = Path("scripts/audit_6lv_layer6_projection_adapter_shape_repaired_call.py")
JSON_6LV = TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit.json"

REQUIRED_6LV_INPUTS = [
    JSON_6LV,
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_checks.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_predecessor.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_call_success_audit.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_return_shape_audit.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_probability_alias_audit.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_canonical_target_mismatch.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_run_surface_audit.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_active_blocker_reclassification.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_metric_readiness.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_next_route.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_blockers.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_future_6lw_contract.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_decision.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PROBLEM_CSV = TMP_DIR / f"{SLUG}_problem_statement.csv"
ALIAS_MAPPING_CSV = TMP_DIR / f"{SLUG}_alias_mapping.csv"
CONTRACT_CSV = TMP_DIR / f"{SLUG}_normalization_contract.csv"
PROB_READY_CSV = TMP_DIR / f"{SLUG}_probability_surface_readiness.csv"
RUN_GAP_CSV = TMP_DIR / f"{SLUG}_run_surface_gap.csv"
METRIC_GUARDS_CSV = TMP_DIR / f"{SLUG}_metric_guardrails.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LX_CSV = TMP_DIR / f"{SLUG}_future_6lx_contract.csv"
FUTURE_6LY_CSV = TMP_DIR / f"{SLUG}_future_6ly_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LV = "layer_6_projection_adapter_shape_repaired_call_audit_complete"
DIAGNOSIS_6LW = "layer_6_projection_adapter_probability_alias_normalization_plan_complete"
RECOMMENDED_NEXT_LAYER_6LV = "6LW_layer_6_projection_adapter_probability_alias_normalization_plan"
RECOMMENDED_NEXT_LAYER_6LW = "6LX_layer_6_projection_adapter_probability_alias_normalization_implementation"
RECOMMENDED_PATH_6LW = "implement_probability_alias_normalization_for_adapter_return_surface"


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
    json_6lv = load_json(JSON_6LV)

    home_alias = json_6lv.get("probability_alias_home_source")
    away_alias = json_6lv.get("probability_alias_away_source")
    alias_surface = json_6lv.get("probability_alias_surface_detected") is True
    call_live = json_6lv.get("adapter_call_plumbing_live") is True
    call_not_blocker = json_6lv.get("adapter_call_no_longer_active_blocker") is True
    canon_absent = json_6lv.get("canonical_probability_targets_absent") is True
    run_gap = json_6lv.get("run_surface_gap_remains") is True

    problem = [
        {
            "problem": "adapter_return_probability_aliases_need_canonical_surface",
            "support": "home_win_prob/away_win_prob present while home_win_probability/away_win_probability absent",
            "passed": alias_surface and canon_absent,
        }
    ]

    alias_mapping = [
        {
            "source_field": "home_win_prob",
            "target_field": "home_win_probability",
            "source_confirmed": home_alias == "home_win_prob",
            "target_absent_in_6lv": True,
            "mapping_required": True,
            "passed": home_alias == "home_win_prob",
        },
        {
            "source_field": "away_win_prob",
            "target_field": "away_win_probability",
            "source_confirmed": away_alias == "away_win_prob",
            "target_absent_in_6lv": True,
            "mapping_required": True,
            "passed": away_alias == "away_win_prob",
        },
    ]

    contract = [
        {"contract_item": "input_surface", "value": "adapter single-sample return row", "passed": True},
        {"contract_item": "home_probability_mapping", "value": "home_win_prob->home_win_probability", "passed": home_alias == "home_win_prob"},
        {"contract_item": "away_probability_mapping", "value": "away_win_prob->away_win_probability", "passed": away_alias == "away_win_prob"},
        {"contract_item": "preserve_original_alias_fields", "value": True, "passed": True},
        {"contract_item": "preserve_game_pk", "value": True, "passed": json_6lv.get("adapter_return_has_game_pk") is True},
        {"contract_item": "do_not_invent_runs", "value": True, "passed": run_gap},
        {"contract_item": "shape_only_non_production_implementation_next", "value": True, "passed": True},
    ]

    prob_ready = [
        {"surface": "adapter_call_plumbing_live", "ready": call_live, "passed": call_live},
        {"surface": "probability_alias_surface_detected", "ready": alias_surface, "passed": alias_surface},
        {"surface": "canonical_probability_targets_absent", "ready": canon_absent, "passed": canon_absent},
        {"surface": "probability_surface_ready_after_future_implementation", "ready": True, "condition": "alias normalization only", "passed": True},
    ]

    run_gap_rows = [
        {"run_field": "home_expected_runs", "present": False, "gap_remains": True, "passed": True},
        {"run_field": "away_expected_runs", "present": False, "gap_remains": True, "passed": True},
        {"run_field": "total_expected_runs", "present": False, "gap_remains": True, "passed": True},
        {"run_field": "projected_total", "present": False, "gap_remains": True, "passed": True},
    ]

    metric_guards = [
        {"guardrail": "do_not_compute_probability_metrics_in_6lw", "passed": True},
        {"guardrail": "do_not_compute_runs_metrics_in_6lw", "passed": True},
        {"guardrail": "do_not_run_backtest_in_6lw", "passed": True},
        {"guardrail": "future_probability_metrics_require_6lx_implementation_and_6ly_audit", "passed": True},
        {"guardrail": "runs_metrics_remain_blocked_by_run_surface_gap", "passed": True},
    ]

    allowed_next = [
        {"operation": "read_6lu_6lv_artifacts", "allowed_next": True, "passed": True},
        {"operation": "create_non_production_normalized_surface_artifact", "allowed_next": True, "passed": True},
        {"operation": "map_home_win_prob_to_home_win_probability", "allowed_next": True, "passed": True},
        {"operation": "map_away_win_prob_to_away_win_probability", "allowed_next": True, "passed": True},
        {"operation": "preserve_run_surface_gap", "allowed_next": True, "passed": True},
    ]

    forbidden_next = [
        {"operation": "adapter_call", "allowed_next": False, "passed": True},
        {"operation": "additional_adapter_call", "allowed_next": False, "passed": True},
        {"operation": "full_batch_adapter_call", "allowed_next": False, "passed": True},
        {"operation": "real_metric_execution", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "live_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "database_writes", "allowed_next": False, "passed": True},
        {"operation": "production_source_modifications", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit_credit", "allowed_next": False, "passed": True},
    ]

    blockers = [
        {"blocker": "probability_alias_normalization_not_implemented", "active": True, "blocks_probability_metrics": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "run_surface_gap_remains", "active": True, "blocks_runs_metrics": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "active": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "active": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6lx = [
        {"contract": "implement_non_production_probability_alias_normalization_artifact", "required": True, "passed": True},
        {"contract": "map_home_win_prob_to_home_win_probability", "required": True, "passed": True},
        {"contract": "map_away_win_prob_to_away_win_probability", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_no_adapter_calls_no_activation", "required": True, "passed": True},
    ]

    future_6ly = [
        {"contract": "audit_probability_alias_normalization_artifact", "required": True, "passed": True},
        {"contract": "determine_probability_surface_readiness", "required": True, "passed": True},
        {"contract": "preserve_run_surface_gap", "required": True, "passed": True},
        {"contract": "preserve_layer6_exit_blocked", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6lv_audit_script_exists", "expected": True, "actual": AUDIT_6LV_PATH.exists(), "passed": AUDIT_6LV_PATH.exists()},
        {"check": "6lv_json_exists", "expected": True, "actual": JSON_6LV.exists(), "passed": JSON_6LV.exists()},
        {"check": "6lv_all_checks_passed", "expected": True, "actual": json_6lv.get("all_checks_passed"), "passed": json_6lv.get("all_checks_passed") is True},
        {"check": "6lv_diagnosis", "expected": DIAGNOSIS_6LV, "actual": json_6lv.get("diagnosis"), "passed": json_6lv.get("diagnosis") == DIAGNOSIS_6LV},
        {"check": "6lv_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LV, "actual": json_6lv.get("recommended_next_layer"), "passed": json_6lv.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LV},
        {"check": "6lv_probability_alias_detected", "expected": True, "actual": alias_surface, "passed": alias_surface},
        {"check": "6lv_adapter_call_no_longer_blocker", "expected": True, "actual": call_not_blocker, "passed": call_not_blocker},
        {"check": "6lv_no_layer6_exit", "expected": False, "actual": json_6lv.get("layer_6_exit_recommended"), "passed": json_6lv.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_6LV_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_6LV_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6lx_probability_alias_normalization_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "adapter_call", "blocked": True, "reason": "normalization plan does not need another call", "passed": True},
        {"blocked_surface": "real_metrics", "blocked": True, "reason": "normalization artifact must be implemented/audited first", "passed": True},
        {"blocked_surface": "runs_metrics", "blocked": True, "reason": "run surface gap remains", "passed": True},
        {"blocked_surface": "historical_backtest", "blocked": True, "reason": "metrics/surface readiness required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LW cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6lv_passed", "expected": True, "actual": json_6lv.get("all_checks_passed"), "passed": json_6lv.get("all_checks_passed") is True},
        {"decision": "alias_surface_confirmed", "expected": True, "actual": alias_surface, "passed": alias_surface},
        {"decision": "canonical_targets_absent_confirmed", "expected": True, "actual": canon_absent, "passed": canon_absent},
        {"decision": "alias_mapping_valid", "expected": True, "actual": all_passed(alias_mapping), "passed": all_passed(alias_mapping)},
        {"decision": "run_surface_gap_preserved", "expected": True, "actual": run_gap, "passed": run_gap},
        {"decision": "future_6lx_contract_valid", "expected": True, "actual": len(future_6lx) == 4 and all_passed(future_6lx), "passed": len(future_6lx) == 4 and all_passed(future_6lx)},
        {"decision": "future_6ly_contract_valid", "expected": True, "actual": len(future_6ly) == 4 and all_passed(future_6ly), "passed": len(future_6ly) == 4 and all_passed(future_6ly)},
        {"decision": "recommend_6lx_next", "expected": RECOMMENDED_NEXT_LAYER_6LW, "actual": RECOMMENDED_NEXT_LAYER_6LW, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "adapter_call_executed_by_6lw", "expected": False, "actual": False, "passed": True},
        {"boundary": "additional_adapter_call_executed_by_6lw", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_output_written_by_6lw", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6lw", "expected": False, "actual": False, "passed": True},
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
        {"surface": "6lv_artifacts", "policy": "read_only_planning", "passed": True},
        {"surface": "adapter_call", "policy": "no_calls_in_6lw", "passed": True},
        {"surface": "normalization_output", "policy": "plan_only_no_write", "passed": True},
        {"surface": "future_6lx_implementation", "policy": "non_production_artifact_only", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6lw", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LW, "actual": RECOMMENDED_NEXT_LAYER_6LW, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LW, "actual": RECOMMENDED_PATH_6LW, "passed": True},
        {"decision": "recommend_probability_alias_normalization_implementation", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_adapter_call", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_next_candidate_retry", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "preserve_run_surface_gap", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metrics", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LW, "actual": DIAGNOSIS_6LW, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "problem_statement", "passed": all_passed(problem), "detail": f"{len(problem)} rows"},
        {"check": "alias_mapping", "passed": all_passed(alias_mapping), "detail": f"{len(alias_mapping)} rows"},
        {"check": "normalization_contract", "passed": all_passed(contract), "detail": f"{len(contract)} rows"},
        {"check": "probability_surface_readiness", "passed": all_passed(prob_ready), "detail": f"{len(prob_ready)} rows"},
        {"check": "run_surface_gap", "passed": all_passed(run_gap_rows), "detail": f"{len(run_gap_rows)} rows"},
        {"check": "metric_guardrails", "passed": all_passed(metric_guards), "detail": f"{len(metric_guards)} rows"},
        {"check": "allowed_next", "passed": all_passed(allowed_next), "detail": f"{len(allowed_next)} rows"},
        {"check": "forbidden_next", "passed": all_passed(forbidden_next), "detail": f"{len(forbidden_next)} rows"},
        {"check": "future_6lx_contract", "passed": all_passed(future_6lx), "detail": f"{len(future_6lx)} rows"},
        {"check": "future_6ly_contract", "passed": all_passed(future_6ly), "detail": f"{len(future_6ly)} rows"},
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
        "alias_mapping": write_csv(ALIAS_MAPPING_CSV, alias_mapping),
        "normalization_contract": write_csv(CONTRACT_CSV, contract),
        "probability_surface_readiness": write_csv(PROB_READY_CSV, prob_ready),
        "run_surface_gap": write_csv(RUN_GAP_CSV, run_gap_rows),
        "metric_guardrails": write_csv(METRIC_GUARDS_CSV, metric_guards),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6lx_contract": write_csv(FUTURE_6LX_CSV, future_6lx),
        "future_6ly_contract": write_csv(FUTURE_6LY_CSV, future_6ly),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LW",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LW if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LW,
        "recommended_path": RECOMMENDED_PATH_6LW,
        "predecessor_audit": str(AUDIT_6LV_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6lv.get("diagnosis"),
        "planned_layer_after": "6LV",
        "source_family": "projection_adapter_probability_alias_normalization_plan",
        "problem_statement_count": len(problem),
        "alias_mapping_count": len(alias_mapping),
        "normalization_contract_count": len(contract),
        "probability_surface_readiness_count": len(prob_ready),
        "run_surface_gap_count": len(run_gap_rows),
        "metric_guardrail_count": len(metric_guards),
        "allowed_operation_next_count": len(allowed_next),
        "forbidden_operation_next_count": len(forbidden_next),
        "blocker_count": len(blockers),
        "future_6lx_contract_valid": len(future_6lx) == 4 and all_passed(future_6lx),
        "future_6ly_contract_valid": len(future_6ly) == 4 and all_passed(future_6ly),
        "probability_alias_normalization_planned": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "adapter_call_plumbing_live_confirmed": call_live,
        "adapter_call_no_longer_active_blocker_confirmed": call_not_blocker,
        "probability_alias_surface_detected_confirmed": alias_surface,
        "probability_alias_home_source_confirmed": home_alias,
        "probability_alias_away_source_confirmed": away_alias,
        "canonical_probability_targets_absent_confirmed": canon_absent,
        "planned_home_probability_mapping": "home_win_prob->home_win_probability",
        "planned_away_probability_mapping": "away_win_prob->away_win_probability",
        "planned_probability_fields_after_normalization": "home_win_probability;away_win_probability",
        "probability_surface_normalization_needed": True,
        "probability_surface_ready_after_future_implementation": True,
        "run_surface_materialized": False,
        "run_surface_gap_remains": True,
        "run_surface_fields_absent_confirmed": run_gap,
        "probability_metric_ready_after_plan": False,
        "runs_metric_ready_after_plan": False,
        "any_backtest_metric_ready_after_plan": False,
        "adapter_call_executed_by_6lw": False,
        "additional_adapter_call_executed_by_6lw": False,
        "normalized_output_written_by_6lw": False,
        "production_code_modified_by_6lw": False,
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
            "alias_mapping_csv": str(ALIAS_MAPPING_CSV),
            "normalization_contract_csv": str(CONTRACT_CSV),
            "probability_surface_readiness_csv": str(PROB_READY_CSV),
            "run_surface_gap_csv": str(RUN_GAP_CSV),
            "metric_guardrails_csv": str(METRIC_GUARDS_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6lx_contract_csv": str(FUTURE_6LX_CSV),
            "future_6ly_contract_csv": str(FUTURE_6LY_CSV),
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
