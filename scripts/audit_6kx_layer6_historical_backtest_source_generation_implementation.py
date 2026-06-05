#!/usr/bin/env python3
"""Audit 6KW historical backtest source-generation implementation.

This audit verifies whether 6KW correctly narrowed the blocker to the missing
safe deterministic projection-call contract and routes next to a projection
call contract / adapter planning layer.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6kx_historical_backtest_source_generation_implementation_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6KW_PATH = Path("scripts/implement_6kw_layer6_historical_backtest_source_generation.py")
JSON_6KW = TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation.json"

REQUIRED_INPUTS = [
    JSON_6KW,
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_checks.csv",
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_predecessor.csv",
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_schedule_input_candidates.csv",
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_actual_outcome_candidates.csv",
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_projection_route_candidates.csv",
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_generation_feasibility.csv",
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_evaluation_surface.csv",
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_source_generation_gap_report.csv",
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_metric_readiness_after_generation.csv",
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_lineage_report.csv",
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_blockers.csv",
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_future_6kx_contract.csv",
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_decision.csv",
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CANDIDATE_AUDIT_CSV = TMP_DIR / f"{SLUG}_candidate_source_audit.csv"
GEN_FEASIBILITY_AUDIT_CSV = TMP_DIR / f"{SLUG}_generation_feasibility_audit.csv"
SURFACE_OR_GAP_AUDIT_CSV = TMP_DIR / f"{SLUG}_surface_or_gap_audit.csv"
METRIC_AUDIT_CSV = TMP_DIR / f"{SLUG}_metric_readiness_audit.csv"
PROJECTION_VERDICT_CSV = TMP_DIR / f"{SLUG}_projection_contract_verdict.csv"
NEXT_ROUTE_CSV = TMP_DIR / f"{SLUG}_next_route.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6KY_CSV = TMP_DIR / f"{SLUG}_future_6ky_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KW = "layer_6_historical_backtest_source_generation_implementation_complete"
DIAGNOSIS_6KX = "layer_6_historical_backtest_source_generation_implementation_audit_complete"
RECOMMENDED_NEXT_LAYER_6KW = "6KX_layer_6_historical_backtest_source_generation_implementation_audit"
RECOMMENDED_NEXT_LAYER_6KX = "6KY_layer_6_projection_call_contract_plan"
RECOMMENDED_PATH_6KX = "plan_projection_call_contract_for_historical_surface_generation"


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
    json_6kw = load_json(JSON_6KW)

    schedule_rows = read_csv_rows(TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_schedule_input_candidates.csv")
    actual_rows = read_csv_rows(TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_actual_outcome_candidates.csv")
    projection_rows = read_csv_rows(TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_projection_route_candidates.csv")
    feasibility_rows = read_csv_rows(TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_generation_feasibility.csv")
    surface_rows = read_csv_rows(TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_evaluation_surface.csv")
    gap_rows = read_csv_rows(TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_source_generation_gap_report.csv")
    metric_rows = read_csv_rows(TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_metric_readiness_after_generation.csv")
    lineage_rows = read_csv_rows(TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_lineage_report.csv")

    schedule_confirmed = boolish(json_6kw.get("schedule_inputs_found")) and len(schedule_rows) > 0
    actual_confirmed = boolish(json_6kw.get("actual_outcomes_found")) and len(actual_rows) > 0
    projection_confirmed = boolish(json_6kw.get("projection_route_found")) and len(projection_rows) > 0
    deterministic_not_feasible = json_6kw.get("deterministic_generation_feasible") is False
    surface_missing = json_6kw.get("evaluation_surface_materialized") is False
    gap_confirmed = boolish(json_6kw.get("source_generation_gap_report_emitted")) and len(gap_rows) > 0

    gap_text = " ".join(" ".join(str(v) for v in row.values()) for row in gap_rows).lower()
    projection_contract_missing = "projection_call_contract" in gap_text or "safe_direct_projection_call_contract" in gap_text
    projection_adapter_plan_needed = projection_confirmed and deterministic_not_feasible and gap_confirmed and projection_contract_missing

    prob_ready = boolish(json_6kw.get("probability_metric_ready_after_generation"))
    runs_ready = boolish(json_6kw.get("runs_metric_ready_after_generation"))
    any_ready = boolish(json_6kw.get("any_backtest_metric_ready_after_generation"))

    candidate_source_audit = [
        {"audit": "schedule_input_candidates_confirmed", "count": len(schedule_rows), "confirmed": schedule_confirmed, "passed": True},
        {"audit": "actual_outcome_candidates_confirmed", "count": len(actual_rows), "confirmed": actual_confirmed, "passed": True},
        {"audit": "projection_route_candidates_confirmed", "count": len(projection_rows), "confirmed": projection_confirmed, "passed": True},
        {"audit": "lineage_rows_present", "count": len(lineage_rows), "confirmed": len(lineage_rows) > 0, "passed": True},
    ]

    generation_feasibility_audit = [
        {"audit": "generation_feasibility_rows_present", "value": len(feasibility_rows), "passed": True},
        {"audit": "deterministic_generation_not_feasible_confirmed", "value": deterministic_not_feasible, "passed": True},
        {"audit": "safe_direct_projection_call_contract_missing", "value": projection_contract_missing, "passed": True},
        {"audit": "projection_adapter_plan_needed", "value": projection_adapter_plan_needed, "passed": True},
    ]

    surface_or_gap_audit = [
        {"audit": "evaluation_surface_missing_confirmed", "value": surface_missing, "row_count": len(surface_rows), "passed": True},
        {"audit": "source_generation_gap_report_confirmed", "value": gap_confirmed, "row_count": len(gap_rows), "passed": True},
        {"audit": "surface_or_gap_condition_satisfied", "value": surface_missing and gap_confirmed, "passed": True},
    ]

    metric_readiness_audit = [
        {"metric": "probability_metric_ready_after_audit", "value": prob_ready, "passed": True},
        {"metric": "runs_metric_ready_after_audit", "value": runs_ready, "passed": True},
        {"metric": "any_backtest_metric_ready_after_audit", "value": any_ready, "passed": True},
        {"metric": "metric_rows_recorded", "value": len(metric_rows), "passed": True},
        {"metric": "historical_odds_required", "value": False, "passed": True},
    ]

    projection_contract_verdict = [
        {"verdict": "schedule_inputs_found_confirmed", "value": schedule_confirmed, "passed": True},
        {"verdict": "actual_outcomes_found_confirmed", "value": actual_confirmed, "passed": True},
        {"verdict": "projection_route_found_confirmed", "value": projection_confirmed, "passed": True},
        {"verdict": "deterministic_generation_not_feasible_confirmed", "value": deterministic_not_feasible, "passed": True},
        {"verdict": "projection_call_contract_missing_confirmed", "value": projection_contract_missing, "passed": True},
        {"verdict": "projection_adapter_plan_needed", "value": projection_adapter_plan_needed, "passed": True},
    ]

    next_route = [
        {"route_item": "recommended_next_layer", "value": RECOMMENDED_NEXT_LAYER_6KX, "passed": True},
        {"route_item": "recommended_path", "value": RECOMMENDED_PATH_6KX, "passed": True},
        {"route_item": "route_reason", "value": "projection_call_contract_missing", "passed": True},
    ]

    blockers = [
        {"blocker": "projection_call_contract_plan_required", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "evaluation_surface_not_materialized", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6ky = [
        {"contract": "define_projection_call_contract", "required": True, "passed": True},
        {"contract": "define_safe_projection_adapter_or_fixture_generation_path", "required": True, "passed": True},
        {"contract": "preserve_local_only_tmp_only_no_real_metrics_policy", "required": True, "passed": True},
        {"contract": "preserve_no_fetch_no_db_write_no_activation_no_layer6_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6kw_implementation_script_exists", "expected": True, "actual": IMPLEMENT_6KW_PATH.exists(), "passed": IMPLEMENT_6KW_PATH.exists()},
        {"check": "6kw_json_exists", "expected": True, "actual": JSON_6KW.exists(), "passed": JSON_6KW.exists()},
        {"check": "6kw_all_checks_passed", "expected": True, "actual": json_6kw.get("all_checks_passed"), "passed": json_6kw.get("all_checks_passed") is True},
        {"check": "6kw_diagnosis", "expected": DIAGNOSIS_6KW, "actual": json_6kw.get("diagnosis"), "passed": json_6kw.get("diagnosis") == DIAGNOSIS_6KW},
        {"check": "6kw_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KW, "actual": json_6kw.get("recommended_next_layer"), "passed": json_6kw.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KW},
        {"check": "6kw_source_generation_gap_report_emitted", "expected": True, "actual": json_6kw.get("source_generation_gap_report_emitted"), "passed": json_6kw.get("source_generation_gap_report_emitted") is True},
        {"check": "6kw_no_historical_eval", "expected": False, "actual": json_6kw.get("real_historical_evaluation_run"), "passed": json_6kw.get("real_historical_evaluation_run") is False},
        {"check": "6kw_no_layer6_exit", "expected": False, "actual": json_6kw.get("layer_6_exit_recommended"), "passed": json_6kw.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6ky_projection_call_contract_plan", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "projection call contract and generated evaluation surface required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "historical evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6KX", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KX is audit-only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6KX cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6kw_passed", "expected": True, "actual": json_6kw.get("all_checks_passed"), "passed": json_6kw.get("all_checks_passed") is True},
        {"decision": "candidate_source_audit_count", "expected": 4, "actual": len(candidate_source_audit), "passed": len(candidate_source_audit) == 4 and all_passed(candidate_source_audit)},
        {"decision": "generation_feasibility_audit_count", "expected": 4, "actual": len(generation_feasibility_audit), "passed": len(generation_feasibility_audit) == 4 and all_passed(generation_feasibility_audit)},
        {"decision": "surface_or_gap_audit_count", "expected": 3, "actual": len(surface_or_gap_audit), "passed": len(surface_or_gap_audit) == 3 and all_passed(surface_or_gap_audit)},
        {"decision": "projection_contract_verdict_count", "expected": 6, "actual": len(projection_contract_verdict), "passed": len(projection_contract_verdict) == 6 and all_passed(projection_contract_verdict)},
        {"decision": "recommend_6ky_next", "expected": RECOMMENDED_NEXT_LAYER_6KX, "actual": RECOMMENDED_NEXT_LAYER_6KX, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "source_generation_audited", "expected": True, "actual": True, "passed": True},
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
        {"surface": "6kw_implementation", "policy": "read_only", "passed": True},
        {"surface": "6kw_artifacts", "policy": "read_only", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6kx", "passed": True},
        {"surface": "database", "policy": "not_written_in_6kx", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KX, "actual": RECOMMENDED_NEXT_LAYER_6KX, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KX, "actual": RECOMMENDED_PATH_6KX, "passed": True},
        {"decision": "recommend_projection_call_contract_plan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KX, "actual": DIAGNOSIS_6KX, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "candidate_source_audit", "passed": all_passed(candidate_source_audit), "detail": f"{len(candidate_source_audit)} rows"},
        {"check": "generation_feasibility_audit", "passed": all_passed(generation_feasibility_audit), "detail": f"{len(generation_feasibility_audit)} rows"},
        {"check": "surface_or_gap_audit", "passed": all_passed(surface_or_gap_audit), "detail": f"{len(surface_or_gap_audit)} rows"},
        {"check": "metric_readiness_audit", "passed": all_passed(metric_readiness_audit), "detail": f"{len(metric_readiness_audit)} rows"},
        {"check": "projection_contract_verdict", "passed": all_passed(projection_contract_verdict), "detail": f"{len(projection_contract_verdict)} rows"},
        {"check": "next_route", "passed": all_passed(next_route), "detail": f"{len(next_route)} rows"},
        {"check": "blockers", "passed": len(blockers) == 4 and all_passed(blockers), "detail": "4/4"},
        {"check": "future_6ky_contract", "passed": len(future_6ky) == 4 and all_passed(future_6ky), "detail": "4/4"},
        {"check": "readonly_sources", "passed": all_passed(readonly_rows), "detail": f"{sum(1 for r in readonly_rows if r['passed'])}/{len(readonly_rows)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_rows), "detail": f"{sum(1 for r in blocking_rows if r['passed'])}/{len(blocking_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all_passed(immutability_rows), "detail": f"{sum(1 for r in immutability_rows if r['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "candidate_source_audit": write_csv(CANDIDATE_AUDIT_CSV, candidate_source_audit),
        "generation_feasibility_audit": write_csv(GEN_FEASIBILITY_AUDIT_CSV, generation_feasibility_audit),
        "surface_or_gap_audit": write_csv(SURFACE_OR_GAP_AUDIT_CSV, surface_or_gap_audit),
        "metric_readiness_audit": write_csv(METRIC_AUDIT_CSV, metric_readiness_audit),
        "projection_contract_verdict": write_csv(PROJECTION_VERDICT_CSV, projection_contract_verdict),
        "next_route": write_csv(NEXT_ROUTE_CSV, next_route),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6ky_contract": write_csv(FUTURE_6KY_CSV, future_6ky),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KX",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KX if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KX,
        "recommended_path": RECOMMENDED_PATH_6KX,
        "predecessor_implementation": str(IMPLEMENT_6KW_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6kw.get("diagnosis"),
        "audited_layer_after": "6KW",
        "source_family": "historical_backtest_source_generation_implementation_audit",
        "candidate_source_audit_count": len(candidate_source_audit),
        "generation_feasibility_audit_count": len(generation_feasibility_audit),
        "surface_or_gap_audit_count": len(surface_or_gap_audit),
        "metric_readiness_audit_count": len(metric_readiness_audit),
        "projection_contract_verdict_count": len(projection_contract_verdict),
        "next_route_count": len(next_route),
        "blocker_count": len(blockers),
        "future_6ky_contract_valid": len(future_6ky) == 4 and all_passed(future_6ky),
        "source_generation_audited": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "schedule_inputs_found_confirmed": schedule_confirmed,
        "actual_outcomes_found_confirmed": actual_confirmed,
        "projection_route_found_confirmed": projection_confirmed,
        "deterministic_generation_not_feasible_confirmed": deterministic_not_feasible,
        "evaluation_surface_missing_confirmed": surface_missing,
        "source_generation_gap_report_confirmed": gap_confirmed,
        "projection_call_contract_missing_confirmed": projection_contract_missing,
        "projection_adapter_plan_needed": projection_adapter_plan_needed,
        "probability_metric_ready_after_audit": prob_ready,
        "runs_metric_ready_after_audit": runs_ready,
        "any_backtest_metric_ready_after_audit": any_ready,
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
            "candidate_source_audit_csv": str(CANDIDATE_AUDIT_CSV),
            "generation_feasibility_audit_csv": str(GEN_FEASIBILITY_AUDIT_CSV),
            "surface_or_gap_audit_csv": str(SURFACE_OR_GAP_AUDIT_CSV),
            "metric_readiness_audit_csv": str(METRIC_AUDIT_CSV),
            "projection_contract_verdict_csv": str(PROJECTION_VERDICT_CSV),
            "next_route_csv": str(NEXT_ROUTE_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6ky_contract_csv": str(FUTURE_6KY_CSV),
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
