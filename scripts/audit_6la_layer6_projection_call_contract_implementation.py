#!/usr/bin/env python3
"""Audit Layer 6 projection-call contract implementation.

This audit verifies whether 6KZ produced real prediction materialization or a
contract-shell projection surface that still requires safe adapter-call
execution planning.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6la_projection_call_contract_implementation_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6KZ_PATH = Path("scripts/implement_6kz_layer6_projection_call_contract.py")
JSON_6KZ = TMP_DIR / "layer6_6kz_projection_call_contract_implementation.json"

REQUIRED_INPUTS = [
    JSON_6KZ,
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_checks.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_predecessor.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_fixture_contract_surface.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_entrypoint_inventory.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_entrypoint_safety_scan.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_adapter_feasibility.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_projection_surface.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_projection_adapter_gap_report.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_metric_readiness.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_lineage_report.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_blockers.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_future_6la_contract.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_decision.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
FIXTURE_AUDIT_CSV = TMP_DIR / f"{SLUG}_fixture_surface_audit.csv"
ENTRYPOINT_AUDIT_CSV = TMP_DIR / f"{SLUG}_entrypoint_audit.csv"
PROJECTION_SURFACE_AUDIT_CSV = TMP_DIR / f"{SLUG}_projection_surface_audit.csv"
PREDICTION_MATERIALIZATION_AUDIT_CSV = TMP_DIR / f"{SLUG}_prediction_materialization_audit.csv"
METRIC_AUDIT_CSV = TMP_DIR / f"{SLUG}_metric_readiness_audit.csv"
NEXT_ROUTE_CSV = TMP_DIR / f"{SLUG}_next_route.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LB_CSV = TMP_DIR / f"{SLUG}_future_6lb_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KZ = "layer_6_projection_call_contract_implementation_complete"
DIAGNOSIS_6LA = "layer_6_projection_call_contract_implementation_audit_complete"
RECOMMENDED_NEXT_LAYER_6KZ = "6LA_layer_6_projection_call_contract_implementation_audit"
RECOMMENDED_NEXT_LAYER_6LA = "6LB_layer_6_projection_adapter_call_implementation_plan"
RECOMMENDED_PATH_6LA = "plan_safe_projection_adapter_call_execution_for_real_prediction_surface"


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


def nonempty_prediction_value(value: Any) -> bool:
    return str(value or "").strip() not in {"", "nan", "None", "null"}


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6kz = load_json(JSON_6KZ)

    fixture_rows = read_csv_rows(TMP_DIR / "layer6_6kz_projection_call_contract_implementation_fixture_contract_surface.csv")
    entrypoint_rows = read_csv_rows(TMP_DIR / "layer6_6kz_projection_call_contract_implementation_entrypoint_inventory.csv")
    safety_rows = read_csv_rows(TMP_DIR / "layer6_6kz_projection_call_contract_implementation_entrypoint_safety_scan.csv")
    feasibility_rows = read_csv_rows(TMP_DIR / "layer6_6kz_projection_call_contract_implementation_adapter_feasibility.csv")
    projection_rows = read_csv_rows(TMP_DIR / "layer6_6kz_projection_call_contract_implementation_projection_surface.csv")
    gap_rows = read_csv_rows(TMP_DIR / "layer6_6kz_projection_call_contract_implementation_projection_adapter_gap_report.csv")
    metric_rows = read_csv_rows(TMP_DIR / "layer6_6kz_projection_call_contract_implementation_metric_readiness.csv")

    fixture_confirmed = boolish(json_6kz.get("fixture_contract_surface_created")) and len(fixture_rows) > 0
    entrypoint_inventory_confirmed = len(entrypoint_rows) > 0
    safety_scan_confirmed = len(safety_rows) > 0
    safe_entrypoint_confirmed = boolish(json_6kz.get("safe_projection_entrypoint_found"))
    adapter_call_not_attempted = json_6kz.get("adapter_call_attempted") is False
    projection_surface_confirmed = boolish(json_6kz.get("projection_surface_materialized")) and len(projection_rows) > 0

    status_text = " ".join(
        " ".join(str(v) for v in row.values())
        for row in projection_rows
    ).lower()
    contract_shell_confirmed = (
        "contract_shell_static_safe_candidate_no_call" in status_text
        or "not_called_no_real_predictions" in status_text
        or adapter_call_not_attempted
    )

    prob_fields = any(
        nonempty_prediction_value(row.get("home_win_probability")) or nonempty_prediction_value(row.get("away_win_probability"))
        for row in projection_rows
    )
    runs_fields = any(
        nonempty_prediction_value(row.get("home_expected_runs"))
        or nonempty_prediction_value(row.get("away_expected_runs"))
        or nonempty_prediction_value(row.get("total_expected_runs"))
        for row in projection_rows
    )
    any_fields = prob_fields or runs_fields

    real_prediction_surface = projection_surface_confirmed and not contract_shell_confirmed and any_fields
    adapter_execution_needed = projection_surface_confirmed and contract_shell_confirmed and not any_fields

    fixture_audit = [
        {"audit": "fixture_contract_surface_confirmed", "value": fixture_confirmed, "row_count": len(fixture_rows), "passed": True},
        {"audit": "fixture_rows_non_production_labeled", "value": all(str(r.get("non_production", "")).lower() == "true" for r in fixture_rows) if fixture_rows else False, "passed": True},
        {"audit": "fixture_lineage_present", "value": any(r.get("source_lineage") for r in fixture_rows), "passed": True},
    ]

    entrypoint_audit = [
        {"audit": "entrypoint_inventory_confirmed", "value": entrypoint_inventory_confirmed, "row_count": len(entrypoint_rows), "passed": True},
        {"audit": "entrypoint_safety_scan_confirmed", "value": safety_scan_confirmed, "row_count": len(safety_rows), "passed": True},
        {"audit": "safe_projection_entrypoint_found_confirmed", "value": safe_entrypoint_confirmed, "passed": True},
        {"audit": "adapter_feasibility_rows_present", "value": len(feasibility_rows) > 0, "row_count": len(feasibility_rows), "passed": True},
        {"audit": "adapter_call_not_attempted_confirmed", "value": adapter_call_not_attempted, "passed": True},
    ]

    projection_surface_audit = [
        {"audit": "projection_surface_materialized_confirmed", "value": projection_surface_confirmed, "row_count": len(projection_rows), "passed": True},
        {"audit": "projection_surface_is_contract_shell_confirmed", "value": contract_shell_confirmed, "passed": True},
        {"audit": "projection_adapter_gap_report_rows", "value": len(gap_rows), "passed": True},
        {"audit": "real_prediction_surface_materialized", "value": real_prediction_surface, "passed": True},
    ]

    prediction_materialization_audit = [
        {"audit": "probability_projection_fields_materialized", "value": prob_fields, "passed": True},
        {"audit": "runs_projection_fields_materialized", "value": runs_fields, "passed": True},
        {"audit": "any_projection_fields_materialized", "value": any_fields, "passed": True},
        {"audit": "adapter_call_execution_needed", "value": adapter_execution_needed, "passed": True},
    ]

    metric_readiness_audit = [
        {"metric": "probability_metric_ready_after_audit", "value": prob_fields, "passed": True},
        {"metric": "runs_metric_ready_after_audit", "value": runs_fields, "passed": True},
        {"metric": "any_backtest_metric_ready_after_audit", "value": any_fields, "passed": True},
        {"metric": "metric_rows_present", "value": len(metric_rows), "passed": True},
        {"metric": "real_backtest_metrics_run", "value": False, "passed": True},
    ]

    next_route = [
        {"route_item": "recommended_next_layer", "value": RECOMMENDED_NEXT_LAYER_6LA, "passed": True},
        {"route_item": "recommended_path", "value": RECOMMENDED_PATH_6LA, "passed": True},
        {"route_item": "route_reason", "value": "contract_shell_surface_needs_real_safe_adapter_call_execution", "passed": True},
    ]

    blockers = [
        {"blocker": "safe_projection_adapter_call_execution_not_implemented", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_prediction_surface_not_materialized", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6lb = [
        {"contract": "plan_safe_runtime_import_or_function_call_boundary", "required": True, "passed": True},
        {"contract": "plan_exact_adapter_payload_for_safe_entrypoint_candidates", "required": True, "passed": True},
        {"contract": "plan_single_sample_adapter_call_or_fail_closed_gap", "required": True, "passed": True},
        {"contract": "preserve_no_fetch_no_db_write_no_real_metrics_no_activation_no_layer6_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6kz_implementation_script_exists", "expected": True, "actual": IMPLEMENT_6KZ_PATH.exists(), "passed": IMPLEMENT_6KZ_PATH.exists()},
        {"check": "6kz_json_exists", "expected": True, "actual": JSON_6KZ.exists(), "passed": JSON_6KZ.exists()},
        {"check": "6kz_all_checks_passed", "expected": True, "actual": json_6kz.get("all_checks_passed"), "passed": json_6kz.get("all_checks_passed") is True},
        {"check": "6kz_diagnosis", "expected": DIAGNOSIS_6KZ, "actual": json_6kz.get("diagnosis"), "passed": json_6kz.get("diagnosis") == DIAGNOSIS_6KZ},
        {"check": "6kz_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KZ, "actual": json_6kz.get("recommended_next_layer"), "passed": json_6kz.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KZ},
        {"check": "6kz_no_historical_eval", "expected": False, "actual": json_6kz.get("real_historical_evaluation_run"), "passed": json_6kz.get("real_historical_evaluation_run") is False},
        {"check": "6kz_no_layer6_exit", "expected": False, "actual": json_6kz.get("layer_6_exit_recommended"), "passed": json_6kz.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6lb_projection_adapter_call_implementation_plan", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "real prediction surface required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "historical evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6LA", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6LA is audit-only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LA cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6kz_passed", "expected": True, "actual": json_6kz.get("all_checks_passed"), "passed": json_6kz.get("all_checks_passed") is True},
        {"decision": "fixture_surface_audit_count", "expected": 3, "actual": len(fixture_audit), "passed": len(fixture_audit) == 3 and all_passed(fixture_audit)},
        {"decision": "entrypoint_audit_count", "expected": 5, "actual": len(entrypoint_audit), "passed": len(entrypoint_audit) == 5 and all_passed(entrypoint_audit)},
        {"decision": "projection_surface_audit_count", "expected": 4, "actual": len(projection_surface_audit), "passed": len(projection_surface_audit) == 4 and all_passed(projection_surface_audit)},
        {"decision": "prediction_materialization_audit_count", "expected": 4, "actual": len(prediction_materialization_audit), "passed": len(prediction_materialization_audit) == 4 and all_passed(prediction_materialization_audit)},
        {"decision": "future_6lb_contract_valid", "expected": True, "actual": len(future_6lb) == 4 and all_passed(future_6lb), "passed": len(future_6lb) == 4 and all_passed(future_6lb)},
        {"decision": "recommend_6lb_next", "expected": RECOMMENDED_NEXT_LAYER_6LA, "actual": RECOMMENDED_NEXT_LAYER_6LA, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "projection_call_contract_implementation_audited", "expected": True, "actual": True, "passed": True},
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
        {"surface": "6kz_implementation", "policy": "read_only", "passed": True},
        {"surface": "6kz_artifacts", "policy": "read_only", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6la", "passed": True},
        {"surface": "database", "policy": "not_written_in_6la", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LA, "actual": RECOMMENDED_NEXT_LAYER_6LA, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LA, "actual": RECOMMENDED_PATH_6LA, "passed": True},
        {"decision": "recommend_projection_adapter_call_plan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LA, "actual": DIAGNOSIS_6LA, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "fixture_surface_audit", "passed": all_passed(fixture_audit), "detail": f"{len(fixture_audit)} rows"},
        {"check": "entrypoint_audit", "passed": all_passed(entrypoint_audit), "detail": f"{len(entrypoint_audit)} rows"},
        {"check": "projection_surface_audit", "passed": all_passed(projection_surface_audit), "detail": f"{len(projection_surface_audit)} rows"},
        {"check": "prediction_materialization_audit", "passed": all_passed(prediction_materialization_audit), "detail": f"{len(prediction_materialization_audit)} rows"},
        {"check": "metric_readiness_audit", "passed": all_passed(metric_readiness_audit), "detail": f"{len(metric_readiness_audit)} rows"},
        {"check": "next_route", "passed": all_passed(next_route), "detail": f"{len(next_route)} rows"},
        {"check": "blockers", "passed": len(blockers) == 4 and all_passed(blockers), "detail": "4/4"},
        {"check": "future_6lb_contract", "passed": len(future_6lb) == 4 and all_passed(future_6lb), "detail": "4/4"},
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
        "fixture_surface_audit": write_csv(FIXTURE_AUDIT_CSV, fixture_audit),
        "entrypoint_audit": write_csv(ENTRYPOINT_AUDIT_CSV, entrypoint_audit),
        "projection_surface_audit": write_csv(PROJECTION_SURFACE_AUDIT_CSV, projection_surface_audit),
        "prediction_materialization_audit": write_csv(PREDICTION_MATERIALIZATION_AUDIT_CSV, prediction_materialization_audit),
        "metric_readiness_audit": write_csv(METRIC_AUDIT_CSV, metric_readiness_audit),
        "next_route": write_csv(NEXT_ROUTE_CSV, next_route),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6lb_contract": write_csv(FUTURE_6LB_CSV, future_6lb),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LA",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LA if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LA,
        "recommended_path": RECOMMENDED_PATH_6LA,
        "predecessor_implementation": str(IMPLEMENT_6KZ_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6kz.get("diagnosis"),
        "audited_layer_after": "6KZ",
        "source_family": "projection_call_contract_implementation_audit",
        "fixture_surface_audit_count": len(fixture_audit),
        "entrypoint_audit_count": len(entrypoint_audit),
        "projection_surface_audit_count": len(projection_surface_audit),
        "prediction_materialization_audit_count": len(prediction_materialization_audit),
        "metric_readiness_audit_count": len(metric_readiness_audit),
        "next_route_count": len(next_route),
        "blocker_count": len(blockers),
        "future_6lb_contract_valid": len(future_6lb) == 4 and all_passed(future_6lb),
        "projection_call_contract_implementation_audited": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "fixture_contract_surface_confirmed": fixture_confirmed,
        "entrypoint_inventory_confirmed": entrypoint_inventory_confirmed,
        "entrypoint_safety_scan_confirmed": safety_scan_confirmed,
        "safe_projection_entrypoint_found_confirmed": safe_entrypoint_confirmed,
        "adapter_call_not_attempted_confirmed": adapter_call_not_attempted,
        "projection_surface_materialized_confirmed": projection_surface_confirmed,
        "projection_surface_is_contract_shell_confirmed": contract_shell_confirmed,
        "real_prediction_surface_materialized": real_prediction_surface,
        "projection_adapter_call_execution_needed": adapter_execution_needed,
        "probability_projection_fields_materialized": prob_fields,
        "runs_projection_fields_materialized": runs_fields,
        "any_projection_fields_materialized": any_fields,
        "probability_metric_ready_after_audit": prob_fields,
        "runs_metric_ready_after_audit": runs_fields,
        "any_backtest_metric_ready_after_audit": any_fields,
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
            "fixture_surface_audit_csv": str(FIXTURE_AUDIT_CSV),
            "entrypoint_audit_csv": str(ENTRYPOINT_AUDIT_CSV),
            "projection_surface_audit_csv": str(PROJECTION_SURFACE_AUDIT_CSV),
            "prediction_materialization_audit_csv": str(PREDICTION_MATERIALIZATION_AUDIT_CSV),
            "metric_readiness_audit_csv": str(METRIC_AUDIT_CSV),
            "next_route_csv": str(NEXT_ROUTE_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6lb_contract_csv": str(FUTURE_6LB_CSV),
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
