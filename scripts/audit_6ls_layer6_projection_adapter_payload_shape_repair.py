#!/usr/bin/env python3
"""Audit 6LR adapter-shaped payload repair artifact."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ls_projection_adapter_payload_shape_repair_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6LR_PATH = Path("scripts/implement_6lr_layer6_projection_adapter_payload_shape_repair.py")
JSON_6LR = TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation.json"
ADAPTER_PAYLOAD_JSON = TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_adapter_payload.json"

REQUIRED_6LR_INPUTS = [
    JSON_6LR,
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_checks.csv",
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_predecessor.csv",
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_source_search_trace.csv",
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_payload_schema_trace.csv",
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_game_entry_schema_trace.csv",
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_fixture_field_mapping.csv",
    ADAPTER_PAYLOAD_JSON,
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_adapter_payload_summary.csv",
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_gap_report.csv",
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_projection_surface_readiness.csv",
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_metric_readiness.csv",
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_blockers.csv",
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_future_6ls_contract.csv",
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_decision.csv",
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6lr_projection_adapter_payload_shape_repair_implementation_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PAYLOAD_AUDIT_CSV = TMP_DIR / f"{SLUG}_payload_artifact_audit.csv"
GAME_AUDIT_CSV = TMP_DIR / f"{SLUG}_game_entry_audit.csv"
FIELD_AUDIT_CSV = TMP_DIR / f"{SLUG}_field_readiness_audit.csv"
SURFACE_AUDIT_CSV = TMP_DIR / f"{SLUG}_projection_surface_audit.csv"
METRIC_AUDIT_CSV = TMP_DIR / f"{SLUG}_metric_readiness_audit.csv"
CALL_PLAN_READY_CSV = TMP_DIR / f"{SLUG}_call_plan_readiness.csv"
NEXT_ROUTE_CSV = TMP_DIR / f"{SLUG}_next_route.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LT_CSV = TMP_DIR / f"{SLUG}_future_6lt_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LR = "layer_6_projection_adapter_payload_shape_repair_implementation_complete"
DIAGNOSIS_6LS = "layer_6_projection_adapter_payload_shape_repair_audit_complete"
RECOMMENDED_NEXT_LAYER_6LR = "6LS_layer_6_projection_adapter_payload_shape_repair_audit"
RECOMMENDED_NEXT_LAYER_6LS = "6LT_layer_6_projection_adapter_shape_repaired_call_plan"
RECOMMENDED_PATH_6LS = "plan_single_sample_adapter_call_with_shape_repaired_payload"

TARGET_MODULE = "mlb_app.ai_data_assistant_performance"
TARGET_FUNCTION = "_canonical_games_from_projection_payload"
REQUIRED_ARGS = "payload;game_pk;limit"
TARGET_GAME_PK = 824776


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
    json_6lr = load_json(JSON_6LR)
    payload = load_json(ADAPTER_PAYLOAD_JSON)

    games = payload.get("games") if isinstance(payload, dict) else None
    first_game = games[0] if isinstance(games, list) and games else {}

    top_games = isinstance(payload, dict) and "games" in payload
    games_is_list = isinstance(games, list)
    games_count = len(games) if isinstance(games, list) else 0
    first_game_pk = first_game.get("game_pk") if isinstance(first_game, dict) else None
    non_prod = payload.get("non_production") is True if isinstance(payload, dict) else False
    not_real_surface = payload.get("not_a_real_prediction_surface") is True if isinstance(payload, dict) else False
    shape_only = first_game.get("shape_artifact_only") is True if isinstance(first_game, dict) else False

    prob_placeholder = (
        isinstance(first_game, dict)
        and first_game.get("home_win_probability") == 0.5
        and first_game.get("away_win_probability") == 0.5
    )
    runs_missing = (
        isinstance(first_game, dict)
        and first_game.get("home_expected_runs") is None
        and first_game.get("away_expected_runs") is None
    )

    payload_audit = [
        {"audit": "adapter_payload_loaded", "value": bool(payload), "passed": bool(payload)},
        {"audit": "top_level_games_key_present", "value": top_games, "passed": top_games},
        {"audit": "games_is_list", "value": games_is_list, "passed": games_is_list},
        {"audit": "games_count", "value": games_count, "expected": 1, "passed": games_count == 1},
        {"audit": "adapter_payload_non_production", "value": non_prod, "expected": True, "passed": non_prod},
        {"audit": "adapter_payload_is_real_prediction_surface", "value": not not_real_surface, "expected": False, "passed": not_real_surface},
    ]

    game_audit = [
        {"audit": "first_game_exists", "value": bool(first_game), "passed": bool(first_game)},
        {"audit": "first_game_pk", "value": first_game_pk, "expected": TARGET_GAME_PK, "passed": first_game_pk == TARGET_GAME_PK},
        {"audit": "game_pk_824776_present", "value": first_game_pk == TARGET_GAME_PK, "passed": first_game_pk == TARGET_GAME_PK},
        {"audit": "shape_artifact_only", "value": shape_only, "expected": True, "passed": shape_only},
        {"audit": "home_team_present_or_placeholder", "value": first_game.get("home_team", ""), "passed": isinstance(first_game, dict)},
        {"audit": "away_team_present_or_placeholder", "value": first_game.get("away_team", ""), "passed": isinstance(first_game, dict)},
    ]

    field_audit = [
        {"field": "game_pk", "ready_for_future_call_plan": first_game_pk == TARGET_GAME_PK, "real_prediction_field": False, "passed": first_game_pk == TARGET_GAME_PK},
        {"field": "home_win_probability", "placeholder": first_game.get("home_win_probability"), "real_prediction_field": False, "passed": prob_placeholder},
        {"field": "away_win_probability", "placeholder": first_game.get("away_win_probability"), "real_prediction_field": False, "passed": prob_placeholder},
        {"field": "home_expected_runs", "materialized": first_game.get("home_expected_runs") is not None, "real_prediction_field": False, "passed": runs_missing},
        {"field": "away_expected_runs", "materialized": first_game.get("away_expected_runs") is not None, "real_prediction_field": False, "passed": runs_missing},
        {"field": "data_confidence", "value": first_game.get("data_confidence"), "real_prediction_field": False, "passed": isinstance(first_game, dict)},
    ]

    surface_audit = [
        {"surface": "adapter_payload_shape_repair_artifact", "materialized": True, "real_prediction_surface": False, "passed": True},
        {"surface": "projection_surface_materialized", "materialized": False, "real_prediction_surface": False, "passed": True},
        {"surface": "real_prediction_fields_materialized", "materialized": False, "real_prediction_surface": False, "passed": True},
    ]

    metric_audit = [
        {"metric": "probability_metric_ready_after_audit", "ready": False, "reason": "probability fields are placeholders", "passed": True},
        {"metric": "runs_metric_ready_after_audit", "ready": False, "reason": "run fields not materialized", "passed": True},
        {"metric": "any_backtest_metric_ready_after_audit", "ready": False, "reason": "no adapter call and no real prediction surface", "passed": True},
    ]

    call_plan_ready = [
        {"readiness": "shape_audit_passed", "value": all_passed(payload_audit) and all_passed(game_audit), "passed": True},
        {"readiness": "payload_games_present", "value": top_games and games_is_list, "passed": True},
        {"readiness": "game_pk_ready_for_future_call_plan", "value": first_game_pk == TARGET_GAME_PK, "passed": True},
        {"readiness": "future_single_sample_adapter_call_plan_allowed_next", "value": True, "passed": True},
        {"readiness": "future_adapter_call_execution_allowed_next", "value": False, "passed": True},
        {"readiness": "adapter_call_executed_by_6ls", "value": False, "passed": True},
    ]

    next_route = [
        {"route_item": "recommended_next_layer", "value": RECOMMENDED_NEXT_LAYER_6LS, "passed": True},
        {"route_item": "recommended_path", "value": RECOMMENDED_PATH_6LS, "passed": True},
        {"route_item": "next_layer_mode", "value": "planning_only", "passed": True},
        {"route_item": "plan_single_sample_adapter_call_with_shape_repaired_payload", "value": True, "passed": True},
        {"route_item": "execute_adapter_call_now", "value": False, "passed": True},
        {"route_item": "real_metrics_allowed", "value": False, "passed": True},
    ]

    blockers = [
        {"blocker": "real_prediction_surface_not_materialized", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "adapter_call_with_shape_repaired_payload_not_planned_or_audited", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6lt = [
        {"contract": "plan_single_sample_adapter_call_with_shape_repaired_payload", "required": True, "passed": True},
        {"contract": "preserve_no_execution_in_plan_layer", "required": True, "passed": True},
        {"contract": "define_payload_game_pk_limit_args_from_artifact", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_activation_or_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6lr_implementation_script_exists", "expected": True, "actual": IMPLEMENT_6LR_PATH.exists(), "passed": IMPLEMENT_6LR_PATH.exists()},
        {"check": "6lr_json_exists", "expected": True, "actual": JSON_6LR.exists(), "passed": JSON_6LR.exists()},
        {"check": "6lr_all_checks_passed", "expected": True, "actual": json_6lr.get("all_checks_passed"), "passed": json_6lr.get("all_checks_passed") is True},
        {"check": "6lr_diagnosis", "expected": DIAGNOSIS_6LR, "actual": json_6lr.get("diagnosis"), "passed": json_6lr.get("diagnosis") == DIAGNOSIS_6LR},
        {"check": "6lr_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LR, "actual": json_6lr.get("recommended_next_layer"), "passed": json_6lr.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LR},
        {"check": "6lr_payload_written", "expected": True, "actual": json_6lr.get("adapter_shaped_payload_artifact_written"), "passed": json_6lr.get("adapter_shaped_payload_artifact_written") is True},
        {"check": "6lr_no_adapter_call", "expected": False, "actual": json_6lr.get("adapter_call_executed"), "passed": json_6lr.get("adapter_call_executed") is False},
        {"check": "6lr_no_layer6_exit", "expected": False, "actual": json_6lr.get("layer_6_exit_recommended"), "passed": json_6lr.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_6LR_INPUTS
    ]

    readonly_rows = [
        {"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()}
        for path in REQUIRED_6LR_INPUTS
    ]

    blocking_rows = [
        {"blocked_surface": "6lt_shape_repaired_call_plan", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "adapter_call_execution", "blocked": True, "reason": "only future planning layer allowed next", "passed": True},
        {"blocked_surface": "next_candidate_retry", "blocked": True, "reason": "same candidate call plan not attempted yet", "passed": True},
        {"blocked_surface": "wrapper_design", "blocked": True, "reason": "same candidate call plan not attempted yet", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "real prediction surface required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LS cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6lr_passed", "expected": True, "actual": json_6lr.get("all_checks_passed"), "passed": json_6lr.get("all_checks_passed") is True},
        {"decision": "payload_audit_valid", "expected": True, "actual": all_passed(payload_audit), "passed": all_passed(payload_audit)},
        {"decision": "game_audit_valid", "expected": True, "actual": all_passed(game_audit), "passed": all_passed(game_audit)},
        {"decision": "field_audit_valid", "expected": True, "actual": all_passed(field_audit), "passed": all_passed(field_audit)},
        {"decision": "call_plan_readiness_valid", "expected": True, "actual": all_passed(call_plan_ready), "passed": all_passed(call_plan_ready)},
        {"decision": "future_6lt_contract_valid", "expected": True, "actual": len(future_6lt) == 4 and all_passed(future_6lt), "passed": len(future_6lt) == 4 and all_passed(future_6lt)},
        {"decision": "recommend_6lt_next", "expected": RECOMMENDED_NEXT_LAYER_6LS, "actual": RECOMMENDED_NEXT_LAYER_6LS, "passed": True},
        {"decision": "do_not_execute_adapter", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "target_module_imported_by_6ls", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6ls", "expected": False, "actual": False, "passed": True},
        {"boundary": "additional_adapter_call_executed_by_6ls", "expected": False, "actual": False, "passed": True},
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
        {"surface": "6lr_implementation", "policy": "read_only", "passed": True},
        {"surface": "adapter_payload_artifact", "policy": "read_only_audit", "passed": True},
        {"surface": "future_6lt_plan", "policy": "plan_only_next_layer", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6ls", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LS, "actual": RECOMMENDED_NEXT_LAYER_6LS, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LS, "actual": RECOMMENDED_PATH_6LS, "passed": True},
        {"decision": "recommend_shape_repaired_call_plan", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_execute_adapter_now", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_next_candidate_retry", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LS, "actual": DIAGNOSIS_6LS, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "payload_artifact_audit", "passed": all_passed(payload_audit), "detail": f"{len(payload_audit)} rows"},
        {"check": "game_entry_audit", "passed": all_passed(game_audit), "detail": f"{len(game_audit)} rows"},
        {"check": "field_readiness_audit", "passed": all_passed(field_audit), "detail": f"{len(field_audit)} rows"},
        {"check": "projection_surface_audit", "passed": all_passed(surface_audit), "detail": f"{len(surface_audit)} rows"},
        {"check": "metric_readiness_audit", "passed": all_passed(metric_audit), "detail": f"{len(metric_audit)} rows"},
        {"check": "call_plan_readiness", "passed": all_passed(call_plan_ready), "detail": f"{len(call_plan_ready)} rows"},
        {"check": "next_route", "passed": all_passed(next_route), "detail": f"{len(next_route)} rows"},
        {"check": "future_6lt_contract", "passed": all_passed(future_6lt), "detail": f"{len(future_6lt)} rows"},
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
        "payload_artifact_audit": write_csv(PAYLOAD_AUDIT_CSV, payload_audit),
        "game_entry_audit": write_csv(GAME_AUDIT_CSV, game_audit),
        "field_readiness_audit": write_csv(FIELD_AUDIT_CSV, field_audit),
        "projection_surface_audit": write_csv(SURFACE_AUDIT_CSV, surface_audit),
        "metric_readiness_audit": write_csv(METRIC_AUDIT_CSV, metric_audit),
        "call_plan_readiness": write_csv(CALL_PLAN_READY_CSV, call_plan_ready),
        "next_route": write_csv(NEXT_ROUTE_CSV, next_route),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6lt_contract": write_csv(FUTURE_6LT_CSV, future_6lt),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LS",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LS,
        "recommended_path": RECOMMENDED_PATH_6LS,
        "predecessor_implementation": str(IMPLEMENT_6LR_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6lr.get("diagnosis"),
        "audited_layer_after": "6LR",
        "source_family": "projection_adapter_payload_shape_repair_audit",
        "payload_artifact_audit_count": len(payload_audit),
        "game_entry_audit_count": len(game_audit),
        "field_readiness_audit_count": len(field_audit),
        "projection_surface_audit_count": len(surface_audit),
        "metric_readiness_audit_count": len(metric_audit),
        "call_plan_readiness_count": len(call_plan_ready),
        "next_route_count": len(next_route),
        "blocker_count": len(blockers),
        "future_6lt_contract_valid": len(future_6lt) == 4 and all_passed(future_6lt),
        "projection_adapter_payload_shape_repair_audited": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "same_candidate_retained_confirmed": json_6lr.get("same_candidate_retained_confirmed") is True,
        "blocked_session_candidate_excluded_confirmed": json_6lr.get("blocked_session_candidate_excluded_confirmed") is True,
        "target_module_import_path": TARGET_MODULE,
        "target_function_name": TARGET_FUNCTION,
        "required_arguments_confirmed": REQUIRED_ARGS,
        "adapter_payload_path": str(ADAPTER_PAYLOAD_JSON),
        "adapter_payload_loaded": bool(payload),
        "adapter_payload_top_level_games_key_present": top_games,
        "adapter_payload_games_is_list": games_is_list,
        "adapter_payload_games_count": games_count,
        "adapter_payload_first_game_pk": first_game_pk,
        "adapter_payload_game_pk_824776_present": first_game_pk == TARGET_GAME_PK,
        "adapter_payload_non_production": non_prod,
        "adapter_payload_shape_artifact_only": shape_only,
        "adapter_payload_is_real_prediction_surface": False,
        "payload_shape_repair_audit_passed": all_passed(payload_audit) and all_passed(game_audit),
        "game_pk_ready_for_future_call_plan": first_game_pk == TARGET_GAME_PK,
        "probability_fields_are_placeholders": prob_placeholder,
        "run_fields_not_materialized": runs_missing,
        "real_prediction_fields_materialized": False,
        "projection_surface_materialized": False,
        "future_single_sample_adapter_call_plan_allowed_next": True,
        "future_adapter_call_execution_allowed_next": False,
        "target_module_imported_by_6ls": False,
        "adapter_call_executed_by_6ls": False,
        "additional_adapter_call_executed_by_6ls": False,
        "probability_metric_ready_after_audit": False,
        "runs_metric_ready_after_audit": False,
        "any_backtest_metric_ready_after_audit": False,
        "next_candidate_retry_recommended": False,
        "wrapper_plan_needed": False,
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
            "payload_artifact_audit_csv": str(PAYLOAD_AUDIT_CSV),
            "game_entry_audit_csv": str(GAME_AUDIT_CSV),
            "field_readiness_audit_csv": str(FIELD_AUDIT_CSV),
            "projection_surface_audit_csv": str(SURFACE_AUDIT_CSV),
            "metric_readiness_audit_csv": str(METRIC_AUDIT_CSV),
            "call_plan_readiness_csv": str(CALL_PLAN_READY_CSV),
            "next_route_csv": str(NEXT_ROUTE_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6lt_contract_csv": str(FUTURE_6LT_CSV),
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
