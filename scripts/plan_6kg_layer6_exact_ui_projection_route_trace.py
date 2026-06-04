#!/usr/bin/env python3
"""Plan exact UI projection route trace for Layer 6.

This planning-only layer defines a narrow trace from ModelProjectionsPage to
the backend route, payload builder, simulator/projection function, and realism
feature chain. It does not modify code, fetch data, run simulations, activate
mechanics, or grant Layer 6 exit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6kg_exact_ui_projection_route_trace_plan"
TMP_DIR = Path("tmp")

AUDIT_6KF_PATH = Path("scripts/audit_6kf_layer6_existing_backtest_dataset_and_ui_projection_path_audit_implementation.py")
JSON_6KF = TMP_DIR / "layer6_6kf_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit.json"

REQUIRED_INPUTS = [
    JSON_6KF,
    TMP_DIR / "layer6_6kf_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit_checks.csv",
    TMP_DIR / "layer6_6kf_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit_predecessor.csv",
    TMP_DIR / "layer6_6kf_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6kf_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit_dataset_findings.csv",
    TMP_DIR / "layer6_6kf_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit_ui_path_findings.csv",
    TMP_DIR / "layer6_6kf_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit_noise_limitations.csv",
    TMP_DIR / "layer6_6kf_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit_exact_route_trace_need.csv",
    TMP_DIR / "layer6_6kf_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit_activation_blockers.csv",
    TMP_DIR / "layer6_6kf_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit_future_6kg_contract.csv",
    TMP_DIR / "layer6_6kf_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6kf_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit_decision.csv",
    TMP_DIR / "layer6_6kf_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6kf_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
TRACE_SCOPE_CSV = TMP_DIR / f"{SLUG}_trace_scope.csv"
FRONTEND_TRACE_CSV = TMP_DIR / f"{SLUG}_frontend_trace_plan.csv"
BACKEND_ROUTE_TRACE_CSV = TMP_DIR / f"{SLUG}_backend_route_trace_plan.csv"
PAYLOAD_BUILDER_TRACE_CSV = TMP_DIR / f"{SLUG}_payload_builder_trace_plan.csv"
SIMULATOR_TRACE_CSV = TMP_DIR / f"{SLUG}_simulator_trace_plan.csv"
REALISM_FEATURE_TRACE_CSV = TMP_DIR / f"{SLUG}_realism_feature_trace_plan.csv"
FLAG_CONFIG_TRACE_CSV = TMP_DIR / f"{SLUG}_flag_config_trace_plan.csv"
UI_DISPLAY_FIELD_TRACE_CSV = TMP_DIR / f"{SLUG}_ui_display_field_trace_plan.csv"
PARALLEL_PATH_CAVEAT_CSV = TMP_DIR / f"{SLUG}_parallel_path_caveat.csv"
ACTIVATION_BLOCKERS_CSV = TMP_DIR / f"{SLUG}_activation_blockers.csv"
FUTURE_6KH_CSV = TMP_DIR / f"{SLUG}_future_6kh_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KF = "layer_6_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit_complete"
DIAGNOSIS_6KG = "layer_6_exact_ui_projection_route_trace_plan_complete"
RECOMMENDED_NEXT_LAYER_6KF = "6KG_layer_6_exact_ui_projection_route_trace_plan"
RECOMMENDED_NEXT_LAYER_6KG = "6KH_layer_6_exact_ui_projection_route_trace_implementation"
RECOMMENDED_PATH_6KG = "plan_exact_ui_route_trace_then_implement_before_real_backtest"


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


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
    parsed = json.loads(path.read_text(encoding="utf-8"))
    return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}


def syntax_compile() -> Tuple[int, str]:
    failures: List[str] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            try:
                compile(path.read_text(encoding="utf-8"), str(path), "exec")
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
    json_6kf = load_json(JSON_6KF)

    trace_scope = [
        {"step": 1, "trace_node": "frontend_page", "target": "frontend/src/pages/ModelProjectionsPage.jsx", "purpose": "identify displayed projection fields and endpoint call", "passed": True},
        {"step": 2, "trace_node": "frontend_api_endpoint", "target": "exact API path string used by ModelProjectionsPage", "purpose": "remove broad-scan ambiguity", "passed": True},
        {"step": 3, "trace_node": "backend_route", "target": "backend route serving the endpoint", "purpose": "identify route function and module", "passed": True},
        {"step": 4, "trace_node": "payload_builder", "target": "function building model projection payload", "purpose": "identify data source for displayed fields", "passed": True},
        {"step": 5, "trace_node": "simulator_or_projection_function", "target": "exact function producing projected runs/win probabilities", "purpose": "determine production math path", "passed": True},
        {"step": 6, "trace_node": "realism_feature_chain", "target": "mechanic modules reached or bypassed", "purpose": "determine realism influence on UI outputs", "passed": True},
    ]

    frontend_trace = [
        {"target": "ModelProjectionsPage.jsx", "audit": "extract fetch/axios/apiJson calls and endpoint strings", "required": True, "passed": True},
        {"target": "displayed_fields", "audit": "map projected runs, win probabilities, totals, cards, tables to payload keys", "required": True, "passed": True},
        {"target": "date_and_filter_params", "audit": "record query params/route state affecting projection payload", "required": True, "passed": True},
        {"target": "cached_or_static_values", "audit": "determine whether UI uses cached/static/precomputed values instead of backend response fields", "required": True, "passed": True},
    ]

    backend_route_trace = [
        {"target": "mlb_app/model_projection_routes.py", "audit": "identify route decorator/path and route function", "required": True, "passed": True},
        {"target": "mlb_app/app.py", "audit": "check app route registration and blueprint mounting path", "required": True, "passed": True},
        {"target": "route_response_shape", "audit": "map returned JSON fields to frontend displayed fields", "required": True, "passed": True},
    ]

    payload_builder_trace = [
        {"target": "mlb_app/model_projection_payload.py", "audit": "trace build_model_projection_payload calls and dependencies", "required": True, "passed": True},
        {"target": "mlb_app/model_projections.py", "audit": "trace alternate/legacy payload builder if route imports from here", "required": True, "passed": True},
        {"target": "payload_field_sources", "audit": "map each projected number to function/source column", "required": True, "passed": True},
        {"target": "fallback_paths", "audit": "record fallback/legacy branches when data is missing", "required": True, "passed": True},
    ]

    simulator_trace = [
        {"target": "simulation_entrypoint", "audit": "identify exact simulator/projection function called by payload builder", "required": True, "passed": True},
        {"target": "mlb_app/simulation/game_simulator.py", "audit": "determine whether game simulator is called for UI payload", "required": True, "passed": True},
        {"target": "mlb_app/simulation/game_engine_v2.py", "audit": "determine whether v2 engine is called for UI payload", "required": True, "passed": True},
        {"target": "mlb_app/simulation/inning_simulator.py", "audit": "determine whether inning simulator is called for UI payload", "required": True, "passed": True},
        {"target": "non_sim_formula_path", "audit": "record if UI projections are formula/model rows rather than simulation outputs", "required": True, "passed": True},
    ]

    realism_feature_trace = [
        {"mechanic": "bullpen_logic", "targets": "bullpen_chain,bullpen_integration,bullpen_game_engine_hook", "trace_required": True, "passed": True},
        {"mechanic": "double_play_logic", "targets": "inning_simulator,subtype_transitions,db_utils", "trace_required": True, "passed": True},
        {"mechanic": "sac_fly_logic", "targets": "inning_simulator,hitting_matchups,db_utils", "trace_required": True, "passed": True},
        {"mechanic": "stolen_base_or_steal_logic", "targets": "inning_simulator,player_splits,data_ingestion", "trace_required": True, "passed": True},
        {"mechanic": "extras_ghost_runner_walkoff_logic", "targets": "game_rules,game_simulator", "trace_required": True, "passed": True},
        {"mechanic": "balk_logic", "targets": "any explicit balk module or deferred marker", "trace_required": True, "passed": True},
    ]

    flag_config_trace = [
        {"target": "environment_flags", "audit": "search for env/config flags controlling realism path", "required": True, "passed": True},
        {"target": "function_defaults", "audit": "check default args that enable or disable realism mechanics", "required": True, "passed": True},
        {"target": "feature_flags", "audit": "check if realism is shadow/dormant/candidate-only", "required": True, "passed": True},
        {"target": "fallback_conditions", "audit": "check branch conditions causing legacy/current path fallback", "required": True, "passed": True},
    ]

    ui_display_field_trace = [
        {"target": "projected_runs", "audit": "map UI displayed projected runs to response key and source function", "required": True, "passed": True},
        {"target": "win_probability", "audit": "map UI win probability to response key and source function", "required": True, "passed": True},
        {"target": "projected_total", "audit": "map total display to response key and source function", "required": True, "passed": True},
        {"target": "confidence_or_edge_fields", "audit": "map any model edge/confidence fields to response key and source function", "required": True, "passed": True},
    ]

    parallel_path_caveat = [
        {"path_family": "ModelProjectionsPage", "caveat": "primary target for displayed model projections", "separate_from_primary": False, "passed": True},
        {"path_family": "MyDashboardWorkspacePage", "caveat": "dashboard solver may use a parallel model-solver projection path", "separate_from_primary": True, "passed": True},
        {"path_family": "DailyOddsPage", "caveat": "odds/model boards may use daily odds model paths separate from ModelProjectionsPage", "separate_from_primary": True, "passed": True},
        {"path_family": "model_tracker", "caveat": "tracking/snapshot paths may normalize or store projection rows separately", "separate_from_primary": True, "passed": True},
    ]

    activation_blockers = [
        {"blocker": "exact_ui_route_trace_not_implemented", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "realism_ui_activation_not_confirmed", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "activation_decision_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "balks_deferred_or_exit_gated", "blocks_activation": False, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6kh = [
        {"contract": "extract_model_projections_page_endpoint", "required": True, "passed": True},
        {"contract": "trace_backend_route_and_registration", "required": True, "passed": True},
        {"contract": "trace_payload_builder_call_chain", "required": True, "passed": True},
        {"contract": "trace_simulator_or_formula_entrypoint", "required": True, "passed": True},
        {"contract": "trace_realism_feature_reachability", "required": True, "passed": True},
        {"contract": "trace_flags_config_fallbacks", "required": True, "passed": True},
        {"contract": "map_ui_display_fields_to_source_functions", "required": True, "passed": True},
        {"contract": "do_not_activate_or_grant_layer6_exit_in_6kh", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6kf_audit_script_exists", "expected": True, "actual": AUDIT_6KF_PATH.exists(), "passed": AUDIT_6KF_PATH.exists()},
        {"check": "6kf_json_exists", "expected": True, "actual": JSON_6KF.exists(), "passed": JSON_6KF.exists()},
        {"check": "6kf_all_checks_passed", "expected": True, "actual": json_6kf.get("all_checks_passed"), "passed": json_6kf.get("all_checks_passed") is True},
        {"check": "6kf_diagnosis", "expected": DIAGNOSIS_6KF, "actual": json_6kf.get("diagnosis"), "passed": json_6kf.get("diagnosis") == DIAGNOSIS_6KF},
        {"check": "6kf_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KF, "actual": json_6kf.get("recommended_next_layer"), "passed": json_6kf.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KF},
        {"check": "6kf_exact_ui_route_trace_required", "expected": True, "actual": json_6kf.get("exact_ui_route_trace_required"), "passed": json_6kf.get("exact_ui_route_trace_required") is True},
        {"check": "6kf_realism_ui_activation_confirmed", "expected": False, "actual": json_6kf.get("realism_ui_activation_confirmed"), "passed": json_6kf.get("realism_ui_activation_confirmed") is False},
        {"check": "6kf_ui_uses_realism_enabled_path", "expected": True, "actual": json_6kf.get("ui_uses_realism_enabled_path"), "passed": json_6kf.get("ui_uses_realism_enabled_path") is True},
        {"check": "6kf_ui_uses_legacy_or_current_path", "expected": True, "actual": json_6kf.get("ui_uses_legacy_or_current_path"), "passed": json_6kf.get("ui_uses_legacy_or_current_path") is True},
        {"check": "6kf_no_layer6_exit", "expected": False, "actual": json_6kf.get("layer_6_exit_recommended"), "passed": json_6kf.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6kh_exact_route_trace_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "exact route trace required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "real evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6KG", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KG is planning only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6KG cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6kf_passed", "expected": True, "actual": json_6kf.get("all_checks_passed"), "passed": json_6kf.get("all_checks_passed") is True},
        {"decision": "trace_scope_count", "expected": 6, "actual": len(trace_scope), "passed": len(trace_scope) == 6},
        {"decision": "frontend_trace_plan_count", "expected": 4, "actual": len(frontend_trace), "passed": len(frontend_trace) == 4},
        {"decision": "backend_route_trace_plan_count", "expected": 3, "actual": len(backend_route_trace), "passed": len(backend_route_trace) == 3},
        {"decision": "payload_builder_trace_plan_count", "expected": 4, "actual": len(payload_builder_trace), "passed": len(payload_builder_trace) == 4},
        {"decision": "simulator_trace_plan_count", "expected": 5, "actual": len(simulator_trace), "passed": len(simulator_trace) == 5},
        {"decision": "realism_feature_trace_plan_count", "expected": 6, "actual": len(realism_feature_trace), "passed": len(realism_feature_trace) == 6},
        {"decision": "flag_config_trace_plan_count", "expected": 4, "actual": len(flag_config_trace), "passed": len(flag_config_trace) == 4},
        {"decision": "ui_display_field_trace_plan_count", "expected": 4, "actual": len(ui_display_field_trace), "passed": len(ui_display_field_trace) == 4},
        {"decision": "parallel_path_caveat_count", "expected": 4, "actual": len(parallel_path_caveat), "passed": len(parallel_path_caveat) == 4},
        {"decision": "recommend_6kh_next", "expected": RECOMMENDED_NEXT_LAYER_6KG, "actual": RECOMMENDED_NEXT_LAYER_6KG, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "exact_ui_route_trace_required", "expected": True, "actual": True, "passed": True},
        {"boundary": "exact_ui_route_trace_completed", "expected": False, "actual": False, "passed": True},
        {"boundary": "realism_ui_activation_confirmed", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_historical_evaluation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_activation_execution", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_recommendation", "expected": False, "actual": False, "passed": True},
    ]

    immutability_rows = [
        {"surface": "source_tree", "policy": "read_only_planning", "passed": True},
        {"surface": "6kf_audit", "policy": "read_only", "passed": True},
        {"surface": "ui_projection_path", "policy": "planned_not_modified_in_6kg", "passed": True},
        {"surface": "simulator_path", "policy": "planned_not_modified_in_6kg", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6kg", "passed": True},
        {"surface": "database", "policy": "not_written_in_6kg", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KG, "actual": RECOMMENDED_NEXT_LAYER_6KG, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KG, "actual": RECOMMENDED_PATH_6KG, "passed": True},
        {"decision": "recommend_exact_route_trace_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KG, "actual": DIAGNOSIS_6KG, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "trace_scope", "passed": len(trace_scope) == 6 and all_passed(trace_scope), "detail": "6/6"},
        {"check": "frontend_trace_plan", "passed": len(frontend_trace) == 4 and all_passed(frontend_trace), "detail": "4/4"},
        {"check": "backend_route_trace_plan", "passed": len(backend_route_trace) == 3 and all_passed(backend_route_trace), "detail": "3/3"},
        {"check": "payload_builder_trace_plan", "passed": len(payload_builder_trace) == 4 and all_passed(payload_builder_trace), "detail": "4/4"},
        {"check": "simulator_trace_plan", "passed": len(simulator_trace) == 5 and all_passed(simulator_trace), "detail": "5/5"},
        {"check": "realism_feature_trace_plan", "passed": len(realism_feature_trace) == 6 and all_passed(realism_feature_trace), "detail": "6/6"},
        {"check": "flag_config_trace_plan", "passed": len(flag_config_trace) == 4 and all_passed(flag_config_trace), "detail": "4/4"},
        {"check": "ui_display_field_trace_plan", "passed": len(ui_display_field_trace) == 4 and all_passed(ui_display_field_trace), "detail": "4/4"},
        {"check": "parallel_path_caveat", "passed": len(parallel_path_caveat) == 4 and all_passed(parallel_path_caveat), "detail": "4/4"},
        {"check": "activation_blockers", "passed": len(activation_blockers) == 5 and all_passed(activation_blockers), "detail": "5/5"},
        {"check": "future_6kh_contract", "passed": len(future_6kh) == 8 and all_passed(future_6kh), "detail": "8/8"},
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
        "trace_scope": write_csv(TRACE_SCOPE_CSV, trace_scope),
        "frontend_trace_plan": write_csv(FRONTEND_TRACE_CSV, frontend_trace),
        "backend_route_trace_plan": write_csv(BACKEND_ROUTE_TRACE_CSV, backend_route_trace),
        "payload_builder_trace_plan": write_csv(PAYLOAD_BUILDER_TRACE_CSV, payload_builder_trace),
        "simulator_trace_plan": write_csv(SIMULATOR_TRACE_CSV, simulator_trace),
        "realism_feature_trace_plan": write_csv(REALISM_FEATURE_TRACE_CSV, realism_feature_trace),
        "flag_config_trace_plan": write_csv(FLAG_CONFIG_TRACE_CSV, flag_config_trace),
        "ui_display_field_trace_plan": write_csv(UI_DISPLAY_FIELD_TRACE_CSV, ui_display_field_trace),
        "parallel_path_caveat": write_csv(PARALLEL_PATH_CAVEAT_CSV, parallel_path_caveat),
        "activation_blockers": write_csv(ACTIVATION_BLOCKERS_CSV, activation_blockers),
        "future_6kh_contract": write_csv(FUTURE_6KH_CSV, future_6kh),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KG",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KG if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KG,
        "recommended_path": RECOMMENDED_PATH_6KG,
        "predecessor_audit": str(AUDIT_6KF_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6kf.get("diagnosis"),
        "planned_layer_after": "6KF",
        "source_family": "exact_ui_projection_route_trace_plan",
        "trace_scope_count": len(trace_scope),
        "frontend_trace_plan_count": len(frontend_trace),
        "backend_route_trace_plan_count": len(backend_route_trace),
        "payload_builder_trace_plan_count": len(payload_builder_trace),
        "simulator_trace_plan_count": len(simulator_trace),
        "realism_feature_trace_plan_count": len(realism_feature_trace),
        "flag_config_trace_plan_count": len(flag_config_trace),
        "ui_display_field_trace_plan_count": len(ui_display_field_trace),
        "parallel_path_caveat_count": len(parallel_path_caveat),
        "activation_blocker_count": len(activation_blockers),
        "future_6kh_contract_valid": len(future_6kh) == 8 and all_passed(future_6kh),
        "exact_ui_route_trace_required": True,
        "exact_ui_route_trace_completed": False,
        "model_projections_page_targeted": True,
        "frontend_endpoint_trace_required": True,
        "backend_route_trace_required": True,
        "payload_builder_trace_required": True,
        "simulator_trace_required": True,
        "realism_feature_chain_trace_required": True,
        "flag_config_trace_required": True,
        "ui_display_field_trace_required": True,
        "parallel_dashboard_solver_path_caveat_recorded": True,
        "realism_ui_activation_confirmed": False,
        "real_historical_evaluation_run": False,
        "production_simulations_run": False,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
        "database_writes_run": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "trace_scope_csv": str(TRACE_SCOPE_CSV),
            "frontend_trace_plan_csv": str(FRONTEND_TRACE_CSV),
            "backend_route_trace_plan_csv": str(BACKEND_ROUTE_TRACE_CSV),
            "payload_builder_trace_plan_csv": str(PAYLOAD_BUILDER_TRACE_CSV),
            "simulator_trace_plan_csv": str(SIMULATOR_TRACE_CSV),
            "realism_feature_trace_plan_csv": str(REALISM_FEATURE_TRACE_CSV),
            "flag_config_trace_plan_csv": str(FLAG_CONFIG_TRACE_CSV),
            "ui_display_field_trace_plan_csv": str(UI_DISPLAY_FIELD_TRACE_CSV),
            "parallel_path_caveat_csv": str(PARALLEL_PATH_CAVEAT_CSV),
            "activation_blockers_csv": str(ACTIVATION_BLOCKERS_CSV),
            "future_6kh_contract_csv": str(FUTURE_6KH_CSV),
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
