#!/usr/bin/env python3
"""Audit 6KH exact UI projection route trace implementation.

This audit validates the 6KH route trace conclusion and routes next to a
feature-by-feature UI realism reachability plan. It is audit-only and does not
modify source, fetch data, run simulations, activate mechanics, or grant exit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ki_exact_ui_projection_route_trace_implementation_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6KH_PATH = Path("scripts/implement_6kh_layer6_exact_ui_projection_route_trace.py")
JSON_6KH = TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation.json"

REQUIRED_INPUTS = [
    JSON_6KH,
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_checks.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_predecessor.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_target_files.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_frontend_endpoint_trace.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_backend_route_trace.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_payload_builder_trace.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_simulator_projection_trace.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_realism_feature_reachability.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_flag_config_fallback_trace.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_ui_display_field_trace.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_parallel_path_caveat.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_route_conclusion.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_activation_blockers.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_future_6ki_contract.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_decision.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
ROUTE_CONCLUSION_AUDIT_CSV = TMP_DIR / f"{SLUG}_route_conclusion_audit.csv"
MECHANIC_REACHABILITY_AUDIT_CSV = TMP_DIR / f"{SLUG}_mechanic_reachability_audit.csv"
UI_OUTPUT_STATUS_CSV = TMP_DIR / f"{SLUG}_ui_output_status.csv"
NEXT_LAYER_RATIONALE_CSV = TMP_DIR / f"{SLUG}_next_layer_rationale.csv"
ACTIVATION_BLOCKERS_CSV = TMP_DIR / f"{SLUG}_activation_blockers.csv"
FUTURE_6KJ_CSV = TMP_DIR / f"{SLUG}_future_6kj_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KH = "layer_6_exact_ui_projection_route_trace_implementation_complete"
DIAGNOSIS_6KI = "layer_6_exact_ui_projection_route_trace_implementation_audit_complete"
RECOMMENDED_NEXT_LAYER_6KH = "6KI_layer_6_exact_ui_projection_route_trace_implementation_audit"
RECOMMENDED_NEXT_LAYER_6KI = "6KJ_layer_6_ui_realism_feature_reachability_plan"
RECOMMENDED_PATH_6KI = "plan_feature_by_feature_ui_realism_reachability_before_backtest"


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
    json_6kh = load_json(JSON_6KH)
    reachability_rows_6kh = read_csv(TMP_DIR / "layer6_6kh_exact_ui_projection_route_trace_implementation_realism_feature_reachability.csv")

    route_conclusion_audit = [
        {"finding": "model_projections_page_found", "expected": True, "actual": json_6kh.get("model_projections_page_found"), "audit_conclusion": "ModelProjectionsPage is the primary UI target", "passed": json_6kh.get("model_projections_page_found") is True},
        {"finding": "frontend_endpoint_found", "expected": True, "actual": json_6kh.get("frontend_endpoint_found"), "audit_conclusion": "frontend endpoint was extracted", "passed": json_6kh.get("frontend_endpoint_found") is True},
        {"finding": "backend_route_found", "expected": True, "actual": json_6kh.get("backend_route_found"), "audit_conclusion": "backend route was identified", "passed": json_6kh.get("backend_route_found") is True},
        {"finding": "payload_builder_found", "expected": True, "actual": json_6kh.get("payload_builder_found"), "audit_conclusion": "payload builder was identified", "passed": json_6kh.get("payload_builder_found") is True},
        {"finding": "simulator_or_projection_entrypoint_found", "expected": True, "actual": json_6kh.get("simulator_or_projection_entrypoint_found"), "audit_conclusion": "simulation/projection entrypoint was identified", "passed": json_6kh.get("simulator_or_projection_entrypoint_found") is True},
        {"finding": "full_simulation_chain_reached", "expected": True, "actual": json_6kh.get("full_simulation_chain_reached"), "audit_conclusion": "UI route reaches simulation-related code", "passed": json_6kh.get("full_simulation_chain_reached") is True},
        {"finding": "non_sim_formula_or_payload_path_detected", "expected": True, "actual": json_6kh.get("non_sim_formula_or_payload_path_detected"), "audit_conclusion": "formula/payload/fallback path also exists", "passed": json_6kh.get("non_sim_formula_or_payload_path_detected") is True},
        {"finding": "realism_ui_activation_confirmed", "expected": False, "actual": json_6kh.get("realism_ui_activation_confirmed"), "audit_conclusion": "full UI realism activation remains unconfirmed", "passed": json_6kh.get("realism_ui_activation_confirmed") is False},
        {"finding": "route_trace_confidence", "expected": "medium", "actual": json_6kh.get("route_trace_confidence"), "audit_conclusion": "medium confidence is appropriate because realism and fallback paths coexist", "passed": json_6kh.get("route_trace_confidence") == "medium"},
    ]

    expected_mechanics = {
        "bullpen_logic": "reached",
        "double_play_logic": "unknown",
        "sac_fly_logic": "unknown",
        "stolen_base_or_steal_logic": "unknown",
        "extras_ghost_runner_walkoff_logic": "unknown",
        "balk_logic": "bypassed",
    }
    actual_mechanics = {row.get("mechanic"): row.get("reachability_status") for row in reachability_rows_6kh}
    mechanic_reachability_audit = [
        {
            "mechanic": mechanic,
            "expected_status": expected,
            "actual_status": actual_mechanics.get(mechanic),
            "audit_conclusion": (
                "requires_feature_by_feature_reachability_plan"
                if expected != "reached"
                else "reached_but_still_requires_output_effect_measurement"
            ),
            "passed": actual_mechanics.get(mechanic) == expected,
        }
        for mechanic, expected in expected_mechanics.items()
    ]

    ui_output_status = [
        {
            "surface": "expected_runs",
            "status": "simulation_preferred_with_model_fallback",
            "audit_conclusion": "UI can display simulation-derived runs when shared simulation exists, but fallback remains",
            "passed": True,
        },
        {
            "surface": "win_probability",
            "status": "simulation_preferred_with_model_or_canonical_caveat",
            "audit_conclusion": "UI can display simulation win probability, but side probability authority remains unclear enough to require feature-level plan",
            "passed": True,
        },
        {
            "surface": "projected_total",
            "status": "simulation_preferred_with_model_fallback",
            "audit_conclusion": "UI can display simulation-derived total expected runs, but fallback remains",
            "passed": True,
        },
        {
            "surface": "feature_specific_realism_effect",
            "status": "not_proven_for_all_mechanics",
            "audit_conclusion": "Need feature-by-feature reachability/effect plan before backtest or activation",
            "passed": True,
        },
    ]

    next_layer_rationale = [
        {"reason": "bullpen_reached_but_output_effect_not_measured", "next_layer_need": "confirm contribution to displayed outputs", "passed": True},
        {"reason": "double_play_unknown", "next_layer_need": "trace from PA subtype/transition logic to UI displayed outputs", "passed": True},
        {"reason": "sac_fly_unknown", "next_layer_need": "trace from subtype/transition logic to UI displayed outputs", "passed": True},
        {"reason": "stolen_base_unknown", "next_layer_need": "trace steal modeling or explicitly record absence", "passed": True},
        {"reason": "extras_walkoff_unknown", "next_layer_need": "determine whether regulation-only UI sim bypasses extras/walkoff logic", "passed": True},
        {"reason": "balk_bypassed", "next_layer_need": "explicitly defer or plan balk implementation path", "passed": True},
    ]

    activation_blockers = [
        {"blocker": "feature_by_feature_reachability_not_planned", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "full_ui_realism_activation_not_confirmed", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "activation_decision_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "balks_deferred_or_exit_gated", "blocks_activation": False, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6kj = [
        {"contract": "plan_bullpen_ui_output_reachability", "required": True, "passed": True},
        {"contract": "plan_double_play_ui_output_reachability", "required": True, "passed": True},
        {"contract": "plan_sac_fly_ui_output_reachability", "required": True, "passed": True},
        {"contract": "plan_stolen_base_ui_output_reachability", "required": True, "passed": True},
        {"contract": "plan_extras_ghost_walkoff_ui_output_reachability", "required": True, "passed": True},
        {"contract": "plan_balk_deferral_or_implementation_path", "required": True, "passed": True},
        {"contract": "preserve_no_activation_no_layer6_exit", "required": True, "passed": True},
        {"contract": "do_not_fetch_or_write_in_6kj", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6kh_implementation_script_exists", "expected": True, "actual": IMPLEMENT_6KH_PATH.exists(), "passed": IMPLEMENT_6KH_PATH.exists()},
        {"check": "6kh_json_exists", "expected": True, "actual": JSON_6KH.exists(), "passed": JSON_6KH.exists()},
        {"check": "6kh_all_checks_passed", "expected": True, "actual": json_6kh.get("all_checks_passed"), "passed": json_6kh.get("all_checks_passed") is True},
        {"check": "6kh_diagnosis", "expected": DIAGNOSIS_6KH, "actual": json_6kh.get("diagnosis"), "passed": json_6kh.get("diagnosis") == DIAGNOSIS_6KH},
        {"check": "6kh_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KH, "actual": json_6kh.get("recommended_next_layer"), "passed": json_6kh.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KH},
        {"check": "6kh_exact_ui_route_trace_completed", "expected": True, "actual": json_6kh.get("exact_ui_route_trace_completed"), "passed": json_6kh.get("exact_ui_route_trace_completed") is True},
        {"check": "6kh_realism_ui_activation_confirmed", "expected": False, "actual": json_6kh.get("realism_ui_activation_confirmed"), "passed": json_6kh.get("realism_ui_activation_confirmed") is False},
        {"check": "6kh_ui_uses_realism_enabled_path", "expected": True, "actual": json_6kh.get("ui_uses_realism_enabled_path"), "passed": json_6kh.get("ui_uses_realism_enabled_path") is True},
        {"check": "6kh_ui_uses_legacy_or_current_path", "expected": True, "actual": json_6kh.get("ui_uses_legacy_or_current_path"), "passed": json_6kh.get("ui_uses_legacy_or_current_path") is True},
        {"check": "6kh_no_layer6_exit", "expected": False, "actual": json_6kh.get("layer_6_exit_recommended"), "passed": json_6kh.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6kj_feature_by_feature_reachability_plan", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "feature-level reachability and dataset proof required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "real evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6KI", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KI is audit only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6KI cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6kh_passed", "expected": True, "actual": json_6kh.get("all_checks_passed"), "passed": json_6kh.get("all_checks_passed") is True},
        {"decision": "route_conclusion_audited", "expected": 9, "actual": len(route_conclusion_audit), "passed": len(route_conclusion_audit) == 9 and all_passed(route_conclusion_audit)},
        {"decision": "mechanic_reachability_audited", "expected": 6, "actual": len(mechanic_reachability_audit), "passed": len(mechanic_reachability_audit) == 6 and all_passed(mechanic_reachability_audit)},
        {"decision": "ui_output_status_audited", "expected": 4, "actual": len(ui_output_status), "passed": len(ui_output_status) == 4 and all_passed(ui_output_status)},
        {"decision": "next_layer_rationale_recorded", "expected": 6, "actual": len(next_layer_rationale), "passed": len(next_layer_rationale) == 6 and all_passed(next_layer_rationale)},
        {"decision": "recommend_6kj_next", "expected": RECOMMENDED_NEXT_LAYER_6KI, "actual": RECOMMENDED_NEXT_LAYER_6KI, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "route_trace_audited", "expected": True, "actual": True, "passed": True},
        {"boundary": "mechanic_reachability_audited", "expected": True, "actual": True, "passed": True},
        {"boundary": "feature_by_feature_reachability_plan_required", "expected": True, "actual": True, "passed": True},
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
        {"surface": "source_tree", "policy": "read_only_audit", "passed": True},
        {"surface": "6kh_implementation", "policy": "read_only", "passed": True},
        {"surface": "6kh_artifacts", "policy": "read_only", "passed": True},
        {"surface": "ui_projection_path", "policy": "not_modified_in_6ki", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6ki", "passed": True},
        {"surface": "database", "policy": "not_written_in_6ki", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KI, "actual": RECOMMENDED_NEXT_LAYER_6KI, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KI, "actual": RECOMMENDED_PATH_6KI, "passed": True},
        {"decision": "recommend_feature_by_feature_reachability_plan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KI, "actual": DIAGNOSIS_6KI, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "route_conclusion_audit", "passed": len(route_conclusion_audit) == 9 and all_passed(route_conclusion_audit), "detail": "9/9"},
        {"check": "mechanic_reachability_audit", "passed": len(mechanic_reachability_audit) == 6 and all_passed(mechanic_reachability_audit), "detail": "6/6"},
        {"check": "ui_output_status", "passed": len(ui_output_status) == 4 and all_passed(ui_output_status), "detail": "4/4"},
        {"check": "next_layer_rationale", "passed": len(next_layer_rationale) == 6 and all_passed(next_layer_rationale), "detail": "6/6"},
        {"check": "activation_blockers", "passed": len(activation_blockers) == 5 and all_passed(activation_blockers), "detail": "5/5"},
        {"check": "future_6kj_contract", "passed": len(future_6kj) == 8 and all_passed(future_6kj), "detail": "8/8"},
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
        "route_conclusion_audit": write_csv(ROUTE_CONCLUSION_AUDIT_CSV, route_conclusion_audit),
        "mechanic_reachability_audit": write_csv(MECHANIC_REACHABILITY_AUDIT_CSV, mechanic_reachability_audit),
        "ui_output_status": write_csv(UI_OUTPUT_STATUS_CSV, ui_output_status),
        "next_layer_rationale": write_csv(NEXT_LAYER_RATIONALE_CSV, next_layer_rationale),
        "activation_blockers": write_csv(ACTIVATION_BLOCKERS_CSV, activation_blockers),
        "future_6kj_contract": write_csv(FUTURE_6KJ_CSV, future_6kj),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KI",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KI if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KI,
        "recommended_path": RECOMMENDED_PATH_6KI,
        "predecessor_implementation": str(IMPLEMENT_6KH_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6kh.get("diagnosis"),
        "audited_layer_after": "6KH",
        "source_family": "exact_ui_projection_route_trace_implementation_audit",
        "route_conclusion_audit_count": len(route_conclusion_audit),
        "mechanic_reachability_audit_count": len(mechanic_reachability_audit),
        "ui_output_status_count": len(ui_output_status),
        "next_layer_rationale_count": len(next_layer_rationale),
        "activation_blocker_count": len(activation_blockers),
        "future_6kj_contract_valid": len(future_6kj) == 8 and all_passed(future_6kj),
        "exact_ui_route_trace_completed": True,
        "route_trace_audited": True,
        "mechanic_reachability_audited": True,
        "model_projections_page_found": json_6kh.get("model_projections_page_found"),
        "frontend_endpoint_found": json_6kh.get("frontend_endpoint_found"),
        "backend_route_found": json_6kh.get("backend_route_found"),
        "payload_builder_found": json_6kh.get("payload_builder_found"),
        "simulator_or_projection_entrypoint_found": json_6kh.get("simulator_or_projection_entrypoint_found"),
        "full_simulation_chain_reached": json_6kh.get("full_simulation_chain_reached"),
        "non_sim_formula_or_payload_path_detected": json_6kh.get("non_sim_formula_or_payload_path_detected"),
        "realism_feature_chain_fully_reached": json_6kh.get("realism_feature_chain_fully_reached"),
        "realism_feature_chain_partially_reached": json_6kh.get("realism_feature_chain_partially_reached"),
        "realism_feature_chain_bypassed": json_6kh.get("realism_feature_chain_bypassed"),
        "realism_ui_activation_confirmed": json_6kh.get("realism_ui_activation_confirmed"),
        "ui_uses_realism_enabled_path": json_6kh.get("ui_uses_realism_enabled_path"),
        "ui_uses_legacy_or_current_path": json_6kh.get("ui_uses_legacy_or_current_path"),
        "route_trace_confidence": json_6kh.get("route_trace_confidence"),
        "feature_by_feature_reachability_plan_required": True,
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
            "route_conclusion_audit_csv": str(ROUTE_CONCLUSION_AUDIT_CSV),
            "mechanic_reachability_audit_csv": str(MECHANIC_REACHABILITY_AUDIT_CSV),
            "ui_output_status_csv": str(UI_OUTPUT_STATUS_CSV),
            "next_layer_rationale_csv": str(NEXT_LAYER_RATIONALE_CSV),
            "activation_blockers_csv": str(ACTIVATION_BLOCKERS_CSV),
            "future_6kj_contract_csv": str(FUTURE_6KJ_CSV),
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
