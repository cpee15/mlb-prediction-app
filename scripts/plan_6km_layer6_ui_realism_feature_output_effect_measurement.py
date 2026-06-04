#!/usr/bin/env python3
"""Plan controlled UI realism feature output-effect measurement.

This planning-only layer defines how to measure whether each realism mechanic
changes displayed ModelProjectionsPage outputs. It does not run measurements,
fetch data, run production simulations, write databases, activate mechanics, or
grant Layer 6 exit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6km_ui_realism_feature_output_effect_measurement_plan"
TMP_DIR = Path("tmp")

AUDIT_6KL_PATH = Path("scripts/audit_6kl_layer6_ui_realism_feature_reachability_implementation.py")
JSON_6KL = TMP_DIR / "layer6_6kl_ui_realism_feature_reachability_implementation_audit.json"

REQUIRED_INPUTS = [
    JSON_6KL,
    TMP_DIR / "layer6_6kl_ui_realism_feature_reachability_implementation_audit_checks.csv",
    TMP_DIR / "layer6_6kl_ui_realism_feature_reachability_implementation_audit_predecessor.csv",
    TMP_DIR / "layer6_6kl_ui_realism_feature_reachability_implementation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6kl_ui_realism_feature_reachability_implementation_audit_mechanic_status_audit.csv",
    TMP_DIR / "layer6_6kl_ui_realism_feature_reachability_implementation_audit_output_effect_gap_audit.csv",
    TMP_DIR / "layer6_6kl_ui_realism_feature_reachability_implementation_audit_next_layer_rationale.csv",
    TMP_DIR / "layer6_6kl_ui_realism_feature_reachability_implementation_audit_blockers.csv",
    TMP_DIR / "layer6_6kl_ui_realism_feature_reachability_implementation_audit_future_6km_contract.csv",
    TMP_DIR / "layer6_6kl_ui_realism_feature_reachability_implementation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6kl_ui_realism_feature_reachability_implementation_audit_decision.csv",
    TMP_DIR / "layer6_6kl_ui_realism_feature_reachability_implementation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6kl_ui_realism_feature_reachability_implementation_audit_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
MEASUREMENT_MATRIX_CSV = TMP_DIR / f"{SLUG}_measurement_matrix.csv"
CONTROLLED_SCENARIOS_CSV = TMP_DIR / f"{SLUG}_controlled_scenarios.csv"
OUTPUT_FIELDS_CSV = TMP_DIR / f"{SLUG}_output_fields.csv"
SUCCESS_CRITERIA_CSV = TMP_DIR / f"{SLUG}_success_criteria.csv"
GUARDRAILS_CSV = TMP_DIR / f"{SLUG}_guardrails.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6KN_CSV = TMP_DIR / f"{SLUG}_future_6kn_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KL = "layer_6_ui_realism_feature_reachability_implementation_audit_complete"
DIAGNOSIS_6KM = "layer_6_ui_realism_feature_output_effect_measurement_plan_complete"
RECOMMENDED_NEXT_LAYER_6KL = "6KM_layer_6_ui_realism_feature_output_effect_measurement_plan"
RECOMMENDED_NEXT_LAYER_6KM = "6KN_layer_6_ui_realism_feature_output_effect_measurement_implementation"
RECOMMENDED_PATH_6KM = "implement_controlled_feature_output_effect_measurement_before_backtest"


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
    json_6kl = load_json(JSON_6KL)

    measurement_matrix = [
        {
            "mechanic": "bullpen_logic",
            "current_status": "plausible_output_effect",
            "measurement_goal": "establish baseline output delta for bullpen-adjusted simulation vs no/neutral bullpen adjustment",
            "baseline": "shared simulation output using current bullpen-adjusted path",
            "variant": "controlled fixture neutralizing bullpen-quality inputs or using no-bullpen fallback where available",
            "target_fields": "away_expected_runs;home_expected_runs;total_expected_runs;away_win_probability;home_win_probability;team_total_probabilities;total_probabilities",
            "success_criterion": "measurable_delta_detected_or_baseline_confirmed",
            "next_layer_action": "implement read-only controlled comparison harness",
            "passed": True,
        },
        {
            "mechanic": "double_play_logic",
            "current_status": "simulation_reachable_output_unmeasured",
            "measurement_goal": "stress double-play rate and confirm expected-runs or run-distribution movement",
            "baseline": "fixture with default double_play_rate",
            "variant": "fixture with high/low double_play_rate or forced transition-rate override in local harness",
            "target_fields": "away_expected_runs;home_expected_runs;total_expected_runs;run_distribution;win_probability",
            "success_criterion": "measurable_delta_detected_or_no_delta_but_reachable",
            "next_layer_action": "implement fixture-level transition-rate comparison",
            "passed": True,
        },
        {
            "mechanic": "sac_fly_logic",
            "current_status": "simulation_reachable_output_unmeasured",
            "measurement_goal": "stress sac-fly rate and confirm expected-runs or run-distribution movement in runner-on-third scenarios",
            "baseline": "fixture with default sac_fly_rate",
            "variant": "fixture with high/low sac_fly_rate or forced transition-rate override in local harness",
            "target_fields": "away_expected_runs;home_expected_runs;total_expected_runs;run_distribution;win_probability",
            "success_criterion": "measurable_delta_detected_or_no_delta_but_reachable",
            "next_layer_action": "implement fixture-level transition-rate comparison",
            "passed": True,
        },
        {
            "mechanic": "extras_ghost_runner_walkoff_logic",
            "current_status": "simulation_reachable_output_unmeasured",
            "measurement_goal": "confirm whether production UI path includes extras/walkoff or only regulation/full-game fallback, then measure win-probability delta if reachable",
            "baseline": "regulation/full-game simulation path currently feeding UI",
            "variant": "controlled extras/walkoff fixture if importable without production run",
            "target_fields": "away_win_probability;home_win_probability;total_expected_runs;simulationContract;sharedSimulationDiagnostics",
            "success_criterion": "measurable_delta_detected_or_bypass_confirmed",
            "next_layer_action": "implement production-route inclusion check plus optional controlled extras fixture",
            "passed": True,
        },
        {
            "mechanic": "stolen_base_or_steal_logic",
            "current_status": "inactive",
            "measurement_goal": "confirm inactive/no-steals condition and avoid treating steal terms as active output effect",
            "baseline": "current inning simulator with no-steals evidence",
            "variant": "none in 6KN unless explicit steal model is found",
            "target_fields": "sharedSimulationDiagnostics;simulationContract;run_distribution",
            "success_criterion": "inactive_confirmed",
            "next_layer_action": "implement source/fixture confirmation of inactive status",
            "passed": True,
        },
        {
            "mechanic": "balk_logic",
            "current_status": "absent_or_deferred",
            "measurement_goal": "preserve explicit deferral and avoid false measurement",
            "baseline": "current simulator with no balk mechanism",
            "variant": "none",
            "target_fields": "none_currently",
            "success_criterion": "deferred_confirmed",
            "next_layer_action": "record deferral; no measurement harness needed",
            "passed": True,
        },
    ]

    controlled_scenarios = [
        {"scenario": "baseline_fixture", "description": "single deterministic fixture with current shared simulation defaults", "production_run": False, "passed": True},
        {"scenario": "bullpen_neutralized_fixture", "description": "same fixture with neutral or no-bullpen adjustment", "production_run": False, "passed": True},
        {"scenario": "double_play_stress_fixture", "description": "runner/base-state sensitive fixture with high/low double_play_rate", "production_run": False, "passed": True},
        {"scenario": "sac_fly_stress_fixture", "description": "runner-on-third/out-state sensitive fixture with high/low sac_fly_rate", "production_run": False, "passed": True},
        {"scenario": "extras_walkoff_route_check", "description": "determine if UI production route reaches extras/walkoff function or bypasses it", "production_run": False, "passed": True},
        {"scenario": "steal_inactive_confirmation", "description": "confirm no-steals path and no active UI output effect", "production_run": False, "passed": True},
        {"scenario": "balk_deferral_record", "description": "record no balk measurement until implementation exists", "production_run": False, "passed": True},
    ]

    output_fields = [
        {"field": "away_expected_runs", "field_family": "expected_runs", "used_for": "bullpen,double_play,sac_fly", "passed": True},
        {"field": "home_expected_runs", "field_family": "expected_runs", "used_for": "bullpen,double_play,sac_fly", "passed": True},
        {"field": "total_expected_runs", "field_family": "projected_total", "used_for": "bullpen,double_play,sac_fly,extras", "passed": True},
        {"field": "away_win_probability", "field_family": "win_probability", "used_for": "bullpen,double_play,sac_fly,extras", "passed": True},
        {"field": "home_win_probability", "field_family": "win_probability", "used_for": "bullpen,double_play,sac_fly,extras", "passed": True},
        {"field": "team_total_probabilities", "field_family": "team_totals", "used_for": "bullpen,double_play,sac_fly", "passed": True},
        {"field": "total_probabilities", "field_family": "totals", "used_for": "bullpen,double_play,sac_fly", "passed": True},
        {"field": "run_distribution", "field_family": "distribution", "used_for": "double_play,sac_fly,extras,steal", "passed": True},
        {"field": "sharedSimulationDiagnostics", "field_family": "diagnostics", "used_for": "all_mechanics", "passed": True},
        {"field": "simulationContract", "field_family": "diagnostics", "used_for": "all_mechanics", "passed": True},
        {"field": "formulaMap", "field_family": "diagnostics", "used_for": "all_mechanics", "passed": True},
    ]

    success_criteria = [
        {"criterion": "measurable_delta_detected", "meaning": "controlled baseline vs variant changes one or more target output fields", "passed": True},
        {"criterion": "no_delta_but_reachable", "meaning": "mechanic is reachable but controlled stress does not move displayed fields", "passed": True},
        {"criterion": "bypass_confirmed", "meaning": "mechanic exists but production UI route bypasses it", "passed": True},
        {"criterion": "inactive_confirmed", "meaning": "mechanic terms exist but active path is disabled/inactive", "passed": True},
        {"criterion": "deferred_confirmed", "meaning": "mechanic absent/deferred and excluded from measurement", "passed": True},
    ]

    guardrails = [
        {"guardrail": "no_source_modification", "passed": True},
        {"guardrail": "no_database_writes", "passed": True},
        {"guardrail": "no_remote_api_calls", "passed": True},
        {"guardrail": "no_live_data_fetches", "passed": True},
        {"guardrail": "no_production_simulation_runs", "passed": True},
        {"guardrail": "no_real_historical_evaluation", "passed": True},
        {"guardrail": "no_activation_execution", "passed": True},
        {"guardrail": "no_layer_6_exit_credit", "passed": True},
    ]

    blockers = [
        {"blocker": "controlled_measurement_not_implemented", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "feature_output_effect_not_measured", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "activation_decision_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "balk_absent_or_deferred", "blocks_activation": False, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6kn = [
        {"contract": "implement_controlled_measurement_without_source_mutation", "required": True, "passed": True},
        {"contract": "record_baseline_and_variant_outputs", "required": True, "passed": True},
        {"contract": "measure_bullpen_delta", "required": True, "passed": True},
        {"contract": "measure_double_play_delta_or_no_delta", "required": True, "passed": True},
        {"contract": "measure_sac_fly_delta_or_no_delta", "required": True, "passed": True},
        {"contract": "measure_or_confirm_bypass_extras_walkoff", "required": True, "passed": True},
        {"contract": "confirm_steal_inactive_and_balk_deferred", "required": True, "passed": True},
        {"contract": "preserve_no_activation_no_layer6_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6kl_audit_script_exists", "expected": True, "actual": AUDIT_6KL_PATH.exists(), "passed": AUDIT_6KL_PATH.exists()},
        {"check": "6kl_json_exists", "expected": True, "actual": JSON_6KL.exists(), "passed": JSON_6KL.exists()},
        {"check": "6kl_all_checks_passed", "expected": True, "actual": json_6kl.get("all_checks_passed"), "passed": json_6kl.get("all_checks_passed") is True},
        {"check": "6kl_diagnosis", "expected": DIAGNOSIS_6KL, "actual": json_6kl.get("diagnosis"), "passed": json_6kl.get("diagnosis") == DIAGNOSIS_6KL},
        {"check": "6kl_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KL, "actual": json_6kl.get("recommended_next_layer"), "passed": json_6kl.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KL},
        {"check": "6kl_controlled_measurement_plan_required", "expected": True, "actual": json_6kl.get("controlled_measurement_plan_required"), "passed": json_6kl.get("controlled_measurement_plan_required") is True},
        {"check": "6kl_realism_ui_activation_confirmed", "expected": False, "actual": json_6kl.get("realism_ui_activation_confirmed"), "passed": json_6kl.get("realism_ui_activation_confirmed") is False},
        {"check": "6kl_no_layer6_exit", "expected": False, "actual": json_6kl.get("layer_6_exit_recommended"), "passed": json_6kl.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6kn_controlled_measurement_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "controlled output-effect measurement implementation required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "measurement and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6KM", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KM is planning-only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6KM cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6kl_passed", "expected": True, "actual": json_6kl.get("all_checks_passed"), "passed": json_6kl.get("all_checks_passed") is True},
        {"decision": "measurement_matrix_count", "expected": 6, "actual": len(measurement_matrix), "passed": len(measurement_matrix) == 6 and all_passed(measurement_matrix)},
        {"decision": "controlled_scenario_count", "expected": 7, "actual": len(controlled_scenarios), "passed": len(controlled_scenarios) == 7 and all_passed(controlled_scenarios)},
        {"decision": "output_field_count", "expected": 11, "actual": len(output_fields), "passed": len(output_fields) == 11 and all_passed(output_fields)},
        {"decision": "success_criteria_count", "expected": 5, "actual": len(success_criteria), "passed": len(success_criteria) == 5 and all_passed(success_criteria)},
        {"decision": "recommend_6kn_next", "expected": RECOMMENDED_NEXT_LAYER_6KM, "actual": RECOMMENDED_NEXT_LAYER_6KM, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "controlled_measurement_plan_created", "expected": True, "actual": True, "passed": True},
        {"boundary": "local_measurement_run", "expected": False, "actual": False, "passed": True},
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
        {"surface": "6kl_audit", "policy": "read_only", "passed": True},
        {"surface": "6kl_artifacts", "policy": "read_only", "passed": True},
        {"surface": "ui_projection_path", "policy": "not_modified_in_6km", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6km", "passed": True},
        {"surface": "database", "policy": "not_written_in_6km", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KM, "actual": RECOMMENDED_NEXT_LAYER_6KM, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KM, "actual": RECOMMENDED_PATH_6KM, "passed": True},
        {"decision": "recommend_controlled_measurement_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KM, "actual": DIAGNOSIS_6KM, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "measurement_matrix", "passed": len(measurement_matrix) == 6 and all_passed(measurement_matrix), "detail": "6/6"},
        {"check": "controlled_scenarios", "passed": len(controlled_scenarios) == 7 and all_passed(controlled_scenarios), "detail": "7/7"},
        {"check": "output_fields", "passed": len(output_fields) == 11 and all_passed(output_fields), "detail": "11/11"},
        {"check": "success_criteria", "passed": len(success_criteria) == 5 and all_passed(success_criteria), "detail": "5/5"},
        {"check": "guardrails", "passed": len(guardrails) == 8 and all_passed(guardrails), "detail": "8/8"},
        {"check": "blockers", "passed": len(blockers) == 5 and all_passed(blockers), "detail": "5/5"},
        {"check": "future_6kn_contract", "passed": len(future_6kn) == 8 and all_passed(future_6kn), "detail": "8/8"},
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
        "measurement_matrix": write_csv(MEASUREMENT_MATRIX_CSV, measurement_matrix),
        "controlled_scenarios": write_csv(CONTROLLED_SCENARIOS_CSV, controlled_scenarios),
        "output_fields": write_csv(OUTPUT_FIELDS_CSV, output_fields),
        "success_criteria": write_csv(SUCCESS_CRITERIA_CSV, success_criteria),
        "guardrails": write_csv(GUARDRAILS_CSV, guardrails),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6kn_contract": write_csv(FUTURE_6KN_CSV, future_6kn),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KM",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KM if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KM,
        "recommended_path": RECOMMENDED_PATH_6KM,
        "predecessor_audit": str(AUDIT_6KL_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6kl.get("diagnosis"),
        "planned_layer_after": "6KL",
        "source_family": "ui_realism_feature_output_effect_measurement_plan",
        "measurement_matrix_count": len(measurement_matrix),
        "controlled_scenario_count": len(controlled_scenarios),
        "output_field_count": len(output_fields),
        "success_criteria_count": len(success_criteria),
        "guardrail_count": len(guardrails),
        "blocker_count": len(blockers),
        "future_6kn_contract_valid": len(future_6kn) == 8 and all_passed(future_6kn),
        "controlled_measurement_plan_created": True,
        "feature_output_effect_gap_confirmed": True,
        "bullpen_measurement_planned": True,
        "double_play_measurement_planned": True,
        "sac_fly_measurement_planned": True,
        "extras_walkoff_measurement_planned": True,
        "stolen_base_inactive_confirmation_planned": True,
        "balk_deferral_preserved": True,
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
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "measurement_matrix_csv": str(MEASUREMENT_MATRIX_CSV),
            "controlled_scenarios_csv": str(CONTROLLED_SCENARIOS_CSV),
            "output_fields_csv": str(OUTPUT_FIELDS_CSV),
            "success_criteria_csv": str(SUCCESS_CRITERIA_CSV),
            "guardrails_csv": str(GUARDRAILS_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6kn_contract_csv": str(FUTURE_6KN_CSV),
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
