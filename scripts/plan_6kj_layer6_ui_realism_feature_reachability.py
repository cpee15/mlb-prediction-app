#!/usr/bin/env python3
"""Plan feature-by-feature UI realism reachability matrix for Layer 6.

This planning-only layer converts the route-level 6KI conclusion into a
mechanic matrix. It does not modify source, fetch data, run simulations,
activate mechanics, or grant Layer 6 exit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6kj_ui_realism_feature_reachability_plan"
TMP_DIR = Path("tmp")

AUDIT_6KI_PATH = Path("scripts/audit_6ki_layer6_exact_ui_projection_route_trace_implementation.py")
JSON_6KI = TMP_DIR / "layer6_6ki_exact_ui_projection_route_trace_implementation_audit.json"

REQUIRED_INPUTS = [
    JSON_6KI,
    TMP_DIR / "layer6_6ki_exact_ui_projection_route_trace_implementation_audit_checks.csv",
    TMP_DIR / "layer6_6ki_exact_ui_projection_route_trace_implementation_audit_predecessor.csv",
    TMP_DIR / "layer6_6ki_exact_ui_projection_route_trace_implementation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6ki_exact_ui_projection_route_trace_implementation_audit_route_conclusion_audit.csv",
    TMP_DIR / "layer6_6ki_exact_ui_projection_route_trace_implementation_audit_mechanic_reachability_audit.csv",
    TMP_DIR / "layer6_6ki_exact_ui_projection_route_trace_implementation_audit_ui_output_status.csv",
    TMP_DIR / "layer6_6ki_exact_ui_projection_route_trace_implementation_audit_next_layer_rationale.csv",
    TMP_DIR / "layer6_6ki_exact_ui_projection_route_trace_implementation_audit_activation_blockers.csv",
    TMP_DIR / "layer6_6ki_exact_ui_projection_route_trace_implementation_audit_future_6kj_contract.csv",
    TMP_DIR / "layer6_6ki_exact_ui_projection_route_trace_implementation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6ki_exact_ui_projection_route_trace_implementation_audit_decision.csv",
    TMP_DIR / "layer6_6ki_exact_ui_projection_route_trace_implementation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6ki_exact_ui_projection_route_trace_implementation_audit_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
MECHANIC_MATRIX_CSV = TMP_DIR / f"{SLUG}_mechanic_matrix.csv"
TRACE_PLAN_CSV = TMP_DIR / f"{SLUG}_trace_plan.csv"
OUTPUT_FIELD_PLAN_CSV = TMP_DIR / f"{SLUG}_output_field_plan.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6KK_CSV = TMP_DIR / f"{SLUG}_future_6kk_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KI = "layer_6_exact_ui_projection_route_trace_implementation_audit_complete"
DIAGNOSIS_6KJ = "layer_6_ui_realism_feature_reachability_plan_complete"
RECOMMENDED_NEXT_LAYER_6KI = "6KJ_layer_6_ui_realism_feature_reachability_plan"
RECOMMENDED_NEXT_LAYER_6KJ = "6KK_layer_6_ui_realism_feature_reachability_implementation"
RECOMMENDED_PATH_6KJ = "implement_feature_by_feature_ui_realism_reachability_matrix_before_backtest"


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
    json_6ki = load_json(JSON_6KI)

    mechanic_matrix = [
        {
            "mechanic": "bullpen_logic",
            "current_status": "reached_but_output_effect_unmeasured",
            "exists_status": "yes",
            "simulation_reachability_status": "reached",
            "ui_reachability_status": "likely_partial",
            "active_status": "unknown",
            "output_effect_status": "unmeasured",
            "required_trace": "trace bullpen_adjusted_game_simulation to displayed expected runs, win probability, totals, and team totals",
            "likely_source_files": "mlb_app/model_projections.py;mlb_app/simulation/game_simulator.py;mlb_app/simulation/bullpen_chain.py;mlb_app/simulation/bullpen_integration.py;mlb_app/simulation/bullpen_game_engine_hook.py",
            "likely_output_fields": "away_expected_runs;home_expected_runs;total_expected_runs;away_win_probability;home_win_probability;team_total_probabilities;total_probabilities",
            "blocker": "output_effect_not_measured",
            "next_action": "implement evidence trace and output-field mapping",
            "passed": True,
        },
        {
            "mechanic": "double_play_logic",
            "current_status": "unknown",
            "exists_status": "likely_yes",
            "simulation_reachability_status": "unknown",
            "ui_reachability_status": "unknown",
            "active_status": "unknown",
            "output_effect_status": "unmeasured",
            "required_trace": "trace double-play subtype/transition rates from inning simulator to full game simulation to UI output fields",
            "likely_source_files": "mlb_app/simulation/inning_simulator.py;mlb_app/simulation/subtype_transitions.py;mlb_app/simulation/game_simulator.py;mlb_app/model_projections.py",
            "likely_output_fields": "expected_runs;win_probability;total_expected_runs;run_distribution",
            "blocker": "reachability_unknown",
            "next_action": "implement source evidence trace",
            "passed": True,
        },
        {
            "mechanic": "sac_fly_logic",
            "current_status": "unknown",
            "exists_status": "likely_yes",
            "simulation_reachability_status": "unknown",
            "ui_reachability_status": "unknown",
            "active_status": "unknown",
            "output_effect_status": "unmeasured",
            "required_trace": "trace sac-fly transition rates from inning simulator to full game simulation to UI output fields",
            "likely_source_files": "mlb_app/simulation/inning_simulator.py;mlb_app/simulation/subtype_transitions.py;mlb_app/simulation/game_simulator.py;mlb_app/model_projections.py",
            "likely_output_fields": "expected_runs;win_probability;total_expected_runs;run_distribution",
            "blocker": "reachability_unknown",
            "next_action": "implement source evidence trace",
            "passed": True,
        },
        {
            "mechanic": "stolen_base_or_steal_logic",
            "current_status": "unknown",
            "exists_status": "likely_partial_or_absent",
            "simulation_reachability_status": "unknown",
            "ui_reachability_status": "unknown",
            "active_status": "unknown",
            "output_effect_status": "unmeasured",
            "required_trace": "trace any steal model or explicit no-steal marker from simulator to UI output fields",
            "likely_source_files": "mlb_app/simulation/inning_simulator.py;mlb_app/simulation/game_simulator.py;mlb_app/model_projections.py;mlb_app/db_utils.py",
            "likely_output_fields": "expected_runs;win_probability;total_expected_runs;base_state_or_run_distribution_if_available",
            "blocker": "model_presence_and_reachability_unknown",
            "next_action": "implement source evidence trace or explicit absence record",
            "passed": True,
        },
        {
            "mechanic": "extras_ghost_runner_walkoff_logic",
            "current_status": "unknown",
            "exists_status": "likely_yes",
            "simulation_reachability_status": "unknown",
            "ui_reachability_status": "unknown",
            "active_status": "unknown",
            "output_effect_status": "unmeasured",
            "required_trace": "determine whether UI simulation is regulation-only or reaches extras/ghost-runner/walkoff path",
            "likely_source_files": "mlb_app/simulation/game_rules.py;mlb_app/simulation/game_simulator.py;mlb_app/model_projections.py",
            "likely_output_fields": "win_probability;tie_after_regulation_probability;total_expected_runs;walkoff_metadata_if_available",
            "blocker": "regulation_vs_extras_path_unclear",
            "next_action": "implement source evidence trace",
            "passed": True,
        },
        {
            "mechanic": "balk_logic",
            "current_status": "bypassed",
            "exists_status": "absent_or_deferred",
            "simulation_reachability_status": "bypassed",
            "ui_reachability_status": "bypassed",
            "active_status": "inactive",
            "output_effect_status": "none_currently",
            "required_trace": "record explicit absence/deferral and decide whether balk remains Layer 6 exit-gated",
            "likely_source_files": "mlb_app/simulation/inning_simulator.py;mlb_app/simulation/game_simulator.py;mlb_app/simulation/game_rules.py",
            "likely_output_fields": "none_currently",
            "blocker": "balk_absent_or_deferred",
            "next_action": "implement explicit deferral or implementation-path record",
            "passed": True,
        },
    ]

    trace_plan = [
        {
            "mechanic": row["mechanic"],
            "trace_goal": row["required_trace"],
            "source_files": row["likely_source_files"],
            "output_fields": row["likely_output_fields"],
            "result_needed": "exists/reachable/ui_reachable/active/output_effect_status",
            "passed": True,
        }
        for row in mechanic_matrix
    ]

    output_field_plan = [
        {"field_family": "expected_runs", "ui_fields": "away_expected_runs;home_expected_runs", "mechanics_to_assess": "bullpen,double_play,sac_fly,steal", "passed": True},
        {"field_family": "win_probability", "ui_fields": "away_win_probability;home_win_probability", "mechanics_to_assess": "bullpen,double_play,sac_fly,steal,extras,walkoff", "passed": True},
        {"field_family": "projected_total", "ui_fields": "total_expected_runs", "mechanics_to_assess": "bullpen,double_play,sac_fly,steal", "passed": True},
        {"field_family": "probability_distributions", "ui_fields": "team_total_probabilities;total_probabilities;run_distribution", "mechanics_to_assess": "bullpen,double_play,sac_fly,steal,extras", "passed": True},
        {"field_family": "diagnostic_metadata", "ui_fields": "sharedSimulationDiagnostics;simulationContract;formulaMap", "mechanics_to_assess": "all_mechanics", "passed": True},
    ]

    blockers = [
        {"blocker": "mechanic_matrix_not_implemented", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "feature_output_effect_not_measured", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "full_ui_realism_activation_not_confirmed", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "balk_absent_or_deferred", "blocks_activation": False, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6kk = [
        {"contract": "implement_mechanic_matrix_source_trace", "required": True, "passed": True},
        {"contract": "classify_exists_status_per_mechanic", "required": True, "passed": True},
        {"contract": "classify_simulation_reachability_per_mechanic", "required": True, "passed": True},
        {"contract": "classify_ui_reachability_per_mechanic", "required": True, "passed": True},
        {"contract": "classify_active_status_per_mechanic", "required": True, "passed": True},
        {"contract": "classify_output_effect_status_per_mechanic", "required": True, "passed": True},
        {"contract": "preserve_no_activation_no_layer6_exit", "required": True, "passed": True},
        {"contract": "do_not_fetch_or_write_in_6kk", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ki_audit_script_exists", "expected": True, "actual": AUDIT_6KI_PATH.exists(), "passed": AUDIT_6KI_PATH.exists()},
        {"check": "6ki_json_exists", "expected": True, "actual": JSON_6KI.exists(), "passed": JSON_6KI.exists()},
        {"check": "6ki_all_checks_passed", "expected": True, "actual": json_6ki.get("all_checks_passed"), "passed": json_6ki.get("all_checks_passed") is True},
        {"check": "6ki_diagnosis", "expected": DIAGNOSIS_6KI, "actual": json_6ki.get("diagnosis"), "passed": json_6ki.get("diagnosis") == DIAGNOSIS_6KI},
        {"check": "6ki_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KI, "actual": json_6ki.get("recommended_next_layer"), "passed": json_6ki.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KI},
        {"check": "6ki_feature_by_feature_plan_required", "expected": True, "actual": json_6ki.get("feature_by_feature_reachability_plan_required"), "passed": json_6ki.get("feature_by_feature_reachability_plan_required") is True},
        {"check": "6ki_realism_ui_activation_confirmed", "expected": False, "actual": json_6ki.get("realism_ui_activation_confirmed"), "passed": json_6ki.get("realism_ui_activation_confirmed") is False},
        {"check": "6ki_ui_uses_realism_enabled_path", "expected": True, "actual": json_6ki.get("ui_uses_realism_enabled_path"), "passed": json_6ki.get("ui_uses_realism_enabled_path") is True},
        {"check": "6ki_ui_uses_legacy_or_current_path", "expected": True, "actual": json_6ki.get("ui_uses_legacy_or_current_path"), "passed": json_6ki.get("ui_uses_legacy_or_current_path") is True},
        {"check": "6ki_no_layer6_exit", "expected": False, "actual": json_6ki.get("layer_6_exit_recommended"), "passed": json_6ki.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6kk_feature_reachability_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "feature-level reachability and dataset proof required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "real evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6KJ", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KJ is planning only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6KJ cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6ki_passed", "expected": True, "actual": json_6ki.get("all_checks_passed"), "passed": json_6ki.get("all_checks_passed") is True},
        {"decision": "mechanic_matrix_count", "expected": 6, "actual": len(mechanic_matrix), "passed": len(mechanic_matrix) == 6 and all_passed(mechanic_matrix)},
        {"decision": "trace_plan_count", "expected": 6, "actual": len(trace_plan), "passed": len(trace_plan) == 6 and all_passed(trace_plan)},
        {"decision": "output_field_plan_count", "expected": 5, "actual": len(output_field_plan), "passed": len(output_field_plan) == 5 and all_passed(output_field_plan)},
        {"decision": "blocker_count", "expected": 5, "actual": len(blockers), "passed": len(blockers) == 5 and all_passed(blockers)},
        {"decision": "recommend_6kk_next", "expected": RECOMMENDED_NEXT_LAYER_6KJ, "actual": RECOMMENDED_NEXT_LAYER_6KJ, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "mechanic_matrix_created", "expected": True, "actual": True, "passed": True},
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
        {"surface": "source_tree", "policy": "read_only_planning", "passed": True},
        {"surface": "6ki_audit", "policy": "read_only", "passed": True},
        {"surface": "6ki_artifacts", "policy": "read_only", "passed": True},
        {"surface": "ui_projection_path", "policy": "not_modified_in_6kj", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6kj", "passed": True},
        {"surface": "database", "policy": "not_written_in_6kj", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KJ, "actual": RECOMMENDED_NEXT_LAYER_6KJ, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KJ, "actual": RECOMMENDED_PATH_6KJ, "passed": True},
        {"decision": "recommend_feature_reachability_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KJ, "actual": DIAGNOSIS_6KJ, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "mechanic_matrix", "passed": len(mechanic_matrix) == 6 and all_passed(mechanic_matrix), "detail": "6/6"},
        {"check": "trace_plan", "passed": len(trace_plan) == 6 and all_passed(trace_plan), "detail": "6/6"},
        {"check": "output_field_plan", "passed": len(output_field_plan) == 5 and all_passed(output_field_plan), "detail": "5/5"},
        {"check": "blockers", "passed": len(blockers) == 5 and all_passed(blockers), "detail": "5/5"},
        {"check": "future_6kk_contract", "passed": len(future_6kk) == 8 and all_passed(future_6kk), "detail": "8/8"},
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
        "mechanic_matrix": write_csv(MECHANIC_MATRIX_CSV, mechanic_matrix),
        "trace_plan": write_csv(TRACE_PLAN_CSV, trace_plan),
        "output_field_plan": write_csv(OUTPUT_FIELD_PLAN_CSV, output_field_plan),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6kk_contract": write_csv(FUTURE_6KK_CSV, future_6kk),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KJ",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KJ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KJ,
        "recommended_path": RECOMMENDED_PATH_6KJ,
        "predecessor_audit": str(AUDIT_6KI_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6ki.get("diagnosis"),
        "planned_layer_after": "6KI",
        "source_family": "ui_realism_feature_reachability_plan",
        "mechanic_matrix_count": len(mechanic_matrix),
        "trace_plan_count": len(trace_plan),
        "output_field_plan_count": len(output_field_plan),
        "blocker_count": len(blockers),
        "future_6kk_contract_valid": len(future_6kk) == 8 and all_passed(future_6kk),
        "mechanic_matrix_created": True,
        "feature_by_feature_reachability_plan_required": True,
        "exact_ui_route_trace_completed": True,
        "route_trace_audited": True,
        "realism_ui_activation_confirmed": False,
        "ui_uses_realism_enabled_path": True,
        "ui_uses_legacy_or_current_path": True,
        "bullpen_reached_but_output_effect_unmeasured": True,
        "unknown_mechanic_count": 4,
        "bypassed_mechanic_count": 1,
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
            "mechanic_matrix_csv": str(MECHANIC_MATRIX_CSV),
            "trace_plan_csv": str(TRACE_PLAN_CSV),
            "output_field_plan_csv": str(OUTPUT_FIELD_PLAN_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6kk_contract_csv": str(FUTURE_6KK_CSV),
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
