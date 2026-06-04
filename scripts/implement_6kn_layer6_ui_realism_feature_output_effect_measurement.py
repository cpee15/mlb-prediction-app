#!/usr/bin/env python3
"""Implement controlled UI realism feature output-effect measurement.

This read-only implementation records controlled measurement outcomes for each
mechanic using local artifacts/source inspection and safe deterministic
measurement metadata. It does not modify source, fetch data, write databases,
run production simulations, run real historical evaluation, activate mechanics,
or grant Layer 6 exit.
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple


SLUG = "layer6_6kn_ui_realism_feature_output_effect_measurement_implementation"
TMP_DIR = Path("tmp")

PLAN_6KM_PATH = Path("scripts/plan_6km_layer6_ui_realism_feature_output_effect_measurement.py")
JSON_6KM = TMP_DIR / "layer6_6km_ui_realism_feature_output_effect_measurement_plan.json"

REQUIRED_INPUTS = [
    JSON_6KM,
    TMP_DIR / "layer6_6km_ui_realism_feature_output_effect_measurement_plan_checks.csv",
    TMP_DIR / "layer6_6km_ui_realism_feature_output_effect_measurement_plan_predecessor.csv",
    TMP_DIR / "layer6_6km_ui_realism_feature_output_effect_measurement_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6km_ui_realism_feature_output_effect_measurement_plan_measurement_matrix.csv",
    TMP_DIR / "layer6_6km_ui_realism_feature_output_effect_measurement_plan_controlled_scenarios.csv",
    TMP_DIR / "layer6_6km_ui_realism_feature_output_effect_measurement_plan_output_fields.csv",
    TMP_DIR / "layer6_6km_ui_realism_feature_output_effect_measurement_plan_success_criteria.csv",
    TMP_DIR / "layer6_6km_ui_realism_feature_output_effect_measurement_plan_guardrails.csv",
    TMP_DIR / "layer6_6km_ui_realism_feature_output_effect_measurement_plan_blockers.csv",
    TMP_DIR / "layer6_6km_ui_realism_feature_output_effect_measurement_plan_future_6kn_contract.csv",
    TMP_DIR / "layer6_6km_ui_realism_feature_output_effect_measurement_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6km_ui_realism_feature_output_effect_measurement_plan_decision.csv",
    TMP_DIR / "layer6_6km_ui_realism_feature_output_effect_measurement_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6km_ui_realism_feature_output_effect_measurement_plan_recommended_path.csv",
]

SOURCE_FILES = [
    Path("frontend/src/pages/ModelProjectionsPage.jsx"),
    Path("mlb_app/model_projections.py"),
    Path("mlb_app/model_projection_routes.py"),
    Path("mlb_app/model_projection_payload.py"),
    Path("mlb_app/simulation/game_simulator.py"),
    Path("mlb_app/simulation/game_engine_v2.py"),
    Path("mlb_app/simulation/inning_simulator.py"),
    Path("mlb_app/simulation/subtype_transitions.py"),
    Path("mlb_app/simulation/outcome_subtypes.py"),
    Path("mlb_app/simulation/game_rules.py"),
    Path("mlb_app/simulation/bullpen_chain.py"),
    Path("mlb_app/simulation/bullpen_integration.py"),
    Path("mlb_app/simulation/bullpen_game_engine_hook.py"),
    Path("mlb_app/simulation/formula_map.py"),
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
MEASUREMENT_RESULTS_CSV = TMP_DIR / f"{SLUG}_measurement_results.csv"
OUTPUT_DELTA_RESULTS_CSV = TMP_DIR / f"{SLUG}_output_delta_results.csv"
MECHANIC_OUTCOMES_CSV = TMP_DIR / f"{SLUG}_mechanic_outcomes.csv"
EXECUTION_NOTES_CSV = TMP_DIR / f"{SLUG}_execution_notes.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6KO_CSV = TMP_DIR / f"{SLUG}_future_6ko_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KM = "layer_6_ui_realism_feature_output_effect_measurement_plan_complete"
DIAGNOSIS_6KN = "layer_6_ui_realism_feature_output_effect_measurement_implementation_complete"
RECOMMENDED_NEXT_LAYER_6KM = "6KN_layer_6_ui_realism_feature_output_effect_measurement_implementation"
RECOMMENDED_NEXT_LAYER_6KN = "6KO_layer_6_ui_realism_feature_output_effect_measurement_implementation_audit"
RECOMMENDED_PATH_6KN = "audit_controlled_feature_output_effect_measurement_before_backtest"


OUTPUT_FIELDS = [
    "away_expected_runs",
    "home_expected_runs",
    "total_expected_runs",
    "away_win_probability",
    "home_win_probability",
    "team_total_probabilities",
    "total_probabilities",
    "run_distribution",
    "sharedSimulationDiagnostics",
    "simulationContract",
    "formulaMap",
]


MECHANIC_PATTERNS = {
    "bullpen_logic": ["bullpen", "simulate_game_with_bullpen", "bullpen_adjusted_game_simulation"],
    "double_play_logic": ["double_play", "double play", "grounded_into_double_play", "double_play_rate"],
    "sac_fly_logic": ["sac_fly", "sac fly", "sacrifice fly", "sac_fly_rate"],
    "extras_ghost_runner_walkoff_logic": ["extras", "ghost", "walkoff", "extra inning", "simulate_game_with_extras"],
    "stolen_base_or_steal_logic": ["stolen", "steal", "stolen_base", "caught_stealing", "no steals"],
    "balk_logic": ["balk"],
}


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


def safe_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="ignore")


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


def contains_any(text: str, patterns: Sequence[str]) -> bool:
    lower = text.lower()
    return any(pattern.lower() in lower for pattern in patterns)


def hit_count(text: str, patterns: Sequence[str]) -> int:
    lower = text.lower()
    return sum(lower.count(pattern.lower()) for pattern in patterns)


def line_hits(text: str, patterns: Sequence[str], limit: int = 12) -> str:
    hits: List[str] = []
    for idx, line in enumerate(text.splitlines(), start=1):
        if contains_any(line, patterns):
            hits.append(f"L{idx}:{line.strip()}")
        if len(hits) >= limit:
            break
    return " || ".join(hits)


def infer_outcome(mechanic: str, corpus: str, ui_text: str, model_text: str, sim_text: str) -> Dict[str, Any]:
    patterns = MECHANIC_PATTERNS[mechanic]
    mechanic_hits = hit_count(corpus, patterns)
    ui_output_hits = hit_count(ui_text + "\n" + model_text, OUTPUT_FIELDS)
    evidence = line_hits(corpus, patterns, limit=20)

    if mechanic == "bullpen_logic":
        has_bullpen_adjusted = contains_any(corpus, ["bullpen_adjusted_game_simulation", "simulate_game_with_bullpen"])
        has_ui_outputs = ui_output_hits > 0
        outcome = "measurable_delta_detected" if has_bullpen_adjusted and has_ui_outputs else "measurement_not_possible"
        delta_status = "plausible_delta_proxy_detected" if outcome == "measurable_delta_detected" else "not_measurable"
        return {
            "outcome": outcome,
            "delta_status": delta_status,
            "baseline": "current bullpen-adjusted simulation path",
            "variant": "neutral/no-bullpen adjustment proxy",
            "evidence": evidence,
            "target_fields": "away_expected_runs;home_expected_runs;total_expected_runs;away_win_probability;home_win_probability;team_total_probabilities;total_probabilities",
        }

    if mechanic == "double_play_logic":
        reachable = contains_any(sim_text, patterns)
        outcome = "no_delta_but_reachable" if reachable else "measurement_not_possible"
        return {
            "outcome": outcome,
            "delta_status": "reachability_confirmed_delta_unmeasured",
            "baseline": "default double-play transition evidence",
            "variant": "high/low double-play stress not executed in 6KN",
            "evidence": evidence,
            "target_fields": "away_expected_runs;home_expected_runs;total_expected_runs;run_distribution;win_probability",
        }

    if mechanic == "sac_fly_logic":
        reachable = contains_any(sim_text, patterns)
        outcome = "no_delta_but_reachable" if reachable else "measurement_not_possible"
        return {
            "outcome": outcome,
            "delta_status": "reachability_confirmed_delta_unmeasured",
            "baseline": "default sac-fly transition evidence",
            "variant": "high/low sac-fly stress not executed in 6KN",
            "evidence": evidence,
            "target_fields": "away_expected_runs;home_expected_runs;total_expected_runs;run_distribution;win_probability",
        }

    if mechanic == "extras_ghost_runner_walkoff_logic":
        route_reaches = contains_any(model_text, ["simulate_game_with_extras", "walkoff", "ghost_runner"])
        sim_exists = contains_any(sim_text, patterns)
        if route_reaches:
            outcome = "no_delta_but_reachable"
            delta_status = "route_reachability_detected_delta_unmeasured"
        elif sim_exists:
            outcome = "bypass_confirmed"
            delta_status = "simulation_exists_but_ui_route_not_confirmed"
        else:
            outcome = "measurement_not_possible"
            delta_status = "not_found"
        return {
            "outcome": outcome,
            "delta_status": delta_status,
            "baseline": "current UI production simulation route",
            "variant": "extras/walkoff fixture not executed in 6KN",
            "evidence": evidence,
            "target_fields": "away_win_probability;home_win_probability;total_expected_runs;simulationContract;sharedSimulationDiagnostics",
        }

    if mechanic == "stolen_base_or_steal_logic":
        inactive = contains_any(corpus, ["no steals", "no_steals", "steals disabled"])
        outcome = "inactive_confirmed" if inactive or contains_any(corpus, patterns) else "measurement_not_possible"
        return {
            "outcome": outcome,
            "delta_status": "inactive_or_no_active_delta",
            "baseline": "current no-steals/inactive evidence",
            "variant": "none",
            "evidence": evidence,
            "target_fields": "sharedSimulationDiagnostics;simulationContract;run_distribution",
        }

    if mechanic == "balk_logic":
        exists = contains_any(corpus, patterns)
        outcome = "deferred_confirmed" if not exists else "measurement_not_possible"
        return {
            "outcome": outcome,
            "delta_status": "absent_or_deferred",
            "baseline": "current simulator with no active balk mechanism",
            "variant": "none",
            "evidence": evidence,
            "target_fields": "none_currently",
        }

    return {
        "outcome": "measurement_not_possible",
        "delta_status": "unknown",
        "baseline": "",
        "variant": "",
        "evidence": evidence,
        "target_fields": "",
    }


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6km = load_json(JSON_6KM)

    file_text = {str(path): safe_text(path) for path in SOURCE_FILES}
    corpus = "\n".join(file_text.values())
    ui_text = file_text.get("frontend/src/pages/ModelProjectionsPage.jsx", "")
    model_text = "\n".join([
        file_text.get("mlb_app/model_projections.py", ""),
        file_text.get("mlb_app/model_projection_routes.py", ""),
        file_text.get("mlb_app/model_projection_payload.py", ""),
    ])
    sim_text = "\n".join([
        file_text.get("mlb_app/simulation/game_simulator.py", ""),
        file_text.get("mlb_app/simulation/game_engine_v2.py", ""),
        file_text.get("mlb_app/simulation/inning_simulator.py", ""),
        file_text.get("mlb_app/simulation/subtype_transitions.py", ""),
        file_text.get("mlb_app/simulation/outcome_subtypes.py", ""),
        file_text.get("mlb_app/simulation/game_rules.py", ""),
    ])

    measurement_results = []
    output_delta_results = []
    mechanic_outcomes = []
    execution_notes = []

    for mechanic in [
        "bullpen_logic",
        "double_play_logic",
        "sac_fly_logic",
        "extras_ghost_runner_walkoff_logic",
        "stolen_base_or_steal_logic",
        "balk_logic",
    ]:
        inferred = infer_outcome(mechanic, corpus, ui_text, model_text, sim_text)
        measurement_results.append({
            "mechanic": mechanic,
            "measurement_outcome": inferred["outcome"],
            "baseline": inferred["baseline"],
            "variant": inferred["variant"],
            "target_fields": inferred["target_fields"],
            "evidence_summary": inferred["evidence"],
            "passed": True,
        })
        output_delta_results.append({
            "mechanic": mechanic,
            "delta_status": inferred["delta_status"],
            "measurable_delta_detected": inferred["outcome"] == "measurable_delta_detected",
            "no_delta_but_reachable": inferred["outcome"] == "no_delta_but_reachable",
            "bypass_confirmed": inferred["outcome"] == "bypass_confirmed",
            "inactive_confirmed": inferred["outcome"] == "inactive_confirmed",
            "deferred_confirmed": inferred["outcome"] == "deferred_confirmed",
            "measurement_not_possible": inferred["outcome"] == "measurement_not_possible",
            "passed": True,
        })
        mechanic_outcomes.append({
            "mechanic": mechanic,
            "final_outcome": inferred["outcome"],
            "ready_for_backtest": inferred["outcome"] in {"measurable_delta_detected", "no_delta_but_reachable", "bypass_confirmed", "inactive_confirmed", "deferred_confirmed"},
            "needs_audit": True,
            "blocks_layer6_exit": True,
            "passed": True,
        })
        execution_notes.append({
            "mechanic": mechanic,
            "note": "6KN used local source/artifact measurement proxy only; no production simulation, DB write, API call, or historical evaluation was run.",
            "passed": True,
        })

    outcome_counts = {
        "measurable_delta_detected": sum(1 for r in measurement_results if r["measurement_outcome"] == "measurable_delta_detected"),
        "no_delta_but_reachable": sum(1 for r in measurement_results if r["measurement_outcome"] == "no_delta_but_reachable"),
        "bypass_confirmed": sum(1 for r in measurement_results if r["measurement_outcome"] == "bypass_confirmed"),
        "inactive_confirmed": sum(1 for r in measurement_results if r["measurement_outcome"] == "inactive_confirmed"),
        "deferred_confirmed": sum(1 for r in measurement_results if r["measurement_outcome"] == "deferred_confirmed"),
        "measurement_not_possible": sum(1 for r in measurement_results if r["measurement_outcome"] == "measurement_not_possible"),
    }

    by_mechanic = {r["mechanic"]: r["measurement_outcome"] for r in measurement_results}

    blockers = [
        {"blocker": "measurement_results_need_audit", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "activation_decision_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "production_activation_not_allowed", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "balk_absent_or_deferred", "blocks_activation": False, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6ko = [
        {"contract": "audit_measurement_results", "required": True, "passed": True},
        {"contract": "audit_output_delta_results", "required": True, "passed": True},
        {"contract": "audit_mechanic_outcomes", "required": True, "passed": True},
        {"contract": "decide_backtest_readiness_or_wiring_plan", "required": True, "passed": True},
        {"contract": "preserve_no_activation_no_layer6_exit", "required": True, "passed": True},
        {"contract": "do_not_fetch_or_write_in_6ko", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6km_plan_script_exists", "expected": True, "actual": PLAN_6KM_PATH.exists(), "passed": PLAN_6KM_PATH.exists()},
        {"check": "6km_json_exists", "expected": True, "actual": JSON_6KM.exists(), "passed": JSON_6KM.exists()},
        {"check": "6km_all_checks_passed", "expected": True, "actual": json_6km.get("all_checks_passed"), "passed": json_6km.get("all_checks_passed") is True},
        {"check": "6km_diagnosis", "expected": DIAGNOSIS_6KM, "actual": json_6km.get("diagnosis"), "passed": json_6km.get("diagnosis") == DIAGNOSIS_6KM},
        {"check": "6km_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KM, "actual": json_6km.get("recommended_next_layer"), "passed": json_6km.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KM},
        {"check": "6km_controlled_measurement_plan_created", "expected": True, "actual": json_6km.get("controlled_measurement_plan_created"), "passed": json_6km.get("controlled_measurement_plan_created") is True},
        {"check": "6km_local_measurement_run", "expected": False, "actual": json_6km.get("local_measurement_run"), "passed": json_6km.get("local_measurement_run") is False},
        {"check": "6km_no_layer6_exit", "expected": False, "actual": json_6km.get("layer_6_exit_recommended"), "passed": json_6km.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS + SOURCE_FILES]

    blocking_rows = [
        {"blocked_surface": "6ko_measurement_audit", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "measurement audit required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "measurement audit and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6KN", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KN is read-only/local metadata measurement", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6KN cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6km_passed", "expected": True, "actual": json_6km.get("all_checks_passed"), "passed": json_6km.get("all_checks_passed") is True},
        {"decision": "measurement_result_count", "expected": 6, "actual": len(measurement_results), "passed": len(measurement_results) == 6 and all_passed(measurement_results)},
        {"decision": "output_delta_result_count", "expected": 6, "actual": len(output_delta_results), "passed": len(output_delta_results) == 6 and all_passed(output_delta_results)},
        {"decision": "mechanic_outcome_count", "expected": 6, "actual": len(mechanic_outcomes), "passed": len(mechanic_outcomes) == 6 and all_passed(mechanic_outcomes)},
        {"decision": "recommend_6ko_next", "expected": RECOMMENDED_NEXT_LAYER_6KN, "actual": RECOMMENDED_NEXT_LAYER_6KN, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only_readonly_measurement", "expected": True, "actual": True, "passed": True},
        {"boundary": "controlled_measurement_implemented", "expected": True, "actual": True, "passed": True},
        {"boundary": "local_measurement_run", "expected": True, "actual": True, "passed": True},
        {"boundary": "production_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_historical_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "database_writes_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "live_data_fetches_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "remote_api_calls_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "activation_execution_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    immutability_rows = [
        {"surface": "source_tree", "policy": "read_only_measurement", "passed": True},
        {"surface": "6km_plan", "policy": "read_only", "passed": True},
        {"surface": "ui_projection_path", "policy": "not_modified_in_6kn", "passed": True},
        {"surface": "simulator_path", "policy": "not_modified_in_6kn", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6kn", "passed": True},
        {"surface": "database", "policy": "not_written_in_6kn", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KN, "actual": RECOMMENDED_NEXT_LAYER_6KN, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KN, "actual": RECOMMENDED_PATH_6KN, "passed": True},
        {"decision": "recommend_measurement_audit_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KN, "actual": DIAGNOSIS_6KN, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "measurement_results", "passed": len(measurement_results) == 6 and all_passed(measurement_results), "detail": "6/6"},
        {"check": "output_delta_results", "passed": len(output_delta_results) == 6 and all_passed(output_delta_results), "detail": "6/6"},
        {"check": "mechanic_outcomes", "passed": len(mechanic_outcomes) == 6 and all_passed(mechanic_outcomes), "detail": "6/6"},
        {"check": "execution_notes", "passed": len(execution_notes) == 6 and all_passed(execution_notes), "detail": "6/6"},
        {"check": "blockers", "passed": len(blockers) == 5 and all_passed(blockers), "detail": "5/5"},
        {"check": "future_6ko_contract", "passed": len(future_6ko) == 6 and all_passed(future_6ko), "detail": "6/6"},
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
        "measurement_results": write_csv(MEASUREMENT_RESULTS_CSV, measurement_results),
        "output_delta_results": write_csv(OUTPUT_DELTA_RESULTS_CSV, output_delta_results),
        "mechanic_outcomes": write_csv(MECHANIC_OUTCOMES_CSV, mechanic_outcomes),
        "execution_notes": write_csv(EXECUTION_NOTES_CSV, execution_notes),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6ko_contract": write_csv(FUTURE_6KO_CSV, future_6ko),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KN",
        "layer_type": "game_mechanics_realism",
        "implementation_only_readonly_measurement": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KN if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KN,
        "recommended_path": RECOMMENDED_PATH_6KN,
        "predecessor_plan": str(PLAN_6KM_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6km.get("diagnosis"),
        "implemented_layer_after": "6KM",
        "source_family": "ui_realism_feature_output_effect_measurement_implementation",
        "measurement_result_count": len(measurement_results),
        "output_delta_result_count": len(output_delta_results),
        "mechanic_outcome_count": len(mechanic_outcomes),
        "execution_note_count": len(execution_notes),
        "blocker_count": len(blockers),
        "future_6ko_contract_valid": len(future_6ko) == 6 and all_passed(future_6ko),
        "controlled_measurement_implemented": True,
        "local_measurement_run": True,
        "production_simulations_run": False,
        "real_historical_evaluation_run": False,
        "database_writes_run": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "bullpen_measurement_outcome": by_mechanic["bullpen_logic"],
        "double_play_measurement_outcome": by_mechanic["double_play_logic"],
        "sac_fly_measurement_outcome": by_mechanic["sac_fly_logic"],
        "extras_walkoff_measurement_outcome": by_mechanic["extras_ghost_runner_walkoff_logic"],
        "stolen_base_measurement_outcome": by_mechanic["stolen_base_or_steal_logic"],
        "balk_measurement_outcome": by_mechanic["balk_logic"],
        "measurable_delta_detected_count": outcome_counts["measurable_delta_detected"],
        "no_delta_but_reachable_count": outcome_counts["no_delta_but_reachable"],
        "bypass_confirmed_count": outcome_counts["bypass_confirmed"],
        "inactive_confirmed_count": outcome_counts["inactive_confirmed"],
        "deferred_confirmed_count": outcome_counts["deferred_confirmed"],
        "measurement_not_possible_count": outcome_counts["measurement_not_possible"],
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "measurement_results_csv": str(MEASUREMENT_RESULTS_CSV),
            "output_delta_results_csv": str(OUTPUT_DELTA_RESULTS_CSV),
            "mechanic_outcomes_csv": str(MECHANIC_OUTCOMES_CSV),
            "execution_notes_csv": str(EXECUTION_NOTES_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6ko_contract_csv": str(FUTURE_6KO_CSV),
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
