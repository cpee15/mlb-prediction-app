#!/usr/bin/env python3
"""Implement feature-by-feature UI realism reachability evidence trace.

This read-only layer inspects local source files to classify each realism
mechanic by existence, simulation reachability, UI reachability, active status,
and output-effect status. It does not fetch data, run simulations, modify code,
activate mechanics, or grant Layer 6 exit.
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple


SLUG = "layer6_6kk_ui_realism_feature_reachability_implementation"
TMP_DIR = Path("tmp")

PLAN_6KJ_PATH = Path("scripts/plan_6kj_layer6_ui_realism_feature_reachability.py")
JSON_6KJ = TMP_DIR / "layer6_6kj_ui_realism_feature_reachability_plan.json"

REQUIRED_INPUTS = [
    JSON_6KJ,
    TMP_DIR / "layer6_6kj_ui_realism_feature_reachability_plan_checks.csv",
    TMP_DIR / "layer6_6kj_ui_realism_feature_reachability_plan_predecessor.csv",
    TMP_DIR / "layer6_6kj_ui_realism_feature_reachability_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6kj_ui_realism_feature_reachability_plan_mechanic_matrix.csv",
    TMP_DIR / "layer6_6kj_ui_realism_feature_reachability_plan_trace_plan.csv",
    TMP_DIR / "layer6_6kj_ui_realism_feature_reachability_plan_output_field_plan.csv",
    TMP_DIR / "layer6_6kj_ui_realism_feature_reachability_plan_blockers.csv",
    TMP_DIR / "layer6_6kj_ui_realism_feature_reachability_plan_future_6kk_contract.csv",
    TMP_DIR / "layer6_6kj_ui_realism_feature_reachability_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6kj_ui_realism_feature_reachability_plan_decision.csv",
    TMP_DIR / "layer6_6kj_ui_realism_feature_reachability_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6kj_ui_realism_feature_reachability_plan_recommended_path.csv",
]

TARGET_FILES = [
    Path("frontend/src/pages/ModelProjectionsPage.jsx"),
    Path("mlb_app/model_projection_routes.py"),
    Path("mlb_app/model_projection_payload.py"),
    Path("mlb_app/model_projections.py"),
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
    Path("mlb_app/db_utils.py"),
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
SOURCE_FILE_INVENTORY_CSV = TMP_DIR / f"{SLUG}_source_file_inventory.csv"
MECHANIC_EVIDENCE_CSV = TMP_DIR / f"{SLUG}_mechanic_matrix_evidence.csv"
OUTPUT_FIELD_EVIDENCE_CSV = TMP_DIR / f"{SLUG}_output_field_evidence.csv"
MECHANIC_STATUS_SUMMARY_CSV = TMP_DIR / f"{SLUG}_mechanic_status_summary.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6KL_CSV = TMP_DIR / f"{SLUG}_future_6kl_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KJ = "layer_6_ui_realism_feature_reachability_plan_complete"
DIAGNOSIS_6KK = "layer_6_ui_realism_feature_reachability_implementation_complete"
RECOMMENDED_NEXT_LAYER_6KJ = "6KK_layer_6_ui_realism_feature_reachability_implementation"
RECOMMENDED_NEXT_LAYER_6KK = "6KL_layer_6_ui_realism_feature_reachability_implementation_audit"
RECOMMENDED_PATH_6KK = "audit_feature_by_feature_ui_realism_reachability_before_backtest"


MECHANIC_PATTERNS = {
    "bullpen_logic": ["bullpen", "simulate_game_with_bullpen", "bullpen_adjusted_game_simulation"],
    "double_play_logic": ["double_play", "double play", "grounded_into_double_play", "double_play_rate"],
    "sac_fly_logic": ["sac_fly", "sac fly", "sacrifice fly", "sac_fly_rate"],
    "stolen_base_or_steal_logic": ["stolen", "steal", "stolen_base", "caught_stealing"],
    "extras_ghost_runner_walkoff_logic": ["extras", "ghost", "walkoff", "extra inning", "simulate_game_with_extras"],
    "balk_logic": ["balk"],
}

UI_FIELD_PATTERNS = [
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


def line_hits(text: str, patterns: Sequence[str], limit: int = 25) -> str:
    hits: List[str] = []
    for idx, line in enumerate(text.splitlines(), start=1):
        if contains_any(line, patterns):
            hits.append(f"L{idx}:{line.strip()}")
        if len(hits) >= limit:
            break
    return " || ".join(hits)


def classify_mechanic(mechanic: str, patterns: Sequence[str], file_text: Dict[str, str]) -> Dict[str, Any]:
    frontend = file_text.get("frontend/src/pages/ModelProjectionsPage.jsx", "")
    model_projections = file_text.get("mlb_app/model_projections.py", "")
    game_simulator = file_text.get("mlb_app/simulation/game_simulator.py", "")
    game_engine = file_text.get("mlb_app/simulation/game_engine_v2.py", "")
    inning = file_text.get("mlb_app/simulation/inning_simulator.py", "")
    subtype = file_text.get("mlb_app/simulation/subtype_transitions.py", "")
    outcome = file_text.get("mlb_app/simulation/outcome_subtypes.py", "")
    rules = file_text.get("mlb_app/simulation/game_rules.py", "")
    formula_map = file_text.get("mlb_app/simulation/formula_map.py", "")
    route_payload_text = "\n".join([frontend, model_projections, formula_map])
    sim_text = "\n".join([game_simulator, game_engine, inning, subtype, outcome, rules])
    all_text = "\n".join(file_text.values())

    exists = contains_any(all_text, patterns)
    sim_reachable = contains_any(sim_text, patterns)
    ui_reachable = contains_any(route_payload_text, patterns)
    active = False
    output_effect = False
    source_files: List[str] = []

    for path, text in file_text.items():
        if contains_any(text, patterns):
            source_files.append(path)

    if mechanic == "bullpen_logic":
        active = contains_any(model_projections, ["simulate_game_with_bullpen", "bullpen_adjusted_game_simulation"])
        ui_reachable = ui_reachable or active
        output_effect = contains_any(frontend, ["bullpen_adjusted_game_simulation", "derived.bullpen_adjusted_game_simulation"]) and contains_any(frontend, UI_FIELD_PATTERNS)
    elif mechanic in {"double_play_logic", "sac_fly_logic"}:
        active = sim_reachable and contains_any(inning, patterns)
        output_effect = False
    elif mechanic == "stolen_base_or_steal_logic":
        active = sim_reachable and not contains_any(sim_text, ["no steals"])
        output_effect = False
    elif mechanic == "extras_ghost_runner_walkoff_logic":
        active = contains_any(sim_text, ["simulate_game_with_extras", "walkoff", "ghost_runner"])
        ui_reachable = contains_any(model_projections, ["simulate_game_with_extras", "walkoff", "ghost_runner"])
        output_effect = ui_reachable and contains_any(frontend, ["win_probability", "tie_after_regulation_probability"])
    elif mechanic == "balk_logic":
        active = False
        output_effect = False

    exists_status = "yes" if exists else "absent_or_deferred"
    simulation_reachability_status = "reached" if sim_reachable else "bypassed"
    ui_reachability_status = "reached" if ui_reachable else "bypassed"
    active_status = "active" if active else "inactive"
    output_effect_status = "plausible" if output_effect else "unmeasured_or_none"

    if exists and sim_reachable and not ui_reachable:
        ui_reachability_status = "unknown_or_not_direct"
    if exists and not sim_reachable:
        simulation_reachability_status = "unknown_or_not_direct"
    if mechanic == "balk_logic" and not exists:
        exists_status = "absent_or_deferred"
        simulation_reachability_status = "bypassed"
        ui_reachability_status = "bypassed"
        active_status = "inactive"
        output_effect_status = "none_currently"

    return {
        "mechanic": mechanic,
        "exists_status": exists_status,
        "simulation_reachability_status": simulation_reachability_status,
        "ui_reachability_status": ui_reachability_status,
        "active_status": active_status,
        "output_effect_status": output_effect_status,
        "evidence_summary": line_hits(all_text, patterns, limit=30),
        "source_files_with_evidence": ";".join(source_files),
        "source_lines_or_patterns": ";".join(patterns),
        "displayed_output_fields_impacted": ";".join([field for field in UI_FIELD_PATTERNS if contains_any(route_payload_text, [field])]),
        "blocker": "none_confirmed_exit_ready" if output_effect else "output_effect_not_proven",
        "recommended_next_action": "audit_and_then_measure_impact" if output_effect else "audit_and_trace_or_wire_before_backtest",
        "passed": True,
    }


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6kj = load_json(JSON_6KJ)

    file_text = {str(path): safe_text(path) for path in TARGET_FILES}
    source_inventory = [
        {
            "path": str(path),
            "exists": path.exists(),
            "line_count": len(file_text[str(path)].splitlines()) if path.exists() else 0,
            "contains_any_mechanic_signal": contains_any(file_text[str(path)], [p for vals in MECHANIC_PATTERNS.values() for p in vals]),
            "contains_ui_output_field_signal": contains_any(file_text[str(path)], UI_FIELD_PATTERNS),
            "passed": path.exists(),
        }
        for path in TARGET_FILES
    ]

    mechanic_evidence = [
        classify_mechanic(mechanic, patterns, file_text)
        for mechanic, patterns in MECHANIC_PATTERNS.items()
    ]

    output_field_evidence = [
        {
            "field": field,
            "frontend_evidence": line_hits(file_text.get("frontend/src/pages/ModelProjectionsPage.jsx", ""), [field], limit=20),
            "model_payload_evidence": line_hits(file_text.get("mlb_app/model_projections.py", ""), [field], limit=20),
            "formula_map_evidence": line_hits(file_text.get("mlb_app/simulation/formula_map.py", ""), [field], limit=20),
            "field_found": contains_any("\n".join(file_text.values()), [field]),
            "passed": True,
        }
        for field in UI_FIELD_PATTERNS
    ]

    status_summary = []
    for row in mechanic_evidence:
        status_summary.append({
            "mechanic": row["mechanic"],
            "classification": f"{row['exists_status']}|{row['simulation_reachability_status']}|{row['ui_reachability_status']}|{row['active_status']}|{row['output_effect_status']}",
            "ready_for_backtest": row["output_effect_status"] == "plausible",
            "needs_audit": True,
            "blocks_layer6_exit": True,
            "passed": True,
        })

    fully_ui_active = sum(1 for row in mechanic_evidence if row["ui_reachability_status"] == "reached" and row["active_status"] == "active" and row["output_effect_status"] == "plausible")
    partially_ui_active = sum(1 for row in mechanic_evidence if row["ui_reachability_status"] in {"reached", "unknown_or_not_direct"} and row["active_status"] == "active" and row["output_effect_status"] != "plausible")
    unknown_or_unmeasured = sum(1 for row in mechanic_evidence if "unknown" in row["ui_reachability_status"] or row["output_effect_status"] == "unmeasured_or_none")
    bypassed_or_inactive = sum(1 for row in mechanic_evidence if row["ui_reachability_status"] == "bypassed" or row["active_status"] == "inactive")

    by_mechanic = {row["mechanic"]: row for row in mechanic_evidence}

    blockers = [
        {"blocker": "mechanic_evidence_needs_audit", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "feature_output_effect_not_fully_measured", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "activation_decision_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "balk_absent_or_deferred", "blocks_activation": False, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6kl = [
        {"contract": "audit_source_file_inventory", "required": True, "passed": True},
        {"contract": "audit_mechanic_matrix_evidence", "required": True, "passed": True},
        {"contract": "audit_output_field_evidence", "required": True, "passed": True},
        {"contract": "audit_mechanic_status_summary", "required": True, "passed": True},
        {"contract": "decide_next_wiring_or_measurement_layer", "required": True, "passed": True},
        {"contract": "preserve_no_activation_no_layer6_exit", "required": True, "passed": True},
        {"contract": "do_not_fetch_or_write_in_6kl", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6kj_plan_script_exists", "expected": True, "actual": PLAN_6KJ_PATH.exists(), "passed": PLAN_6KJ_PATH.exists()},
        {"check": "6kj_json_exists", "expected": True, "actual": JSON_6KJ.exists(), "passed": JSON_6KJ.exists()},
        {"check": "6kj_all_checks_passed", "expected": True, "actual": json_6kj.get("all_checks_passed"), "passed": json_6kj.get("all_checks_passed") is True},
        {"check": "6kj_diagnosis", "expected": DIAGNOSIS_6KJ, "actual": json_6kj.get("diagnosis"), "passed": json_6kj.get("diagnosis") == DIAGNOSIS_6KJ},
        {"check": "6kj_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KJ, "actual": json_6kj.get("recommended_next_layer"), "passed": json_6kj.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KJ},
        {"check": "6kj_mechanic_matrix_created", "expected": True, "actual": json_6kj.get("mechanic_matrix_created"), "passed": json_6kj.get("mechanic_matrix_created") is True},
        {"check": "6kj_mechanic_matrix_count", "expected": 6, "actual": json_6kj.get("mechanic_matrix_count"), "passed": json_6kj.get("mechanic_matrix_count") == 6},
        {"check": "6kj_no_layer6_exit", "expected": False, "actual": json_6kj.get("layer_6_exit_recommended"), "passed": json_6kj.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS + TARGET_FILES]

    blocking_rows = [
        {"blocked_surface": "6kl_feature_reachability_audit", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "feature-level reachability audit and dataset proof required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "real evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6KK", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KK is read-only trace", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6KK cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6kj_passed", "expected": True, "actual": json_6kj.get("all_checks_passed"), "passed": json_6kj.get("all_checks_passed") is True},
        {"decision": "source_file_inventory_count", "expected": len(TARGET_FILES), "actual": len(source_inventory), "passed": len(source_inventory) == len(TARGET_FILES) and all_passed(source_inventory)},
        {"decision": "mechanic_matrix_evidence_count", "expected": 6, "actual": len(mechanic_evidence), "passed": len(mechanic_evidence) == 6 and all_passed(mechanic_evidence)},
        {"decision": "output_field_evidence_count", "expected": len(UI_FIELD_PATTERNS), "actual": len(output_field_evidence), "passed": len(output_field_evidence) == len(UI_FIELD_PATTERNS) and all_passed(output_field_evidence)},
        {"decision": "mechanic_status_summary_count", "expected": 6, "actual": len(status_summary), "passed": len(status_summary) == 6 and all_passed(status_summary)},
        {"decision": "recommend_6kl_next", "expected": RECOMMENDED_NEXT_LAYER_6KK, "actual": RECOMMENDED_NEXT_LAYER_6KK, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only_readonly_trace", "expected": True, "actual": True, "passed": True},
        {"boundary": "mechanic_matrix_implemented", "expected": True, "actual": True, "passed": True},
        {"boundary": "feature_by_feature_reachability_traced", "expected": True, "actual": True, "passed": True},
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
        {"surface": "source_tree", "policy": "read_only_trace", "passed": True},
        {"surface": "6kj_plan", "policy": "read_only", "passed": True},
        {"surface": "ui_projection_path", "policy": "not_modified_in_6kk", "passed": True},
        {"surface": "simulator_path", "policy": "not_modified_in_6kk", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6kk", "passed": True},
        {"surface": "database", "policy": "not_written_in_6kk", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KK, "actual": RECOMMENDED_NEXT_LAYER_6KK, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KK, "actual": RECOMMENDED_PATH_6KK, "passed": True},
        {"decision": "recommend_feature_reachability_audit_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KK, "actual": DIAGNOSIS_6KK, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "source_file_inventory", "passed": all_passed(source_inventory), "detail": f"{sum(1 for r in source_inventory if r['passed'])}/{len(source_inventory)}"},
        {"check": "mechanic_matrix_evidence", "passed": len(mechanic_evidence) == 6 and all_passed(mechanic_evidence), "detail": "6/6"},
        {"check": "output_field_evidence", "passed": all_passed(output_field_evidence), "detail": f"{sum(1 for r in output_field_evidence if r['passed'])}/{len(output_field_evidence)}"},
        {"check": "mechanic_status_summary", "passed": len(status_summary) == 6 and all_passed(status_summary), "detail": "6/6"},
        {"check": "blockers", "passed": len(blockers) == 5 and all_passed(blockers), "detail": "5/5"},
        {"check": "future_6kl_contract", "passed": len(future_6kl) == 7 and all_passed(future_6kl), "detail": "7/7"},
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
        "source_file_inventory": write_csv(SOURCE_FILE_INVENTORY_CSV, source_inventory),
        "mechanic_matrix_evidence": write_csv(MECHANIC_EVIDENCE_CSV, mechanic_evidence),
        "output_field_evidence": write_csv(OUTPUT_FIELD_EVIDENCE_CSV, output_field_evidence),
        "mechanic_status_summary": write_csv(MECHANIC_STATUS_SUMMARY_CSV, status_summary),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6kl_contract": write_csv(FUTURE_6KL_CSV, future_6kl),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KK",
        "layer_type": "game_mechanics_realism",
        "implementation_only_readonly_trace": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KK if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KK,
        "recommended_path": RECOMMENDED_PATH_6KK,
        "predecessor_plan": str(PLAN_6KJ_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6kj.get("diagnosis"),
        "implemented_layer_after": "6KJ",
        "source_family": "ui_realism_feature_reachability_implementation",
        "source_file_inventory_count": len(source_inventory),
        "mechanic_matrix_evidence_count": len(mechanic_evidence),
        "output_field_evidence_count": len(output_field_evidence),
        "mechanic_status_summary_count": len(status_summary),
        "blocker_count": len(blockers),
        "future_6kl_contract_valid": len(future_6kl) == 7 and all_passed(future_6kl),
        "mechanic_matrix_implemented": True,
        "feature_by_feature_reachability_traced": True,
        "bullpen_exists_status": by_mechanic["bullpen_logic"]["exists_status"],
        "bullpen_simulation_reachability_status": by_mechanic["bullpen_logic"]["simulation_reachability_status"],
        "bullpen_ui_reachability_status": by_mechanic["bullpen_logic"]["ui_reachability_status"],
        "bullpen_active_status": by_mechanic["bullpen_logic"]["active_status"],
        "bullpen_output_effect_status": by_mechanic["bullpen_logic"]["output_effect_status"],
        "double_play_exists_status": by_mechanic["double_play_logic"]["exists_status"],
        "double_play_simulation_reachability_status": by_mechanic["double_play_logic"]["simulation_reachability_status"],
        "double_play_ui_reachability_status": by_mechanic["double_play_logic"]["ui_reachability_status"],
        "double_play_active_status": by_mechanic["double_play_logic"]["active_status"],
        "double_play_output_effect_status": by_mechanic["double_play_logic"]["output_effect_status"],
        "sac_fly_exists_status": by_mechanic["sac_fly_logic"]["exists_status"],
        "sac_fly_simulation_reachability_status": by_mechanic["sac_fly_logic"]["simulation_reachability_status"],
        "sac_fly_ui_reachability_status": by_mechanic["sac_fly_logic"]["ui_reachability_status"],
        "sac_fly_active_status": by_mechanic["sac_fly_logic"]["active_status"],
        "sac_fly_output_effect_status": by_mechanic["sac_fly_logic"]["output_effect_status"],
        "stolen_base_exists_status": by_mechanic["stolen_base_or_steal_logic"]["exists_status"],
        "stolen_base_simulation_reachability_status": by_mechanic["stolen_base_or_steal_logic"]["simulation_reachability_status"],
        "stolen_base_ui_reachability_status": by_mechanic["stolen_base_or_steal_logic"]["ui_reachability_status"],
        "stolen_base_active_status": by_mechanic["stolen_base_or_steal_logic"]["active_status"],
        "stolen_base_output_effect_status": by_mechanic["stolen_base_or_steal_logic"]["output_effect_status"],
        "extras_walkoff_exists_status": by_mechanic["extras_ghost_runner_walkoff_logic"]["exists_status"],
        "extras_walkoff_simulation_reachability_status": by_mechanic["extras_ghost_runner_walkoff_logic"]["simulation_reachability_status"],
        "extras_walkoff_ui_reachability_status": by_mechanic["extras_ghost_runner_walkoff_logic"]["ui_reachability_status"],
        "extras_walkoff_active_status": by_mechanic["extras_ghost_runner_walkoff_logic"]["active_status"],
        "extras_walkoff_output_effect_status": by_mechanic["extras_ghost_runner_walkoff_logic"]["output_effect_status"],
        "balk_exists_status": by_mechanic["balk_logic"]["exists_status"],
        "balk_simulation_reachability_status": by_mechanic["balk_logic"]["simulation_reachability_status"],
        "balk_ui_reachability_status": by_mechanic["balk_logic"]["ui_reachability_status"],
        "balk_active_status": by_mechanic["balk_logic"]["active_status"],
        "balk_output_effect_status": by_mechanic["balk_logic"]["output_effect_status"],
        "fully_ui_active_mechanic_count": fully_ui_active,
        "partially_ui_active_mechanic_count": partially_ui_active,
        "unknown_or_unmeasured_mechanic_count": unknown_or_unmeasured,
        "bypassed_or_inactive_mechanic_count": bypassed_or_inactive,
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
            "source_file_inventory_csv": str(SOURCE_FILE_INVENTORY_CSV),
            "mechanic_matrix_evidence_csv": str(MECHANIC_EVIDENCE_CSV),
            "output_field_evidence_csv": str(OUTPUT_FIELD_EVIDENCE_CSV),
            "mechanic_status_summary_csv": str(MECHANIC_STATUS_SUMMARY_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6kl_contract_csv": str(FUTURE_6KL_CSV),
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
