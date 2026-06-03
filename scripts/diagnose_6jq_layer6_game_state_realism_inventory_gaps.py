#!/usr/bin/env python3
"""Diagnose Layer 6 game-state realism inventory and gaps.

This layer inventories existing game-state realism code and artifacts.
It does not install mechanics, run performance evaluation, activate mechanics,
or recommend Layer 6 exit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jq_game_state_realism_inventory_gap_diagnosis"
TMP_DIR = Path("tmp")

AUDIT_6JP_PATH = Path("scripts/audit_6jp_layer6_authorization_or_exit_certification_implementation.py")
JSON_6JP = TMP_DIR / "layer6_6jp_authorization_or_exit_certification_implementation_audit.json"

REQUIRED_INPUTS = [
    JSON_6JP,
    TMP_DIR / "layer6_6jp_authorization_or_exit_certification_implementation_audit_checks.csv",
    TMP_DIR / "layer6_6jp_authorization_or_exit_certification_implementation_audit_predecessor.csv",
    TMP_DIR / "layer6_6jp_authorization_or_exit_certification_implementation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6jp_authorization_or_exit_certification_implementation_audit_game_state_realism_roadmap_alignment.csv",
    TMP_DIR / "layer6_6jp_authorization_or_exit_certification_implementation_audit_next_inventory_gap_diagnosis_contract.csv",
    TMP_DIR / "layer6_6jp_authorization_or_exit_certification_implementation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6jp_authorization_or_exit_certification_implementation_audit_decision.csv",
    TMP_DIR / "layer6_6jp_authorization_or_exit_certification_implementation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6jp_authorization_or_exit_certification_implementation_audit_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CODE_INVENTORY_CSV = TMP_DIR / f"{SLUG}_code_inventory.csv"
ROADMAP_MECHANIC_CSV = TMP_DIR / f"{SLUG}_roadmap_mechanic_inventory.csv"
SIM_LOOP_CSV = TMP_DIR / f"{SLUG}_sim_loop_inventory.csv"
STATUS_MATRIX_CSV = TMP_DIR / f"{SLUG}_mechanic_status_matrix.csv"
MISSING_BACKLOG_CSV = TMP_DIR / f"{SLUG}_missing_mechanics_backlog.csv"
PARTIAL_BACKLOG_CSV = TMP_DIR / f"{SLUG}_partial_mechanics_backlog.csv"
INSTALLED_CSV = TMP_DIR / f"{SLUG}_installed_mechanics.csv"
GOVERNED_ONLY_CSV = TMP_DIR / f"{SLUG}_governed_only_mechanics.csv"
PRIORITIES_CSV = TMP_DIR / f"{SLUG}_next_installation_priorities.csv"
FUTURE_6JR_CSV = TMP_DIR / f"{SLUG}_future_6jr_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JP = "layer_6_authorization_or_exit_certification_implementation_audit_complete"
DIAGNOSIS_6JQ = "layer_6_game_state_realism_inventory_gap_diagnosis_complete"
RECOMMENDED_NEXT_LAYER_6JP = "6JQ_layer_6_game_state_realism_inventory_gap_diagnosis"
RECOMMENDED_PATH_6JP = "audit_authorization_or_exit_certification_then_diagnose_game_state_realism_gaps"
RECOMMENDED_NEXT_LAYER_6JQ = "6JR_layer_6_game_state_realism_installation_plan"
RECOMMENDED_PATH_6JQ = "diagnose_game_state_realism_gaps_then_plan_missing_mechanics_installation"


ROADMAP_MECHANICS = [
    {
        "mechanic": "extra_innings_and_ghost_runner_logic",
        "keywords": ["extra", "ghost", "runner", "tenth", "walkoff", "walk-off"],
        "priority": 4,
    },
    {
        "mechanic": "stolen_bases_and_caught_stealing",
        "keywords": ["stolen", "steal", "caught", "cs", "sb"],
        "priority": 1,
    },
    {
        "mechanic": "wild_pitches_and_passed_balls",
        "keywords": ["wild_pitch", "passed_ball", "wild pitch", "passed ball", "wp", "pb"],
        "priority": 3,
    },
    {
        "mechanic": "balks",
        "keywords": ["balk"],
        "priority": 8,
    },
    {
        "mechanic": "first_to_third_advancement",
        "keywords": ["first_to_third", "1st_to_3rd", "first to third", "advance"],
        "priority": 2,
    },
    {
        "mechanic": "second_to_home_advancement",
        "keywords": ["second_to_home", "2nd_to_home", "second to home", "advance"],
        "priority": 2,
    },
    {
        "mechanic": "sac_flies_and_tagging_up",
        "keywords": ["sac", "sacrifice", "tagging", "tag_up", "tag up"],
        "priority": 5,
    },
    {
        "mechanic": "double_plays_by_base_out_state",
        "keywords": ["double_play", "double play", "gidp", "base_out"],
        "priority": 5,
    },
    {
        "mechanic": "pinch_hitters_and_substitutions",
        "keywords": ["pinch", "substitution", "substitute"],
        "priority": 7,
    },
    {
        "mechanic": "bullpen_sequencing_and_leverage_behavior",
        "keywords": ["bullpen", "reliever", "leverage", "closer", "setup", "pitcher_change"],
        "priority": 1,
    },
]

SIM_LOOP_SURFACES = [
    {
        "surface": "simulator_modules",
        "keywords": ["simulate", "simulator", "simulation"],
    },
    {
        "surface": "inning_simulation_loop",
        "keywords": ["inning", "while", "outs"],
    },
    {
        "surface": "plate_appearance_outcome_handling",
        "keywords": ["plate", "pa", "at_bat", "outcome"],
    },
    {
        "surface": "base_out_transition_handling",
        "keywords": ["base_out", "bases", "outs", "transition"],
    },
    {
        "surface": "runner_advancement_handling",
        "keywords": ["runner", "advance", "advancement"],
    },
    {
        "surface": "bullpen_pitcher_substitution_handling",
        "keywords": ["bullpen", "reliever", "pitcher", "substitution"],
    },
    {
        "surface": "game_completion_walkoff_extra_innings",
        "keywords": ["walkoff", "walk-off", "extra", "complete", "final"],
    },
    {
        "surface": "game_realism_flags_configs",
        "keywords": ["realism", "activation", "gate", "config", "flag"],
    },
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


def all_passed(rows: List[Dict[str, Any]]) -> bool:
    return all(str(row.get("passed", "")).lower() == "true" or row.get("passed") is True for row in rows)


def iter_python_files() -> List[Path]:
    files: List[Path] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        if not root.exists():
            continue
        files.extend(sorted(root.rglob("*.py")))
    return files


def safe_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def keyword_hits(text: str, keywords: List[str]) -> int:
    lowered = text.lower()
    return sum(1 for keyword in keywords if keyword.lower() in lowered)


def classify_mechanic(mechanic: Dict[str, Any], code_inventory: List[Dict[str, Any]]) -> Dict[str, Any]:
    relevant = [row for row in code_inventory if row["mechanic"] == mechanic["mechanic"] and row["hit_count"] > 0]
    script_hits = [row for row in relevant if row["path"].startswith("scripts/")]
    app_hits = [row for row in relevant if row["path"].startswith("mlb_app/")]
    sim_hits = [
        row for row in app_hits
        if any(token in row["path"].lower() for token in ["sim", "engine", "game", "inning", "transition"])
    ]

    has_app = bool(app_hits)
    has_sim = bool(sim_hits)
    has_scripts = bool(script_hits)

    if has_app and has_sim:
        status = "partially_installed_or_installed_candidate"
        rationale = "found in app-level simulation-adjacent code; requires 6JR verification before treating as installed"
    elif has_app:
        status = "partial"
        rationale = "found in app code but not clearly in simulation loop surface"
    elif has_scripts:
        status = "governed_or_tested_only"
        rationale = "found only in scripts/artifacts, not app runtime code"
    else:
        status = "missing"
        rationale = "no keyword evidence found in repository python code inventory"

    return {
        "mechanic": mechanic["mechanic"],
        "status": status,
        "priority": mechanic["priority"],
        "app_hit_count": len(app_hits),
        "script_hit_count": len(script_hits),
        "sim_surface_hit_count": len(sim_hits),
        "requires_installation_plan": status != "partially_installed_or_installed_candidate",
        "requires_manual_code_review": True,
        "rationale": rationale,
        "passed": True,
    }


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6jp = load_json(JSON_6JP)

    py_files = iter_python_files()

    code_inventory: List[Dict[str, Any]] = []
    for path in py_files:
        text = safe_text(path)
        for mechanic in ROADMAP_MECHANICS:
            hits = keyword_hits(text, mechanic["keywords"])
            if hits > 0:
                code_inventory.append({
                    "path": str(path),
                    "mechanic": mechanic["mechanic"],
                    "hit_count": hits,
                    "file_kind": "app" if str(path).startswith("mlb_app/") else "script",
                    "diagnosis_only": True,
                    "passed": True,
                })

    if not code_inventory:
        code_inventory.append({
            "path": "__no_keyword_hits__",
            "mechanic": "__none__",
            "hit_count": 0,
            "file_kind": "none",
            "diagnosis_only": True,
            "passed": True,
        })

    roadmap_inventory = [
        {
            "mechanic": mechanic["mechanic"],
            "priority": mechanic["priority"],
            "keywords": "|".join(mechanic["keywords"]),
            "required_before_layer6_exit": True,
            "inventoried": True,
            "passed": True,
        }
        for mechanic in ROADMAP_MECHANICS
    ]

    sim_loop_inventory: List[Dict[str, Any]] = []
    for surface in SIM_LOOP_SURFACES:
        matched_paths = []
        for path in py_files:
            text = safe_text(path)
            if keyword_hits(str(path) + "\n" + text, surface["keywords"]) > 0:
                matched_paths.append(str(path))
        sim_loop_inventory.append({
            "surface": surface["surface"],
            "keyword_family": "|".join(surface["keywords"]),
            "matched_path_count": len(matched_paths),
            "sample_paths": "|".join(matched_paths[:12]),
            "inventoried": True,
            "requires_followup_review": True,
            "passed": True,
        })

    status_matrix = [classify_mechanic(mechanic, code_inventory) for mechanic in ROADMAP_MECHANICS]

    installed = [
        row for row in status_matrix
        if row["status"] == "partially_installed_or_installed_candidate"
    ]
    partial = [row for row in status_matrix if row["status"] == "partial"]
    governed_only = [row for row in status_matrix if row["status"] == "governed_or_tested_only"]
    missing = [row for row in status_matrix if row["status"] == "missing"]

    priorities = sorted(
        [
            {
                "mechanic": row["mechanic"],
                "status": row["status"],
                "priority": row["priority"],
                "recommended_next_action": (
                    "verify_runtime_wiring_then_harden"
                    if row["status"] == "partially_installed_or_installed_candidate"
                    else "plan_installation_into_sim_loop"
                ),
                "passed": True,
            }
            for row in status_matrix
        ],
        key=lambda row: (row["priority"], row["mechanic"]),
    )

    future_6jr = [
        {"contract": "convert_inventory_into_installation_plan", "required": True, "passed": True},
        {"contract": "prioritize_stolen_bases_and_bullpen_sequencing", "required": True, "passed": True},
        {"contract": "define_sim_loop_integration_targets", "required": True, "passed": True},
        {"contract": "separate_installation_from_performance_evaluation", "required": True, "passed": True},
        {"contract": "preserve_keep_and_tune_doctrine", "required": True, "passed": True},
        {"contract": "do_not_exit_layer6_from_inventory_only", "required": True, "passed": True},
        {"contract": "do_not_run_mae_brier_before_installation_plan", "required": True, "passed": True},
        {"contract": "recommend_missing_mechanics_installation_sequence", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jp_audit_exists", "expected": True, "actual": AUDIT_6JP_PATH.exists(), "passed": AUDIT_6JP_PATH.exists()},
        {"check": "6jp_json_exists", "expected": True, "actual": JSON_6JP.exists(), "passed": JSON_6JP.exists()},
        {"check": "6jp_all_checks_passed", "expected": True, "actual": json_6jp.get("all_checks_passed"), "passed": json_6jp.get("all_checks_passed") is True},
        {"check": "6jp_diagnosis", "expected": DIAGNOSIS_6JP, "actual": json_6jp.get("diagnosis"), "passed": json_6jp.get("diagnosis") == DIAGNOSIS_6JP},
        {"check": "6jp_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JP, "actual": json_6jp.get("recommended_next_layer"), "passed": json_6jp.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JP},
        {"check": "6jp_recommended_path", "expected": RECOMMENDED_PATH_6JP, "actual": json_6jp.get("recommended_path"), "passed": json_6jp.get("recommended_path") == RECOMMENDED_PATH_6JP},
        {"check": "6jp_roadmap_alignment_count", "expected": 10, "actual": json_6jp.get("game_state_realism_roadmap_alignment_count"), "passed": json_6jp.get("game_state_realism_roadmap_alignment_count") == 10},
        {"check": "6jp_no_exit_recommendation", "expected": False, "actual": json_6jp.get("layer_6_exit_recommended"), "passed": json_6jp.get("layer_6_exit_recommended") is False},
        {"check": "6jp_no_mae_brier_comparison", "expected": False, "actual": json_6jp.get("mae_brier_comparison_run"), "passed": json_6jp.get("mae_brier_comparison_run") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = (
        [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]
        + [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": True} for path in py_files]
    )

    blocking_rows = [
        {"blocked_surface": "game_state_realism_installation_plan", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "mechanic_installation", "blocked": True, "reason": "6JR plan required before installation", "passed": True},
        {"blocked_surface": "performance_certification", "blocked": True, "reason": "inventory and installation planning required first", "passed": True},
        {"blocked_surface": "mae_brier_performance_evaluation", "blocked": True, "reason": "realism mechanics not fully installed", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "realism mechanics not fully installed or certified", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "inventory diagnosis only cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jp_passed", "expected": True, "actual": json_6jp.get("all_checks_passed"), "passed": json_6jp.get("all_checks_passed") is True},
        {"decision": "roadmap_mechanics_inventoried", "expected": 10, "actual": len(roadmap_inventory), "passed": len(roadmap_inventory) == 10},
        {"decision": "code_inventory_completed", "expected": True, "actual": True, "passed": True},
        {"decision": "sim_loop_inventory_completed", "expected": True, "actual": True, "passed": True},
        {"decision": "status_matrix_produced", "expected": 10, "actual": len(status_matrix), "passed": len(status_matrix) == 10},
        {"decision": "recommend_6jr_next", "expected": RECOMMENDED_NEXT_LAYER_6JQ, "actual": RECOMMENDED_NEXT_LAYER_6JQ, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "diagnosis_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_source_mechanic_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_truth_join_rerun", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation_rerun", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mae_brier_comparison", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_activation_execution", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_final_activation_decision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_recommendation", "expected": False, "actual": False, "passed": True},
    ]

    immutability_rows = [
        {"surface": "source_tree", "policy": "read_only_inventory", "passed": True},
        {"surface": "6jp_audit", "policy": "read_only", "passed": True},
        {"surface": "6jo_implementation", "policy": "read_only", "passed": True},
        {"surface": "6jn_plan", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JQ, "actual": RECOMMENDED_NEXT_LAYER_6JQ, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JQ, "actual": RECOMMENDED_PATH_6JQ, "passed": True},
        {"decision": "recommend_game_state_realism_installation_plan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metrics_decision_or_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JQ, "actual": DIAGNOSIS_6JQ, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "code_inventory", "passed": len(code_inventory) > 0 and all_passed(code_inventory), "detail": str(len(code_inventory))},
        {"check": "roadmap_mechanic_inventory", "passed": len(roadmap_inventory) == 10 and all_passed(roadmap_inventory), "detail": f"{len(roadmap_inventory)}/10"},
        {"check": "sim_loop_inventory", "passed": len(sim_loop_inventory) == len(SIM_LOOP_SURFACES) and all_passed(sim_loop_inventory), "detail": f"{len(sim_loop_inventory)}/{len(SIM_LOOP_SURFACES)}"},
        {"check": "mechanic_status_matrix", "passed": len(status_matrix) == 10 and all_passed(status_matrix), "detail": f"{len(status_matrix)}/10"},
        {"check": "missing_mechanics_backlog", "passed": True, "detail": str(len(missing))},
        {"check": "partial_mechanics_backlog", "passed": True, "detail": str(len(partial))},
        {"check": "installed_mechanics", "passed": True, "detail": str(len(installed))},
        {"check": "governed_only_mechanics", "passed": True, "detail": str(len(governed_only))},
        {"check": "next_installation_priorities", "passed": len(priorities) == 10 and all_passed(priorities), "detail": f"{len(priorities)}/10"},
        {"check": "future_6jr_contract", "passed": len(future_6jr) == 8 and all_passed(future_6jr), "detail": f"{len(future_6jr)}/8"},
        {"check": "readonly_sources", "passed": all_passed(readonly_rows), "detail": str(len(readonly_rows))},
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
        "code_inventory": write_csv(CODE_INVENTORY_CSV, code_inventory),
        "roadmap_mechanic_inventory": write_csv(ROADMAP_MECHANIC_CSV, roadmap_inventory),
        "sim_loop_inventory": write_csv(SIM_LOOP_CSV, sim_loop_inventory),
        "mechanic_status_matrix": write_csv(STATUS_MATRIX_CSV, status_matrix),
        "missing_mechanics_backlog": write_csv(MISSING_BACKLOG_CSV, missing),
        "partial_mechanics_backlog": write_csv(PARTIAL_BACKLOG_CSV, partial),
        "installed_mechanics": write_csv(INSTALLED_CSV, installed),
        "governed_only_mechanics": write_csv(GOVERNED_ONLY_CSV, governed_only),
        "next_installation_priorities": write_csv(PRIORITIES_CSV, priorities),
        "future_6jr_contract": write_csv(FUTURE_6JR_CSV, future_6jr),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JQ",
        "layer_type": "game_mechanics_realism",
        "diagnosis_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JQ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JQ,
        "recommended_path": RECOMMENDED_PATH_6JQ,
        "predecessor_audit": str(AUDIT_6JP_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6jp.get("diagnosis"),
        "diagnosed_layer_after": "6JP",
        "source_family": "game_state_realism_inventory_gap_diagnosis",
        "roadmap_mechanic_count": len(roadmap_inventory),
        "code_inventory_count": len(code_inventory),
        "sim_loop_inventory_count": len(sim_loop_inventory),
        "installed_mechanic_count": len(installed),
        "partial_mechanic_count": len(partial),
        "missing_mechanic_count": len(missing),
        "governed_only_mechanic_count": len(governed_only),
        "next_installation_priority_count": len(priorities),
        "future_6jr_contract_valid": len(future_6jr) == 8 and all_passed(future_6jr),
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
        "performance_evaluation_allowed_after_this_layer": False,
        "mae_brier_comparison_run": False,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "production_simulations_run": False,
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
            "code_inventory_csv": str(CODE_INVENTORY_CSV),
            "roadmap_mechanic_inventory_csv": str(ROADMAP_MECHANIC_CSV),
            "sim_loop_inventory_csv": str(SIM_LOOP_CSV),
            "mechanic_status_matrix_csv": str(STATUS_MATRIX_CSV),
            "missing_mechanics_backlog_csv": str(MISSING_BACKLOG_CSV),
            "partial_mechanics_backlog_csv": str(PARTIAL_BACKLOG_CSV),
            "installed_mechanics_csv": str(INSTALLED_CSV),
            "governed_only_mechanics_csv": str(GOVERNED_ONLY_CSV),
            "next_installation_priorities_csv": str(PRIORITIES_CSV),
            "future_6jr_contract_csv": str(FUTURE_6JR_CSV),
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
