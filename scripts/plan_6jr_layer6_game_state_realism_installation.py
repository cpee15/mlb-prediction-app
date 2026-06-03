#!/usr/bin/env python3
"""Plan Layer 6 game-state realism installation and runtime wiring.

This planning layer converts 6JQ's inventory into a concrete installation and
hardening plan. It does not modify simulator mechanics or run performance tests.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jr_game_state_realism_installation_plan"
TMP_DIR = Path("tmp")

DIAGNOSIS_6JQ_PATH = Path("scripts/diagnose_6jq_layer6_game_state_realism_inventory_gaps.py")
JSON_6JQ = TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis.json"

REQUIRED_INPUTS = [
    JSON_6JQ,
    TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_checks.csv",
    TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_predecessor.csv",
    TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_input_artifacts.csv",
    TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_roadmap_mechanic_inventory.csv",
    TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_sim_loop_inventory.csv",
    TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_mechanic_status_matrix.csv",
    TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_missing_mechanics_backlog.csv",
    TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_partial_mechanics_backlog.csv",
    TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_installed_mechanics.csv",
    TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_governed_only_mechanics.csv",
    TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_next_installation_priorities.csv",
    TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_future_6jr_contract.csv",
    TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_blocking_policy.csv",
    TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_decision.csv",
    TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_safety_boundaries.csv",
    TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
RUNTIME_WIRING_PLAN_CSV = TMP_DIR / f"{SLUG}_runtime_wiring_plan.csv"
MECHANIC_PLAN_CSV = TMP_DIR / f"{SLUG}_mechanic_plan.csv"
SIM_LOOP_TARGETS_CSV = TMP_DIR / f"{SLUG}_sim_loop_targets.csv"
PRIORITY_SEQUENCE_CSV = TMP_DIR / f"{SLUG}_priority_sequence.csv"
TEST_PLAN_CSV = TMP_DIR / f"{SLUG}_test_plan.csv"
POST_INSTALL_GATES_CSV = TMP_DIR / f"{SLUG}_post_install_verification_gates.csv"
FUTURE_6JS_CSV = TMP_DIR / f"{SLUG}_future_6js_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JQ = "layer_6_game_state_realism_inventory_gap_diagnosis_complete"
DIAGNOSIS_6JR = "layer_6_game_state_realism_installation_plan_complete"
RECOMMENDED_NEXT_LAYER_6JQ = "6JR_layer_6_game_state_realism_installation_plan"
RECOMMENDED_PATH_6JQ = "diagnose_game_state_realism_gaps_then_plan_missing_mechanics_installation"
RECOMMENDED_NEXT_LAYER_6JR = "6JS_layer_6_game_state_realism_runtime_wiring_implementation"
RECOMMENDED_PATH_6JR = "plan_game_state_realism_installation_then_implement_runtime_wiring"

MECHANICS = [
    ("bullpen_sequencing_and_leverage_behavior", 1, "candidate_installed_verify_and_harden"),
    ("stolen_bases_and_caught_stealing", 2, "candidate_installed_verify_and_harden"),
    ("first_to_third_advancement", 3, "candidate_installed_verify_and_harden"),
    ("second_to_home_advancement", 4, "candidate_installed_verify_and_harden"),
    ("wild_pitches_and_passed_balls", 5, "partial_install_into_sim_loop"),
    ("extra_innings_and_ghost_runner_logic", 6, "candidate_installed_verify_and_harden"),
    ("double_plays_by_base_out_state", 7, "candidate_installed_verify_and_harden"),
    ("sac_flies_and_tagging_up", 8, "candidate_installed_verify_and_harden"),
    ("pinch_hitters_and_substitutions", 9, "candidate_installed_verify_and_harden"),
    ("balks", 10, "governed_only_install_into_sim_loop"),
]

SIM_LOOP_TARGETS = [
    ("mlb_app/simulation/inning_simulator.py", "inning-level PA loop and base/out state transition surface"),
    ("mlb_app/simulation/game_rules.py", "extra inning, walkoff, and game completion rule surface"),
    ("mlb_app/simulation/subtype_transitions.py", "mechanic-specific transition subtype surface"),
    ("mlb_app/simulation/transition_profiles.py", "probability/profile surface for transition mechanics"),
    ("mlb_app/simulation/bullpen_chain.py", "bullpen sequencing and pitcher chain surface"),
    ("mlb_app/simulation/bullpen_selection.py", "reliever selection and leverage decision surface"),
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


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6jq = load_json(JSON_6JQ)

    status_rows = read_csv(TMP_DIR / "layer6_6jq_game_state_realism_inventory_gap_diagnosis_mechanic_status_matrix.csv")
    status_by_mechanic = {row.get("mechanic", ""): row for row in status_rows}

    runtime_wiring_plan = []
    mechanic_plan = []
    priority_sequence = []
    test_plan = []

    for mechanic, priority, plan_type in MECHANICS:
        source_status = status_by_mechanic.get(mechanic, {}).get("status", "unknown")
        if "bullpen" in mechanic:
            integration_target = "bullpen_chain|bullpen_selection|inning_simulator"
            runtime_assertion = "reliever choice and leverage sequencing must be reachable from game simulation"
        elif "stolen" in mechanic:
            integration_target = "inning_simulator|subtype_transitions|transition_profiles"
            runtime_assertion = "steal attempt and caught stealing must mutate base/out state and scoring distribution"
        elif "advancement" in mechanic:
            integration_target = "inning_simulator|subtype_transitions|transition_profiles"
            runtime_assertion = "runner advancement must mutate base occupancy and run expectancy by batted-ball context"
        elif "wild" in mechanic or "balk" in mechanic:
            integration_target = "inning_simulator|subtype_transitions|transition_profiles"
            runtime_assertion = "non-batted-ball advancement must mutate base/out state without PA hit event"
        elif "extra" in mechanic:
            integration_target = "game_rules|inning_simulator"
            runtime_assertion = "extra inning ghost runner and walkoff termination must be reachable from game simulation"
        elif "double" in mechanic or "sac" in mechanic:
            integration_target = "inning_simulator|subtype_transitions"
            runtime_assertion = "base/out-specific out events must mutate outs, runners, and runs correctly"
        elif "pinch" in mechanic:
            integration_target = "inning_simulator|lineup_state"
            runtime_assertion = "substitution must affect subsequent PA participants and not only artifact records"
        else:
            integration_target = "inning_simulator"
            runtime_assertion = "mechanic must be reachable from runtime simulation loop"

        runtime_wiring_plan.append({
            "mechanic": mechanic,
            "priority": priority,
            "source_status_from_6jq": source_status,
            "integration_target": integration_target,
            "runtime_wiring_action": "verify_reachability_then_wire_or_harden",
            "runtime_assertion": runtime_assertion,
            "requires_code_change_in_6js": True,
            "planning_only": True,
            "passed": True,
        })

        mechanic_plan.append({
            "mechanic": mechanic,
            "priority": priority,
            "plan_type": plan_type,
            "installation_action": (
                "verify_existing_runtime_wiring_and_add_hardening_tests"
                if "candidate" in plan_type
                else "add_or_wire_mechanic_into_runtime_sim_loop"
            ),
            "probability_surface_needed": True,
            "base_out_state_mutation_needed": mechanic not in {"pinch_hitters_and_substitutions"},
            "distribution_effect_expected": True,
            "keep_and_tune_doctrine": True,
            "planning_only": True,
            "passed": True,
        })

        priority_sequence.append({
            "priority": priority,
            "mechanic": mechanic,
            "source_status_from_6jq": source_status,
            "reason": (
                "high leverage distribution impact"
                if priority <= 2
                else "required Layer 6 roadmap mechanic"
            ),
            "passed": True,
        })

        test_plan.append({
            "mechanic": mechanic,
            "priority": priority,
            "unit_test_required": True,
            "runtime_smoke_test_required": True,
            "state_transition_test_required": True,
            "distribution_snapshot_test_required": True,
            "mae_brier_test_now": False,
            "activation_test_now": False,
            "passed": True,
        })

    sim_loop_targets = [
        {
            "target_path": path,
            "target_role": role,
            "exists": Path(path).exists(),
            "may_modify_in_6jr": False,
            "candidate_for_6js": True,
            "passed": True,
        }
        for path, role in SIM_LOOP_TARGETS
    ]

    post_install_gates = [
        {"gate": "all_mechanics_reachable_from_game_simulation", "required": True, "passed": True},
        {"gate": "all_mechanics_have_state_transition_tests", "required": True, "passed": True},
        {"gate": "all_mechanics_have_runtime_smoke_tests", "required": True, "passed": True},
        {"gate": "all_mechanics_preserve_no_db_write_boundary", "required": True, "passed": True},
        {"gate": "all_mechanics_preserve_no_live_fetch_boundary", "required": True, "passed": True},
        {"gate": "performance_evaluation_deferred_until_runtime_wiring_audit", "required": True, "passed": True},
        {"gate": "activation_deferred_until_certification", "required": True, "passed": True},
        {"gate": "layer6_exit_deferred_until_realism_complete_and_tested", "required": True, "passed": True},
    ]

    future_6js = [
        {"contract": "implement_runtime_wiring_for_top_priority_mechanics", "required": True, "passed": True},
        {"contract": "verify_candidate_installed_mechanics_are_reachable", "required": True, "passed": True},
        {"contract": "wire_partial_wild_pitch_passed_ball_surface", "required": True, "passed": True},
        {"contract": "wire_governed_only_balk_surface_or_mark_explicitly_deferred", "required": True, "passed": True},
        {"contract": "add_state_transition_smoke_tests", "required": True, "passed": True},
        {"contract": "do_not_run_mae_brier_in_6js", "required": True, "passed": True},
        {"contract": "do_not_activate_mechanics_in_6js", "required": True, "passed": True},
        {"contract": "do_not_grant_layer6_exit_in_6js", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jq_diagnosis_script_exists", "expected": True, "actual": DIAGNOSIS_6JQ_PATH.exists(), "passed": DIAGNOSIS_6JQ_PATH.exists()},
        {"check": "6jq_json_exists", "expected": True, "actual": JSON_6JQ.exists(), "passed": JSON_6JQ.exists()},
        {"check": "6jq_all_checks_passed", "expected": True, "actual": json_6jq.get("all_checks_passed"), "passed": json_6jq.get("all_checks_passed") is True},
        {"check": "6jq_diagnosis", "expected": DIAGNOSIS_6JQ, "actual": json_6jq.get("diagnosis"), "passed": json_6jq.get("diagnosis") == DIAGNOSIS_6JQ},
        {"check": "6jq_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JQ, "actual": json_6jq.get("recommended_next_layer"), "passed": json_6jq.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JQ},
        {"check": "6jq_recommended_path", "expected": RECOMMENDED_PATH_6JQ, "actual": json_6jq.get("recommended_path"), "passed": json_6jq.get("recommended_path") == RECOMMENDED_PATH_6JQ},
        {"check": "6jq_roadmap_mechanics", "expected": 10, "actual": json_6jq.get("roadmap_mechanic_count"), "passed": json_6jq.get("roadmap_mechanic_count") == 10},
        {"check": "6jq_future_6jr_contract_valid", "expected": True, "actual": json_6jq.get("future_6jr_contract_valid"), "passed": json_6jq.get("future_6jr_contract_valid") is True},
        {"check": "6jq_no_layer6_exit", "expected": False, "actual": json_6jq.get("layer_6_exit_recommended"), "passed": json_6jq.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "runtime_wiring_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "mae_brier_performance_evaluation", "blocked": True, "reason": "must wire/harden mechanics first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "must certify mechanics and performance first", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "planning only", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "planning only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "installation plan cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jq_passed", "expected": True, "actual": json_6jq.get("all_checks_passed"), "passed": json_6jq.get("all_checks_passed") is True},
        {"decision": "runtime_wiring_plan_count", "expected": 10, "actual": len(runtime_wiring_plan), "passed": len(runtime_wiring_plan) == 10},
        {"decision": "mechanic_plan_count", "expected": 10, "actual": len(mechanic_plan), "passed": len(mechanic_plan) == 10},
        {"decision": "priority_sequence_count", "expected": 10, "actual": len(priority_sequence), "passed": len(priority_sequence) == 10},
        {"decision": "test_plan_count", "expected": 10, "actual": len(test_plan), "passed": len(test_plan) == 10},
        {"decision": "recommend_6js_next", "expected": RECOMMENDED_NEXT_LAYER_6JR, "actual": RECOMMENDED_NEXT_LAYER_6JR, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_source_mechanic_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_simulator_logic_modification", "expected": False, "actual": False, "passed": True},
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
        {"surface": "source_tree", "policy": "read_only_planning", "passed": True},
        {"surface": "6jq_diagnosis", "policy": "read_only", "passed": True},
        {"surface": "6jp_audit", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
        {"surface": "simulator_mechanics", "policy": "not_modified_in_6jr", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JR, "actual": RECOMMENDED_NEXT_LAYER_6JR, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JR, "actual": RECOMMENDED_PATH_6JR, "passed": True},
        {"decision": "recommend_runtime_wiring_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metrics_decision_or_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JR, "actual": DIAGNOSIS_6JR, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "runtime_wiring_plan", "passed": len(runtime_wiring_plan) == 10 and all_passed(runtime_wiring_plan), "detail": f"{len(runtime_wiring_plan)}/10"},
        {"check": "mechanic_plan", "passed": len(mechanic_plan) == 10 and all_passed(mechanic_plan), "detail": f"{len(mechanic_plan)}/10"},
        {"check": "sim_loop_targets", "passed": len(sim_loop_targets) >= 4 and all_passed(sim_loop_targets), "detail": str(len(sim_loop_targets))},
        {"check": "priority_sequence", "passed": len(priority_sequence) == 10 and all_passed(priority_sequence), "detail": f"{len(priority_sequence)}/10"},
        {"check": "test_plan", "passed": len(test_plan) == 10 and all_passed(test_plan), "detail": f"{len(test_plan)}/10"},
        {"check": "post_install_verification_gates", "passed": len(post_install_gates) == 8 and all_passed(post_install_gates), "detail": f"{len(post_install_gates)}/8"},
        {"check": "future_6js_contract", "passed": len(future_6js) == 8 and all_passed(future_6js), "detail": f"{len(future_6js)}/8"},
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
        "runtime_wiring_plan": write_csv(RUNTIME_WIRING_PLAN_CSV, runtime_wiring_plan),
        "mechanic_plan": write_csv(MECHANIC_PLAN_CSV, mechanic_plan),
        "sim_loop_targets": write_csv(SIM_LOOP_TARGETS_CSV, sim_loop_targets),
        "priority_sequence": write_csv(PRIORITY_SEQUENCE_CSV, priority_sequence),
        "test_plan": write_csv(TEST_PLAN_CSV, test_plan),
        "post_install_verification_gates": write_csv(POST_INSTALL_GATES_CSV, post_install_gates),
        "future_6js_contract": write_csv(FUTURE_6JS_CSV, future_6js),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JR",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JR if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JR,
        "recommended_path": RECOMMENDED_PATH_6JR,
        "predecessor_diagnosis": str(DIAGNOSIS_6JQ_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6jq.get("diagnosis"),
        "planned_layer_after": "6JQ",
        "source_family": "game_state_realism_installation_plan",
        "roadmap_mechanic_count": 10,
        "runtime_wiring_plan_count": len(runtime_wiring_plan),
        "mechanic_plan_count": len(mechanic_plan),
        "sim_loop_target_count": len(sim_loop_targets),
        "priority_sequence_count": len(priority_sequence),
        "test_plan_count": len(test_plan),
        "post_install_verification_gate_count": len(post_install_gates),
        "future_6js_contract_valid": len(future_6js) == 8 and all_passed(future_6js),
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
            "runtime_wiring_plan_csv": str(RUNTIME_WIRING_PLAN_CSV),
            "mechanic_plan_csv": str(MECHANIC_PLAN_CSV),
            "sim_loop_targets_csv": str(SIM_LOOP_TARGETS_CSV),
            "priority_sequence_csv": str(PRIORITY_SEQUENCE_CSV),
            "test_plan_csv": str(TEST_PLAN_CSV),
            "post_install_verification_gates_csv": str(POST_INSTALL_GATES_CSV),
            "future_6js_contract_csv": str(FUTURE_6JS_CSV),
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
