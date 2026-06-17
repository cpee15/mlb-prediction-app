#!/usr/bin/env python3
"""Layer 6NT — Model Projection Realism Injection Map Plan.

Planning-only layer.

Purpose:
Define the feature-by-feature evidence map needed to confirm whether Layer 6
realism mechanics flow into the simulator path that feeds Model Projections UI
output.

No wiring, tuning, historical validation, joins, backtests, pricing, or edge
detection are performed here.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


LAYER_ID = "6NT"
LAYER_NAME = "layer6_model_projection_realism_injection_map_plan"
SLUG = "layer_6NT_model_projection_realism_injection_map_plan"

TMP_DIR = Path("tmp") / SLUG
TMP_DIR.mkdir(parents=True, exist_ok=True)

PREDECESSOR_SCRIPT = Path("scripts/audit_6NS_prediction_artifact_discovery_plan.py")

JSON_PATH = TMP_DIR / "diagnosis.json"
CHECKS_CSV = TMP_DIR / "checks.csv"
PREDECESSOR_CSV = TMP_DIR / "predecessor.csv"
FEATURE_SCOPE_CSV = TMP_DIR / "realism_feature_scope.csv"
INJECTION_PATH_CSV = TMP_DIR / "injection_path_contract.csv"
BASE_ADVANCEMENT_CSV = TMP_DIR / "base_advancement_transition_scope.csv"
PITCHER_USAGE_CSV = TMP_DIR / "opener_bulk_pitcher_scope.csv"
EVALUATION_PLAN_CSV = TMP_DIR / "future_evaluation_plan.csv"
BLOCKERS_CSV = TMP_DIR / "blockers.csv"
SAFETY_CSV = TMP_DIR / "safety_boundaries.csv"
DECISION_CSV = TMP_DIR / "decision.csv"
RECOMMENDED_CSV = TMP_DIR / "recommended_path.csv"

RECOMMENDED_NEXT_LAYER = "6NU_layer6_model_projection_realism_injection_map_audit"
RECOMMENDED_PATH = "audit_realism_injection_map_plan_before_inventory_or_wiring"

FEATURES = [
    {
        "feature_family": "bullpen_transition",
        "required_question": "Does starter-to-bullpen transition feed Model Projections diagnostic simulation output?",
        "expected_state_mutation": "pitcher context changes from starter probabilities to bullpen probabilities",
        "target_sim_component": "simulate_game_with_bullpen",
        "target_projection_component": "model_projections._build_projection_simulation_cards",
        "target_ui_surface": "Model Projections simulation cards/workspace",
        "status_to_determine_next": "confirm_active_and_payload_visible",
    },
    {
        "feature_family": "dynamic_starter_exit",
        "required_question": "Does probabilistic starter exit affect innings, run distributions, and UI diagnostic output?",
        "expected_state_mutation": "starter exit inning varies by distribution/context",
        "target_sim_component": "simulate_game_with_bullpen(dynamic_starter_exit=True)",
        "target_projection_component": "model_projections._build_projection_simulation_cards",
        "target_ui_surface": "starter innings distribution / simulation diagnostics",
        "status_to_determine_next": "confirm_active_and_payload_visible",
    },
    {
        "feature_family": "opener_bulk_pitcher",
        "required_question": "Can opener and bulk pitcher usage be represented as probabilistic pitcher-role transitions?",
        "expected_state_mutation": "opener handles early batters/innings, bulk pitcher enters with probabilistic length",
        "target_sim_component": "future pitcher role transition engine",
        "target_projection_component": "future Model Projections pitcher-role simulation context",
        "target_ui_surface": "pitcher role diagnostics / expected runs / distribution output",
        "status_to_determine_next": "map_existing_or_missing",
    },
    {
        "feature_family": "individual_reliever_selection",
        "required_question": "Does the sim choose individual relievers or only aggregate bullpen probabilities?",
        "expected_state_mutation": "reliever identity changes PA probabilities and inning context",
        "target_sim_component": "bullpen chain / bullpen integration",
        "target_projection_component": "Model Projections simulation workspace",
        "target_ui_surface": "bullpen adjusted simulation output",
        "status_to_determine_next": "map_existing_or_missing",
    },
    {
        "feature_family": "leverage_bullpen_usage",
        "required_question": "Does game state influence which bullpen quality bucket or reliever is used?",
        "expected_state_mutation": "score/inning/leverage affects pitcher choice",
        "target_sim_component": "bullpen leverage logic",
        "target_projection_component": "Model Projections simulation workspace",
        "target_ui_surface": "bullpen adjusted simulation output",
        "status_to_determine_next": "map_existing_or_missing",
    },
    {
        "feature_family": "base_out_state",
        "required_question": "Does the sim explicitly track bases occupied and outs across plate appearances?",
        "expected_state_mutation": "base occupancy and outs update after each PA",
        "target_sim_component": "base/out transition engine",
        "target_projection_component": "Model Projections game simulation output",
        "target_ui_surface": "run distribution / inning distribution / diagnostics",
        "status_to_determine_next": "map_existing_or_missing",
    },
    {
        "feature_family": "base_advancement_transitions",
        "required_question": "Are runner advancements probabilistic by event/context rather than deterministic aggregate scoring?",
        "expected_state_mutation": "runners advance, hold, score, or are thrown out based on transition probabilities",
        "target_sim_component": "runner advancement transition engine",
        "target_projection_component": "Model Projections game simulation output",
        "target_ui_surface": "run distribution / scoring tails / diagnostics",
        "status_to_determine_next": "map_existing_or_missing",
    },
    {
        "feature_family": "double_play_logic",
        "required_question": "Does ground-ball/base/out context create probabilistic double-play outcomes?",
        "expected_state_mutation": "outs and bases update through double play / force / fielder choice alternatives",
        "target_sim_component": "double play transition logic",
        "target_projection_component": "Model Projections game simulation output",
        "target_ui_surface": "run distribution / inning run distribution",
        "status_to_determine_next": "map_existing_or_missing",
    },
    {
        "feature_family": "sac_fly_logic",
        "required_question": "Does fly-ball/base/out context create probabilistic tag-up scoring?",
        "expected_state_mutation": "runner on third scores, holds, or is out based on context",
        "target_sim_component": "sac fly transition logic",
        "target_projection_component": "Model Projections game simulation output",
        "target_ui_surface": "run distribution / inning run distribution",
        "status_to_determine_next": "map_existing_or_missing",
    },
    {
        "feature_family": "extra_innings_ghost_runner",
        "required_question": "Are extra innings represented with ghost runner state and correct continuation rules?",
        "expected_state_mutation": "10th+ inning begins with runner on second when applicable",
        "target_sim_component": "extras / ghost runner engine",
        "target_projection_component": "Model Projections game simulation output",
        "target_ui_surface": "win probability / total run distribution / extras diagnostics",
        "status_to_determine_next": "map_existing_or_missing",
    },
    {
        "feature_family": "walkoff_shortening",
        "required_question": "Does home team stop batting once it leads in final/extra inning walkoff states?",
        "expected_state_mutation": "bottom inning terminates immediately once home team leads",
        "target_sim_component": "walkoff rule engine",
        "target_projection_component": "Model Projections game simulation output",
        "target_ui_surface": "home win probability / total run distribution",
        "status_to_determine_next": "map_existing_or_missing",
    },
    {
        "feature_family": "steals_caught_stealing",
        "required_question": "Are steal attempts and success modeled probabilistically by base/out/game context?",
        "expected_state_mutation": "runner advances or out is added before/within PA sequence",
        "target_sim_component": "steal transition engine",
        "target_projection_component": "Model Projections game simulation output",
        "target_ui_surface": "run distribution / diagnostics",
        "status_to_determine_next": "map_existing_or_missing",
    },
    {
        "feature_family": "balks",
        "required_question": "Are balks represented as rare probabilistic advancement events?",
        "expected_state_mutation": "eligible runners advance one base",
        "target_sim_component": "balk transition engine",
        "target_projection_component": "Model Projections game simulation output",
        "target_ui_surface": "run distribution / diagnostics",
        "status_to_determine_next": "map_existing_or_missing",
    },
    {
        "feature_family": "wild_pitch_passed_ball",
        "required_question": "Are wild pitches and passed balls represented as probabilistic runner advancement events?",
        "expected_state_mutation": "runners advance or score based on base state",
        "target_sim_component": "wild pitch / passed ball transition engine",
        "target_projection_component": "Model Projections game simulation output",
        "target_ui_surface": "run distribution / diagnostics",
        "status_to_determine_next": "map_existing_or_missing",
    },
    {
        "feature_family": "lineup_order_state",
        "required_question": "Does batting order persist across innings rather than resetting or using team aggregates only?",
        "expected_state_mutation": "next batter index advances across innings",
        "target_sim_component": "lineup order state engine",
        "target_projection_component": "Model Projections game simulation output",
        "target_ui_surface": "run distribution / matchup diagnostics",
        "status_to_determine_next": "map_existing_or_missing",
    },
]

BASE_ADVANCEMENT_TRANSITIONS = [
    {"event": "single", "start_state": "runner_on_1st", "possible_outcomes": "1st_to_2nd;1st_to_3rd;1st_out;other", "probabilistic_required": True},
    {"event": "single", "start_state": "runner_on_2nd", "possible_outcomes": "2nd_to_3rd;2nd_scores;2nd_out;other", "probabilistic_required": True},
    {"event": "single", "start_state": "runner_on_3rd", "possible_outcomes": "3rd_scores;3rd_holds;3rd_out;other", "probabilistic_required": True},
    {"event": "double", "start_state": "runner_on_1st", "possible_outcomes": "1st_to_3rd;1st_scores;1st_out;other", "probabilistic_required": True},
    {"event": "double", "start_state": "runner_on_2nd", "possible_outcomes": "2nd_scores;2nd_to_3rd;2nd_out;other", "probabilistic_required": True},
    {"event": "fly_ball_out", "start_state": "runner_on_3rd_less_than_2_outs", "possible_outcomes": "tag_and_score;hold;out_at_home", "probabilistic_required": True},
    {"event": "ground_ball", "start_state": "runner_on_1st_less_than_2_outs", "possible_outcomes": "double_play;force_out_only;fielder_choice;runner_advances;infield_hit", "probabilistic_required": True},
    {"event": "wild_pitch_or_passed_ball", "start_state": "runners_on_base", "possible_outcomes": "advance_one_base;hold;score_from_3rd;out", "probabilistic_required": True},
]

OPENER_BULK_SCOPE = [
    {"component": "opener_detection", "required_behavior": "identify opener game context or probable opener role", "probabilistic_required": True},
    {"component": "opener_length_distribution", "required_behavior": "sample opener batters/innings rather than fixed one-inning rule", "probabilistic_required": True},
    {"component": "bulk_pitcher_entry", "required_behavior": "transition from opener to bulk pitcher at sampled point", "probabilistic_required": True},
    {"component": "bulk_pitcher_length_distribution", "required_behavior": "sample bulk pitcher length and handoff to bullpen", "probabilistic_required": True},
    {"component": "ui_payload_visibility", "required_behavior": "show pitcher role assumptions/diagnostics in Model Projections output", "probabilistic_required": False},
]


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True}]
    fields: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    predecessor_rows = [
        {
            "check": "6ns_audit_script_exists",
            "expected": True,
            "actual": PREDECESSOR_SCRIPT.exists(),
            "passed": PREDECESSOR_SCRIPT.exists(),
        }
    ]

    injection_rows = [
        {
            "path_step": 1,
            "name": "realism_feature_logic",
            "required_evidence": "feature implementation exists and has clear entry point",
            "example": "base advancement transition function or opener/bulk pitcher role handler",
        },
        {
            "path_step": 2,
            "name": "simulation_state_mutation",
            "required_evidence": "feature mutates game state or pitcher context during simulation",
            "example": "bases/outs/runs/pitcher role change",
        },
        {
            "path_step": 3,
            "name": "game_simulation_output",
            "required_evidence": "feature affects simulated run/win/distribution output",
            "example": "expected runs, team totals, total probabilities, run distribution",
        },
        {
            "path_step": 4,
            "name": "model_projection_builder",
            "required_evidence": "Model Projections builder consumes the realism-enabled sim output",
            "example": "model_projections._build_projection_simulation_cards",
        },
        {
            "path_step": 5,
            "name": "api_payload_or_workspace",
            "required_evidence": "payload contains feature-aware output or diagnostics",
            "example": "bullpenAdjustedGameSimulation, simulationDiagnostics, simulationContract",
        },
        {
            "path_step": 6,
            "name": "model_projections_ui_display",
            "required_evidence": "UI displays or exposes the feature-aware output",
            "example": "Model Projections cards/workspace diagnostics",
        },
    ]

    evaluation_rows = [
        {
            "future_phase": "inventory",
            "description": "inspect source files and classify each feature as absent, present, sim-reachable, projection-reachable, ui-visible, or active",
            "allowed_now": False,
        },
        {
            "future_phase": "wiring",
            "description": "wire missing realism features into the common simulation path after audit approves the map",
            "allowed_now": False,
        },
        {
            "future_phase": "historical_validation",
            "description": "evaluate full realism-enabled simulator against historical actuals after injection is complete",
            "allowed_now": False,
        },
        {
            "future_phase": "tuning",
            "description": "tune transition probabilities only after full engine output is measurable",
            "allowed_now": False,
        },
    ]

    blockers = [
        {"blocker": "feature_injection_inventory_not_yet_executed", "active": True},
        {"blocker": "all_realism_features_not_confirmed_ui_visible", "active": True},
        {"blocker": "historical_validation_deferred_until_full_injection_map_audited", "active": True},
        {"blocker": "tuning_deferred_until_full_engine_output_measurable", "active": True},
        {"blocker": "layer6_exit_not_allowed", "active": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "passed": True},
        {"boundary": "no_feature_wiring", "passed": True},
        {"boundary": "no_tuning", "passed": True},
        {"boundary": "no_historical_validation", "passed": True},
        {"boundary": "no_prediction_join", "passed": True},
        {"boundary": "no_accuracy_metrics", "passed": True},
        {"boundary": "no_backtests", "passed": True},
        {"boundary": "no_pricing", "passed": True},
        {"boundary": "no_edge_detection", "passed": True},
        {"boundary": "no_live_fetches_or_remote_apis", "passed": True},
        {"boundary": "no_production_writes", "passed": True},
    ]

    decision_rows = [
        {"decision": "feature_scope_defined", "expected": True, "actual": len(FEATURES) >= 15, "passed": len(FEATURES) >= 15},
        {"decision": "opener_bulk_pitcher_included", "expected": True, "actual": any(row["feature_family"] == "opener_bulk_pitcher" for row in FEATURES), "passed": True},
        {"decision": "base_advancement_transitions_included", "expected": True, "actual": any(row["feature_family"] == "base_advancement_transitions" for row in FEATURES), "passed": True},
        {"decision": "base_transition_scope_specific", "expected": True, "actual": len(BASE_ADVANCEMENT_TRANSITIONS) >= 8, "passed": len(BASE_ADVANCEMENT_TRANSITIONS) >= 8},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows)},
        {"check": "feature_scope", "passed": len(FEATURES) >= 15},
        {"check": "injection_path_contract", "passed": len(injection_rows) == 6},
        {"check": "base_advancement_scope", "passed": len(BASE_ADVANCEMENT_TRANSITIONS) >= 8},
        {"check": "opener_bulk_scope", "passed": len(OPENER_BULK_SCOPE) >= 5},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows)},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows)},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    recommended_rows = [
        {
            "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
            "recommended_path": RECOMMENDED_PATH,
            "reason": "Audit the planning map before executing source inventory or feature wiring.",
            "passed": True,
        }
    ]

    write_csv(CHECKS_CSV, checks)
    write_csv(PREDECESSOR_CSV, predecessor_rows)
    write_csv(FEATURE_SCOPE_CSV, FEATURES)
    write_csv(INJECTION_PATH_CSV, injection_rows)
    write_csv(BASE_ADVANCEMENT_CSV, BASE_ADVANCEMENT_TRANSITIONS)
    write_csv(PITCHER_USAGE_CSV, OPENER_BULK_SCOPE)
    write_csv(EVALUATION_PLAN_CSV, evaluation_rows)
    write_csv(BLOCKERS_CSV, blockers)
    write_csv(SAFETY_CSV, safety_rows)
    write_csv(DECISION_CSV, decision_rows)
    write_csv(RECOMMENDED_CSV, recommended_rows)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_model_projection_realism_injection_map_plan_complete" if all_checks_passed else "layer_6_model_projection_realism_injection_map_plan_failed",
        "all_checks_passed": all_checks_passed,
        "feature_families_planned": len(FEATURES),
        "opener_bulk_pitcher_included": True,
        "base_advancement_transitions_included": True,
        "base_advancement_transition_cases_planned": len(BASE_ADVANCEMENT_TRANSITIONS),
        "injection_path_steps_defined": len(injection_rows),
        "historical_validation_allowed_next": False,
        "feature_wiring_allowed_next": False,
        "tuning_allowed_next": False,
        "prediction_join_execution_allowed_next": False,
        "accuracy_metrics_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "layer6_exit_recommended": False,
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "generated_csv_artifacts": [
            str(CHECKS_CSV),
            str(PREDECESSOR_CSV),
            str(FEATURE_SCOPE_CSV),
            str(INJECTION_PATH_CSV),
            str(BASE_ADVANCEMENT_CSV),
            str(PITCHER_USAGE_CSV),
            str(EVALUATION_PLAN_CSV),
            str(BLOCKERS_CSV),
            str(SAFETY_CSV),
            str(DECISION_CSV),
            str(RECOMMENDED_CSV),
        ],
        "generated_json_artifacts": [str(JSON_PATH)],
    }

    JSON_PATH.write_text(json.dumps(diagnosis, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(diagnosis, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
