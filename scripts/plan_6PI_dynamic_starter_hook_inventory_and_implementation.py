#!/usr/bin/env python3
"""
Layer 6PI
Dynamic Starter-Hook Inventory and Implementation Plan

Inventories the existing starter-exit mechanism and defines the contract for
GM-02 without changing production simulation behavior.

Current mechanism:
- derives starter quality from pregame pitcher-profile inputs;
- maps quality to an expected-innings estimate;
- samples one whole-inning exit threshold before each simulation;
- switches from starter PA probabilities to aggregate bullpen probabilities
  after that threshold.

This is probabilistic starter-duration modeling, but it is not yet a
game-state-responsive managerial hook.

This layer grants planning authority only. It does not alter:
- starter innings;
- bullpen usage;
- plate-appearance probabilities;
- simulation scores;
- win probabilities;
- canonical probability authority;
- backend or frontend behavior.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6PI"
LAYER_NAME = (
    "dynamic_starter_hook_"
    "inventory_and_implementation_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PI_dynamic_starter_hook_"
    "inventory_and_implementation_plan"
)

COMPLETION_ASSESSMENT_PATH = (
    ROOT
    / "scripts/assess_6PH_"
    "pitching_plan_classification_"
    "diagnostic_integration_completion.py"
)

ENGINE_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "game_engine_v2.py"
)

SIMULATOR_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "game_simulator.py"
)

INNING_SIMULATOR_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "inning_simulator.py"
)

REQUIRED_PATHS = [
    COMPLETION_ASSESSMENT_PATH,
    ENGINE_PATH,
    SIMULATOR_PATH,
    INNING_SIMULATOR_PATH,
]

PROHIBITED_ACTIONS = [
    "production_starter_hook_change",
    "starter_exit_distribution_change",
    "starter_innings_change",
    "bullpen_transition_change",
    "bullpen_sequence_change",
    "plate_appearance_probability_change",
    "simulation_parameter_change",
    "simulation_score_change",
    "win_probability_change",
    "canonical_probability_replacement",
    "pitching_plan_production_activation",
    "backend_response_change",
    "frontend_behavior_change",
    "historical_outcome_join",
    "accuracy_metric_generation",
    "parameter_tuning",
    "backtest_execution",
    "pricing",
    "edge_detection",
    "bet_recommendation",
    "broad_layer6_exit",
]


def write_csv(
    path: Path,
    fieldnames: list[str],
    rows: Iterable[dict[str, Any]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(
    path: Path,
    payload: Any,
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )


def read_text(path: Path) -> str:
    if not path.exists():
        return ""

    return path.read_text(
        encoding="utf-8",
        errors="ignore",
    )


def string_constants(path: Path) -> set[str]:
    if not path.exists():
        return set()

    tree = ast.parse(
        read_text(path),
        filename=str(path),
    )

    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }


def function_inventory(
    path: Path,
) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}

    source = read_text(path)

    tree = ast.parse(
        source,
        filename=str(path),
    )

    inventory: dict[str, dict[str, Any]] = {}

    for node in ast.walk(tree):
        if not isinstance(
            node,
            (
                ast.FunctionDef,
                ast.AsyncFunctionDef,
            ),
        ):
            continue

        arguments = [
            argument.arg
            for argument in (
                list(node.args.posonlyargs)
                + list(node.args.args)
                + list(node.args.kwonlyargs)
            )
        ]

        inventory[node.name] = {
            "arguments": arguments,
            "line_start": node.lineno,
            "line_end": getattr(
                node,
                "end_lineno",
                None,
            ),
        }

    return inventory


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    required_files_exist = all(
        path.exists()
        for path in REQUIRED_PATHS
    )

    completion_constants = string_constants(
        COMPLETION_ASSESSMENT_PATH
    )

    predecessor_contract_present = all(
        value in completion_constants
        for value in [
            (
                "pitching_plan_classification_"
                "diagnostic_integration_"
                "completion_assessment_passed"
            ),
            (
                "6PI_dynamic_starter_hook_"
                "inventory_and_implementation_plan"
            ),
        ]
    )

    engine_text = read_text(ENGINE_PATH)
    simulator_text = read_text(SIMULATOR_PATH)

    engine_functions = function_inventory(
        ENGINE_PATH
    )

    simulator_functions = function_inventory(
        SIMULATOR_PATH
    )

    required_engine_functions = {
        "_starter_quality_score",
        "_expected_starter_innings",
        "_build_bullpen_adjusted_game_sim",
        "run_full_game_simulation",
    }

    required_simulator_functions = {
        "_sample_from_distribution",
        "_blend_distributions",
        "_starter_exit_distribution_from_score",
        "starter_quality_score",
        "simulate_game_with_bullpen",
    }

    engine_functions_present = (
        required_engine_functions.issubset(
            engine_functions
        )
    )

    simulator_functions_present = (
        required_simulator_functions.issubset(
            simulator_functions
        )
    )

    simulator_signature = (
        simulator_functions.get(
            "simulate_game_with_bullpen",
            {},
        ).get("arguments", [])
    )

    state_responsive_signature_inputs = {
        "pitch_count",
        "batters_faced",
        "times_through_order",
        "runs_allowed",
        "base_state",
        "outs",
        "score_margin",
        "leverage",
        "recent_traffic",
        "fatigue",
    }

    current_state_signature_inputs = sorted(
        set(simulator_signature)
        & state_responsive_signature_inputs
    )

    dynamic_flag_hardcoded_true = (
        "dynamic_starter_exit=True"
        in engine_text
    )

    starter_override_present = (
        'config.get("starter_innings")'
        in engine_text
    )

    side_specific_expected_innings_present = all(
        token in engine_text
        for token in [
            "away_expected_starter_innings",
            "home_expected_starter_innings",
        ]
    )

    shared_starter_innings_average_present = all(
        token in engine_text
        for token in [
            (
                "Current simulator accepts one shared "
                "starter_innings value"
            ),
            (
                "(away_expected_starter_innings "
                "+ home_expected_starter_innings) / 2"
            ),
        ]
    )

    quality_distribution_present = all(
        token in simulator_text
        for token in [
            (
                "_starter_exit_distribution_"
                "from_score"
            ),
            "weak = {",
            "average = {",
            "strong = {",
        ]
    )

    exit_sample_position = simulator_text.find(
        "_sample_from_distribution("
    )

    inning_loop_position = simulator_text.find(
        "for inning_index in range("
    )

    exit_sampled_before_inning_loop = all(
        [
            exit_sample_position >= 0,
            inning_loop_position >= 0,
            exit_sample_position
            < inning_loop_position,
        ]
    )

    starter_to_bullpen_switch_present = all(
        token in simulator_text
        for token in [
            (
                "if inning_index "
                "<= home_starter_innings"
            ),
            (
                "else "
                "away_bullpen_probabilities"
            ),
            (
                "if inning_index "
                "<= away_starter_innings"
            ),
            (
                "else "
                "home_bullpen_probabilities"
            ),
        ]
    )

    exit_distribution_outputs_present = all(
        token in simulator_text
        for token in [
            (
                "away_starter_innings_"
                "distribution"
            ),
            (
                "home_starter_innings_"
                "distribution"
            ),
            "dynamic_starter_exit",
        ]
    )

    engine_diagnostics_present = all(
        token in engine_text
        for token in [
            "starter_exit_model",
            "away_expected_starter_innings",
            "home_expected_starter_innings",
            "simulator_starter_innings",
        ]
    )

    pitching_plan_behavior_reach = any(
        token in engine_text
        or token in simulator_text
        for token in [
            "classify_pitching_plan",
            "pitching_plan_classifier",
            (
                "pitching_plan_diagnostics"
                '["classification"]'
            ),
        ]
    )

    current_authority_map = [
        {
            "authority_id": "SH-A01",
            "component": (
                "Pregame starter quality"
            ),
            "path": (
                "mlb_app/simulation/"
                "game_engine_v2.py"
            ),
            "function": (
                "_starter_quality_score"
            ),
            "current_authority": (
                "Derives a scalar quality score from "
                "pitcher-profile signals."
            ),
            "behavioral": True,
        },
        {
            "authority_id": "SH-A02",
            "component": (
                "Expected starter innings"
            ),
            "path": (
                "mlb_app/simulation/"
                "game_engine_v2.py"
            ),
            "function": (
                "_expected_starter_innings"
            ),
            "current_authority": (
                "Maps quality to a bounded pregame "
                "expected-innings estimate."
            ),
            "behavioral": True,
        },
        {
            "authority_id": "SH-A03",
            "component": (
                "Exit-inning distribution"
            ),
            "path": (
                "mlb_app/simulation/"
                "game_simulator.py"
            ),
            "function": (
                "_starter_exit_distribution_"
                "from_score"
            ),
            "current_authority": (
                "Blends weak, average, and strong "
                "whole-inning distributions."
            ),
            "behavioral": True,
        },
        {
            "authority_id": "SH-A04",
            "component": (
                "Per-simulation exit sample"
            ),
            "path": (
                "mlb_app/simulation/"
                "game_simulator.py"
            ),
            "function": (
                "simulate_game_with_bullpen"
            ),
            "current_authority": (
                "Samples one exit threshold before "
                "inning simulation begins."
            ),
            "behavioral": True,
        },
        {
            "authority_id": "SH-A05",
            "component": (
                "Starter-to-bullpen transition"
            ),
            "path": (
                "mlb_app/simulation/"
                "game_simulator.py"
            ),
            "function": (
                "simulate_game_with_bullpen"
            ),
            "current_authority": (
                "Selects starter or aggregate bullpen "
                "PA probabilities by inning threshold."
            ),
            "behavioral": True,
        },
        {
            "authority_id": "SH-A06",
            "component": (
                "Dynamic-exit activation"
            ),
            "path": (
                "mlb_app/simulation/"
                "game_engine_v2.py"
            ),
            "function": (
                "run_full_game_simulation"
            ),
            "current_authority": (
                "Hardcodes dynamic_starter_exit=True "
                "for the bullpen-adjusted simulation."
            ),
            "behavioral": True,
        },
        {
            "authority_id": "SH-A07",
            "component": (
                "Pitching-plan classification"
            ),
            "path": (
                "mlb_app/simulation/"
                "game_simulation_builder.py"
            ),
            "function": (
                "_attach_pitching_plan_diagnostics"
            ),
            "current_authority": (
                "Diagnostic metadata only; no starter-"
                "hook behavior authority."
            ),
            "behavioral": False,
        },
    ]

    current_input_inventory = [
        {
            "input_id": "SH-I01",
            "input": "starter_quality_score",
            "currently_available": True,
            "currently_used": True,
            "source": (
                "pregame pitcher profile"
            ),
            "granularity": "game",
        },
        {
            "input_id": "SH-I02",
            "input": (
                "expected_starter_innings"
            ),
            "currently_available": True,
            "currently_used": True,
            "source": (
                "quality-score mapping or config override"
            ),
            "granularity": "game",
        },
        {
            "input_id": "SH-I03",
            "input": "inning_index",
            "currently_available": True,
            "currently_used": True,
            "source": "simulation loop",
            "granularity": "inning",
        },
        {
            "input_id": "SH-I04",
            "input": "pitch_count",
            "currently_available": False,
            "currently_used": False,
            "source": None,
            "granularity": "plate_appearance",
        },
        {
            "input_id": "SH-I05",
            "input": "batters_faced",
            "currently_available": False,
            "currently_used": False,
            "source": None,
            "granularity": "plate_appearance",
        },
        {
            "input_id": "SH-I06",
            "input": "times_through_order",
            "currently_available": False,
            "currently_used": False,
            "source": None,
            "granularity": "plate_appearance",
        },
        {
            "input_id": "SH-I07",
            "input": (
                "starter_runs_allowed"
            ),
            "currently_available": False,
            "currently_used": False,
            "source": None,
            "granularity": "inning",
        },
        {
            "input_id": "SH-I08",
            "input": (
                "starter_recent_traffic"
            ),
            "currently_available": False,
            "currently_used": False,
            "source": None,
            "granularity": "plate_appearance",
        },
        {
            "input_id": "SH-I09",
            "input": "base_out_state",
            "currently_available": False,
            "currently_used": False,
            "source": None,
            "granularity": "plate_appearance",
        },
        {
            "input_id": "SH-I10",
            "input": "score_margin",
            "currently_available": False,
            "currently_used": False,
            "source": None,
            "granularity": "inning",
        },
        {
            "input_id": "SH-I11",
            "input": "leverage_proxy",
            "currently_available": False,
            "currently_used": False,
            "source": None,
            "granularity": "plate_appearance",
        },
        {
            "input_id": "SH-I12",
            "input": "fatigue_index",
            "currently_available": False,
            "currently_used": False,
            "source": None,
            "granularity": "plate_appearance",
        },
        {
            "input_id": "SH-I13",
            "input": (
                "bullpen_availability"
            ),
            "currently_available": False,
            "currently_used": False,
            "source": None,
            "granularity": "game",
        },
        {
            "input_id": "SH-I14",
            "input": (
                "pitching_plan_classification"
            ),
            "currently_available": True,
            "currently_used": False,
            "source": (
                "disabled-by-default diagnostic metadata"
            ),
            "granularity": "game",
        },
    ]

    current_output_inventory = [
        {
            "output_id": "SH-O01",
            "output": (
                "away_starter_innings_distribution"
            ),
            "currently_emitted": True,
            "behavioral_authority": True,
        },
        {
            "output_id": "SH-O02",
            "output": (
                "home_starter_innings_distribution"
            ),
            "currently_emitted": True,
            "behavioral_authority": True,
        },
        {
            "output_id": "SH-O03",
            "output": "dynamic_starter_exit",
            "currently_emitted": True,
            "behavioral_authority": True,
        },
        {
            "output_id": "SH-O04",
            "output": "starter_quality_scores",
            "currently_emitted": True,
            "behavioral_authority": True,
        },
        {
            "output_id": "SH-O05",
            "output": (
                "starter_hook_decision_log"
            ),
            "currently_emitted": False,
            "behavioral_authority": False,
        },
        {
            "output_id": "SH-O06",
            "output": (
                "starter_hook_trigger_reasons"
            ),
            "currently_emitted": False,
            "behavioral_authority": False,
        },
        {
            "output_id": "SH-O07",
            "output": (
                "starter_hook_state_trace"
            ),
            "currently_emitted": False,
            "behavioral_authority": False,
        },
        {
            "output_id": "SH-O08",
            "output": (
                "starter_hook_fallback_status"
            ),
            "currently_emitted": False,
            "behavioral_authority": False,
        },
    ]

    gap_analysis = [
        {
            "gap_id": "SH-G01",
            "gap": (
                "Exit inning is sampled before game "
                "state develops."
            ),
            "impact": (
                "The hook cannot react to simulated "
                "performance."
            ),
            "priority": "critical",
        },
        {
            "gap_id": "SH-G02",
            "gap": (
                "No pitch-count or batters-faced state."
            ),
            "impact": (
                "Workload and fatigue are not represented."
            ),
            "priority": "critical",
        },
        {
            "gap_id": "SH-G03",
            "gap": (
                "No times-through-order state."
            ),
            "impact": (
                "Opponent familiarity cannot affect "
                "managerial removal."
            ),
            "priority": "high",
        },
        {
            "gap_id": "SH-G04",
            "gap": (
                "No runs-allowed or traffic response."
            ),
            "impact": (
                "Poor simulated outings do not trigger "
                "earlier removal."
            ),
            "priority": "critical",
        },
        {
            "gap_id": "SH-G05",
            "gap": (
                "No score or leverage context."
            ),
            "impact": (
                "Close-game and blowout hook behavior "
                "cannot differ."
            ),
            "priority": "high",
        },
        {
            "gap_id": "SH-G06",
            "gap": (
                "No bullpen-availability input."
            ),
            "impact": (
                "Starter removal has no roster-resource "
                "constraint."
            ),
            "priority": "high",
        },
        {
            "gap_id": "SH-G07",
            "gap": (
                "Simulator accepts one shared static "
                "starter_innings fallback."
            ),
            "impact": (
                "Side-specific fallback duration is lost "
                "when dynamic mode is disabled."
            ),
            "priority": "medium",
        },
        {
            "gap_id": "SH-G08",
            "gap": (
                "Starter-innings override is bypassed by "
                "the sampled distribution when dynamic "
                "mode is enabled."
            ),
            "impact": (
                "Override semantics are not explicit."
            ),
            "priority": "high",
        },
        {
            "gap_id": "SH-G09",
            "gap": (
                "Pitching-plan classification is not "
                "allowed to affect starter-hook behavior."
            ),
            "impact": (
                "Opener and bulk plans remain diagnostic "
                "only, as currently required."
            ),
            "priority": "intentional_boundary",
        },
        {
            "gap_id": "SH-G10",
            "gap": (
                "No decision trace explains why a starter "
                "was retained or removed."
            ),
            "impact": (
                "Hook behavior is difficult to audit."
            ),
            "priority": "high",
        },
    ]

    state_contract = [
        {
            "field": "inning",
            "type": "integer",
            "required": True,
            "initial_implementation_source": (
                "simulation loop"
            ),
        },
        {
            "field": "outs",
            "type": "integer",
            "required": True,
            "initial_implementation_source": (
                "future PA-level state"
            ),
        },
        {
            "field": "base_state",
            "type": "object",
            "required": True,
            "initial_implementation_source": (
                "future PA-level state"
            ),
        },
        {
            "field": "batters_faced",
            "type": "integer",
            "required": True,
            "initial_implementation_source": (
                "future starter state accumulator"
            ),
        },
        {
            "field": "pitch_count_estimate",
            "type": "number",
            "required": True,
            "initial_implementation_source": (
                "future pitch-count estimator"
            ),
        },
        {
            "field": "times_through_order",
            "type": "number",
            "required": True,
            "initial_implementation_source": (
                "batters_faced / lineup size"
            ),
        },
        {
            "field": "runs_allowed",
            "type": "integer",
            "required": True,
            "initial_implementation_source": (
                "starter state accumulator"
            ),
        },
        {
            "field": "recent_traffic_index",
            "type": "number",
            "required": True,
            "initial_implementation_source": (
                "recent baserunner events"
            ),
        },
        {
            "field": "score_margin",
            "type": "integer",
            "required": True,
            "initial_implementation_source": (
                "current game score"
            ),
        },
        {
            "field": "leverage_proxy",
            "type": "number",
            "required": True,
            "initial_implementation_source": (
                "inning, score, base-out state"
            ),
        },
        {
            "field": "starter_quality_score",
            "type": "number",
            "required": True,
            "initial_implementation_source": (
                "existing pregame quality model"
            ),
        },
        {
            "field": "expected_starter_innings",
            "type": "number",
            "required": True,
            "initial_implementation_source": (
                "existing expected-innings model"
            ),
        },
        {
            "field": "fatigue_index",
            "type": "number",
            "required": True,
            "initial_implementation_source": (
                "workload state"
            ),
        },
        {
            "field": "bullpen_availability",
            "type": "object",
            "required": False,
            "initial_implementation_source": (
                "future GM-03 contract"
            ),
        },
        {
            "field": "pitching_plan",
            "type": "object",
            "required": False,
            "initial_implementation_source": (
                "diagnostic only; no behavioral use "
                "authorized"
            ),
        },
    ]

    evaluator_contract = [
        {
            "field": "decision",
            "type": "enum",
            "required": True,
            "allowed_values": (
                "keep,pull,insufficient_state"
            ),
        },
        {
            "field": "pull_probability",
            "type": "number",
            "required": True,
            "allowed_values": "0.0_to_1.0",
        },
        {
            "field": "trigger_reasons",
            "type": "array",
            "required": True,
            "allowed_values": (
                "auditable_reason_codes"
            ),
        },
        {
            "field": "state_completeness",
            "type": "number",
            "required": True,
            "allowed_values": "0.0_to_1.0",
        },
        {
            "field": "fallback_used",
            "type": "boolean",
            "required": True,
            "allowed_values": "true,false",
        },
        {
            "field": "fallback_reason",
            "type": "nullable_string",
            "required": True,
            "allowed_values": (
                "explicit_reason_code_or_null"
            ),
        },
        {
            "field": "behavioral_effect",
            "type": "string",
            "required": True,
            "allowed_values": (
                "none_in_first_implementation"
            ),
        },
        {
            "field": (
                "canonical_probability_"
                "authority_changed"
            ),
            "type": "boolean",
            "required": True,
            "allowed_values": "false",
        },
        {
            "field": "production_activation",
            "type": "boolean",
            "required": True,
            "allowed_values": "false",
        },
    ]

    implementation_plan = [
        {
            "step": 1,
            "layer_scope": (
                "pure_evaluator"
            ),
            "action": (
                "Create a deterministic starter-hook "
                "state evaluator in a new isolated module."
            ),
            "production_behavior_change": False,
        },
        {
            "step": 2,
            "layer_scope": (
                "contract_validation"
            ),
            "action": (
                "Validate state and output payloads with "
                "explicit fallback behavior."
            ),
            "production_behavior_change": False,
        },
        {
            "step": 3,
            "layer_scope": "fixtures",
            "action": (
                "Test early traffic, high workload, third "
                "time through, low leverage, strong outing, "
                "and incomplete-state cases."
            ),
            "production_behavior_change": False,
        },
        {
            "step": 4,
            "layer_scope": (
                "diagnostic_integration_plan"
            ),
            "action": (
                "Plan disabled-by-default decision tracing "
                "without changing starter transitions."
            ),
            "production_behavior_change": False,
        },
        {
            "step": 5,
            "layer_scope": (
                "independent_audit"
            ),
            "action": (
                "Prove baseline equivalence and zero "
                "production behavior reach."
            ),
            "production_behavior_change": False,
        },
        {
            "step": 6,
            "layer_scope": (
                "future_behavioral_gate"
            ),
            "action": (
                "Require separate historical validation and "
                "authorization before replacing the current "
                "exit mechanism."
            ),
            "production_behavior_change": False,
        },
    ]

    fixture_matrix = [
        {
            "fixture_id": "SH-F01",
            "scenario": (
                "first_inning_clean_low_workload"
            ),
            "expected_decision": "keep",
            "behavioral_wiring_required": False,
        },
        {
            "fixture_id": "SH-F02",
            "scenario": (
                "fifth_inning_high_pitch_count"
            ),
            "expected_decision": "pull",
            "behavioral_wiring_required": False,
        },
        {
            "fixture_id": "SH-F03",
            "scenario": (
                "third_time_through_order"
            ),
            "expected_decision": "pull",
            "behavioral_wiring_required": False,
        },
        {
            "fixture_id": "SH-F04",
            "scenario": (
                "heavy_recent_traffic"
            ),
            "expected_decision": "pull",
            "behavioral_wiring_required": False,
        },
        {
            "fixture_id": "SH-F05",
            "scenario": (
                "strong_starter_moderate_workload"
            ),
            "expected_decision": "keep",
            "behavioral_wiring_required": False,
        },
        {
            "fixture_id": "SH-F06",
            "scenario": (
                "weak_starter_moderate_workload"
            ),
            "expected_decision": "pull",
            "behavioral_wiring_required": False,
        },
        {
            "fixture_id": "SH-F07",
            "scenario": (
                "late_close_game_high_leverage"
            ),
            "expected_decision": "pull",
            "behavioral_wiring_required": False,
        },
        {
            "fixture_id": "SH-F08",
            "scenario": (
                "late_blowout_low_leverage"
            ),
            "expected_decision": "keep",
            "behavioral_wiring_required": False,
        },
        {
            "fixture_id": "SH-F09",
            "scenario": (
                "incomplete_state"
            ),
            "expected_decision": (
                "insufficient_state"
            ),
            "behavioral_wiring_required": False,
        },
        {
            "fixture_id": "SH-F10",
            "scenario": (
                "deterministic_replay"
            ),
            "expected_decision": (
                "identical_output"
            ),
            "behavioral_wiring_required": False,
        },
    ]

    integration_seams = [
        {
            "seam_id": "SH-S01",
            "path": (
                "mlb_app/simulation/"
                "game_simulator.py"
            ),
            "current_anchor": (
                "simulate_game_with_bullpen"
            ),
            "future_role": (
                "Supply evolving starter state to an "
                "isolated evaluator."
            ),
            "first_implementation_allowed": False,
        },
        {
            "seam_id": "SH-S02",
            "path": (
                "mlb_app/simulation/"
                "inning_simulator.py"
            ),
            "current_anchor": (
                "simulate_half_inning"
            ),
            "future_role": (
                "Expose PA-level state or event trace "
                "without changing event probabilities."
            ),
            "first_implementation_allowed": False,
        },
        {
            "seam_id": "SH-S03",
            "path": (
                "mlb_app/simulation/"
                "game_engine_v2.py"
            ),
            "current_anchor": (
                "run_full_game_simulation"
            ),
            "future_role": (
                "Provide pregame quality and expected-"
                "innings context."
            ),
            "first_implementation_allowed": False,
        },
        {
            "seam_id": "SH-S04",
            "path": (
                "mlb_app/simulation/"
                "starter_hook_evaluator.py"
            ),
            "current_anchor": None,
            "future_role": (
                "New pure deterministic evaluator module."
            ),
            "first_implementation_allowed": True,
        },
    ]

    safety_rows = [
        {
            "boundary": action,
            "allowed_in_6PI": False,
            "reason": (
                "6PI is inventory and planning only."
            ),
        }
        for action in PROHIBITED_ACTIONS
    ]

    safety_rows.extend(
        [
            {
                "boundary": (
                    "starter_hook_inventory"
                ),
                "allowed_in_6PI": True,
                "reason": (
                    "Static inventory does not alter "
                    "simulation behavior."
                ),
            },
            {
                "boundary": (
                    "pure_evaluator_contract"
                ),
                "allowed_in_6PI": True,
                "reason": (
                    "Contract definition grants no "
                    "production authority."
                ),
            },
            {
                "boundary": (
                    "fixture_and_audit_plan"
                ),
                "allowed_in_6PI": True,
                "reason": (
                    "Planning test coverage is "
                    "non-behavioral."
                ),
            },
        ]
    )

    checks = [
        {
            "check": "required_files_exist",
            "actual": sum(
                1
                for path in REQUIRED_PATHS
                if path.exists()
            ),
            "expected": len(REQUIRED_PATHS),
            "passed": required_files_exist,
        },
        {
            "check": (
                "six_ph_completion_contract_present"
            ),
            "actual": (
                predecessor_contract_present
            ),
            "expected": True,
            "passed": (
                predecessor_contract_present
            ),
        },
        {
            "check": (
                "required_engine_functions_present"
            ),
            "actual": sorted(
                required_engine_functions
                & set(engine_functions)
            ),
            "expected": sorted(
                required_engine_functions
            ),
            "passed": (
                engine_functions_present
            ),
        },
        {
            "check": (
                "required_simulator_functions_present"
            ),
            "actual": sorted(
                required_simulator_functions
                & set(simulator_functions)
            ),
            "expected": sorted(
                required_simulator_functions
            ),
            "passed": (
                simulator_functions_present
            ),
        },
        {
            "check": (
                "dynamic_exit_hardcoded_true"
            ),
            "actual": (
                dynamic_flag_hardcoded_true
            ),
            "expected": True,
            "passed": (
                dynamic_flag_hardcoded_true
            ),
        },
        {
            "check": (
                "starter_override_present"
            ),
            "actual": (
                starter_override_present
            ),
            "expected": True,
            "passed": starter_override_present,
        },
        {
            "check": (
                "side_specific_expected_innings_present"
            ),
            "actual": (
                side_specific_expected_innings_present
            ),
            "expected": True,
            "passed": (
                side_specific_expected_innings_present
            ),
        },
        {
            "check": (
                "shared_static_fallback_present"
            ),
            "actual": (
                shared_starter_innings_average_present
            ),
            "expected": True,
            "passed": (
                shared_starter_innings_average_present
            ),
        },
        {
            "check": (
                "quality_exit_distribution_present"
            ),
            "actual": (
                quality_distribution_present
            ),
            "expected": True,
            "passed": (
                quality_distribution_present
            ),
        },
        {
            "check": (
                "exit_sampled_before_inning_loop"
            ),
            "actual": (
                exit_sampled_before_inning_loop
            ),
            "expected": True,
            "passed": (
                exit_sampled_before_inning_loop
            ),
        },
        {
            "check": (
                "starter_bullpen_switch_present"
            ),
            "actual": (
                starter_to_bullpen_switch_present
            ),
            "expected": True,
            "passed": (
                starter_to_bullpen_switch_present
            ),
        },
        {
            "check": (
                "state_responsive_signature_inputs"
            ),
            "actual": (
                current_state_signature_inputs
            ),
            "expected": [],
            "passed": (
                current_state_signature_inputs == []
            ),
        },
        {
            "check": (
                "exit_distribution_outputs_present"
            ),
            "actual": (
                exit_distribution_outputs_present
            ),
            "expected": True,
            "passed": (
                exit_distribution_outputs_present
            ),
        },
        {
            "check": (
                "engine_exit_diagnostics_present"
            ),
            "actual": (
                engine_diagnostics_present
            ),
            "expected": True,
            "passed": (
                engine_diagnostics_present
            ),
        },
        {
            "check": (
                "pitching_plan_has_no_behavior_reach"
            ),
            "actual": (
                pitching_plan_behavior_reach
            ),
            "expected": False,
            "passed": (
                not pitching_plan_behavior_reach
            ),
        },
        {
            "check": (
                "ten_hook_fixtures_planned"
            ),
            "actual": len(fixture_matrix),
            "expected": 10,
            "passed": (
                len(fixture_matrix) == 10
            ),
        },
        {
            "check": (
                "all_implementation_steps_nonbehavioral"
            ),
            "actual": any(
                row[
                    "production_behavior_change"
                ]
                for row in implementation_plan
            ),
            "expected": False,
            "passed": not any(
                row[
                    "production_behavior_change"
                ]
                for row in implementation_plan
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    recommended_next_layer = (
        "6PJ_dynamic_starter_hook_"
        "state_contract_and_evaluator_implementation"
    )

    write_csv(
        OUTPUT_DIR / "inventory_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "current_authority_map.csv",
        [
            "authority_id",
            "component",
            "path",
            "function",
            "current_authority",
            "behavioral",
        ],
        current_authority_map,
    )

    write_csv(
        OUTPUT_DIR / "current_input_inventory.csv",
        [
            "input_id",
            "input",
            "currently_available",
            "currently_used",
            "source",
            "granularity",
        ],
        current_input_inventory,
    )

    write_csv(
        OUTPUT_DIR / "current_output_inventory.csv",
        [
            "output_id",
            "output",
            "currently_emitted",
            "behavioral_authority",
        ],
        current_output_inventory,
    )

    write_csv(
        OUTPUT_DIR / "gap_analysis.csv",
        [
            "gap_id",
            "gap",
            "impact",
            "priority",
        ],
        gap_analysis,
    )

    write_csv(
        OUTPUT_DIR / "state_contract.csv",
        [
            "field",
            "type",
            "required",
            "initial_implementation_source",
        ],
        state_contract,
    )

    write_csv(
        OUTPUT_DIR / "evaluator_contract.csv",
        [
            "field",
            "type",
            "required",
            "allowed_values",
        ],
        evaluator_contract,
    )

    write_csv(
        OUTPUT_DIR / "implementation_plan.csv",
        [
            "step",
            "layer_scope",
            "action",
            "production_behavior_change",
        ],
        implementation_plan,
    )

    write_csv(
        OUTPUT_DIR / "fixture_matrix.csv",
        [
            "fixture_id",
            "scenario",
            "expected_decision",
            "behavioral_wiring_required",
        ],
        fixture_matrix,
    )

    write_csv(
        OUTPUT_DIR / "integration_seams.csv",
        [
            "seam_id",
            "path",
            "current_anchor",
            "future_role",
            "first_implementation_allowed",
        ],
        integration_seams,
    )

    write_csv(
        OUTPUT_DIR / "safety_boundaries.csv",
        [
            "boundary",
            "allowed_in_6PI",
            "reason",
        ],
        safety_rows,
    )

    write_csv(
        OUTPUT_DIR / "recommended_path.csv",
        [
            "recommended_next_layer",
            "recommended_action",
            "entry_condition",
            "passed",
        ],
        [
            {
                "recommended_next_layer": (
                    recommended_next_layer
                ),
                "recommended_action": (
                    "Implement a pure deterministic "
                    "starter-hook state evaluator with no "
                    "production simulation wiring."
                ),
                "entry_condition": (
                    "All 6PI inventory and planning "
                    "checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    inventory_summary = {
        "current_model_type": (
            "pregame_quality_based_presampled_"
            "whole_inning_exit_threshold"
        ),
        "current_model_is_game_state_responsive": (
            False
        ),
        "dynamic_exit_hardcoded_true": (
            dynamic_flag_hardcoded_true
        ),
        "exit_sampled_before_inning_loop": (
            exit_sampled_before_inning_loop
        ),
        "state_responsive_signature_inputs": (
            current_state_signature_inputs
        ),
        "starter_override_present": (
            starter_override_present
        ),
        "shared_static_fallback_present": (
            shared_starter_innings_average_present
        ),
        "pitching_plan_behavior_reach": (
            pitching_plan_behavior_reach
        ),
        "production_behavior_changed": False,
        "canonical_probability_authority_changed": (
            False
        ),
        "new_authority_granted": False,
    }

    write_json(
        OUTPUT_DIR / "inventory_summary.json",
        inventory_summary,
    )

    plan_summary = {
        "next_module": (
            "mlb_app/simulation/"
            "starter_hook_evaluator.py"
        ),
        "next_scope": (
            "pure_deterministic_evaluator_only"
        ),
        "state_fields_defined": len(
            state_contract
        ),
        "evaluator_fields_defined": len(
            evaluator_contract
        ),
        "fixtures_planned": len(
            fixture_matrix
        ),
        "implementation_steps": len(
            implementation_plan
        ),
        "production_wiring_allowed_next": False,
        "simulation_behavior_change_allowed_next": False,
        "historical_validation_allowed_next": False,
        "parameter_tuning_allowed_next": False,
        "canonical_probability_replacement_allowed_next": False,
    }

    write_json(
        OUTPUT_DIR / "implementation_plan.json",
        plan_summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "dynamic_starter_hook_inventory_"
            "and_implementation_plan_complete"
            if all_checks_passed
            else
            "dynamic_starter_hook_inventory_"
            "and_implementation_plan_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "inventory_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "inventory_checks_required": len(
            checks
        ),
        "current_authorities_mapped": len(
            current_authority_map
        ),
        "current_inputs_inventoried": len(
            current_input_inventory
        ),
        "current_outputs_inventoried": len(
            current_output_inventory
        ),
        "gaps_identified": len(
            gap_analysis
        ),
        "state_contract_fields_defined": len(
            state_contract
        ),
        "evaluator_contract_fields_defined": len(
            evaluator_contract
        ),
        "implementation_steps_planned": len(
            implementation_plan
        ),
        "fixtures_planned": len(
            fixture_matrix
        ),
        "integration_seams_mapped": len(
            integration_seams
        ),
        "current_model_type": (
            "pregame_quality_based_presampled_"
            "whole_inning_exit_threshold"
        ),
        "current_model_is_game_state_responsive": False,
        "dynamic_exit_hardcoded_true": (
            dynamic_flag_hardcoded_true
        ),
        "exit_sampled_before_inning_loop": (
            exit_sampled_before_inning_loop
        ),
        "pitching_plan_behavior_reach": (
            pitching_plan_behavior_reach
        ),
        "gm01_diagnostic_scope_complete": True,
        "gm01_production_behavior_authorized": False,
        "production_starter_hook_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": (
            False
        ),
        "broad_layer6_exit_paused": True,
        "layer6_exit_recommended": False,
        "layer6_exit_finalized": False,
        "new_authority_granted": False,
        "backend_behavior_change_allowed_next": False,
        "frontend_behavior_change_allowed_next": False,
        "simulation_parameter_change_allowed_next": False,
        "final_probability_replacement_allowed_next": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "prediction_join_execution_allowed_next": False,
        "accuracy_metrics_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "pure_evaluator_implementation_allowed_next": (
            all_checks_passed
        ),
        "diagnostic_integration_allowed_next": False,
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "inventory_checks.csv"
            ),
            str(
                OUTPUT_DIR
                / "current_authority_map.csv"
            ),
            str(
                OUTPUT_DIR
                / "current_input_inventory.csv"
            ),
            str(
                OUTPUT_DIR
                / "current_output_inventory.csv"
            ),
            str(
                OUTPUT_DIR / "gap_analysis.csv"
            ),
            str(
                OUTPUT_DIR / "state_contract.csv"
            ),
            str(
                OUTPUT_DIR / "evaluator_contract.csv"
            ),
            str(
                OUTPUT_DIR / "implementation_plan.csv"
            ),
            str(
                OUTPUT_DIR / "fixture_matrix.csv"
            ),
            str(
                OUTPUT_DIR / "integration_seams.csv"
            ),
            str(
                OUTPUT_DIR / "safety_boundaries.csv"
            ),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR / "inventory_summary.json"
            ),
            str(
                OUTPUT_DIR / "implementation_plan.json"
            ),
            str(OUTPUT_DIR / "diagnosis.json"),
        ],
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(json.dumps(diagnosis, indent=2))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
