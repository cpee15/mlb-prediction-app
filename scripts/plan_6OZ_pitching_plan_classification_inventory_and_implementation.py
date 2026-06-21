#!/usr/bin/env python3
"""
Layer 6OZ
Pitching-Plan Classification Inventory and Implementation Plan

GM-01 planning layer.

Inventories the exact production pitching path and defines the
implementation contract for:

- traditional starter
- opener
- bulk follower
- tandem starter
- bullpen game
- workload-capped starter
- unknown / fallback plan

This layer does not change or activate production behavior.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6OZ"
LAYER_NAME = (
    "pitching_plan_classification_"
    "inventory_and_implementation_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6OZ_pitching_plan_classification_"
    "inventory_and_implementation_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/plan_6OY_layer6_game_management_"
    "scope_resolution.py"
)

SOURCE_PATHS = {
    "shared_builder": (
        ROOT
        / "mlb_app/simulation/game_simulation_builder.py"
    ),
    "game_engine": (
        ROOT
        / "mlb_app/simulation/game_engine_v2.py"
    ),
    "game_simulator": (
        ROOT
        / "mlb_app/simulation/game_simulator.py"
    ),
    "model_projections": (
        ROOT / "mlb_app/model_projections.py"
    ),
    "bullpen_profile": (
        ROOT / "mlb_app/bullpen_profile.py"
    ),
    "bullpen_chain": (
        ROOT
        / "mlb_app/simulation/bullpen_chain.py"
    ),
    "bullpen_selection": (
        ROOT
        / "mlb_app/simulation/bullpen_selection.py"
    ),
    "bullpen_integration": (
        ROOT
        / "mlb_app/simulation/bullpen_integration.py"
    ),
    "bullpen_game_hook": (
        ROOT
        / "mlb_app/simulation/bullpen_game_engine_hook.py"
    ),
    "bullpen_simulation_path": (
        ROOT
        / "mlb_app/simulation/bullpen_simulation_path.py"
    ),
}

REQUIRED_PATHS = [
    PREDECESSOR_PATH,
    *SOURCE_PATHS.values(),
]

PROHIBITED_ACTIONS = [
    "backend_behavior_change",
    "frontend_behavior_change",
    "production_classifier_activation",
    "simulation_parameter_change",
    "simulation_probability_change",
    "canonical_probability_replacement",
    "historical_outcome_join",
    "accuracy_metric_generation",
    "parameter_tuning",
    "backtest_execution",
    "pricing",
    "edge_detection",
    "bet_recommendation",
    "layer6_exit_finalization",
]


def write_csv(
    path: Path,
    fieldnames: list[str],
    rows: Iterable[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

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


def write_json(path: Path, payload: Any) -> None:
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
        path.read_text(
            encoding="utf-8",
            errors="ignore",
        )
    )

    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }


def token_presence(
    text: str,
    tokens: list[str],
) -> dict[str, bool]:
    lowered = text.lower()

    return {
        token: token.lower() in lowered
        for token in tokens
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    required_paths_exist = all(
        path.exists()
        for path in REQUIRED_PATHS
    )

    predecessor_constants = string_constants(
        PREDECESSOR_PATH
    )

    predecessor_contract_present = all(
        token in predecessor_constants
        for token in [
            (
                "layer_6_game_management_"
                "scope_resolution_plan_complete"
            ),
            "GM-01",
            "pitching_plan_classification",
            (
                "6OZ_pitching_plan_classification_"
                "inventory_and_implementation_plan"
            ),
            "broad_layer6_exit_paused",
            "new_authority_granted",
        ]
    )

    source_text = {
        key: read_text(path)
        for key, path in SOURCE_PATHS.items()
    }

    combined_production_text = "\n".join(
        source_text.values()
    )

    production_tokens = token_presence(
        combined_production_text,
        [
            "starter_quality_score",
            "starter_innings",
            "dynamic_starter_exit",
            "bullpen",
            "candidate_leverage",
            "selection_history",
            "classify_pitching_plan",
            "pitching_plan_type",
            "traditional_starter",
            "bulk_follower",
            "tandem_starter",
            "workload_cap",
        ],
    )

    shared_builder_present = all(
        token in source_text["shared_builder"]
        for token in [
            "build_game_simulation",
            "game_engine_v2",
            "starter_exit_enabled",
        ]
    )

    starter_quality_present = all(
        token in source_text["game_engine"]
        for token in [
            "_starter_quality_score",
            "_expected_starter_innings",
            "starter_innings",
        ]
    )

    starter_bullpen_split_present = all(
        token in source_text["game_engine"]
        for token in [
            "_build_bullpen_pa_model",
            "simulate_game_with_bullpen",
            "starter_innings",
            "dynamic_starter_exit",
        ]
    )

    candidate_bullpen_path_present = all(
        token in combined_production_text
        for token in [
            "simulate_candidate_bullpen_chain",
            "select_candidate_reliever",
            "candidate_leverage",
            "selection_history",
        ]
    )

    explicit_plan_classifier_present = any(
        production_tokens[token]
        for token in [
            "classify_pitching_plan",
            "pitching_plan_type",
        ]
    )

    explicit_plan_types_present = all(
        production_tokens[token]
        for token in [
            "traditional_starter",
            "bulk_follower",
            "tandem_starter",
        ]
    )

    explicit_workload_cap_present = (
        production_tokens["workload_cap"]
    )

    explicit_pitching_sequence_present = any(
        token in combined_production_text
        for token in [
            "pitching_plan_sequence",
            "planned_pitcher_sequence",
            "pitcher_sequence_plan",
            "bulk_follower_id",
            "tandem_pitcher_id",
        ]
    )

    current_inventory_rows = [
        {
            "component": "shared_simulation_builder",
            "status": (
                "present"
                if shared_builder_present
                else "missing"
            ),
            "production_verified": (
                shared_builder_present
            ),
            "evidence": str(
                SOURCE_PATHS["shared_builder"]
            ),
            "interpretation": (
                "Shared production builder exists and routes "
                "to game_engine_v2."
            ),
        },
        {
            "component": "starter_quality_and_expected_innings",
            "status": (
                "present"
                if starter_quality_present
                else "missing"
            ),
            "production_verified": (
                starter_quality_present
            ),
            "evidence": str(
                SOURCE_PATHS["game_engine"]
            ),
            "interpretation": (
                "Current engine derives expected starter innings "
                "from starter quality."
            ),
        },
        {
            "component": "starter_bullpen_probability_split",
            "status": (
                "present"
                if starter_bullpen_split_present
                else "missing"
            ),
            "production_verified": (
                starter_bullpen_split_present
            ),
            "evidence": str(
                SOURCE_PATHS["game_engine"]
            ),
            "interpretation": (
                "Current engine supports starter and aggregate "
                "bullpen probability profiles."
            ),
        },
        {
            "component": "candidate_bullpen_sequence_path",
            "status": (
                "present_optional"
                if candidate_bullpen_path_present
                else "missing"
            ),
            "production_verified": (
                candidate_bullpen_path_present
            ),
            "evidence": (
                "bullpen_chain|bullpen_selection|"
                "bullpen_integration"
            ),
            "interpretation": (
                "Candidate leverage sequencing infrastructure "
                "exists but is separate from explicit pregame "
                "pitching-plan classification."
            ),
        },
        {
            "component": "explicit_pitching_plan_classifier",
            "status": (
                "present"
                if explicit_plan_classifier_present
                else "absent"
            ),
            "production_verified": (
                explicit_plan_classifier_present
            ),
            "evidence": (
                "classify_pitching_plan|pitching_plan_type"
            ),
            "interpretation": (
                "No explicit production classifier is expected "
                "before GM-01 implementation."
            ),
        },
        {
            "component": "explicit_pitching_plan_types",
            "status": (
                "present"
                if explicit_plan_types_present
                else "absent"
            ),
            "production_verified": (
                explicit_plan_types_present
            ),
            "evidence": (
                "traditional_starter|bulk_follower|"
                "tandem_starter"
            ),
            "interpretation": (
                "Current production path does not expose the "
                "required complete plan taxonomy."
            ),
        },
        {
            "component": "explicit_pitcher_sequence_plan",
            "status": (
                "present"
                if explicit_pitching_sequence_present
                else "absent"
            ),
            "production_verified": (
                explicit_pitching_sequence_present
            ),
            "evidence": (
                "pitching_plan_sequence|"
                "planned_pitcher_sequence"
            ),
            "interpretation": (
                "No explicit pregame ordered pitcher sequence "
                "is verified."
            ),
        },
        {
            "component": "explicit_workload_cap",
            "status": (
                "present"
                if explicit_workload_cap_present
                else "absent"
            ),
            "production_verified": (
                explicit_workload_cap_present
            ),
            "evidence": "workload_cap",
            "interpretation": (
                "No explicit workload-cap contract is verified."
            ),
        },
    ]

    plan_types = [
        {
            "plan_type": "traditional_starter",
            "description": (
                "Listed starter is expected to handle the first "
                "and largest pitching segment."
            ),
            "minimum_required_roles": "starter",
            "ordered_sequence_required": False,
            "workload_cap_allowed": True,
            "fallback_allowed": True,
        },
        {
            "plan_type": "opener_bulk",
            "description": (
                "Short opener segment is followed by a designated "
                "bulk pitcher."
            ),
            "minimum_required_roles": "opener|bulk_follower",
            "ordered_sequence_required": True,
            "workload_cap_allowed": True,
            "fallback_allowed": True,
        },
        {
            "plan_type": "tandem",
            "description": (
                "Two planned pitchers divide substantial early "
                "and middle innings."
            ),
            "minimum_required_roles": (
                "tandem_primary|tandem_secondary"
            ),
            "ordered_sequence_required": True,
            "workload_cap_allowed": True,
            "fallback_allowed": True,
        },
        {
            "plan_type": "bullpen_game",
            "description": (
                "No traditional starter or bulk pitcher is planned; "
                "multiple relievers cover the game."
            ),
            "minimum_required_roles": "bullpen_sequence",
            "ordered_sequence_required": False,
            "workload_cap_allowed": True,
            "fallback_allowed": True,
        },
        {
            "plan_type": "workload_capped_starter",
            "description": (
                "Listed starter begins the game but has an explicit "
                "innings, pitch, or batter cap."
            ),
            "minimum_required_roles": "starter",
            "ordered_sequence_required": False,
            "workload_cap_allowed": True,
            "fallback_allowed": True,
        },
        {
            "plan_type": "unknown_fallback",
            "description": (
                "Insufficient or contradictory evidence prevents "
                "a stronger plan classification."
            ),
            "minimum_required_roles": "listed_starter_or_team",
            "ordered_sequence_required": False,
            "workload_cap_allowed": False,
            "fallback_allowed": True,
        },
    ]

    classification_inputs = [
        {
            "input": "listed_starter_id",
            "required": True,
            "source_priority": 1,
            "fallback": "unknown",
            "purpose": (
                "Identify the officially listed first pitcher."
            ),
        },
        {
            "input": "expected_primary_pitcher_id",
            "required": False,
            "source_priority": 2,
            "fallback": "listed_starter_id",
            "purpose": (
                "Identify the pitcher expected to face the most "
                "batters when different from the opener."
            ),
        },
        {
            "input": "expected_bulk_pitcher_id",
            "required": False,
            "source_priority": 2,
            "fallback": "none",
            "purpose": (
                "Identify a designated follower or bulk pitcher."
            ),
        },
        {
            "input": "announced_pitching_plan",
            "required": False,
            "source_priority": 1,
            "fallback": "none",
            "purpose": (
                "Use explicit team or verified reporting when "
                "available."
            ),
        },
        {
            "input": "starter_recent_workload",
            "required": False,
            "source_priority": 3,
            "fallback": "unknown",
            "purpose": (
                "Detect likely workload restrictions."
            ),
        },
        {
            "input": "starter_expected_innings",
            "required": False,
            "source_priority": 4,
            "fallback": (
                "existing_quality_based_expectation"
            ),
            "purpose": (
                "Retain the current expected-innings model as a "
                "fallback rather than a plan classifier."
            ),
        },
        {
            "input": "team_bullpen_game_indicator",
            "required": False,
            "source_priority": 3,
            "fallback": False,
            "purpose": (
                "Identify an expected multi-reliever game."
            ),
        },
        {
            "input": "roster_and_availability_state",
            "required": False,
            "source_priority": 3,
            "fallback": "unknown",
            "purpose": (
                "Confirm that planned pitchers are active and "
                "available."
            ),
        },
    ]

    output_contract = [
        {
            "field": "plan_type",
            "type": "string",
            "required": True,
            "description": (
                "One value from the approved pitching-plan taxonomy."
            ),
        },
        {
            "field": "confidence",
            "type": "float_0_1",
            "required": True,
            "description": (
                "Confidence in the classification."
            ),
        },
        {
            "field": "source_status",
            "type": "string",
            "required": True,
            "description": (
                "verified, inferred, fallback, or contradictory."
            ),
        },
        {
            "field": "source_provenance",
            "type": "object",
            "required": True,
            "description": (
                "Source names, timestamps, and fallback reasons."
            ),
        },
        {
            "field": "listed_starter_id",
            "type": "nullable_string",
            "required": True,
            "description": (
                "Officially listed first pitcher."
            ),
        },
        {
            "field": "primary_pitcher_id",
            "type": "nullable_string",
            "required": True,
            "description": (
                "Pitcher expected to cover the largest segment."
            ),
        },
        {
            "field": "bulk_pitcher_id",
            "type": "nullable_string",
            "required": True,
            "description": (
                "Designated follower when applicable."
            ),
        },
        {
            "field": "planned_sequence",
            "type": "list",
            "required": True,
            "description": (
                "Ordered known or inferred pitching segments."
            ),
        },
        {
            "field": "workload_cap",
            "type": "nullable_object",
            "required": True,
            "description": (
                "Pitch, batter, or inning cap with provenance."
            ),
        },
        {
            "field": "fallback_used",
            "type": "boolean",
            "required": True,
            "description": (
                "Whether the classification used fallback logic."
            ),
        },
        {
            "field": "diagnostics",
            "type": "object",
            "required": True,
            "description": (
                "Explainable classification evidence."
            ),
        },
    ]

    decision_rules = [
        {
            "rule_id": "PP-R01",
            "priority": 1,
            "condition": (
                "Verified announcement identifies opener and "
                "bulk follower."
            ),
            "plan_type": "opener_bulk",
            "confidence_band": "high",
        },
        {
            "rule_id": "PP-R02",
            "priority": 2,
            "condition": (
                "Verified plan identifies two substantial planned "
                "pitching segments."
            ),
            "plan_type": "tandem",
            "confidence_band": "high",
        },
        {
            "rule_id": "PP-R03",
            "priority": 3,
            "condition": (
                "Verified plan or roster context indicates no "
                "traditional starter and broad reliever coverage."
            ),
            "plan_type": "bullpen_game",
            "confidence_band": "high_or_medium",
        },
        {
            "rule_id": "PP-R04",
            "priority": 4,
            "condition": (
                "Listed starter has a verified pitch, batter, or "
                "inning restriction."
            ),
            "plan_type": "workload_capped_starter",
            "confidence_band": "high_or_medium",
        },
        {
            "rule_id": "PP-R05",
            "priority": 5,
            "condition": (
                "Listed starter is expected to cover the first and "
                "largest segment with no conflicting evidence."
            ),
            "plan_type": "traditional_starter",
            "confidence_band": "medium",
        },
        {
            "rule_id": "PP-R06",
            "priority": 6,
            "condition": (
                "Evidence is missing or contradictory."
            ),
            "plan_type": "unknown_fallback",
            "confidence_band": "low",
        },
    ]

    integration_points = [
        {
            "integration_id": "PP-I01",
            "path": (
                "mlb_app/simulation/"
                "game_simulation_builder.py"
            ),
            "anchor": "build_game_simulation",
            "planned_change": (
                "Accept or construct a pitching-plan payload before "
                "calling the game engine."
            ),
            "activation_in_6OZ": False,
        },
        {
            "integration_id": "PP-I02",
            "path": (
                "mlb_app/simulation/game_engine_v2.py"
            ),
            "anchor": "run_full_game_simulation",
            "planned_change": (
                "Pass plan classification into starter, bulk, and "
                "bullpen simulation state."
            ),
            "activation_in_6OZ": False,
        },
        {
            "integration_id": "PP-I03",
            "path": (
                "mlb_app/simulation/game_engine_v2.py"
            ),
            "anchor": "_expected_starter_innings",
            "planned_change": (
                "Use existing quality-based innings only as a "
                "traditional-starter fallback."
            ),
            "activation_in_6OZ": False,
        },
        {
            "integration_id": "PP-I04",
            "path": (
                "mlb_app/simulation/"
                "bullpen_simulation_path.py"
            ),
            "anchor": (
                "run_candidate_bullpen_simulation_path"
            ),
            "planned_change": (
                "Receive the post-primary pitching segment context "
                "without silently activating candidate sequencing."
            ),
            "activation_in_6OZ": False,
        },
        {
            "integration_id": "PP-I05",
            "path": "mlb_app/model_projections.py",
            "anchor": "projection_payload",
            "planned_change": (
                "Expose plan diagnostics only after backend contract "
                "and runtime audits pass."
            ),
            "activation_in_6OZ": False,
        },
    ]

    fixture_matrix = [
        {
            "fixture_id": "PP-F01",
            "scenario": "traditional_starter",
            "expected_plan_type": "traditional_starter",
            "required_assertion": (
                "Listed and primary pitcher match; fallback is "
                "clearly labeled when no announcement exists."
            ),
        },
        {
            "fixture_id": "PP-F02",
            "scenario": "verified_opener_and_bulk",
            "expected_plan_type": "opener_bulk",
            "required_assertion": (
                "Ordered opener and bulk identities are preserved."
            ),
        },
        {
            "fixture_id": "PP-F03",
            "scenario": "verified_tandem",
            "expected_plan_type": "tandem",
            "required_assertion": (
                "Primary and secondary planned segments are legal."
            ),
        },
        {
            "fixture_id": "PP-F04",
            "scenario": "bullpen_game",
            "expected_plan_type": "bullpen_game",
            "required_assertion": (
                "No traditional-starter assumption is silently used."
            ),
        },
        {
            "fixture_id": "PP-F05",
            "scenario": "workload_capped_starter",
            "expected_plan_type": "workload_capped_starter",
            "required_assertion": (
                "Cap type, value, and source are explicit."
            ),
        },
        {
            "fixture_id": "PP-F06",
            "scenario": "missing_information",
            "expected_plan_type": "unknown_fallback",
            "required_assertion": (
                "Fallback is deterministic and provenance is visible."
            ),
        },
        {
            "fixture_id": "PP-F07",
            "scenario": "contradictory_sources",
            "expected_plan_type": "unknown_fallback",
            "required_assertion": (
                "Contradiction is surfaced instead of guessed away."
            ),
        },
        {
            "fixture_id": "PP-F08",
            "scenario": "inactive_planned_bulk_pitcher",
            "expected_plan_type": "unknown_fallback",
            "required_assertion": (
                "Unavailable player is excluded from the sequence."
            ),
        },
    ]

    implementation_steps = [
        {
            "step": 1,
            "action": (
                "Create a pure pitching-plan classifier module."
            ),
            "required_result": (
                "No simulation or route changes."
            ),
        },
        {
            "step": 2,
            "action": (
                "Implement the approved taxonomy, inputs, decision "
                "rules, provenance, and deterministic fallbacks."
            ),
            "required_result": (
                "All unit fixtures pass."
            ),
        },
        {
            "step": 3,
            "action": (
                "Add plan payload support to the shared builder "
                "behind an explicit disabled-by-default mode."
            ),
            "required_result": (
                "Baseline behavior remains identical while disabled."
            ),
        },
        {
            "step": 4,
            "action": (
                "Wire plan payload into game-engine state without "
                "changing PA probabilities."
            ),
            "required_result": (
                "Plan is reachable and diagnostic-only."
            ),
        },
        {
            "step": 5,
            "action": (
                "Prove controlled classification differences for "
                "traditional, opener/bulk, tandem, bullpen-game, "
                "and capped-starter fixtures."
            ),
            "required_result": (
                "Classification changes while canonical probability "
                "authority remains unchanged."
            ),
        },
        {
            "step": 6,
            "action": (
                "Run an independent implementation audit."
            ),
            "required_result": (
                "Source, fixture, reachability, fallback, and safety "
                "contracts all pass."
            ),
        },
    ]

    stop_conditions = [
        {
            "condition_id": "PP-HOLD-01",
            "condition": (
                "No reliable source distinguishes listed starter "
                "from expected primary pitcher."
            ),
            "required_action": (
                "Use unknown_fallback; do not infer opener/bulk."
            ),
        },
        {
            "condition_id": "PP-HOLD-02",
            "condition": (
                "Planned pitcher identity is unavailable or inactive."
            ),
            "required_action": (
                "Remove the identity and emit fallback provenance."
            ),
        },
        {
            "condition_id": "PP-HOLD-03",
            "condition": (
                "Contradictory sources produce multiple plausible "
                "plan types."
            ),
            "required_action": (
                "Use unknown_fallback until conflict resolution "
                "is explicitly designed."
            ),
        },
        {
            "condition_id": "PP-HOLD-04",
            "condition": (
                "Classifier changes production probabilities before "
                "its behavior layer is authorized."
            ),
            "required_action": (
                "Disable integration and restore diagnostic-only mode."
            ),
        },
        {
            "condition_id": "PP-HOLD-05",
            "condition": (
                "Production route cannot prove exact plan payload "
                "reachability."
            ),
            "required_action": (
                "Hold activation and return to integration mapping."
            ),
        },
        {
            "condition_id": "PP-HOLD-06",
            "condition": (
                "Deterministic replay or fallback labeling fails."
            ),
            "required_action": (
                "Hold implementation and correct the classifier."
            ),
        },
    ]

    safety_rows = [
        {
            "boundary": action,
            "allowed_in_6OZ": False,
            "reason": (
                "6OZ inventories and plans GM-01 only."
            ),
        }
        for action in PROHIBITED_ACTIONS
    ]

    safety_rows.extend(
        [
            {
                "boundary": "source_inventory",
                "allowed_in_6OZ": True,
                "reason": (
                    "Exact current-state inventory is required."
                ),
            },
            {
                "boundary": "classifier_contract_definition",
                "allowed_in_6OZ": True,
                "reason": (
                    "Schema and decision-rule planning do not "
                    "change production behavior."
                ),
            },
            {
                "boundary": "fixture_plan_definition",
                "allowed_in_6OZ": True,
                "reason": (
                    "Fixture planning is non-behavioral."
                ),
            },
        ]
    )

    planning_checks = [
        {
            "check": "required_source_files_exist",
            "actual": sum(
                1
                for path in REQUIRED_PATHS
                if path.exists()
            ),
            "expected": len(REQUIRED_PATHS),
            "passed": required_paths_exist,
        },
        {
            "check": "6oy_predecessor_contract_present",
            "actual": predecessor_contract_present,
            "expected": True,
            "passed": predecessor_contract_present,
        },
        {
            "check": "current_shared_builder_inventory",
            "actual": shared_builder_present,
            "expected": True,
            "passed": shared_builder_present,
        },
        {
            "check": "current_starter_innings_inventory",
            "actual": starter_quality_present,
            "expected": True,
            "passed": starter_quality_present,
        },
        {
            "check": "current_starter_bullpen_split_inventory",
            "actual": starter_bullpen_split_present,
            "expected": True,
            "passed": starter_bullpen_split_present,
        },
        {
            "check": "candidate_bullpen_path_inventory",
            "actual": candidate_bullpen_path_present,
            "expected": True,
            "passed": candidate_bullpen_path_present,
        },
        {
            "check": "explicit_classifier_absent",
            "actual": explicit_plan_classifier_present,
            "expected": False,
            "passed": not explicit_plan_classifier_present,
        },
        {
            "check": "explicit_plan_taxonomy_absent",
            "actual": explicit_plan_types_present,
            "expected": False,
            "passed": not explicit_plan_types_present,
        },
        {
            "check": "explicit_sequence_plan_absent",
            "actual": explicit_pitching_sequence_present,
            "expected": False,
            "passed": not explicit_pitching_sequence_present,
        },
        {
            "check": "explicit_workload_cap_absent",
            "actual": explicit_workload_cap_present,
            "expected": False,
            "passed": not explicit_workload_cap_present,
        },
        {
            "check": "six_plan_types_defined",
            "actual": len(plan_types),
            "expected": 6,
            "passed": len(plan_types) == 6,
        },
        {
            "check": "implementation_plan_is_nonactivating",
            "actual": any(
                row["activation_in_6OZ"]
                for row in integration_points
            ),
            "expected": False,
            "passed": not any(
                row["activation_in_6OZ"]
                for row in integration_points
            ),
        },
    ]

    all_checks_passed = all(
        bool(row["passed"])
        for row in planning_checks
    )

    recommended_next_layer = (
        "6PA_pitching_plan_classification_implementation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        planning_checks,
    )

    write_csv(
        OUTPUT_DIR / "current_inventory.csv",
        [
            "component",
            "status",
            "production_verified",
            "evidence",
            "interpretation",
        ],
        current_inventory_rows,
    )

    write_csv(
        OUTPUT_DIR / "plan_types.csv",
        [
            "plan_type",
            "description",
            "minimum_required_roles",
            "ordered_sequence_required",
            "workload_cap_allowed",
            "fallback_allowed",
        ],
        plan_types,
    )

    write_csv(
        OUTPUT_DIR / "classification_inputs.csv",
        [
            "input",
            "required",
            "source_priority",
            "fallback",
            "purpose",
        ],
        classification_inputs,
    )

    write_csv(
        OUTPUT_DIR / "output_contract.csv",
        [
            "field",
            "type",
            "required",
            "description",
        ],
        output_contract,
    )

    write_csv(
        OUTPUT_DIR / "decision_rules.csv",
        [
            "rule_id",
            "priority",
            "condition",
            "plan_type",
            "confidence_band",
        ],
        decision_rules,
    )

    write_csv(
        OUTPUT_DIR / "integration_points.csv",
        [
            "integration_id",
            "path",
            "anchor",
            "planned_change",
            "activation_in_6OZ",
        ],
        integration_points,
    )

    write_csv(
        OUTPUT_DIR / "fixture_matrix.csv",
        [
            "fixture_id",
            "scenario",
            "expected_plan_type",
            "required_assertion",
        ],
        fixture_matrix,
    )

    write_csv(
        OUTPUT_DIR / "implementation_steps.csv",
        [
            "step",
            "action",
            "required_result",
        ],
        implementation_steps,
    )

    write_csv(
        OUTPUT_DIR / "stop_conditions.csv",
        [
            "condition_id",
            "condition",
            "required_action",
        ],
        stop_conditions,
    )

    write_csv(
        OUTPUT_DIR / "safety_boundaries.csv",
        [
            "boundary",
            "allowed_in_6OZ",
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
                    "Implement the pure disabled-by-default "
                    "pitching-plan classifier and deterministic "
                    "fixture contract."
                ),
                "entry_condition": (
                    "All 6OZ inventory and planning checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    inventory_summary = {
        "shared_builder_present": (
            shared_builder_present
        ),
        "starter_quality_present": (
            starter_quality_present
        ),
        "starter_bullpen_split_present": (
            starter_bullpen_split_present
        ),
        "candidate_bullpen_path_present": (
            candidate_bullpen_path_present
        ),
        "explicit_plan_classifier_present": (
            explicit_plan_classifier_present
        ),
        "explicit_plan_types_present": (
            explicit_plan_types_present
        ),
        "explicit_pitching_sequence_present": (
            explicit_pitching_sequence_present
        ),
        "explicit_workload_cap_present": (
            explicit_workload_cap_present
        ),
        "current_architecture_interpretation": (
            "quality_based_starter_innings_plus_aggregate_"
            "bullpen_with_optional_candidate_bullpen_path"
        ),
        "gm01_gap_confirmed": True,
        "new_authority_granted": False,
    }

    write_json(
        OUTPUT_DIR / "inventory_summary.json",
        inventory_summary,
    )

    implementation_plan = {
        "workstream_id": "GM-01",
        "domain": "pitching_plan_classification",
        "plan_types": [
            row["plan_type"]
            for row in plan_types
        ],
        "classification_inputs": [
            row["input"]
            for row in classification_inputs
        ],
        "required_output_fields": [
            row["field"]
            for row in output_contract
        ],
        "decision_rules": [
            row["rule_id"]
            for row in decision_rules
        ],
        "fixtures": [
            row["fixture_id"]
            for row in fixture_matrix
        ],
        "implementation_steps": len(
            implementation_steps
        ),
        "activation_default": "disabled",
        "diagnostic_only_until_audited": True,
        "canonical_probability_authority_changed": False,
    }

    write_json(
        OUTPUT_DIR / "implementation_plan.json",
        implementation_plan,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "pitching_plan_classification_"
            "inventory_and_implementation_plan_complete"
            if all_checks_passed
            else
            "pitching_plan_classification_"
            "inventory_and_implementation_plan_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "planning_checks_passed": sum(
            1
            for row in planning_checks
            if row["passed"]
        ),
        "planning_checks_required": len(
            planning_checks
        ),
        "inventory_components": len(
            current_inventory_rows
        ),
        "plan_types_defined": len(plan_types),
        "classification_inputs_defined": len(
            classification_inputs
        ),
        "output_fields_defined": len(
            output_contract
        ),
        "decision_rules_defined": len(
            decision_rules
        ),
        "integration_points_defined": len(
            integration_points
        ),
        "fixtures_planned": len(
            fixture_matrix
        ),
        "implementation_steps_planned": len(
            implementation_steps
        ),
        "stop_conditions_planned": len(
            stop_conditions
        ),
        "gm01_gap_confirmed": True,
        "production_classifier_present": (
            explicit_plan_classifier_present
        ),
        "production_classifier_activated": False,
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
        "classifier_implementation_allowed_next": (
            all_checks_passed
        ),
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "planning_checks.csv"
            ),
            str(
                OUTPUT_DIR / "current_inventory.csv"
            ),
            str(OUTPUT_DIR / "plan_types.csv"),
            str(
                OUTPUT_DIR / "classification_inputs.csv"
            ),
            str(OUTPUT_DIR / "output_contract.csv"),
            str(OUTPUT_DIR / "decision_rules.csv"),
            str(
                OUTPUT_DIR / "integration_points.csv"
            ),
            str(OUTPUT_DIR / "fixture_matrix.csv"),
            str(
                OUTPUT_DIR / "implementation_steps.csv"
            ),
            str(OUTPUT_DIR / "stop_conditions.csv"),
            str(
                OUTPUT_DIR / "safety_boundaries.csv"
            ),
            str(OUTPUT_DIR / "recommended_path.csv"),
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
