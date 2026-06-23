#!/usr/bin/env python3
"""
Layer 6QD
Position-Player Substitution Inventory and Implementation Plan

Inventories the current lineup, roster, batting-order, base-state, and
simulation architecture and defines a safe deterministic implementation path
for GM-05 position-player substitutions.

This layer does not:

- replace a batter in production;
- replace a baserunner in production;
- change a defensive alignment in production;
- change batting order or lineup slots;
- apply injury substitutions;
- apply double switches;
- change base/out state;
- change plate-appearance probabilities;
- change simulation scores or win probabilities;
- activate candidate substitution behavior;
- authorize historical validation, tuning, backtesting, pricing, or edge
  detection.
"""

from __future__ import annotations

import ast
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6QD"

LAYER_NAME = (
    "position_player_substitution_"
    "inventory_and_implementation_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6QD_position_player_substitution_"
    "inventory_and_implementation_plan"
)

COMPLETION_PATH = (
    ROOT
    / "scripts/assess_6QC_stolen_base_and_"
    "pickoff_state_diagnostic_scope_completion.py"
)

SIMULATION_ROOT = (
    ROOT
    / "mlb_app/simulation"
)

INNING_SIMULATOR_PATH = (
    SIMULATION_ROOT
    / "inning_simulator.py"
)

ENGINE_PATH = (
    SIMULATION_ROOT
    / "game_engine_v2.py"
)

SIMULATOR_PATH = (
    SIMULATION_ROOT
    / "game_simulator.py"
)

BUILDER_PATH = (
    SIMULATION_ROOT
    / "game_simulation_builder.py"
)

MODEL_PROJECTIONS_PATH = (
    ROOT
    / "mlb_app/model_projections.py"
)

SEARCH_TERMS = [
    "substitution",
    "substitute",
    "pinch hitter",
    "pinch_hitter",
    "pinch hit",
    "pinch runner",
    "pinch_runner",
    "defensive replacement",
    "defensive_replacement",
    "injury replacement",
    "injury_replacement",
    "double switch",
    "double_switch",
    "designated hitter",
    "designated_hitter",
    "batting order",
    "batting_order",
    "lineup slot",
    "lineup_slot",
    "lineup",
    "roster",
    "bench",
    "fielding position",
    "fielding_position",
    "position player",
    "position_player",
    "runner",
    "batter",
]

SUBSTITUTION_TYPES = [
    {
        "substitution_type": "pinch_hitter",
        "purpose": (
            "Evaluate replacing the scheduled batter with an eligible "
            "bench hitter."
        ),
    },
    {
        "substitution_type": "pinch_runner",
        "purpose": (
            "Evaluate replacing an identified baserunner with an eligible "
            "bench runner."
        ),
    },
    {
        "substitution_type": "defensive_replacement",
        "purpose": (
            "Evaluate a fielding replacement without applying a defensive "
            "alignment change."
        ),
    },
    {
        "substitution_type": "injury_replacement",
        "purpose": (
            "Represent a required replacement path while retaining "
            "conservative fallback behavior."
        ),
    },
    {
        "substitution_type": "double_switch_or_lineup_reassignment",
        "purpose": (
            "Represent lineup-slot and defensive-position implications "
            "without changing the production batting order."
        ),
    },
]

CANDIDATE_COMPONENTS = [
    {
        "component": "substitution_state_container",
        "purpose": (
            "Represent inning, half, outs, score, lineup, base state, current "
            "participants, and available bench."
        ),
    },
    {
        "component": "substitution_type_classifier",
        "purpose": (
            "Identify pinch-hit, pinch-run, defensive, injury, or lineup "
            "reassignment evaluation mode."
        ),
    },
    {
        "component": "candidate_eligibility_filter",
        "purpose": (
            "Reject unavailable, already-used, ineligible, or position-"
            "incompatible candidates."
        ),
    },
    {
        "component": "pinch_hit_evaluator",
        "purpose": (
            "Compare candidate offensive fit with the scheduled batter "
            "without changing plate-appearance probabilities."
        ),
    },
    {
        "component": "pinch_run_evaluator",
        "purpose": (
            "Compare candidate running fit with the current runner without "
            "changing base state."
        ),
    },
    {
        "component": "defensive_replacement_evaluator",
        "purpose": (
            "Compare candidate defensive fit without changing fielding "
            "alignment or run prevention."
        ),
    },
    {
        "component": "injury_substitution_guard",
        "purpose": (
            "Represent mandatory replacement context separately from "
            "strategic substitution context."
        ),
    },
    {
        "component": "lineup_slot_constraint_evaluator",
        "purpose": (
            "Validate batting-order, designated-hitter, and lineup-slot "
            "constraints without applying them."
        ),
    },
    {
        "component": "reentry_and_usage_guard",
        "purpose": (
            "Prevent reuse of removed or previously used players where "
            "applicable."
        ),
    },
    {
        "component": "default_equivalence_guardrail",
        "purpose": (
            "Guarantee no simulation change when diagnostics are absent or "
            "disabled."
        ),
    },
    {
        "component": "diagnostic_metadata_adapter",
        "purpose": (
            "Expose evaluator output as metadata only before any behavioral "
            "integration is considered."
        ),
    },
    {
        "component": "promotion_guardrails",
        "purpose": (
            "Require independent audit, exact equivalence, historical "
            "validation, and explicit authorization before activation."
        ),
    },
]

STATE_FIELDS = [
    {
        "field": "inning",
        "required": True,
        "type": "integer",
        "purpose": "Current inning.",
    },
    {
        "field": "half",
        "required": True,
        "type": "string",
        "purpose": "Top or bottom half.",
    },
    {
        "field": "outs",
        "required": True,
        "type": "integer",
        "purpose": "Current outs.",
    },
    {
        "field": "score_margin",
        "required": True,
        "type": "integer",
        "purpose": "Offense score minus defense score.",
    },
    {
        "field": "base_state",
        "required": True,
        "type": "object",
        "purpose": "Occupied bases and identified runners.",
    },
    {
        "field": "substitution_type",
        "required": True,
        "type": "string",
        "purpose": "Requested candidate substitution category.",
    },
    {
        "field": "current_player",
        "required": True,
        "type": "object",
        "purpose": "Player who would leave the game or role.",
    },
    {
        "field": "candidate_players",
        "required": True,
        "type": "array",
        "purpose": "Eligible or potentially eligible bench candidates.",
    },
    {
        "field": "batting_order",
        "required": True,
        "type": "array",
        "purpose": "Current batting-order representation.",
    },
    {
        "field": "current_lineup_slot",
        "required": True,
        "type": "integer",
        "purpose": "Affected batting-order slot.",
    },
    {
        "field": "defensive_alignment",
        "required": False,
        "type": "object",
        "purpose": "Current defensive-position assignments.",
    },
    {
        "field": "used_player_ids",
        "required": False,
        "type": "array",
        "purpose": "Players already used or removed.",
    },
    {
        "field": "designated_hitter_active",
        "required": False,
        "type": "boolean",
        "purpose": "Current designated-hitter rule state.",
    },
    {
        "field": "injury_required",
        "required": False,
        "type": "boolean",
        "purpose": "Whether replacement is mandatory.",
    },
    {
        "field": "evidence_version",
        "required": False,
        "type": "string|null",
        "purpose": "Input evidence provenance.",
    },
]

PLAYER_FIELDS = [
    {
        "field": "player_id",
        "required": True,
        "purpose": "Stable player identity.",
    },
    {
        "field": "active",
        "required": True,
        "purpose": "Current active-roster availability.",
    },
    {
        "field": "already_used",
        "required": True,
        "purpose": "Whether the player has already appeared or been removed.",
    },
    {
        "field": "primary_position",
        "required": False,
        "purpose": "Primary defensive position.",
    },
    {
        "field": "eligible_positions",
        "required": False,
        "purpose": "Permitted defensive positions.",
    },
    {
        "field": "bats",
        "required": False,
        "purpose": "Batting handedness.",
    },
    {
        "field": "offense_score",
        "required": False,
        "purpose": "Normalized offensive evidence.",
    },
    {
        "field": "running_score",
        "required": False,
        "purpose": "Normalized baserunning evidence.",
    },
    {
        "field": "defense_score",
        "required": False,
        "purpose": "Normalized defensive evidence.",
    },
    {
        "field": "evidence_complete",
        "required": True,
        "purpose": "Whether minimum player evidence is verified.",
    },
]

OUTPUT_FIELDS = [
    {
        "field": "substitution_eligible",
        "purpose": "Whether the candidate substitution can be evaluated.",
    },
    {
        "field": "recommended_action",
        "purpose": "substitute, retain, required_replacement, or fallback.",
    },
    {
        "field": "recommended_player_id",
        "purpose": "Selected candidate identity or null.",
    },
    {
        "field": "substitution_type",
        "purpose": "Evaluated substitution category.",
    },
    {
        "field": "candidate_score",
        "purpose": "Deterministic candidate suitability score.",
    },
    {
        "field": "current_player_score",
        "purpose": "Deterministic current-player suitability score.",
    },
    {
        "field": "selection_reason",
        "purpose": "Deterministic explanation.",
    },
    {
        "field": "lineup_constraint_valid",
        "purpose": "Whether modeled lineup constraints are satisfied.",
    },
    {
        "field": "fallback_used",
        "purpose": "Whether conservative fallback was required.",
    },
    {
        "field": "fallback_reason",
        "purpose": "Why fallback was required.",
    },
    {
        "field": "state_completeness",
        "purpose": "complete, partial, or invalid.",
    },
    {
        "field": "behavioral_effect",
        "purpose": "Always none in evaluator and diagnostic scope.",
    },
    {
        "field": "production_activation",
        "purpose": "Always false.",
    },
]

FIXTURES = [
    {
        "fixture_id": "QD-F01",
        "scenario": "pinch_hitter_complete_evidence",
        "expected": "eligible_deterministic_candidate_evaluation",
    },
    {
        "fixture_id": "QD-F02",
        "scenario": "pinch_runner_complete_evidence",
        "expected": "eligible_deterministic_candidate_evaluation",
    },
    {
        "fixture_id": "QD-F03",
        "scenario": "defensive_replacement_complete_evidence",
        "expected": "eligible_deterministic_candidate_evaluation",
    },
    {
        "fixture_id": "QD-F04",
        "scenario": "mandatory_injury_replacement",
        "expected": "required_replacement_path",
    },
    {
        "fixture_id": "QD-F05",
        "scenario": "candidate_already_used",
        "expected": "candidate_ineligible",
    },
    {
        "fixture_id": "QD-F06",
        "scenario": "candidate_position_incompatible",
        "expected": "candidate_ineligible",
    },
    {
        "fixture_id": "QD-F07",
        "scenario": "designated_hitter_constraint",
        "expected": "lineup_constraint_evaluation",
    },
    {
        "fixture_id": "QD-F08",
        "scenario": "partial_candidate_evidence",
        "expected": "partial_state_conservative_fallback",
    },
    {
        "fixture_id": "QD-F09",
        "scenario": "invalid_lineup_state",
        "expected": "invalid_state_no_activation",
    },
    {
        "fixture_id": "QD-F10",
        "scenario": "input_immutability_and_repeatability",
        "expected": "identical_output_and_unchanged_input",
    },
]

PROHIBITED_AUTHORITIES = [
    "production_pinch_hit_activation",
    "production_pinch_run_activation",
    "production_defensive_replacement_activation",
    "production_injury_replacement_activation",
    "production_double_switch_activation",
    "batting_order_change",
    "lineup_slot_change",
    "defensive_alignment_change",
    "designated_hitter_state_change",
    "base_state_transition_change",
    "out_state_transition_change",
    "runner_identity_change",
    "plate_appearance_probability_change",
    "simulation_parameter_change",
    "simulation_score_change",
    "win_probability_change",
    "canonical_probability_replacement",
    "public_api_dependency",
    "frontend_dependency",
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
        json.dumps(
            payload,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def read_text(
    path: Path,
) -> str:
    if not path.exists():
        return ""

    return path.read_text(
        encoding="utf-8",
        errors="ignore",
    )


def parse_last_json_object(
    text: str,
) -> dict[str, Any]:
    positions = [
        index
        for index, character in enumerate(text)
        if character == "{"
    ]

    for index in reversed(
        positions
    ):
        try:
            payload = json.loads(
                text[index:].strip()
            )
        except json.JSONDecodeError:
            continue

        if isinstance(
            payload,
            dict,
        ):
            return payload

    return {}


def relative(
    path: Path,
) -> str:
    try:
        return str(
            path.relative_to(
                ROOT
            )
        )
    except ValueError:
        return str(
            path
        )


def python_files() -> list[Path]:
    roots = [
        ROOT / "mlb_app",
        ROOT / "scripts",
    ]

    found: list[Path] = []

    for root in roots:
        if not root.exists():
            continue

        found.extend(
            sorted(
                root.rglob(
                    "*.py"
                )
            )
        )

    return found


def inventory_references() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for path in python_files():
        text = read_text(
            path
        )

        lowered = text.lower()

        matched_terms = sorted(
            {
                term
                for term
                in SEARCH_TERMS
                if term.lower()
                in lowered
            }
        )

        if not matched_terms:
            continue

        rows.append(
            {
                "path": relative(
                    path
                ),
                "matched_term_count": len(
                    matched_terms
                ),
                "matched_terms": "|".join(
                    matched_terms
                ),
                "is_simulation_runtime": (
                    relative(
                        path
                    ).startswith(
                        "mlb_app/simulation/"
                    )
                ),
                "is_script": (
                    relative(
                        path
                    ).startswith(
                        "scripts/"
                    )
                ),
            }
        )

    return rows


def function_inventory(
    path: Path,
) -> list[dict[str, Any]]:
    text = read_text(
        path
    )

    if not text:
        return []

    tree = ast.parse(
        text,
        filename=str(
            path
        ),
    )

    rows: list[dict[str, Any]] = []

    for node in ast.walk(
        tree
    ):
        if not isinstance(
            node,
            (
                ast.FunctionDef,
                ast.AsyncFunctionDef,
            ),
        ):
            continue

        source = (
            ast.get_source_segment(
                text,
                node,
            )
            or ""
        )

        lowered = source.lower()

        matched_terms = sorted(
            {
                term
                for term
                in SEARCH_TERMS
                if term.lower()
                in lowered
            }
        )

        if not matched_terms:
            continue

        rows.append(
            {
                "path": relative(
                    path
                ),
                "function": node.name,
                "line": node.lineno,
                "matched_terms": "|".join(
                    matched_terms
                ),
            }
        )

    return rows


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    required_files = [
        COMPLETION_PATH,
        INNING_SIMULATOR_PATH,
        ENGINE_PATH,
        SIMULATOR_PATH,
        BUILDER_PATH,
        MODEL_PROJECTIONS_PATH,
    ]

    required_files_exist = all(
        path.exists()
        for path in required_files
    )

    completion_run = subprocess.run(
        [
            sys.executable,
            str(
                COMPLETION_PATH
            ),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    completion_payload = (
        parse_last_json_object(
            completion_run.stdout
        )
    )

    six_qc_contract_passed = all(
        [
            completion_run.returncode
            == 0,
            completion_payload.get(
                "diagnosis"
            )
            == (
                "stolen_base_and_pickoff_state_"
                "diagnostic_scope_complete"
            ),
            completion_payload.get(
                "all_checks_passed"
            )
            is True,
            completion_payload.get(
                "completion_checks_passed"
            )
            == 13,
            completion_payload.get(
                "completion_checks_required"
            )
            == 13,
            completion_payload.get(
                "predecessors_accepted"
            )
            == 6,
            completion_payload.get(
                "gm04_diagnostic_scope_complete"
            )
            is True,
            completion_payload.get(
                "gm05_inventory_and_implementation_"
                "planning_allowed_next"
            )
            is True,
            completion_payload.get(
                "production_behavior_"
                "integration_allowed_next"
            )
            is False,
        ]
    )

    reference_rows = (
        inventory_references()
    )

    function_rows: list[
        dict[str, Any]
    ] = []

    for path in [
        INNING_SIMULATOR_PATH,
        ENGINE_PATH,
        SIMULATOR_PATH,
        BUILDER_PATH,
        MODEL_PROJECTIONS_PATH,
    ]:
        function_rows.extend(
            function_inventory(
                path
            )
        )

    runtime_text = "\n".join(
        [
            read_text(
                INNING_SIMULATOR_PATH
            ),
            read_text(
                ENGINE_PATH
            ),
            read_text(
                SIMULATOR_PATH
            ),
            read_text(
                BUILDER_PATH
            ),
        ]
    )

    production_activation_tokens = [
        "apply_pinch_hitter(",
        "apply_pinch_runner(",
        "apply_defensive_replacement(",
        "apply_injury_replacement(",
        "apply_double_switch(",
        "replace_lineup_slot(",
        "replace_baserunner(",
        "activate_position_player_substitution(",
    ]

    production_activation_absent = not any(
        token in runtime_text
        for token
        in production_activation_tokens
    )

    candidate_component_names = {
        row[
            "component"
        ]
        for row
        in CANDIDATE_COMPONENTS
    }

    default_guardrail_planned = (
        "default_equivalence_guardrail"
        in candidate_component_names
    )

    metadata_adapter_planned = (
        "diagnostic_metadata_adapter"
        in candidate_component_names
    )

    promotion_guardrails_planned = (
        "promotion_guardrails"
        in candidate_component_names
    )

    checks = [
        {
            "check": "required_files_exist",
            "actual": required_files_exist,
            "expected": True,
            "passed": required_files_exist,
        },
        {
            "check": (
                "six_qc_completion_contract_passed"
            ),
            "actual": (
                six_qc_contract_passed
            ),
            "expected": True,
            "passed": (
                six_qc_contract_passed
            ),
        },
        {
            "check": (
                "repository_inventory_completed"
            ),
            "actual": len(
                python_files()
            )
            > 0,
            "expected": True,
            "passed": len(
                python_files()
            )
            > 0,
        },
        {
            "check": (
                "five_substitution_types_planned"
            ),
            "actual": len(
                SUBSTITUTION_TYPES
            ),
            "expected": 5,
            "passed": len(
                SUBSTITUTION_TYPES
            )
            == 5,
        },
        {
            "check": (
                "twelve_candidate_components_planned"
            ),
            "actual": len(
                CANDIDATE_COMPONENTS
            ),
            "expected": 12,
            "passed": len(
                CANDIDATE_COMPONENTS
            )
            == 12,
        },
        {
            "check": (
                "fifteen_state_fields_planned"
            ),
            "actual": len(
                STATE_FIELDS
            ),
            "expected": 15,
            "passed": len(
                STATE_FIELDS
            )
            == 15,
        },
        {
            "check": (
                "ten_player_fields_planned"
            ),
            "actual": len(
                PLAYER_FIELDS
            ),
            "expected": 10,
            "passed": len(
                PLAYER_FIELDS
            )
            == 10,
        },
        {
            "check": (
                "thirteen_output_fields_planned"
            ),
            "actual": len(
                OUTPUT_FIELDS
            ),
            "expected": 13,
            "passed": len(
                OUTPUT_FIELDS
            )
            == 13,
        },
        {
            "check": (
                "ten_fixtures_planned"
            ),
            "actual": len(
                FIXTURES
            ),
            "expected": 10,
            "passed": len(
                FIXTURES
            )
            == 10,
        },
        {
            "check": (
                "default_equivalence_guardrail_planned"
            ),
            "actual": (
                default_guardrail_planned
            ),
            "expected": True,
            "passed": (
                default_guardrail_planned
            ),
        },
        {
            "check": (
                "diagnostic_metadata_adapter_planned"
            ),
            "actual": (
                metadata_adapter_planned
            ),
            "expected": True,
            "passed": (
                metadata_adapter_planned
            ),
        },
        {
            "check": (
                "promotion_guardrails_planned"
            ),
            "actual": (
                promotion_guardrails_planned
            ),
            "expected": True,
            "passed": (
                promotion_guardrails_planned
            ),
        },
        {
            "check": (
                "production_activation_absent"
            ),
            "actual": (
                production_activation_absent
            ),
            "expected": True,
            "passed": (
                production_activation_absent
            ),
        },
        {
            "check": (
                "evaluator_implementation_allowed"
            ),
            "actual": all(
                [
                    six_qc_contract_passed,
                    required_files_exist,
                    production_activation_absent,
                    default_guardrail_planned,
                    metadata_adapter_planned,
                    promotion_guardrails_planned,
                ]
            ),
            "expected": True,
            "passed": all(
                [
                    six_qc_contract_passed,
                    required_files_exist,
                    production_activation_absent,
                    default_guardrail_planned,
                    metadata_adapter_planned,
                    promotion_guardrails_planned,
                ]
            ),
        },
    ]

    all_checks_passed = all(
        row[
            "passed"
        ]
        for row
        in checks
    )

    authority_rows = [
        {
            "authority": authority,
            "granted": False,
            "reason": (
                "6QD is an inventory and "
                "implementation-planning layer only."
            ),
        }
        for authority
        in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "pure_deterministic_"
                    "substitution_evaluator_implementation"
                ),
                "granted": (
                    all_checks_passed
                ),
                "reason": (
                    "Only an isolated evaluator with "
                    "no production references may be built."
                ),
            },
            {
                "authority": (
                    "production_behavior_integration"
                ),
                "granted": False,
                "reason": (
                    "Evaluation, independent audit, and "
                    "diagnostic integration remain required."
                ),
            },
        ]
    )

    recommended_next_layer = (
        "6QE_position_player_substitution_"
        "state_contract_and_evaluator_implementation"
        if all_checks_passed
        else
        "6QE_position_player_substitution_"
        "inventory_plan_remediation"
    )

    diagnosis_name = (
        "position_player_substitution_"
        "inventory_and_implementation_plan_complete"
        if all_checks_passed
        else
        "position_player_substitution_"
        "inventory_and_implementation_plan_failed"
    )

    write_csv(
        OUTPUT_DIR
        / "planning_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR
        / "repository_reference_inventory.csv",
        [
            "path",
            "matched_term_count",
            "matched_terms",
            "is_simulation_runtime",
            "is_script",
        ],
        reference_rows,
    )

    write_csv(
        OUTPUT_DIR
        / "function_inventory.csv",
        [
            "path",
            "function",
            "line",
            "matched_terms",
        ],
        function_rows,
    )

    write_csv(
        OUTPUT_DIR
        / "substitution_types.csv",
        [
            "substitution_type",
            "purpose",
        ],
        SUBSTITUTION_TYPES,
    )

    write_csv(
        OUTPUT_DIR
        / "candidate_components.csv",
        [
            "component",
            "purpose",
        ],
        CANDIDATE_COMPONENTS,
    )

    write_csv(
        OUTPUT_DIR
        / "state_contract.csv",
        [
            "field",
            "required",
            "type",
            "purpose",
        ],
        STATE_FIELDS,
    )

    write_csv(
        OUTPUT_DIR
        / "player_contract.csv",
        [
            "field",
            "required",
            "purpose",
        ],
        PLAYER_FIELDS,
    )

    write_csv(
        OUTPUT_DIR
        / "output_contract.csv",
        [
            "field",
            "purpose",
        ],
        OUTPUT_FIELDS,
    )

    write_csv(
        OUTPUT_DIR
        / "fixture_plan.csv",
        [
            "fixture_id",
            "scenario",
            "expected",
        ],
        FIXTURES,
    )

    write_csv(
        OUTPUT_DIR
        / "authority_boundaries.csv",
        [
            "authority",
            "granted",
            "reason",
        ],
        authority_rows,
    )

    implementation_plan = {
        "target_module": (
            "mlb_app/simulation/"
            "position_player_substitution_evaluator.py"
        ),
        "evaluator_function": (
            "evaluate_position_player_substitution"
        ),
        "validator_function": (
            "validate_position_player_"
            "substitution_evaluation"
        ),
        "input_contract": {
            "state_fields": [
                row[
                    "field"
                ]
                for row
                in STATE_FIELDS
            ],
            "player_fields": [
                row[
                    "field"
                ]
                for row
                in PLAYER_FIELDS
            ],
        },
        "output_fields": [
            row[
                "field"
            ]
            for row
            in OUTPUT_FIELDS
        ],
        "substitution_types": [
            row[
                "substitution_type"
            ]
            for row
            in SUBSTITUTION_TYPES
        ],
        "pure_function": True,
        "deterministic": True,
        "input_mutation_allowed": False,
        "simulation_imports_allowed": False,
        "production_references_allowed": False,
        "behavioral_effect": "none",
        "production_activation": False,
    }

    write_json(
        OUTPUT_DIR
        / "implementation_plan.json",
        implementation_plan,
    )

    summary = {
        "planning_checks_required": len(
            checks
        ),
        "planning_checks_passed": sum(
            1
            for row
            in checks
            if row[
                "passed"
            ]
        ),
        "six_qc_completion_contract_passed": (
            six_qc_contract_passed
        ),
        "repository_reference_files": len(
            reference_rows
        ),
        "inventory_functions_found": len(
            function_rows
        ),
        "substitution_types_planned": len(
            SUBSTITUTION_TYPES
        ),
        "candidate_components_planned": len(
            CANDIDATE_COMPONENTS
        ),
        "state_fields_planned": len(
            STATE_FIELDS
        ),
        "player_fields_planned": len(
            PLAYER_FIELDS
        ),
        "output_fields_planned": len(
            OUTPUT_FIELDS
        ),
        "fixtures_planned": len(
            FIXTURES
        ),
        "production_activation_absent": (
            production_activation_absent
        ),
        "evaluator_implementation_allowed": (
            all_checks_passed
        ),
        "production_behavior_changed": False,
        "batting_order_changed": False,
        "defensive_alignment_changed": False,
        "base_out_state_changed": False,
        "simulation_behavior_changed": False,
        (
            "canonical_probability_"
            "authority_changed"
        ): False,
        "production_activation": False,
    }

    write_json(
        OUTPUT_DIR
        / "planning_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            diagnosis_name
        ),
        "all_checks_passed": (
            all_checks_passed
        ),
        "planning_checks_passed": sum(
            1
            for row
            in checks
            if row[
                "passed"
            ]
        ),
        "planning_checks_required": len(
            checks
        ),
        "six_qc_completion_contract_passed": (
            six_qc_contract_passed
        ),
        "repository_reference_files": len(
            reference_rows
        ),
        "inventory_functions_found": len(
            function_rows
        ),
        "substitution_types_planned": len(
            SUBSTITUTION_TYPES
        ),
        "candidate_components_planned": len(
            CANDIDATE_COMPONENTS
        ),
        "state_fields_planned": len(
            STATE_FIELDS
        ),
        "player_fields_planned": len(
            PLAYER_FIELDS
        ),
        "output_fields_planned": len(
            OUTPUT_FIELDS
        ),
        "fixtures_planned": len(
            FIXTURES
        ),
        "production_activation_absent": (
            production_activation_absent
        ),
        "production_substitutions_changed": False,
        "batting_order_changed": False,
        "defensive_alignment_changed": False,
        "base_out_state_changed": False,
        "simulation_behavior_changed": False,
        (
            "canonical_probability_"
            "authority_changed"
        ): False,
        "production_activation": False,
        "broad_layer6_exit_paused": True,
        "layer6_exit_recommended": False,
        "layer6_exit_finalized": False,
        "new_authority_granted": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        (
            "pure_evaluator_implementation_"
            "allowed_next"
        ): (
            all_checks_passed
        ),
        (
            "production_behavior_"
            "integration_allowed_next"
        ): False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR
                / "planning_checks.csv"
            ),
            str(
                OUTPUT_DIR
                / "repository_reference_inventory.csv"
            ),
            str(
                OUTPUT_DIR
                / "function_inventory.csv"
            ),
            str(
                OUTPUT_DIR
                / "substitution_types.csv"
            ),
            str(
                OUTPUT_DIR
                / "candidate_components.csv"
            ),
            str(
                OUTPUT_DIR
                / "state_contract.csv"
            ),
            str(
                OUTPUT_DIR
                / "player_contract.csv"
            ),
            str(
                OUTPUT_DIR
                / "output_contract.csv"
            ),
            str(
                OUTPUT_DIR
                / "fixture_plan.csv"
            ),
            str(
                OUTPUT_DIR
                / "authority_boundaries.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "implementation_plan.json"
            ),
            str(
                OUTPUT_DIR
                / "planning_summary.json"
            ),
            str(
                OUTPUT_DIR
                / "diagnosis.json"
            ),
        ],
    }

    write_json(
        OUTPUT_DIR
        / "diagnosis.json",
        diagnosis,
    )

    print(
        json.dumps(
            diagnosis,
            indent=2,
        )
    )

    return (
        0
        if all_checks_passed
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
