#!/usr/bin/env python3
"""
Layer 6PE
Pitching-Plan Classification Diagnostic Integration Plan

Plans disabled-by-default, diagnostics-only integration of the approved
GM-01 pitching-plan classifier into the shared simulation route.

This layer does not modify production code, activate the classifier,
change simulation behavior, or change canonical probability authority.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6PE"
LAYER_NAME = (
    "pitching_plan_classification_"
    "diagnostic_integration_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PE_pitching_plan_classification_"
    "diagnostic_integration_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/audit_6PD_pitching_plan_"
    "classification_post_remediation.py"
)

CLASSIFIER_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "pitching_plan_classifier.py"
)

SHARED_BUILDER_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "game_simulation_builder.py"
)

GAME_ENGINE_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "game_engine_v2.py"
)

MODEL_PROJECTIONS_PATH = (
    ROOT / "mlb_app/model_projections.py"
)

APP_PATH = ROOT / "mlb_app/app.py"

REQUIRED_PATHS = [
    PREDECESSOR_PATH,
    CLASSIFIER_PATH,
    SHARED_BUILDER_PATH,
    GAME_ENGINE_PATH,
    MODEL_PROJECTIONS_PATH,
    APP_PATH,
]

PROHIBITED_ACTIONS = [
    "production_route_change",
    "production_classifier_activation",
    "classifier_behavior_change",
    "starter_innings_change",
    "dynamic_starter_hook_change",
    "bullpen_sequence_change",
    "plate_appearance_probability_change",
    "simulation_parameter_change",
    "simulation_score_change",
    "win_probability_change",
    "canonical_probability_replacement",
    "backend_response_change",
    "frontend_behavior_change",
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


def function_names(path: Path) -> set[str]:
    if not path.exists():
        return set()

    tree = ast.parse(
        read_text(path),
        filename=str(path),
    )

    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(
            node,
            (
                ast.FunctionDef,
                ast.AsyncFunctionDef,
            ),
        )
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
                "pitching_plan_classification_"
                "post_remediation_audit_passed"
            ),
            (
                "diagnostic_integration_"
                "planning_allowed_next"
            ),
            (
                "production_integration_"
                "allowed_next"
            ),
            (
                "6PE_pitching_plan_classification_"
                "diagnostic_integration_plan"
            ),
        ]
    )

    classifier_functions = function_names(
        CLASSIFIER_PATH
    )

    classifier_contract_present = {
        "classify_pitching_plan",
        "validate_pitching_plan_payload",
    }.issubset(classifier_functions)

    builder_text = read_text(
        SHARED_BUILDER_PATH
    )

    engine_text = read_text(
        GAME_ENGINE_PATH
    )

    builder_contract_present = all(
        token in builder_text
        for token in [
            "build_game_simulation",
            "_normalize_metadata",
            "config",
            "meta",
            "metadata",
        ]
    )

    engine_contract_present = all(
        token in engine_text
        for token in [
            "run_full_game_simulation",
            "_expected_starter_innings",
            "simulate_game_with_bullpen",
            "dynamic_starter_exit",
        ]
    )

    current_production_reference_count = 0

    production_reference_rows: list[
        dict[str, Any]
    ] = []

    for path in sorted(
        (ROOT / "mlb_app").rglob("*.py")
    ):
        if path == CLASSIFIER_PATH:
            continue

        text = read_text(path)

        module_reference = (
            "pitching_plan_classifier"
            in text
        )

        function_reference = (
            "classify_pitching_plan"
            in text
        )

        if not (
            module_reference
            or function_reference
        ):
            continue

        current_production_reference_count += 1

        production_reference_rows.append(
            {
                "path": str(
                    path.relative_to(ROOT)
                ),
                "module_reference": (
                    module_reference
                ),
                "function_reference": (
                    function_reference
                ),
            }
        )

    currently_unwired = (
        current_production_reference_count == 0
    )

    integration_contract = [
        {
            "contract_id": "DI-C01",
            "contract": (
                "Configuration switch is explicit and "
                "disabled by default."
            ),
            "required_value": (
                "pitching_plan_diagnostics_enabled=False"
            ),
            "behavioral_effect_allowed": False,
        },
        {
            "contract_id": "DI-C02",
            "contract": (
                "Classifier is not imported or called when "
                "diagnostic mode is disabled."
            ),
            "required_value": (
                "zero disabled-path classifier calls"
            ),
            "behavioral_effect_allowed": False,
        },
        {
            "contract_id": "DI-C03",
            "contract": (
                "Disabled-path output is exactly equivalent "
                "to the pre-integration baseline."
            ),
            "required_value": (
                "deep equality after removing no fields"
            ),
            "behavioral_effect_allowed": False,
        },
        {
            "contract_id": "DI-C04",
            "contract": (
                "Enabled mode may append diagnostic metadata "
                "only."
            ),
            "required_value": (
                "metadata.pitching_plan_diagnostics"
            ),
            "behavioral_effect_allowed": False,
        },
        {
            "contract_id": "DI-C05",
            "contract": (
                "Diagnostic classification cannot alter "
                "engine inputs."
            ),
            "required_value": (
                "no mutation of config, matchup, pitcher "
                "profiles, PA models, or simulation arguments"
            ),
            "behavioral_effect_allowed": False,
        },
        {
            "contract_id": "DI-C06",
            "contract": (
                "Diagnostic classification cannot alter "
                "simulation outputs."
            ),
            "required_value": (
                "scores, run distributions, totals, and win "
                "probabilities unchanged"
            ),
            "behavioral_effect_allowed": False,
        },
        {
            "contract_id": "DI-C07",
            "contract": (
                "Classifier failure must not fail the "
                "simulation route."
            ),
            "required_value": (
                "diagnostic error payload with baseline "
                "simulation preserved"
            ),
            "behavioral_effect_allowed": False,
        },
        {
            "contract_id": "DI-C08",
            "contract": (
                "Diagnostic output must retain source and "
                "fallback provenance."
            ),
            "required_value": (
                "source_status, source_provenance, "
                "fallback_used, diagnostics"
            ),
            "behavioral_effect_allowed": False,
        },
        {
            "contract_id": "DI-C09",
            "contract": (
                "Production activation remains explicitly "
                "false."
            ),
            "required_value": (
                "production_activation=False"
            ),
            "behavioral_effect_allowed": False,
        },
        {
            "contract_id": "DI-C10",
            "contract": (
                "Canonical probability authority remains "
                "unchanged."
            ),
            "required_value": (
                "canonical_probability_authority_changed=False"
            ),
            "behavioral_effect_allowed": False,
        },
    ]

    configuration_contract = [
        {
            "field": (
                "pitching_plan_diagnostics_enabled"
            ),
            "type": "boolean",
            "default": False,
            "required": True,
            "purpose": (
                "Explicitly controls diagnostic-only "
                "classification."
            ),
        },
        {
            "field": (
                "pitching_plan_evidence"
            ),
            "type": "nullable_mapping",
            "default": None,
            "required": False,
            "purpose": (
                "Carries explicit pregame classification "
                "evidence without changing engine inputs."
            ),
        },
        {
            "field": (
                "pitching_plan_diagnostics_version"
            ),
            "type": "string",
            "default": (
                "pitching-plan-diagnostics-v1"
            ),
            "required": True,
            "purpose": (
                "Version-labels the metadata contract."
            ),
        },
    ]

    diagnostic_payload_contract = [
        {
            "field": "enabled",
            "type": "boolean",
            "required": True,
            "source": "integration_wrapper",
        },
        {
            "field": "status",
            "type": "string",
            "required": True,
            "source": "integration_wrapper",
        },
        {
            "field": "version",
            "type": "string",
            "required": True,
            "source": "integration_wrapper",
        },
        {
            "field": "classification",
            "type": "nullable_object",
            "required": True,
            "source": (
                "pitching_plan_classifier"
            ),
        },
        {
            "field": "validation",
            "type": "nullable_object",
            "required": True,
            "source": (
                "validate_pitching_plan_payload"
            ),
        },
        {
            "field": "error",
            "type": "nullable_object",
            "required": True,
            "source": "integration_wrapper",
        },
        {
            "field": "behavioral_effect",
            "type": "string",
            "required": True,
            "source": "constant_none",
        },
        {
            "field": (
                "canonical_probability_"
                "authority_changed"
            ),
            "type": "boolean",
            "required": True,
            "source": "constant_false",
        },
        {
            "field": "production_activation",
            "type": "boolean",
            "required": True,
            "source": "constant_false",
        },
    ]

    planned_integration_points = [
        {
            "integration_id": "DI-I01",
            "path": (
                "mlb_app/simulation/"
                "game_simulation_builder.py"
            ),
            "anchor": "build_game_simulation",
            "planned_change": (
                "Read the explicit diagnostic switch and "
                "evidence mapping without mutating config."
            ),
            "enabled_path_only": True,
            "production_behavior_change": False,
        },
        {
            "integration_id": "DI-I02",
            "path": (
                "mlb_app/simulation/"
                "game_simulation_builder.py"
            ),
            "anchor": "_normalize_metadata",
            "planned_change": (
                "Append validated diagnostic metadata only "
                "after the engine payload already exists."
            ),
            "enabled_path_only": True,
            "production_behavior_change": False,
        },
        {
            "integration_id": "DI-I03",
            "path": (
                "mlb_app/simulation/"
                "pitching_plan_classifier.py"
            ),
            "anchor": "classify_pitching_plan",
            "planned_change": (
                "Call existing pure classifier without "
                "changing classifier behavior."
            ),
            "enabled_path_only": True,
            "production_behavior_change": False,
        },
        {
            "integration_id": "DI-I04",
            "path": (
                "mlb_app/simulation/"
                "pitching_plan_classifier.py"
            ),
            "anchor": (
                "validate_pitching_plan_payload"
            ),
            "planned_change": (
                "Validate classification before attaching "
                "diagnostics."
            ),
            "enabled_path_only": True,
            "production_behavior_change": False,
        },
    ]

    forbidden_integration_points = [
        {
            "path": (
                "mlb_app/simulation/game_engine_v2.py"
            ),
            "forbidden_change": (
                "Do not pass classification into engine "
                "state or simulation arguments."
            ),
        },
        {
            "path": (
                "mlb_app/simulation/game_simulator.py"
            ),
            "forbidden_change": (
                "Do not alter starter innings, bullpen "
                "switching, or event probabilities."
            ),
        },
        {
            "path": (
                "mlb_app/model_projections.py"
            ),
            "forbidden_change": (
                "Do not expose diagnostics through public "
                "projection payloads in the planning or first "
                "implementation layer."
            ),
        },
        {
            "path": "mlb_app/app.py",
            "forbidden_change": (
                "Do not change route defaults or public API "
                "contracts."
            ),
        },
        {
            "path": "frontend",
            "forbidden_change": (
                "Do not render or consume diagnostic metadata."
            ),
        },
    ]

    evidence_mapping = [
        {
            "classifier_input": (
                "listed_starter_id"
            ),
            "planned_source": (
                "explicit config evidence only"
            ),
            "fallback": None,
            "automatic_inference_allowed": False,
        },
        {
            "classifier_input": (
                "expected_primary_pitcher_id"
            ),
            "planned_source": (
                "explicit config evidence only"
            ),
            "fallback": None,
            "automatic_inference_allowed": False,
        },
        {
            "classifier_input": (
                "expected_bulk_pitcher_id"
            ),
            "planned_source": (
                "explicit config evidence only"
            ),
            "fallback": None,
            "automatic_inference_allowed": False,
        },
        {
            "classifier_input": (
                "announced_pitching_plan"
            ),
            "planned_source": (
                "explicit config evidence only"
            ),
            "fallback": None,
            "automatic_inference_allowed": False,
        },
        {
            "classifier_input": (
                "starter_recent_workload"
            ),
            "planned_source": (
                "explicit config evidence only"
            ),
            "fallback": None,
            "automatic_inference_allowed": False,
        },
        {
            "classifier_input": (
                "team_bullpen_game_indicator"
            ),
            "planned_source": (
                "explicit config evidence only"
            ),
            "fallback": False,
            "automatic_inference_allowed": False,
        },
        {
            "classifier_input": (
                "roster_and_availability_state"
            ),
            "planned_source": (
                "explicit config evidence only"
            ),
            "fallback": {},
            "automatic_inference_allowed": False,
        },
    ]

    fixture_matrix = [
        {
            "fixture_id": "DI-F01",
            "scenario": "disabled_no_evidence",
            "diagnostics_enabled": False,
            "expected_classifier_calls": 0,
            "baseline_equality_required": True,
            "expected_status": "absent",
        },
        {
            "fixture_id": "DI-F02",
            "scenario": "disabled_with_evidence",
            "diagnostics_enabled": False,
            "expected_classifier_calls": 0,
            "baseline_equality_required": True,
            "expected_status": "absent",
        },
        {
            "fixture_id": "DI-F03",
            "scenario": (
                "enabled_traditional_starter"
            ),
            "diagnostics_enabled": True,
            "expected_classifier_calls": 1,
            "baseline_equality_required": (
                "simulation_fields_only"
            ),
            "expected_status": "classified",
        },
        {
            "fixture_id": "DI-F04",
            "scenario": "enabled_opener_bulk",
            "diagnostics_enabled": True,
            "expected_classifier_calls": 1,
            "baseline_equality_required": (
                "simulation_fields_only"
            ),
            "expected_status": "classified",
        },
        {
            "fixture_id": "DI-F05",
            "scenario": "enabled_unknown_fallback",
            "diagnostics_enabled": True,
            "expected_classifier_calls": 1,
            "baseline_equality_required": (
                "simulation_fields_only"
            ),
            "expected_status": "classified",
        },
        {
            "fixture_id": "DI-F06",
            "scenario": "enabled_invalid_payload",
            "diagnostics_enabled": True,
            "expected_classifier_calls": 1,
            "baseline_equality_required": (
                "simulation_fields_only"
            ),
            "expected_status": (
                "validation_failed"
            ),
        },
        {
            "fixture_id": "DI-F07",
            "scenario": "enabled_classifier_exception",
            "diagnostics_enabled": True,
            "expected_classifier_calls": 1,
            "baseline_equality_required": (
                "simulation_fields_only"
            ),
            "expected_status": "error",
        },
        {
            "fixture_id": "DI-F08",
            "scenario": "input_immutability",
            "diagnostics_enabled": True,
            "expected_classifier_calls": 1,
            "baseline_equality_required": (
                "simulation_fields_only"
            ),
            "expected_status": "classified",
        },
        {
            "fixture_id": "DI-F09",
            "scenario": "deterministic_replay",
            "diagnostics_enabled": True,
            "expected_classifier_calls": 1,
            "baseline_equality_required": (
                "simulation_fields_only"
            ),
            "expected_status": "classified",
        },
        {
            "fixture_id": "DI-F10",
            "scenario": (
                "no_engine_argument_change"
            ),
            "diagnostics_enabled": True,
            "expected_classifier_calls": 1,
            "baseline_equality_required": (
                "simulation_fields_only"
            ),
            "expected_status": "classified",
        },
    ]

    equivalence_contract = [
        {
            "equivalence_id": "EQ-01",
            "scope": "disabled_full_payload",
            "comparison": "deep_equality",
            "allowed_difference": "none",
        },
        {
            "equivalence_id": "EQ-02",
            "scope": "enabled_engine_result",
            "comparison": "deep_equality",
            "allowed_difference": (
                "metadata.pitching_plan_diagnostics only"
            ),
        },
        {
            "equivalence_id": "EQ-03",
            "scope": "engine_call_arguments",
            "comparison": "deep_equality",
            "allowed_difference": "none",
        },
        {
            "equivalence_id": "EQ-04",
            "scope": "input_config",
            "comparison": "deep_equality",
            "allowed_difference": "none",
        },
        {
            "equivalence_id": "EQ-05",
            "scope": "random_seed_and_count",
            "comparison": "exact_equality",
            "allowed_difference": "none",
        },
        {
            "equivalence_id": "EQ-06",
            "scope": (
                "scores_totals_win_probabilities"
            ),
            "comparison": "exact_equality",
            "allowed_difference": "none",
        },
    ]

    implementation_steps = [
        {
            "step": 1,
            "action": (
                "Add a private diagnostic wrapper in the "
                "shared builder."
            ),
            "required_result": (
                "No engine or classifier behavior change."
            ),
        },
        {
            "step": 2,
            "action": (
                "Add explicit disabled-by-default config "
                "parsing."
            ),
            "required_result": (
                "Disabled path performs zero classifier calls."
            ),
        },
        {
            "step": 3,
            "action": (
                "Call the pure classifier only after the "
                "engine payload is complete."
            ),
            "required_result": (
                "No engine arguments or outputs change."
            ),
        },
        {
            "step": 4,
            "action": (
                "Validate and append diagnostic metadata."
            ),
            "required_result": (
                "Only metadata receives a new optional field."
            ),
        },
        {
            "step": 5,
            "action": (
                "Catch classifier and validation failures."
            ),
            "required_result": (
                "Simulation succeeds with explicit diagnostic "
                "error metadata."
            ),
        },
        {
            "step": 6,
            "action": (
                "Execute baseline-equivalence and diagnostic "
                "fixture suites."
            ),
            "required_result": (
                "All ten fixtures pass."
            ),
        },
        {
            "step": 7,
            "action": (
                "Run an independent integration audit."
            ),
            "required_result": (
                "Non-behavioral reachability and equivalence "
                "are independently proven."
            ),
        },
    ]

    stop_conditions = [
        {
            "condition_id": "DI-HOLD-01",
            "condition": (
                "Disabled path imports or calls the "
                "classifier."
            ),
            "required_action": (
                "Reject implementation and restore zero-call "
                "disabled behavior."
            ),
        },
        {
            "condition_id": "DI-HOLD-02",
            "condition": (
                "Disabled output differs from baseline."
            ),
            "required_action": (
                "Reject implementation."
            ),
        },
        {
            "condition_id": "DI-HOLD-03",
            "condition": (
                "Enabled mode changes any non-diagnostic "
                "field."
            ),
            "required_action": (
                "Reject implementation."
            ),
        },
        {
            "condition_id": "DI-HOLD-04",
            "condition": (
                "Classifier failure causes simulation failure."
            ),
            "required_action": (
                "Reject implementation and isolate failure."
            ),
        },
        {
            "condition_id": "DI-HOLD-05",
            "condition": (
                "Config, evidence, matchup, or payload input "
                "is mutated."
            ),
            "required_action": (
                "Reject implementation."
            ),
        },
        {
            "condition_id": "DI-HOLD-06",
            "condition": (
                "Classification reaches starter innings, "
                "bullpen sequencing, or PA probabilities."
            ),
            "required_action": (
                "Remove behavioral wiring immediately."
            ),
        },
        {
            "condition_id": "DI-HOLD-07",
            "condition": (
                "Public routes or frontend begin depending on "
                "diagnostic metadata."
            ),
            "required_action": (
                "Hold integration at internal diagnostic scope."
            ),
        },
    ]

    safety_rows = [
        {
            "boundary": action,
            "allowed_in_6PE": False,
            "reason": (
                "6PE is planning-only and grants no "
                "production behavior authority."
            ),
        }
        for action in PROHIBITED_ACTIONS
    ]

    safety_rows.extend(
        [
            {
                "boundary": (
                    "diagnostic_integration_contract"
                ),
                "allowed_in_6PE": True,
                "reason": (
                    "Contract planning is non-behavioral."
                ),
            },
            {
                "boundary": (
                    "baseline_equivalence_plan"
                ),
                "allowed_in_6PE": True,
                "reason": (
                    "Equivalence requirements protect the "
                    "existing production path."
                ),
            },
            {
                "boundary": (
                    "diagnostic_fixture_plan"
                ),
                "allowed_in_6PE": True,
                "reason": (
                    "Fixture planning does not modify "
                    "production."
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
            "passed": required_paths_exist,
        },
        {
            "check": (
                "six_pd_predecessor_contract_present"
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
                "classifier_contract_present"
            ),
            "actual": (
                classifier_contract_present
            ),
            "expected": True,
            "passed": (
                classifier_contract_present
            ),
        },
        {
            "check": (
                "shared_builder_contract_present"
            ),
            "actual": builder_contract_present,
            "expected": True,
            "passed": builder_contract_present,
        },
        {
            "check": (
                "engine_contract_present"
            ),
            "actual": engine_contract_present,
            "expected": True,
            "passed": engine_contract_present,
        },
        {
            "check": (
                "classifier_currently_unwired"
            ),
            "actual": (
                current_production_reference_count
            ),
            "expected": 0,
            "passed": currently_unwired,
        },
        {
            "check": (
                "ten_integration_contracts"
            ),
            "actual": len(
                integration_contract
            ),
            "expected": 10,
            "passed": (
                len(integration_contract) == 10
            ),
        },
        {
            "check": (
                "three_config_fields"
            ),
            "actual": len(
                configuration_contract
            ),
            "expected": 3,
            "passed": (
                len(configuration_contract) == 3
            ),
        },
        {
            "check": (
                "nine_diagnostic_fields"
            ),
            "actual": len(
                diagnostic_payload_contract
            ),
            "expected": 9,
            "passed": (
                len(diagnostic_payload_contract)
                == 9
            ),
        },
        {
            "check": (
                "four_planned_integration_points"
            ),
            "actual": len(
                planned_integration_points
            ),
            "expected": 4,
            "passed": (
                len(planned_integration_points)
                == 4
            ),
        },
        {
            "check": "ten_fixtures_planned",
            "actual": len(fixture_matrix),
            "expected": 10,
            "passed": (
                len(fixture_matrix) == 10
            ),
        },
        {
            "check": (
                "six_equivalence_contracts"
            ),
            "actual": len(
                equivalence_contract
            ),
            "expected": 6,
            "passed": (
                len(equivalence_contract) == 6
            ),
        },
        {
            "check": (
                "all_integration_points_nonbehavioral"
            ),
            "actual": any(
                row[
                    "production_behavior_change"
                ]
                for row in planned_integration_points
            ),
            "expected": False,
            "passed": not any(
                row[
                    "production_behavior_change"
                ]
                for row in planned_integration_points
            ),
        },
        {
            "check": (
                "default_diagnostics_disabled"
            ),
            "actual": (
                configuration_contract[0][
                    "default"
                ]
            ),
            "expected": False,
            "passed": (
                configuration_contract[0][
                    "default"
                ]
                is False
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    recommended_next_layer = (
        "6PF_pitching_plan_classification_"
        "diagnostic_integration_implementation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "integration_contract.csv",
        [
            "contract_id",
            "contract",
            "required_value",
            "behavioral_effect_allowed",
        ],
        integration_contract,
    )

    write_csv(
        OUTPUT_DIR / "configuration_contract.csv",
        [
            "field",
            "type",
            "default",
            "required",
            "purpose",
        ],
        configuration_contract,
    )

    write_csv(
        OUTPUT_DIR / "diagnostic_payload_contract.csv",
        [
            "field",
            "type",
            "required",
            "source",
        ],
        diagnostic_payload_contract,
    )

    write_csv(
        OUTPUT_DIR / "planned_integration_points.csv",
        [
            "integration_id",
            "path",
            "anchor",
            "planned_change",
            "enabled_path_only",
            "production_behavior_change",
        ],
        planned_integration_points,
    )

    write_csv(
        OUTPUT_DIR / "forbidden_integration_points.csv",
        [
            "path",
            "forbidden_change",
        ],
        forbidden_integration_points,
    )

    write_csv(
        OUTPUT_DIR / "evidence_mapping.csv",
        [
            "classifier_input",
            "planned_source",
            "fallback",
            "automatic_inference_allowed",
        ],
        evidence_mapping,
    )

    write_csv(
        OUTPUT_DIR / "fixture_matrix.csv",
        [
            "fixture_id",
            "scenario",
            "diagnostics_enabled",
            "expected_classifier_calls",
            "baseline_equality_required",
            "expected_status",
        ],
        fixture_matrix,
    )

    write_csv(
        OUTPUT_DIR / "equivalence_contract.csv",
        [
            "equivalence_id",
            "scope",
            "comparison",
            "allowed_difference",
        ],
        equivalence_contract,
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
        OUTPUT_DIR / "production_reference_scan.csv",
        [
            "path",
            "module_reference",
            "function_reference",
        ],
        production_reference_rows,
    )

    write_csv(
        OUTPUT_DIR / "safety_boundaries.csv",
        [
            "boundary",
            "allowed_in_6PE",
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
                    "Implement disabled-by-default, "
                    "metadata-only diagnostic integration "
                    "in the shared simulation builder."
                ),
                "entry_condition": (
                    "All 6PE planning checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    plan_summary = {
        "integration_scope": (
            "shared_builder_metadata_only"
        ),
        "configuration_switch": (
            "pitching_plan_diagnostics_enabled"
        ),
        "default_enabled": False,
        "classifier_call_allowed_when_disabled": (
            False
        ),
        "production_engine_change_planned": False,
        "simulation_behavior_change_planned": False,
        "canonical_probability_authority_changed": (
            False
        ),
        "public_api_change_planned": False,
        "frontend_change_planned": False,
        "production_activation_planned": False,
        "new_authority_granted": False,
    }

    write_json(
        OUTPUT_DIR / "plan_summary.json",
        plan_summary,
    )

    implementation_plan = {
        "planned_integration_points": [
            row["integration_id"]
            for row in planned_integration_points
        ],
        "forbidden_integration_paths": [
            row["path"]
            for row in forbidden_integration_points
        ],
        "configuration_fields": [
            row["field"]
            for row in configuration_contract
        ],
        "diagnostic_payload_fields": [
            row["field"]
            for row in diagnostic_payload_contract
        ],
        "fixtures": [
            row["fixture_id"]
            for row in fixture_matrix
        ],
        "equivalence_contracts": [
            row["equivalence_id"]
            for row in equivalence_contract
        ],
        "implementation_steps": len(
            implementation_steps
        ),
        "stop_conditions": len(
            stop_conditions
        ),
        "disabled_by_default": True,
        "diagnostic_only": True,
        "production_behavior_authority": False,
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
            "diagnostic_integration_plan_complete"
            if all_checks_passed
            else
            "pitching_plan_classification_"
            "diagnostic_integration_plan_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "planning_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "planning_checks_required": len(checks),
        "integration_contracts_defined": len(
            integration_contract
        ),
        "configuration_fields_defined": len(
            configuration_contract
        ),
        "diagnostic_fields_defined": len(
            diagnostic_payload_contract
        ),
        "planned_integration_points": len(
            planned_integration_points
        ),
        "forbidden_integration_points": len(
            forbidden_integration_points
        ),
        "evidence_mappings_defined": len(
            evidence_mapping
        ),
        "fixtures_planned": len(
            fixture_matrix
        ),
        "equivalence_contracts_defined": len(
            equivalence_contract
        ),
        "implementation_steps_planned": len(
            implementation_steps
        ),
        "stop_conditions_planned": len(
            stop_conditions
        ),
        "current_production_reference_count": (
            current_production_reference_count
        ),
        "classifier_currently_unwired": (
            currently_unwired
        ),
        "diagnostics_default_enabled": False,
        "production_route_changed": False,
        "production_classifier_activated": False,
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
        "diagnostic_integration_implementation_allowed_next": (
            all_checks_passed
        ),
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "planning_checks.csv"
            ),
            str(
                OUTPUT_DIR / "integration_contract.csv"
            ),
            str(
                OUTPUT_DIR / "configuration_contract.csv"
            ),
            str(
                OUTPUT_DIR
                / "diagnostic_payload_contract.csv"
            ),
            str(
                OUTPUT_DIR
                / "planned_integration_points.csv"
            ),
            str(
                OUTPUT_DIR
                / "forbidden_integration_points.csv"
            ),
            str(
                OUTPUT_DIR / "evidence_mapping.csv"
            ),
            str(OUTPUT_DIR / "fixture_matrix.csv"),
            str(
                OUTPUT_DIR
                / "equivalence_contract.csv"
            ),
            str(
                OUTPUT_DIR / "implementation_steps.csv"
            ),
            str(OUTPUT_DIR / "stop_conditions.csv"),
            str(
                OUTPUT_DIR
                / "production_reference_scan.csv"
            ),
            str(
                OUTPUT_DIR / "safety_boundaries.csv"
            ),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(OUTPUT_DIR / "plan_summary.json"),
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
