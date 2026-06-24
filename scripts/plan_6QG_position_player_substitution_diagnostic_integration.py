#!/usr/bin/env python3
"""
Layer 6QG
Position-Player Substitution Diagnostic Integration Plan

Plans disabled-by-default, metadata-only integration of the audited pure
position-player substitution evaluator through the shared game simulation
builder.

This layer grants planning authority only.

It does not:

- activate production substitutions;
- alter batting order or lineup slots;
- alter defensive alignment or designated-hitter state;
- alter runner identity or base/out state;
- alter plate-appearance probabilities;
- alter simulation scores or win probabilities;
- replace canonical probability authority;
- modify frontend behavior;
- authorize validation, tuning, backtesting, pricing, or edge detection.
"""

from __future__ import annotations

import ast
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6QG"

LAYER_NAME = (
    "position_player_substitution_"
    "diagnostic_integration_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6QG_position_player_substitution_"
    "diagnostic_integration_plan"
)

AUDIT_PATH = (
    ROOT
    / "scripts/audit_6QF_position_player_"
    "substitution_evaluator.py"
)

EVALUATOR_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "position_player_substitution_evaluator.py"
)

BUILDER_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "game_simulation_builder.py"
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
    AUDIT_PATH,
    EVALUATOR_PATH,
    BUILDER_PATH,
    ENGINE_PATH,
    SIMULATOR_PATH,
    INNING_SIMULATOR_PATH,
]

PROHIBITED_ACTIONS = [
    "production_pinch_hitter_activation",
    "production_pinch_runner_activation",
    "production_defensive_replacement_activation",
    "production_injury_replacement_activation",
    "production_double_switch_activation",
    "batting_order_change",
    "lineup_slot_change",
    "defensive_alignment_change",
    "designated_hitter_state_change",
    "runner_identity_change",
    "base_state_transition_change",
    "out_state_transition_change",
    "plate_appearance_probability_change",
    "simulation_parameter_change",
    "simulation_score_change",
    "win_probability_change",
    "canonical_probability_replacement",
    "backend_behavior_change",
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
        json.dumps(
            payload,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def read_text(path: Path) -> str:
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

    for index in reversed(positions):
        try:
            payload = json.loads(
                text[index:].strip()
            )
        except json.JSONDecodeError:
            continue

        if isinstance(payload, dict):
            return payload

    return {}


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


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    required_files_exist = all(
        path.exists()
        for path in REQUIRED_PATHS
    )

    audit_run = subprocess.run(
        [
            sys.executable,
            str(AUDIT_PATH),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    audit_payload = parse_last_json_object(
        audit_run.stdout
    )

    six_qf_contract_passed = all(
        [
            audit_run.returncode == 0,
            audit_payload.get("diagnosis")
            == (
                "position_player_substitution_"
                "evaluator_independent_audit_passed"
            ),
            audit_payload.get("all_checks_passed") is True,
            audit_payload.get("audit_checks_passed") == 14,
            audit_payload.get("audit_checks_required") == 14,
            audit_payload.get("independent_cases_passed") == 10,
            audit_payload.get("independent_cases_required") == 10,
            audit_payload.get("production_reference_count") == 0,
            audit_payload.get(
                "diagnostic_integration_planning_allowed_next"
            )
            is True,
            audit_payload.get(
                "production_behavior_integration_allowed_next"
            )
            is False,
        ]
    )

    builder_text = read_text(
        BUILDER_PATH
    )
    engine_text = read_text(
        ENGINE_PATH
    )
    simulator_text = read_text(
        SIMULATOR_PATH
    )
    inning_text = read_text(
        INNING_SIMULATOR_PATH
    )
    evaluator_text = read_text(
        EVALUATOR_PATH
    )

    builder_functions = function_names(
        BUILDER_PATH
    )
    builder_strings = string_constants(
        BUILDER_PATH
    )
    evaluator_strings = string_constants(
        EVALUATOR_PATH
    )

    existing_diagnostic_pattern_present = all(
        [
            "_attach_pitching_plan_diagnostics"
            in builder_functions,
            "_attach_starter_hook_diagnostics"
            in builder_functions,
            "_attach_bullpen_sequence_diagnostics"
            in builder_functions,
            "_attach_stolen_base_pickoff_diagnostics"
            in builder_functions,
            "behavioral_effect"
            in builder_strings,
            "production_activation"
            in builder_strings,
        ]
    )

    builder_has_post_engine_metadata_seam = all(
        token in builder_text
        for token in [
            "normalized_payload = _normalize_metadata",
            "_attach_pitching_plan_diagnostics",
            "_attach_starter_hook_diagnostics",
            "_attach_bullpen_sequence_diagnostics",
            "_attach_stolen_base_pickoff_diagnostics",
        ]
    )

    evaluator_tokens = [
        "position_player_substitution_evaluator",
        "evaluate_position_player_substitution",
        "validate_position_player_substitution_evaluation",
    ]

    evaluator_references = {
        "builder": any(
            token in builder_text
            for token in evaluator_tokens
        ),
        "engine": any(
            token in engine_text
            for token in evaluator_tokens
        ),
        "simulator": any(
            token in simulator_text
            for token in evaluator_tokens
        ),
        "inning_simulator": any(
            token in inning_text
            for token in evaluator_tokens
        ),
    }

    current_production_reference_count = sum(
        1
        for value in evaluator_references.values()
        if value
    )

    evaluator_contract_present = all(
        [
            "evaluate_position_player_substitution"
            in evaluator_text,
            "validate_position_player_substitution_evaluation"
            in evaluator_text,
            "behavioral_effect"
            in evaluator_strings,
            "production_activation"
            in evaluator_strings,
        ]
    )

    proposed_config_contract = [
        {
            "config_key": (
                "position_player_substitution_"
                "diagnostics_enabled"
            ),
            "type": "boolean",
            "default": False,
            "forwarded_to_engine": False,
            "behavioral_authority": False,
        },
        {
            "config_key": (
                "position_player_substitution_"
                "diagnostics_version"
            ),
            "type": "string",
            "default": (
                "position-player-substitution-"
                "diagnostics-v1"
            ),
            "forwarded_to_engine": False,
            "behavioral_authority": False,
        },
        {
            "config_key": (
                "position_player_substitution_state"
            ),
            "type": "object",
            "default": {},
            "forwarded_to_engine": False,
            "behavioral_authority": False,
        },
    ]

    proposed_metadata_contract = [
        {
            "field": "enabled",
            "type": "boolean",
            "required": True,
            "expected_value": True,
        },
        {
            "field": "status",
            "type": "enum",
            "required": True,
            "expected_value": (
                "evaluated|validation_failed|error"
            ),
        },
        {
            "field": "version",
            "type": "string",
            "required": True,
            "expected_value": (
                "position-player-substitution-"
                "diagnostics-v1"
            ),
        },
        {
            "field": "evaluation",
            "type": "object_or_null",
            "required": True,
            "expected_value": (
                "pure evaluator payload"
            ),
        },
        {
            "field": "validation",
            "type": "object_or_null",
            "required": True,
            "expected_value": (
                "output contract validation"
            ),
        },
        {
            "field": "error",
            "type": "object_or_null",
            "required": True,
            "expected_value": None,
        },
        {
            "field": "behavioral_effect",
            "type": "string",
            "required": True,
            "expected_value": "none",
        },
        {
            "field": (
                "canonical_probability_"
                "authority_changed"
            ),
            "type": "boolean",
            "required": True,
            "expected_value": False,
        },
        {
            "field": "production_activation",
            "type": "boolean",
            "required": True,
            "expected_value": False,
        },
    ]

    planned_function_contract = [
        {
            "function": (
                "_attach_position_player_"
                "substitution_diagnostics"
            ),
            "path": (
                "mlb_app/simulation/"
                "game_simulation_builder.py"
            ),
            "import_strategy": "lazy",
            "enabled_default": False,
            "metadata_only": True,
            "engine_input_change": False,
            "simulation_output_change": False,
        },
        {
            "function": "build_game_simulation",
            "path": (
                "mlb_app/simulation/"
                "game_simulation_builder.py"
            ),
            "import_strategy": "none",
            "enabled_default": False,
            "metadata_only": True,
            "engine_input_change": False,
            "simulation_output_change": False,
        },
    ]

    equivalence_contracts = [
        {
            "contract_id": "QG-E01",
            "condition": (
                "diagnostics key absent"
            ),
            "requirement": (
                "exact payload equivalence"
            ),
        },
        {
            "contract_id": "QG-E02",
            "condition": (
                "diagnostics explicitly disabled"
            ),
            "requirement": (
                "exact payload equivalence"
            ),
        },
        {
            "contract_id": "QG-E03",
            "condition": "disabled path",
            "requirement": (
                "zero evaluator imports"
            ),
        },
        {
            "contract_id": "QG-E04",
            "condition": (
                "diagnostics enabled"
            ),
            "requirement": (
                "simulation fields unchanged"
            ),
        },
        {
            "contract_id": "QG-E05",
            "condition": (
                "diagnostics enabled"
            ),
            "requirement": (
                "metadata-only attachment"
            ),
        },
        {
            "contract_id": "QG-E06",
            "condition": (
                "diagnostics enabled"
            ),
            "requirement": (
                "caller config and state unchanged"
            ),
        },
        {
            "contract_id": "QG-E07",
            "condition": (
                "diagnostics enabled"
            ),
            "requirement": (
                "diagnostic keys stripped "
                "before engine invocation"
            ),
        },
        {
            "contract_id": "QG-E08",
            "condition": (
                "evaluator raises"
            ),
            "requirement": (
                "simulation payload preserved "
                "with error metadata"
            ),
        },
        {
            "contract_id": "QG-E09",
            "condition": (
                "output validation fails"
            ),
            "requirement": (
                "simulation payload preserved "
                "with validation metadata"
            ),
        },
        {
            "contract_id": "QG-E10",
            "condition": (
                "all diagnostic paths"
            ),
            "requirement": (
                "no lineup, alignment, base/out, "
                "simulation, or probability change"
            ),
        },
    ]

    fixture_plan = [
        {
            "fixture_id": "QG-F01",
            "scenario": "config_key_absent",
            "expected": (
                "exact payload equivalence"
            ),
        },
        {
            "fixture_id": "QG-F02",
            "scenario": (
                "config_explicitly_disabled"
            ),
            "expected": (
                "exact payload equivalence"
            ),
        },
        {
            "fixture_id": "QG-F03",
            "scenario": (
                "enabled_complete_state"
            ),
            "expected": (
                "evaluated metadata only"
            ),
        },
        {
            "fixture_id": "QG-F04",
            "scenario": (
                "enabled_partial_state"
            ),
            "expected": (
                "partial fallback metadata only"
            ),
        },
        {
            "fixture_id": "QG-F05",
            "scenario": (
                "enabled_invalid_state"
            ),
            "expected": (
                "invalid fallback metadata only"
            ),
        },
        {
            "fixture_id": "QG-F06",
            "scenario": (
                "validation_failure"
            ),
            "expected": (
                "validation_failed metadata"
            ),
        },
        {
            "fixture_id": "QG-F07",
            "scenario": "evaluator_error",
            "expected": (
                "error metadata with preserved "
                "simulation payload"
            ),
        },
        {
            "fixture_id": "QG-F08",
            "scenario": (
                "engine_config_isolation"
            ),
            "expected": (
                "diagnostic keys absent from engine"
            ),
        },
        {
            "fixture_id": "QG-F09",
            "scenario": (
                "input_immutability"
            ),
            "expected": (
                "config and state unchanged"
            ),
        },
        {
            "fixture_id": "QG-F10",
            "scenario": (
                "production_authority_guard"
            ),
            "expected": (
                "no lineup, alignment, base/out, "
                "simulation, or probability change"
            ),
        },
    ]

    expected_metadata_fields = {
        "enabled",
        "status",
        "version",
        "evaluation",
        "validation",
        "error",
        "behavioral_effect",
        (
            "canonical_probability_"
            "authority_changed"
        ),
        "production_activation",
    }

    planned_metadata_fields = {
        row["field"]
        for row in proposed_metadata_contract
    }

    config_contract_valid = all(
        [
            len(
                proposed_config_contract
            )
            == 3,
            all(
                row[
                    "forwarded_to_engine"
                ]
                is False
                and row[
                    "behavioral_authority"
                ]
                is False
                for row
                in proposed_config_contract
            ),
            proposed_config_contract[
                0
            ][
                "default"
            ]
            is False,
        ]
    )

    metadata_contract_valid = all(
        [
            len(
                proposed_metadata_contract
            )
            == 9,
            planned_metadata_fields
            == expected_metadata_fields,
        ]
    )

    function_contract_valid = all(
        [
            len(
                planned_function_contract
            )
            == 2,
            planned_function_contract[
                0
            ][
                "import_strategy"
            ]
            == "lazy",
            all(
                row[
                    "metadata_only"
                ]
                is True
                and row[
                    "engine_input_change"
                ]
                is False
                and row[
                    "simulation_output_change"
                ]
                is False
                for row
                in planned_function_contract
            ),
        ]
    )

    implementation_allowed = all(
        [
            required_files_exist,
            six_qf_contract_passed,
            existing_diagnostic_pattern_present,
            builder_has_post_engine_metadata_seam,
            evaluator_contract_present,
            current_production_reference_count
            == 0,
            config_contract_valid,
            metadata_contract_valid,
            function_contract_valid,
            len(
                equivalence_contracts
            )
            == 10,
            len(
                fixture_plan
            )
            == 10,
        ]
    )

    checks = [
        {
            "check": (
                "required_files_exist"
            ),
            "actual": required_files_exist,
            "expected": True,
            "passed": required_files_exist,
        },
        {
            "check": (
                "six_qf_audit_contract_passes"
            ),
            "actual": (
                six_qf_contract_passed
            ),
            "expected": True,
            "passed": (
                six_qf_contract_passed
            ),
        },
        {
            "check": (
                "existing_diagnostic_pattern_present"
            ),
            "actual": (
                existing_diagnostic_pattern_present
            ),
            "expected": True,
            "passed": (
                existing_diagnostic_pattern_present
            ),
        },
        {
            "check": (
                "post_engine_metadata_seam_present"
            ),
            "actual": (
                builder_has_post_engine_metadata_seam
            ),
            "expected": True,
            "passed": (
                builder_has_post_engine_metadata_seam
            ),
        },
        {
            "check": (
                "evaluator_contract_present"
            ),
            "actual": (
                evaluator_contract_present
            ),
            "expected": True,
            "passed": (
                evaluator_contract_present
            ),
        },
        {
            "check": (
                "current_production_reference_count"
            ),
            "actual": (
                current_production_reference_count
            ),
            "expected": 0,
            "passed": (
                current_production_reference_count
                == 0
            ),
        },
        {
            "check": (
                "config_contract_valid"
            ),
            "actual": config_contract_valid,
            "expected": True,
            "passed": config_contract_valid,
        },
        {
            "check": (
                "metadata_contract_valid"
            ),
            "actual": metadata_contract_valid,
            "expected": True,
            "passed": metadata_contract_valid,
        },
        {
            "check": (
                "function_contract_valid"
            ),
            "actual": function_contract_valid,
            "expected": True,
            "passed": function_contract_valid,
        },
        {
            "check": (
                "ten_equivalence_contracts_planned"
            ),
            "actual": len(
                equivalence_contracts
            ),
            "expected": 10,
            "passed": len(
                equivalence_contracts
            )
            == 10,
        },
        {
            "check": (
                "ten_fixtures_planned"
            ),
            "actual": len(
                fixture_plan
            ),
            "expected": 10,
            "passed": len(
                fixture_plan
            )
            == 10,
        },
        {
            "check": (
                "prohibited_actions_declared"
            ),
            "actual": len(
                PROHIBITED_ACTIONS
            ),
            "expected": 27,
            "passed": len(
                PROHIBITED_ACTIONS
            )
            == 27,
        },
        {
            "check": (
                "diagnostic_implementation_allowed"
            ),
            "actual": implementation_allowed,
            "expected": True,
            "passed": implementation_allowed,
        },
    ]

    all_checks_passed = all(
        row[
            "passed"
        ]
        for row in checks
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
        / "proposed_config_contract.csv",
        [
            "config_key",
            "type",
            "default",
            "forwarded_to_engine",
            "behavioral_authority",
        ],
        proposed_config_contract,
    )

    write_csv(
        OUTPUT_DIR
        / "proposed_metadata_contract.csv",
        [
            "field",
            "type",
            "required",
            "expected_value",
        ],
        proposed_metadata_contract,
    )

    write_csv(
        OUTPUT_DIR
        / "planned_function_contract.csv",
        [
            "function",
            "path",
            "import_strategy",
            "enabled_default",
            "metadata_only",
            "engine_input_change",
            "simulation_output_change",
        ],
        planned_function_contract,
    )

    write_csv(
        OUTPUT_DIR
        / "equivalence_contracts.csv",
        [
            "contract_id",
            "condition",
            "requirement",
        ],
        equivalence_contracts,
    )

    write_csv(
        OUTPUT_DIR
        / "fixture_plan.csv",
        [
            "fixture_id",
            "scenario",
            "expected",
        ],
        fixture_plan,
    )

    write_csv(
        OUTPUT_DIR
        / "current_evaluator_references.csv",
        [
            "component",
            "reference_present",
        ],
        [
            {
                "component": component,
                "reference_present": present,
            }
            for component, present
            in evaluator_references.items()
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "prohibited_actions.csv",
        [
            "action",
            "prohibited",
        ],
        [
            {
                "action": action,
                "prohibited": True,
            }
            for action in PROHIBITED_ACTIONS
        ],
    )

    summary = {
        "planning_checks_required": len(
            checks
        ),
        "planning_checks_passed": sum(
            1
            for row in checks
            if row[
                "passed"
            ]
        ),
        "six_qf_audit_contract_passed": (
            six_qf_contract_passed
        ),
        "existing_diagnostic_pattern_present": (
            existing_diagnostic_pattern_present
        ),
        "post_engine_metadata_seam_present": (
            builder_has_post_engine_metadata_seam
        ),
        "current_production_reference_count": (
            current_production_reference_count
        ),
        "config_contract_fields": len(
            proposed_config_contract
        ),
        "metadata_contract_fields": len(
            proposed_metadata_contract
        ),
        "planned_functions": len(
            planned_function_contract
        ),
        "equivalence_contracts_planned": len(
            equivalence_contracts
        ),
        "fixtures_planned": len(
            fixture_plan
        ),
        "prohibited_actions_declared": len(
            PROHIBITED_ACTIONS
        ),
        "production_substitutions_changed": False,
        "batting_order_changed": False,
        "lineup_slots_changed": False,
        "defensive_alignment_changed": False,
        "designated_hitter_state_changed": False,
        "base_out_state_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
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
            "position_player_substitution_"
            "diagnostic_integration_plan_complete"
            if all_checks_passed
            else
            "position_player_substitution_"
            "diagnostic_integration_plan_incomplete"
        ),
        "all_checks_passed": (
            all_checks_passed
        ),
        "planning_checks_passed": sum(
            1
            for row in checks
            if row[
                "passed"
            ]
        ),
        "planning_checks_required": len(
            checks
        ),
        "six_qf_audit_contract_passed": (
            six_qf_contract_passed
        ),
        "existing_diagnostic_pattern_present": (
            existing_diagnostic_pattern_present
        ),
        "post_engine_metadata_seam_present": (
            builder_has_post_engine_metadata_seam
        ),
        "evaluator_contract_present": (
            evaluator_contract_present
        ),
        "current_production_reference_count": (
            current_production_reference_count
        ),
        "config_contract_valid": (
            config_contract_valid
        ),
        "metadata_contract_valid": (
            metadata_contract_valid
        ),
        "function_contract_valid": (
            function_contract_valid
        ),
        "equivalence_contracts_planned": len(
            equivalence_contracts
        ),
        "fixtures_planned": len(
            fixture_plan
        ),
        "prohibited_actions_declared": len(
            PROHIBITED_ACTIONS
        ),
        "production_substitutions_changed": False,
        "batting_order_changed": False,
        "lineup_slots_changed": False,
        "defensive_alignment_changed": False,
        "designated_hitter_state_changed": False,
        "base_out_state_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
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
        "diagnostic_implementation_allowed_next": (
            implementation_allowed
        ),
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            "6QH_position_player_substitution_"
            "diagnostic_integration"
            if implementation_allowed
            else
            "6QH_position_player_substitution_"
            "diagnostic_integration_plan_remediation"
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR
                / "planning_checks.csv"
            ),
            str(
                OUTPUT_DIR
                / "proposed_config_contract.csv"
            ),
            str(
                OUTPUT_DIR
                / "proposed_metadata_contract.csv"
            ),
            str(
                OUTPUT_DIR
                / "planned_function_contract.csv"
            ),
            str(
                OUTPUT_DIR
                / "equivalence_contracts.csv"
            ),
            str(
                OUTPUT_DIR
                / "fixture_plan.csv"
            ),
            str(
                OUTPUT_DIR
                / "current_evaluator_references.csv"
            ),
            str(
                OUTPUT_DIR
                / "prohibited_actions.csv"
            ),
        ],
        "generated_json_artifacts": [
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
