#!/usr/bin/env python3
"""
Layer 6PL
Dynamic Starter-Hook Diagnostic Integration Plan

Plans disabled-by-default, metadata-only integration of the pure starter-hook
evaluator through the shared game simulation builder.

This layer grants planning authority only.

It does not:
- change starter-hook behavior;
- change starter innings;
- change bullpen transitions;
- change PA probabilities;
- change simulation scores;
- change win probabilities;
- replace canonical probability authority;
- activate pitching-plan behavior;
- modify backend or frontend behavior.
"""

from __future__ import annotations

import ast
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6PL"
LAYER_NAME = (
    "dynamic_starter_hook_"
    "diagnostic_integration_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PL_dynamic_starter_hook_"
    "diagnostic_integration_plan"
)

AUDIT_PATH = (
    ROOT
    / "scripts/audit_6PK_"
    "dynamic_starter_hook_evaluator.py"
)

EVALUATOR_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "starter_hook_evaluator.py"
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

REQUIRED_PATHS = [
    AUDIT_PATH,
    EVALUATOR_PATH,
    BUILDER_PATH,
    ENGINE_PATH,
    SIMULATOR_PATH,
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


def parse_last_json_object(
    text: str,
) -> dict[str, Any]:
    positions = [
        index
        for index, character in enumerate(text)
        if character == "{"
    ]

    for index in reversed(positions):
        candidate = text[index:].strip()

        try:
            payload = json.loads(candidate)
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

    six_pk_contract_passed = all(
        [
            audit_run.returncode == 0,
            audit_payload.get(
                "all_checks_passed"
            )
            is True,
            audit_payload.get(
                "audit_checks_passed"
            )
            == 13,
            audit_payload.get(
                "audit_checks_required"
            )
            == 13,
            audit_payload.get(
                "independent_cases_passed"
            )
            == 8,
            audit_payload.get(
                "production_reference_count"
            )
            == 0,
            audit_payload.get(
                "evaluator_is_pure"
            )
            is True,
            audit_payload.get(
                "diagnostic_integration_planning_allowed_next"
            )
            is True,
            audit_payload.get(
                "diagnostic_integration_allowed_next"
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

    evaluator_text = read_text(
        EVALUATOR_PATH
    )

    builder_functions = function_names(
        BUILDER_PATH
    )

    builder_tree = ast.parse(
        builder_text,
        filename=str(BUILDER_PATH),
    )

    builder_string_constants = {
        node.value
        for node in ast.walk(builder_tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }

    pitching_plan_pattern_present = all(
        [
            (
                "_attach_pitching_plan_diagnostics"
                in builder_text
            ),
            (
                "from mlb_app.simulation."
                "pitching_plan_classifier import"
                in builder_text
            ),
            {
                "pitching_plan_diagnostics_enabled",
                "pitching_plan_evidence",
                "pitching_plan_diagnostics_version",
                "behavioral_effect",
                (
                    "canonical_probability_"
                    "authority_changed"
                ),
                "production_activation",
            }.issubset(
                builder_string_constants
            ),
        ]
    )

    builder_has_post_engine_metadata_seam = all(
        token in builder_text
        for token in [
            "normalized_payload = _normalize_metadata",
            (
                "return "
                "_attach_pitching_plan_diagnostics"
            ),
        ]
    )

    builder_strips_diagnostic_config = all(
        token in builder_text
        for token in [
            "engine_config = {",
            (
                '"pitching_plan_'
                'diagnostics_enabled"'
            ),
            '"pitching_plan_evidence"',
            (
                '"pitching_plan_'
                'diagnostics_version"'
            ),
        ]
    )

    evaluator_references_in_production = {
        "builder": (
            "starter_hook_evaluator"
            in builder_text
            or "evaluate_starter_hook"
            in builder_text
        ),
        "engine": (
            "starter_hook_evaluator"
            in engine_text
            or "evaluate_starter_hook"
            in engine_text
        ),
        "simulator": (
            "starter_hook_evaluator"
            in simulator_text
            or "evaluate_starter_hook"
            in simulator_text
        ),
    }

    current_production_reference_count = sum(
        1
        for value in (
            evaluator_references_in_production
            .values()
        )
        if value
    )

    evaluator_contract_present = all(
        token in evaluator_text
        for token in [
            "evaluate_starter_hook",
            (
                "validate_starter_hook_"
                "evaluation"
            ),
            "behavioral_effect",
            (
                "canonical_probability_"
                "authority_changed"
            ),
            "production_activation",
        ]
    )

    proposed_config_contract = [
        {
            "config_key": (
                "starter_hook_diagnostics_enabled"
            ),
            "type": "boolean",
            "default": False,
            "forwarded_to_engine": False,
            "behavioral_authority": False,
        },
        {
            "config_key": (
                "starter_hook_diagnostics_version"
            ),
            "type": "string",
            "default": (
                "starter-hook-diagnostics-v1"
            ),
            "forwarded_to_engine": False,
            "behavioral_authority": False,
        },
        {
            "config_key": (
                "starter_hook_state"
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
                "starter-hook-diagnostics-v1"
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
                "_attach_starter_hook_diagnostics"
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
            "function": (
                "build_game_simulation"
            ),
            "path": (
                "mlb_app/simulation/"
                "game_simulation_builder.py"
            ),
            "import_strategy": (
                "no direct evaluator import"
            ),
            "enabled_default": False,
            "metadata_only": True,
            "engine_input_change": False,
            "simulation_output_change": False,
        },
    ]

    integration_steps = [
        {
            "step": 1,
            "action": (
                "Add a private metadata attachment "
                "function to the shared builder."
            ),
            "production_behavior_change": False,
        },
        {
            "step": 2,
            "action": (
                "Return the original payload unchanged "
                "when diagnostics are disabled."
            ),
            "production_behavior_change": False,
        },
        {
            "step": 3,
            "action": (
                "Lazy-import the evaluator only after "
                "the enabled flag is true."
            ),
            "production_behavior_change": False,
        },
        {
            "step": 4,
            "action": (
                "Deep-copy starter_hook_state before "
                "evaluation."
            ),
            "production_behavior_change": False,
        },
        {
            "step": 5,
            "action": (
                "Validate the evaluator output and attach "
                "it only under meta/metadata."
            ),
            "production_behavior_change": False,
        },
        {
            "step": 6,
            "action": (
                "Strip all starter-hook diagnostic config "
                "keys before engine invocation."
            ),
            "production_behavior_change": False,
        },
        {
            "step": 7,
            "action": (
                "Prove disabled-path exact equivalence and "
                "enabled-path simulation-field equivalence."
            ),
            "production_behavior_change": False,
        },
        {
            "step": 8,
            "action": (
                "Prove engine and simulator have zero "
                "evaluator references."
            ),
            "production_behavior_change": False,
        },
    ]

    equivalence_contracts = [
        {
            "contract_id": "PL-E01",
            "contract": (
                "disabled_payload_exact_equivalence"
            ),
            "requirement": (
                "Disabled builder result equals baseline "
                "result exactly."
            ),
        },
        {
            "contract_id": "PL-E02",
            "contract": (
                "disabled_zero_evaluator_imports"
            ),
            "requirement": (
                "Evaluator module is not imported on the "
                "disabled path."
            ),
        },
        {
            "contract_id": "PL-E03",
            "contract": (
                "disabled_zero_evaluator_calls"
            ),
            "requirement": (
                "Evaluator is never called on the disabled "
                "path."
            ),
        },
        {
            "contract_id": "PL-E04",
            "contract": (
                "engine_config_exact_equivalence"
            ),
            "requirement": (
                "The engine receives no diagnostic-only "
                "keys."
            ),
        },
        {
            "contract_id": "PL-E05",
            "contract": (
                "enabled_simulation_field_equivalence"
            ),
            "requirement": (
                "All non-metadata simulation fields remain "
                "exactly equal."
            ),
        },
        {
            "contract_id": "PL-E06",
            "contract": (
                "input_state_immutability"
            ),
            "requirement": (
                "Caller config and starter-hook state remain "
                "unchanged."
            ),
        },
        {
            "contract_id": "PL-E07",
            "contract": (
                "lazy_import_only"
            ),
            "requirement": (
                "The evaluator import appears only inside "
                "the enabled diagnostic function."
            ),
        },
        {
            "contract_id": "PL-E08",
            "contract": (
                "engine_zero_reachability"
            ),
            "requirement": (
                "game_engine_v2.py has no evaluator "
                "reference."
            ),
        },
        {
            "contract_id": "PL-E09",
            "contract": (
                "simulator_zero_reachability"
            ),
            "requirement": (
                "game_simulator.py has no evaluator "
                "reference."
            ),
        },
        {
            "contract_id": "PL-E10",
            "contract": (
                "canonical_authority_unchanged"
            ),
            "requirement": (
                "Diagnostic payload explicitly reports no "
                "canonical probability authority change."
            ),
        },
    ]

    fixture_plan = [
        {
            "fixture_id": "PL-F01",
            "scenario": "diagnostics_disabled",
            "expected_status": "absent",
            "exact_equivalence_required": True,
        },
        {
            "fixture_id": "PL-F02",
            "scenario": (
                "enabled_valid_keep_state"
            ),
            "expected_status": "evaluated",
            "exact_equivalence_required": False,
        },
        {
            "fixture_id": "PL-F03",
            "scenario": (
                "enabled_valid_pull_state"
            ),
            "expected_status": "evaluated",
            "exact_equivalence_required": False,
        },
        {
            "fixture_id": "PL-F04",
            "scenario": (
                "enabled_incomplete_state"
            ),
            "expected_status": "evaluated",
            "exact_equivalence_required": False,
        },
        {
            "fixture_id": "PL-F05",
            "scenario": (
                "enabled_invalid_state_type"
            ),
            "expected_status": "evaluated",
            "exact_equivalence_required": False,
        },
        {
            "fixture_id": "PL-F06",
            "scenario": (
                "enabled_custom_version"
            ),
            "expected_status": "evaluated",
            "exact_equivalence_required": False,
        },
        {
            "fixture_id": "PL-F07",
            "scenario": (
                "enabled_evaluator_exception"
            ),
            "expected_status": "error",
            "exact_equivalence_required": False,
        },
        {
            "fixture_id": "PL-F08",
            "scenario": (
                "caller_config_immutability"
            ),
            "expected_status": "evaluated",
            "exact_equivalence_required": False,
        },
        {
            "fixture_id": "PL-F09",
            "scenario": (
                "engine_config_isolation"
            ),
            "expected_status": "evaluated",
            "exact_equivalence_required": False,
        },
        {
            "fixture_id": "PL-F10",
            "scenario": (
                "metadata_alias_consistency"
            ),
            "expected_status": "evaluated",
            "exact_equivalence_required": False,
        },
    ]

    authority_boundaries = [
        {
            "authority": action,
            "granted_in_6PL": False,
            "reason": (
                "6PL is planning only."
            ),
        }
        for action in PROHIBITED_ACTIONS
    ]

    authority_boundaries.extend(
        [
            {
                "authority": (
                    "diagnostic_integration_implementation"
                ),
                "granted_in_6PL": True,
                "reason": (
                    "Allowed next only if every planning "
                    "check passes."
                ),
            },
            {
                "authority": (
                    "production_behavior_integration"
                ),
                "granted_in_6PL": False,
                "reason": (
                    "No historical validation or explicit "
                    "behavioral authorization exists."
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
            "expected": len(
                REQUIRED_PATHS
            ),
            "passed": required_files_exist,
        },
        {
            "check": (
                "six_pk_contract_passes"
            ),
            "actual": (
                six_pk_contract_passed
            ),
            "expected": True,
            "passed": (
                six_pk_contract_passed
            ),
        },
        {
            "check": (
                "pitching_plan_pattern_present"
            ),
            "actual": (
                pitching_plan_pattern_present
            ),
            "expected": True,
            "passed": (
                pitching_plan_pattern_present
            ),
        },
        {
            "check": (
                "builder_post_engine_metadata_seam_present"
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
                "builder_strips_existing_diagnostic_config"
            ),
            "actual": (
                builder_strips_diagnostic_config
            ),
            "expected": True,
            "passed": (
                builder_strips_diagnostic_config
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
                "three_config_keys_planned"
            ),
            "actual": len(
                proposed_config_contract
            ),
            "expected": 3,
            "passed": (
                len(
                    proposed_config_contract
                )
                == 3
            ),
        },
        {
            "check": (
                "nine_metadata_fields_planned"
            ),
            "actual": len(
                proposed_metadata_contract
            ),
            "expected": 9,
            "passed": (
                len(
                    proposed_metadata_contract
                )
                == 9
            ),
        },
        {
            "check": (
                "ten_equivalence_contracts_planned"
            ),
            "actual": len(
                equivalence_contracts
            ),
            "expected": 10,
            "passed": (
                len(
                    equivalence_contracts
                )
                == 10
            ),
        },
        {
            "check": (
                "ten_fixtures_planned"
            ),
            "actual": len(
                fixture_plan
            ),
            "expected": 10,
            "passed": (
                len(
                    fixture_plan
                )
                == 10
            ),
        },
        {
            "check": (
                "all_integration_steps_nonbehavioral"
            ),
            "actual": any(
                row[
                    "production_behavior_change"
                ]
                for row in integration_steps
            ),
            "expected": False,
            "passed": not any(
                row[
                    "production_behavior_change"
                ]
                for row in integration_steps
            ),
        },
        {
            "check": (
                "builder_function_available"
            ),
            "actual": (
                "build_game_simulation"
                in builder_functions
            ),
            "expected": True,
            "passed": (
                "build_game_simulation"
                in builder_functions
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    recommended_next_layer = (
        "6PM_dynamic_starter_hook_"
        "diagnostic_integration_implementation"
        if all_checks_passed
        else
        "6PM_dynamic_starter_hook_"
        "diagnostic_integration_plan_remediation"
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
        OUTPUT_DIR / "config_contract.csv",
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
        OUTPUT_DIR / "metadata_contract.csv",
        [
            "field",
            "type",
            "required",
            "expected_value",
        ],
        proposed_metadata_contract,
    )

    write_csv(
        OUTPUT_DIR / "function_contract.csv",
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
        OUTPUT_DIR / "integration_steps.csv",
        [
            "step",
            "action",
            "production_behavior_change",
        ],
        integration_steps,
    )

    write_csv(
        OUTPUT_DIR / "equivalence_contracts.csv",
        [
            "contract_id",
            "contract",
            "requirement",
        ],
        equivalence_contracts,
    )

    write_csv(
        OUTPUT_DIR / "fixture_plan.csv",
        [
            "fixture_id",
            "scenario",
            "expected_status",
            "exact_equivalence_required",
        ],
        fixture_plan,
    )

    write_csv(
        OUTPUT_DIR / "authority_boundaries.csv",
        [
            "authority",
            "granted_in_6PL",
            "reason",
        ],
        authority_boundaries,
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
                    "metadata-only starter-hook diagnostics "
                    "in the shared builder."
                ),
                "entry_condition": (
                    "All 6PL planning checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    integration_summary = {
        "integration_path": (
            "mlb_app/simulation/"
            "game_simulation_builder.py"
        ),
        "planned_function": (
            "_attach_starter_hook_diagnostics"
        ),
        "enabled_default": False,
        "lazy_import_required": True,
        "metadata_only": True,
        "config_keys_planned": len(
            proposed_config_contract
        ),
        "metadata_fields_planned": len(
            proposed_metadata_contract
        ),
        "equivalence_contracts_planned": len(
            equivalence_contracts
        ),
        "fixtures_planned": len(
            fixture_plan
        ),
        "engine_references_allowed": False,
        "simulator_references_allowed": False,
        "production_behavior_change_allowed": False,
        "canonical_probability_authority_change_allowed": False,
    }

    write_json(
        OUTPUT_DIR / "integration_summary.json",
        integration_summary,
    )

    write_json(
        OUTPUT_DIR / "predecessor_contract.json",
        {
            "returncode": (
                audit_run.returncode
            ),
            "contract_passed": (
                six_pk_contract_passed
            ),
            "diagnosis": audit_payload,
            "stderr": audit_run.stderr,
        },
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "dynamic_starter_hook_diagnostic_"
            "integration_plan_complete"
            if all_checks_passed
            else
            "dynamic_starter_hook_diagnostic_"
            "integration_plan_failed"
        ),
        "all_checks_passed": (
            all_checks_passed
        ),
        "planning_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "planning_checks_required": len(
            checks
        ),
        "six_pk_contract_passed": (
            six_pk_contract_passed
        ),
        "current_production_reference_count": (
            current_production_reference_count
        ),
        "config_keys_planned": len(
            proposed_config_contract
        ),
        "metadata_fields_planned": len(
            proposed_metadata_contract
        ),
        "integration_steps_planned": len(
            integration_steps
        ),
        "equivalence_contracts_planned": len(
            equivalence_contracts
        ),
        "fixtures_planned": len(
            fixture_plan
        ),
        "integration_path": (
            "mlb_app/simulation/"
            "game_simulation_builder.py"
        ),
        "enabled_default": False,
        "lazy_import_required": True,
        "metadata_only": True,
        "engine_config_isolation_required": True,
        "disabled_exact_equivalence_required": True,
        "enabled_simulation_field_equivalence_required": True,
        "input_immutability_required": True,
        "engine_zero_reachability_required": True,
        "simulator_zero_reachability_required": True,
        "production_starter_hook_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
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
                OUTPUT_DIR / "config_contract.csv"
            ),
            str(
                OUTPUT_DIR / "metadata_contract.csv"
            ),
            str(
                OUTPUT_DIR / "function_contract.csv"
            ),
            str(
                OUTPUT_DIR / "integration_steps.csv"
            ),
            str(
                OUTPUT_DIR / "equivalence_contracts.csv"
            ),
            str(
                OUTPUT_DIR / "fixture_plan.csv"
            ),
            str(
                OUTPUT_DIR / "authority_boundaries.csv"
            ),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR / "integration_summary.json"
            ),
            str(
                OUTPUT_DIR / "predecessor_contract.json"
            ),
            str(
                OUTPUT_DIR / "diagnosis.json"
            ),
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
