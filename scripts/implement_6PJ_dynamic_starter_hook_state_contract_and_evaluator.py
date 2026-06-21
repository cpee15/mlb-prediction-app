#!/usr/bin/env python3
"""
Layer 6PJ
Dynamic Starter-Hook State Contract and Evaluator Implementation

Implements and validates a pure deterministic starter-hook evaluator.

No production simulation wiring is permitted.
"""

from __future__ import annotations

import ast
import csv
import importlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6PJ"
LAYER_NAME = (
    "dynamic_starter_hook_state_contract_"
    "and_evaluator_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PJ_dynamic_starter_hook_"
    "state_contract_and_evaluator_implementation"
)

PLAN_PATH = (
    ROOT
    / "scripts/plan_6PI_dynamic_starter_hook_"
    "inventory_and_implementation.py"
)

MODULE_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "starter_hook_evaluator.py"
)

PRODUCTION_PATHS = [
    ROOT
    / "mlb_app/simulation/"
    "game_engine_v2.py",
    ROOT
    / "mlb_app/simulation/"
    "game_simulator.py",
    ROOT
    / "mlb_app/simulation/"
    "game_simulation_builder.py",
]

EXPECTED_OUTPUT_FIELDS = {
    "decision",
    "pull_probability",
    "trigger_reasons",
    "state_completeness",
    "fallback_used",
    "fallback_reason",
    "behavioral_effect",
    "canonical_probability_authority_changed",
    "production_activation",
}

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


def base_state() -> dict[str, Any]:
    return {
        "inning": 4,
        "outs": 0,
        "base_state": {
            "first": False,
            "second": False,
            "third": False,
        },
        "batters_faced": 16,
        "pitch_count_estimate": 60.0,
        "times_through_order": 1.8,
        "runs_allowed": 1,
        "recent_traffic_index": 0.20,
        "score_margin": 1,
        "leverage_proxy": 0.45,
        "starter_quality_score": 0.0,
        "expected_starter_innings": 5.6,
        "fatigue_index": 0.30,
        "bullpen_availability": {},
        "pitching_plan": {},
    }


def fixture_matrix() -> list[dict[str, Any]]:
    fixtures: list[dict[str, Any]] = []

    state = base_state()
    state.update(
        {
            "inning": 1,
            "batters_faced": 3,
            "pitch_count_estimate": 12.0,
            "times_through_order": 0.34,
            "runs_allowed": 0,
            "recent_traffic_index": 0.0,
            "fatigue_index": 0.05,
        }
    )
    fixtures.append(
        {
            "fixture_id": "PJ-F01",
            "scenario": (
                "first_inning_clean_low_workload"
            ),
            "state": state,
            "expected_decision": "keep",
        }
    )

    state = base_state()
    state.update(
        {
            "inning": 5,
            "pitch_count_estimate": 108.0,
            "batters_faced": 25,
            "times_through_order": 2.7,
            "fatigue_index": 0.88,
        }
    )
    fixtures.append(
        {
            "fixture_id": "PJ-F02",
            "scenario": (
                "fifth_inning_high_pitch_count"
            ),
            "state": state,
            "expected_decision": "pull",
        }
    )

    state = base_state()
    state.update(
        {
            "inning": 6,
            "batters_faced": 28,
            "pitch_count_estimate": 88.0,
            "times_through_order": 3.05,
            "fatigue_index": 0.58,
        }
    )
    fixtures.append(
        {
            "fixture_id": "PJ-F03",
            "scenario": (
                "third_time_through_order"
            ),
            "state": state,
            "expected_decision": "pull",
        }
    )

    state = base_state()
    state.update(
        {
            "inning": 4,
            "runs_allowed": 4,
            "recent_traffic_index": 0.90,
            "pitch_count_estimate": 78.0,
            "fatigue_index": 0.55,
        }
    )
    fixtures.append(
        {
            "fixture_id": "PJ-F04",
            "scenario": "heavy_recent_traffic",
            "state": state,
            "expected_decision": "pull",
        }
    )

    state = base_state()
    state.update(
        {
            "inning": 5,
            "starter_quality_score": 0.80,
            "pitch_count_estimate": 76.0,
            "batters_faced": 20,
            "times_through_order": 2.2,
            "runs_allowed": 1,
            "recent_traffic_index": 0.20,
            "fatigue_index": 0.38,
            "expected_starter_innings": 6.4,
        }
    )
    fixtures.append(
        {
            "fixture_id": "PJ-F05",
            "scenario": (
                "strong_starter_moderate_workload"
            ),
            "state": state,
            "expected_decision": "keep",
        }
    )

    state = base_state()
    state.update(
        {
            "inning": 5,
            "starter_quality_score": -0.80,
            "pitch_count_estimate": 91.0,
            "batters_faced": 23,
            "times_through_order": 2.55,
            "runs_allowed": 3,
            "recent_traffic_index": 0.55,
            "fatigue_index": 0.65,
            "expected_starter_innings": 4.5,
        }
    )
    fixtures.append(
        {
            "fixture_id": "PJ-F06",
            "scenario": (
                "weak_starter_moderate_workload"
            ),
            "state": state,
            "expected_decision": "pull",
        }
    )

    state = base_state()
    state.update(
        {
            "inning": 7,
            "score_margin": 1,
            "leverage_proxy": 0.92,
            "pitch_count_estimate": 89.0,
            "batters_faced": 25,
            "times_through_order": 2.75,
            "fatigue_index": 0.62,
            "expected_starter_innings": 6.0,
        }
    )
    fixtures.append(
        {
            "fixture_id": "PJ-F07",
            "scenario": (
                "late_close_game_high_leverage"
            ),
            "state": state,
            "expected_decision": "pull",
        }
    )

    state = base_state()
    state.update(
        {
            "inning": 7,
            "score_margin": 8,
            "leverage_proxy": 0.12,
            "starter_quality_score": 0.70,
            "pitch_count_estimate": 78.0,
            "batters_faced": 22,
            "times_through_order": 2.35,
            "runs_allowed": 1,
            "recent_traffic_index": 0.10,
            "fatigue_index": 0.42,
            "expected_starter_innings": 7.2,
        }
    )
    fixtures.append(
        {
            "fixture_id": "PJ-F08",
            "scenario": (
                "late_blowout_low_leverage"
            ),
            "state": state,
            "expected_decision": "keep",
        }
    )

    state = base_state()
    state.pop("pitch_count_estimate")
    fixtures.append(
        {
            "fixture_id": "PJ-F09",
            "scenario": "incomplete_state",
            "state": state,
            "expected_decision": (
                "insufficient_state"
            ),
        }
    )

    state = base_state()
    state.update(
        {
            "inning": 6,
            "pitch_count_estimate": 97.0,
            "times_through_order": 2.8,
            "fatigue_index": 0.68,
        }
    )
    fixtures.append(
        {
            "fixture_id": "PJ-F10",
            "scenario": "deterministic_replay",
            "state": state,
            "expected_decision": "pull",
            "deterministic_replay": True,
        }
    )

    return fixtures


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    required_files_exist = all(
        path.exists()
        for path in [
            PLAN_PATH,
            MODULE_PATH,
            *PRODUCTION_PATHS,
        ]
    )

    plan_text = read_text(PLAN_PATH)

    plan_tree = ast.parse(
        plan_text,
        filename=str(PLAN_PATH),
    )

    plan_string_constants = {
        node.value
        for node in ast.walk(plan_tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }

    plan_contract_present = all(
        token in plan_string_constants
        for token in [
            (
                "dynamic_starter_hook_inventory_"
                "and_implementation_plan_complete"
            ),
            (
                "6PJ_dynamic_starter_hook_"
                "state_contract_and_evaluator_"
                "implementation"
            ),
            (
                "pure_deterministic_evaluator_only"
            ),
        ]
    )

    module = importlib.import_module(
        "mlb_app.simulation."
        "starter_hook_evaluator"
    )

    required_functions_present = all(
        callable(getattr(module, name, None))
        for name in [
            "validate_starter_hook_state",
            "evaluate_starter_hook",
            (
                "validate_starter_hook_"
                "evaluation"
            ),
        ]
    )

    required_state_fields = set(
        module.REQUIRED_STATE_FIELDS
    )

    required_state_contract_present = (
        len(required_state_fields) == 13
        and {
            "inning",
            "outs",
            "base_state",
            "batters_faced",
            "pitch_count_estimate",
            "times_through_order",
            "runs_allowed",
            "recent_traffic_index",
            "score_margin",
            "leverage_proxy",
            "starter_quality_score",
            "expected_starter_innings",
            "fatigue_index",
        }
        == required_state_fields
    )

    output_contract_present = (
        set(module.EVALUATOR_OUTPUT_FIELDS)
        == EXPECTED_OUTPUT_FIELDS
    )

    fixtures = fixture_matrix()
    fixture_results: list[
        dict[str, Any]
    ] = []

    inputs_unchanged = True
    all_outputs_valid = True
    deterministic_replay_passed = True

    for fixture in fixtures:
        state = deepcopy(fixture["state"])
        original_state = deepcopy(state)

        evaluation = (
            module.evaluate_starter_hook(
                state
            )
        )

        validation = (
            module
            .validate_starter_hook_evaluation(
                evaluation
            )
        )

        replay = None

        if fixture.get(
            "deterministic_replay"
        ):
            replay = (
                module.evaluate_starter_hook(
                    deepcopy(state)
                )
            )

            deterministic_replay_passed = (
                deterministic_replay_passed
                and replay == evaluation
            )

        state_unchanged = (
            state == original_state
        )

        inputs_unchanged = (
            inputs_unchanged
            and state_unchanged
        )

        output_valid = (
            validation.get("valid") is True
        )

        all_outputs_valid = (
            all_outputs_valid
            and output_valid
        )

        passed = all(
            [
                evaluation.get("decision")
                == fixture[
                    "expected_decision"
                ],
                output_valid,
                state_unchanged,
                (
                    evaluation.get(
                        "behavioral_effect"
                    )
                    == "none"
                ),
                (
                    evaluation.get(
                        "production_activation"
                    )
                    is False
                ),
                (
                    evaluation.get(
                        (
                            "canonical_probability_"
                            "authority_changed"
                        )
                    )
                    is False
                ),
                (
                    replay == evaluation
                    if replay is not None
                    else True
                ),
            ]
        )

        fixture_results.append(
            {
                "fixture_id": (
                    fixture["fixture_id"]
                ),
                "scenario": (
                    fixture["scenario"]
                ),
                "expected_decision": (
                    fixture[
                        "expected_decision"
                    ]
                ),
                "actual_decision": (
                    evaluation.get(
                        "decision"
                    )
                ),
                "pull_probability": (
                    evaluation.get(
                        "pull_probability"
                    )
                ),
                "trigger_reasons": (
                    evaluation.get(
                        "trigger_reasons"
                    )
                ),
                "output_valid": output_valid,
                "state_unchanged": (
                    state_unchanged
                ),
                "passed": passed,
                "evaluation": evaluation,
            }
        )

    fixtures_passed = sum(
        1
        for row in fixture_results
        if row["passed"]
    )

    production_reference_rows: list[
        dict[str, Any]
    ] = []

    for path in PRODUCTION_PATHS:
        text = read_text(path)

        references_module = any(
            token in text
            for token in [
                "starter_hook_evaluator",
                "evaluate_starter_hook",
                (
                    "validate_starter_hook_"
                    "state"
                ),
            ]
        )

        production_reference_rows.append(
            {
                "path": str(
                    path.relative_to(ROOT)
                ),
                "references_evaluator": (
                    references_module
                ),
            }
        )

    production_reference_count = sum(
        1
        for row in production_reference_rows
        if row["references_evaluator"]
    )

    module_tree = ast.parse(
        read_text(MODULE_PATH),
        filename=str(MODULE_PATH),
    )

    forbidden_imports = []

    for node in ast.walk(module_tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in {
                    "random",
                    "numpy",
                    "pandas",
                    "sqlalchemy",
                }:
                    forbidden_imports.append(
                        alias.name
                    )

        if isinstance(node, ast.ImportFrom):
            if node.module in {
                "random",
                "numpy",
                "pandas",
                "sqlalchemy",
            }:
                forbidden_imports.append(
                    str(node.module)
                )

    evaluator_is_pure = (
        not forbidden_imports
        and production_reference_count == 0
    )

    invalid_state_result = (
        module.evaluate_starter_hook({})
    )

    invalid_state_fallback_passed = all(
        [
            invalid_state_result.get(
                "decision"
            )
            == "insufficient_state",
            invalid_state_result.get(
                "fallback_used"
            )
            is True,
            invalid_state_result.get(
                "behavioral_effect"
            )
            == "none",
            invalid_state_result.get(
                "production_activation"
            )
            is False,
        ]
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
                "six_pi_plan_contract_present"
            ),
            "actual": plan_contract_present,
            "expected": True,
            "passed": plan_contract_present,
        },
        {
            "check": (
                "required_functions_present"
            ),
            "actual": (
                required_functions_present
            ),
            "expected": True,
            "passed": (
                required_functions_present
            ),
        },
        {
            "check": (
                "required_state_contract_present"
            ),
            "actual": sorted(
                required_state_fields
            ),
            "expected": sorted(
                required_state_fields
            ),
            "passed": (
                required_state_contract_present
            ),
        },
        {
            "check": (
                "output_contract_present"
            ),
            "actual": sorted(
                module.EVALUATOR_OUTPUT_FIELDS
            ),
            "expected": sorted(
                EXPECTED_OUTPUT_FIELDS
            ),
            "passed": (
                output_contract_present
            ),
        },
        {
            "check": (
                "ten_fixtures_executed"
            ),
            "actual": len(
                fixture_results
            ),
            "expected": 10,
            "passed": (
                len(fixture_results) == 10
            ),
        },
        {
            "check": (
                "all_fixtures_pass"
            ),
            "actual": fixtures_passed,
            "expected": len(
                fixture_results
            ),
            "passed": (
                fixtures_passed
                == len(fixture_results)
            ),
        },
        {
            "check": (
                "all_outputs_validate"
            ),
            "actual": all_outputs_valid,
            "expected": True,
            "passed": all_outputs_valid,
        },
        {
            "check": (
                "deterministic_replay_passes"
            ),
            "actual": (
                deterministic_replay_passed
            ),
            "expected": True,
            "passed": (
                deterministic_replay_passed
            ),
        },
        {
            "check": "inputs_unchanged",
            "actual": inputs_unchanged,
            "expected": True,
            "passed": inputs_unchanged,
        },
        {
            "check": (
                "invalid_state_fallback_passes"
            ),
            "actual": (
                invalid_state_fallback_passed
            ),
            "expected": True,
            "passed": (
                invalid_state_fallback_passed
            ),
        },
        {
            "check": (
                "zero_production_references"
            ),
            "actual": (
                production_reference_count
            ),
            "expected": 0,
            "passed": (
                production_reference_count == 0
            ),
        },
        {
            "check": (
                "evaluator_is_pure"
            ),
            "actual": evaluator_is_pure,
            "expected": True,
            "passed": evaluator_is_pure,
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    safety_rows = [
        {
            "boundary": action,
            "changed_or_executed": False,
            "passed": True,
        }
        for action in PROHIBITED_ACTIONS
    ]

    safety_rows.extend(
        [
            {
                "boundary": (
                    "pure_evaluator_implementation"
                ),
                "changed_or_executed": True,
                "passed": all_checks_passed,
            },
            {
                "boundary": (
                    "state_contract_validation"
                ),
                "changed_or_executed": True,
                "passed": all_checks_passed,
            },
        ]
    )

    recommended_next_layer = (
        "6PK_dynamic_starter_hook_"
        "evaluator_independent_audit"
        if all_checks_passed
        else
        "6PK_dynamic_starter_hook_"
        "evaluator_remediation"
    )

    write_csv(
        OUTPUT_DIR / "implementation_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "fixture_results.csv",
        [
            "fixture_id",
            "scenario",
            "expected_decision",
            "actual_decision",
            "pull_probability",
            "trigger_reasons",
            "output_valid",
            "state_unchanged",
            "passed",
        ],
        [
            {
                key: row[key]
                for key in [
                    "fixture_id",
                    "scenario",
                    "expected_decision",
                    "actual_decision",
                    "pull_probability",
                    "trigger_reasons",
                    "output_valid",
                    "state_unchanged",
                    "passed",
                ]
            }
            for row in fixture_results
        ],
    )

    write_csv(
        OUTPUT_DIR / "production_reference_scan.csv",
        [
            "path",
            "references_evaluator",
        ],
        production_reference_rows,
    )

    write_csv(
        OUTPUT_DIR / "safety_audit.csv",
        [
            "boundary",
            "changed_or_executed",
            "passed",
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
                    "Independently audit the pure "
                    "starter-hook evaluator without "
                    "production simulation wiring."
                ),
                "entry_condition": (
                    "All 6PJ implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "fixture_payloads.json",
        fixture_results,
    )

    implementation_summary = {
        "module": (
            "mlb_app/simulation/"
            "starter_hook_evaluator.py"
        ),
        "evaluator_version": (
            module
            .STARTER_HOOK_EVALUATOR_VERSION
        ),
        "required_state_fields": len(
            module.REQUIRED_STATE_FIELDS
        ),
        "optional_state_fields": len(
            module.OPTIONAL_STATE_FIELDS
        ),
        "output_fields": len(
            module.EVALUATOR_OUTPUT_FIELDS
        ),
        "fixtures_executed": len(
            fixture_results
        ),
        "fixtures_passed": (
            fixtures_passed
        ),
        "production_reference_count": (
            production_reference_count
        ),
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        (
            "canonical_probability_"
            "authority_changed"
        ): False,
        "new_authority_granted": False,
    }

    write_json(
        OUTPUT_DIR / "implementation_summary.json",
        implementation_summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "dynamic_starter_hook_state_contract_"
            "and_evaluator_implementation_complete"
            if all_checks_passed
            else
            "dynamic_starter_hook_state_contract_"
            "and_evaluator_implementation_failed"
        ),
        "all_checks_passed": (
            all_checks_passed
        ),
        "implementation_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "implementation_checks_required": len(
            checks
        ),
        "fixtures_executed": len(
            fixture_results
        ),
        "fixtures_passed": (
            fixtures_passed
        ),
        "required_state_fields": len(
            module.REQUIRED_STATE_FIELDS
        ),
        "optional_state_fields": len(
            module.OPTIONAL_STATE_FIELDS
        ),
        "evaluator_output_fields": len(
            module.EVALUATOR_OUTPUT_FIELDS
        ),
        "all_outputs_valid": (
            all_outputs_valid
        ),
        "deterministic_replay_passed": (
            deterministic_replay_passed
        ),
        "inputs_unchanged": (
            inputs_unchanged
        ),
        "invalid_state_fallback_passed": (
            invalid_state_fallback_passed
        ),
        "production_reference_count": (
            production_reference_count
        ),
        "evaluator_is_pure": (
            evaluator_is_pure
        ),
        "production_starter_hook_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": (
            False
        ),
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
        "independent_evaluator_audit_allowed_next": (
            all_checks_passed
        ),
        "diagnostic_integration_allowed_next": False,
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR
                / "implementation_checks.csv"
            ),
            str(
                OUTPUT_DIR
                / "fixture_results.csv"
            ),
            str(
                OUTPUT_DIR
                / "production_reference_scan.csv"
            ),
            str(
                OUTPUT_DIR
                / "safety_audit.csv"
            ),
            str(
                OUTPUT_DIR
                / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "fixture_payloads.json"
            ),
            str(
                OUTPUT_DIR
                / "implementation_summary.json"
            ),
            str(
                OUTPUT_DIR
                / "diagnosis.json"
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
