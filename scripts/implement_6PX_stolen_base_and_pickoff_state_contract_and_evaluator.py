#!/usr/bin/env python3
"""
Layer 6PX
Stolen-Base and Pickoff State Contract and Evaluator Implementation

Implements and validates a pure deterministic stolen-base and pickoff evaluator.

The evaluator is not connected to production simulation paths and has no
base-state, out-state, run-scoring, simulation, or probability authority.
"""

from __future__ import annotations

import ast
import csv
import importlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6PX"

LAYER_NAME = (
    "stolen_base_and_pickoff_state_"
    "contract_and_evaluator_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PX_stolen_base_and_pickoff_state_"
    "contract_and_evaluator_implementation"
)

PLAN_PATH = (
    ROOT
    / "scripts/plan_6PW_stolen_base_and_pickoff_"
    "state_inventory_and_implementation.py"
)

EVALUATOR_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "stolen_base_pickoff_evaluator.py"
)

SIMULATION_ROOT = (
    ROOT
    / "mlb_app/simulation"
)

EXPECTED_OUTPUT_FIELDS = {
    "steal_eligible",
    "attempt_recommendation",
    "attempt_probability",
    "success_probability",
    "pickoff_pressure",
    "pickoff_out_probability",
    "selection_reason",
    "fallback_used",
    "fallback_reason",
    "state_completeness",
    "behavioral_effect",
    "canonical_probability_authority_changed",
    "production_activation",
}


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
    return path.read_text(
        encoding="utf-8",
        errors="ignore",
    )


def static_plan_contract_passes() -> bool:
    if not PLAN_PATH.exists():
        return False

    text = read_text(
        PLAN_PATH
    )

    tree = ast.parse(
        text,
        filename=str(
            PLAN_PATH
        ),
    )

    strings = {
        node.value
        for node in ast.walk(
            tree
        )
        if isinstance(
            node,
            ast.Constant,
        )
        and isinstance(
            node.value,
            str,
        )
    }

    return all(
        [
            (
                "stolen_base_and_pickoff_state_"
                "inventory_and_implementation_"
                "plan_complete"
            )
            in strings,
            (
                "6PX_stolen_base_and_pickoff_"
                "state_contract_and_evaluator_"
                "implementation"
            )
            in strings,
            (
                "state_contract_and_evaluator_"
                "implementation_allowed_next"
            )
            in strings,
            (
                "production_behavior_"
                "integration_allowed_next"
            )
            in strings,
        ]
    )


def participant(
    participant_id: str,
    *,
    kind: str,
    evidence_complete: bool = True,
    strength: float = 0.50,
) -> dict[str, Any]:
    if kind == "runner":
        return {
            "runner_id": participant_id,
            "speed_score": strength,
            "attempt_rate": strength,
            "success_rate": strength,
            "lead_quality": strength,
            "fatigue_index": 0.10,
            "injury_limit_flag": False,
            "evidence_complete": (
                evidence_complete
            ),
        }

    if kind == "pitcher":
        return {
            "pitcher_id": participant_id,
            "throws": "R",
            "hold_score": strength,
            "delivery_time_score": (
                strength
            ),
            "pickoff_attempt_rate": (
                strength
            ),
            "pickoff_success_rate": (
                strength * 0.10
            ),
            "evidence_complete": (
                evidence_complete
            ),
        }

    return {
        "catcher_id": participant_id,
        "throws": "R",
        "throwing_score": strength,
        "pop_time_score": strength,
        "caught_stealing_rate": (
            strength
        ),
        "evidence_complete": (
            evidence_complete
        ),
    }


def base_state() -> dict[str, Any]:
    return {
        "inning": 5,
        "half": "top",
        "outs": 1,
        "base_state": {
            "first": True,
            "second": False,
            "third": False,
        },
        "score_margin": 0,
        "runner": participant(
            "RUNNER",
            kind="runner",
            strength=0.80,
        ),
        "origin_base": "first",
        "target_base": "second",
        "pitcher": participant(
            "PITCHER",
            kind="pitcher",
            strength=0.25,
        ),
        "catcher": participant(
            "CATCHER",
            kind="catcher",
            strength=0.25,
        ),
        "batter_id": "BATTER",
        "count": {
            "balls": 1,
            "strikes": 1,
        },
        "disengagements_used": 0,
        "game_date": "2026-06-23",
        "extra_inning_flag": False,
        "evidence_version": "6px-v1",
    }


def production_reference_rows() -> list[
    dict[str, Any]
]:
    rows: list[dict[str, Any]] = []

    for path in sorted(
        SIMULATION_ROOT.rglob(
            "*.py"
        )
    ):
        if path == EVALUATOR_PATH:
            continue

        text = read_text(
            path
        )

        matched = [
            token
            for token in [
                (
                    "stolen_base_"
                    "pickoff_evaluator"
                ),
                (
                    "evaluate_stolen_base_"
                    "and_pickoff_state"
                ),
                (
                    "validate_stolen_base_"
                    "and_pickoff_evaluation"
                ),
            ]
            if token in text
        ]

        if matched:
            rows.append(
                {
                    "path": str(
                        path.relative_to(
                            ROOT
                        )
                    ),
                    "matched_tokens": (
                        "|".join(
                            matched
                        )
                    ),
                }
            )

    return rows


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    required_files_exist = all(
        path.exists()
        for path in [
            PLAN_PATH,
            EVALUATOR_PATH,
        ]
    )

    plan_contract_passed = (
        static_plan_contract_passes()
    )

    module = importlib.import_module(
        "mlb_app.simulation."
        "stolen_base_pickoff_evaluator"
    )

    evaluate = (
        module
        .evaluate_stolen_base_and_pickoff_state
    )

    validate_state = (
        module
        .validate_stolen_base_and_pickoff_state
    )

    validate_output = (
        module
        .validate_stolen_base_and_pickoff_evaluation
    )

    evaluator_text = read_text(
        EVALUATOR_PATH
    )

    tree = ast.parse(
        evaluator_text,
        filename=str(
            EVALUATOR_PATH
        ),
    )

    imported_modules = sorted(
        {
            node.module
            for node in ast.walk(
                tree
            )
            if isinstance(
                node,
                ast.ImportFrom,
            )
            and node.module
            is not None
        }
        | {
            alias.name
            for node in ast.walk(
                tree
            )
            if isinstance(
                node,
                ast.Import,
            )
            for alias in node.names
        }
    )

    forbidden_imports = sorted(
        set(
            imported_modules
        )
        & {
            (
                "mlb_app.simulation."
                "game_engine_v2"
            ),
            (
                "mlb_app.simulation."
                "game_simulator"
            ),
            (
                "mlb_app.simulation."
                "game_simulation_builder"
            ),
            (
                "mlb_app.simulation."
                "inning_simulator"
            ),
            "pandas",
            "numpy",
            "requests",
            "random",
        }
    )

    production_references = (
        production_reference_rows()
    )

    complete_state = (
        base_state()
    )

    complete_result = evaluate(
        complete_state
    )

    occupied_state = (
        base_state()
    )

    occupied_state[
        "base_state"
    ][
        "second"
    ] = True

    occupied_result = evaluate(
        occupied_state
    )

    two_out_state = (
        base_state()
    )

    two_out_state["outs"] = 2

    two_out_result = evaluate(
        two_out_state
    )

    late_close_state = (
        base_state()
    )

    late_close_state.update(
        {
            "inning": 8,
            "score_margin": 1,
        }
    )

    late_close_result = evaluate(
        late_close_state
    )

    slow_runner_state = (
        base_state()
    )

    slow_runner_state[
        "runner"
    ] = participant(
        "SLOW",
        kind="runner",
        strength=0.10,
    )

    slow_runner_state[
        "pitcher"
    ] = participant(
        "STRONG_PITCHER",
        kind="pitcher",
        strength=0.90,
    )

    slow_runner_state[
        "catcher"
    ] = participant(
        "STRONG_CATCHER",
        kind="catcher",
        strength=0.90,
    )

    slow_result = evaluate(
        slow_runner_state
    )

    fast_runner_state = (
        base_state()
    )

    fast_runner_state[
        "runner"
    ] = participant(
        "FAST",
        kind="runner",
        strength=0.95,
    )

    fast_runner_state[
        "pitcher"
    ] = participant(
        "WEAK_PITCHER",
        kind="pitcher",
        strength=0.05,
    )

    fast_runner_state[
        "catcher"
    ] = participant(
        "WEAK_CATCHER",
        kind="catcher",
        strength=0.05,
    )

    fast_result = evaluate(
        fast_runner_state
    )

    partial_runner_state = (
        base_state()
    )

    partial_runner_state[
        "runner"
    ][
        "evidence_complete"
    ] = False

    partial_runner_result = evaluate(
        partial_runner_state
    )

    partial_battery_state = (
        base_state()
    )

    partial_battery_state[
        "pitcher"
    ][
        "evidence_complete"
    ] = False

    partial_battery_result = evaluate(
        partial_battery_state
    )

    invalid_state = (
        base_state()
    )

    invalid_state.pop(
        "base_state"
    )

    invalid_result = evaluate(
        invalid_state
    )

    immutable_state = (
        base_state()
    )

    immutable_original = deepcopy(
        immutable_state
    )

    immutable_result_one = evaluate(
        immutable_state
    )

    immutable_result_two = evaluate(
        immutable_state
    )

    fixtures = [
        {
            "fixture_id": "PX-F01",
            "scenario": (
                "complete_eligible_state"
            ),
            "passed": all(
                [
                    complete_result[
                        "steal_eligible"
                    ]
                    is True,
                    complete_result[
                        "state_completeness"
                    ]
                    == "complete",
                    complete_result[
                        "fallback_used"
                    ]
                    is False,
                    complete_result[
                        "attempt_recommendation"
                    ]
                    in {
                        "attempt",
                        "hold",
                    },
                ]
            ),
        },
        {
            "fixture_id": "PX-F02",
            "scenario": (
                "occupied_target_ineligible"
            ),
            "passed": all(
                [
                    occupied_result[
                        "steal_eligible"
                    ]
                    is False,
                    occupied_result[
                        "attempt_recommendation"
                    ]
                    == "hold",
                    occupied_result[
                        "selection_reason"
                    ]
                    == "target_base_occupied",
                ]
            ),
        },
        {
            "fixture_id": "PX-F03",
            "scenario": (
                "two_out_context_directionality"
            ),
            "passed": (
                two_out_result[
                    "attempt_probability"
                ]
                <= complete_result[
                    "attempt_probability"
                ]
            ),
        },
        {
            "fixture_id": "PX-F04",
            "scenario": (
                "late_close_context_directionality"
            ),
            "passed": (
                late_close_result[
                    "attempt_probability"
                ]
                >= complete_result[
                    "attempt_probability"
                ]
            ),
        },
        {
            "fixture_id": "PX-F05",
            "scenario": (
                "slow_runner_strong_battery"
            ),
            "passed": all(
                [
                    slow_result[
                        "attempt_recommendation"
                    ]
                    == "hold",
                    slow_result[
                        "attempt_probability"
                    ]
                    < fast_result[
                        "attempt_probability"
                    ],
                    slow_result[
                        "success_probability"
                    ]
                    < fast_result[
                        "success_probability"
                    ],
                ]
            ),
        },
        {
            "fixture_id": "PX-F06",
            "scenario": (
                "fast_runner_weak_battery"
            ),
            "passed": all(
                [
                    fast_result[
                        "attempt_recommendation"
                    ]
                    == "attempt",
                    fast_result[
                        "steal_eligible"
                    ]
                    is True,
                ]
            ),
        },
        {
            "fixture_id": "PX-F07",
            "scenario": (
                "partial_runner_evidence"
            ),
            "passed": all(
                [
                    partial_runner_result[
                        "state_completeness"
                    ]
                    == "partial",
                    partial_runner_result[
                        "fallback_used"
                    ]
                    is True,
                    partial_runner_result[
                        "attempt_recommendation"
                    ]
                    == "unknown_fallback",
                ]
            ),
        },
        {
            "fixture_id": "PX-F08",
            "scenario": (
                "partial_battery_evidence"
            ),
            "passed": all(
                [
                    partial_battery_result[
                        "state_completeness"
                    ]
                    == "partial",
                    partial_battery_result[
                        "fallback_used"
                    ]
                    is True,
                ]
            ),
        },
        {
            "fixture_id": "PX-F09",
            "scenario": (
                "invalid_state"
            ),
            "passed": all(
                [
                    invalid_result[
                        "state_completeness"
                    ]
                    == "invalid",
                    invalid_result[
                        "steal_eligible"
                    ]
                    is False,
                    invalid_result[
                        "production_activation"
                    ]
                    is False,
                ]
            ),
        },
        {
            "fixture_id": "PX-F10",
            "scenario": (
                "repeatability_and_immutability"
            ),
            "passed": all(
                [
                    immutable_state
                    == immutable_original,
                    immutable_result_one
                    == immutable_result_two,
                ]
            ),
        },
    ]

    fixtures_passed = sum(
        1
        for fixture in fixtures
        if fixture["passed"]
    )

    evaluated_results = [
        complete_result,
        occupied_result,
        two_out_result,
        late_close_result,
        slow_result,
        fast_result,
        partial_runner_result,
        partial_battery_result,
        invalid_result,
        immutable_result_one,
    ]

    output_contract_valid = all(
        set(result)
        == EXPECTED_OUTPUT_FIELDS
        and validate_output(
            result
        ).get(
            "valid"
        )
        is True
        for result in evaluated_results
    )

    state_contract_valid = all(
        [
            validate_state(
                complete_state
            ).get(
                "state_completeness"
            )
            == "complete",
            validate_state(
                partial_runner_state
            ).get(
                "state_completeness"
            )
            == "partial",
            validate_state(
                invalid_state
            ).get(
                "state_completeness"
            )
            == "invalid",
        ]
    )

    deterministic_directionality = all(
        [
            slow_result[
                "attempt_probability"
            ]
            < fast_result[
                "attempt_probability"
            ],
            slow_result[
                "success_probability"
            ]
            < fast_result[
                "success_probability"
            ],
            slow_result[
                "pickoff_out_probability"
            ]
            >= fast_result[
                "pickoff_out_probability"
            ],
        ]
    )

    safety_contract_valid = all(
        result[
            "behavioral_effect"
        ]
        == "none"
        and result[
            "canonical_probability_"
            "authority_changed"
        ]
        is False
        and result[
            "production_activation"
        ]
        is False
        for result in evaluated_results
    )

    checks = [
        {
            "check": (
                "required_files_exist"
            ),
            "actual": (
                required_files_exist
            ),
            "expected": True,
            "passed": (
                required_files_exist
            ),
        },
        {
            "check": (
                "six_pw_plan_contract_passed"
            ),
            "actual": (
                plan_contract_passed
            ),
            "expected": True,
            "passed": (
                plan_contract_passed
            ),
        },
        {
            "check": (
                "forbidden_imports_absent"
            ),
            "actual": len(
                forbidden_imports
            ),
            "expected": 0,
            "passed": not forbidden_imports,
        },
        {
            "check": (
                "production_references_absent"
            ),
            "actual": len(
                production_references
            ),
            "expected": 0,
            "passed": not production_references,
        },
        {
            "check": (
                "state_contract_valid"
            ),
            "actual": (
                state_contract_valid
            ),
            "expected": True,
            "passed": (
                state_contract_valid
            ),
        },
        {
            "check": (
                "output_contract_valid"
            ),
            "actual": (
                output_contract_valid
            ),
            "expected": True,
            "passed": (
                output_contract_valid
            ),
        },
        {
            "check": (
                "deterministic_directionality"
            ),
            "actual": (
                deterministic_directionality
            ),
            "expected": True,
            "passed": (
                deterministic_directionality
            ),
        },
        {
            "check": (
                "eligibility_guard_valid"
            ),
            "actual": (
                occupied_result[
                    "steal_eligible"
                ]
                is False
            ),
            "expected": True,
            "passed": (
                occupied_result[
                    "steal_eligible"
                ]
                is False
            ),
        },
        {
            "check": (
                "fallback_behavior_valid"
            ),
            "actual": all(
                [
                    partial_runner_result[
                        "fallback_used"
                    ]
                    is True,
                    partial_battery_result[
                        "fallback_used"
                    ]
                    is True,
                    invalid_result[
                        "fallback_used"
                    ]
                    is True,
                ]
            ),
            "expected": True,
            "passed": all(
                [
                    partial_runner_result[
                        "fallback_used"
                    ]
                    is True,
                    partial_battery_result[
                        "fallback_used"
                    ]
                    is True,
                    invalid_result[
                        "fallback_used"
                    ]
                    is True,
                ]
            ),
        },
        {
            "check": (
                "repeatability_and_immutability"
            ),
            "actual": all(
                [
                    immutable_state
                    == immutable_original,
                    immutable_result_one
                    == immutable_result_two,
                ]
            ),
            "expected": True,
            "passed": all(
                [
                    immutable_state
                    == immutable_original,
                    immutable_result_one
                    == immutable_result_two,
                ]
            ),
        },
        {
            "check": (
                "safety_contract_valid"
            ),
            "actual": (
                safety_contract_valid
            ),
            "expected": True,
            "passed": (
                safety_contract_valid
            ),
        },
        {
            "check": (
                "ten_fixtures_pass"
            ),
            "actual": (
                fixtures_passed
            ),
            "expected": 10,
            "passed": (
                fixtures_passed
                == 10
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    write_csv(
        OUTPUT_DIR
        / "implementation_checks.csv",
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
        / "fixture_results.csv",
        [
            "fixture_id",
            "scenario",
            "passed",
        ],
        fixtures,
    )

    write_csv(
        OUTPUT_DIR
        / "production_reference_scan.csv",
        [
            "path",
            "matched_tokens",
        ],
        production_references,
    )

    write_json(
        OUTPUT_DIR
        / "evaluations.json",
        {
            "complete": (
                complete_result
            ),
            "occupied_target": (
                occupied_result
            ),
            "two_out": (
                two_out_result
            ),
            "late_close": (
                late_close_result
            ),
            "slow_runner": (
                slow_result
            ),
            "fast_runner": (
                fast_result
            ),
            "partial_runner": (
                partial_runner_result
            ),
            "partial_battery": (
                partial_battery_result
            ),
            "invalid": (
                invalid_result
            ),
        },
    )

    summary = {
        "implementation_checks_required": len(
            checks
        ),
        "implementation_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "fixtures_required": 10,
        "fixtures_passed": (
            fixtures_passed
        ),
        "six_pw_plan_contract_passed": (
            plan_contract_passed
        ),
        "state_contract_valid": (
            state_contract_valid
        ),
        "output_contract_valid": (
            output_contract_valid
        ),
        "deterministic_directionality": (
            deterministic_directionality
        ),
        "repeatability_and_immutability": all(
            [
                immutable_state
                == immutable_original,
                immutable_result_one
                == immutable_result_two,
            ]
        ),
        "production_reference_count": len(
            production_references
        ),
        "forbidden_import_count": len(
            forbidden_imports
        ),
        "production_behavior_changed": False,
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
        / "implementation_summary.json",
        summary,
    )

    recommended_next_layer = (
        "6PY_stolen_base_and_pickoff_state_"
        "evaluator_independent_audit"
        if all_checks_passed
        else
        "6PY_stolen_base_and_pickoff_state_"
        "evaluator_remediation"
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "stolen_base_and_pickoff_state_"
            "contract_and_evaluator_"
            "implementation_complete"
            if all_checks_passed
            else
            "stolen_base_and_pickoff_state_"
            "contract_and_evaluator_"
            "implementation_incomplete"
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
        "fixtures_passed": (
            fixtures_passed
        ),
        "fixtures_required": 10,
        "six_pw_plan_contract_passed": (
            plan_contract_passed
        ),
        "state_contract_valid": (
            state_contract_valid
        ),
        "output_contract_valid": (
            output_contract_valid
        ),
        "deterministic_directionality": (
            deterministic_directionality
        ),
        "repeatability_and_immutability": all(
            [
                immutable_state
                == immutable_original,
                immutable_result_one
                == immutable_result_two,
            ]
        ),
        "production_reference_count": len(
            production_references
        ),
        "forbidden_import_count": len(
            forbidden_imports
        ),
        "production_baserunning_changed": False,
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
        "independent_evaluator_audit_allowed_next": (
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
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "evaluations.json"
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
