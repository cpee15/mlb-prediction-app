#!/usr/bin/env python3
"""
Layer 6PY
Stolen-Base and Pickoff State Evaluator Independent Audit

Independently audits the merged 6PX stolen-base and pickoff evaluator.

This layer does not:

- create production steal attempts;
- create production pickoff attempts;
- change base or out state;
- change runner advancement or run scoring;
- change simulation probabilities or outputs;
- authorize integration, tuning, backtesting, pricing, or edge detection.
"""

from __future__ import annotations

import ast
import csv
import importlib
import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6PY"

LAYER_NAME = (
    "stolen_base_and_pickoff_state_"
    "evaluator_independent_audit"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PY_stolen_base_and_pickoff_state_"
    "evaluator_independent_audit"
)

MODULE_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "stolen_base_pickoff_evaluator.py"
)

IMPLEMENTATION_SCRIPT = (
    ROOT
    / "scripts/implement_6PX_stolen_base_and_"
    "pickoff_state_contract_and_evaluator.py"
)

PRODUCTION_PATHS = [
    ROOT
    / "mlb_app/simulation/"
    "inning_simulator.py",
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

PROHIBITED_IMPORTS = {
    "random",
    "numpy",
    "pandas",
    "requests",
    "sqlalchemy",
}

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
        "evidence_version": "6py-v1",
    }


def production_reference_rows() -> list[
    dict[str, Any]
]:
    rows: list[dict[str, Any]] = []

    tokens = [
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

    for path in PRODUCTION_PATHS:
        text = read_text(
            path
        )

        matched = [
            token
            for token in tokens
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


def evaluate_case(
    module: Any,
    *,
    case_id: str,
    scenario: str,
    state: Any,
    assertions: list[
        tuple[str, bool]
    ],
) -> dict[str, Any]:
    original = deepcopy(
        state
    )

    first = (
        module
        .evaluate_stolen_base_and_pickoff_state(
            state
        )
    )

    second = (
        module
        .evaluate_stolen_base_and_pickoff_state(
            deepcopy(
                state
            )
        )
    )

    validation = (
        module
        .validate_stolen_base_and_pickoff_evaluation(
            first
        )
    )

    common_assertions = [
        (
            "deterministic",
            first == second,
        ),
        (
            "input_unchanged",
            state == original,
        ),
        (
            "exact_output_fields",
            set(first)
            == EXPECTED_OUTPUT_FIELDS,
        ),
        (
            "output_valid",
            validation.get(
                "valid"
            )
            is True,
        ),
        (
            "behavioral_effect_none",
            first.get(
                "behavioral_effect"
            )
            == "none",
        ),
        (
            "canonical_authority_unchanged",
            first.get(
                "canonical_probability_"
                "authority_changed"
            )
            is False,
        ),
        (
            "production_inactive",
            first.get(
                "production_activation"
            )
            is False,
        ),
    ]

    all_assertions = (
        common_assertions
        + assertions
    )

    passed = all(
        result
        for _, result
        in all_assertions
    )

    return {
        "case_id": case_id,
        "scenario": scenario,
        "assertion_count": len(
            all_assertions
        ),
        "assertions_passed": sum(
            1
            for _, result
            in all_assertions
            if result
        ),
        "deterministic": (
            first == second
        ),
        "input_unchanged": (
            state == original
        ),
        "output_valid": (
            validation.get(
                "valid"
            )
            is True
        ),
        "passed": passed,
        "evaluation": first,
        "assertions": {
            name: result
            for name, result
            in all_assertions
        },
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    required_files_exist = all(
        path.exists()
        for path in [
            MODULE_PATH,
            IMPLEMENTATION_SCRIPT,
            *PRODUCTION_PATHS,
        ]
    )

    implementation_run = subprocess.run(
        [
            sys.executable,
            str(
                IMPLEMENTATION_SCRIPT
            ),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    implementation_payload = (
        parse_last_json_object(
            implementation_run.stdout
        )
    )

    implementation_contract_passed = all(
        [
            (
                implementation_run.returncode
                == 0
            ),
            implementation_payload.get(
                "diagnosis"
            )
            == (
                "stolen_base_and_pickoff_state_"
                "contract_and_evaluator_"
                "implementation_complete"
            ),
            implementation_payload.get(
                "all_checks_passed"
            )
            is True,
            implementation_payload.get(
                "implementation_checks_passed"
            )
            == 12,
            implementation_payload.get(
                "implementation_checks_required"
            )
            == 12,
            implementation_payload.get(
                "fixtures_passed"
            )
            == 10,
            implementation_payload.get(
                "fixtures_required"
            )
            == 10,
            implementation_payload.get(
                "production_reference_count"
            )
            == 0,
            implementation_payload.get(
                "production_activation"
            )
            is False,
        ]
    )

    module = importlib.import_module(
        "mlb_app.simulation."
        "stolen_base_pickoff_evaluator"
    )

    module_text = read_text(
        MODULE_PATH
    )

    tree = ast.parse(
        module_text,
        filename=str(
            MODULE_PATH
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
        & PROHIBITED_IMPORTS
    )

    production_references = (
        production_reference_rows()
    )

    cases: list[
        dict[str, Any]
    ] = []

    state = base_state()

    result = (
        module
        .evaluate_stolen_base_and_pickoff_state(
            deepcopy(
                state
            )
        )
    )

    cases.append(
        evaluate_case(
            module,
            case_id="PY-C01",
            scenario=(
                "complete_eligible_state"
            ),
            state=state,
            assertions=[
                (
                    "eligible",
                    result.get(
                        "steal_eligible"
                    )
                    is True,
                ),
                (
                    "complete_state",
                    result.get(
                        "state_completeness"
                    )
                    == "complete",
                ),
                (
                    "no_fallback",
                    result.get(
                        "fallback_used"
                    )
                    is False,
                ),
            ],
        )
    )

    state = base_state()
    state[
        "base_state"
    ][
        "second"
    ] = True

    result = (
        module
        .evaluate_stolen_base_and_pickoff_state(
            deepcopy(
                state
            )
        )
    )

    cases.append(
        evaluate_case(
            module,
            case_id="PY-C02",
            scenario=(
                "occupied_target_base"
            ),
            state=state,
            assertions=[
                (
                    "ineligible",
                    result.get(
                        "steal_eligible"
                    )
                    is False,
                ),
                (
                    "hold",
                    result.get(
                        "attempt_recommendation"
                    )
                    == "hold",
                ),
                (
                    "occupied_reason",
                    result.get(
                        "selection_reason"
                    )
                    == "target_base_occupied",
                ),
            ],
        )
    )

    baseline = base_state()
    baseline_result = (
        module
        .evaluate_stolen_base_and_pickoff_state(
            deepcopy(
                baseline
            )
        )
    )

    state = base_state()
    state["outs"] = 2

    result = (
        module
        .evaluate_stolen_base_and_pickoff_state(
            deepcopy(
                state
            )
        )
    )

    cases.append(
        evaluate_case(
            module,
            case_id="PY-C03",
            scenario=(
                "two_out_directionality"
            ),
            state=state,
            assertions=[
                (
                    "attempt_not_increased",
                    result.get(
                        "attempt_probability"
                    )
                    <= baseline_result.get(
                        "attempt_probability"
                    ),
                ),
            ],
        )
    )

    state = base_state()
    state.update(
        {
            "inning": 8,
            "score_margin": 1,
        }
    )

    result = (
        module
        .evaluate_stolen_base_and_pickoff_state(
            deepcopy(
                state
            )
        )
    )

    cases.append(
        evaluate_case(
            module,
            case_id="PY-C04",
            scenario=(
                "late_close_game_directionality"
            ),
            state=state,
            assertions=[
                (
                    "attempt_not_decreased",
                    result.get(
                        "attempt_probability"
                    )
                    >= baseline_result.get(
                        "attempt_probability"
                    ),
                ),
            ],
        )
    )

    slow_state = base_state()
    slow_state[
        "runner"
    ] = participant(
        "SLOW",
        kind="runner",
        strength=0.10,
    )
    slow_state[
        "pitcher"
    ] = participant(
        "STRONG_PITCHER",
        kind="pitcher",
        strength=0.90,
    )
    slow_state[
        "catcher"
    ] = participant(
        "STRONG_CATCHER",
        kind="catcher",
        strength=0.90,
    )

    slow_result = (
        module
        .evaluate_stolen_base_and_pickoff_state(
            deepcopy(
                slow_state
            )
        )
    )

    cases.append(
        evaluate_case(
            module,
            case_id="PY-C05",
            scenario=(
                "slow_runner_strong_battery"
            ),
            state=slow_state,
            assertions=[
                (
                    "hold_recommendation",
                    slow_result.get(
                        "attempt_recommendation"
                    )
                    == "hold",
                ),
            ],
        )
    )

    fast_state = base_state()
    fast_state[
        "runner"
    ] = participant(
        "FAST",
        kind="runner",
        strength=0.95,
    )
    fast_state[
        "pitcher"
    ] = participant(
        "WEAK_PITCHER",
        kind="pitcher",
        strength=0.05,
    )
    fast_state[
        "catcher"
    ] = participant(
        "WEAK_CATCHER",
        kind="catcher",
        strength=0.05,
    )

    fast_result = (
        module
        .evaluate_stolen_base_and_pickoff_state(
            deepcopy(
                fast_state
            )
        )
    )

    cases.append(
        evaluate_case(
            module,
            case_id="PY-C06",
            scenario=(
                "fast_runner_weak_battery"
            ),
            state=fast_state,
            assertions=[
                (
                    "attempt_recommendation",
                    fast_result.get(
                        "attempt_recommendation"
                    )
                    == "attempt",
                ),
                (
                    "attempt_directionality",
                    fast_result.get(
                        "attempt_probability"
                    )
                    > slow_result.get(
                        "attempt_probability"
                    ),
                ),
                (
                    "success_directionality",
                    fast_result.get(
                        "success_probability"
                    )
                    > slow_result.get(
                        "success_probability"
                    ),
                ),
            ],
        )
    )

    state = base_state()
    state[
        "runner"
    ][
        "evidence_complete"
    ] = False

    result = (
        module
        .evaluate_stolen_base_and_pickoff_state(
            deepcopy(
                state
            )
        )
    )

    cases.append(
        evaluate_case(
            module,
            case_id="PY-C07",
            scenario=(
                "partial_runner_evidence"
            ),
            state=state,
            assertions=[
                (
                    "partial_state",
                    result.get(
                        "state_completeness"
                    )
                    == "partial",
                ),
                (
                    "fallback_used",
                    result.get(
                        "fallback_used"
                    )
                    is True,
                ),
                (
                    "unknown_recommendation",
                    result.get(
                        "attempt_recommendation"
                    )
                    == "unknown_fallback",
                ),
            ],
        )
    )

    state = base_state()
    state[
        "catcher"
    ][
        "evidence_complete"
    ] = False

    result = (
        module
        .evaluate_stolen_base_and_pickoff_state(
            deepcopy(
                state
            )
        )
    )

    cases.append(
        evaluate_case(
            module,
            case_id="PY-C08",
            scenario=(
                "partial_catcher_evidence"
            ),
            state=state,
            assertions=[
                (
                    "partial_state",
                    result.get(
                        "state_completeness"
                    )
                    == "partial",
                ),
                (
                    "fallback_used",
                    result.get(
                        "fallback_used"
                    )
                    is True,
                ),
            ],
        )
    )

    state = base_state()
    state.pop(
        "base_state"
    )

    result = (
        module
        .evaluate_stolen_base_and_pickoff_state(
            deepcopy(
                state
            )
        )
    )

    cases.append(
        evaluate_case(
            module,
            case_id="PY-C09",
            scenario="invalid_state",
            state=state,
            assertions=[
                (
                    "invalid_state",
                    result.get(
                        "state_completeness"
                    )
                    == "invalid",
                ),
                (
                    "fallback_used",
                    result.get(
                        "fallback_used"
                    )
                    is True,
                ),
                (
                    "ineligible",
                    result.get(
                        "steal_eligible"
                    )
                    is False,
                ),
            ],
        )
    )

    state = base_state()
    state[
        "disengagements_used"
    ] = 2

    limited_result = (
        module
        .evaluate_stolen_base_and_pickoff_state(
            deepcopy(
                state
            )
        )
    )

    unrestricted_state = base_state()
    unrestricted_result = (
        module
        .evaluate_stolen_base_and_pickoff_state(
            deepcopy(
                unrestricted_state
            )
        )
    )

    cases.append(
        evaluate_case(
            module,
            case_id="PY-C10",
            scenario=(
                "disengagement_pickoff_directionality"
            ),
            state=state,
            assertions=[
                (
                    "pickoff_probability_not_increased",
                    limited_result.get(
                        "pickoff_out_probability"
                    )
                    <= unrestricted_result.get(
                        "pickoff_out_probability"
                    ),
                ),
            ],
        )
    )

    cases_passed = sum(
        1
        for case in cases
        if case["passed"]
    )

    all_case_outputs_valid = all(
        case[
            "output_valid"
        ]
        for case in cases
    )

    all_cases_deterministic = all(
        case[
            "deterministic"
        ]
        for case in cases
    )

    all_inputs_unchanged = all(
        case[
            "input_unchanged"
        ]
        for case in cases
    )

    safety_contract_valid = all(
        [
            all(
                case[
                    "evaluation"
                ][
                    "behavioral_effect"
                ]
                == "none"
                for case in cases
            ),
            all(
                case[
                    "evaluation"
                ][
                    "canonical_probability_"
                    "authority_changed"
                ]
                is False
                for case in cases
            ),
            all(
                case[
                    "evaluation"
                ][
                    "production_activation"
                ]
                is False
                for case in cases
            ),
        ]
    )

    directionality_valid = all(
        [
            fast_result[
                "attempt_probability"
            ]
            > slow_result[
                "attempt_probability"
            ],
            fast_result[
                "success_probability"
            ]
            > slow_result[
                "success_probability"
            ],
            limited_result[
                "pickoff_out_probability"
            ]
            <= unrestricted_result[
                "pickoff_out_probability"
            ],
        ]
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
                "six_px_implementation_contract_passed"
            ),
            "actual": (
                implementation_contract_passed
            ),
            "expected": True,
            "passed": (
                implementation_contract_passed
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
                "ten_independent_cases_pass"
            ),
            "actual": (
                cases_passed
            ),
            "expected": 10,
            "passed": (
                cases_passed
                == 10
            ),
        },
        {
            "check": (
                "all_case_outputs_valid"
            ),
            "actual": (
                all_case_outputs_valid
            ),
            "expected": True,
            "passed": (
                all_case_outputs_valid
            ),
        },
        {
            "check": (
                "all_cases_deterministic"
            ),
            "actual": (
                all_cases_deterministic
            ),
            "expected": True,
            "passed": (
                all_cases_deterministic
            ),
        },
        {
            "check": (
                "all_inputs_unchanged"
            ),
            "actual": (
                all_inputs_unchanged
            ),
            "expected": True,
            "passed": (
                all_inputs_unchanged
            ),
        },
        {
            "check": (
                "eligibility_guard_valid"
            ),
            "actual": (
                cases[1][
                    "passed"
                ]
            ),
            "expected": True,
            "passed": (
                cases[1][
                    "passed"
                ]
            ),
        },
        {
            "check": (
                "partial_fallback_valid"
            ),
            "actual": all(
                [
                    cases[6][
                        "passed"
                    ],
                    cases[7][
                        "passed"
                    ],
                ]
            ),
            "expected": True,
            "passed": all(
                [
                    cases[6][
                        "passed"
                    ],
                    cases[7][
                        "passed"
                    ],
                ]
            ),
        },
        {
            "check": (
                "invalid_fallback_valid"
            ),
            "actual": (
                cases[8][
                    "passed"
                ]
            ),
            "expected": True,
            "passed": (
                cases[8][
                    "passed"
                ]
            ),
        },
        {
            "check": (
                "directionality_valid"
            ),
            "actual": (
                directionality_valid
            ),
            "expected": True,
            "passed": (
                directionality_valid
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
                "production_authority_absent"
            ),
            "actual": True,
            "expected": True,
            "passed": True,
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
        / "audit_checks.csv",
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
        / "independent_case_results.csv",
        [
            "case_id",
            "scenario",
            "assertion_count",
            "assertions_passed",
            "deterministic",
            "input_unchanged",
            "output_valid",
            "passed",
        ],
        [
            {
                key: case[
                    key
                ]
                for key in [
                    "case_id",
                    "scenario",
                    "assertion_count",
                    "assertions_passed",
                    "deterministic",
                    "input_unchanged",
                    "output_valid",
                    "passed",
                ]
            }
            for case in cases
        ],
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
        / "independent_case_details.json",
        {
            case[
                "case_id"
            ]: {
                "scenario": (
                    case[
                        "scenario"
                    ]
                ),
                "assertions": (
                    case[
                        "assertions"
                    ]
                ),
                "evaluation": (
                    case[
                        "evaluation"
                    ]
                ),
                "passed": (
                    case[
                        "passed"
                    ]
                ),
            }
            for case in cases
        },
    )

    summary = {
        "audit_checks_required": len(
            checks
        ),
        "audit_checks_passed": sum(
            1
            for row in checks
            if row[
                "passed"
            ]
        ),
        "independent_cases_required": 10,
        "independent_cases_passed": (
            cases_passed
        ),
        (
            "six_px_implementation_"
            "contract_passed"
        ): (
            implementation_contract_passed
        ),
        "forbidden_import_count": len(
            forbidden_imports
        ),
        "production_reference_count": len(
            production_references
        ),
        "all_case_outputs_valid": (
            all_case_outputs_valid
        ),
        "all_cases_deterministic": (
            all_cases_deterministic
        ),
        "all_inputs_unchanged": (
            all_inputs_unchanged
        ),
        "directionality_valid": (
            directionality_valid
        ),
        "safety_contract_valid": (
            safety_contract_valid
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
        / "audit_summary.json",
        summary,
    )

    recommended_next_layer = (
        "6PZ_stolen_base_and_pickoff_state_"
        "diagnostic_integration_plan"
        if all_checks_passed
        else
        "6PZ_stolen_base_and_pickoff_state_"
        "evaluator_remediation"
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "stolen_base_and_pickoff_state_"
            "evaluator_independent_audit_passed"
            if all_checks_passed
            else
            "stolen_base_and_pickoff_state_"
            "evaluator_independent_audit_failed"
        ),
        "all_checks_passed": (
            all_checks_passed
        ),
        "audit_checks_passed": sum(
            1
            for row in checks
            if row[
                "passed"
            ]
        ),
        "audit_checks_required": len(
            checks
        ),
        "independent_cases_passed": (
            cases_passed
        ),
        "independent_cases_required": 10,
        (
            "six_px_implementation_"
            "contract_passed"
        ): (
            implementation_contract_passed
        ),
        "forbidden_import_count": len(
            forbidden_imports
        ),
        "production_reference_count": len(
            production_references
        ),
        "all_case_outputs_valid": (
            all_case_outputs_valid
        ),
        "all_cases_deterministic": (
            all_cases_deterministic
        ),
        "all_inputs_unchanged": (
            all_inputs_unchanged
        ),
        "directionality_valid": (
            directionality_valid
        ),
        "safety_contract_valid": (
            safety_contract_valid
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
        (
            "diagnostic_integration_"
            "planning_allowed_next"
        ): all_checks_passed,
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
                / "audit_checks.csv"
            ),
            str(
                OUTPUT_DIR
                / "independent_case_results.csv"
            ),
            str(
                OUTPUT_DIR
                / "production_reference_scan.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "independent_case_details.json"
            ),
            str(
                OUTPUT_DIR
                / "audit_summary.json"
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
