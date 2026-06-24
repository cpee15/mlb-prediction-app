#!/usr/bin/env python3
"""
Layer 6QF
Position-Player Substitution Evaluator Independent Audit

Independently audits the merged 6QE position-player substitution evaluator.

This layer does not:

- activate production substitutions;
- alter batting order or lineup slots;
- alter defensive alignment or designated-hitter state;
- alter runner identity or base/out state;
- alter simulation probabilities or outputs;
- authorize integration, validation, tuning, backtesting, pricing, or edge
  detection.
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


LAYER_ID = "6QF"

LAYER_NAME = (
    "position_player_substitution_"
    "evaluator_independent_audit"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6QF_position_player_substitution_"
    "evaluator_independent_audit"
)

MODULE_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "position_player_substitution_evaluator.py"
)

IMPLEMENTATION_SCRIPT = (
    ROOT
    / "scripts/implement_6QE_position_player_"
    "substitution_state_contract_and_evaluator.py"
)

PRODUCTION_PATHS = [
    ROOT / "mlb_app/simulation/inning_simulator.py",
    ROOT / "mlb_app/simulation/game_engine_v2.py",
    ROOT / "mlb_app/simulation/game_simulator.py",
    ROOT / "mlb_app/simulation/game_simulation_builder.py",
]

PROHIBITED_IMPORTS = {
    "random",
    "numpy",
    "pandas",
    "requests",
    "sqlalchemy",
}

EXPECTED_OUTPUT_FIELDS = {
    "substitution_eligible",
    "recommended_action",
    "recommended_player_id",
    "substitution_type",
    "candidate_score",
    "current_player_score",
    "selection_reason",
    "lineup_constraint_valid",
    "fallback_used",
    "fallback_reason",
    "state_completeness",
    "behavioral_effect",
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


def player(
    player_id: str,
    *,
    active: bool = True,
    already_used: bool = False,
    position: str = "LF",
    eligible_positions: list[str] | None = None,
    offense: float = 0.50,
    running: float = 0.50,
    defense: float = 0.50,
    evidence_complete: bool = True,
) -> dict[str, Any]:
    return {
        "player_id": player_id,
        "active": active,
        "already_used": already_used,
        "primary_position": position,
        "eligible_positions": (
            eligible_positions
            if eligible_positions is not None
            else [position]
        ),
        "bats": "R",
        "offense_score": offense,
        "running_score": running,
        "defense_score": defense,
        "evidence_complete": evidence_complete,
    }


def base_state(
    substitution_type: str = "pinch_hitter",
) -> dict[str, Any]:
    return {
        "inning": 8,
        "half": "bottom",
        "outs": 1,
        "score_margin": -1,
        "base_state": {
            "first": None,
            "second": None,
            "third": None,
        },
        "substitution_type": substitution_type,
        "current_player": player(
            "CURRENT",
            offense=0.35,
            running=0.35,
            defense=0.35,
        ),
        "candidate_players": [
            player(
                "BENCH_A",
                offense=0.85,
                running=0.80,
                defense=0.75,
            ),
            player(
                "BENCH_B",
                offense=0.65,
                running=0.60,
                defense=0.70,
            ),
        ],
        "batting_order": [
            f"PLAYER_{index}"
            for index in range(1, 10)
        ],
        "current_lineup_slot": 5,
        "defensive_alignment": {
            "LF": "CURRENT",
        },
        "used_player_ids": [],
        "designated_hitter_active": False,
        "injury_required": False,
        "evidence_version": "6qf-v1",
    }


def production_reference_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    tokens = [
        "position_player_substitution_evaluator",
        "evaluate_position_player_substitution",
        "validate_position_player_substitution_evaluation",
    ]

    for path in PRODUCTION_PATHS:
        text = read_text(path)

        matched = [
            token
            for token in tokens
            if token in text
        ]

        if matched:
            rows.append(
                {
                    "path": str(
                        path.relative_to(ROOT)
                    ),
                    "matched_tokens": "|".join(
                        matched
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
    assertions: list[tuple[str, bool]],
) -> dict[str, Any]:
    original = deepcopy(state)

    first = (
        module
        .evaluate_position_player_substitution(
            state
        )
    )

    second = (
        module
        .evaluate_position_player_substitution(
            deepcopy(state)
        )
    )

    validation = (
        module
        .validate_position_player_substitution_evaluation(
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
            set(first) == EXPECTED_OUTPUT_FIELDS,
        ),
        (
            "output_valid",
            validation.get("valid") is True,
        ),
        (
            "behavioral_effect_none",
            first.get("behavioral_effect") == "none",
        ),
        (
            "production_inactive",
            first.get("production_activation") is False,
        ),
    ]

    all_assertions = (
        common_assertions
        + assertions
    )

    passed = all(
        result
        for _, result in all_assertions
    )

    return {
        "case_id": case_id,
        "scenario": scenario,
        "assertion_count": len(
            all_assertions
        ),
        "assertions_passed": sum(
            1
            for _, result in all_assertions
            if result
        ),
        "deterministic": first == second,
        "input_unchanged": state == original,
        "output_valid": (
            validation.get("valid") is True
        ),
        "passed": passed,
        "evaluation": first,
        "assertions": {
            name: result
            for name, result in all_assertions
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
            str(IMPLEMENTATION_SCRIPT),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    implementation_payload = parse_last_json_object(
        implementation_run.stdout
    )

    implementation_contract_passed = all(
        [
            implementation_run.returncode == 0,
            implementation_payload.get("diagnosis")
            == (
                "position_player_substitution_"
                "state_contract_and_evaluator_"
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
                "forbidden_import_count"
            )
            == 0,
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
        "position_player_substitution_evaluator"
    )

    module_text = read_text(
        MODULE_PATH
    )

    tree = ast.parse(
        module_text,
        filename=str(MODULE_PATH),
    )

    imported_modules = sorted(
        {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and node.module is not None
        }
        | {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
    )

    prohibited_imports_found = sorted(
        set(imported_modules)
        & PROHIBITED_IMPORTS
    )

    production_references = (
        production_reference_rows()
    )

    pinch_hit_state = base_state(
        "pinch_hitter"
    )
    pinch_hit_result = (
        module
        .evaluate_position_player_substitution(
            pinch_hit_state
        )
    )

    pinch_run_state = base_state(
        "pinch_runner"
    )
    pinch_run_result = (
        module
        .evaluate_position_player_substitution(
            pinch_run_state
        )
    )

    defense_state = base_state(
        "defensive_replacement"
    )
    defense_result = (
        module
        .evaluate_position_player_substitution(
            defense_state
        )
    )

    injury_state = base_state(
        "injury_replacement"
    )
    injury_state[
        "injury_required"
    ] = True

    injury_result = (
        module
        .evaluate_position_player_substitution(
            injury_state
        )
    )

    used_state = base_state(
        "pinch_hitter"
    )

    for candidate in used_state[
        "candidate_players"
    ]:
        candidate[
            "already_used"
        ] = True

    used_result = (
        module
        .evaluate_position_player_substitution(
            used_state
        )
    )

    incompatible_state = base_state(
        "defensive_replacement"
    )

    incompatible_state[
        "current_player"
    ][
        "primary_position"
    ] = "C"

    for candidate in incompatible_state[
        "candidate_players"
    ]:
        candidate[
            "primary_position"
        ] = "LF"
        candidate[
            "eligible_positions"
        ] = [
            "LF"
        ]

    incompatible_result = (
        module
        .evaluate_position_player_substitution(
            incompatible_state
        )
    )

    dh_state = base_state(
        "double_switch_or_lineup_reassignment"
    )

    dh_state[
        "designated_hitter_active"
    ] = True

    dh_result = (
        module
        .evaluate_position_player_substitution(
            dh_state
        )
    )

    partial_state = base_state(
        "pinch_hitter"
    )

    partial_state[
        "candidate_players"
    ][
        0
    ][
        "evidence_complete"
    ] = False

    partial_result = (
        module
        .evaluate_position_player_substitution(
            partial_state
        )
    )

    invalid_state = base_state(
        "pinch_hitter"
    )

    invalid_state.pop(
        "batting_order"
    )

    invalid_result = (
        module
        .evaluate_position_player_substitution(
            invalid_state
        )
    )

    tie_state = base_state(
        "pinch_hitter"
    )

    tie_state[
        "candidate_players"
    ] = [
        player(
            "BENCH_Z",
            offense=0.80,
        ),
        player(
            "BENCH_A",
            offense=0.80,
        ),
    ]

    tie_result = (
        module
        .evaluate_position_player_substitution(
            tie_state
        )
    )

    cases = [
        evaluate_case(
            module,
            case_id="QF-C01",
            scenario="pinch_hitter_complete_evidence",
            state=pinch_hit_state,
            assertions=[
                (
                    "substitute_action",
                    pinch_hit_result.get(
                        "recommended_action"
                    )
                    == "substitute",
                ),
                (
                    "best_candidate_selected",
                    pinch_hit_result.get(
                        "recommended_player_id"
                    )
                    == "BENCH_A",
                ),
                (
                    "candidate_improves_score",
                    pinch_hit_result.get(
                        "candidate_score"
                    )
                    > pinch_hit_result.get(
                        "current_player_score"
                    ),
                ),
            ],
        ),
        evaluate_case(
            module,
            case_id="QF-C02",
            scenario="pinch_runner_complete_evidence",
            state=pinch_run_state,
            assertions=[
                (
                    "substitute_action",
                    pinch_run_result.get(
                        "recommended_action"
                    )
                    == "substitute",
                ),
                (
                    "best_candidate_selected",
                    pinch_run_result.get(
                        "recommended_player_id"
                    )
                    == "BENCH_A",
                ),
            ],
        ),
        evaluate_case(
            module,
            case_id="QF-C03",
            scenario="defensive_replacement_complete_evidence",
            state=defense_state,
            assertions=[
                (
                    "substitute_action",
                    defense_result.get(
                        "recommended_action"
                    )
                    == "substitute",
                ),
                (
                    "lineup_constraint_valid",
                    defense_result.get(
                        "lineup_constraint_valid"
                    )
                    is True,
                ),
            ],
        ),
        evaluate_case(
            module,
            case_id="QF-C04",
            scenario="mandatory_injury_replacement",
            state=injury_state,
            assertions=[
                (
                    "required_replacement_action",
                    injury_result.get(
                        "recommended_action"
                    )
                    == "required_replacement",
                ),
                (
                    "replacement_candidate_selected",
                    injury_result.get(
                        "recommended_player_id"
                    )
                    == "BENCH_A",
                ),
            ],
        ),
        evaluate_case(
            module,
            case_id="QF-C05",
            scenario="candidate_already_used",
            state=used_state,
            assertions=[
                (
                    "not_eligible",
                    used_result.get(
                        "substitution_eligible"
                    )
                    is False,
                ),
                (
                    "no_candidate_selected",
                    used_result.get(
                        "recommended_player_id"
                    )
                    is None,
                ),
                (
                    "retain_action",
                    used_result.get(
                        "recommended_action"
                    )
                    == "retain",
                ),
            ],
        ),
        evaluate_case(
            module,
            case_id="QF-C06",
            scenario="candidate_position_incompatible",
            state=incompatible_state,
            assertions=[
                (
                    "not_eligible",
                    incompatible_result.get(
                        "substitution_eligible"
                    )
                    is False,
                ),
                (
                    "no_candidate_selected",
                    incompatible_result.get(
                        "recommended_player_id"
                    )
                    is None,
                ),
            ],
        ),
        evaluate_case(
            module,
            case_id="QF-C07",
            scenario="designated_hitter_constraint",
            state=dh_state,
            assertions=[
                (
                    "constraint_invalid",
                    dh_result.get(
                        "lineup_constraint_valid"
                    )
                    is False,
                ),
                (
                    "retain_action",
                    dh_result.get(
                        "recommended_action"
                    )
                    == "retain",
                ),
            ],
        ),
        evaluate_case(
            module,
            case_id="QF-C08",
            scenario="partial_candidate_evidence",
            state=partial_state,
            assertions=[
                (
                    "partial_state",
                    partial_result.get(
                        "state_completeness"
                    )
                    == "partial",
                ),
                (
                    "fallback_used",
                    partial_result.get(
                        "fallback_used"
                    )
                    is True,
                ),
                (
                    "fallback_action",
                    partial_result.get(
                        "recommended_action"
                    )
                    == "fallback",
                ),
            ],
        ),
        evaluate_case(
            module,
            case_id="QF-C09",
            scenario="invalid_lineup_state",
            state=invalid_state,
            assertions=[
                (
                    "invalid_state",
                    invalid_result.get(
                        "state_completeness"
                    )
                    == "invalid",
                ),
                (
                    "fallback_used",
                    invalid_result.get(
                        "fallback_used"
                    )
                    is True,
                ),
                (
                    "fallback_action",
                    invalid_result.get(
                        "recommended_action"
                    )
                    == "fallback",
                ),
            ],
        ),
        evaluate_case(
            module,
            case_id="QF-C10",
            scenario="deterministic_tie_break",
            state=tie_state,
            assertions=[
                (
                    "lexicographic_tie_break",
                    tie_result.get(
                        "recommended_player_id"
                    )
                    == "BENCH_A",
                ),
                (
                    "substitute_action",
                    tie_result.get(
                        "recommended_action"
                    )
                    == "substitute",
                ),
            ],
        ),
    ]

    cases_passed = sum(
        1
        for case in cases
        if case[
            "passed"
        ]
    )

    exact_output_fields_valid = all(
        set(
            case[
                "evaluation"
            ]
        )
        == EXPECTED_OUTPUT_FIELDS
        for case in cases
    )

    output_validation_passed = all(
        case[
            "output_valid"
        ]
        for case in cases
    )

    deterministic_passed = all(
        case[
            "deterministic"
        ]
        for case in cases
    )

    input_immutability_passed = all(
        case[
            "input_unchanged"
        ]
        for case in cases
    )

    authority_safe = all(
        case[
            "evaluation"
        ].get(
            "behavioral_effect"
        )
        == "none"
        and case[
            "evaluation"
        ].get(
            "production_activation"
        )
        is False
        for case in cases
    )

    audit_checks = [
        {
            "check": "required_files_exist",
            "actual": required_files_exist,
            "expected": True,
            "passed": required_files_exist,
        },
        {
            "check": (
                "six_qe_implementation_contract_passed"
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
                "exact_output_fields_valid"
            ),
            "actual": (
                exact_output_fields_valid
            ),
            "expected": True,
            "passed": (
                exact_output_fields_valid
            ),
        },
        {
            "check": (
                "output_validation_passed"
            ),
            "actual": (
                output_validation_passed
            ),
            "expected": True,
            "passed": (
                output_validation_passed
            ),
        },
        {
            "check": (
                "deterministic_repeatability"
            ),
            "actual": (
                deterministic_passed
            ),
            "expected": True,
            "passed": (
                deterministic_passed
            ),
        },
        {
            "check": (
                "input_immutability"
            ),
            "actual": (
                input_immutability_passed
            ),
            "expected": True,
            "passed": (
                input_immutability_passed
            ),
        },
        {
            "check": (
                "pinch_hit_directionality"
            ),
            "actual": (
                pinch_hit_result.get(
                    "candidate_score"
                )
                > pinch_hit_result.get(
                    "current_player_score"
                )
            ),
            "expected": True,
            "passed": (
                pinch_hit_result.get(
                    "candidate_score"
                )
                > pinch_hit_result.get(
                    "current_player_score"
                )
            ),
        },
        {
            "check": (
                "pinch_run_directionality"
            ),
            "actual": (
                pinch_run_result.get(
                    "candidate_score"
                )
                > pinch_run_result.get(
                    "current_player_score"
                )
            ),
            "expected": True,
            "passed": (
                pinch_run_result.get(
                    "candidate_score"
                )
                > pinch_run_result.get(
                    "current_player_score"
                )
            ),
        },
        {
            "check": (
                "defensive_directionality"
            ),
            "actual": (
                defense_result.get(
                    "candidate_score"
                )
                > defense_result.get(
                    "current_player_score"
                )
            ),
            "expected": True,
            "passed": (
                defense_result.get(
                    "candidate_score"
                )
                > defense_result.get(
                    "current_player_score"
                )
            ),
        },
        {
            "check": (
                "partial_and_invalid_fallbacks"
            ),
            "actual": all(
                [
                    partial_result.get(
                        "fallback_used"
                    )
                    is True,
                    invalid_result.get(
                        "fallback_used"
                    )
                    is True,
                ]
            ),
            "expected": True,
            "passed": all(
                [
                    partial_result.get(
                        "fallback_used"
                    )
                    is True,
                    invalid_result.get(
                        "fallback_used"
                    )
                    is True,
                ]
            ),
        },
        {
            "check": (
                "ten_independent_cases_pass"
            ),
            "actual": cases_passed,
            "expected": 10,
            "passed": (
                cases_passed
                == 10
            ),
        },
        {
            "check": (
                "zero_prohibited_imports"
            ),
            "actual": len(
                prohibited_imports_found
            ),
            "expected": 0,
            "passed": len(
                prohibited_imports_found
            )
            == 0,
        },
        {
            "check": (
                "zero_production_references"
            ),
            "actual": len(
                production_references
            ),
            "expected": 0,
            "passed": len(
                production_references
            )
            == 0,
        },
        {
            "check": (
                "production_authority_absent"
            ),
            "actual": authority_safe,
            "expected": True,
            "passed": authority_safe,
        },
    ]

    all_checks_passed = all(
        row[
            "passed"
        ]
        for row in audit_checks
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
        audit_checks,
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

    write_csv(
        OUTPUT_DIR
        / "import_scan.csv",
        [
            "imported_module",
            "prohibited",
        ],
        [
            {
                "imported_module": module_name,
                "prohibited": (
                    module_name
                    in prohibited_imports_found
                ),
            }
            for module_name in imported_modules
        ],
    )

    write_json(
        OUTPUT_DIR
        / "audit_examples.json",
        {
            case[
                "case_id"
            ]: {
                "scenario": case[
                    "scenario"
                ],
                "evaluation": case[
                    "evaluation"
                ],
                "assertions": case[
                    "assertions"
                ],
            }
            for case in cases
        },
    )

    summary = {
        "audit_checks_required": len(
            audit_checks
        ),
        "audit_checks_passed": sum(
            1
            for row in audit_checks
            if row[
                "passed"
            ]
        ),
        "independent_cases_required": 10,
        "independent_cases_passed": cases_passed,
        "six_qe_implementation_contract_passed": (
            implementation_contract_passed
        ),
        "exact_output_fields_valid": (
            exact_output_fields_valid
        ),
        "output_validation_passed": (
            output_validation_passed
        ),
        "deterministic_repeatability": (
            deterministic_passed
        ),
        "input_immutability": (
            input_immutability_passed
        ),
        "prohibited_import_count": len(
            prohibited_imports_found
        ),
        "production_reference_count": len(
            production_references
        ),
        "production_substitutions_changed": False,
        "batting_order_changed": False,
        "defensive_alignment_changed": False,
        "base_out_state_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
    }

    write_json(
        OUTPUT_DIR
        / "audit_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "position_player_substitution_"
            "evaluator_independent_audit_passed"
            if all_checks_passed
            else
            "position_player_substitution_"
            "evaluator_independent_audit_failed"
        ),
        "all_checks_passed": (
            all_checks_passed
        ),
        "audit_checks_passed": sum(
            1
            for row in audit_checks
            if row[
                "passed"
            ]
        ),
        "audit_checks_required": len(
            audit_checks
        ),
        "independent_cases_passed": (
            cases_passed
        ),
        "independent_cases_required": 10,
        "six_qe_implementation_contract_passed": (
            implementation_contract_passed
        ),
        "exact_output_fields_valid": (
            exact_output_fields_valid
        ),
        "output_validation_passed": (
            output_validation_passed
        ),
        "deterministic_repeatability": (
            deterministic_passed
        ),
        "input_immutability": (
            input_immutability_passed
        ),
        "prohibited_import_count": len(
            prohibited_imports_found
        ),
        "production_reference_count": len(
            production_references
        ),
        "production_substitutions_changed": False,
        "batting_order_changed": False,
        "defensive_alignment_changed": False,
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
        "diagnostic_integration_planning_allowed_next": (
            all_checks_passed
        ),
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            "6QG_position_player_substitution_"
            "diagnostic_integration_plan"
            if all_checks_passed
            else
            "6QG_position_player_substitution_"
            "evaluator_remediation"
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
            str(
                OUTPUT_DIR
                / "import_scan.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "audit_examples.json"
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
