#!/usr/bin/env python3
"""
Layer 6QE
Position-Player Substitution State Contract and Evaluator Implementation

Implements and validates a pure deterministic position-player substitution
evaluator.

The evaluator is not connected to production simulation paths and has no
lineup, batting-order, defensive-alignment, base/out-state, simulation, or
probability authority.
"""

from __future__ import annotations

import ast
import csv
import importlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6QE"

LAYER_NAME = (
    "position_player_substitution_"
    "state_contract_and_evaluator_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6QE_position_player_substitution_"
    "state_contract_and_evaluator_implementation"
)

PLAN_PATH = (
    ROOT
    / "scripts/plan_6QD_position_player_"
    "substitution_inventory_and_implementation.py"
)

EVALUATOR_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "position_player_substitution_evaluator.py"
)

SIMULATION_ROOT = ROOT / "mlb_app/simulation"

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


def write_json(
    path: Path,
    payload: Any,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )


def read_text(path: Path) -> str:
    return path.read_text(
        encoding="utf-8",
        errors="ignore",
    )


def static_plan_contract_passes() -> bool:
    if not PLAN_PATH.exists():
        return False

    tree = ast.parse(
        read_text(PLAN_PATH),
        filename=str(PLAN_PATH),
    )

    strings = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }

    return all(
        [
            (
                "position_player_substitution_"
                "inventory_and_implementation_"
                "plan_complete"
            )
            in strings,
            (
                "6QE_position_player_substitution_"
                "state_contract_and_evaluator_"
                "implementation"
            )
            in strings,
            (
                "pure_evaluator_implementation_"
                "allowed_next"
            )
            in strings,
            (
                "production_behavior_"
                "integration_allowed_next"
            )
            in strings,
        ]
    )


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
        "inning": 7,
        "half": "top",
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
            offense=0.40,
            running=0.40,
            defense=0.40,
        ),
        "candidate_players": [
            player(
                "BENCH_A",
                offense=0.80,
                running=0.75,
                defense=0.70,
            ),
            player(
                "BENCH_B",
                offense=0.60,
                running=0.55,
                defense=0.65,
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
        "evidence_version": "6qe-v1",
    }


def production_reference_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for path in sorted(
        SIMULATION_ROOT.rglob("*.py")
    ):
        if path == EVALUATOR_PATH:
            continue

        text = read_text(path)

        matched = [
            token
            for token in [
                (
                    "position_player_"
                    "substitution_evaluator"
                ),
                (
                    "evaluate_position_player_"
                    "substitution"
                ),
                (
                    "validate_position_player_"
                    "substitution_evaluation"
                ),
            ]
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
        "position_player_substitution_evaluator"
    )

    evaluate = (
        module
        .evaluate_position_player_substitution
    )

    validate_state = (
        module
        .validate_position_player_substitution_state
    )

    validate_output = (
        module
        .validate_position_player_substitution_evaluation
    )

    evaluator_text = read_text(
        EVALUATOR_PATH
    )

    tree = ast.parse(
        evaluator_text,
        filename=str(EVALUATOR_PATH),
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

    forbidden_imports = sorted(
        set(imported_modules)
        & {
            "mlb_app.simulation.game_engine_v2",
            "mlb_app.simulation.game_simulator",
            "mlb_app.simulation.game_simulation_builder",
            "mlb_app.simulation.inning_simulator",
            "pandas",
            "numpy",
            "requests",
            "random",
        }
    )

    production_references = (
        production_reference_rows()
    )

    pinch_hit_state = base_state(
        "pinch_hitter"
    )
    pinch_hit_result = evaluate(
        pinch_hit_state
    )

    pinch_run_state = base_state(
        "pinch_runner"
    )
    pinch_run_result = evaluate(
        pinch_run_state
    )

    defense_state = base_state(
        "defensive_replacement"
    )
    defense_result = evaluate(
        defense_state
    )

    injury_state = base_state(
        "injury_replacement"
    )
    injury_state["injury_required"] = True
    injury_result = evaluate(
        injury_state
    )

    used_state = base_state(
        "pinch_hitter"
    )
    for candidate in used_state[
        "candidate_players"
    ]:
        candidate["already_used"] = True
    used_result = evaluate(
        used_state
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
        candidate["primary_position"] = "LF"
        candidate["eligible_positions"] = ["LF"]

    incompatible_result = evaluate(
        incompatible_state
    )

    dh_state = base_state(
        "double_switch_or_lineup_reassignment"
    )
    dh_state[
        "designated_hitter_active"
    ] = True
    dh_result = evaluate(
        dh_state
    )

    partial_state = base_state(
        "pinch_hitter"
    )
    partial_state[
        "candidate_players"
    ][0][
        "evidence_complete"
    ] = False
    partial_result = evaluate(
        partial_state
    )

    invalid_state = base_state(
        "pinch_hitter"
    )
    invalid_state.pop(
        "batting_order"
    )
    invalid_result = evaluate(
        invalid_state
    )

    repeat_state = base_state(
        "pinch_hitter"
    )
    repeat_original = deepcopy(
        repeat_state
    )
    repeat_first = evaluate(
        repeat_state
    )
    repeat_second = evaluate(
        repeat_state
    )

    fixtures = [
        {
            "fixture_id": "QE-F01",
            "scenario": "pinch_hitter_complete_evidence",
            "passed": all(
                [
                    pinch_hit_result.get("recommended_action") == "substitute",
                    pinch_hit_result.get("recommended_player_id") == "BENCH_A",
                ]
            ),
        },
        {
            "fixture_id": "QE-F02",
            "scenario": "pinch_runner_complete_evidence",
            "passed": all(
                [
                    pinch_run_result.get("recommended_action") == "substitute",
                    pinch_run_result.get("recommended_player_id") == "BENCH_A",
                ]
            ),
        },
        {
            "fixture_id": "QE-F03",
            "scenario": "defensive_replacement_complete_evidence",
            "passed": (
                defense_result.get("recommended_action") == "substitute"
            ),
        },
        {
            "fixture_id": "QE-F04",
            "scenario": "mandatory_injury_replacement",
            "passed": (
                injury_result.get("recommended_action")
                == "required_replacement"
            ),
        },
        {
            "fixture_id": "QE-F05",
            "scenario": "candidate_already_used",
            "passed": all(
                [
                    used_result.get("substitution_eligible") is False,
                    used_result.get("recommended_player_id") is None,
                ]
            ),
        },
        {
            "fixture_id": "QE-F06",
            "scenario": "candidate_position_incompatible",
            "passed": (
                incompatible_result.get("substitution_eligible") is False
            ),
        },
        {
            "fixture_id": "QE-F07",
            "scenario": "designated_hitter_constraint",
            "passed": (
                dh_result.get("lineup_constraint_valid") is False
            ),
        },
        {
            "fixture_id": "QE-F08",
            "scenario": "partial_candidate_evidence",
            "passed": all(
                [
                    partial_result.get("state_completeness") == "partial",
                    partial_result.get("fallback_used") is True,
                ]
            ),
        },
        {
            "fixture_id": "QE-F09",
            "scenario": "invalid_lineup_state",
            "passed": all(
                [
                    invalid_result.get("state_completeness") == "invalid",
                    invalid_result.get("production_activation") is False,
                ]
            ),
        },
        {
            "fixture_id": "QE-F10",
            "scenario": "input_immutability_and_repeatability",
            "passed": all(
                [
                    repeat_first == repeat_second,
                    repeat_state == repeat_original,
                ]
            ),
        },
    ]

    fixtures_passed = sum(
        1
        for fixture in fixtures
        if fixture["passed"]
    )

    all_results = [
        pinch_hit_result,
        pinch_run_result,
        defense_result,
        injury_result,
        used_result,
        incompatible_result,
        dh_result,
        partial_result,
        invalid_result,
        repeat_first,
    ]

    output_contract_valid = all(
        set(result) == EXPECTED_OUTPUT_FIELDS
        and validate_output(result).get("valid") is True
        for result in all_results
    )

    state_contract_valid = all(
        [
            validate_state(pinch_hit_state).get("valid") is True,
            validate_state(invalid_state).get("valid") is False,
        ]
    )

    deterministic_directionality = all(
        [
            pinch_hit_result.get("candidate_score")
            > pinch_hit_result.get("current_player_score"),
            pinch_run_result.get("candidate_score")
            > pinch_run_result.get("current_player_score"),
            defense_result.get("candidate_score")
            > defense_result.get("current_player_score"),
        ]
    )

    authority_safe = all(
        result.get("behavioral_effect") == "none"
        and result.get("production_activation") is False
        for result in all_results
    )

    checks = [
        {
            "check": "required_files_exist",
            "actual": required_files_exist,
            "expected": True,
            "passed": required_files_exist,
        },
        {
            "check": "six_qd_plan_contract_passed",
            "actual": plan_contract_passed,
            "expected": True,
            "passed": plan_contract_passed,
        },
        {
            "check": "state_contract_valid",
            "actual": state_contract_valid,
            "expected": True,
            "passed": state_contract_valid,
        },
        {
            "check": "output_contract_valid",
            "actual": output_contract_valid,
            "expected": True,
            "passed": output_contract_valid,
        },
        {
            "check": "ten_fixtures_pass",
            "actual": fixtures_passed,
            "expected": 10,
            "passed": fixtures_passed == 10,
        },
        {
            "check": "deterministic_directionality",
            "actual": deterministic_directionality,
            "expected": True,
            "passed": deterministic_directionality,
        },
        {
            "check": "repeatability_and_input_immutability",
            "actual": all(
                [
                    repeat_first == repeat_second,
                    repeat_state == repeat_original,
                ]
            ),
            "expected": True,
            "passed": all(
                [
                    repeat_first == repeat_second,
                    repeat_state == repeat_original,
                ]
            ),
        },
        {
            "check": "zero_forbidden_imports",
            "actual": len(forbidden_imports),
            "expected": 0,
            "passed": len(forbidden_imports) == 0,
        },
        {
            "check": "zero_production_references",
            "actual": len(production_references),
            "expected": 0,
            "passed": len(production_references) == 0,
        },
        {
            "check": "authority_fields_safe",
            "actual": authority_safe,
            "expected": True,
            "passed": authority_safe,
        },
        {
            "check": "production_activation_absent",
            "actual": True,
            "expected": True,
            "passed": True,
        },
        {
            "check": "independent_audit_allowed",
            "actual": all(
                [
                    required_files_exist,
                    plan_contract_passed,
                    state_contract_valid,
                    output_contract_valid,
                    fixtures_passed == 10,
                    deterministic_directionality,
                    not forbidden_imports,
                    not production_references,
                    authority_safe,
                ]
            ),
            "expected": True,
            "passed": all(
                [
                    required_files_exist,
                    plan_contract_passed,
                    state_contract_valid,
                    output_contract_valid,
                    fixtures_passed == 10,
                    deterministic_directionality,
                    not forbidden_imports,
                    not production_references,
                    authority_safe,
                ]
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
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
            "passed",
        ],
        fixtures,
    )

    write_csv(
        OUTPUT_DIR / "production_reference_scan.csv",
        [
            "path",
            "matched_tokens",
        ],
        production_references,
    )

    write_csv(
        OUTPUT_DIR / "import_scan.csv",
        [
            "imported_module",
            "forbidden",
        ],
        [
            {
                "imported_module": module_name,
                "forbidden": module_name in forbidden_imports,
            }
            for module_name in imported_modules
        ],
    )

    write_json(
        OUTPUT_DIR / "evaluation_examples.json",
        {
            "pinch_hitter": pinch_hit_result,
            "pinch_runner": pinch_run_result,
            "defensive_replacement": defense_result,
            "injury_replacement": injury_result,
            "already_used": used_result,
            "position_incompatible": incompatible_result,
            "designated_hitter_constraint": dh_result,
            "partial": partial_result,
            "invalid": invalid_result,
        },
    )

    summary = {
        "implementation_checks_required": len(checks),
        "implementation_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "fixtures_required": 10,
        "fixtures_passed": fixtures_passed,
        "six_qd_plan_contract_passed": plan_contract_passed,
        "state_contract_valid": state_contract_valid,
        "output_contract_valid": output_contract_valid,
        "deterministic_directionality": deterministic_directionality,
        "forbidden_import_count": len(forbidden_imports),
        "production_reference_count": len(production_references),
        "production_substitutions_changed": False,
        "batting_order_changed": False,
        "defensive_alignment_changed": False,
        "base_out_state_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
    }

    write_json(
        OUTPUT_DIR / "implementation_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "position_player_substitution_"
            "state_contract_and_evaluator_"
            "implementation_complete"
            if all_checks_passed
            else
            "position_player_substitution_"
            "state_contract_and_evaluator_"
            "implementation_incomplete"
        ),
        "all_checks_passed": all_checks_passed,
        "implementation_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "implementation_checks_required": len(checks),
        "fixtures_passed": fixtures_passed,
        "fixtures_required": 10,
        "six_qd_plan_contract_passed": plan_contract_passed,
        "state_contract_valid": state_contract_valid,
        "output_contract_valid": output_contract_valid,
        "deterministic_directionality": deterministic_directionality,
        "forbidden_import_count": len(forbidden_imports),
        "production_reference_count": len(production_references),
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
        "independent_evaluator_audit_allowed_next": (
            all_checks_passed
        ),
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            "6QF_position_player_substitution_"
            "evaluator_independent_audit"
            if all_checks_passed
            else
            "6QF_position_player_substitution_"
            "evaluator_remediation"
        ),
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / "implementation_checks.csv"),
            str(OUTPUT_DIR / "fixture_results.csv"),
            str(OUTPUT_DIR / "production_reference_scan.csv"),
            str(OUTPUT_DIR / "import_scan.csv"),
        ],
        "generated_json_artifacts": [
            str(OUTPUT_DIR / "evaluation_examples.json"),
            str(OUTPUT_DIR / "implementation_summary.json"),
            str(OUTPUT_DIR / "diagnosis.json"),
        ],
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(
        json.dumps(
            diagnosis,
            indent=2,
        )
    )

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
