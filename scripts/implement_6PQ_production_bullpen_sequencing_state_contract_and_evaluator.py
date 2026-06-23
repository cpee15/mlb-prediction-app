#!/usr/bin/env python3
"""
Layer 6PQ
Production Bullpen Sequencing State Contract and Evaluator Implementation

Implements and validates a pure deterministic bullpen-sequence evaluator.

The evaluator is not connected to production simulation paths and has no
pitcher-selection, bullpen-transition, or probability authority.
"""

from __future__ import annotations

import ast
import csv
import importlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6PQ"

LAYER_NAME = (
    "production_bullpen_sequencing_state_"
    "contract_and_evaluator_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PQ_production_bullpen_sequencing_"
    "state_contract_and_evaluator_implementation"
)

PLAN_PATH = (
    ROOT
    / "scripts/plan_6PP_production_bullpen_"
    "sequencing_inventory_and_implementation.py"
)

EVALUATOR_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "bullpen_sequence_evaluator.py"
)

SIMULATION_ROOT = (
    ROOT
    / "mlb_app/simulation"
)

EXPECTED_OUTPUT_FIELDS = {
    "recommended_pitcher_id",
    "ranked_candidates",
    "leverage_band",
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

    text = read_text(PLAN_PATH)

    tree = ast.parse(
        text,
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
                "production_bullpen_sequencing_"
                "inventory_and_implementation_"
                "plan_complete"
            )
            in strings,
            (
                "6PQ_production_bullpen_"
                "sequencing_state_contract_and_"
                "evaluator_implementation"
            )
            in strings,
            (
                "pure_evaluator_"
                "implementation_allowed_next"
            )
            in text,
            (
                "production_behavior_"
                "integration_allowed_next"
            )
            in text,
        ]
    )


def reliever(
    pitcher_id: str,
    role: str,
    *,
    availability: str = "available",
    quality: float = 0.0,
    fatigue: float = 0.20,
    recent_usage: int = 0,
    back_to_back: bool = False,
    capacity: float = 1.0,
    evidence_complete: bool = True,
) -> dict[str, Any]:
    return {
        "pitcher_id": pitcher_id,
        "role": role,
        "throws": "R",
        "quality_score": quality,
        "availability_status": (
            availability
        ),
        "fatigue_index": fatigue,
        "recent_usage_count": (
            recent_usage
        ),
        "back_to_back_flag": (
            back_to_back
        ),
        "innings_capacity": capacity,
        "evidence_complete": (
            evidence_complete
        ),
    }


def base_state() -> dict[str, Any]:
    return {
        "team_id": "TEAM",
        "inning": 8,
        "outs": 0,
        "base_state": {
            "first": False,
            "second": False,
            "third": False,
        },
        "score_margin": 1,
        "leverage_proxy": 0.75,
        "current_pitcher_id": "CURRENT",
        "available_relievers": [
            reliever(
                "CLOSER",
                "closer",
                quality=0.40,
            ),
            reliever(
                "SETUP",
                "setup",
                quality=0.30,
            ),
            reliever(
                "MIDDLE",
                "middle_relief",
                quality=0.10,
                capacity=2.0,
            ),
            reliever(
                "LOW",
                "low_leverage",
                capacity=2.0,
            ),
        ],
        "used_pitcher_ids": [],
        "usage_log": [],
        "bullpen_depletion_index": 0.20,
        "extra_inning_flag": False,
    }


def production_reference_rows() -> list[
    dict[str, Any]
]:
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
                "bullpen_sequence_evaluator",
                "evaluate_bullpen_sequence",
                (
                    "validate_bullpen_"
                    "sequence_evaluation"
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
        "bullpen_sequence_evaluator"
    )

    evaluate = (
        module.evaluate_bullpen_sequence
    )

    validate_state = (
        module.validate_bullpen_sequence_state
    )

    validate_output = (
        module
        .validate_bullpen_sequence_evaluation
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
            "pandas",
            "numpy",
            "requests",
        }
    )

    production_references = (
        production_reference_rows()
    )

    high_state = base_state()
    high_state["leverage_proxy"] = 0.90

    high_result = evaluate(
        high_state
    )

    low_state = base_state()
    low_state.update(
        {
            "inning": 6,
            "score_margin": 5,
            "leverage_proxy": 0.10,
        }
    )

    low_result = evaluate(
        low_state
    )

    closer_unavailable_state = (
        base_state()
    )

    closer_unavailable_state[
        "available_relievers"
    ][0][
        "availability_status"
    ] = "unavailable"

    closer_unavailable_result = evaluate(
        closer_unavailable_state
    )

    depleted_state = base_state()

    for candidate in depleted_state[
        "available_relievers"
    ]:
        candidate[
            "availability_status"
        ] = "unavailable"

    depleted_result = evaluate(
        depleted_state
    )

    partial_state = base_state()

    partial_state[
        "available_relievers"
    ][1][
        "evidence_complete"
    ] = False

    partial_result = evaluate(
        partial_state
    )

    invalid_state = base_state()
    invalid_state.pop(
        "available_relievers"
    )

    invalid_result = evaluate(
        invalid_state
    )

    tie_state = base_state()

    tie_state[
        "available_relievers"
    ] = [
        reliever(
            "B",
            "middle_relief",
        ),
        reliever(
            "A",
            "middle_relief",
        ),
    ]

    tie_result = evaluate(
        tie_state
    )

    extra_state = base_state()
    extra_state.update(
        {
            "inning": 10,
            "score_margin": 0,
            "leverage_proxy": 0.70,
            "extra_inning_flag": True,
            "bullpen_depletion_index": 0.80,
        }
    )

    extra_state[
        "available_relievers"
    ] = [
        reliever(
            "LONG",
            "long_relief",
            capacity=3.0,
        ),
        reliever(
            "LIMITED_SETUP",
            "setup",
            availability="limited",
            fatigue=0.70,
            back_to_back=True,
        ),
    ]

    extra_result = evaluate(
        extra_state
    )

    immutability_state = base_state()

    immutability_original = deepcopy(
        immutability_state
    )

    immutability_result = evaluate(
        immutability_state
    )

    fixtures = [
        {
            "fixture_id": "PQ-F01",
            "scenario": (
                "complete_high_leverage_state"
            ),
            "passed": all(
                [
                    high_result[
                        "recommended_pitcher_id"
                    ]
                    == "CLOSER",
                    high_result[
                        "leverage_band"
                    ]
                    == "critical",
                    high_result[
                        "fallback_used"
                    ]
                    is False,
                ]
            ),
        },
        {
            "fixture_id": "PQ-F02",
            "scenario": (
                "complete_low_leverage_state"
            ),
            "passed": all(
                [
                    low_result[
                        "recommended_pitcher_id"
                    ]
                    == "LOW",
                    low_result[
                        "leverage_band"
                    ]
                    == "low",
                ]
            ),
        },
        {
            "fixture_id": "PQ-F03",
            "scenario": "closer_unavailable",
            "passed": all(
                [
                    closer_unavailable_result[
                        "recommended_pitcher_id"
                    ]
                    == "SETUP",
                    all(
                        row["pitcher_id"]
                        != "CLOSER"
                        for row in (
                            closer_unavailable_result[
                                "ranked_candidates"
                            ]
                        )
                    ),
                ]
            ),
        },
        {
            "fixture_id": "PQ-F04",
            "scenario": (
                "all_primary_roles_unavailable"
            ),
            "passed": all(
                [
                    depleted_result[
                        "recommended_pitcher_id"
                    ]
                    is None,
                    depleted_result[
                        "fallback_used"
                    ]
                    is True,
                    depleted_result[
                        "fallback_reason"
                    ]
                    == (
                        "bullpen_depleted_"
                        "or_unavailable"
                    ),
                ]
            ),
        },
        {
            "fixture_id": "PQ-F05",
            "scenario": (
                "partial_availability_evidence"
            ),
            "passed": (
                partial_result[
                    "state_completeness"
                ]
                == "partial"
            ),
        },
        {
            "fixture_id": "PQ-F06",
            "scenario": (
                "invalid_bullpen_state"
            ),
            "passed": all(
                [
                    invalid_result[
                        "fallback_used"
                    ]
                    is True,
                    invalid_result[
                        "state_completeness"
                    ]
                    == "invalid",
                ]
            ),
        },
        {
            "fixture_id": "PQ-F07",
            "scenario": (
                "tie_between_candidates"
            ),
            "passed": (
                tie_result[
                    "recommended_pitcher_id"
                ]
                == "A"
            ),
        },
        {
            "fixture_id": "PQ-F08",
            "scenario": (
                "extra_inning_depletion"
            ),
            "passed": all(
                [
                    extra_result[
                        "leverage_band"
                    ]
                    == "critical",
                    extra_result[
                        "recommended_pitcher_id"
                    ]
                    == "LONG",
                ]
            ),
        },
        {
            "fixture_id": "PQ-F09",
            "scenario": "input_immutability",
            "passed": all(
                [
                    immutability_state
                    == immutability_original,
                    immutability_result[
                        "recommended_pitcher_id"
                    ]
                    is not None,
                ]
            ),
        },
        {
            "fixture_id": "PQ-F10",
            "scenario": (
                "production_authority_guard"
            ),
            "passed": all(
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
                for result in [
                    high_result,
                    low_result,
                    closer_unavailable_result,
                    depleted_result,
                    partial_result,
                    invalid_result,
                    tie_result,
                    extra_result,
                ]
            ),
        },
    ]

    fixture_count = sum(
        1
        for row in fixtures
        if row["passed"]
    )

    all_payloads = [
        high_result,
        low_result,
        closer_unavailable_result,
        depleted_result,
        partial_result,
        invalid_result,
        tie_result,
        extra_result,
        immutability_result,
    ]

    output_contract_valid = all(
        set(payload)
        == EXPECTED_OUTPUT_FIELDS
        and validate_output(payload)[
            "valid"
        ]
        is True
        for payload in all_payloads
    )

    deterministic_result = (
        evaluate(high_state)
        == high_result
    )

    state_validation_valid = all(
        [
            validate_state(
                base_state()
            )["valid"]
            is True,
            validate_state(
                invalid_state
            )["valid"]
            is False,
            validate_state(
                []
            )["valid"]
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
            "check": "six_pp_plan_contract_passes",
            "actual": plan_contract_passed,
            "expected": True,
            "passed": plan_contract_passed,
        },
        {
            "check": "state_validation_contract_valid",
            "actual": state_validation_valid,
            "expected": True,
            "passed": state_validation_valid,
        },
        {
            "check": "output_contract_valid",
            "actual": output_contract_valid,
            "expected": True,
            "passed": output_contract_valid,
        },
        {
            "check": "deterministic_evaluation",
            "actual": deterministic_result,
            "expected": True,
            "passed": deterministic_result,
        },
        {
            "check": "input_immutability",
            "actual": (
                immutability_state
                == immutability_original
            ),
            "expected": True,
            "passed": (
                immutability_state
                == immutability_original
            ),
        },
        {
            "check": "forbidden_import_count",
            "actual": len(
                forbidden_imports
            ),
            "expected": 0,
            "passed": not forbidden_imports,
        },
        {
            "check": (
                "production_reference_count"
            ),
            "actual": len(
                production_references
            ),
            "expected": 0,
            "passed": not production_references,
        },
        {
            "check": "ten_fixtures_pass",
            "actual": fixture_count,
            "expected": 10,
            "passed": fixture_count == 10,
        },
        {
            "check": (
                "production_authority_absent"
            ),
            "actual": all(
                payload[
                    "production_activation"
                ]
                is False
                and payload[
                    "canonical_probability_"
                    "authority_changed"
                ]
                is False
                for payload in all_payloads
            ),
            "expected": True,
            "passed": all(
                payload[
                    "production_activation"
                ]
                is False
                and payload[
                    "canonical_probability_"
                    "authority_changed"
                ]
                is False
                for payload in all_payloads
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
        OUTPUT_DIR / "production_references.csv",
        [
            "path",
            "matched_tokens",
        ],
        production_references,
    )

    write_json(
        OUTPUT_DIR / "fixture_payloads.json",
        {
            "high_leverage": high_result,
            "low_leverage": low_result,
            "closer_unavailable": (
                closer_unavailable_result
            ),
            "depleted": depleted_result,
            "partial": partial_result,
            "invalid": invalid_result,
            "tie": tie_result,
            "extra_inning": extra_result,
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
        "fixtures_passed": fixture_count,
        "plan_contract_passed": (
            plan_contract_passed
        ),
        "state_validation_contract_valid": (
            state_validation_valid
        ),
        "output_contract_valid": (
            output_contract_valid
        ),
        "deterministic": deterministic_result,
        "input_immutability": (
            immutability_state
            == immutability_original
        ),
        "forbidden_import_count": len(
            forbidden_imports
        ),
        "production_reference_count": len(
            production_references
        ),
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
    }

    write_json(
        OUTPUT_DIR / "implementation_summary.json",
        summary,
    )

    recommended_next_layer = (
        "6PR_production_bullpen_sequencing_"
        "evaluator_independent_audit"
        if all_checks_passed
        else
        "6PR_production_bullpen_sequencing_"
        "evaluator_remediation"
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "production_bullpen_sequencing_"
            "state_contract_and_evaluator_"
            "implementation_complete"
            if all_checks_passed
            else
            "production_bullpen_sequencing_"
            "state_contract_and_evaluator_"
            "implementation_failed"
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
        "fixtures_passed": fixture_count,
        "fixtures_required": 10,
        "six_pp_plan_contract_passed": (
            plan_contract_passed
        ),
        "state_validation_contract_valid": (
            state_validation_valid
        ),
        "output_contract_valid": (
            output_contract_valid
        ),
        "deterministic": deterministic_result,
        "input_immutability": (
            immutability_state
            == immutability_original
        ),
        "forbidden_import_count": len(
            forbidden_imports
        ),
        "production_reference_count": len(
            production_references
        ),
        "production_bullpen_changed": False,
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
                / "production_references.csv"
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
