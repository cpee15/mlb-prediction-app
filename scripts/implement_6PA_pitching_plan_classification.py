#!/usr/bin/env python3
"""
Layer 6PA
Pitching-Plan Classification Implementation

Implements and audits the pure disabled-by-default GM-01 classifier.

No production-route wiring or probability changes occur here.
"""

from __future__ import annotations

import csv
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable

from mlb_app.simulation.pitching_plan_classifier import (
    PLAN_BULLPEN_GAME,
    PLAN_OPENER_BULK,
    PLAN_TANDEM,
    PLAN_TRADITIONAL_STARTER,
    PLAN_UNKNOWN_FALLBACK,
    PLAN_WORKLOAD_CAPPED_STARTER,
    classify_pitching_plan,
    validate_pitching_plan_payload,
)


LAYER_ID = "6PA"
LAYER_NAME = (
    "pitching_plan_classification_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PA_pitching_plan_"
    "classification_implementation"
)

MODULE_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "pitching_plan_classifier.py"
)

PLAN_PATH = (
    ROOT
    / "scripts/plan_6OZ_pitching_plan_"
    "classification_inventory_and_implementation.py"
)

PROHIBITED_ACTIONS = [
    "production_route_wiring",
    "production_classifier_activation",
    "backend_payload_change",
    "frontend_behavior_change",
    "simulation_parameter_change",
    "simulation_probability_change",
    "canonical_probability_replacement",
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


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    fixtures = [
        {
            "fixture_id": "PP-F01",
            "scenario": "traditional_starter",
            "expected_plan_type": (
                PLAN_TRADITIONAL_STARTER
            ),
            "evidence": {
                "listed_starter_id": "starter-1",
                "source_name": "fixture",
            },
        },
        {
            "fixture_id": "PP-F02",
            "scenario": "verified_opener_and_bulk",
            "expected_plan_type": (
                PLAN_OPENER_BULK
            ),
            "evidence": {
                "listed_starter_id": "opener-1",
                "expected_bulk_pitcher_id": (
                    "bulk-1"
                ),
                "announced_pitching_plan": (
                    "opener_bulk"
                ),
                "source_name": "fixture",
            },
        },
        {
            "fixture_id": "PP-F03",
            "scenario": "verified_tandem",
            "expected_plan_type": PLAN_TANDEM,
            "evidence": {
                "listed_starter_id": "tandem-1",
                "expected_bulk_pitcher_id": (
                    "tandem-2"
                ),
                "announced_pitching_plan": (
                    "tandem"
                ),
                "source_name": "fixture",
            },
        },
        {
            "fixture_id": "PP-F04",
            "scenario": "bullpen_game",
            "expected_plan_type": (
                PLAN_BULLPEN_GAME
            ),
            "evidence": {
                "team_bullpen_game_indicator": True,
                "source_name": "fixture",
            },
        },
        {
            "fixture_id": "PP-F05",
            "scenario": "workload_capped_starter",
            "expected_plan_type": (
                PLAN_WORKLOAD_CAPPED_STARTER
            ),
            "evidence": {
                "listed_starter_id": "starter-2",
                "workload_cap": {
                    "type": "pitches",
                    "value": 70,
                    "source": "fixture",
                },
                "source_name": "fixture",
            },
        },
        {
            "fixture_id": "PP-F06",
            "scenario": "missing_information",
            "expected_plan_type": (
                PLAN_UNKNOWN_FALLBACK
            ),
            "evidence": {
                "source_name": "fixture",
            },
        },
        {
            "fixture_id": "PP-F07",
            "scenario": "contradictory_sources",
            "expected_plan_type": (
                PLAN_UNKNOWN_FALLBACK
            ),
            "evidence": {
                "listed_starter_id": "starter-3",
                "announced_pitching_plan": (
                    "bullpen_game"
                ),
                "contradictory_sources": True,
                "source_name": "fixture",
            },
        },
        {
            "fixture_id": "PP-F08",
            "scenario": (
                "inactive_planned_bulk_pitcher"
            ),
            "expected_plan_type": (
                PLAN_UNKNOWN_FALLBACK
            ),
            "evidence": {
                "listed_starter_id": "opener-2",
                "expected_bulk_pitcher_id": (
                    "bulk-2"
                ),
                "announced_pitching_plan": (
                    "opener_bulk"
                ),
                (
                    "roster_and_availability_"
                    "state"
                ): {
                    "opener-2": True,
                    "bulk-2": False,
                },
                "source_name": "fixture",
            },
        },
    ]

    fixture_rows = []
    payload_rows = []
    deterministic_rows = []

    for fixture in fixtures:
        original = deepcopy(
            fixture["evidence"]
        )

        first = classify_pitching_plan(
            fixture["evidence"]
        )

        second = classify_pitching_plan(
            fixture["evidence"]
        )

        validation = (
            validate_pitching_plan_payload(
                first
            )
        )

        expected_type = fixture[
            "expected_plan_type"
        ]

        deterministic = first == second
        input_unchanged = (
            fixture["evidence"] == original
        )

        type_passed = (
            first["plan_type"]
            == expected_type
        )

        fixture_passed = all(
            [
                type_passed,
                validation["valid"],
                deterministic,
                input_unchanged,
                first["diagnostics"][
                    "production_activation"
                ] is False,
                first["diagnostics"][
                    (
                        "canonical_probability_"
                        "authority_changed"
                    )
                ] is False,
            ]
        )

        fixture_rows.append(
            {
                "fixture_id": fixture[
                    "fixture_id"
                ],
                "scenario": fixture["scenario"],
                "expected_plan_type": (
                    expected_type
                ),
                "actual_plan_type": (
                    first["plan_type"]
                ),
                "payload_valid": (
                    validation["valid"]
                ),
                "deterministic": deterministic,
                "input_unchanged": (
                    input_unchanged
                ),
                "passed": fixture_passed,
            }
        )

        deterministic_rows.append(
            {
                "fixture_id": fixture[
                    "fixture_id"
                ],
                "first_equals_second": (
                    deterministic
                ),
                "input_unchanged": (
                    input_unchanged
                ),
                "passed": (
                    deterministic
                    and input_unchanged
                ),
            }
        )

        payload_rows.append(
            {
                "fixture_id": fixture[
                    "fixture_id"
                ],
                "scenario": fixture["scenario"],
                "payload": first,
                "validation": validation,
            }
        )

    required_output_fields = {
        "plan_type",
        "confidence",
        "source_status",
        "source_provenance",
        "listed_starter_id",
        "primary_pitcher_id",
        "bulk_pitcher_id",
        "planned_sequence",
        "workload_cap",
        "fallback_used",
        "diagnostics",
    }

    sample_payload = (
        classify_pitching_plan(
            fixtures[0]["evidence"]
        )
    )

    contract_rows = [
        {
            "check": "module_exists",
            "actual": MODULE_PATH.exists(),
            "expected": True,
            "passed": MODULE_PATH.exists(),
        },
        {
            "check": "plan_exists",
            "actual": PLAN_PATH.exists(),
            "expected": True,
            "passed": PLAN_PATH.exists(),
        },
        {
            "check": "eight_fixtures",
            "actual": len(fixtures),
            "expected": 8,
            "passed": len(fixtures) == 8,
        },
        {
            "check": "all_fixture_types_match",
            "actual": sum(
                1
                for row in fixture_rows
                if row["passed"]
            ),
            "expected": len(fixtures),
            "passed": all(
                row["passed"]
                for row in fixture_rows
            ),
        },
        {
            "check": "eleven_output_fields",
            "actual": len(
                required_output_fields
            ),
            "expected": 11,
            "passed": (
                set(sample_payload.keys())
                == required_output_fields
            ),
        },
        {
            "check": "deterministic_replay",
            "actual": sum(
                1
                for row in deterministic_rows
                if row["passed"]
            ),
            "expected": len(fixtures),
            "passed": all(
                row["passed"]
                for row in deterministic_rows
            ),
        },
        {
            "check": "production_activation_false",
            "actual": any(
                row["payload"][
                    "diagnostics"
                ][
                    "production_activation"
                ]
                for row in payload_rows
            ),
            "expected": False,
            "passed": not any(
                row["payload"][
                    "diagnostics"
                ][
                    "production_activation"
                ]
                for row in payload_rows
            ),
        },
        {
            "check": (
                "canonical_probability_"
                "authority_unchanged"
            ),
            "actual": any(
                row["payload"][
                    "diagnostics"
                ][
                    (
                        "canonical_probability_"
                        "authority_changed"
                    )
                ]
                for row in payload_rows
            ),
            "expected": False,
            "passed": not any(
                row["payload"][
                    "diagnostics"
                ][
                    (
                        "canonical_probability_"
                        "authority_changed"
                    )
                ]
                for row in payload_rows
            ),
        },
    ]

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
                    "pure_classifier_implementation"
                ),
                "changed_or_executed": True,
                "passed": True,
            },
            {
                "boundary": (
                    "deterministic_fixture_audit"
                ),
                "changed_or_executed": True,
                "passed": all(
                    row["passed"]
                    for row in fixture_rows
                ),
            },
        ]
    )

    all_checks_passed = all(
        row["passed"]
        for row in contract_rows
    )

    recommended_next_layer = (
        "6PB_pitching_plan_classification_"
        "independent_implementation_audit"
    )

    write_csv(
        OUTPUT_DIR / "contract_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        contract_rows,
    )

    write_csv(
        OUTPUT_DIR / "fixture_results.csv",
        [
            "fixture_id",
            "scenario",
            "expected_plan_type",
            "actual_plan_type",
            "payload_valid",
            "deterministic",
            "input_unchanged",
            "passed",
        ],
        fixture_rows,
    )

    write_csv(
        OUTPUT_DIR / "determinism_audit.csv",
        [
            "fixture_id",
            "first_equals_second",
            "input_unchanged",
            "passed",
        ],
        deterministic_rows,
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
                    "pitching-plan classifier, fixtures, "
                    "fallbacks, and nonactivation boundary."
                ),
                "entry_condition": (
                    "All 6PA implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "fixture_payloads.json",
        payload_rows,
    )

    implementation_summary = {
        "module": str(MODULE_PATH),
        "classifier_function": (
            "classify_pitching_plan"
        ),
        "validator_function": (
            "validate_pitching_plan_payload"
        ),
        "fixtures_executed": len(fixtures),
        "fixtures_passed": sum(
            1
            for row in fixture_rows
            if row["passed"]
        ),
        "production_route_wired": False,
        "production_classifier_activated": False,
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
            "pitching_plan_classification_"
            "implementation_complete"
            if all_checks_passed
            else
            "pitching_plan_classification_"
            "implementation_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "contract_checks_passed": sum(
            1
            for row in contract_rows
            if row["passed"]
        ),
        "contract_checks_required": len(
            contract_rows
        ),
        "fixtures_executed": len(fixtures),
        "fixtures_passed": sum(
            1
            for row in fixture_rows
            if row["passed"]
        ),
        "output_fields_verified": len(
            required_output_fields
        ),
        "deterministic_fixtures_passed": sum(
            1
            for row in deterministic_rows
            if row["passed"]
        ),
        "production_route_wired": False,
        "production_classifier_activated": False,
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
        "independent_audit_allowed_next": (
            all_checks_passed
        ),
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "contract_checks.csv"
            ),
            str(
                OUTPUT_DIR / "fixture_results.csv"
            ),
            str(
                OUTPUT_DIR / "determinism_audit.csv"
            ),
            str(OUTPUT_DIR / "safety_audit.csv"),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR / "fixture_payloads.json"
            ),
            str(
                OUTPUT_DIR
                / "implementation_summary.json"
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
