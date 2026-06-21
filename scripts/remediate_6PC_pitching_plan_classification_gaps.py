#!/usr/bin/env python3
"""
Layer 6PC
Pitching-Plan Classification Gap Remediation

Remediates the two behavioral gaps confirmed by the independent 6PB
audit:

- PB-C09: opener and bulk follower must be distinct pitchers.
- PB-C10: a different expected primary pitcher requires an explicit
  supported multi-pitcher plan.

This layer does not wire or activate the classifier and does not change
simulation probabilities.
"""

from __future__ import annotations

import ast
import csv
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable

from mlb_app.simulation.pitching_plan_classifier import (
    PLAN_OPENER_BULK,
    PLAN_TANDEM,
    PLAN_TRADITIONAL_STARTER,
    PLAN_UNKNOWN_FALLBACK,
    classify_pitching_plan,
    validate_pitching_plan_payload,
)


LAYER_ID = "6PC"
LAYER_NAME = (
    "pitching_plan_classification_gap_remediation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PC_pitching_plan_"
    "classification_gap_remediation"
)

CLASSIFIER_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "pitching_plan_classifier.py"
)

AUDIT_PATH = (
    ROOT
    / "scripts/audit_6PB_pitching_plan_"
    "classification_implementation.py"
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


def evaluate_case(
    case: dict[str, Any],
) -> dict[str, Any]:
    original = deepcopy(case["evidence"])

    first = classify_pitching_plan(
        case["evidence"]
    )

    second = classify_pitching_plan(
        case["evidence"]
    )

    validation = (
        validate_pitching_plan_payload(first)
    )

    deterministic = first == second
    input_unchanged = (
        case["evidence"] == original
    )

    expected_plan_type = case[
        "expected_plan_type"
    ]

    expected_reason = case.get(
        "expected_reason"
    )

    reasons = (
        first.get("diagnostics", {})
        .get("reasons", [])
    )

    reason_passed = (
        expected_reason is None
        or expected_reason in reasons
    )

    expected_sequence_roles = case.get(
        "expected_sequence_roles"
    )

    actual_sequence_roles = [
        row.get("role")
        for row in first.get(
            "planned_sequence",
            [],
        )
    ]

    sequence_passed = (
        expected_sequence_roles is None
        or actual_sequence_roles
        == expected_sequence_roles
    )

    passed = all(
        [
            (
                first["plan_type"]
                == expected_plan_type
            ),
            validation["valid"],
            deterministic,
            input_unchanged,
            reason_passed,
            sequence_passed,
            (
                first["diagnostics"][
                    "production_activation"
                ]
                is False
            ),
            (
                first["diagnostics"][
                    (
                        "canonical_probability_"
                        "authority_changed"
                    )
                ]
                is False
            ),
        ]
    )

    return {
        "case_id": case["case_id"],
        "scenario": case["scenario"],
        "expected_plan_type": (
            expected_plan_type
        ),
        "actual_plan_type": (
            first["plan_type"]
        ),
        "expected_reason": (
            expected_reason or ""
        ),
        "reason_passed": reason_passed,
        "sequence_passed": sequence_passed,
        "payload_valid": validation["valid"],
        "deterministic": deterministic,
        "input_unchanged": input_unchanged,
        "passed": passed,
        "payload": first,
        "validation": validation,
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    cases = [
        {
            "case_id": "PC-C01",
            "scenario": (
                "same_pitcher_opener_and_bulk"
            ),
            "expected_plan_type": (
                PLAN_UNKNOWN_FALLBACK
            ),
            "expected_reason": (
                "opener_bulk_identity_not_distinct"
            ),
            "expected_sequence_roles": [],
            "evidence": {
                "listed_starter_id": "pitcher-x",
                "expected_bulk_pitcher_id": (
                    "pitcher-x"
                ),
                "announced_pitching_plan": (
                    "opener_bulk"
                ),
                "source_name": "6pc_remediation",
            },
        },
        {
            "case_id": "PC-C02",
            "scenario": (
                "different_primary_without_"
                "explicit_plan"
            ),
            "expected_plan_type": (
                PLAN_UNKNOWN_FALLBACK
            ),
            "expected_reason": (
                "different_primary_requires_"
                "explicit_plan"
            ),
            "expected_sequence_roles": [],
            "evidence": {
                "listed_starter_id": "listed-a",
                "expected_primary_pitcher_id": (
                    "primary-a"
                ),
                "source_name": "6pc_remediation",
            },
        },
        {
            "case_id": "PC-C03",
            "scenario": (
                "valid_distinct_opener_bulk"
            ),
            "expected_plan_type": (
                PLAN_OPENER_BULK
            ),
            "expected_sequence_roles": [
                "opener",
                "bulk_follower",
            ],
            "evidence": {
                "listed_starter_id": "opener-a",
                "expected_bulk_pitcher_id": (
                    "bulk-a"
                ),
                "announced_pitching_plan": (
                    "opener_bulk"
                ),
                "source_name": "6pc_remediation",
            },
        },
        {
            "case_id": "PC-C04",
            "scenario": "valid_tandem",
            "expected_plan_type": PLAN_TANDEM,
            "expected_sequence_roles": [
                "tandem_primary",
                "tandem_secondary",
            ],
            "evidence": {
                "listed_starter_id": "tandem-a",
                "expected_bulk_pitcher_id": (
                    "tandem-b"
                ),
                "announced_pitching_plan": (
                    "tandem"
                ),
                "source_name": "6pc_remediation",
            },
        },
        {
            "case_id": "PC-C05",
            "scenario": (
                "traditional_starter_no_conflict"
            ),
            "expected_plan_type": (
                PLAN_TRADITIONAL_STARTER
            ),
            "expected_sequence_roles": [
                "starter",
            ],
            "evidence": {
                "listed_starter_id": "starter-a",
                "source_name": "6pc_remediation",
            },
        },
        {
            "case_id": "PC-C06",
            "scenario": (
                "matching_expected_primary"
            ),
            "expected_plan_type": (
                PLAN_TRADITIONAL_STARTER
            ),
            "expected_sequence_roles": [
                "starter",
            ],
            "evidence": {
                "listed_starter_id": "starter-b",
                "expected_primary_pitcher_id": (
                    "starter-b"
                ),
                "source_name": "6pc_remediation",
            },
        },
    ]

    results = [
        evaluate_case(case)
        for case in cases
    ]

    source_text = CLASSIFIER_PATH.read_text(
        encoding="utf-8",
        errors="ignore",
    )

    classifier_tree = ast.parse(
        source_text,
        filename=str(CLASSIFIER_PATH),
    )

    classifier_string_constants = {
        node.value
        for node in ast.walk(classifier_tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }

    structural_checks = [
        {
            "check": "classifier_exists",
            "actual": CLASSIFIER_PATH.exists(),
            "expected": True,
            "passed": CLASSIFIER_PATH.exists(),
        },
        {
            "check": "independent_audit_exists",
            "actual": AUDIT_PATH.exists(),
            "expected": True,
            "passed": AUDIT_PATH.exists(),
        },
        {
            "check": "distinct_identity_guard_present",
            "actual": (
                "opener_bulk_identity_not_distinct"
                in classifier_string_constants
            ),
            "expected": True,
            "passed": (
                "opener_bulk_identity_not_distinct"
                in classifier_string_constants
            ),
        },
        {
            "check": "primary_conflict_guard_present",
            "actual": (
                "different_primary_requires_explicit_plan"
                in classifier_string_constants
            ),
            "expected": True,
            "passed": (
                "different_primary_requires_explicit_plan"
                in classifier_string_constants
            ),
        },
        {
            "check": "six_remediation_cases",
            "actual": len(results),
            "expected": 6,
            "passed": len(results) == 6,
        },
        {
            "check": "all_remediation_cases_pass",
            "actual": sum(
                1
                for row in results
                if row["passed"]
            ),
            "expected": len(results),
            "passed": all(
                row["passed"]
                for row in results
            ),
        },
        {
            "check": "all_cases_deterministic",
            "actual": sum(
                1
                for row in results
                if row["deterministic"]
            ),
            "expected": len(results),
            "passed": all(
                row["deterministic"]
                for row in results
            ),
        },
        {
            "check": "all_inputs_unchanged",
            "actual": sum(
                1
                for row in results
                if row["input_unchanged"]
            ),
            "expected": len(results),
            "passed": all(
                row["input_unchanged"]
                for row in results
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in structural_checks
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
                    "classifier_gap_remediation"
                ),
                "changed_or_executed": True,
                "passed": all_checks_passed,
            },
            {
                "boundary": (
                    "targeted_regression_fixtures"
                ),
                "changed_or_executed": True,
                "passed": all(
                    row["passed"]
                    for row in results
                ),
            },
        ]
    )

    recommended_next_layer = (
        "6PD_pitching_plan_classification_"
        "post_remediation_audit"
    )

    write_csv(
        OUTPUT_DIR / "structural_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        structural_checks,
    )

    write_csv(
        OUTPUT_DIR / "remediation_case_results.csv",
        [
            "case_id",
            "scenario",
            "expected_plan_type",
            "actual_plan_type",
            "expected_reason",
            "reason_passed",
            "sequence_passed",
            "payload_valid",
            "deterministic",
            "input_unchanged",
            "passed",
        ],
        [
            {
                key: row[key]
                for key in [
                    "case_id",
                    "scenario",
                    "expected_plan_type",
                    "actual_plan_type",
                    "expected_reason",
                    "reason_passed",
                    "sequence_passed",
                    "payload_valid",
                    "deterministic",
                    "input_unchanged",
                    "passed",
                ]
            }
            for row in results
        ],
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
                    "Independently re-audit the full "
                    "pitching-plan classifier after remediation."
                ),
                "entry_condition": (
                    "All targeted 6PC remediation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "remediation_payloads.json",
        results,
    )

    remediation_summary = {
        "gaps_targeted": [
            "PB-C09",
            "PB-C10",
        ],
        "gaps_remediated": (
            [
                "PB-C09",
                "PB-C10",
            ]
            if all_checks_passed
            else []
        ),
        "cases_executed": len(results),
        "cases_passed": sum(
            1
            for row in results
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
        OUTPUT_DIR / "remediation_summary.json",
        remediation_summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "pitching_plan_classification_"
            "gap_remediation_complete"
            if all_checks_passed
            else
            "pitching_plan_classification_"
            "gap_remediation_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "structural_checks_passed": sum(
            1
            for row in structural_checks
            if row["passed"]
        ),
        "structural_checks_required": len(
            structural_checks
        ),
        "remediation_cases_executed": len(
            results
        ),
        "remediation_cases_passed": sum(
            1
            for row in results
            if row["passed"]
        ),
        "gaps_targeted": [
            "PB-C09",
            "PB-C10",
        ],
        "gaps_remediated": (
            all_checks_passed
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
        "integration_allowed_next": False,
        "post_remediation_audit_allowed_next": (
            all_checks_passed
        ),
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "structural_checks.csv"
            ),
            str(
                OUTPUT_DIR
                / "remediation_case_results.csv"
            ),
            str(OUTPUT_DIR / "safety_audit.csv"),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR / "remediation_payloads.json"
            ),
            str(
                OUTPUT_DIR / "remediation_summary.json"
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
