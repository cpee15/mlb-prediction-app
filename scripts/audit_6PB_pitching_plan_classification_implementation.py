#!/usr/bin/env python3
"""
Layer 6PB
Independent Pitching-Plan Classification Implementation Audit

Independently audits the 6PA pure classifier using structural checks,
production non-reachability checks, contract validation, deterministic
replay, input immutability, and adversarial behavioral cases.

This audit does not modify production behavior or probabilities.
"""

from __future__ import annotations

import ast
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


LAYER_ID = "6PB"
LAYER_NAME = (
    "pitching_plan_classification_"
    "independent_implementation_audit"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PB_pitching_plan_classification_"
    "independent_implementation_audit"
)

MODULE_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "pitching_plan_classifier.py"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "scripts/implement_6PA_"
    "pitching_plan_classification.py"
)

PLAN_PATH = (
    ROOT
    / "scripts/plan_6OZ_pitching_plan_"
    "classification_inventory_and_implementation.py"
)

PRODUCTION_SCAN_ROOTS = [
    ROOT / "mlb_app",
]

REQUIRED_OUTPUT_FIELDS = {
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


def read_text(path: Path) -> str:
    if not path.exists():
        return ""

    return path.read_text(
        encoding="utf-8",
        errors="ignore",
    )


def function_names(path: Path) -> set[str]:
    tree = ast.parse(read_text(path))

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


def assignment_names(path: Path) -> set[str]:
    tree = ast.parse(read_text(path))
    names: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)

        if isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name):
                names.add(node.target.id)

    return names


def production_import_matches() -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []

    for root in PRODUCTION_SCAN_ROOTS:
        if not root.exists():
            continue

        for path in sorted(root.rglob("*.py")):
            if path == MODULE_PATH:
                continue

            text = read_text(path)

            if (
                "pitching_plan_classifier"
                not in text
                and "classify_pitching_plan"
                not in text
            ):
                continue

            matches.append(
                {
                    "path": str(
                        path.relative_to(ROOT)
                    ),
                    "module_reference": (
                        "pitching_plan_classifier"
                        in text
                    ),
                    "function_reference": (
                        "classify_pitching_plan"
                        in text
                    ),
                }
            )

    return matches


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

    type_passed = (
        first["plan_type"]
        == expected_plan_type
    )

    extra_assertions: list[bool] = []

    assertion_name = case.get(
        "assertion_name"
    )

    if assertion_name == "opener_bulk_sequence":
        sequence = first["planned_sequence"]

        extra_assertions.append(
            len(sequence) == 2
        )

        extra_assertions.append(
            [
                row.get("role")
                for row in sequence
            ]
            == [
                "opener",
                "bulk_follower",
            ]
        )

        extra_assertions.append(
            len(
                {
                    row.get("pitcher_id")
                    for row in sequence
                }
            )
            == 2
        )

    elif assertion_name == "tandem_sequence":
        sequence = first["planned_sequence"]

        extra_assertions.append(
            len(sequence) == 2
        )

        extra_assertions.append(
            first["primary_pitcher_id"]
            == "tandem-a"
        )

        extra_assertions.append(
            first["bulk_pitcher_id"]
            == "tandem-b"
        )

    elif assertion_name == "fallback_empty_sequence":
        extra_assertions.append(
            first["planned_sequence"] == []
        )

        extra_assertions.append(
            first["fallback_used"] is True
        )

    elif assertion_name == "invalid_cap_ignored":
        extra_assertions.append(
            first["workload_cap"] is None
        )

    elif assertion_name == "unavailable_removed":
        extra_assertions.append(
            first["bulk_pitcher_id"] is None
        )

        extra_assertions.append(
            first["planned_sequence"] == []
        )

    elif assertion_name == "probability_boundary":
        extra_assertions.append(
            first["diagnostics"][
                "production_activation"
            ]
            is False
        )

        extra_assertions.append(
            first["diagnostics"][
                (
                    "canonical_probability_"
                    "authority_changed"
                )
            ]
            is False
        )

    extra_passed = all(
        extra_assertions
    ) if extra_assertions else True

    passed = all(
        [
            type_passed,
            validation["valid"],
            deterministic,
            input_unchanged,
            extra_passed,
            (
                set(first.keys())
                == REQUIRED_OUTPUT_FIELDS
            ),
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
        "payload_valid": validation["valid"],
        "deterministic": deterministic,
        "input_unchanged": input_unchanged,
        "extra_assertion": (
            assertion_name or "none"
        ),
        "extra_assertion_passed": (
            extra_passed
        ),
        "passed": passed,
        "payload": first,
        "validation": validation,
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
            IMPLEMENTATION_PATH,
            PLAN_PATH,
        ]
    )

    module_functions = function_names(
        MODULE_PATH
    )

    module_assignments = assignment_names(
        MODULE_PATH
    )

    required_functions_present = {
        "classify_pitching_plan",
        "validate_pitching_plan_payload",
    }.issubset(module_functions)

    required_constants_present = {
        "PLAN_TRADITIONAL_STARTER",
        "PLAN_OPENER_BULK",
        "PLAN_TANDEM",
        "PLAN_BULLPEN_GAME",
        "PLAN_WORKLOAD_CAPPED_STARTER",
        "PLAN_UNKNOWN_FALLBACK",
        "ALLOWED_PLAN_TYPES",
    }.issubset(module_assignments)

    production_matches = (
        production_import_matches()
    )

    production_unwired = (
        len(production_matches) == 0
    )

    cases = [
        {
            "case_id": "PB-C01",
            "scenario": "traditional_starter",
            "expected_plan_type": (
                PLAN_TRADITIONAL_STARTER
            ),
            "assertion_name": (
                "probability_boundary"
            ),
            "evidence": {
                "listed_starter_id": "starter-a",
                "source_name": "independent_audit",
            },
        },
        {
            "case_id": "PB-C02",
            "scenario": "verified_opener_bulk",
            "expected_plan_type": (
                PLAN_OPENER_BULK
            ),
            "assertion_name": (
                "opener_bulk_sequence"
            ),
            "evidence": {
                "listed_starter_id": "opener-a",
                "expected_bulk_pitcher_id": (
                    "bulk-a"
                ),
                "announced_pitching_plan": (
                    "opener_bulk"
                ),
                "source_name": "independent_audit",
            },
        },
        {
            "case_id": "PB-C03",
            "scenario": "verified_tandem",
            "expected_plan_type": PLAN_TANDEM,
            "assertion_name": "tandem_sequence",
            "evidence": {
                "listed_starter_id": "tandem-a",
                "expected_bulk_pitcher_id": (
                    "tandem-b"
                ),
                "announced_pitching_plan": (
                    "tandem"
                ),
                "source_name": "independent_audit",
            },
        },
        {
            "case_id": "PB-C04",
            "scenario": "verified_bullpen_game",
            "expected_plan_type": (
                PLAN_BULLPEN_GAME
            ),
            "assertion_name": (
                "probability_boundary"
            ),
            "evidence": {
                "announced_pitching_plan": (
                    "bullpen_game"
                ),
                "source_name": "independent_audit",
            },
        },
        {
            "case_id": "PB-C05",
            "scenario": "valid_workload_cap",
            "expected_plan_type": (
                PLAN_WORKLOAD_CAPPED_STARTER
            ),
            "assertion_name": (
                "probability_boundary"
            ),
            "evidence": {
                "listed_starter_id": "starter-b",
                "workload_cap": {
                    "type": "innings",
                    "value": 4,
                    "source": "independent_audit",
                },
                "source_name": "independent_audit",
            },
        },
        {
            "case_id": "PB-C06",
            "scenario": "missing_evidence",
            "expected_plan_type": (
                PLAN_UNKNOWN_FALLBACK
            ),
            "assertion_name": (
                "fallback_empty_sequence"
            ),
            "evidence": {
                "source_name": "independent_audit",
            },
        },
        {
            "case_id": "PB-C07",
            "scenario": "contradictory_sources",
            "expected_plan_type": (
                PLAN_UNKNOWN_FALLBACK
            ),
            "assertion_name": (
                "fallback_empty_sequence"
            ),
            "evidence": {
                "listed_starter_id": "starter-c",
                "contradictory_sources": True,
                "source_name": "independent_audit",
            },
        },
        {
            "case_id": "PB-C08",
            "scenario": "unavailable_bulk",
            "expected_plan_type": (
                PLAN_UNKNOWN_FALLBACK
            ),
            "assertion_name": (
                "unavailable_removed"
            ),
            "evidence": {
                "listed_starter_id": "opener-b",
                "expected_bulk_pitcher_id": (
                    "bulk-b"
                ),
                "announced_pitching_plan": (
                    "opener_bulk"
                ),
                (
                    "roster_and_availability_"
                    "state"
                ): {
                    "opener-b": True,
                    "bulk-b": False,
                },
                "source_name": "independent_audit",
            },
        },
        {
            "case_id": "PB-C09",
            "scenario": (
                "same_pitcher_opener_and_bulk"
            ),
            "expected_plan_type": (
                PLAN_UNKNOWN_FALLBACK
            ),
            "assertion_name": (
                "fallback_empty_sequence"
            ),
            "evidence": {
                "listed_starter_id": "pitcher-x",
                "expected_bulk_pitcher_id": (
                    "pitcher-x"
                ),
                "announced_pitching_plan": (
                    "opener_bulk"
                ),
                "source_name": "independent_audit",
            },
        },
        {
            "case_id": "PB-C10",
            "scenario": (
                "different_primary_without_"
                "explicit_plan"
            ),
            "expected_plan_type": (
                PLAN_UNKNOWN_FALLBACK
            ),
            "assertion_name": (
                "fallback_empty_sequence"
            ),
            "evidence": {
                "listed_starter_id": "listed-a",
                "expected_primary_pitcher_id": (
                    "primary-a"
                ),
                "source_name": "independent_audit",
            },
        },
        {
            "case_id": "PB-C11",
            "scenario": "invalid_workload_cap",
            "expected_plan_type": (
                PLAN_TRADITIONAL_STARTER
            ),
            "assertion_name": (
                "invalid_cap_ignored"
            ),
            "evidence": {
                "listed_starter_id": "starter-d",
                "workload_cap": {
                    "type": "pitches",
                    "value": -10,
                    "source": "independent_audit",
                },
                "source_name": "independent_audit",
            },
        },
        {
            "case_id": "PB-C12",
            "scenario": "nested_input_immutability",
            "expected_plan_type": (
                PLAN_OPENER_BULK
            ),
            "assertion_name": (
                "opener_bulk_sequence"
            ),
            "evidence": {
                "listed_starter_id": "opener-c",
                "expected_bulk_pitcher_id": (
                    "bulk-c"
                ),
                "announced_pitching_plan": (
                    "opener_bulk"
                ),
                (
                    "roster_and_availability_"
                    "state"
                ): {
                    "opener-c": True,
                    "bulk-c": True,
                },
                "workload_cap": {
                    "type": "batters",
                    "value": 6,
                    "source": "independent_audit",
                },
                "source_name": "independent_audit",
            },
        },
    ]

    results = [
        evaluate_case(case)
        for case in cases
    ]

    failed_results = [
        row
        for row in results
        if not row["passed"]
    ]

    behavioral_cases_executed = (
        len(results) == len(cases) == 12
    )

    structural_rows = [
        {
            "check": "required_files_exist",
            "actual": required_files_exist,
            "expected": True,
            "passed": required_files_exist,
        },
        {
            "check": "required_functions_present",
            "actual": required_functions_present,
            "expected": True,
            "passed": required_functions_present,
        },
        {
            "check": "required_constants_present",
            "actual": required_constants_present,
            "expected": True,
            "passed": required_constants_present,
        },
        {
            "check": "production_route_unwired",
            "actual": len(production_matches),
            "expected": 0,
            "passed": production_unwired,
        },
        {
            "check": "twelve_cases_executed",
            "actual": len(results),
            "expected": 12,
            "passed": behavioral_cases_executed,
        },
        {
            "check": "all_payloads_contract_valid",
            "actual": sum(
                1
                for row in results
                if row["payload_valid"]
            ),
            "expected": len(results),
            "passed": all(
                row["payload_valid"]
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
        {
            "check": (
                "all_probability_boundaries_preserved"
            ),
            "actual": sum(
                1
                for row in results
                if (
                    row["payload"][
                        "diagnostics"
                    ][
                        "production_activation"
                    ]
                    is False
                    and row["payload"][
                        "diagnostics"
                    ][
                        (
                            "canonical_probability_"
                            "authority_changed"
                        )
                    ]
                    is False
                )
            ),
            "expected": len(results),
            "passed": all(
                (
                    row["payload"][
                        "diagnostics"
                    ][
                        "production_activation"
                    ]
                    is False
                    and row["payload"][
                        "diagnostics"
                    ][
                        (
                            "canonical_probability_"
                            "authority_changed"
                        )
                    ]
                    is False
                )
                for row in results
            ),
        },
    ]

    audit_execution_passed = all(
        row["passed"]
        for row in structural_rows
    )

    implementation_approved = (
        audit_execution_passed
        and len(failed_results) == 0
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
                    "independent_structural_audit"
                ),
                "changed_or_executed": True,
                "passed": audit_execution_passed,
            },
            {
                "boundary": (
                    "independent_adversarial_"
                    "behavior_audit"
                ),
                "changed_or_executed": True,
                "passed": behavioral_cases_executed,
            },
        ]
    )

    if implementation_approved:
        diagnosis_value = (
            "pitching_plan_classification_"
            "independent_implementation_audit_passed"
        )
        recommended_next_layer = (
            "6PC_pitching_plan_classification_"
            "diagnostic_integration_plan"
        )
        recommended_action = (
            "Plan disabled-by-default diagnostic integration "
            "into the shared simulation route."
        )
    else:
        diagnosis_value = (
            "pitching_plan_classification_"
            "independent_implementation_audit_"
            "gaps_confirmed"
        )
        recommended_next_layer = (
            "6PC_pitching_plan_classification_"
            "gap_remediation"
        )
        recommended_action = (
            "Remediate independently detected behavioral "
            "classification gaps before any integration."
        )

    write_csv(
        OUTPUT_DIR / "structural_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        structural_rows,
    )

    write_csv(
        OUTPUT_DIR / "behavioral_case_results.csv",
        [
            "case_id",
            "scenario",
            "expected_plan_type",
            "actual_plan_type",
            "payload_valid",
            "deterministic",
            "input_unchanged",
            "extra_assertion",
            "extra_assertion_passed",
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
                    "payload_valid",
                    "deterministic",
                    "input_unchanged",
                    "extra_assertion",
                    "extra_assertion_passed",
                    "passed",
                ]
            }
            for row in results
        ],
    )

    write_csv(
        OUTPUT_DIR / "production_reference_scan.csv",
        [
            "path",
            "module_reference",
            "function_reference",
        ],
        production_matches,
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
                    recommended_action
                ),
                "entry_condition": (
                    "6PB independent audit execution completes."
                ),
                "passed": audit_execution_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "behavioral_payloads.json",
        results,
    )

    audit_summary = {
        "audit_execution_passed": (
            audit_execution_passed
        ),
        "implementation_approved": (
            implementation_approved
        ),
        "cases_executed": len(results),
        "cases_passed": sum(
            1
            for row in results
            if row["passed"]
        ),
        "cases_failed": len(failed_results),
        "failed_case_ids": [
            row["case_id"]
            for row in failed_results
        ],
        "production_reference_count": len(
            production_matches
        ),
        "production_route_wired": (
            not production_unwired
        ),
        "production_classifier_activated": False,
        "canonical_probability_authority_changed": (
            False
        ),
        "new_authority_granted": False,
    }

    write_json(
        OUTPUT_DIR / "audit_summary.json",
        audit_summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_value,
        "audit_execution_passed": (
            audit_execution_passed
        ),
        "implementation_approved": (
            implementation_approved
        ),
        "structural_checks_passed": sum(
            1
            for row in structural_rows
            if row["passed"]
        ),
        "structural_checks_required": len(
            structural_rows
        ),
        "behavioral_cases_executed": len(
            results
        ),
        "behavioral_cases_passed": sum(
            1
            for row in results
            if row["passed"]
        ),
        "behavioral_cases_failed": len(
            failed_results
        ),
        "failed_case_ids": [
            row["case_id"]
            for row in failed_results
        ],
        "production_reference_count": len(
            production_matches
        ),
        "production_route_wired": (
            not production_unwired
        ),
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
        "integration_allowed_next": (
            implementation_approved
        ),
        "gap_remediation_allowed_next": (
            audit_execution_passed
            and not implementation_approved
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
                / "behavioral_case_results.csv"
            ),
            str(
                OUTPUT_DIR
                / "production_reference_scan.csv"
            ),
            str(OUTPUT_DIR / "safety_audit.csv"),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR / "behavioral_payloads.json"
            ),
            str(OUTPUT_DIR / "audit_summary.json"),
            str(OUTPUT_DIR / "diagnosis.json"),
        ],
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(json.dumps(diagnosis, indent=2))

    return 0 if audit_execution_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
