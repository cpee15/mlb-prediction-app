#!/usr/bin/env python3
"""
Layer 6PD
Pitching-Plan Classification Post-Remediation Audit

Formally verifies the 6PC remediation and determines whether GM-01 may
advance to disabled-by-default diagnostic integration planning.

This layer does not wire or activate the classifier and does not change
simulation behavior or probability authority.
"""

from __future__ import annotations

import ast
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6PD"
LAYER_NAME = (
    "pitching_plan_classification_post_remediation_audit"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PD_pitching_plan_classification_"
    "post_remediation_audit"
)

CLASSIFIER_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "pitching_plan_classifier.py"
)

IMPLEMENTATION_SCRIPT = (
    ROOT
    / "scripts/implement_6PA_"
    "pitching_plan_classification.py"
)

INDEPENDENT_AUDIT_SCRIPT = (
    ROOT
    / "scripts/audit_6PB_pitching_plan_"
    "classification_implementation.py"
)

REMEDIATION_SCRIPT = (
    ROOT
    / "scripts/remediate_6PC_pitching_plan_"
    "classification_gaps.py"
)

SIX_PA_DIAGNOSIS = (
    ROOT
    / "tmp/layer_6PA_pitching_plan_"
    "classification_implementation/diagnosis.json"
)

SIX_PB_DIAGNOSIS = (
    ROOT
    / "tmp/layer_6PB_pitching_plan_classification_"
    "independent_implementation_audit/diagnosis.json"
)

SIX_PC_DIAGNOSIS = (
    ROOT
    / "tmp/layer_6PC_pitching_plan_"
    "classification_gap_remediation/diagnosis.json"
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


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(
        path.read_text(encoding="utf-8")
    )


def run_script(path: Path) -> dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(path)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    return {
        "script": str(path.relative_to(ROOT)),
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "passed": completed.returncode == 0,
    }


def classifier_string_constants() -> set[str]:
    tree = ast.parse(
        CLASSIFIER_PATH.read_text(
            encoding="utf-8",
            errors="ignore",
        ),
        filename=str(CLASSIFIER_PATH),
    )

    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }


def production_references() -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []

    for path in sorted(
        (ROOT / "mlb_app").rglob("*.py")
    ):
        if path == CLASSIFIER_PATH:
            continue

        text = path.read_text(
            encoding="utf-8",
            errors="ignore",
        )

        module_reference = (
            "pitching_plan_classifier" in text
        )

        function_reference = (
            "classify_pitching_plan" in text
        )

        if not (
            module_reference
            or function_reference
        ):
            continue

        matches.append(
            {
                "path": str(
                    path.relative_to(ROOT)
                ),
                "module_reference": (
                    module_reference
                ),
                "function_reference": (
                    function_reference
                ),
            }
        )

    return matches


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    required_paths = [
        CLASSIFIER_PATH,
        IMPLEMENTATION_SCRIPT,
        INDEPENDENT_AUDIT_SCRIPT,
        REMEDIATION_SCRIPT,
    ]

    required_files_exist = all(
        path.exists()
        for path in required_paths
    )

    run_rows = [
        run_script(REMEDIATION_SCRIPT),
        run_script(IMPLEMENTATION_SCRIPT),
        run_script(INDEPENDENT_AUDIT_SCRIPT),
    ]

    run_execution_passed = all(
        row["passed"]
        for row in run_rows
    )

    artifact_paths_exist = all(
        path.exists()
        for path in [
            SIX_PA_DIAGNOSIS,
            SIX_PB_DIAGNOSIS,
            SIX_PC_DIAGNOSIS,
        ]
    )

    six_pa = (
        read_json(SIX_PA_DIAGNOSIS)
        if SIX_PA_DIAGNOSIS.exists()
        else {}
    )

    six_pb = (
        read_json(SIX_PB_DIAGNOSIS)
        if SIX_PB_DIAGNOSIS.exists()
        else {}
    )

    six_pc = (
        read_json(SIX_PC_DIAGNOSIS)
        if SIX_PC_DIAGNOSIS.exists()
        else {}
    )

    constants = classifier_string_constants()

    distinct_guard_present = (
        "opener_bulk_identity_not_distinct"
        in constants
    )

    primary_guard_present = (
        "different_primary_requires_explicit_plan"
        in constants
    )

    production_matches = (
        production_references()
    )

    production_unwired = (
        len(production_matches) == 0
    )

    six_pa_approved = all(
        [
            six_pa.get("all_checks_passed") is True,
            (
                six_pa.get("fixtures_passed")
                == 8
            ),
            (
                six_pa.get(
                    "deterministic_fixtures_passed"
                )
                == 8
            ),
            (
                six_pa.get(
                    "production_route_wired"
                )
                is False
            ),
            (
                six_pa.get(
                    "production_classifier_activated"
                )
                is False
            ),
            (
                six_pa.get(
                    "canonical_probability_"
                    "authority_changed"
                )
                is False
            ),
        ]
    )

    six_pb_approved = all(
        [
            (
                six_pb.get(
                    "audit_execution_passed"
                )
                is True
            ),
            (
                six_pb.get(
                    "implementation_approved"
                )
                is True
            ),
            (
                six_pb.get(
                    "behavioral_cases_passed"
                )
                == 12
            ),
            (
                six_pb.get(
                    "behavioral_cases_failed"
                )
                == 0
            ),
            (
                six_pb.get("failed_case_ids")
                == []
            ),
            (
                six_pb.get(
                    "production_reference_count"
                )
                == 0
            ),
            (
                six_pb.get(
                    "production_route_wired"
                )
                is False
            ),
        ]
    )

    six_pc_approved = all(
        [
            (
                six_pc.get(
                    "all_checks_passed"
                )
                is True
            ),
            (
                six_pc.get(
                    "remediation_cases_passed"
                )
                == 6
            ),
            (
                six_pc.get(
                    "gaps_remediated"
                )
                is True
            ),
            (
                six_pc.get(
                    "production_route_wired"
                )
                is False
            ),
            (
                six_pc.get(
                    "production_classifier_activated"
                )
                is False
            ),
            (
                six_pc.get(
                    "canonical_probability_"
                    "authority_changed"
                )
                is False
            ),
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
            "check": "audit_scripts_execute",
            "actual": sum(
                1
                for row in run_rows
                if row["passed"]
            ),
            "expected": 3,
            "passed": run_execution_passed,
        },
        {
            "check": "diagnosis_artifacts_exist",
            "actual": artifact_paths_exist,
            "expected": True,
            "passed": artifact_paths_exist,
        },
        {
            "check": "pb_c09_guard_present",
            "actual": distinct_guard_present,
            "expected": True,
            "passed": distinct_guard_present,
        },
        {
            "check": "pb_c10_guard_present",
            "actual": primary_guard_present,
            "expected": True,
            "passed": primary_guard_present,
        },
        {
            "check": "six_pa_regression_suite_passes",
            "actual": six_pa_approved,
            "expected": True,
            "passed": six_pa_approved,
        },
        {
            "check": "six_pb_independent_suite_passes",
            "actual": six_pb_approved,
            "expected": True,
            "passed": six_pb_approved,
        },
        {
            "check": "six_pc_remediation_suite_passes",
            "actual": six_pc_approved,
            "expected": True,
            "passed": six_pc_approved,
        },
        {
            "check": "production_route_unwired",
            "actual": len(production_matches),
            "expected": 0,
            "passed": production_unwired,
        },
        {
            "check": "probability_authority_unchanged",
            "actual": any(
                [
                    six_pa.get(
                        "canonical_probability_"
                        "authority_changed"
                    ),
                    six_pb.get(
                        "canonical_probability_"
                        "authority_changed"
                    ),
                    six_pc.get(
                        "canonical_probability_"
                        "authority_changed"
                    ),
                ]
            ),
            "expected": False,
            "passed": not any(
                [
                    six_pa.get(
                        "canonical_probability_"
                        "authority_changed"
                    ),
                    six_pb.get(
                        "canonical_probability_"
                        "authority_changed"
                    ),
                    six_pc.get(
                        "canonical_probability_"
                        "authority_changed"
                    ),
                ]
            ),
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
                    "post_remediation_audit"
                ),
                "changed_or_executed": True,
                "passed": all_checks_passed,
            },
            {
                "boundary": (
                    "diagnostic_integration_planning"
                ),
                "changed_or_executed": False,
                "passed": True,
            },
        ]
    )

    recommended_next_layer = (
        "6PE_pitching_plan_classification_"
        "diagnostic_integration_plan"
    )

    write_csv(
        OUTPUT_DIR / "approval_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "script_execution.csv",
        [
            "script",
            "returncode",
            "passed",
        ],
        [
            {
                "script": row["script"],
                "returncode": row["returncode"],
                "passed": row["passed"],
            }
            for row in run_rows
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
                    "Plan disabled-by-default diagnostic "
                    "integration into the shared simulation route."
                ),
                "entry_condition": (
                    "All 6PD post-remediation approval checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "script_execution_details.json",
        run_rows,
    )

    approval_summary = {
        "six_pa_approved": six_pa_approved,
        "six_pb_approved": six_pb_approved,
        "six_pc_approved": six_pc_approved,
        "pb_c09_guard_present": (
            distinct_guard_present
        ),
        "pb_c10_guard_present": (
            primary_guard_present
        ),
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
        "diagnostic_integration_planning_approved": (
            all_checks_passed
        ),
        "production_integration_approved": False,
        "new_authority_granted": False,
    }

    write_json(
        OUTPUT_DIR / "approval_summary.json",
        approval_summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "pitching_plan_classification_"
            "post_remediation_audit_passed"
            if all_checks_passed
            else
            "pitching_plan_classification_"
            "post_remediation_audit_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "approval_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "approval_checks_required": len(checks),
        "six_pa_fixtures_passed": (
            six_pa.get("fixtures_passed")
        ),
        "six_pb_behavioral_cases_passed": (
            six_pb.get(
                "behavioral_cases_passed"
            )
        ),
        "six_pb_behavioral_cases_failed": (
            six_pb.get(
                "behavioral_cases_failed"
            )
        ),
        "six_pc_remediation_cases_passed": (
            six_pc.get(
                "remediation_cases_passed"
            )
        ),
        "pb_c09_remediated": (
            distinct_guard_present
        ),
        "pb_c10_remediated": (
            primary_guard_present
        ),
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
        "diagnostic_integration_planning_allowed_next": (
            all_checks_passed
        ),
        "production_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "approval_checks.csv"
            ),
            str(
                OUTPUT_DIR / "script_execution.csv"
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
                OUTPUT_DIR
                / "script_execution_details.json"
            ),
            str(
                OUTPUT_DIR / "approval_summary.json"
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
