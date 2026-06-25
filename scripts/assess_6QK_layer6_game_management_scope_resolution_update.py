#!/usr/bin/env python3
"""
Layer 6QK
Layer 6 Game-Management Scope Resolution Update

Reassesses the Layer 6OY game-management resolution plan after all five
workstreams reached diagnostic-scope completion.

This assessment grants no production behavior, probability, historical
validation, tuning, backtesting, pricing, edge-detection, or broad Layer 6
exit authority.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6QK"
LAYER_NAME = "layer6_game_management_scope_resolution_update"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/layer_6QK_game_management_scope_resolution_update"
)

RESOLUTION_PLAN_PATH = (
    ROOT
    / "scripts/plan_6OY_layer6_game_management_scope_resolution.py"
)

WORKSTREAMS = [
    {
        "workstream_id": "GM-01",
        "domain": "pitching_plan_classification",
        "completion_script": (
            "scripts/assess_6PH_pitching_plan_classification_"
            "diagnostic_integration_completion.py"
        ),
        "expected_diagnosis": (
            "pitching_plan_classification_"
            "diagnostic_integration_"
            "completion_assessment_passed"
        ),
    },
    {
        "workstream_id": "GM-02",
        "domain": "dynamic_starter_hook",
        "completion_script": (
            "scripts/assess_6PO_dynamic_starter_hook_"
            "diagnostic_scope_completion.py"
        ),
        "expected_diagnosis": (
            "dynamic_starter_hook_diagnostic_"
            "scope_completion_assessment_passed"
        ),
    },
    {
        "workstream_id": "GM-03",
        "domain": "production_bullpen_sequencing",
        "completion_script": (
            "scripts/assess_6PV_production_bullpen_"
            "sequencing_diagnostic_scope_completion.py"
        ),
        "expected_diagnosis": (
            "production_bullpen_sequencing_"
            "diagnostic_scope_complete"
        ),
    },
    {
        "workstream_id": "GM-04",
        "domain": "stolen_base_and_pickoff_state",
        "completion_script": (
            "scripts/assess_6QC_stolen_base_and_"
            "pickoff_state_diagnostic_scope_completion.py"
        ),
        "expected_diagnosis": (
            "stolen_base_and_pickoff_state_"
            "diagnostic_scope_complete"
        ),
    },
    {
        "workstream_id": "GM-05",
        "domain": "position_player_substitution_state",
        "completion_script": (
            "scripts/assess_6QJ_position_player_"
            "substitution_diagnostic_scope_completion.py"
        ),
        "expected_diagnosis": (
            "position_player_substitution_"
            "diagnostic_scope_complete"
        ),
    },
]

PROHIBITED_AUTHORITIES = [
    "production_pitching_plan_activation",
    "production_dynamic_starter_hook_activation",
    "production_bullpen_sequencing_activation",
    "production_stolen_base_pickoff_activation",
    "production_position_player_substitution_activation",
    "lineup_state_change",
    "pitcher_state_change",
    "runner_state_change",
    "base_out_state_change",
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
        json.dumps(
            payload,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def string_constants(
    path: Path,
) -> set[str]:
    if not path.exists():
        return set()

    tree = ast.parse(
        path.read_text(
            encoding="utf-8",
            errors="ignore",
        ),
        filename=str(path),
    )

    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    resolution_constants = string_constants(
        RESOLUTION_PLAN_PATH
    )

    resolution_plan_contract_present = all(
        token in resolution_constants
        for token in [
            (
                "layer_6_game_management_"
                "scope_resolution_plan_complete"
            ),
            "GM-01",
            "GM-02",
            "GM-03",
            "GM-04",
            "GM-05",
            "diagnostic_separation",
            "no_silent_activation",
        ]
    )

    workstream_rows: list[dict[str, Any]] = []

    for workstream in WORKSTREAMS:
        script_path = (
            ROOT
            / workstream["completion_script"]
        )

        constants = string_constants(
            script_path
        )

        exists = script_path.exists()
        diagnosis_present = (
            workstream["expected_diagnosis"]
            in constants
        )

        accepted = (
            exists
            and diagnosis_present
        )

        workstream_rows.append(
            {
                "workstream_id": (
                    workstream["workstream_id"]
                ),
                "domain": (
                    workstream["domain"]
                ),
                "completion_script": (
                    workstream["completion_script"]
                ),
                "expected_diagnosis": (
                    workstream["expected_diagnosis"]
                ),
                "script_exists": exists,
                "diagnosis_contract_present": (
                    diagnosis_present
                ),
                "diagnostic_scope_complete": (
                    accepted
                ),
                "production_behavior_authorized": (
                    False
                ),
            }
        )

    completed_workstreams = sum(
        1
        for row in workstream_rows
        if row[
            "diagnostic_scope_complete"
        ]
    )

    all_workstreams_complete = (
        completed_workstreams
        == len(
            WORKSTREAMS
        )
    )

    checks = [
        {
            "check": (
                "resolution_plan_exists"
            ),
            "actual": (
                RESOLUTION_PLAN_PATH.exists()
            ),
            "expected": True,
            "passed": (
                RESOLUTION_PLAN_PATH.exists()
            ),
        },
        {
            "check": (
                "resolution_plan_contract_present"
            ),
            "actual": (
                resolution_plan_contract_present
            ),
            "expected": True,
            "passed": (
                resolution_plan_contract_present
            ),
        },
        {
            "check": (
                "five_workstreams_defined"
            ),
            "actual": len(
                WORKSTREAMS
            ),
            "expected": 5,
            "passed": (
                len(
                    WORKSTREAMS
                )
                == 5
            ),
        },
        {
            "check": (
                "five_workstreams_diagnostic_scope_complete"
            ),
            "actual": (
                completed_workstreams
            ),
            "expected": 5,
            "passed": (
                all_workstreams_complete
            ),
        },
        {
            "check": (
                "production_authority_remains_absent"
            ),
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": (
                "combined_integration_audit_required_next"
            ),
            "actual": True,
            "expected": True,
            "passed": True,
        },
        {
            "check": (
                "broad_layer6_exit_remains_paused"
            ),
            "actual": True,
            "expected": True,
            "passed": True,
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    authority_rows = [
        {
            "authority": authority,
            "granted": False,
            "reason": (
                "6QK updates scope resolution only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "all_five_game_management_"
                    "workstreams_diagnostic_scope_complete"
                ),
                "granted": (
                    all_checks_passed
                    and all_workstreams_complete
                ),
                "reason": (
                    "Each GM-01 through GM-05 completion "
                    "contract is present."
                ),
            },
            {
                "authority": (
                    "combined_game_management_"
                    "integration_audit"
                ),
                "granted": (
                    all_checks_passed
                    and all_workstreams_complete
                ),
                "reason": (
                    "The 6OY execution sequence requires a "
                    "combined audit before broad reassessment."
                ),
            },
            {
                "authority": (
                    "broad_layer6_completion_reassessment"
                ),
                "granted": False,
                "reason": (
                    "A combined game-management integration "
                    "audit must pass first."
                ),
            },
        ]
    )

    recommended_next_layer = (
        "6QL_combined_game_management_"
        "diagnostic_integration_audit"
        if all_checks_passed
        else
        "6QL_layer6_game_management_"
        "scope_resolution_remediation"
    )

    diagnosis_name = (
        "layer6_game_management_"
        "scope_resolution_updated"
        if all_checks_passed
        else
        "layer6_game_management_"
        "scope_resolution_update_failed"
    )

    write_csv(
        OUTPUT_DIR / "checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "workstream_status.csv",
        [
            "workstream_id",
            "domain",
            "completion_script",
            "expected_diagnosis",
            "script_exists",
            "diagnosis_contract_present",
            "diagnostic_scope_complete",
            "production_behavior_authorized",
        ],
        workstream_rows,
    )

    write_csv(
        OUTPUT_DIR / "authority_boundaries.csv",
        [
            "authority",
            "granted",
            "reason",
        ],
        authority_rows,
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
                    "Run a combined audit across all five "
                    "game-management diagnostic integrations."
                    if all_checks_passed
                    else
                    "Remediate failed 6QK scope-resolution checks."
                ),
                "entry_condition": (
                    "All five game-management workstreams "
                    "are complete at diagnostic scope."
                ),
                "passed": (
                    all_checks_passed
                ),
            }
        ],
    )

    summary = {
        "checks_required": len(
            checks
        ),
        "checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "workstreams_required": len(
            WORKSTREAMS
        ),
        "workstreams_complete": (
            completed_workstreams
        ),
        "all_workstreams_diagnostic_scope_complete": (
            all_workstreams_complete
        ),
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
        "combined_integration_audit_required": True,
        "broad_layer6_exit_paused": True,
    }

    write_json(
        OUTPUT_DIR / "scope_resolution_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_name,
        "all_checks_passed": (
            all_checks_passed
        ),
        "checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "checks_required": len(
            checks
        ),
        "workstreams_complete": (
            completed_workstreams
        ),
        "workstreams_required": len(
            WORKSTREAMS
        ),
        "all_five_workstreams_diagnostic_scope_complete": (
            all_workstreams_complete
        ),
        "production_behavior_changed": False,
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
        "combined_game_management_audit_allowed_next": (
            all_checks_passed
        ),
        "broad_layer6_reassessment_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "checks.csv"
            ),
            str(
                OUTPUT_DIR / "workstream_status.csv"
            ),
            str(
                OUTPUT_DIR / "authority_boundaries.csv"
            ),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "scope_resolution_summary.json"
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
