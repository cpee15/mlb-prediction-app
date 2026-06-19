#!/usr/bin/env python3
"""
Layer 6OW
Layer 6 Game-State Realism Exit Finalization Plan

Planning-only layer.

Defines the deterministic framework for formally finalizing the
independently audited Layer 6 game-state realism exit decision.

This script does not:
- finalize Layer 6
- change backend or frontend behavior
- change simulation parameters or probabilities
- replace canonical probabilities
- perform historical validation
- tune parameters
- run backtests
- perform pricing or edge detection
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6OW"
LAYER_NAME = (
    "layer6_game_state_realism_exit_finalization_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6OW_game_state_realism_"
    "exit_finalization_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/audit_6OV_layer6_game_state_realism_"
    "exit_decision.py"
)

REQUIRED_SOURCE_PATHS = [
    PREDECESSOR_PATH,
    ROOT
    / "scripts/implement_6OU_layer6_game_state_realism_"
    "exit_decision.py",
    ROOT
    / "scripts/plan_6OT_layer6_game_state_realism_"
    "exit_decision.py",
    ROOT
    / "scripts/audit_6OS_layer6_game_state_realism_"
    "exit_readiness.py",
    ROOT / "mlb_app/model_projections.py",
    ROOT / "frontend/src/pages/ModelProjectionsPage.jsx",
]

PROHIBITED_ACTIONS = [
    "backend_behavior_change",
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
    "unaudited_layer6_exit",
]


def write_csv(
    path: Path,
    fieldnames: list[str],
    rows: Iterable[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )


def string_constants(path: Path) -> set[str]:
    if not path.exists():
        return set()

    tree = ast.parse(
        path.read_text(
            encoding="utf-8",
            errors="ignore",
        )
    )

    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    predecessor_constants = string_constants(
        PREDECESSOR_PATH
    )

    required_sources_exist = all(
        path.exists()
        for path in REQUIRED_SOURCE_PATHS
    )

    predecessor_contract_present = all(
        token in predecessor_constants
        for token in [
            (
                "layer_6_game_state_realism_"
                "exit_decision_audit_complete"
            ),
            "selected_outcome_verified",
            "exit_layer6_game_state_realism",
            "audit_checks_passed",
            "artifacts_verified",
            "implementation_checks_verified",
            "decision_criteria_verified",
            "scope_boundaries_verified",
            "safety_boundaries_verified",
            "layer6_exit_recommended",
            "layer6_exit_finalized",
            (
                "exit_finalization_planning_allowed_next"
            ),
            (
                "6OW_layer6_game_state_realism_"
                "exit_finalization_plan"
            ),
        ]
    )

    predecessor_guard_contract_present = all(
        token in predecessor_constants
        for token in [
            "blocking_known_gaps",
            "unidentified_blocking_gaps",
            "probability_guard_verified",
            "frontend_runtime_verified",
            "new_authority_granted",
            "backend_behavior_change_allowed_next",
            "frontend_behavior_change_allowed_next",
            "simulation_parameter_change_allowed_next",
            "final_probability_replacement_allowed_next",
            "historical_validation_allowed_next",
            "tuning_allowed_next",
            "backtests_allowed_next",
            "pricing_allowed_next",
            "edge_detection_allowed_next",
        ]
    )

    planning_checks = [
        {
            "check": "required_source_files_exist",
            "passed": required_sources_exist,
            "evidence": ",".join(
                str(path)
                for path in REQUIRED_SOURCE_PATHS
            ),
        },
        {
            "check": "6ov_audit_contract_present",
            "passed": predecessor_contract_present,
            "evidence": str(PREDECESSOR_PATH),
        },
        {
            "check": "6ov_guard_contract_present",
            "passed": (
                predecessor_guard_contract_present
            ),
            "evidence": (
                "gap, probability, runtime, scope, safety, "
                "and authority guards"
            ),
        },
        {
            "check": "finalization_is_record_only",
            "passed": True,
            "evidence": (
                "Finalization records audited completion without "
                "changing production behavior or model authority."
            ),
        },
        {
            "check": "planning_layer_only",
            "passed": True,
            "evidence": (
                "6OW defines finalization rules and does not "
                "finalize Layer 6."
            ),
        },
    ]

    finalization_outcomes = [
        {
            "outcome": (
                "finalize_layer6_game_state_realism_exit"
            ),
            "meaning": (
                "Record Layer 6 game-state realism as complete "
                "under its audited and documented scope."
            ),
            "allowed_when": (
                "Every mandatory finalization criterion passes "
                "and no boundary or evidence violation exists."
            ),
        },
        {
            "outcome": (
                "hold_finalization_for_audit_failure"
            ),
            "meaning": (
                "Do not finalize because the independent exit "
                "audit is missing, failed, or inconsistent."
            ),
            "allowed_when": (
                "The 6OV audit cannot be independently "
                "re-executed or verified."
            ),
        },
        {
            "outcome": (
                "hold_finalization_for_boundary_violation"
            ),
            "meaning": (
                "Do not finalize because finalization would "
                "expand scope or grant unsupported authority."
            ),
            "allowed_when": (
                "Any accepted or post-exit boundary is altered, "
                "removed, or contradicted."
            ),
        },
    ]

    finalization_criteria = [
        {
            "criterion_id": "L6-FINAL-01",
            "criterion": (
                "The independent 6OV exit-decision audit "
                "re-executes successfully."
            ),
            "mandatory": True,
            "failure_outcome": (
                "hold_finalization_for_audit_failure"
            ),
        },
        {
            "criterion_id": "L6-FINAL-02",
            "criterion": (
                "6OV verifies the single selected outcome "
                "exit_layer6_game_state_realism."
            ),
            "mandatory": True,
            "failure_outcome": (
                "hold_finalization_for_audit_failure"
            ),
        },
        {
            "criterion_id": "L6-FINAL-03",
            "criterion": (
                "All required audit artifacts, implementation "
                "checks, and decision criteria are verified."
            ),
            "mandatory": True,
            "failure_outcome": (
                "hold_finalization_for_audit_failure"
            ),
        },
        {
            "criterion_id": "L6-FINAL-04",
            "criterion": (
                "Zero blocking known gaps and zero unidentified "
                "blocking gaps remain."
            ),
            "mandatory": True,
            "failure_outcome": (
                "hold_finalization_for_audit_failure"
            ),
        },
        {
            "criterion_id": "L6-FINAL-05",
            "criterion": (
                "Probability separation, frontend runtime, and "
                "safety guards remain verified."
            ),
            "mandatory": True,
            "failure_outcome": (
                "hold_finalization_for_boundary_violation"
            ),
        },
        {
            "criterion_id": "L6-FINAL-06",
            "criterion": (
                "All accepted and post-exit scope boundaries "
                "remain intact with no new authority granted."
            ),
            "mandatory": True,
            "failure_outcome": (
                "hold_finalization_for_boundary_violation"
            ),
        },
        {
            "criterion_id": "L6-FINAL-07",
            "criterion": (
                "Finalization records completion only and does "
                "not authorize accuracy, tuning, pricing, edge "
                "detection, probability replacement, or behavior "
                "changes."
            ),
            "mandatory": True,
            "failure_outcome": (
                "hold_finalization_for_boundary_violation"
            ),
        },
    ]

    accepted_boundaries = [
        {
            "boundary_id": "L6-SCOPE-01",
            "status": "deferred_not_active",
            "description": (
                "Steal simulation remains deferred."
            ),
            "must_persist_after_finalization": True,
        },
        {
            "boundary_id": "L6-SCOPE-02",
            "status": (
                "diagnostic_only_not_accuracy_validated"
            ),
            "description": (
                "Game-state realism diagnostics remain separate "
                "from calibration and historical accuracy claims."
            ),
            "must_persist_after_finalization": True,
        },
        {
            "boundary_id": "L6-SCOPE-03",
            "status": (
                "nonblocking_unrelated_technical_debt"
            ),
            "description": (
                "Unrelated frontend warnings remain outside "
                "Layer 6 finalization scope."
            ),
            "must_persist_after_finalization": True,
        },
    ]

    post_exit_boundaries = [
        {
            "boundary": "game_state_realism_scope",
            "final_status": (
                "complete_under_documented_scope"
            ),
            "new_authority_granted": False,
        },
        {
            "boundary": "steal_simulation",
            "final_status": "deferred_not_active",
            "new_authority_granted": False,
        },
        {
            "boundary": "historical_accuracy_validation",
            "final_status": "not_authorized",
            "new_authority_granted": False,
        },
        {
            "boundary": "parameter_tuning",
            "final_status": "not_authorized",
            "new_authority_granted": False,
        },
        {
            "boundary": "pricing_and_edge_detection",
            "final_status": "not_authorized",
            "new_authority_granted": False,
        },
        {
            "boundary": (
                "canonical_probability_replacement"
            ),
            "final_status": "not_authorized",
            "new_authority_granted": False,
        },
    ]

    finalization_record_fields = [
        {
            "field": "layer_id",
            "required_value": "6",
            "purpose": (
                "Identifies the completed layer."
            ),
        },
        {
            "field": "scope",
            "required_value": "game_state_realism",
            "purpose": (
                "Identifies the bounded completion scope."
            ),
        },
        {
            "field": "final_status",
            "required_value": (
                "complete_under_documented_scope"
            ),
            "purpose": (
                "Records the formal bounded exit."
            ),
        },
        {
            "field": "audited_outcome",
            "required_value": (
                "exit_layer6_game_state_realism"
            ),
            "purpose": (
                "Records the independently audited decision."
            ),
        },
        {
            "field": "new_authority_granted",
            "required_value": "False",
            "purpose": (
                "Prevents scope expansion through finalization."
            ),
        },
        {
            "field": "next_layer_authority",
            "required_value": "separately_planned",
            "purpose": (
                "Requires future work to establish its own scope."
            ),
        },
    ]

    required_artifacts = [
        {
            "artifact": "finalization_checks.csv",
            "purpose": (
                "Evaluation of every mandatory finalization "
                "criterion."
            ),
            "required": True,
        },
        {
            "artifact": "finalization_outcome.csv",
            "purpose": (
                "Single deterministic finalization result."
            ),
            "required": True,
        },
        {
            "artifact": "finalization_record.csv",
            "purpose": (
                "Machine-readable Layer 6 completion record."
            ),
            "required": True,
        },
        {
            "artifact": "scope_boundaries.csv",
            "purpose": (
                "Persisted accepted and post-exit boundaries."
            ),
            "required": True,
        },
        {
            "artifact": "evidence_summary.json",
            "purpose": (
                "Consolidated independently audited evidence."
            ),
            "required": True,
        },
        {
            "artifact": "diagnosis.json",
            "purpose": (
                "Formal finalization diagnosis and routing."
            ),
            "required": True,
        },
    ]

    execution_sequence = [
        {
            "step": 1,
            "action": (
                "Re-execute the independent 6OV audit."
            ),
            "success_criterion": (
                "Every 6OV audit check passes."
            ),
        },
        {
            "step": 2,
            "action": (
                "Verify all mandatory finalization criteria."
            ),
            "success_criterion": (
                "All seven criteria have passing evidence."
            ),
        },
        {
            "step": 3,
            "action": (
                "Reconcile accepted and post-exit boundaries."
            ),
            "success_criterion": (
                "All nine boundaries remain intact."
            ),
        },
        {
            "step": 4,
            "action": (
                "Confirm no new model authority is granted."
            ),
            "success_criterion": (
                "Every prohibited authority remains false."
            ),
        },
        {
            "step": 5,
            "action": (
                "Select exactly one finalization outcome."
            ),
            "success_criterion": (
                "The outcome follows deterministic rules."
            ),
        },
        {
            "step": 6,
            "action": (
                "Emit the finalization record and evidence."
            ),
            "success_criterion": (
                "All six required artifacts are produced."
            ),
        },
    ]

    safety_rows = [
        {
            "boundary": action,
            "allowed_in_6OW": False,
            "reason": (
                "6OW plans finalization only."
            ),
        }
        for action in PROHIBITED_ACTIONS
    ]

    safety_rows.extend(
        [
            {
                "boundary": (
                    "finalization_criteria_definition"
                ),
                "allowed_in_6OW": True,
                "reason": (
                    "Defining finalization criteria is the "
                    "purpose of 6OW."
                ),
            },
            {
                "boundary": (
                    "finalization_record_schema_definition"
                ),
                "allowed_in_6OW": True,
                "reason": (
                    "Defining the record does not finalize Layer 6."
                ),
            },
            {
                "boundary": (
                    "post_exit_boundary_definition"
                ),
                "allowed_in_6OW": True,
                "reason": (
                    "Boundary definition prevents unsupported "
                    "authority expansion."
                ),
            },
        ]
    )

    all_checks_passed = all(
        bool(row["passed"])
        for row in planning_checks
    )

    recommended_next_layer = (
        "6OX_layer6_game_state_realism_"
        "exit_finalization_implementation"
    )

    write_csv(
        OUTPUT_DIR / "checks.csv",
        ["check", "passed", "evidence"],
        planning_checks,
    )

    write_csv(
        OUTPUT_DIR / "finalization_outcomes.csv",
        [
            "outcome",
            "meaning",
            "allowed_when",
        ],
        finalization_outcomes,
    )

    write_csv(
        OUTPUT_DIR / "finalization_criteria.csv",
        [
            "criterion_id",
            "criterion",
            "mandatory",
            "failure_outcome",
        ],
        finalization_criteria,
    )

    write_csv(
        OUTPUT_DIR / "accepted_boundaries.csv",
        [
            "boundary_id",
            "status",
            "description",
            "must_persist_after_finalization",
        ],
        accepted_boundaries,
    )

    write_csv(
        OUTPUT_DIR / "post_exit_boundaries.csv",
        [
            "boundary",
            "final_status",
            "new_authority_granted",
        ],
        post_exit_boundaries,
    )

    write_csv(
        OUTPUT_DIR / "finalization_record_schema.csv",
        [
            "field",
            "required_value",
            "purpose",
        ],
        finalization_record_fields,
    )

    write_csv(
        OUTPUT_DIR / "required_artifacts.csv",
        [
            "artifact",
            "purpose",
            "required",
        ],
        required_artifacts,
    )

    write_csv(
        OUTPUT_DIR / "execution_sequence.csv",
        [
            "step",
            "action",
            "success_criterion",
        ],
        execution_sequence,
    )

    write_csv(
        OUTPUT_DIR / "safety_boundaries.csv",
        [
            "boundary",
            "allowed_in_6OW",
            "reason",
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
                    "Implement the formal Layer 6 game-state "
                    "realism exit finalization record."
                ),
                "entry_condition": (
                    "All 6OW planning checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "layer_6_game_state_realism_"
            "exit_finalization_plan_complete"
            if all_checks_passed
            else
            "layer_6_game_state_realism_"
            "exit_finalization_plan_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "planning_checks_passed": sum(
            1
            for row in planning_checks
            if row["passed"]
        ),
        "planning_checks_required": len(
            planning_checks
        ),
        "finalization_outcomes_planned": len(
            finalization_outcomes
        ),
        "finalization_criteria_planned": len(
            finalization_criteria
        ),
        "mandatory_finalization_criteria": sum(
            1
            for row in finalization_criteria
            if row["mandatory"]
        ),
        "accepted_boundaries_planned": len(
            accepted_boundaries
        ),
        "post_exit_boundaries_planned": len(
            post_exit_boundaries
        ),
        "finalization_record_fields_planned": len(
            finalization_record_fields
        ),
        "required_artifacts_planned": len(
            required_artifacts
        ),
        "execution_steps_planned": len(
            execution_sequence
        ),
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
        "layer6_exit_finalized": False,
        "exit_finalization_implementation_allowed_next": (
            all_checks_passed
        ),
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / "checks.csv"),
            str(
                OUTPUT_DIR / "finalization_outcomes.csv"
            ),
            str(
                OUTPUT_DIR / "finalization_criteria.csv"
            ),
            str(
                OUTPUT_DIR / "accepted_boundaries.csv"
            ),
            str(
                OUTPUT_DIR / "post_exit_boundaries.csv"
            ),
            str(
                OUTPUT_DIR
                / "finalization_record_schema.csv"
            ),
            str(
                OUTPUT_DIR / "required_artifacts.csv"
            ),
            str(
                OUTPUT_DIR / "execution_sequence.csv"
            ),
            str(
                OUTPUT_DIR / "safety_boundaries.csv"
            ),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
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
