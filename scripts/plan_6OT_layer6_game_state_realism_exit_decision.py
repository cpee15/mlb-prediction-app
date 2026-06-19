#!/usr/bin/env python3
"""
Layer 6OT
Layer 6 Game-State Realism Exit Decision Plan

Planning-only layer.

Defines the formal decision framework required before Layer 6
game-state realism can be exited.

This script does not:
- change backend or frontend behavior
- change simulation parameters or probabilities
- replace canonical probabilities
- perform historical validation
- tune parameters
- run backtests
- perform pricing or edge detection
- exit Layer 6
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6OT"
LAYER_NAME = (
    "layer6_game_state_realism_exit_decision_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6OT_game_state_realism_exit_decision_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/audit_6OS_layer6_game_state_realism_"
    "exit_readiness.py"
)

REQUIRED_SOURCE_PATHS = [
    PREDECESSOR_PATH,
    ROOT
    / "scripts/implement_6OR_layer6_game_state_realism_"
    "exit_readiness.py",
    ROOT
    / "scripts/plan_6OQ_layer6_game_state_realism_"
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
    "layer6_exit_execution",
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
                "exit_readiness_audit_complete"
            ),
            "all_checks_passed",
            "evidence_chain_verified",
            "probability_guard_verified",
            "frontend_runtime_verified",
            "safety_boundaries_verified",
            "exit_decision_planning_allowed_next",
            (
                "6OT_layer6_game_state_realism_"
                "exit_decision_plan"
            ),
        ]
    )

    predecessor_counts_present = all(
        token in predecessor_constants
        for token in [
            "audit_checks_passed",
            "artifacts_verified",
            "domains_verified",
            "criteria_verified",
            "blocking_criteria_verified",
            "known_gaps_verified",
            "blocking_known_gaps",
            "unidentified_blocking_gaps",
        ]
    )

    scope_guards_present = all(
        token in predecessor_constants
        for token in [
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
            "check": "6os_predecessor_contract_present",
            "passed": predecessor_contract_present,
            "evidence": str(PREDECESSOR_PATH),
        },
        {
            "check": "6os_evidence_counts_present",
            "passed": predecessor_counts_present,
            "evidence": (
                "audit, artifact, domain, criterion, gap, "
                "and blocking-gap evidence"
            ),
        },
        {
            "check": "6os_scope_guards_present",
            "passed": scope_guards_present,
            "evidence": (
                "behavior, probability, validation, tuning, "
                "backtest, pricing, and edge guards"
            ),
        },
        {
            "check": "planning_layer_only",
            "passed": True,
            "evidence": (
                "6OT defines decision logic and does not "
                "perform the exit decision"
            ),
        },
    ]

    decision_outcomes = [
        {
            "outcome": "exit_layer6_game_state_realism",
            "meaning": (
                "Layer 6 game-state realism scope is complete "
                "under its documented boundaries."
            ),
            "allowed_when": (
                "Every mandatory decision criterion passes and "
                "no blocking or unidentified gap exists."
            ),
            "terminal_for_6OT": False,
        },
        {
            "outcome": "hold_layer6_for_blocking_gap",
            "meaning": (
                "At least one blocking requirement is missing, "
                "failed, contradictory, or unsupported."
            ),
            "allowed_when": (
                "Any mandatory criterion fails or a blocking "
                "gap is identified."
            ),
            "terminal_for_6OT": False,
        },
        {
            "outcome": "hold_layer6_for_evidence_inconsistency",
            "meaning": (
                "Evidence exists but cannot be independently "
                "reconciled across implementation and audit artifacts."
            ),
            "allowed_when": (
                "Counts, diagnoses, artifacts, or guards conflict."
            ),
            "terminal_for_6OT": False,
        },
    ]

    decision_criteria = [
        {
            "criterion_id": "L6-EXIT-01",
            "criterion": (
                "The independent 6OS exit-readiness audit "
                "re-executes successfully."
            ),
            "mandatory": True,
            "failure_outcome": (
                "hold_layer6_for_evidence_inconsistency"
            ),
        },
        {
            "criterion_id": "L6-EXIT-02",
            "criterion": (
                "All readiness domains and exit criteria are "
                "independently verified."
            ),
            "mandatory": True,
            "failure_outcome": (
                "hold_layer6_for_blocking_gap"
            ),
        },
        {
            "criterion_id": "L6-EXIT-03",
            "criterion": (
                "All blocking exit criteria pass."
            ),
            "mandatory": True,
            "failure_outcome": (
                "hold_layer6_for_blocking_gap"
            ),
        },
        {
            "criterion_id": "L6-EXIT-04",
            "criterion": (
                "No blocking known gap or unidentified blocking "
                "gap remains."
            ),
            "mandatory": True,
            "failure_outcome": (
                "hold_layer6_for_blocking_gap"
            ),
        },
        {
            "criterion_id": "L6-EXIT-05",
            "criterion": (
                "Canonical probabilities remain unchanged and "
                "separate from diagnostic exposure."
            ),
            "mandatory": True,
            "failure_outcome": (
                "hold_layer6_for_blocking_gap"
            ),
        },
        {
            "criterion_id": "L6-EXIT-06",
            "criterion": (
                "Frontend build and deployed-page runtime evidence "
                "remain successful."
            ),
            "mandatory": True,
            "failure_outcome": (
                "hold_layer6_for_blocking_gap"
            ),
        },
        {
            "criterion_id": "L6-EXIT-07",
            "criterion": (
                "Accepted gaps remain explicitly nonblocking and "
                "do not overstate implemented scope."
            ),
            "mandatory": True,
            "failure_outcome": (
                "hold_layer6_for_blocking_gap"
            ),
        },
        {
            "criterion_id": "L6-EXIT-08",
            "criterion": (
                "The exit decision does not authorize historical "
                "accuracy, tuning, pricing, or edge claims."
            ),
            "mandatory": True,
            "failure_outcome": (
                "hold_layer6_for_blocking_gap"
            ),
        },
    ]

    accepted_scope_boundaries = [
        {
            "boundary_id": "L6-SCOPE-01",
            "description": (
                "Steal simulation remains deferred and inactive."
            ),
            "accepted": True,
            "blocks_exit": False,
            "required_post_exit_label": (
                "deferred_not_active"
            ),
        },
        {
            "boundary_id": "L6-SCOPE-02",
            "description": (
                "Game-state realism diagnostics are not historical "
                "accuracy or calibration evidence."
            ),
            "accepted": True,
            "blocks_exit": False,
            "required_post_exit_label": (
                "diagnostic_only_not_accuracy_validated"
            ),
        },
        {
            "boundary_id": "L6-SCOPE-03",
            "description": (
                "Unrelated frontend duplicate-key, dependency, and "
                "bundle warnings remain separate technical debt."
            ),
            "accepted": True,
            "blocks_exit": False,
            "required_post_exit_label": (
                "nonblocking_unrelated_technical_debt"
            ),
        },
    ]

    hold_conditions = [
        {
            "hold_id": "L6-HOLD-01",
            "condition": (
                "The predecessor audit fails or cannot execute."
            ),
            "decision": (
                "hold_layer6_for_evidence_inconsistency"
            ),
        },
        {
            "hold_id": "L6-HOLD-02",
            "condition": (
                "A required artifact is missing or empty."
            ),
            "decision": (
                "hold_layer6_for_evidence_inconsistency"
            ),
        },
        {
            "hold_id": "L6-HOLD-03",
            "condition": (
                "Any mandatory exit criterion fails."
            ),
            "decision": (
                "hold_layer6_for_blocking_gap"
            ),
        },
        {
            "hold_id": "L6-HOLD-04",
            "condition": (
                "Any blocking or unidentified blocking gap exists."
            ),
            "decision": (
                "hold_layer6_for_blocking_gap"
            ),
        },
        {
            "hold_id": "L6-HOLD-05",
            "condition": (
                "Probability separation or runtime evidence fails."
            ),
            "decision": (
                "hold_layer6_for_blocking_gap"
            ),
        },
        {
            "hold_id": "L6-HOLD-06",
            "condition": (
                "The proposed exit expands authority into tuning, "
                "accuracy, pricing, or edge detection."
            ),
            "decision": (
                "hold_layer6_for_blocking_gap"
            ),
        },
    ]

    post_exit_boundaries = [
        {
            "boundary": "game_state_realism_scope",
            "status_after_exit": (
                "complete_under_documented_scope"
            ),
            "new_authority_granted": False,
        },
        {
            "boundary": "steal_simulation",
            "status_after_exit": "deferred_not_active",
            "new_authority_granted": False,
        },
        {
            "boundary": "historical_accuracy_validation",
            "status_after_exit": "not_authorized",
            "new_authority_granted": False,
        },
        {
            "boundary": "parameter_tuning",
            "status_after_exit": "not_authorized",
            "new_authority_granted": False,
        },
        {
            "boundary": "pricing_and_edge_detection",
            "status_after_exit": "not_authorized",
            "new_authority_granted": False,
        },
        {
            "boundary": "canonical_probability_replacement",
            "status_after_exit": "not_authorized",
            "new_authority_granted": False,
        },
    ]

    required_decision_artifacts = [
        {
            "artifact": "decision_checks.csv",
            "purpose": (
                "Machine-readable evaluation of every exit criterion."
            ),
            "required": True,
        },
        {
            "artifact": "decision_outcome.csv",
            "purpose": (
                "Single selected decision with supporting rationale."
            ),
            "required": True,
        },
        {
            "artifact": "scope_boundaries.csv",
            "purpose": (
                "Accepted and post-exit scope boundaries."
            ),
            "required": True,
        },
        {
            "artifact": "evidence_summary.json",
            "purpose": (
                "Consolidated audited evidence used by the decision."
            ),
            "required": True,
        },
        {
            "artifact": "diagnosis.json",
            "purpose": (
                "Formal decision diagnosis and next-layer routing."
            ),
            "required": True,
        },
    ]

    execution_sequence = [
        {
            "step": 1,
            "action": (
                "Re-execute the independent 6OS audit."
            ),
            "success_criterion": (
                "All 6OS checks and artifacts pass."
            ),
        },
        {
            "step": 2,
            "action": (
                "Evaluate all mandatory exit criteria."
            ),
            "success_criterion": (
                "Every mandatory criterion has explicit evidence."
            ),
        },
        {
            "step": 3,
            "action": (
                "Reconcile blocking, unidentified, and accepted gaps."
            ),
            "success_criterion": (
                "Zero blocking and unidentified gaps remain."
            ),
        },
        {
            "step": 4,
            "action": (
                "Verify probability, runtime, and safety guards."
            ),
            "success_criterion": (
                "All guards remain intact."
            ),
        },
        {
            "step": 5,
            "action": (
                "Select exactly one formal decision outcome."
            ),
            "success_criterion": (
                "The outcome follows deterministic decision rules."
            ),
        },
        {
            "step": 6,
            "action": (
                "Produce decision and boundary artifacts."
            ),
            "success_criterion": (
                "Every required artifact is emitted."
            ),
        },
    ]

    safety_rows = [
        {
            "boundary": action,
            "allowed_in_6OT": False,
            "reason": (
                "6OT plans the decision framework only."
            ),
        }
        for action in PROHIBITED_ACTIONS
    ]

    safety_rows.extend(
        [
            {
                "boundary": "decision_criteria_definition",
                "allowed_in_6OT": True,
                "reason": (
                    "Defining exit criteria is the purpose of 6OT."
                ),
            },
            {
                "boundary": "hold_condition_definition",
                "allowed_in_6OT": True,
                "reason": (
                    "Hold rules prevent unsupported exit decisions."
                ),
            },
            {
                "boundary": "post_exit_boundary_definition",
                "allowed_in_6OT": True,
                "reason": (
                    "Defining boundaries does not grant new authority."
                ),
            },
        ]
    )

    all_checks_passed = all(
        bool(row["passed"])
        for row in planning_checks
    )

    recommended_next_layer = (
        "6OU_layer6_game_state_realism_"
        "exit_decision_implementation"
    )

    write_csv(
        OUTPUT_DIR / "checks.csv",
        ["check", "passed", "evidence"],
        planning_checks,
    )

    write_csv(
        OUTPUT_DIR / "decision_outcomes.csv",
        [
            "outcome",
            "meaning",
            "allowed_when",
            "terminal_for_6OT",
        ],
        decision_outcomes,
    )

    write_csv(
        OUTPUT_DIR / "decision_criteria.csv",
        [
            "criterion_id",
            "criterion",
            "mandatory",
            "failure_outcome",
        ],
        decision_criteria,
    )

    write_csv(
        OUTPUT_DIR / "accepted_scope_boundaries.csv",
        [
            "boundary_id",
            "description",
            "accepted",
            "blocks_exit",
            "required_post_exit_label",
        ],
        accepted_scope_boundaries,
    )

    write_csv(
        OUTPUT_DIR / "hold_conditions.csv",
        [
            "hold_id",
            "condition",
            "decision",
        ],
        hold_conditions,
    )

    write_csv(
        OUTPUT_DIR / "post_exit_boundaries.csv",
        [
            "boundary",
            "status_after_exit",
            "new_authority_granted",
        ],
        post_exit_boundaries,
    )

    write_csv(
        OUTPUT_DIR / "required_decision_artifacts.csv",
        [
            "artifact",
            "purpose",
            "required",
        ],
        required_decision_artifacts,
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
            "allowed_in_6OT",
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
                    "Implement the deterministic Layer 6 "
                    "game-state realism exit decision."
                ),
                "entry_condition": (
                    "All 6OT planning checks pass."
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
            "exit_decision_plan_complete"
            if all_checks_passed
            else
            "layer_6_game_state_realism_"
            "exit_decision_plan_failed"
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
        "decision_outcomes_planned": len(
            decision_outcomes
        ),
        "decision_criteria_planned": len(
            decision_criteria
        ),
        "mandatory_decision_criteria": sum(
            1
            for row in decision_criteria
            if row["mandatory"]
        ),
        "accepted_scope_boundaries": len(
            accepted_scope_boundaries
        ),
        "blocking_scope_boundaries": sum(
            1
            for row in accepted_scope_boundaries
            if row["blocks_exit"]
        ),
        "hold_conditions_planned": len(
            hold_conditions
        ),
        "post_exit_boundaries_planned": len(
            post_exit_boundaries
        ),
        "required_decision_artifacts_planned": len(
            required_decision_artifacts
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
        "layer6_exit_recommended": False,
        "exit_decision_implementation_allowed_next": (
            all_checks_passed
        ),
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / "checks.csv"),
            str(
                OUTPUT_DIR / "decision_outcomes.csv"
            ),
            str(
                OUTPUT_DIR / "decision_criteria.csv"
            ),
            str(
                OUTPUT_DIR
                / "accepted_scope_boundaries.csv"
            ),
            str(
                OUTPUT_DIR / "hold_conditions.csv"
            ),
            str(
                OUTPUT_DIR / "post_exit_boundaries.csv"
            ),
            str(
                OUTPUT_DIR
                / "required_decision_artifacts.csv"
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
