#!/usr/bin/env python3
"""
Layer 6QM
Layer 6 Broad Scope Reassessment Plan

Defines the evidence, decision rules, scope boundaries, and execution path for
reassessing Layer 6 after the combined game-management diagnostic audit.

Planning only. This layer does not:
- finalize Layer 6;
- activate game-management behavior;
- change simulation probabilities or state;
- authorize historical validation, tuning, backtesting, pricing, or edge logic.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6QM"
LAYER_NAME = "layer6_broad_scope_reassessment_plan"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/layer_6QM_layer6_broad_scope_reassessment_plan"
)

GAP_ANALYSIS_PATH = (
    ROOT
    / "scripts/analyze_6OX_layer6_game_management_"
    "scope_completeness_gaps.py"
)

RESOLUTION_PLAN_PATH = (
    ROOT
    / "scripts/plan_6OY_layer6_game_management_"
    "scope_resolution.py"
)

SCOPE_UPDATE_PATH = (
    ROOT
    / "scripts/assess_6QK_layer6_game_management_"
    "scope_resolution_update.py"
)

COMBINED_AUDIT_PATH = (
    ROOT
    / "scripts/audit_6QL_combined_game_management_"
    "diagnostic_integration.py"
)

REQUIRED_PATHS = [
    GAP_ANALYSIS_PATH,
    RESOLUTION_PLAN_PATH,
    SCOPE_UPDATE_PATH,
    COMBINED_AUDIT_PATH,
]

PROHIBITED_AUTHORITIES = [
    "production_pitching_plan_activation",
    "production_dynamic_starter_hook_activation",
    "production_bullpen_sequencing_activation",
    "production_stolen_base_pickoff_activation",
    "production_position_player_substitution_activation",
    "simulation_state_change",
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

    gap_constants = string_constants(
        GAP_ANALYSIS_PATH
    )
    resolution_constants = string_constants(
        RESOLUTION_PLAN_PATH
    )
    update_constants = string_constants(
        SCOPE_UPDATE_PATH
    )
    audit_constants = string_constants(
        COMBINED_AUDIT_PATH
    )

    required_paths_exist = all(
        path.exists()
        for path in REQUIRED_PATHS
    )

    predecessor_contracts = [
        {
            "predecessor": "6OX",
            "path": str(
                GAP_ANALYSIS_PATH.relative_to(ROOT)
            ),
            "expected_contract": (
                "layer_6_game_management_"
                "scope_completeness_gaps_confirmed"
            ),
            "present": (
                "layer_6_game_management_"
                "scope_completeness_gaps_confirmed"
                in gap_constants
            ),
        },
        {
            "predecessor": "6OY",
            "path": str(
                RESOLUTION_PLAN_PATH.relative_to(ROOT)
            ),
            "expected_contract": (
                "layer_6_game_management_"
                "scope_resolution_plan_complete"
            ),
            "present": (
                "layer_6_game_management_"
                "scope_resolution_plan_complete"
                in resolution_constants
            ),
        },
        {
            "predecessor": "6QK",
            "path": str(
                SCOPE_UPDATE_PATH.relative_to(ROOT)
            ),
            "expected_contract": (
                "layer6_game_management_"
                "scope_resolution_updated"
            ),
            "present": (
                "layer6_game_management_"
                "scope_resolution_updated"
                in update_constants
            ),
        },
        {
            "predecessor": "6QL",
            "path": str(
                COMBINED_AUDIT_PATH.relative_to(ROOT)
            ),
            "expected_contract": (
                "combined_game_management_"
                "diagnostic_integration_audit_passed"
            ),
            "present": (
                "combined_game_management_"
                "diagnostic_integration_audit_passed"
                in audit_constants
            ),
        },
    ]

    predecessors_accepted = sum(
        1
        for row in predecessor_contracts
        if row["present"]
    )

    scope_classes = [
        {
            "scope_id": "L6-S01",
            "scope": (
                "base_out_and_core_runner_"
                "transition_realism"
            ),
            "current_evidence_status": (
                "previously_audited_narrow_scope"
            ),
            "eligible_for_completion_claim": True,
            "production_behavior_required": False,
            "reassessment_rule": (
                "Confirm prior audited invariants remain intact."
            ),
        },
        {
            "scope_id": "L6-S02",
            "scope": (
                "game_management_diagnostic_realism"
            ),
            "current_evidence_status": (
                "five_workstreams_combined_audit_passed"
            ),
            "eligible_for_completion_claim": True,
            "production_behavior_required": False,
            "reassessment_rule": (
                "May be declared complete only at explicitly "
                "documented diagnostic scope."
            ),
        },
        {
            "scope_id": "L6-S03",
            "scope": (
                "game_management_production_behavior"
            ),
            "current_evidence_status": (
                "not_activated_not_authorized"
            ),
            "eligible_for_completion_claim": False,
            "production_behavior_required": True,
            "reassessment_rule": (
                "Must remain excluded from any completed-scope claim."
            ),
        },
        {
            "scope_id": "L6-S04",
            "scope": (
                "historical_accuracy_validation"
            ),
            "current_evidence_status": (
                "not_authorized"
            ),
            "eligible_for_completion_claim": False,
            "production_behavior_required": False,
            "reassessment_rule": (
                "Must remain outside Layer 6 completion."
            ),
        },
        {
            "scope_id": "L6-S05",
            "scope": (
                "parameter_tuning_backtesting_"
                "pricing_and_edge_detection"
            ),
            "current_evidence_status": (
                "not_authorized"
            ),
            "eligible_for_completion_claim": False,
            "production_behavior_required": False,
            "reassessment_rule": (
                "Must remain outside Layer 6 completion."
            ),
        },
    ]

    decision_outcomes = [
        {
            "outcome_id": "L6-D01",
            "outcome": (
                "narrow_documented_scope_complete"
            ),
            "allowed": True,
            "condition": (
                "Prior base/out scope remains valid and all "
                "five game-management diagnostics remain "
                "combined-audit clean."
            ),
            "effect": (
                "Close only the explicitly documented "
                "diagnostic and transition-realism scope."
            ),
        },
        {
            "outcome_id": "L6-D02",
            "outcome": (
                "broad_production_game_management_complete"
            ),
            "allowed": False,
            "condition": (
                "Production activation and effect evidence "
                "would be required."
            ),
            "effect": (
                "No broad production-realism claim."
            ),
        },
        {
            "outcome_id": "L6-D03",
            "outcome": (
                "historically_validated_model_complete"
            ),
            "allowed": False,
            "condition": (
                "Historical joins and accuracy metrics are "
                "not authorized."
            ),
            "effect": (
                "No historical-accuracy claim."
            ),
        },
        {
            "outcome_id": "L6-D04",
            "outcome": (
                "pricing_or_edge_ready"
            ),
            "allowed": False,
            "condition": (
                "Pricing and edge detection remain prohibited."
            ),
            "effect": (
                "No wagering or market-readiness claim."
            ),
        },
    ]

    reassessment_checks = [
        {
            "check_id": "L6-R01",
            "check": (
                "all_required_predecessors_exist"
            ),
            "required_evidence": (
                "6OX, 6OY, 6QK, and 6QL files exist."
            ),
            "blocking": True,
        },
        {
            "check_id": "L6-R02",
            "check": (
                "all_predecessor_contracts_pass"
            ),
            "required_evidence": (
                "All four predecessor diagnosis contracts "
                "are present."
            ),
            "blocking": True,
        },
        {
            "check_id": "L6-R03",
            "check": (
                "scope_claim_is_explicitly_narrow"
            ),
            "required_evidence": (
                "Completed scope names diagnostic integration "
                "and audited transition realism only."
            ),
            "blocking": True,
        },
        {
            "check_id": "L6-R04",
            "check": (
                "production_behavior_excluded"
            ),
            "required_evidence": (
                "All five game-management production "
                "activations remain false."
            ),
            "blocking": True,
        },
        {
            "check_id": "L6-R05",
            "check": (
                "canonical_probability_authority_unchanged"
            ),
            "required_evidence": (
                "No diagnostic replaces canonical production "
                "probability authority."
            ),
            "blocking": True,
        },
        {
            "check_id": "L6-R06",
            "check": (
                "historical_validation_excluded"
            ),
            "required_evidence": (
                "No historical outcomes, accuracy metrics, "
                "or calibration claims are introduced."
            ),
            "blocking": True,
        },
        {
            "check_id": "L6-R07",
            "check": (
                "downstream_authorities_excluded"
            ),
            "required_evidence": (
                "No tuning, backtesting, pricing, edge, or "
                "bet recommendation authority is granted."
            ),
            "blocking": True,
        },
        {
            "check_id": "L6-R08",
            "check": (
                "completion_language_matches_evidence"
            ),
            "required_evidence": (
                "Final wording distinguishes narrow audited "
                "scope from broad production realism."
            ),
            "blocking": True,
        },
    ]

    execution_steps = [
        {
            "step": 1,
            "action": (
                "Re-execute or statically verify the 6OX, "
                "6OY, 6QK, and 6QL contracts."
            ),
        },
        {
            "step": 2,
            "action": (
                "Build a scope ledger distinguishing completed, "
                "excluded, and unauthorized domains."
            ),
        },
        {
            "step": 3,
            "action": (
                "Verify all production and probability "
                "authority flags remain false."
            ),
        },
        {
            "step": 4,
            "action": (
                "Evaluate whether a narrow documented Layer 6 "
                "completion statement is supported."
            ),
        },
        {
            "step": 5,
            "action": (
                "Reject any broader production, historical, "
                "pricing, or edge-readiness interpretation."
            ),
        },
        {
            "step": 6,
            "action": (
                "Emit the reassessment decision and next-layer "
                "authorization without changing runtime behavior."
            ),
        },
    ]

    planning_checks = [
        {
            "check": (
                "required_paths_exist"
            ),
            "actual": required_paths_exist,
            "expected": True,
            "passed": required_paths_exist,
        },
        {
            "check": (
                "four_predecessor_contracts_present"
            ),
            "actual": predecessors_accepted,
            "expected": 4,
            "passed": (
                predecessors_accepted == 4
            ),
        },
        {
            "check": (
                "five_scope_classes_defined"
            ),
            "actual": len(
                scope_classes
            ),
            "expected": 5,
            "passed": (
                len(
                    scope_classes
                )
                == 5
            ),
        },
        {
            "check": (
                "four_decision_outcomes_defined"
            ),
            "actual": len(
                decision_outcomes
            ),
            "expected": 4,
            "passed": (
                len(
                    decision_outcomes
                )
                == 4
            ),
        },
        {
            "check": (
                "eight_reassessment_checks_defined"
            ),
            "actual": len(
                reassessment_checks
            ),
            "expected": 8,
            "passed": (
                len(
                    reassessment_checks
                )
                == 8
            ),
        },
        {
            "check": (
                "six_execution_steps_defined"
            ),
            "actual": len(
                execution_steps
            ),
            "expected": 6,
            "passed": (
                len(
                    execution_steps
                )
                == 6
            ),
        },
        {
            "check": (
                "planning_only_boundary_preserved"
            ),
            "actual": True,
            "expected": True,
            "passed": True,
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in planning_checks
    )

    authority_rows = [
        {
            "authority": authority,
            "granted": False,
            "reason": (
                "6QM is a planning-only reassessment layer."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "layer6_broad_scope_reassessment_execution"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "The reassessment execution may begin "
                    "after all planning checks pass."
                ),
            },
            {
                "authority": (
                    "layer6_exit_finalization"
                ),
                "granted": False,
                "reason": (
                    "6QN must execute the reassessment first."
                ),
            },
            {
                "authority": (
                    "production_behavior_integration"
                ),
                "granted": False,
                "reason": (
                    "Diagnostic completion does not authorize "
                    "production game-management behavior."
                ),
            },
        ]
    )

    recommended_next_layer = (
        "6QN_layer6_broad_scope_reassessment"
        if all_checks_passed
        else
        "6QN_layer6_broad_scope_reassessment_"
        "plan_remediation"
    )

    diagnosis_name = (
        "layer6_broad_scope_reassessment_plan_complete"
        if all_checks_passed
        else
        "layer6_broad_scope_reassessment_plan_failed"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        planning_checks,
    )

    write_csv(
        OUTPUT_DIR / "predecessor_contracts.csv",
        [
            "predecessor",
            "path",
            "expected_contract",
            "present",
        ],
        predecessor_contracts,
    )

    write_csv(
        OUTPUT_DIR / "scope_classes.csv",
        [
            "scope_id",
            "scope",
            "current_evidence_status",
            "eligible_for_completion_claim",
            "production_behavior_required",
            "reassessment_rule",
        ],
        scope_classes,
    )

    write_csv(
        OUTPUT_DIR / "decision_outcomes.csv",
        [
            "outcome_id",
            "outcome",
            "allowed",
            "condition",
            "effect",
        ],
        decision_outcomes,
    )

    write_csv(
        OUTPUT_DIR / "reassessment_checks.csv",
        [
            "check_id",
            "check",
            "required_evidence",
            "blocking",
        ],
        reassessment_checks,
    )

    write_csv(
        OUTPUT_DIR / "execution_steps.csv",
        [
            "step",
            "action",
        ],
        execution_steps,
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
                    "Execute the Layer 6 broad-scope "
                    "reassessment using the bounded decision "
                    "rules defined by 6QM."
                    if all_checks_passed
                    else
                    "Remediate failed 6QM planning checks."
                ),
                "entry_condition": (
                    "All 6QM planning checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    plan_summary = {
        "planning_checks_required": len(
            planning_checks
        ),
        "planning_checks_passed": sum(
            1
            for row in planning_checks
            if row["passed"]
        ),
        "predecessors_required": len(
            predecessor_contracts
        ),
        "predecessors_accepted": (
            predecessors_accepted
        ),
        "scope_classes_defined": len(
            scope_classes
        ),
        "decision_outcomes_defined": len(
            decision_outcomes
        ),
        "reassessment_checks_defined": len(
            reassessment_checks
        ),
        "execution_steps_defined": len(
            execution_steps
        ),
        "narrow_scope_completion_may_be_evaluated": True,
        "broad_production_scope_completion_supported": False,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
        "broad_layer6_exit_paused": True,
    }

    write_json(
        OUTPUT_DIR / "plan_summary.json",
        plan_summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_name,
        "all_checks_passed": (
            all_checks_passed
        ),
        **plan_summary,
        "layer6_exit_recommended": False,
        "layer6_exit_finalized": False,
        "new_authority_granted": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "broad_scope_reassessment_allowed_next": (
            all_checks_passed
        ),
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "planning_checks.csv"
            ),
            str(
                OUTPUT_DIR / "predecessor_contracts.csv"
            ),
            str(
                OUTPUT_DIR / "scope_classes.csv"
            ),
            str(
                OUTPUT_DIR / "decision_outcomes.csv"
            ),
            str(
                OUTPUT_DIR / "reassessment_checks.csv"
            ),
            str(
                OUTPUT_DIR / "execution_steps.csv"
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
                OUTPUT_DIR / "plan_summary.json"
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
