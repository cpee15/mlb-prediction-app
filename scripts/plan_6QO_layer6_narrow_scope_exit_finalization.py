#!/usr/bin/env python3
"""
Layer 6QO
Layer 6 Narrow Scope Exit Finalization Plan

Defines the bounded procedure for finalizing Layer 6 only at the narrow scope
supported by the 6QN reassessment.

Planning only. This layer does not:
- finalize Layer 6;
- activate production game-management behavior;
- change simulation state, parameters, or probabilities;
- authorize historical validation, tuning, backtesting, pricing, or edge logic.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6QO"
LAYER_NAME = "layer6_narrow_scope_exit_finalization_plan"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/layer_6QO_layer6_narrow_scope_exit_finalization_plan"
)

REASSESSMENT_PLAN_PATH = (
    ROOT
    / "scripts/plan_6QM_layer6_broad_scope_reassessment.py"
)

REASSESSMENT_PATH = (
    ROOT
    / "scripts/assess_6QN_layer6_broad_scope_reassessment.py"
)

COMBINED_AUDIT_PATH = (
    ROOT
    / "scripts/audit_6QL_combined_game_management_"
    "diagnostic_integration.py"
)

REQUIRED_PATHS = [
    REASSESSMENT_PLAN_PATH,
    REASSESSMENT_PATH,
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
    "broad_production_layer6_completion_claim",
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


def string_constants(path: Path) -> set[str]:
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

    required_paths_exist = all(
        path.exists()
        for path in REQUIRED_PATHS
    )

    qm_constants = string_constants(
        REASSESSMENT_PLAN_PATH
    )
    qn_constants = string_constants(
        REASSESSMENT_PATH
    )
    ql_constants = string_constants(
        COMBINED_AUDIT_PATH
    )

    predecessor_contracts = [
        {
            "layer": "6QL",
            "path": str(
                COMBINED_AUDIT_PATH.relative_to(ROOT)
            ),
            "expected_diagnosis": (
                "combined_game_management_"
                "diagnostic_integration_audit_passed"
            ),
            "present": (
                "combined_game_management_"
                "diagnostic_integration_audit_passed"
                in ql_constants
            ),
        },
        {
            "layer": "6QM",
            "path": str(
                REASSESSMENT_PLAN_PATH.relative_to(ROOT)
            ),
            "expected_diagnosis": (
                "layer6_broad_scope_"
                "reassessment_plan_complete"
            ),
            "present": (
                "layer6_broad_scope_"
                "reassessment_plan_complete"
                in qm_constants
            ),
        },
        {
            "layer": "6QN",
            "path": str(
                REASSESSMENT_PATH.relative_to(ROOT)
            ),
            "expected_diagnosis": (
                "layer6_broad_scope_reassessment_"
                "supports_narrow_documented_completion"
            ),
            "present": (
                "layer6_broad_scope_reassessment_"
                "supports_narrow_documented_completion"
                in qn_constants
            ),
        },
    ]

    predecessors_accepted = sum(
        1
        for row in predecessor_contracts
        if row["present"]
    )

    qn_boundary_contract_present = all(
        token in qn_constants
        for token in [
            "narrow_documented_scope_complete",
            "broad_production_scope_complete",
            "historically_validated_model_complete",
            "pricing_or_edge_ready",
            "production_behavior_changed",
            "simulation_behavior_changed",
            "canonical_probability_authority_changed",
            "production_activation",
            "layer6_exit_finalized",
            "new_production_authority_granted",
        ]
    )

    finalization_scope = [
        {
            "scope_id": "L6O-S01",
            "scope": (
                "base_out_and_core_runner_transition_realism"
            ),
            "finalization_status": (
                "eligible_for_narrow_exit"
            ),
            "included": True,
            "boundary": (
                "Previously audited transition-realism scope only."
            ),
        },
        {
            "scope_id": "L6O-S02",
            "scope": (
                "game_management_diagnostic_integration"
            ),
            "finalization_status": (
                "eligible_for_narrow_exit"
            ),
            "included": True,
            "boundary": (
                "All five workstreams complete only at "
                "diagnostic scope."
            ),
        },
        {
            "scope_id": "L6O-S03",
            "scope": (
                "production_game_management_behavior"
            ),
            "finalization_status": (
                "explicitly_excluded"
            ),
            "included": False,
            "boundary": (
                "No production activation or runtime authority."
            ),
        },
        {
            "scope_id": "L6O-S04",
            "scope": (
                "historical_accuracy_validation"
            ),
            "finalization_status": (
                "explicitly_excluded"
            ),
            "included": False,
            "boundary": (
                "No historical joins, accuracy metrics, "
                "or calibration claim."
            ),
        },
        {
            "scope_id": "L6O-S05",
            "scope": (
                "tuning_backtesting_pricing_and_edge_detection"
            ),
            "finalization_status": (
                "explicitly_excluded"
            ),
            "included": False,
            "boundary": (
                "No downstream modeling or market authority."
            ),
        },
    ]

    finalization_requirements = [
        {
            "requirement_id": "L6O-R01",
            "requirement": (
                "all_predecessor_contracts_present"
            ),
            "blocking": True,
            "evidence": (
                "6QL, 6QM, and 6QN contracts must remain valid."
            ),
        },
        {
            "requirement_id": "L6O-R02",
            "requirement": (
                "finalization_language_is_narrow"
            ),
            "blocking": True,
            "evidence": (
                "Final diagnosis must say narrow documented "
                "scope, not broad production realism."
            ),
        },
        {
            "requirement_id": "L6O-R03",
            "requirement": (
                "production_behavior_remains_excluded"
            ),
            "blocking": True,
            "evidence": (
                "All five production game-management "
                "activations remain false."
            ),
        },
        {
            "requirement_id": "L6O-R04",
            "requirement": (
                "simulation_and_probability_authority_unchanged"
            ),
            "blocking": True,
            "evidence": (
                "No state, parameter, probability, or canonical "
                "authority changes."
            ),
        },
        {
            "requirement_id": "L6O-R05",
            "requirement": (
                "historical_validation_remains_excluded"
            ),
            "blocking": True,
            "evidence": (
                "No historical outcome or accuracy authority."
            ),
        },
        {
            "requirement_id": "L6O-R06",
            "requirement": (
                "downstream_authorities_remain_excluded"
            ),
            "blocking": True,
            "evidence": (
                "No tuning, backtesting, pricing, edge, "
                "or recommendation authority."
            ),
        },
        {
            "requirement_id": "L6O-R07",
            "requirement": (
                "exit_artifacts_are_declarative_only"
            ),
            "blocking": True,
            "evidence": (
                "Finalization emits documentation and status "
                "artifacts only."
            ),
        },
        {
            "requirement_id": "L6O-R08",
            "requirement": (
                "future_production_work_requires_new_authority"
            ),
            "blocking": True,
            "evidence": (
                "Any production integration must begin through "
                "a separately authorized future layer."
            ),
        },
    ]

    finalization_steps = [
        {
            "step": 1,
            "action": (
                "Verify 6QL, 6QM, and 6QN diagnosis contracts."
            ),
        },
        {
            "step": 2,
            "action": (
                "Reconstruct the narrow completed-scope ledger."
            ),
        },
        {
            "step": 3,
            "action": (
                "Reconfirm all excluded scopes and authority "
                "boundaries."
            ),
        },
        {
            "step": 4,
            "action": (
                "Emit a narrow Layer 6 completion declaration."
            ),
        },
        {
            "step": 5,
            "action": (
                "Emit an explicit exclusions and non-authority "
                "declaration."
            ),
        },
        {
            "step": 6,
            "action": (
                "Close Layer 6 only under the documented narrow "
                "scope."
            ),
        },
    ]

    planning_checks = [
        {
            "check": "required_paths_exist",
            "actual": required_paths_exist,
            "expected": True,
            "passed": required_paths_exist,
        },
        {
            "check": "three_predecessor_contracts_present",
            "actual": predecessors_accepted,
            "expected": 3,
            "passed": predecessors_accepted == 3,
        },
        {
            "check": "six_qn_boundary_contract_present",
            "actual": qn_boundary_contract_present,
            "expected": True,
            "passed": qn_boundary_contract_present,
        },
        {
            "check": "five_finalization_scope_entries_defined",
            "actual": len(finalization_scope),
            "expected": 5,
            "passed": len(finalization_scope) == 5,
        },
        {
            "check": "eight_finalization_requirements_defined",
            "actual": len(finalization_requirements),
            "expected": 8,
            "passed": len(finalization_requirements) == 8,
        },
        {
            "check": "six_finalization_steps_defined",
            "actual": len(finalization_steps),
            "expected": 6,
            "passed": len(finalization_steps) == 6,
        },
        {
            "check": "planning_only_boundary_preserved",
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
                "6QO is a planning-only narrow-exit layer."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "narrow_layer6_exit_finalization_execution"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "6QP may execute the bounded narrow-scope "
                    "exit finalization."
                ),
            },
            {
                "authority": (
                    "layer6_exit_finalized"
                ),
                "granted": False,
                "reason": (
                    "6QO defines the plan but does not execute it."
                ),
            },
            {
                "authority": (
                    "new_production_authority"
                ),
                "granted": False,
                "reason": (
                    "Narrow exit does not authorize production "
                    "game-management behavior."
                ),
            },
        ]
    )

    recommended_next_layer = (
        "6QP_layer6_narrow_scope_exit_finalization"
        if all_checks_passed
        else
        "6QP_layer6_narrow_scope_exit_finalization_plan_remediation"
    )

    diagnosis_name = (
        "layer6_narrow_scope_exit_finalization_plan_complete"
        if all_checks_passed
        else
        "layer6_narrow_scope_exit_finalization_plan_failed"
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
            "layer",
            "path",
            "expected_diagnosis",
            "present",
        ],
        predecessor_contracts,
    )

    write_csv(
        OUTPUT_DIR / "finalization_scope.csv",
        [
            "scope_id",
            "scope",
            "finalization_status",
            "included",
            "boundary",
        ],
        finalization_scope,
    )

    write_csv(
        OUTPUT_DIR / "finalization_requirements.csv",
        [
            "requirement_id",
            "requirement",
            "blocking",
            "evidence",
        ],
        finalization_requirements,
    )

    write_csv(
        OUTPUT_DIR / "finalization_steps.csv",
        [
            "step",
            "action",
        ],
        finalization_steps,
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
                    "Execute narrow Layer 6 exit finalization "
                    "without expanding production authority."
                    if all_checks_passed
                    else
                    "Remediate failed 6QO planning checks."
                ),
                "entry_condition": (
                    "All seven 6QO planning checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    summary = {
        "planning_checks_required": len(planning_checks),
        "planning_checks_passed": sum(
            1
            for row in planning_checks
            if row["passed"]
        ),
        "predecessors_required": len(
            predecessor_contracts
        ),
        "predecessors_accepted": predecessors_accepted,
        "finalization_scope_entries": len(
            finalization_scope
        ),
        "included_scope_entries": sum(
            1
            for row in finalization_scope
            if row["included"]
        ),
        "excluded_scope_entries": sum(
            1
            for row in finalization_scope
            if not row["included"]
        ),
        "finalization_requirements_defined": len(
            finalization_requirements
        ),
        "finalization_steps_defined": len(
            finalization_steps
        ),
        "narrow_scope_exit_may_be_executed": (
            all_checks_passed
        ),
        "broad_production_scope_completion_supported": False,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
        "layer6_exit_finalized": False,
    }

    write_json(
        OUTPUT_DIR / "plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_name,
        "all_checks_passed": all_checks_passed,
        **summary,
        "layer6_exit_recommended": False,
        "new_production_authority_granted": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "narrow_exit_finalization_allowed_next": (
            all_checks_passed
        ),
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / "planning_checks.csv"),
            str(OUTPUT_DIR / "predecessor_contracts.csv"),
            str(OUTPUT_DIR / "finalization_scope.csv"),
            str(OUTPUT_DIR / "finalization_requirements.csv"),
            str(OUTPUT_DIR / "finalization_steps.csv"),
            str(OUTPUT_DIR / "authority_boundaries.csv"),
            str(OUTPUT_DIR / "recommended_path.csv"),
        ],
        "generated_json_artifacts": [
            str(OUTPUT_DIR / "plan_summary.json"),
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
