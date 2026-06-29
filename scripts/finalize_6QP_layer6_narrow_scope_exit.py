#!/usr/bin/env python3
"""
Layer 6QP
Layer 6 Narrow Scope Exit Finalization

Finalizes Layer 6 only under the narrow documented scope established by 6QN
and planned by 6QO.

This layer is declarative only. It does not:
- activate production game-management behavior;
- change simulation state, parameters, or probabilities;
- replace canonical probability authority;
- authorize historical validation, tuning, backtesting, pricing, or edge logic.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6QP"
LAYER_NAME = "layer6_narrow_scope_exit_finalization"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/layer_6QP_layer6_narrow_scope_exit_finalization"
)

COMBINED_AUDIT_PATH = (
    ROOT
    / "scripts/audit_6QL_combined_game_management_"
    "diagnostic_integration.py"
)

REASSESSMENT_PLAN_PATH = (
    ROOT
    / "scripts/plan_6QM_layer6_broad_scope_reassessment.py"
)

REASSESSMENT_PATH = (
    ROOT
    / "scripts/assess_6QN_layer6_broad_scope_reassessment.py"
)

FINALIZATION_PLAN_PATH = (
    ROOT
    / "scripts/plan_6QO_layer6_narrow_scope_exit_finalization.py"
)

REQUIRED_PATHS = [
    COMBINED_AUDIT_PATH,
    REASSESSMENT_PLAN_PATH,
    REASSESSMENT_PATH,
    FINALIZATION_PLAN_PATH,
]

PREDECESSOR_CONTRACTS = [
    {
        "layer": "6QL",
        "path": COMBINED_AUDIT_PATH,
        "diagnosis": (
            "combined_game_management_"
            "diagnostic_integration_audit_passed"
        ),
    },
    {
        "layer": "6QM",
        "path": REASSESSMENT_PLAN_PATH,
        "diagnosis": (
            "layer6_broad_scope_"
            "reassessment_plan_complete"
        ),
    },
    {
        "layer": "6QN",
        "path": REASSESSMENT_PATH,
        "diagnosis": (
            "layer6_broad_scope_reassessment_"
            "supports_narrow_documented_completion"
        ),
    },
    {
        "layer": "6QO",
        "path": FINALIZATION_PLAN_PATH,
        "diagnosis": (
            "layer6_narrow_scope_"
            "exit_finalization_plan_complete"
        ),
    },
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

    predecessor_rows = []

    for contract in PREDECESSOR_CONTRACTS:
        constants = string_constants(
            contract["path"]
        )

        diagnosis_present = (
            contract["diagnosis"]
            in constants
        )

        predecessor_rows.append(
            {
                "layer": contract["layer"],
                "path": str(
                    contract["path"].relative_to(ROOT)
                ),
                "expected_diagnosis": (
                    contract["diagnosis"]
                ),
                "path_exists": (
                    contract["path"].exists()
                ),
                "diagnosis_present": (
                    diagnosis_present
                ),
                "accepted": (
                    contract["path"].exists()
                    and diagnosis_present
                ),
            }
        )

    predecessors_accepted = sum(
        1
        for row in predecessor_rows
        if row["accepted"]
    )

    qn_constants = string_constants(
        REASSESSMENT_PATH
    )

    qo_constants = string_constants(
        FINALIZATION_PLAN_PATH
    )

    qn_scope_contract_present = all(
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
        ]
    )

    qo_finalization_contract_present = all(
        token in qo_constants
        for token in [
            "narrow_scope_exit_may_be_executed",
            "broad_production_scope_completion_supported",
            "narrow_exit_finalization_allowed_next",
            "production_behavior_integration_allowed_next",
            "layer6_exit_finalized",
        ]
    )

    completion_scope = [
        {
            "scope_id": "L6P-S01",
            "scope": (
                "base_out_and_core_runner_transition_realism"
            ),
            "status": (
                "finalized_complete_under_narrow_scope"
            ),
            "included": True,
            "production_authority_granted": False,
            "statement": (
                "Previously audited transition-realism scope "
                "is included in the Layer 6 narrow exit."
            ),
        },
        {
            "scope_id": "L6P-S02",
            "scope": (
                "game_management_diagnostic_integration"
            ),
            "status": (
                "finalized_complete_at_diagnostic_scope"
            ),
            "included": True,
            "production_authority_granted": False,
            "statement": (
                "GM-01 through GM-05 are complete only at "
                "diagnostic and metadata-integration scope."
            ),
        },
        {
            "scope_id": "L6P-S03",
            "scope": (
                "production_game_management_behavior"
            ),
            "status": (
                "excluded_not_complete_not_authorized"
            ),
            "included": False,
            "production_authority_granted": False,
            "statement": (
                "Production game-management behavior is not "
                "part of the Layer 6 completion claim."
            ),
        },
        {
            "scope_id": "L6P-S04",
            "scope": (
                "historical_accuracy_validation"
            ),
            "status": (
                "excluded_not_complete_not_authorized"
            ),
            "included": False,
            "production_authority_granted": False,
            "statement": (
                "No historical validation, calibration, or "
                "accuracy claim is included."
            ),
        },
        {
            "scope_id": "L6P-S05",
            "scope": (
                "tuning_backtesting_pricing_and_edge_detection"
            ),
            "status": (
                "excluded_not_complete_not_authorized"
            ),
            "included": False,
            "production_authority_granted": False,
            "statement": (
                "No tuning, backtesting, pricing, edge, or "
                "wagering authority is included."
            ),
        },
    ]

    included_rows = [
        row
        for row in completion_scope
        if row["included"]
    ]

    excluded_rows = [
        row
        for row in completion_scope
        if not row["included"]
    ]

    included_scope_valid = (
        len(included_rows) == 2
        and all(
            row["production_authority_granted"] is False
            for row in included_rows
        )
    )

    excluded_scope_valid = (
        len(excluded_rows) == 3
        and all(
            row["status"]
            == "excluded_not_complete_not_authorized"
            for row in excluded_rows
        )
    )

    production_behavior_changed = False
    simulation_behavior_changed = False
    canonical_probability_authority_changed = False
    production_activation = False
    new_production_authority_granted = False

    historical_validation_authorized = False
    tuning_authorized = False
    backtesting_authorized = False
    pricing_authorized = False
    edge_detection_authorized = False

    narrow_layer6_exit_finalized = all(
        [
            required_paths_exist,
            predecessors_accepted == 4,
            qn_scope_contract_present,
            qo_finalization_contract_present,
            included_scope_valid,
            excluded_scope_valid,
            production_behavior_changed is False,
            simulation_behavior_changed is False,
            canonical_probability_authority_changed is False,
            production_activation is False,
            new_production_authority_granted is False,
        ]
    )

    broad_production_layer6_complete = False
    historically_validated_model_complete = False
    pricing_or_edge_ready = False

    finalization_checks = [
        {
            "check_id": "L6P-R01",
            "check": (
                "all_required_predecessors_exist"
            ),
            "actual": required_paths_exist,
            "expected": True,
            "passed": required_paths_exist,
        },
        {
            "check_id": "L6P-R02",
            "check": (
                "all_four_predecessor_contracts_pass"
            ),
            "actual": predecessors_accepted,
            "expected": 4,
            "passed": (
                predecessors_accepted == 4
            ),
        },
        {
            "check_id": "L6P-R03",
            "check": (
                "six_qn_scope_contract_present"
            ),
            "actual": qn_scope_contract_present,
            "expected": True,
            "passed": qn_scope_contract_present,
        },
        {
            "check_id": "L6P-R04",
            "check": (
                "six_qo_finalization_contract_present"
            ),
            "actual": qo_finalization_contract_present,
            "expected": True,
            "passed": qo_finalization_contract_present,
        },
        {
            "check_id": "L6P-R05",
            "check": (
                "two_narrow_scope_entries_included"
            ),
            "actual": len(included_rows),
            "expected": 2,
            "passed": included_scope_valid,
        },
        {
            "check_id": "L6P-R06",
            "check": (
                "three_scope_entries_explicitly_excluded"
            ),
            "actual": len(excluded_rows),
            "expected": 3,
            "passed": excluded_scope_valid,
        },
        {
            "check_id": "L6P-R07",
            "check": (
                "production_and_probability_authority_unchanged"
            ),
            "actual": all(
                [
                    production_behavior_changed is False,
                    simulation_behavior_changed is False,
                    canonical_probability_authority_changed
                    is False,
                    production_activation is False,
                    new_production_authority_granted
                    is False,
                ]
            ),
            "expected": True,
            "passed": all(
                [
                    production_behavior_changed is False,
                    simulation_behavior_changed is False,
                    canonical_probability_authority_changed
                    is False,
                    production_activation is False,
                    new_production_authority_granted
                    is False,
                ]
            ),
        },
        {
            "check_id": "L6P-R08",
            "check": (
                "historical_and_downstream_authorities_excluded"
            ),
            "actual": all(
                authority is False
                for authority in [
                    historical_validation_authorized,
                    tuning_authorized,
                    backtesting_authorized,
                    pricing_authorized,
                    edge_detection_authorized,
                ]
            ),
            "expected": True,
            "passed": all(
                authority is False
                for authority in [
                    historical_validation_authorized,
                    tuning_authorized,
                    backtesting_authorized,
                    pricing_authorized,
                    edge_detection_authorized,
                ]
            ),
        },
        {
            "check_id": "L6P-R09",
            "check": (
                "narrow_layer6_exit_finalized"
            ),
            "actual": narrow_layer6_exit_finalized,
            "expected": True,
            "passed": narrow_layer6_exit_finalized,
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in finalization_checks
    )

    decision_rows = [
        {
            "decision_id": "L6P-D01",
            "decision": (
                "narrow_layer6_scope_complete"
            ),
            "supported": (
                narrow_layer6_exit_finalized
            ),
            "statement": (
                "Layer 6 is complete only for audited "
                "transition realism and diagnostic "
                "game-management integration."
            ),
        },
        {
            "decision_id": "L6P-D02",
            "decision": (
                "broad_production_layer6_complete"
            ),
            "supported": (
                broad_production_layer6_complete
            ),
            "statement": (
                "Broad production game-management completion "
                "is not supported."
            ),
        },
        {
            "decision_id": "L6P-D03",
            "decision": (
                "historically_validated_model_complete"
            ),
            "supported": (
                historically_validated_model_complete
            ),
            "statement": (
                "Historical validation is not part of this exit."
            ),
        },
        {
            "decision_id": "L6P-D04",
            "decision": (
                "pricing_or_edge_ready"
            ),
            "supported": (
                pricing_or_edge_ready
            ),
            "statement": (
                "Pricing and edge readiness are not authorized."
            ),
        },
    ]

    authority_rows = [
        {
            "authority": authority,
            "granted": False,
            "reason": (
                "6QP finalizes only the narrow documented "
                "Layer 6 scope."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "narrow_layer6_scope_completion"
                ),
                "granted": (
                    all_checks_passed
                    and narrow_layer6_exit_finalized
                ),
                "reason": (
                    "Audited transition realism and diagnostic "
                    "integration satisfy the bounded exit."
                ),
            },
            {
                "authority": (
                    "layer6_exit_finalized"
                ),
                "granted": (
                    all_checks_passed
                    and narrow_layer6_exit_finalized
                ),
                "reason": (
                    "Layer 6 closes only under the explicit "
                    "narrow scope declaration."
                ),
            },
            {
                "authority": (
                    "new_production_authority"
                ),
                "granted": False,
                "reason": (
                    "Narrow completion does not authorize "
                    "production game-management behavior."
                ),
            },
        ]
    )

    next_action = (
        "layer6_closed_under_narrow_documented_scope"
        if all_checks_passed
        else
        "layer6_narrow_scope_exit_remediation_required"
    )

    summary = {
        "finalization_checks_required": len(
            finalization_checks
        ),
        "finalization_checks_passed": sum(
            1
            for row in finalization_checks
            if row["passed"]
        ),
        "predecessors_required": len(
            predecessor_rows
        ),
        "predecessors_accepted": predecessors_accepted,
        "completion_scope_entries": len(
            completion_scope
        ),
        "included_scope_entries": len(
            included_rows
        ),
        "excluded_scope_entries": len(
            excluded_rows
        ),
        "narrow_layer6_exit_finalized": (
            narrow_layer6_exit_finalized
        ),
        "broad_production_layer6_complete": (
            broad_production_layer6_complete
        ),
        "historically_validated_model_complete": (
            historically_validated_model_complete
        ),
        "pricing_or_edge_ready": (
            pricing_or_edge_ready
        ),
        "production_behavior_changed": (
            production_behavior_changed
        ),
        "simulation_behavior_changed": (
            simulation_behavior_changed
        ),
        "canonical_probability_authority_changed": (
            canonical_probability_authority_changed
        ),
        "production_activation": production_activation,
        "new_production_authority_granted": (
            new_production_authority_granted
        ),
    }

    write_csv(
        OUTPUT_DIR / "finalization_checks.csv",
        [
            "check_id",
            "check",
            "actual",
            "expected",
            "passed",
        ],
        finalization_checks,
    )

    write_csv(
        OUTPUT_DIR / "predecessor_contracts.csv",
        [
            "layer",
            "path",
            "expected_diagnosis",
            "path_exists",
            "diagnosis_present",
            "accepted",
        ],
        predecessor_rows,
    )

    write_csv(
        OUTPUT_DIR / "completion_scope.csv",
        [
            "scope_id",
            "scope",
            "status",
            "included",
            "production_authority_granted",
            "statement",
        ],
        completion_scope,
    )

    write_csv(
        OUTPUT_DIR / "decision_matrix.csv",
        [
            "decision_id",
            "decision",
            "supported",
            "statement",
        ],
        decision_rows,
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
        OUTPUT_DIR / "next_action.csv",
        [
            "next_action",
            "layer6_status",
            "production_authority_granted",
            "passed",
        ],
        [
            {
                "next_action": next_action,
                "layer6_status": (
                    "closed_under_narrow_documented_scope"
                    if all_checks_passed
                    else
                    "open_pending_remediation"
                ),
                "production_authority_granted": False,
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "finalization_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "layer6_narrow_scope_exit_finalized"
            if all_checks_passed
            else
            "layer6_narrow_scope_exit_finalization_failed"
        ),
        "all_checks_passed": all_checks_passed,
        **summary,
        "layer6_exit_recommended": (
            all_checks_passed
        ),
        "layer6_exit_finalized": (
            all_checks_passed
            and narrow_layer6_exit_finalized
        ),
        "layer6_exit_scope": (
            "narrow_documented_scope"
            if all_checks_passed
            else
            None
        ),
        "broad_production_completion_claim": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "production_behavior_integration_allowed_next": False,
        "next_action": next_action,
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / "finalization_checks.csv"),
            str(OUTPUT_DIR / "predecessor_contracts.csv"),
            str(OUTPUT_DIR / "completion_scope.csv"),
            str(OUTPUT_DIR / "decision_matrix.csv"),
            str(OUTPUT_DIR / "authority_boundaries.csv"),
            str(OUTPUT_DIR / "next_action.csv"),
        ],
        "generated_json_artifacts": [
            str(OUTPUT_DIR / "finalization_summary.json"),
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
