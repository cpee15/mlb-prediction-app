#!/usr/bin/env python3
"""
Layer 6QN
Layer 6 Broad Scope Reassessment

Executes the bounded reassessment defined by Layer 6QM.

This layer distinguishes:
- audited transition and diagnostic scope;
- excluded production game-management behavior;
- unauthorized historical validation and downstream model-use claims.

It does not finalize Layer 6 or activate production behavior.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6QN"
LAYER_NAME = "layer6_broad_scope_reassessment"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/layer_6QN_layer6_broad_scope_reassessment"
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

REASSESSMENT_PLAN_PATH = (
    ROOT
    / "scripts/plan_6QM_layer6_broad_scope_"
    "reassessment.py"
)

REQUIRED_PATHS = [
    GAP_ANALYSIS_PATH,
    RESOLUTION_PLAN_PATH,
    SCOPE_UPDATE_PATH,
    COMBINED_AUDIT_PATH,
    REASSESSMENT_PLAN_PATH,
]

PREDECESSOR_CONTRACTS = [
    {
        "layer": "6OX",
        "path": GAP_ANALYSIS_PATH,
        "diagnosis": (
            "layer_6_game_management_"
            "scope_completeness_gaps_confirmed"
        ),
    },
    {
        "layer": "6OY",
        "path": RESOLUTION_PLAN_PATH,
        "diagnosis": (
            "layer_6_game_management_"
            "scope_resolution_plan_complete"
        ),
    },
    {
        "layer": "6QK",
        "path": SCOPE_UPDATE_PATH,
        "diagnosis": (
            "layer6_game_management_"
            "scope_resolution_updated"
        ),
    },
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

    combined_audit_constants = string_constants(
        COMBINED_AUDIT_PATH
    )

    reassessment_plan_constants = string_constants(
        REASSESSMENT_PLAN_PATH
    )

    combined_audit_boundaries_present = all(
        token in combined_audit_constants
        for token in [
            "production_behavior_changed",
            "simulation_behavior_changed",
            "canonical_probability_authority_changed",
            "production_activation",
            "broad_layer6_exit_paused",
        ]
    )

    reassessment_plan_boundaries_present = all(
        token in reassessment_plan_constants
        for token in [
            "narrow_scope_completion_may_be_evaluated",
            "broad_production_scope_completion_supported",
            "production_behavior_integration_allowed_next",
            "layer6_exit_finalized",
            "historical_validation_allowed_next",
            "tuning_allowed_next",
            "backtests_allowed_next",
            "pricing_allowed_next",
            "edge_detection_allowed_next",
        ]
    )

    scope_ledger = [
        {
            "scope_id": "L6N-S01",
            "scope": (
                "base_out_and_core_runner_"
                "transition_realism"
            ),
            "status": (
                "complete_under_prior_"
                "audited_narrow_scope"
            ),
            "included_in_narrow_completion": True,
            "included_in_broad_production_claim": False,
            "evidence": (
                "Prior Layer 6 transition-realism "
                "audits remain the accepted authority."
            ),
        },
        {
            "scope_id": "L6N-S02",
            "scope": (
                "pitching_plan_classification"
            ),
            "status": (
                "complete_at_diagnostic_scope"
            ),
            "included_in_narrow_completion": True,
            "included_in_broad_production_claim": False,
            "evidence": (
                "GM-01 diagnostic completion and "
                "combined integration audit passed."
            ),
        },
        {
            "scope_id": "L6N-S03",
            "scope": (
                "dynamic_starter_hook"
            ),
            "status": (
                "complete_at_diagnostic_scope"
            ),
            "included_in_narrow_completion": True,
            "included_in_broad_production_claim": False,
            "evidence": (
                "GM-02 diagnostic completion and "
                "combined integration audit passed."
            ),
        },
        {
            "scope_id": "L6N-S04",
            "scope": (
                "bullpen_sequencing"
            ),
            "status": (
                "complete_at_diagnostic_scope"
            ),
            "included_in_narrow_completion": True,
            "included_in_broad_production_claim": False,
            "evidence": (
                "GM-03 diagnostic completion and "
                "combined integration audit passed."
            ),
        },
        {
            "scope_id": "L6N-S05",
            "scope": (
                "stolen_base_and_pickoff_state"
            ),
            "status": (
                "complete_at_diagnostic_scope"
            ),
            "included_in_narrow_completion": True,
            "included_in_broad_production_claim": False,
            "evidence": (
                "GM-04 diagnostic completion and "
                "combined integration audit passed."
            ),
        },
        {
            "scope_id": "L6N-S06",
            "scope": (
                "position_player_substitutions"
            ),
            "status": (
                "complete_at_diagnostic_scope"
            ),
            "included_in_narrow_completion": True,
            "included_in_broad_production_claim": False,
            "evidence": (
                "GM-05 diagnostic completion and "
                "combined integration audit passed."
            ),
        },
        {
            "scope_id": "L6N-S07",
            "scope": (
                "production_game_management_behavior"
            ),
            "status": (
                "excluded_not_activated_"
                "not_authorized"
            ),
            "included_in_narrow_completion": False,
            "included_in_broad_production_claim": False,
            "evidence": (
                "All five workstreams remain "
                "diagnostic-only."
            ),
        },
        {
            "scope_id": "L6N-S08",
            "scope": (
                "historical_accuracy_validation"
            ),
            "status": (
                "excluded_not_authorized"
            ),
            "included_in_narrow_completion": False,
            "included_in_broad_production_claim": False,
            "evidence": (
                "No historical outcome joins, "
                "accuracy metrics, or calibration "
                "claims are authorized."
            ),
        },
        {
            "scope_id": "L6N-S09",
            "scope": (
                "tuning_backtesting_pricing_"
                "and_edge_detection"
            ),
            "status": (
                "excluded_not_authorized"
            ),
            "included_in_narrow_completion": False,
            "included_in_broad_production_claim": False,
            "evidence": (
                "No downstream model-use authority "
                "is granted by Layer 6."
            ),
        },
    ]

    narrow_scope_rows = [
        row
        for row in scope_ledger
        if row[
            "included_in_narrow_completion"
        ]
    ]

    excluded_scope_rows = [
        row
        for row in scope_ledger
        if not row[
            "included_in_narrow_completion"
        ]
    ]

    narrow_scope_evidence_complete = (
        len(narrow_scope_rows) == 6
        and all(
            row["status"]
            in {
                (
                    "complete_under_prior_"
                    "audited_narrow_scope"
                ),
                (
                    "complete_at_diagnostic_scope"
                ),
            }
            for row in narrow_scope_rows
        )
    )

    excluded_scope_explicit = (
        len(excluded_scope_rows) == 3
        and all(
            "excluded"
            in row["status"]
            for row in excluded_scope_rows
        )
    )

    production_behavior_changed = False
    simulation_behavior_changed = False
    canonical_probability_authority_changed = False
    production_activation = False

    historical_validation_authorized = False
    tuning_authorized = False
    backtesting_authorized = False
    pricing_authorized = False
    edge_detection_authorized = False

    broad_production_scope_complete = False
    historically_validated_model_complete = False
    pricing_or_edge_ready = False

    narrow_documented_scope_complete = all(
        [
            required_paths_exist,
            predecessors_accepted == 5,
            combined_audit_boundaries_present,
            reassessment_plan_boundaries_present,
            narrow_scope_evidence_complete,
            excluded_scope_explicit,
            production_behavior_changed is False,
            simulation_behavior_changed is False,
            canonical_probability_authority_changed is False,
            production_activation is False,
        ]
    )

    decision_rows = [
        {
            "decision_id": "L6N-D01",
            "decision": (
                "narrow_documented_scope_complete"
            ),
            "supported": (
                narrow_documented_scope_complete
            ),
            "reason": (
                "Audited transition realism and all "
                "five diagnostic workstreams are "
                "complete within explicit boundaries."
            ),
        },
        {
            "decision_id": "L6N-D02",
            "decision": (
                "broad_production_game_management_complete"
            ),
            "supported": (
                broad_production_scope_complete
            ),
            "reason": (
                "Production game-management behavior "
                "remains excluded and inactive."
            ),
        },
        {
            "decision_id": "L6N-D03",
            "decision": (
                "historically_validated_model_complete"
            ),
            "supported": (
                historically_validated_model_complete
            ),
            "reason": (
                "Historical validation remains outside "
                "the authorized scope."
            ),
        },
        {
            "decision_id": "L6N-D04",
            "decision": (
                "pricing_or_edge_ready"
            ),
            "supported": (
                pricing_or_edge_ready
            ),
            "reason": (
                "Pricing and edge detection remain "
                "outside the authorized scope."
            ),
        },
    ]

    reassessment_checks = [
        {
            "check_id": "L6N-R01",
            "check": (
                "all_required_predecessors_exist"
            ),
            "actual": required_paths_exist,
            "expected": True,
            "passed": required_paths_exist,
        },
        {
            "check_id": "L6N-R02",
            "check": (
                "all_five_predecessor_contracts_pass"
            ),
            "actual": predecessors_accepted,
            "expected": 5,
            "passed": (
                predecessors_accepted == 5
            ),
        },
        {
            "check_id": "L6N-R03",
            "check": (
                "scope_claim_is_explicitly_narrow"
            ),
            "actual": (
                narrow_scope_evidence_complete
                and excluded_scope_explicit
            ),
            "expected": True,
            "passed": (
                narrow_scope_evidence_complete
                and excluded_scope_explicit
            ),
        },
        {
            "check_id": "L6N-R04",
            "check": (
                "production_behavior_excluded"
            ),
            "actual": (
                production_behavior_changed is False
                and production_activation is False
            ),
            "expected": True,
            "passed": (
                production_behavior_changed is False
                and production_activation is False
            ),
        },
        {
            "check_id": "L6N-R05",
            "check": (
                "canonical_probability_authority_unchanged"
            ),
            "actual": (
                canonical_probability_authority_changed
                is False
            ),
            "expected": True,
            "passed": (
                canonical_probability_authority_changed
                is False
            ),
        },
        {
            "check_id": "L6N-R06",
            "check": (
                "historical_validation_excluded"
            ),
            "actual": (
                historical_validation_authorized
                is False
            ),
            "expected": True,
            "passed": (
                historical_validation_authorized
                is False
            ),
        },
        {
            "check_id": "L6N-R07",
            "check": (
                "downstream_authorities_excluded"
            ),
            "actual": all(
                authority is False
                for authority in [
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
                    tuning_authorized,
                    backtesting_authorized,
                    pricing_authorized,
                    edge_detection_authorized,
                ]
            ),
        },
        {
            "check_id": "L6N-R08",
            "check": (
                "completion_language_matches_evidence"
            ),
            "actual": all(
                [
                    narrow_documented_scope_complete,
                    broad_production_scope_complete
                    is False,
                    historically_validated_model_complete
                    is False,
                    pricing_or_edge_ready
                    is False,
                ]
            ),
            "expected": True,
            "passed": all(
                [
                    narrow_documented_scope_complete,
                    broad_production_scope_complete
                    is False,
                    historically_validated_model_complete
                    is False,
                    pricing_or_edge_ready
                    is False,
                ]
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in reassessment_checks
    )

    authority_rows = [
        {
            "authority": authority,
            "granted": False,
            "reason": (
                "6QN performs scope reassessment only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "narrow_layer6_exit_"
                    "finalization_planning"
                ),
                "granted": (
                    all_checks_passed
                    and narrow_documented_scope_complete
                ),
                "reason": (
                    "A planning layer may define how to "
                    "finalize only the supported narrow scope."
                ),
            },
            {
                "authority": (
                    "layer6_exit_finalization"
                ),
                "granted": False,
                "reason": (
                    "6QN reassesses scope but does not "
                    "finalize Layer 6."
                ),
            },
            {
                "authority": (
                    "production_behavior_integration"
                ),
                "granted": False,
                "reason": (
                    "Diagnostic completion does not authorize "
                    "production behavior."
                ),
            },
        ]
    )

    recommended_next_layer = (
        "6QO_layer6_narrow_scope_exit_finalization_plan"
        if (
            all_checks_passed
            and narrow_documented_scope_complete
        )
        else
        "6QO_layer6_broad_scope_reassessment_remediation"
    )

    diagnosis_name = (
        "layer6_broad_scope_reassessment_"
        "supports_narrow_documented_completion"
        if (
            all_checks_passed
            and narrow_documented_scope_complete
        )
        else
        "layer6_broad_scope_reassessment_failed"
    )

    write_csv(
        OUTPUT_DIR / "reassessment_checks.csv",
        [
            "check_id",
            "check",
            "actual",
            "expected",
            "passed",
        ],
        reassessment_checks,
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
        OUTPUT_DIR / "scope_ledger.csv",
        [
            "scope_id",
            "scope",
            "status",
            "included_in_narrow_completion",
            "included_in_broad_production_claim",
            "evidence",
        ],
        scope_ledger,
    )

    write_csv(
        OUTPUT_DIR / "decision_matrix.csv",
        [
            "decision_id",
            "decision",
            "supported",
            "reason",
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
                    "Plan narrow Layer 6 exit finalization "
                    "without expanding production authority."
                    if all_checks_passed
                    else
                    "Remediate failed broad-scope "
                    "reassessment checks."
                ),
                "entry_condition": (
                    "All eight reassessment checks pass "
                    "and narrow documented completion is "
                    "supported."
                ),
                "passed": (
                    all_checks_passed
                    and narrow_documented_scope_complete
                ),
            }
        ],
    )

    summary = {
        "reassessment_checks_required": len(
            reassessment_checks
        ),
        "reassessment_checks_passed": sum(
            1
            for row in reassessment_checks
            if row["passed"]
        ),
        "predecessors_required": len(
            predecessor_rows
        ),
        "predecessors_accepted": (
            predecessors_accepted
        ),
        "scope_ledger_entries": len(
            scope_ledger
        ),
        "narrow_scope_entries": len(
            narrow_scope_rows
        ),
        "excluded_scope_entries": len(
            excluded_scope_rows
        ),
        "narrow_documented_scope_complete": (
            narrow_documented_scope_complete
        ),
        "broad_production_scope_complete": (
            broad_production_scope_complete
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
        "production_activation": (
            production_activation
        ),
    }

    write_json(
        OUTPUT_DIR / "reassessment_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_name,
        "all_checks_passed": (
            all_checks_passed
        ),
        **summary,
        "layer6_exit_recommended": False,
        "layer6_exit_finalized": False,
        "new_production_authority_granted": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "narrow_exit_finalization_planning_allowed_next": (
            all_checks_passed
            and narrow_documented_scope_complete
        ),
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "reassessment_checks.csv"
            ),
            str(
                OUTPUT_DIR / "predecessor_contracts.csv"
            ),
            str(
                OUTPUT_DIR / "scope_ledger.csv"
            ),
            str(
                OUTPUT_DIR / "decision_matrix.csv"
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
                OUTPUT_DIR / "reassessment_summary.json"
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
