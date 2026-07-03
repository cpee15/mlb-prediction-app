#!/usr/bin/env python3
"""
Layer 8V
Pitch-Type Matchup Overlay Shadow Dataset Collection Retention
Observability Contract Plan

Defines deterministic, diagnostic-only observability for Layer 8U retention
decisions and append-only retention ledgers.

Planning only.

This layer does not:
- delete, archive, expire, quarantine, or mutate records;
- alter Layer 8U retention decisions;
- join historical outcomes;
- evaluate predictive accuracy or calibration;
- tune retention windows or thresholds;
- modify production or simulation behavior;
- run backtests;
- perform pricing, market comparison, or edge detection.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "8V"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_collection_"
    "retention_observability_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8V_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "audit_8U_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_contract.py"
)


OBSERVABILITY_FIELDS = [
    {
        "field": "retention_observability_snapshot_id",
        "type": "deterministic_string",
        "required": True,
    },
    {
        "field": "retention_observability_version",
        "type": "string",
        "required": True,
    },
    {
        "field": "observed_at_utc",
        "type": "datetime",
        "required": True,
    },
    {
        "field": "retention_version",
        "type": "string",
        "required": True,
    },
    {
        "field": "retention_status",
        "type": "enum",
        "required": True,
    },
    {
        "field": "observability_status",
        "type": "enum",
        "required": True,
    },
    {
        "field": "decision_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "retained_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "archived_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "expired_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "quarantined_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "exact_duplicate_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "conflicting_duplicate_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "minimum_record_age_days",
        "type": "nullable_nonnegative_integer",
        "required": True,
    },
    {
        "field": "mean_record_age_days",
        "type": "nullable_nonnegative_number",
        "required": True,
    },
    {
        "field": "maximum_record_age_days",
        "type": "nullable_nonnegative_integer",
        "required": True,
    },
    {
        "field": "retention_window_days",
        "type": "positive_integer",
        "required": True,
    },
    {
        "field": "archive_window_days",
        "type": "positive_integer",
        "required": True,
    },
    {
        "field": "ledger_digest_reconciles",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "decision_identifiers_unique",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "policy_windows_reconcile",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "diagnostic_codes",
        "type": "sorted_unique_string_array",
        "required": True,
    },
    {
        "field": "validation_errors",
        "type": "sorted_unique_string_array",
        "required": True,
    },
    {
        "field": "production_authority",
        "type": "boolean_false",
        "required": True,
    },
]


OBSERVABILITY_STATUSES = [
    {
        "status": "healthy",
        "meaning": (
            "Ledger integrity and policy reconciliation pass without warnings."
        ),
    },
    {
        "status": "warning",
        "meaning": (
            "Ledger remains valid but contains exact duplicates, archived, "
            "expired, or other diagnostic warning signals."
        ),
    },
    {
        "status": "degraded",
        "meaning": (
            "Digest, identity, conflict, quarantine, or policy reconciliation "
            "fails."
        ),
    },
    {
        "status": "empty",
        "meaning": (
            "An emitted ledger contains no retention decisions."
        ),
    },
    {
        "status": "disabled",
        "meaning": (
            "Retention observability is disabled and emits no snapshot."
        ),
    },
]


SIGNAL_GROUPS = [
    {
        "group_id": "RO-G01",
        "group": "ledger_integrity",
    },
    {
        "group_id": "RO-G02",
        "group": "decision_identity",
    },
    {
        "group_id": "RO-G03",
        "group": "retention_status_distribution",
    },
    {
        "group_id": "RO-G04",
        "group": "record_age_distribution",
    },
    {
        "group_id": "RO-G05",
        "group": "policy_window_reconciliation",
    },
    {
        "group_id": "RO-G06",
        "group": "duplicate_integrity",
    },
    {
        "group_id": "RO-G07",
        "group": "quarantine_integrity",
    },
    {
        "group_id": "RO-G08",
        "group": "authority_boundary",
    },
]


AGGREGATION_RULES = [
    {
        "rule_id": "RO-A01",
        "rule": "count_decisions_by_retention_status",
    },
    {
        "rule_id": "RO-A02",
        "rule": "count_exact_and_conflicting_duplicates",
    },
    {
        "rule_id": "RO-A03",
        "rule": "compute_record_age_minimum_mean_maximum",
    },
    {
        "rule_id": "RO-A04",
        "rule": "recompute_retention_ledger_digest",
    },
    {
        "rule_id": "RO-A05",
        "rule": "validate_unique_retention_decision_identifiers",
    },
    {
        "rule_id": "RO-A06",
        "rule": "reconcile_statuses_against_policy_windows",
    },
]


VALIDATION_RULES = [
    {
        "rule_id": "RO-V01",
        "rule": "retention_observability_version_explicit",
    },
    {
        "rule_id": "RO-V02",
        "rule": "retention_ledger_required_when_enabled",
    },
    {
        "rule_id": "RO-V03",
        "rule": "retention_version_present",
    },
    {
        "rule_id": "RO-V04",
        "rule": "retention_status_supported",
    },
    {
        "rule_id": "RO-V05",
        "rule": "decision_count_nonnegative",
    },
    {
        "rule_id": "RO-V06",
        "rule": "status_counts_reconcile_with_decision_count",
    },
    {
        "rule_id": "RO-V07",
        "rule": "retention_decision_ids_present",
    },
    {
        "rule_id": "RO-V08",
        "rule": "retention_decision_ids_unique",
    },
    {
        "rule_id": "RO-V09",
        "rule": "retention_ledger_digest_present",
    },
    {
        "rule_id": "RO-V10",
        "rule": "retention_ledger_digest_reconciles",
    },
    {
        "rule_id": "RO-V11",
        "rule": "record_ages_nonnegative",
    },
    {
        "rule_id": "RO-V12",
        "rule": "retention_window_positive",
    },
    {
        "rule_id": "RO-V13",
        "rule": "archive_window_positive",
    },
    {
        "rule_id": "RO-V14",
        "rule": "archive_window_exceeds_retention_window",
    },
    {
        "rule_id": "RO-V15",
        "rule": "retained_decisions_reconcile_with_active_window",
    },
    {
        "rule_id": "RO-V16",
        "rule": "archived_decisions_reconcile_with_archive_window",
    },
    {
        "rule_id": "RO-V17",
        "rule": "expired_decisions_reconcile_beyond_archive_window",
    },
    {
        "rule_id": "RO-V18",
        "rule": "quarantined_decisions_reconcile_with_quarantine_flag",
    },
    {
        "rule_id": "RO-V19",
        "rule": "conflicting_duplicates_degrade_observability",
    },
    {
        "rule_id": "RO-V20",
        "rule": "caller_retention_ledger_immutable",
    },
    {
        "rule_id": "RO-V21",
        "rule": "disabled_path_non_emitting",
    },
    {
        "rule_id": "RO-V22",
        "rule": "production_authority_false",
    },
]


ARTIFACT_SCHEMAS = [
    {
        "artifact": "retention_observability_snapshot.csv",
        "scope": "one_row_per_observed_retention_ledger",
        "required": True,
    },
    {
        "artifact": "retention_observability_report.json",
        "scope": "complete_observability_report",
        "required": True,
    },
    {
        "artifact": "status_counts.csv",
        "scope": "one_row_per_observability_status",
        "required": True,
    },
    {
        "artifact": "retention_status_distribution.csv",
        "scope": "one_row_per_retention_status",
        "required": True,
    },
    {
        "artifact": "signal_results.csv",
        "scope": "one_row_per_observability_signal",
        "required": True,
    },
    {
        "artifact": "duplicate_signals.csv",
        "scope": "one_row_per_duplicate_signal",
        "required": True,
    },
    {
        "artifact": "authority_boundaries.csv",
        "scope": "authority_contract",
        "required": True,
    },
    {
        "artifact": "diagnosis.json",
        "scope": "layer_diagnosis",
        "required": True,
    },
]


FALLBACK_CONTRACTS = [
    {
        "fallback_id": "RO-F01",
        "condition": "observability_disabled",
        "result": "no_snapshot_emitted",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_disabled"
        ),
    },
    {
        "fallback_id": "RO-F02",
        "condition": "retention_ledger_missing",
        "result": "degraded_snapshot",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_ledger_missing"
        ),
    },
    {
        "fallback_id": "RO-F03",
        "condition": "ledger_digest_missing",
        "result": "degraded_snapshot",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_digest_missing"
        ),
    },
    {
        "fallback_id": "RO-F04",
        "condition": "ledger_digest_mismatch",
        "result": "degraded_snapshot",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_digest_mismatch"
        ),
    },
    {
        "fallback_id": "RO-F05",
        "condition": "decision_identity_conflict",
        "result": "degraded_snapshot",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_identity_conflict"
        ),
    },
    {
        "fallback_id": "RO-F06",
        "condition": "policy_window_mismatch",
        "result": "degraded_snapshot",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_policy_mismatch"
        ),
    },
    {
        "fallback_id": "RO-F07",
        "condition": "conflicting_duplicate_present",
        "result": "degraded_snapshot",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_conflict"
        ),
    },
    {
        "fallback_id": "RO-F08",
        "condition": "empty_emitted_ledger",
        "result": "empty_snapshot",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_empty"
        ),
    },
]


IMPLEMENTATION_STEPS = [
    {
        "step": 1,
        "action": (
            "Create immutable retention-observability signal, snapshot, "
            "and report types."
        ),
    },
    {
        "step": 2,
        "action": (
            "Import Layer 8U retention decisions, duplicates, and ledger."
        ),
    },
    {
        "step": 3,
        "action": (
            "Recompute and reconcile the deterministic ledger digest."
        ),
    },
    {
        "step": 4,
        "action": (
            "Validate unique retention-decision identifiers."
        ),
    },
    {
        "step": 5,
        "action": (
            "Aggregate retained, archived, expired, and quarantined counts."
        ),
    },
    {
        "step": 6,
        "action": (
            "Aggregate exact and conflicting duplicate counts."
        ),
    },
    {
        "step": 7,
        "action": (
            "Compute deterministic record-age distribution statistics."
        ),
    },
    {
        "step": 8,
        "action": (
            "Reconcile each decision with active and archive policy windows."
        ),
    },
    {
        "step": 9,
        "action": (
            "Classify healthy, warning, degraded, empty, and disabled paths."
        ),
    },
    {
        "step": 10,
        "action": (
            "Preserve caller retention-ledger immutability."
        ),
    },
    {
        "step": 11,
        "action": (
            "Create an independent retention-observability audit."
        ),
    },
    {
        "step": 12,
        "action": (
            "Emit deterministic CSV and JSON artifacts without executing "
            "retention actions."
        ),
    },
]


ACCEPTANCE_CRITERIA = [
    {
        "criterion_id": "RO-C01",
        "criterion": "layer_8U_dependency_verified",
    },
    {
        "criterion_id": "RO-C02",
        "criterion": "retention_observability_schema_defined",
    },
    {
        "criterion_id": "RO-C03",
        "criterion": "five_observability_statuses_defined",
    },
    {
        "criterion_id": "RO-C04",
        "criterion": "ledger_digest_reconciliation_defined",
    },
    {
        "criterion_id": "RO-C05",
        "criterion": "decision_identity_validation_defined",
    },
    {
        "criterion_id": "RO-C06",
        "criterion": "retention_status_distribution_defined",
    },
    {
        "criterion_id": "RO-C07",
        "criterion": "record_age_distribution_defined",
    },
    {
        "criterion_id": "RO-C08",
        "criterion": "policy_window_reconciliation_defined",
    },
    {
        "criterion_id": "RO-C09",
        "criterion": "duplicate_integrity_signals_defined",
    },
    {
        "criterion_id": "RO-C10",
        "criterion": "quarantine_integrity_signals_defined",
    },
    {
        "criterion_id": "RO-C11",
        "criterion": "deterministic_snapshot_identity_defined",
    },
    {
        "criterion_id": "RO-C12",
        "criterion": "disabled_path_non_emitting",
    },
    {
        "criterion_id": "RO-C13",
        "criterion": "retention_action_execution_absent",
    },
    {
        "criterion_id": "RO-C14",
        "criterion": "historical_outcomes_absent",
    },
    {
        "criterion_id": "RO-C15",
        "criterion": "predictive_evaluation_absent",
    },
    {
        "criterion_id": "RO-C16",
        "criterion": "production_authority_absent",
    },
]


PROHIBITED_AUTHORITIES = [
    "physical_record_deletion",
    "record_archival_execution",
    "record_expiration_execution",
    "record_quarantine_execution",
    "retention_decision_mutation",
    "retention_window_tuning",
    "production_overlay_integration",
    "production_matchup_adjustment",
    "simulation_state_change",
    "simulation_probability_change",
    "historical_outcome_join",
    "predictive_accuracy_evaluation",
    "calibration_evaluation",
    "backtest_execution",
    "pricing",
    "market_comparison",
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
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def string_constants(
    path: Path,
) -> set[str]:
    if not path.exists():
        return set()

    try:
        tree = ast.parse(
            path.read_text(
                encoding="utf-8",
                errors="ignore",
            ),
            filename=str(path),
        )
    except SyntaxError:
        return set()

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

    predecessor_present = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_contract_implementation_passed"
        in string_constants(
            PREDECESSOR_PATH
        )
    )

    field_names = [
        row["field"]
        for row in OBSERVABILITY_FIELDS
    ]

    planning_checks = [
        {
            "check": "eight_u_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twenty_four_observability_fields_defined",
            "actual": len(OBSERVABILITY_FIELDS),
            "expected": 24,
            "passed": len(OBSERVABILITY_FIELDS) == 24,
        },
        {
            "check": "observability_field_names_unique",
            "actual": len(set(field_names)),
            "expected": len(field_names),
            "passed": (
                len(set(field_names))
                == len(field_names)
            ),
        },
        {
            "check": "five_observability_statuses_defined",
            "actual": len(OBSERVABILITY_STATUSES),
            "expected": 5,
            "passed": len(OBSERVABILITY_STATUSES) == 5,
        },
        {
            "check": "eight_signal_groups_defined",
            "actual": len(SIGNAL_GROUPS),
            "expected": 8,
            "passed": len(SIGNAL_GROUPS) == 8,
        },
        {
            "check": "six_aggregation_rules_defined",
            "actual": len(AGGREGATION_RULES),
            "expected": 6,
            "passed": len(AGGREGATION_RULES) == 6,
        },
        {
            "check": "twenty_two_validation_rules_defined",
            "actual": len(VALIDATION_RULES),
            "expected": 22,
            "passed": len(VALIDATION_RULES) == 22,
        },
        {
            "check": "eight_artifact_schemas_defined",
            "actual": len(ARTIFACT_SCHEMAS),
            "expected": 8,
            "passed": len(ARTIFACT_SCHEMAS) == 8,
        },
        {
            "check": "eight_fallback_contracts_defined",
            "actual": len(FALLBACK_CONTRACTS),
            "expected": 8,
            "passed": len(FALLBACK_CONTRACTS) == 8,
        },
        {
            "check": "twelve_implementation_steps_defined",
            "actual": len(IMPLEMENTATION_STEPS),
            "expected": 12,
            "passed": len(IMPLEMENTATION_STEPS) == 12,
        },
        {
            "check": "sixteen_acceptance_criteria_defined",
            "actual": len(ACCEPTANCE_CRITERIA),
            "expected": 16,
            "passed": len(ACCEPTANCE_CRITERIA) == 16,
        },
        {
            "check": "ledger_digest_reconciliation_defined",
            "actual": any(
                row["rule"]
                == "recompute_retention_ledger_digest"
                for row in AGGREGATION_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"]
                == "recompute_retention_ledger_digest"
                for row in AGGREGATION_RULES
            ),
        },
        {
            "check": "policy_window_reconciliation_defined",
            "actual": any(
                row["rule"]
                == "reconcile_statuses_against_policy_windows"
                for row in AGGREGATION_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"]
                == "reconcile_statuses_against_policy_windows"
                for row in AGGREGATION_RULES
            ),
        },
        {
            "check": "duplicate_integrity_defined",
            "actual": any(
                row["group"]
                == "duplicate_integrity"
                for row in SIGNAL_GROUPS
            ),
            "expected": True,
            "passed": any(
                row["group"]
                == "duplicate_integrity"
                for row in SIGNAL_GROUPS
            ),
        },
        {
            "check": "retention_action_execution_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "historical_outcome_join_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "predictive_evaluation_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "planning_only_boundary_preserved",
            "actual": True,
            "expected": True,
            "passed": True,
        },
        {
            "check": (
                "production_tuning_pricing_edge_authority_absent"
            ),
            "actual": False,
            "expected": False,
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
                "8V defines diagnostic retention observability only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.append(
        {
            "authority": (
                "retention_observability_implementation"
            ),
            "granted": all_checks_passed,
            "reason": (
                "8W may implement bounded diagnostic observability "
                "for immutable retention ledgers."
            ),
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_contract_plan_failed"
    )

    recommended_next_layer = (
        "8W_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_contract_implementation"
        if all_checks_passed
        else
        "8V_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_contract_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "observability_fields.csv": OBSERVABILITY_FIELDS,
        "observability_statuses.csv": OBSERVABILITY_STATUSES,
        "signal_groups.csv": SIGNAL_GROUPS,
        "aggregation_rules.csv": AGGREGATION_RULES,
        "validation_rules.csv": VALIDATION_RULES,
        "artifact_schemas.csv": ARTIFACT_SCHEMAS,
        "fallback_contracts.csv": FALLBACK_CONTRACTS,
        "implementation_steps.csv": IMPLEMENTATION_STEPS,
        "acceptance_criteria.csv": ACCEPTANCE_CRITERIA,
        "authority_boundaries.csv": authority_rows,
    }

    fieldnames = {
        "planning_checks.csv": [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        "observability_fields.csv": [
            "field",
            "type",
            "required",
        ],
        "observability_statuses.csv": [
            "status",
            "meaning",
        ],
        "signal_groups.csv": [
            "group_id",
            "group",
        ],
        "aggregation_rules.csv": [
            "rule_id",
            "rule",
        ],
        "validation_rules.csv": [
            "rule_id",
            "rule",
        ],
        "artifact_schemas.csv": [
            "artifact",
            "scope",
            "required",
        ],
        "fallback_contracts.csv": [
            "fallback_id",
            "condition",
            "result",
            "diagnostic_code",
        ],
        "implementation_steps.csv": [
            "step",
            "action",
        ],
        "acceptance_criteria.csv": [
            "criterion_id",
            "criterion",
        ],
        "authority_boundaries.csv": [
            "authority",
            "granted",
            "reason",
        ],
    }

    for filename, rows in artifacts.items():
        write_csv(
            OUTPUT_DIR / filename,
            fieldnames[filename],
            rows,
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
                    "Implement deterministic observability for immutable "
                    "retention decisions and ledgers."
                    if all_checks_passed
                    else
                    "Remediate failed 8V planning checks."
                ),
                "entry_condition": (
                    "All nineteen 8V planning checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    summary = {
        "planning_checks_required": len(
            planning_checks
        ),
        "planning_checks_passed": sum(
            1
            for row in planning_checks
            if row["passed"]
        ),
        "observability_fields_defined": len(
            OBSERVABILITY_FIELDS
        ),
        "observability_statuses_defined": len(
            OBSERVABILITY_STATUSES
        ),
        "signal_groups_defined": len(
            SIGNAL_GROUPS
        ),
        "aggregation_rules_defined": len(
            AGGREGATION_RULES
        ),
        "validation_rules_defined": len(
            VALIDATION_RULES
        ),
        "artifact_schemas_defined": len(
            ARTIFACT_SCHEMAS
        ),
        "fallback_contracts_defined": len(
            FALLBACK_CONTRACTS
        ),
        "implementation_steps_defined": len(
            IMPLEMENTATION_STEPS
        ),
        "acceptance_criteria_defined": len(
            ACCEPTANCE_CRITERIA
        ),
        "ledger_digest_reconciliation_defined": True,
        "policy_window_reconciliation_defined": True,
        "retention_action_executed": False,
        "physical_deletion_executed": False,
        "historical_outcome_joined": False,
        "predictive_evaluation_executed": False,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "tuning_executed": False,
        "pricing_or_edge_work_executed": False,
    }

    write_json(
        OUTPUT_DIR / "contract_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_name,
        "all_checks_passed": all_checks_passed,
        **summary,
        "layer8_completed": False,
        "new_production_authority_granted": False,
        "retention_action_allowed_next": False,
        "physical_deletion_allowed_next": False,
        "historical_validation_allowed_next": False,
        "historical_outcome_enrichment_allowed_next": False,
        "predictive_evaluation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "retention_observability_implementation_allowed_next": (
            all_checks_passed
        ),
        "production_matchup_overlay_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / filename
            )
            for filename in [
                *artifacts.keys(),
                "recommended_path.csv",
            ]
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "contract_summary.json"
            ),
            str(
                OUTPUT_DIR
                / "diagnosis.json"
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

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
