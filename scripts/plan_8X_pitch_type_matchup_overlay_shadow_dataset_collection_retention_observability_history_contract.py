#!/usr/bin/env python3
"""
Layer 8X
Pitch-Type Matchup Overlay Shadow Dataset Collection Retention
Observability History Contract Plan

Defines an immutable, append-only diagnostic history for Layer 8W retention-
observability snapshots and reports.

Planning only.

This layer does not:
- execute retention actions;
- delete, archive, expire, quarantine, or mutate records;
- alter Layer 8U retention decisions;
- alter Layer 8W observability snapshots;
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


LAYER_ID = "8X"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_collection_"
    "retention_observability_history_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8X_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_history_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "audit_8W_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_contract.py"
)


HISTORY_RECORD_FIELDS = [
    {
        "field": "history_record_id",
        "type": "deterministic_string",
        "required": True,
    },
    {
        "field": "history_version",
        "type": "string",
        "required": True,
    },
    {
        "field": "recorded_at_utc",
        "type": "datetime",
        "required": True,
    },
    {
        "field": "retention_observability_snapshot_id",
        "type": "string",
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
        "field": "snapshot_payload_digest",
        "type": "sha256_string",
        "required": True,
    },
    {
        "field": "report_payload_digest",
        "type": "sha256_string",
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


HISTORY_STATUSES = [
    {
        "status": "appended",
        "meaning": (
            "A new immutable observability-history record was appended."
        ),
    },
    {
        "status": "idempotent",
        "meaning": (
            "An exact existing history record was encountered and retained "
            "without mutation."
        ),
    },
    {
        "status": "conflicted",
        "meaning": (
            "An existing history identity contained different immutable "
            "content."
        ),
    },
    {
        "status": "empty",
        "meaning": (
            "An emitted history contains no observability records."
        ),
    },
    {
        "status": "disabled",
        "meaning": (
            "Observability-history recording is disabled and emits no record."
        ),
    },
]


IDENTITY_RULES = [
    {
        "rule_id": "RH-I01",
        "rule": "history_record_id_is_deterministic",
    },
    {
        "rule_id": "RH-I02",
        "rule": "identity_includes_snapshot_id",
    },
    {
        "rule_id": "RH-I03",
        "rule": "identity_includes_history_version",
    },
    {
        "rule_id": "RH-I04",
        "rule": "identity_includes_snapshot_payload_digest",
    },
    {
        "rule_id": "RH-I05",
        "rule": "identity_includes_report_payload_digest",
    },
    {
        "rule_id": "RH-I06",
        "rule": "input_order_does_not_change_history_identity",
    },
]


HISTORY_RULES = [
    {
        "rule_id": "RH-H01",
        "rule": "history_is_append_only",
    },
    {
        "rule_id": "RH-H02",
        "rule": "existing_history_records_are_immutable",
    },
    {
        "rule_id": "RH-H03",
        "rule": "exact_duplicate_history_records_are_idempotent",
    },
    {
        "rule_id": "RH-H04",
        "rule": "conflicting_history_records_are_rejected",
    },
    {
        "rule_id": "RH-H05",
        "rule": "history_version_change_required_for_contract_change",
    },
    {
        "rule_id": "RH-H06",
        "rule": "history_record_order_is_deterministic",
    },
]


VALIDATION_RULES = [
    {
        "rule_id": "RH-V01",
        "rule": "history_version_explicit",
    },
    {
        "rule_id": "RH-V02",
        "rule": "observability_report_required_when_enabled",
    },
    {
        "rule_id": "RH-V03",
        "rule": "observability_snapshot_required_when_emitted",
    },
    {
        "rule_id": "RH-V04",
        "rule": "snapshot_id_present",
    },
    {
        "rule_id": "RH-V05",
        "rule": "observability_version_present",
    },
    {
        "rule_id": "RH-V06",
        "rule": "observed_at_utc_present",
    },
    {
        "rule_id": "RH-V07",
        "rule": "retention_version_present",
    },
    {
        "rule_id": "RH-V08",
        "rule": "retention_status_supported",
    },
    {
        "rule_id": "RH-V09",
        "rule": "observability_status_supported",
    },
    {
        "rule_id": "RH-V10",
        "rule": "decision_count_nonnegative",
    },
    {
        "rule_id": "RH-V11",
        "rule": "retention_status_counts_nonnegative",
    },
    {
        "rule_id": "RH-V12",
        "rule": "status_counts_reconcile_with_decision_count",
    },
    {
        "rule_id": "RH-V13",
        "rule": "snapshot_payload_digest_present",
    },
    {
        "rule_id": "RH-V14",
        "rule": "report_payload_digest_present",
    },
    {
        "rule_id": "RH-V15",
        "rule": "history_record_id_deterministic",
    },
    {
        "rule_id": "RH-V16",
        "rule": "exact_duplicate_history_is_idempotent",
    },
    {
        "rule_id": "RH-V17",
        "rule": "conflicting_duplicate_history_is_rejected",
    },
    {
        "rule_id": "RH-V18",
        "rule": "caller_observability_report_immutable",
    },
    {
        "rule_id": "RH-V19",
        "rule": "caller_existing_history_immutable",
    },
    {
        "rule_id": "RH-V20",
        "rule": "retention_action_not_executed",
    },
    {
        "rule_id": "RH-V21",
        "rule": "disabled_path_non_emitting",
    },
    {
        "rule_id": "RH-V22",
        "rule": "production_authority_false",
    },
]


ARTIFACT_SCHEMAS = [
    {
        "artifact": "observability_history_records.csv",
        "scope": "one_row_per_immutable_history_record",
        "required": True,
    },
    {
        "artifact": "observability_history_ledger.json",
        "scope": "append_only_history_ledger",
        "required": True,
    },
    {
        "artifact": "history_status_counts.csv",
        "scope": "one_row_per_history_status",
        "required": True,
    },
    {
        "artifact": "observability_status_timeline.csv",
        "scope": "one_row_per_observability_history_record",
        "required": True,
    },
    {
        "artifact": "duplicate_history_report.csv",
        "scope": "one_row_per_duplicate_history_identity",
        "required": True,
    },
    {
        "artifact": "integrity_failures.csv",
        "scope": "one_row_per_history_integrity_failure",
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
        "fallback_id": "RH-F01",
        "condition": "history_disabled",
        "result": "no_history_record_emitted",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_disabled"
        ),
    },
    {
        "fallback_id": "RH-F02",
        "condition": "observability_report_missing",
        "result": "conflicted_history_result",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_report_missing"
        ),
    },
    {
        "fallback_id": "RH-F03",
        "condition": "snapshot_missing_from_emitted_report",
        "result": "conflicted_history_result",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_snapshot_missing"
        ),
    },
    {
        "fallback_id": "RH-F04",
        "condition": "snapshot_digest_missing",
        "result": "conflicted_history_result",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_snapshot_digest_missing"
        ),
    },
    {
        "fallback_id": "RH-F05",
        "condition": "report_digest_missing",
        "result": "conflicted_history_result",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_report_digest_missing"
        ),
    },
    {
        "fallback_id": "RH-F06",
        "condition": "history_identity_conflict",
        "result": "conflicted_history_result",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_identity_conflict"
        ),
    },
    {
        "fallback_id": "RH-F07",
        "condition": "status_count_mismatch",
        "result": "conflicted_history_result",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_count_mismatch"
        ),
    },
    {
        "fallback_id": "RH-F08",
        "condition": "empty_existing_history",
        "result": "empty_or_first_append_result",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_empty"
        ),
    },
]


IMPLEMENTATION_STEPS = [
    {
        "step": 1,
        "action": (
            "Create immutable observability-history record, duplicate, "
            "and ledger types."
        ),
    },
    {
        "step": 2,
        "action": (
            "Import Layer 8W retention-observability reports and snapshots."
        ),
    },
    {
        "step": 3,
        "action": (
            "Create canonical snapshot and report payload digests."
        ),
    },
    {
        "step": 4,
        "action": (
            "Create deterministic observability-history record identifiers."
        ),
    },
    {
        "step": 5,
        "action": (
            "Validate snapshot status counts and immutable source fields."
        ),
    },
    {
        "step": 6,
        "action": (
            "Append new observability snapshots without mutating prior records."
        ),
    },
    {
        "step": 7,
        "action": (
            "Treat exact duplicate history records as idempotent."
        ),
    },
    {
        "step": 8,
        "action": (
            "Reject conflicting duplicate history identities."
        ),
    },
    {
        "step": 9,
        "action": (
            "Order history records deterministically by observation time "
            "and identity."
        ),
    },
    {
        "step": 10,
        "action": (
            "Keep observability-history recording disabled by default."
        ),
    },
    {
        "step": 11,
        "action": (
            "Create an independent observability-history contract audit."
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
        "criterion_id": "RH-C01",
        "criterion": "layer_8W_dependency_verified",
    },
    {
        "criterion_id": "RH-C02",
        "criterion": "history_record_schema_defined",
    },
    {
        "criterion_id": "RH-C03",
        "criterion": "five_history_statuses_defined",
    },
    {
        "criterion_id": "RH-C04",
        "criterion": "snapshot_payload_digest_defined",
    },
    {
        "criterion_id": "RH-C05",
        "criterion": "report_payload_digest_defined",
    },
    {
        "criterion_id": "RH-C06",
        "criterion": "deterministic_history_identity_defined",
    },
    {
        "criterion_id": "RH-C07",
        "criterion": "append_only_history_defined",
    },
    {
        "criterion_id": "RH-C08",
        "criterion": "immutable_existing_records_defined",
    },
    {
        "criterion_id": "RH-C09",
        "criterion": "idempotent_duplicate_handling_defined",
    },
    {
        "criterion_id": "RH-C10",
        "criterion": "conflicting_duplicate_rejection_defined",
    },
    {
        "criterion_id": "RH-C11",
        "criterion": "deterministic_history_order_defined",
    },
    {
        "criterion_id": "RH-C12",
        "criterion": "disabled_path_non_emitting",
    },
    {
        "criterion_id": "RH-C13",
        "criterion": "retention_action_execution_absent",
    },
    {
        "criterion_id": "RH-C14",
        "criterion": "historical_outcomes_absent",
    },
    {
        "criterion_id": "RH-C15",
        "criterion": "predictive_evaluation_absent",
    },
    {
        "criterion_id": "RH-C16",
        "criterion": "production_authority_absent",
    },
]


PROHIBITED_AUTHORITIES = [
    "physical_record_deletion",
    "record_archival_execution",
    "record_expiration_execution",
    "record_quarantine_execution",
    "retention_decision_mutation",
    "retention_observability_mutation",
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
        "retention_observability_contract_implementation_passed"
        in string_constants(
            PREDECESSOR_PATH
        )
    )

    field_names = [
        row["field"]
        for row in HISTORY_RECORD_FIELDS
    ]

    planning_checks = [
        {
            "check": "eight_w_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twenty_four_history_fields_defined",
            "actual": len(HISTORY_RECORD_FIELDS),
            "expected": 24,
            "passed": (
                len(HISTORY_RECORD_FIELDS) == 24
            ),
        },
        {
            "check": "history_field_names_unique",
            "actual": len(set(field_names)),
            "expected": len(field_names),
            "passed": (
                len(set(field_names))
                == len(field_names)
            ),
        },
        {
            "check": "five_history_statuses_defined",
            "actual": len(HISTORY_STATUSES),
            "expected": 5,
            "passed": len(HISTORY_STATUSES) == 5,
        },
        {
            "check": "six_identity_rules_defined",
            "actual": len(IDENTITY_RULES),
            "expected": 6,
            "passed": len(IDENTITY_RULES) == 6,
        },
        {
            "check": "six_history_rules_defined",
            "actual": len(HISTORY_RULES),
            "expected": 6,
            "passed": len(HISTORY_RULES) == 6,
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
            "check": "append_only_history_defined",
            "actual": any(
                row["rule"]
                == "history_is_append_only"
                for row in HISTORY_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"]
                == "history_is_append_only"
                for row in HISTORY_RULES
            ),
        },
        {
            "check": "idempotent_duplicate_handling_defined",
            "actual": any(
                row["rule"]
                == "exact_duplicate_history_records_are_idempotent"
                for row in HISTORY_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"]
                == "exact_duplicate_history_records_are_idempotent"
                for row in HISTORY_RULES
            ),
        },
        {
            "check": "conflicting_duplicate_rejection_defined",
            "actual": any(
                row["rule"]
                == "conflicting_history_records_are_rejected"
                for row in HISTORY_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"]
                == "conflicting_history_records_are_rejected"
                for row in HISTORY_RULES
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
                "8X defines diagnostic observability history only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.append(
        {
            "authority": (
                "retention_observability_history_implementation"
            ),
            "granted": all_checks_passed,
            "reason": (
                "8Y may implement a bounded immutable history of "
                "retention-observability snapshots."
            ),
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_contract_plan_failed"
    )

    recommended_next_layer = (
        "8Y_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_contract_implementation"
        if all_checks_passed
        else
        "8X_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_contract_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "history_record_fields.csv": (
            HISTORY_RECORD_FIELDS
        ),
        "history_statuses.csv": HISTORY_STATUSES,
        "identity_rules.csv": IDENTITY_RULES,
        "history_rules.csv": HISTORY_RULES,
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
        "history_record_fields.csv": [
            "field",
            "type",
            "required",
        ],
        "history_statuses.csv": [
            "status",
            "meaning",
        ],
        "identity_rules.csv": [
            "rule_id",
            "rule",
        ],
        "history_rules.csv": [
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
                    "Implement an immutable append-only history of "
                    "retention-observability snapshots."
                    if all_checks_passed
                    else
                    "Remediate failed 8X planning checks."
                ),
                "entry_condition": (
                    "All nineteen 8X planning checks pass."
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
            row["passed"]
            for row in planning_checks
        ),
        "history_record_fields_defined": len(
            HISTORY_RECORD_FIELDS
        ),
        "history_statuses_defined": len(
            HISTORY_STATUSES
        ),
        "identity_rules_defined": len(
            IDENTITY_RULES
        ),
        "history_rules_defined": len(
            HISTORY_RULES
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
        "append_only_history_defined": True,
        "immutable_existing_records_defined": True,
        "idempotent_duplicate_handling_defined": True,
        "conflicting_duplicate_rejection_defined": True,
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
        "retention_observability_history_implementation_allowed_next": (
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
                OUTPUT_DIR / "contract_summary.json"
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

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
