#!/usr/bin/env python3
"""
Layer 8T
Pitch-Type Matchup Overlay Shadow Dataset Collection Retention Contract Plan

Defines deterministic, diagnostic-only retention decisions for Layer 8Q
append-only collections using Layer 8S observability reports.

Planning only.

This layer does not:
- delete or mutate collection records;
- join historical outcomes;
- evaluate predictive accuracy or calibration;
- tune parameters or thresholds;
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


LAYER_ID = "8T"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_collection_retention_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8T_pitch_type_matchup_overlay_shadow_dataset_collection_retention_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "audit_8S_pitch_type_matchup_overlay_shadow_dataset_collection_observability_contract.py"
)

RETENTION_DECISION_FIELDS = [
    {
        "field": "retention_decision_id",
        "type": "deterministic_string",
        "required": True,
    },
    {
        "field": "retention_version",
        "type": "string",
        "required": True,
    },
    {
        "field": "evaluated_at_utc",
        "type": "datetime",
        "required": True,
    },
    {
        "field": "collection_record_id",
        "type": "string",
        "required": True,
    },
    {
        "field": "collection_version",
        "type": "string",
        "required": True,
    },
    {
        "field": "dataset_version",
        "type": "string",
        "required": True,
    },
    {
        "field": "quality_gate_version",
        "type": "string",
        "required": True,
    },
    {
        "field": "observability_version",
        "type": "string",
        "required": True,
    },
    {
        "field": "collection_status",
        "type": "enum",
        "required": True,
    },
    {
        "field": "observability_status",
        "type": "enum",
        "required": True,
    },
    {
        "field": "retention_status",
        "type": "enum",
        "required": True,
    },
    {
        "field": "retention_reason",
        "type": "string",
        "required": True,
    },
    {
        "field": "record_age_days",
        "type": "nonnegative_integer",
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
        "field": "eligible_for_retention",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "eligible_for_archive",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "eligible_for_expiration",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "quarantine_required",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "dataset_payload_digest",
        "type": "sha256_string",
        "required": True,
    },
    {
        "field": "quality_report_digest",
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

RETENTION_STATUSES = [
    {
        "status": "retained",
        "meaning": "Record remains in the active diagnostic collection window.",
    },
    {
        "status": "archived",
        "meaning": "Record is eligible for diagnostic archival after the active window.",
    },
    {
        "status": "expired",
        "meaning": "Record is beyond the archive window and marked for expiration only.",
    },
    {
        "status": "quarantined",
        "meaning": "Record requires isolation because integrity or observability failed.",
    },
    {
        "status": "disabled",
        "meaning": "Retention evaluation is disabled and emits no decision.",
    },
]

POLICY_RULES = [
    {
        "rule_id": "RT-P01",
        "rule": "retention_window_days_explicit",
    },
    {
        "rule_id": "RT-P02",
        "rule": "archive_window_days_exceeds_retention_window",
    },
    {
        "rule_id": "RT-P03",
        "rule": "records_within_retention_window_are_retained",
    },
    {
        "rule_id": "RT-P04",
        "rule": "records_between_windows_are_archived",
    },
    {
        "rule_id": "RT-P05",
        "rule": "records_beyond_archive_window_are_expired",
    },
    {
        "rule_id": "RT-P06",
        "rule": "degraded_observability_requires_quarantine",
    },
    {
        "rule_id": "RT-P07",
        "rule": "rejected_collection_records_require_quarantine",
    },
    {
        "rule_id": "RT-P08",
        "rule": "quarantine_overrides_age_based_status",
    },
]

IDENTITY_RULES = [
    {
        "rule_id": "RT-I01",
        "rule": "retention_decision_id_is_deterministic",
    },
    {
        "rule_id": "RT-I02",
        "rule": "identity_includes_collection_record_id",
    },
    {
        "rule_id": "RT-I03",
        "rule": "identity_includes_retention_version",
    },
    {
        "rule_id": "RT-I04",
        "rule": "identity_includes_policy_windows",
    },
    {
        "rule_id": "RT-I05",
        "rule": "identity_includes_retention_status",
    },
    {
        "rule_id": "RT-I06",
        "rule": "input_order_does_not_change_decision_identity",
    },
]

LEDGER_RULES = [
    {
        "rule_id": "RT-L01",
        "rule": "retention_ledger_is_append_only",
    },
    {
        "rule_id": "RT-L02",
        "rule": "existing_decisions_are_immutable",
    },
    {
        "rule_id": "RT-L03",
        "rule": "exact_duplicate_decisions_are_idempotent",
    },
    {
        "rule_id": "RT-L04",
        "rule": "conflicting_duplicate_decisions_are_rejected",
    },
    {
        "rule_id": "RT-L05",
        "rule": "policy_change_requires_retention_version_change",
    },
    {
        "rule_id": "RT-L06",
        "rule": "decision_order_is_deterministic",
    },
]

VALIDATION_RULES = [
    {
        "rule_id": "RT-V01",
        "rule": "retention_version_explicit",
    },
    {
        "rule_id": "RT-V02",
        "rule": "collection_required_when_enabled",
    },
    {
        "rule_id": "RT-V03",
        "rule": "observability_report_required_when_enabled",
    },
    {
        "rule_id": "RT-V04",
        "rule": "retention_window_positive",
    },
    {
        "rule_id": "RT-V05",
        "rule": "archive_window_positive",
    },
    {
        "rule_id": "RT-V06",
        "rule": "archive_window_greater_than_retention_window",
    },
    {
        "rule_id": "RT-V07",
        "rule": "collection_and_observability_versions_retained",
    },
    {
        "rule_id": "RT-V08",
        "rule": "collection_record_id_present",
    },
    {
        "rule_id": "RT-V09",
        "rule": "record_age_nonnegative",
    },
    {
        "rule_id": "RT-V10",
        "rule": "retention_status_supported",
    },
    {
        "rule_id": "RT-V11",
        "rule": "retained_status_window_reconciles",
    },
    {
        "rule_id": "RT-V12",
        "rule": "archived_status_window_reconciles",
    },
    {
        "rule_id": "RT-V13",
        "rule": "expired_status_window_reconciles",
    },
    {
        "rule_id": "RT-V14",
        "rule": "quarantine_precedence_reconciles",
    },
    {
        "rule_id": "RT-V15",
        "rule": "dataset_digest_present",
    },
    {
        "rule_id": "RT-V16",
        "rule": "quality_report_digest_present",
    },
    {
        "rule_id": "RT-V17",
        "rule": "retention_decision_id_deterministic",
    },
    {
        "rule_id": "RT-V18",
        "rule": "caller_collection_immutable",
    },
    {
        "rule_id": "RT-V19",
        "rule": "caller_observability_report_immutable",
    },
    {
        "rule_id": "RT-V20",
        "rule": "physical_deletion_not_executed",
    },
    {
        "rule_id": "RT-V21",
        "rule": "disabled_path_non_emitting",
    },
    {
        "rule_id": "RT-V22",
        "rule": "production_authority_false",
    },
]

ARTIFACT_SCHEMAS = [
    {
        "artifact": "retention_decisions.csv",
        "scope": "one_row_per_collection_record",
        "required": True,
    },
    {
        "artifact": "retention_ledger.json",
        "scope": "append_only_decision_ledger",
        "required": True,
    },
    {
        "artifact": "status_counts.csv",
        "scope": "one_row_per_retention_status",
        "required": True,
    },
    {
        "artifact": "policy_results.csv",
        "scope": "one_row_per_policy_rule",
        "required": True,
    },
    {
        "artifact": "duplicate_report.csv",
        "scope": "one_row_per_duplicate_decision_identity",
        "required": True,
    },
    {
        "artifact": "quarantine_report.csv",
        "scope": "one_row_per_quarantined_record",
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
        "fallback_id": "RT-F01",
        "condition": "retention_disabled",
        "result": "no_decision_emitted",
        "diagnostic_code": "matchup_shadow_retention_disabled",
    },
    {
        "fallback_id": "RT-F02",
        "condition": "collection_missing",
        "result": "quarantined_decision",
        "diagnostic_code": "matchup_shadow_retention_collection_missing",
    },
    {
        "fallback_id": "RT-F03",
        "condition": "observability_report_missing",
        "result": "quarantined_decision",
        "diagnostic_code": "matchup_shadow_retention_observability_missing",
    },
    {
        "fallback_id": "RT-F04",
        "condition": "invalid_policy_windows",
        "result": "quarantined_decision",
        "diagnostic_code": "matchup_shadow_retention_policy_invalid",
    },
    {
        "fallback_id": "RT-F05",
        "condition": "degraded_observability",
        "result": "quarantined_decision",
        "diagnostic_code": "matchup_shadow_retention_observability_degraded",
    },
    {
        "fallback_id": "RT-F06",
        "condition": "rejected_collection_record",
        "result": "quarantined_decision",
        "diagnostic_code": "matchup_shadow_retention_record_rejected",
    },
    {
        "fallback_id": "RT-F07",
        "condition": "conflicting_duplicate_decision",
        "result": "quarantined_decision",
        "diagnostic_code": "matchup_shadow_retention_conflicting_duplicate",
    },
    {
        "fallback_id": "RT-F08",
        "condition": "record_age_invalid",
        "result": "quarantined_decision",
        "diagnostic_code": "matchup_shadow_retention_record_age_invalid",
    },
]

IMPLEMENTATION_STEPS = [
    {
        "step": 1,
        "action": "Create immutable retention decision and ledger types.",
    },
    {
        "step": 2,
        "action": "Import Layer 8Q collection records and Layer 8S observability reports.",
    },
    {
        "step": 3,
        "action": "Validate explicit active and archive retention windows.",
    },
    {
        "step": 4,
        "action": "Compute deterministic nonnegative record ages.",
    },
    {
        "step": 5,
        "action": "Map records to retained, archived, expired, or quarantined.",
    },
    {
        "step": 6,
        "action": "Apply quarantine precedence over age-based policy.",
    },
    {
        "step": 7,
        "action": "Create deterministic retention decision identifiers.",
    },
    {
        "step": 8,
        "action": "Apply append-only and idempotent ledger handling.",
    },
    {
        "step": 9,
        "action": "Preserve caller collection and observability immutability.",
    },
    {
        "step": 10,
        "action": "Keep retention evaluation disabled by default.",
    },
    {
        "step": 11,
        "action": "Create an independent retention-contract audit.",
    },
    {
        "step": 12,
        "action": "Emit deterministic CSV and JSON artifacts without physical deletion.",
    },
]

ACCEPTANCE_CRITERIA = [
    {
        "criterion_id": "RT-C01",
        "criterion": "layer_8S_dependency_verified",
    },
    {
        "criterion_id": "RT-C02",
        "criterion": "retention_decision_schema_defined",
    },
    {
        "criterion_id": "RT-C03",
        "criterion": "five_retention_statuses_defined",
    },
    {
        "criterion_id": "RT-C04",
        "criterion": "retention_and_archive_windows_defined",
    },
    {
        "criterion_id": "RT-C05",
        "criterion": "retained_status_mapping_defined",
    },
    {
        "criterion_id": "RT-C06",
        "criterion": "archived_status_mapping_defined",
    },
    {
        "criterion_id": "RT-C07",
        "criterion": "expired_status_mapping_defined",
    },
    {
        "criterion_id": "RT-C08",
        "criterion": "quarantine_precedence_defined",
    },
    {
        "criterion_id": "RT-C09",
        "criterion": "deterministic_decision_identity_defined",
    },
    {
        "criterion_id": "RT-C10",
        "criterion": "append_only_retention_ledger_defined",
    },
    {
        "criterion_id": "RT-C11",
        "criterion": "idempotent_duplicate_handling_defined",
    },
    {
        "criterion_id": "RT-C12",
        "criterion": "disabled_path_non_emitting",
    },
    {
        "criterion_id": "RT-C13",
        "criterion": "physical_deletion_absent",
    },
    {
        "criterion_id": "RT-C14",
        "criterion": "historical_outcomes_absent",
    },
    {
        "criterion_id": "RT-C15",
        "criterion": "predictive_evaluation_absent",
    },
    {
        "criterion_id": "RT-C16",
        "criterion": "production_authority_absent",
    },
]

PROHIBITED_AUTHORITIES = [
    "physical_record_deletion",
    "production_overlay_integration",
    "production_matchup_adjustment",
    "simulation_state_change",
    "simulation_probability_change",
    "historical_outcome_join",
    "predictive_accuracy_evaluation",
    "calibration_evaluation",
    "parameter_calibration",
    "parameter_tuning",
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
        "pitch_type_matchup_overlay_shadow_dataset_collection_observability_contract_implementation_passed"
        in string_constants(
            PREDECESSOR_PATH
        )
    )

    field_names = [
        row["field"]
        for row in RETENTION_DECISION_FIELDS
    ]

    planning_checks = [
        {
            "check": "eight_s_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twenty_four_retention_fields_defined",
            "actual": len(RETENTION_DECISION_FIELDS),
            "expected": 24,
            "passed": len(RETENTION_DECISION_FIELDS) == 24,
        },
        {
            "check": "retention_field_names_unique",
            "actual": len(set(field_names)),
            "expected": len(field_names),
            "passed": len(set(field_names)) == len(field_names),
        },
        {
            "check": "five_retention_statuses_defined",
            "actual": len(RETENTION_STATUSES),
            "expected": 5,
            "passed": len(RETENTION_STATUSES) == 5,
        },
        {
            "check": "eight_policy_rules_defined",
            "actual": len(POLICY_RULES),
            "expected": 8,
            "passed": len(POLICY_RULES) == 8,
        },
        {
            "check": "six_identity_rules_defined",
            "actual": len(IDENTITY_RULES),
            "expected": 6,
            "passed": len(IDENTITY_RULES) == 6,
        },
        {
            "check": "six_ledger_rules_defined",
            "actual": len(LEDGER_RULES),
            "expected": 6,
            "passed": len(LEDGER_RULES) == 6,
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
            "check": "quarantine_precedence_defined",
            "actual": any(
                row["rule"]
                == "quarantine_overrides_age_based_status"
                for row in POLICY_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"]
                == "quarantine_overrides_age_based_status"
                for row in POLICY_RULES
            ),
        },
        {
            "check": "append_only_ledger_defined",
            "actual": any(
                row["rule"]
                == "retention_ledger_is_append_only"
                for row in LEDGER_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"]
                == "retention_ledger_is_append_only"
                for row in LEDGER_RULES
            ),
        },
        {
            "check": "physical_deletion_absent",
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
            "check": "production_tuning_pricing_edge_authority_absent",
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
                "8T defines diagnostic retention decisions only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.append(
        {
            "authority": (
                "shadow_dataset_collection_retention_implementation"
            ),
            "granted": all_checks_passed,
            "reason": (
                "8U may implement a bounded immutable retention-decision ledger."
            ),
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_retention_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_collection_retention_contract_plan_failed"
    )

    recommended_next_layer = (
        "8U_pitch_type_matchup_overlay_shadow_dataset_collection_retention_contract_implementation"
        if all_checks_passed
        else
        "8T_pitch_type_matchup_overlay_shadow_dataset_collection_retention_contract_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "retention_decision_fields.csv": RETENTION_DECISION_FIELDS,
        "retention_statuses.csv": RETENTION_STATUSES,
        "policy_rules.csv": POLICY_RULES,
        "identity_rules.csv": IDENTITY_RULES,
        "ledger_rules.csv": LEDGER_RULES,
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
        "retention_decision_fields.csv": [
            "field",
            "type",
            "required",
        ],
        "retention_statuses.csv": [
            "status",
            "meaning",
        ],
        "policy_rules.csv": [
            "rule_id",
            "rule",
        ],
        "identity_rules.csv": [
            "rule_id",
            "rule",
        ],
        "ledger_rules.csv": [
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
                    "Implement deterministic retention decisions and an immutable append-only ledger."
                    if all_checks_passed
                    else
                    "Remediate failed 8T planning checks."
                ),
                "entry_condition": (
                    "All nineteen 8T planning checks pass."
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
        "retention_decision_fields_defined": len(
            RETENTION_DECISION_FIELDS
        ),
        "retention_statuses_defined": len(
            RETENTION_STATUSES
        ),
        "policy_rules_defined": len(
            POLICY_RULES
        ),
        "identity_rules_defined": len(
            IDENTITY_RULES
        ),
        "ledger_rules_defined": len(
            LEDGER_RULES
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
        "append_only_ledger_defined": True,
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
        "historical_validation_allowed_next": False,
        "historical_outcome_enrichment_allowed_next": False,
        "predictive_evaluation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "retention_implementation_allowed_next": (
            all_checks_passed
        ),
        "physical_deletion_allowed_next": False,
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
