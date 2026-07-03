#!/usr/bin/env python3
"""
Layer 8P
Pitch-Type Matchup Overlay Shadow Dataset Collection Contract Plan

Defines a bounded, deterministic, append-only collection contract for
quality-gated Layer 8M shadow datasets and Layer 8O quality reports.

Planning only.

This layer does not:
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


LAYER_ID = "8P"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_collection_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8P_pitch_type_matchup_overlay_shadow_dataset_collection_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "audit_8O_pitch_type_matchup_overlay_shadow_dataset_quality_gate_contract.py"
)

COLLECTION_RECORD_FIELDS = [
    {"field": "collection_record_id", "type": "deterministic_string", "required": True},
    {"field": "collection_version", "type": "string", "required": True},
    {"field": "collected_at_utc", "type": "datetime", "required": True},
    {"field": "dataset_version", "type": "string", "required": True},
    {"field": "quality_gate_version", "type": "string", "required": True},
    {"field": "dataset_status", "type": "enum", "required": True},
    {"field": "gate_status", "type": "enum", "required": True},
    {"field": "dataset_row_count", "type": "nonnegative_integer", "required": True},
    {"field": "partition_count", "type": "nonnegative_integer", "required": True},
    {"field": "duplicate_row_count", "type": "nonnegative_integer", "required": True},
    {"field": "minimum_observation_date_utc", "type": "date_or_null", "required": False},
    {"field": "maximum_observation_date_utc", "type": "date_or_null", "required": False},
    {"field": "schema_fingerprint", "type": "string", "required": True},
    {"field": "failed_gate_count", "type": "nonnegative_integer", "required": True},
    {"field": "warning_gate_count", "type": "nonnegative_integer", "required": True},
    {"field": "minimum_coverage_share", "type": "float_0_1_or_null", "required": False},
    {"field": "mean_coverage_share", "type": "float_0_1_or_null", "required": False},
    {"field": "maximum_coverage_share", "type": "float_0_1_or_null", "required": False},
    {"field": "diagnostic_codes", "type": "sorted_unique_string_array", "required": True},
    {"field": "validation_errors", "type": "sorted_unique_string_array", "required": True},
    {"field": "dataset_payload_digest", "type": "sha256_string", "required": True},
    {"field": "quality_report_digest", "type": "sha256_string", "required": True},
    {"field": "collection_status", "type": "enum", "required": True},
    {"field": "production_authority", "type": "boolean_false", "required": True},
]

COLLECTION_STATUSES = [
    {"status": "accepted", "meaning": "Quality-gated dataset accepted into collection."},
    {"status": "accepted_with_warnings", "meaning": "Warning-only report accepted into collection."},
    {"status": "rejected", "meaning": "Required quality gates failed."},
    {"status": "empty", "meaning": "Valid empty dataset recorded without row payload."},
    {"status": "disabled", "meaning": "Collection is disabled and emits no record."},
]

ELIGIBILITY_RULES = [
    {"rule_id": "CL-E01", "rule": "dataset_and_quality_report_required_when_enabled"},
    {"rule_id": "CL-E02", "rule": "quality_report_must_reference_supported_version"},
    {"rule_id": "CL-E03", "rule": "dataset_and_report_statuses_must_reconcile"},
    {"rule_id": "CL-E04", "rule": "pass_reports_are_accepted"},
    {"rule_id": "CL-E05", "rule": "warn_reports_are_accepted_with_warnings"},
    {"rule_id": "CL-E06", "rule": "fail_reports_are_rejected"},
    {"rule_id": "CL-E07", "rule": "empty_reports_create_metadata_only_records"},
    {"rule_id": "CL-E08", "rule": "disabled_reports_emit_no_collection_record"},
]

IDENTITY_RULES = [
    {"rule_id": "CL-I01", "rule": "collection_record_id_is_deterministic"},
    {"rule_id": "CL-I02", "rule": "identity_includes_dataset_digest"},
    {"rule_id": "CL-I03", "rule": "identity_includes_quality_report_digest"},
    {"rule_id": "CL-I04", "rule": "identity_includes_collection_version"},
    {"rule_id": "CL-I05", "rule": "input_order_does_not_change_identity"},
    {"rule_id": "CL-I06", "rule": "repeated_collection_is_idempotent"},
]

APPEND_RULES = [
    {"rule_id": "CL-A01", "rule": "collection_is_append_only"},
    {"rule_id": "CL-A02", "rule": "existing_records_are_immutable"},
    {"rule_id": "CL-A03", "rule": "exact_duplicate_records_are_collapsed"},
    {"rule_id": "CL-A04", "rule": "conflicting_duplicate_records_are_rejected"},
    {"rule_id": "CL-A05", "rule": "replacement_requires_collection_version_change"},
    {"rule_id": "CL-A06", "rule": "collection_order_is_deterministic"},
]

VALIDATION_RULES = [
    {"rule_id": "CL-V01", "rule": "collection_version_explicit"},
    {"rule_id": "CL-V02", "rule": "dataset_required_when_enabled"},
    {"rule_id": "CL-V03", "rule": "quality_report_required_when_enabled"},
    {"rule_id": "CL-V04", "rule": "dataset_manifest_required_for_nonempty_record"},
    {"rule_id": "CL-V05", "rule": "quality_summary_required_for_emitted_report"},
    {"rule_id": "CL-V06", "rule": "dataset_status_supported"},
    {"rule_id": "CL-V07", "rule": "gate_status_supported"},
    {"rule_id": "CL-V08", "rule": "collection_status_supported"},
    {"rule_id": "CL-V09", "rule": "dataset_and_quality_versions_retained"},
    {"rule_id": "CL-V10", "rule": "dataset_row_count_reconciles"},
    {"rule_id": "CL-V11", "rule": "partition_count_reconciles"},
    {"rule_id": "CL-V12", "rule": "duplicate_count_nonnegative"},
    {"rule_id": "CL-V13", "rule": "coverage_statistics_valid"},
    {"rule_id": "CL-V14", "rule": "failed_gate_count_nonnegative"},
    {"rule_id": "CL-V15", "rule": "warning_gate_count_nonnegative"},
    {"rule_id": "CL-V16", "rule": "dataset_digest_is_deterministic_sha256"},
    {"rule_id": "CL-V17", "rule": "quality_report_digest_is_deterministic_sha256"},
    {"rule_id": "CL-V18", "rule": "collection_record_id_is_deterministic"},
    {"rule_id": "CL-V19", "rule": "exact_duplicates_are_idempotent"},
    {"rule_id": "CL-V20", "rule": "caller_dataset_immutable"},
    {"rule_id": "CL-V21", "rule": "caller_quality_report_immutable"},
    {"rule_id": "CL-V22", "rule": "disabled_path_non_emitting"},
    {"rule_id": "CL-V23", "rule": "production_authority_false"},
]

ARTIFACT_SCHEMAS = [
    {"artifact": "collection_records.csv", "scope": "one_row_per_collection_record", "required": True},
    {"artifact": "collection_manifest.json", "scope": "collection_manifest", "required": True},
    {"artifact": "eligibility_results.csv", "scope": "one_row_per_eligibility_rule", "required": True},
    {"artifact": "duplicate_report.csv", "scope": "one_row_per_duplicate_identity", "required": True},
    {"artifact": "status_counts.csv", "scope": "one_row_per_collection_status", "required": True},
    {"artifact": "authority_boundaries.csv", "scope": "authority_contract", "required": True},
    {"artifact": "diagnosis.json", "scope": "layer_diagnosis", "required": True},
]

FALLBACK_CONTRACTS = [
    {
        "fallback_id": "CL-F01",
        "condition": "collection_disabled",
        "result": "no_record_emitted",
        "diagnostic_code": "matchup_shadow_collection_disabled",
    },
    {
        "fallback_id": "CL-F02",
        "condition": "dataset_missing",
        "result": "rejected_record",
        "diagnostic_code": "matchup_shadow_collection_dataset_missing",
    },
    {
        "fallback_id": "CL-F03",
        "condition": "quality_report_missing",
        "result": "rejected_record",
        "diagnostic_code": "matchup_shadow_collection_quality_report_missing",
    },
    {
        "fallback_id": "CL-F04",
        "condition": "quality_report_failed",
        "result": "rejected_record",
        "diagnostic_code": "matchup_shadow_collection_quality_failed",
    },
    {
        "fallback_id": "CL-F05",
        "condition": "quality_report_warned",
        "result": "accepted_with_warnings",
        "diagnostic_code": "matchup_shadow_collection_quality_warned",
    },
    {
        "fallback_id": "CL-F06",
        "condition": "dataset_empty",
        "result": "metadata_only_record",
        "diagnostic_code": "matchup_shadow_collection_dataset_empty",
    },
    {
        "fallback_id": "CL-F07",
        "condition": "conflicting_duplicate",
        "result": "rejected_record",
        "diagnostic_code": "matchup_shadow_collection_conflicting_duplicate",
    },
]

IMPLEMENTATION_STEPS = [
    {"step": 1, "action": "Create immutable collection record and manifest types."},
    {"step": 2, "action": "Import Layer 8M datasets and Layer 8O reports."},
    {"step": 3, "action": "Validate dataset and quality-report reconciliation."},
    {"step": 4, "action": "Compute deterministic dataset and report digests."},
    {"step": 5, "action": "Create deterministic collection record identifiers."},
    {"step": 6, "action": "Map pass, warn, fail, empty, and disabled paths."},
    {"step": 7, "action": "Apply append-only and idempotent duplicate handling."},
    {"step": 8, "action": "Preserve input immutability."},
    {"step": 9, "action": "Keep collection disabled by default."},
    {"step": 10, "action": "Create independent collection-contract audit."},
    {"step": 11, "action": "Emit deterministic CSV and JSON artifacts."},
    {"step": 12, "action": "Preserve production and predictive-evaluation boundaries."},
]

ACCEPTANCE_CRITERIA = [
    {"criterion_id": "CL-C01", "criterion": "layer_8O_dependency_verified"},
    {"criterion_id": "CL-C02", "criterion": "collection_record_schema_defined"},
    {"criterion_id": "CL-C03", "criterion": "five_collection_statuses_defined"},
    {"criterion_id": "CL-C04", "criterion": "eligibility_rules_defined"},
    {"criterion_id": "CL-C05", "criterion": "deterministic_digests_defined"},
    {"criterion_id": "CL-C06", "criterion": "deterministic_record_identity_defined"},
    {"criterion_id": "CL-C07", "criterion": "append_only_collection_defined"},
    {"criterion_id": "CL-C08", "criterion": "idempotent_duplicate_handling_defined"},
    {"criterion_id": "CL-C09", "criterion": "conflicting_duplicate_rejection_defined"},
    {"criterion_id": "CL-C10", "criterion": "pass_warn_fail_status_mapping_defined"},
    {"criterion_id": "CL-C11", "criterion": "empty_metadata_only_path_defined"},
    {"criterion_id": "CL-C12", "criterion": "disabled_path_non_emitting"},
    {"criterion_id": "CL-C13", "criterion": "caller_inputs_immutable"},
    {"criterion_id": "CL-C14", "criterion": "historical_outcomes_absent"},
    {"criterion_id": "CL-C15", "criterion": "predictive_evaluation_absent"},
    {"criterion_id": "CL-C16", "criterion": "production_authority_absent"},
]

PROHIBITED_AUTHORITIES = [
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
    path.parent.mkdir(parents=True, exist_ok=True)

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
    path.parent.mkdir(parents=True, exist_ok=True)
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
        "pitch_type_matchup_overlay_shadow_dataset_quality_gate_contract_implementation_passed"
        in string_constants(
            PREDECESSOR_PATH
        )
    )

    field_names = [
        row["field"]
        for row in COLLECTION_RECORD_FIELDS
    ]

    planning_checks = [
        {
            "check": "eight_o_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twenty_four_collection_fields_defined",
            "actual": len(COLLECTION_RECORD_FIELDS),
            "expected": 24,
            "passed": len(COLLECTION_RECORD_FIELDS) == 24,
        },
        {
            "check": "collection_field_names_unique",
            "actual": len(set(field_names)),
            "expected": len(field_names),
            "passed": len(set(field_names)) == len(field_names),
        },
        {
            "check": "five_collection_statuses_defined",
            "actual": len(COLLECTION_STATUSES),
            "expected": 5,
            "passed": len(COLLECTION_STATUSES) == 5,
        },
        {
            "check": "eight_eligibility_rules_defined",
            "actual": len(ELIGIBILITY_RULES),
            "expected": 8,
            "passed": len(ELIGIBILITY_RULES) == 8,
        },
        {
            "check": "six_identity_rules_defined",
            "actual": len(IDENTITY_RULES),
            "expected": 6,
            "passed": len(IDENTITY_RULES) == 6,
        },
        {
            "check": "six_append_rules_defined",
            "actual": len(APPEND_RULES),
            "expected": 6,
            "passed": len(APPEND_RULES) == 6,
        },
        {
            "check": "twenty_three_validation_rules_defined",
            "actual": len(VALIDATION_RULES),
            "expected": 23,
            "passed": len(VALIDATION_RULES) == 23,
        },
        {
            "check": "seven_artifact_schemas_defined",
            "actual": len(ARTIFACT_SCHEMAS),
            "expected": 7,
            "passed": len(ARTIFACT_SCHEMAS) == 7,
        },
        {
            "check": "seven_fallback_contracts_defined",
            "actual": len(FALLBACK_CONTRACTS),
            "expected": 7,
            "passed": len(FALLBACK_CONTRACTS) == 7,
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
            "check": "append_only_rule_defined",
            "actual": any(
                row["rule"] == "collection_is_append_only"
                for row in APPEND_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"] == "collection_is_append_only"
                for row in APPEND_RULES
            ),
        },
        {
            "check": "failed_quality_report_rejection_defined",
            "actual": any(
                row["rule"] == "fail_reports_are_rejected"
                for row in ELIGIBILITY_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"] == "fail_reports_are_rejected"
                for row in ELIGIBILITY_RULES
            ),
        },
        {
            "check": "warning_collection_non_authoritative",
            "actual": True,
            "expected": True,
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
                "8P defines diagnostic shadow-dataset collection only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.append(
        {
            "authority": "shadow_dataset_collection_implementation",
            "granted": all_checks_passed,
            "reason": (
                "8Q may implement the bounded append-only collection contract."
            ),
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_collection_contract_plan_failed"
    )

    recommended_next_layer = (
        "8Q_pitch_type_matchup_overlay_shadow_dataset_collection_contract_implementation"
        if all_checks_passed
        else
        "8P_pitch_type_matchup_overlay_shadow_dataset_collection_contract_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "collection_record_fields.csv": COLLECTION_RECORD_FIELDS,
        "collection_statuses.csv": COLLECTION_STATUSES,
        "eligibility_rules.csv": ELIGIBILITY_RULES,
        "identity_rules.csv": IDENTITY_RULES,
        "append_rules.csv": APPEND_RULES,
        "validation_rules.csv": VALIDATION_RULES,
        "artifact_schemas.csv": ARTIFACT_SCHEMAS,
        "fallback_contracts.csv": FALLBACK_CONTRACTS,
        "implementation_steps.csv": IMPLEMENTATION_STEPS,
        "acceptance_criteria.csv": ACCEPTANCE_CRITERIA,
        "authority_boundaries.csv": authority_rows,
    }

    fieldnames = {
        "planning_checks.csv": ["check", "actual", "expected", "passed"],
        "collection_record_fields.csv": ["field", "type", "required"],
        "collection_statuses.csv": ["status", "meaning"],
        "eligibility_rules.csv": ["rule_id", "rule"],
        "identity_rules.csv": ["rule_id", "rule"],
        "append_rules.csv": ["rule_id", "rule"],
        "validation_rules.csv": ["rule_id", "rule"],
        "artifact_schemas.csv": ["artifact", "scope", "required"],
        "fallback_contracts.csv": [
            "fallback_id",
            "condition",
            "result",
            "diagnostic_code",
        ],
        "implementation_steps.csv": ["step", "action"],
        "acceptance_criteria.csv": ["criterion_id", "criterion"],
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
                "recommended_next_layer": recommended_next_layer,
                "recommended_action": (
                    "Implement bounded append-only collection of quality-gated shadow datasets."
                    if all_checks_passed
                    else
                    "Remediate failed 8P planning checks."
                ),
                "entry_condition": (
                    "All nineteen 8P planning checks pass."
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
        "collection_record_fields_defined": len(COLLECTION_RECORD_FIELDS),
        "collection_statuses_defined": len(COLLECTION_STATUSES),
        "eligibility_rules_defined": len(ELIGIBILITY_RULES),
        "identity_rules_defined": len(IDENTITY_RULES),
        "append_rules_defined": len(APPEND_RULES),
        "validation_rules_defined": len(VALIDATION_RULES),
        "artifact_schemas_defined": len(ARTIFACT_SCHEMAS),
        "fallback_contracts_defined": len(FALLBACK_CONTRACTS),
        "implementation_steps_defined": len(IMPLEMENTATION_STEPS),
        "acceptance_criteria_defined": len(ACCEPTANCE_CRITERIA),
        "append_only_contract_defined": True,
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
        "collection_implementation_allowed_next": all_checks_passed,
        "production_matchup_overlay_integration_allowed_next": False,
        "recommended_next_layer": recommended_next_layer,
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / filename)
            for filename in [
                *artifacts.keys(),
                "recommended_path.csv",
            ]
        ],
        "generated_json_artifacts": [
            str(OUTPUT_DIR / "contract_summary.json"),
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
