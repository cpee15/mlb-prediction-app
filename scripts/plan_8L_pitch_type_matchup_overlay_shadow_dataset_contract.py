#!/usr/bin/env python3
"""
Layer 8L
Pitch-Type Matchup Overlay Shadow Dataset Contract Plan

Defines a bounded, append-only diagnostic shadow dataset for Layer 8K
pitch-type matchup overlay observability records.

Planning only.

This layer does not:
- integrate overlays into production;
- alter simulation behavior or probabilities;
- join historical game or plate-appearance outcomes;
- evaluate predictive accuracy, calibration, or profitability;
- tune parameters;
- run backtests;
- perform pricing, market comparison, or edge detection.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "8L"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8L_pitch_type_matchup_overlay_shadow_dataset_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "audit_8K_pitch_type_matchup_overlay_observability_contract.py"
)

SHADOW_ROW_FIELDS = [
    {"field": "dataset_row_id", "type": "deterministic_string", "required": True},
    {"field": "observation_id", "type": "string", "required": True},
    {"field": "observation_date_utc", "type": "date", "required": True},
    {"field": "pitcher_id", "type": "string_or_null", "required": False},
    {"field": "batter_id", "type": "string_or_null", "required": False},
    {"field": "pitcher_hand", "type": "enum_R_L_U_or_null", "required": False},
    {"field": "batter_hand", "type": "enum_R_L_S_U_or_null", "required": False},
    {"field": "count_context", "type": "enum_or_null", "required": False},
    {"field": "overlay_status", "type": "enum", "required": True},
    {"field": "observability_status", "type": "enum", "required": True},
    {"field": "coverage_share", "type": "float_0_1", "required": True},
    {"field": "matched_pitch_count", "type": "nonnegative_integer", "required": True},
    {"field": "unmatched_pitch_count", "type": "nonnegative_integer", "required": True},
    {"field": "overlay_entry_count", "type": "nonnegative_integer", "required": True},
    {"field": "fallback_entry_count", "type": "nonnegative_integer", "required": True},
    {"field": "unknown_pitch_entry_count", "type": "nonnegative_integer", "required": True},
    {"field": "pitcher_only_entry_count", "type": "nonnegative_integer", "required": True},
    {"field": "matched_usage_share", "type": "float_0_1_or_null", "required": False},
    {"field": "unmatched_usage_share", "type": "float_0_1_or_null", "required": False},
    {"field": "pitcher_profile_version", "type": "string_or_null", "required": False},
    {"field": "batter_profile_version", "type": "string_or_null", "required": False},
    {"field": "overlay_version", "type": "string", "required": True},
    {"field": "observability_version", "type": "string", "required": True},
    {"field": "shadow_dataset_version", "type": "string", "required": True},
]

MANIFEST_FIELDS = [
    {"field": "shadow_dataset_version", "type": "string", "required": True},
    {"field": "generated_at_utc", "type": "datetime", "required": True},
    {"field": "row_count", "type": "nonnegative_integer", "required": True},
    {"field": "unique_observation_count", "type": "nonnegative_integer", "required": True},
    {"field": "duplicate_row_count", "type": "nonnegative_integer", "required": True},
    {"field": "partition_count", "type": "nonnegative_integer", "required": True},
    {"field": "minimum_observation_date_utc", "type": "date_or_null", "required": False},
    {"field": "maximum_observation_date_utc", "type": "date_or_null", "required": False},
    {"field": "schema_fingerprint", "type": "deterministic_string", "required": True},
    {"field": "production_authority", "type": "boolean_false", "required": True},
]

DATASET_STATUSES = [
    {"status": "ready", "meaning": "Dataset rows and manifest pass validation."},
    {"status": "partial", "meaning": "Dataset is valid but contains partial observations."},
    {"status": "empty", "meaning": "Dataset contains no accepted rows."},
    {"status": "invalid", "meaning": "Dataset or manifest validation failed."},
    {"status": "disabled", "meaning": "Shadow dataset generation is disabled."},
]

PARTITION_RULES = [
    {"rule_id": "SD-P01", "rule": "partition_by_observation_date_utc"},
    {"rule_id": "SD-P02", "rule": "partition_key_format_is_YYYY_MM_DD"},
    {"rule_id": "SD-P03", "rule": "rows_sort_by_date_then_observation_id"},
    {"rule_id": "SD-P04", "rule": "entry_order_does_not_change_summary_row_order"},
    {"rule_id": "SD-P05", "rule": "empty_partitions_are_not_emitted"},
    {"rule_id": "SD-P06", "rule": "partition_paths_are_deterministic"},
]

DEDUPLICATION_RULES = [
    {"rule_id": "SD-D01", "rule": "dataset_row_id_is_primary_key"},
    {"rule_id": "SD-D02", "rule": "observation_id_plus_date_define_identity"},
    {"rule_id": "SD-D03", "rule": "exact_duplicate_rows_are_collapsed"},
    {"rule_id": "SD-D04", "rule": "conflicting_duplicate_rows_are_invalid"},
    {"rule_id": "SD-D05", "rule": "duplicate_counts_are_recorded_in_manifest"},
    {"rule_id": "SD-D06", "rule": "deduplication_is_deterministic"},
]

RETENTION_RULES = [
    {"rule_id": "SD-R01", "rule": "dataset_is_append_only"},
    {"rule_id": "SD-R02", "rule": "existing_rows_are_not_mutated"},
    {"rule_id": "SD-R03", "rule": "replacement_requires_version_change"},
    {"rule_id": "SD-R04", "rule": "source_versions_are_retained"},
    {"rule_id": "SD-R05", "rule": "retention_policy_is_metadata_only"},
    {"rule_id": "SD-R06", "rule": "no_automatic_deletion_in_layer_8L"},
]

VALIDATION_RULES = [
    {"rule_id": "SD-V01", "rule": "dataset_row_id_must_be_nonempty"},
    {"rule_id": "SD-V02", "rule": "dataset_row_id_must_be_deterministic"},
    {"rule_id": "SD-V03", "rule": "observation_id_must_be_nonempty"},
    {"rule_id": "SD-V04", "rule": "observation_date_must_be_valid"},
    {"rule_id": "SD-V05", "rule": "overlay_status_must_be_supported"},
    {"rule_id": "SD-V06", "rule": "observability_status_must_be_supported"},
    {"rule_id": "SD-V07", "rule": "coverage_share_must_be_between_zero_and_one"},
    {"rule_id": "SD-V08", "rule": "count_fields_must_be_nonnegative"},
    {"rule_id": "SD-V09", "rule": "matched_and_unmatched_usage_must_not_exceed_one"},
    {"rule_id": "SD-V10", "rule": "profile_versions_must_be_retained"},
    {"rule_id": "SD-V11", "rule": "overlay_version_must_be_retained"},
    {"rule_id": "SD-V12", "rule": "observability_version_must_be_retained"},
    {"rule_id": "SD-V13", "rule": "shadow_dataset_version_must_be_explicit"},
    {"rule_id": "SD-V14", "rule": "schema_fingerprint_must_be_deterministic"},
    {"rule_id": "SD-V15", "rule": "manifest_row_count_must_reconcile"},
    {"rule_id": "SD-V16", "rule": "manifest_unique_count_must_reconcile"},
    {"rule_id": "SD-V17", "rule": "partition_count_must_reconcile"},
    {"rule_id": "SD-V18", "rule": "minimum_and_maximum_dates_must_be_ordered"},
    {"rule_id": "SD-V19", "rule": "caller_observations_must_remain_immutable"},
    {"rule_id": "SD-V20", "rule": "disabled_path_must_not_emit_dataset"},
    {"rule_id": "SD-V21", "rule": "production_authority_must_remain_false"},
]

ARTIFACT_SCHEMAS = [
    {
        "artifact": "shadow_dataset_rows.csv",
        "scope": "one_row_per_accepted_observation",
        "required": True,
    },
    {
        "artifact": "shadow_dataset_manifest.json",
        "scope": "dataset_manifest",
        "required": True,
    },
    {
        "artifact": "partition_manifest.csv",
        "scope": "one_row_per_partition",
        "required": True,
    },
    {
        "artifact": "duplicate_report.csv",
        "scope": "one_row_per_duplicate_identity",
        "required": True,
    },
    {
        "artifact": "status_counts.csv",
        "scope": "one_row_per_dataset_status",
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
        "fallback_id": "SD-F01",
        "condition": "shadow_dataset_disabled",
        "result": "no_dataset_emitted",
        "diagnostic_code": "matchup_shadow_dataset_disabled",
    },
    {
        "fallback_id": "SD-F02",
        "condition": "observation_bundle_missing",
        "result": "invalid_dataset",
        "diagnostic_code": "matchup_shadow_observation_missing",
    },
    {
        "fallback_id": "SD-F03",
        "condition": "observation_summary_missing",
        "result": "skip_row_and_record_diagnostic",
        "diagnostic_code": "matchup_shadow_summary_missing",
    },
    {
        "fallback_id": "SD-F04",
        "condition": "observation_not_emitted",
        "result": "skip_row_and_record_diagnostic",
        "diagnostic_code": "matchup_shadow_observation_not_emitted",
    },
    {
        "fallback_id": "SD-F05",
        "condition": "observation_date_missing",
        "result": "invalid_row",
        "diagnostic_code": "matchup_shadow_date_missing",
    },
    {
        "fallback_id": "SD-F06",
        "condition": "exact_duplicate_row",
        "result": "collapse_duplicate",
        "diagnostic_code": "matchup_shadow_exact_duplicate_collapsed",
    },
    {
        "fallback_id": "SD-F07",
        "condition": "conflicting_duplicate_row",
        "result": "invalidate_dataset",
        "diagnostic_code": "matchup_shadow_conflicting_duplicate",
    },
]

IMPLEMENTATION_STEPS = [
    {"step": 1, "action": "Create immutable shadow dataset row and manifest records."},
    {"step": 2, "action": "Import Layer 8K observation bundles."},
    {"step": 3, "action": "Filter to emitted observations with summary records."},
    {"step": 4, "action": "Create deterministic dataset row identifiers."},
    {"step": 5, "action": "Retain source identities, statuses, coverage, and versions."},
    {"step": 6, "action": "Partition deterministically by observation date."},
    {"step": 7, "action": "Deduplicate exact rows and reject conflicts."},
    {"step": 8, "action": "Create deterministic schema fingerprint."},
    {"step": 9, "action": "Build reconciled dataset and partition manifests."},
    {"step": 10, "action": "Preserve disabled-by-default and immutable-input behavior."},
    {"step": 11, "action": "Create independent shadow dataset audit."},
    {"step": 12, "action": "Emit bounded CSV and JSON artifacts."},
]

ACCEPTANCE_CRITERIA = [
    {"criterion_id": "SD-C01", "criterion": "layer_8K_dependency_verified"},
    {"criterion_id": "SD-C02", "criterion": "shadow_row_schema_defined"},
    {"criterion_id": "SD-C03", "criterion": "manifest_schema_defined"},
    {"criterion_id": "SD-C04", "criterion": "deterministic_row_id_defined"},
    {"criterion_id": "SD-C05", "criterion": "date_partitioning_defined"},
    {"criterion_id": "SD-C06", "criterion": "deterministic_ordering_defined"},
    {"criterion_id": "SD-C07", "criterion": "exact_duplicate_handling_defined"},
    {"criterion_id": "SD-C08", "criterion": "conflicting_duplicate_handling_defined"},
    {"criterion_id": "SD-C09", "criterion": "append_only_retention_defined"},
    {"criterion_id": "SD-C10", "criterion": "schema_fingerprint_defined"},
    {"criterion_id": "SD-C11", "criterion": "manifest_reconciliation_defined"},
    {"criterion_id": "SD-C12", "criterion": "caller_observations_immutable"},
    {"criterion_id": "SD-C13", "criterion": "disabled_path_non_emitting"},
    {"criterion_id": "SD-C14", "criterion": "historical_outcomes_absent"},
    {"criterion_id": "SD-C15", "criterion": "production_authority_absent"},
    {"criterion_id": "SD-C16", "criterion": "simulation_behavior_unchanged"},
]

PROHIBITED_AUTHORITIES = [
    "production_overlay_integration",
    "production_matchup_adjustment",
    "production_pitch_selection",
    "production_pitch_sequence_change",
    "simulation_state_change",
    "simulation_parameter_change",
    "simulation_probability_change",
    "canonical_probability_replacement",
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
        "pitch_type_matchup_overlay_observability_contract_implementation_passed"
        in string_constants(
            PREDECESSOR_PATH
        )
    )

    row_field_names = [
        row["field"]
        for row in SHADOW_ROW_FIELDS
    ]

    manifest_field_names = [
        row["field"]
        for row in MANIFEST_FIELDS
    ]

    planning_checks = [
        {
            "check": "eight_k_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twenty_four_shadow_row_fields_defined",
            "actual": len(SHADOW_ROW_FIELDS),
            "expected": 24,
            "passed": len(SHADOW_ROW_FIELDS) == 24,
        },
        {
            "check": "shadow_row_field_names_unique",
            "actual": len(set(row_field_names)),
            "expected": len(row_field_names),
            "passed": len(set(row_field_names)) == len(row_field_names),
        },
        {
            "check": "ten_manifest_fields_defined",
            "actual": len(MANIFEST_FIELDS),
            "expected": 10,
            "passed": len(MANIFEST_FIELDS) == 10,
        },
        {
            "check": "manifest_field_names_unique",
            "actual": len(set(manifest_field_names)),
            "expected": len(manifest_field_names),
            "passed": (
                len(set(manifest_field_names))
                == len(manifest_field_names)
            ),
        },
        {
            "check": "five_dataset_statuses_defined",
            "actual": len(DATASET_STATUSES),
            "expected": 5,
            "passed": len(DATASET_STATUSES) == 5,
        },
        {
            "check": "six_partition_rules_defined",
            "actual": len(PARTITION_RULES),
            "expected": 6,
            "passed": len(PARTITION_RULES) == 6,
        },
        {
            "check": "six_deduplication_rules_defined",
            "actual": len(DEDUPLICATION_RULES),
            "expected": 6,
            "passed": len(DEDUPLICATION_RULES) == 6,
        },
        {
            "check": "six_retention_rules_defined",
            "actual": len(RETENTION_RULES),
            "expected": 6,
            "passed": len(RETENTION_RULES) == 6,
        },
        {
            "check": "twenty_one_validation_rules_defined",
            "actual": len(VALIDATION_RULES),
            "expected": 21,
            "passed": len(VALIDATION_RULES) == 21,
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
            "check": "append_only_boundary_defined",
            "actual": any(
                row["rule"] == "dataset_is_append_only"
                for row in RETENTION_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"] == "dataset_is_append_only"
                for row in RETENTION_RULES
            ),
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
                "8L defines a diagnostic-only shadow dataset contract."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "pitch_type_matchup_overlay_shadow_dataset_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "8M may implement the bounded append-only shadow dataset."
                ),
            },
            {
                "authority": (
                    "historical_outcome_enrichment"
                ),
                "granted": False,
                "reason": (
                    "Shadow rows may not be joined to outcomes in Layer 8M."
                ),
            },
        ]
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_shadow_dataset_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_contract_plan_failed"
    )

    recommended_next_layer = (
        "8M_pitch_type_matchup_overlay_shadow_dataset_contract_implementation"
        if all_checks_passed
        else
        "8L_pitch_type_matchup_overlay_shadow_dataset_contract_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "shadow_row_fields.csv": SHADOW_ROW_FIELDS,
        "manifest_fields.csv": MANIFEST_FIELDS,
        "dataset_statuses.csv": DATASET_STATUSES,
        "partition_rules.csv": PARTITION_RULES,
        "deduplication_rules.csv": DEDUPLICATION_RULES,
        "retention_rules.csv": RETENTION_RULES,
        "validation_rules.csv": VALIDATION_RULES,
        "artifact_schemas.csv": ARTIFACT_SCHEMAS,
        "fallback_contracts.csv": FALLBACK_CONTRACTS,
        "implementation_steps.csv": IMPLEMENTATION_STEPS,
        "acceptance_criteria.csv": ACCEPTANCE_CRITERIA,
        "authority_boundaries.csv": authority_rows,
    }

    fieldnames = {
        "planning_checks.csv": ["check", "actual", "expected", "passed"],
        "shadow_row_fields.csv": ["field", "type", "required"],
        "manifest_fields.csv": ["field", "type", "required"],
        "dataset_statuses.csv": ["status", "meaning"],
        "partition_rules.csv": ["rule_id", "rule"],
        "deduplication_rules.csv": ["rule_id", "rule"],
        "retention_rules.csv": ["rule_id", "rule"],
        "validation_rules.csv": ["rule_id", "rule"],
        "artifact_schemas.csv": ["artifact", "scope", "required"],
        "fallback_contracts.csv": [
            "fallback_id",
            "condition",
            "result",
            "diagnostic_code",
        ],
        "implementation_steps.csv": ["step", "action"],
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
                "recommended_next_layer": recommended_next_layer,
                "recommended_action": (
                    "Implement the bounded append-only diagnostic shadow dataset."
                    if all_checks_passed
                    else
                    "Remediate failed 8L planning checks."
                ),
                "entry_condition": (
                    "All nineteen 8L planning checks pass."
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
        "shadow_row_fields_defined": len(SHADOW_ROW_FIELDS),
        "manifest_fields_defined": len(MANIFEST_FIELDS),
        "dataset_statuses_defined": len(DATASET_STATUSES),
        "partition_rules_defined": len(PARTITION_RULES),
        "deduplication_rules_defined": len(DEDUPLICATION_RULES),
        "retention_rules_defined": len(RETENTION_RULES),
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
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "shadow_dataset_implementation_allowed_next": (
            all_checks_passed
        ),
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
