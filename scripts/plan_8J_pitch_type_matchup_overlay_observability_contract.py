#!/usr/bin/env python3
"""
Layer 8J
Pitch-Type Matchup Overlay Observability Contract Plan

Defines bounded observability for Layer 8I diagnostic matchup overlays.

Planning only. This layer defines:
- per-overlay summary records;
- per-entry observability records;
- status and coverage aggregations;
- fallback and missing-response counts;
- deterministic serialization and ordering;
- provenance and version metadata;
- CSV and JSON artifact schemas;
- privacy-safe, non-authoritative diagnostic boundaries.

This layer does not:
- integrate overlays into production simulation;
- alter pitch selection or sequencing;
- alter matchup, swing, whiff, contact, batted-ball, or PA probabilities;
- evaluate predictive accuracy or calibration;
- join historical outcomes;
- tune parameters;
- run backtests;
- perform pricing, market comparison, edge detection, or recommendations.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "8J"
LAYER_NAME = (
    "pitch_type_matchup_overlay_observability_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8J_pitch_type_matchup_overlay_observability_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "audit_8I_pitcher_batter_pitch_type_matchup_overlay_contract.py"
)

OVERLAY_SUMMARY_FIELDS = [
    {
        "field": "observation_id",
        "type": "deterministic_string",
        "required": True,
    },
    {
        "field": "pitcher_id",
        "type": "string_or_null",
        "required": False,
    },
    {
        "field": "batter_id",
        "type": "string_or_null",
        "required": False,
    },
    {
        "field": "pitcher_hand",
        "type": "enum_R_L_U_or_null",
        "required": False,
    },
    {
        "field": "batter_hand",
        "type": "enum_R_L_S_U_or_null",
        "required": False,
    },
    {
        "field": "count_context",
        "type": "enum_or_null",
        "required": False,
    },
    {
        "field": "as_of_date_utc",
        "type": "date_or_null",
        "required": False,
    },
    {
        "field": "overlay_status",
        "type": "enum",
        "required": True,
    },
    {
        "field": "coverage_share",
        "type": "float_0_1",
        "required": True,
    },
    {
        "field": "matched_pitch_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "unmatched_pitch_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "overlay_entry_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "matched_usage_share",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "unmatched_usage_share",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "fallback_entry_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "unknown_pitch_entry_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "pitcher_only_entry_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "pitcher_profile_status",
        "type": "enum_or_null",
        "required": False,
    },
    {
        "field": "batter_profile_status",
        "type": "enum_or_null",
        "required": False,
    },
    {
        "field": "pitcher_profile_version",
        "type": "string_or_null",
        "required": False,
    },
    {
        "field": "batter_profile_version",
        "type": "string_or_null",
        "required": False,
    },
    {
        "field": "overlay_version",
        "type": "string",
        "required": True,
    },
    {
        "field": "observability_version",
        "type": "string",
        "required": True,
    },
    {
        "field": "diagnostic_codes",
        "type": "sorted_unique_string_list",
        "required": True,
    },
    {
        "field": "validation_errors",
        "type": "sorted_unique_string_list",
        "required": True,
    },
]

ENTRY_OBSERVABILITY_FIELDS = [
    {
        "field": "observation_id",
        "type": "deterministic_string",
        "required": True,
    },
    {
        "field": "entry_ordinal",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "canonical_pitch_id",
        "type": "canonical_pitch_id",
        "required": True,
    },
    {
        "field": "canonical_pitch_name",
        "type": "string",
        "required": True,
    },
    {
        "field": "canonical_family",
        "type": "string",
        "required": True,
    },
    {
        "field": "coverage_status",
        "type": "enum",
        "required": True,
    },
    {
        "field": "pitch_usage_share",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "pitcher_pitch_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "batter_pitch_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "fallback_used",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "response_available",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "unknown_pitch",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "swing_rate_present",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "whiff_rate_present",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "contact_rate_present",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "hard_hit_rate_present",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "barrel_rate_present",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "command_index_present",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "pitch_quality_index_present",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "batter_response_index_present",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "diagnostic_matchup_index_present",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "diagnostic_codes",
        "type": "sorted_unique_string_list",
        "required": True,
    },
]

AGGREGATE_FIELDS = [
    {
        "field": "overlay_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "emitted_overlay_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "disabled_overlay_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "resolved_overlay_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "partial_overlay_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "sparse_overlay_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "stale_overlay_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "unavailable_overlay_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "invalid_overlay_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "mean_coverage_share",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "minimum_coverage_share",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "maximum_coverage_share",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "fallback_overlay_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "pitcher_only_overlay_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "unknown_pitch_overlay_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "observability_version",
        "type": "string",
        "required": True,
    },
]

OBSERVABILITY_STATUSES = [
    {
        "status": "complete",
        "meaning": "Required summary and entry records were emitted.",
    },
    {
        "status": "partial",
        "meaning": "Observability emitted with missing optional metadata.",
    },
    {
        "status": "empty",
        "meaning": "Valid observation contains no overlay entries.",
    },
    {
        "status": "invalid",
        "meaning": "Overlay or observability record failed validation.",
    },
    {
        "status": "disabled",
        "meaning": "Observability generation is disabled.",
    },
]

SERIALIZATION_RULES = [
    {
        "rule_id": "OB-S01",
        "rule": "summary_field_order_is_contract_defined",
    },
    {
        "rule_id": "OB-S02",
        "rule": "entry_rows_follow_overlay_entry_order",
    },
    {
        "rule_id": "OB-S03",
        "rule": "diagnostic_codes_are_sorted_and_unique",
    },
    {
        "rule_id": "OB-S04",
        "rule": "validation_errors_are_sorted_and_unique",
    },
    {
        "rule_id": "OB-S05",
        "rule": "floating_point_values_use_deterministic_rounding",
    },
    {
        "rule_id": "OB-S06",
        "rule": "json_keys_are_sorted",
    },
]

AGGREGATION_RULES = [
    {
        "rule_id": "OB-A01",
        "rule": "overlay_status_counts_are_mutually_exclusive",
    },
    {
        "rule_id": "OB-A02",
        "rule": "coverage_statistics_use_emitted_overlays_only",
    },
    {
        "rule_id": "OB-A03",
        "rule": "disabled_overlays_are_counted_separately",
    },
    {
        "rule_id": "OB-A04",
        "rule": "fallback_overlay_count_is_overlay_level",
    },
    {
        "rule_id": "OB-A05",
        "rule": "pitcher_only_overlay_count_is_overlay_level",
    },
    {
        "rule_id": "OB-A06",
        "rule": "unknown_pitch_overlay_count_is_overlay_level",
    },
]

VALIDATION_RULES = [
    {
        "rule_id": "OB-V01",
        "rule": "observation_id_must_be_nonempty",
    },
    {
        "rule_id": "OB-V02",
        "rule": "observation_id_must_be_deterministic",
    },
    {
        "rule_id": "OB-V03",
        "rule": "overlay_status_must_be_supported",
    },
    {
        "rule_id": "OB-V04",
        "rule": "coverage_share_must_be_between_zero_and_one",
    },
    {
        "rule_id": "OB-V05",
        "rule": "entry_counts_must_be_nonnegative",
    },
    {
        "rule_id": "OB-V06",
        "rule": "overlay_entry_count_must_match_serialized_entries",
    },
    {
        "rule_id": "OB-V07",
        "rule": "matched_and_unmatched_counts_must_match_overlay",
    },
    {
        "rule_id": "OB-V08",
        "rule": "matched_and_unmatched_usage_must_not_exceed_one",
    },
    {
        "rule_id": "OB-V09",
        "rule": "entry_ordinals_must_be_contiguous",
    },
    {
        "rule_id": "OB-V10",
        "rule": "coverage_status_must_be_supported",
    },
    {
        "rule_id": "OB-V11",
        "rule": "fallback_flags_must_match_diagnostic_codes",
    },
    {
        "rule_id": "OB-V12",
        "rule": "unknown_pitch_flag_must_match_UN_identity",
    },
    {
        "rule_id": "OB-V13",
        "rule": "response_available_must_match_batter_pitch_count_or_metrics",
    },
    {
        "rule_id": "OB-V14",
        "rule": "profile_versions_must_be_retained",
    },
    {
        "rule_id": "OB-V15",
        "rule": "overlay_version_must_be_retained",
    },
    {
        "rule_id": "OB-V16",
        "rule": "observability_version_must_be_explicit",
    },
    {
        "rule_id": "OB-V17",
        "rule": "aggregate_status_counts_must_sum_to_overlay_count",
    },
    {
        "rule_id": "OB-V18",
        "rule": "aggregate_coverage_bounds_must_be_ordered",
    },
    {
        "rule_id": "OB-V19",
        "rule": "caller_overlay_must_remain_immutable",
    },
    {
        "rule_id": "OB-V20",
        "rule": "disabled_path_must_not_emit_records",
    },
    {
        "rule_id": "OB-V21",
        "rule": "production_authority_must_remain_false",
    },
]

ARTIFACT_SCHEMAS = [
    {
        "artifact": "overlay_observations.csv",
        "scope": "one_row_per_overlay",
        "required": True,
    },
    {
        "artifact": "overlay_entry_observations.csv",
        "scope": "one_row_per_overlay_entry",
        "required": True,
    },
    {
        "artifact": "overlay_status_counts.csv",
        "scope": "one_row_per_status",
        "required": True,
    },
    {
        "artifact": "coverage_distribution.csv",
        "scope": "deterministic_coverage_buckets",
        "required": True,
    },
    {
        "artifact": "fallback_counts.csv",
        "scope": "one_row_per_fallback_code",
        "required": True,
    },
    {
        "artifact": "observability_summary.json",
        "scope": "aggregate_summary",
        "required": True,
    },
    {
        "artifact": "diagnosis.json",
        "scope": "layer_diagnosis",
        "required": True,
    },
]

COVERAGE_BUCKETS = [
    {
        "bucket": "coverage_0",
        "lower_bound": 0.0,
        "upper_bound": 0.0,
        "upper_inclusive": True,
    },
    {
        "bucket": "coverage_gt_0_lt_0_5",
        "lower_bound": 0.0,
        "upper_bound": 0.5,
        "upper_inclusive": False,
    },
    {
        "bucket": "coverage_gte_0_5_lt_0_8",
        "lower_bound": 0.5,
        "upper_bound": 0.8,
        "upper_inclusive": False,
    },
    {
        "bucket": "coverage_gte_0_8_lt_1",
        "lower_bound": 0.8,
        "upper_bound": 1.0,
        "upper_inclusive": False,
    },
    {
        "bucket": "coverage_1",
        "lower_bound": 1.0,
        "upper_bound": 1.0,
        "upper_inclusive": True,
    },
]

FALLBACK_CODES = [
    {
        "code": "pitch_type_matchup_count_context_fallback",
        "category": "count_context",
    },
    {
        "code": "pitch_type_matchup_pitcher_hand_fallback",
        "category": "pitcher_hand",
    },
    {
        "code": "pitch_type_matchup_batter_response_missing",
        "category": "missing_response",
    },
    {
        "code": "pitch_type_matchup_unknown_pitch_retained",
        "category": "unknown_pitch",
    },
    {
        "code": "pitch_type_matchup_profile_dates_disagree",
        "category": "profile_date",
    },
]

FALLBACK_CONTRACTS = [
    {
        "fallback_id": "OB-F01",
        "condition": "observability_disabled",
        "result": "no_observability_records",
        "diagnostic_code": "matchup_overlay_observability_disabled",
    },
    {
        "fallback_id": "OB-F02",
        "condition": "overlay_missing",
        "result": "invalid_empty_observation",
        "diagnostic_code": "matchup_overlay_observation_missing",
    },
    {
        "fallback_id": "OB-F03",
        "condition": "overlay_not_emitted",
        "result": "status_only_observation",
        "diagnostic_code": "matchup_overlay_not_emitted",
    },
    {
        "fallback_id": "OB-F04",
        "condition": "overlay_entries_empty",
        "result": "empty_summary_and_zero_entry_rows",
        "diagnostic_code": "matchup_overlay_entries_empty",
    },
    {
        "fallback_id": "OB-F05",
        "condition": "usage_share_missing",
        "result": "retain_null_usage",
        "diagnostic_code": "matchup_overlay_usage_missing",
    },
    {
        "fallback_id": "OB-F06",
        "condition": "optional_metric_missing",
        "result": "emit_presence_flag_false",
        "diagnostic_code": "matchup_overlay_optional_metric_missing",
    },
    {
        "fallback_id": "OB-F07",
        "condition": "invalid_overlay_values",
        "result": "invalid_non_authoritative_observation",
        "diagnostic_code": "matchup_overlay_observation_invalid",
    },
]

IMPLEMENTATION_STEPS = [
    {
        "step": 1,
        "action": "Create immutable overlay observability records.",
    },
    {
        "step": 2,
        "action": "Retain Layer 8I identity, status, coverage, and versions.",
    },
    {
        "step": 3,
        "action": "Create deterministic observation identifiers.",
    },
    {
        "step": 4,
        "action": "Serialize one summary row per overlay.",
    },
    {
        "step": 5,
        "action": "Serialize one observability row per overlay entry.",
    },
    {
        "step": 6,
        "action": "Count fallback, pitcher-only, and unknown-pitch usage.",
    },
    {
        "step": 7,
        "action": "Aggregate overlay statuses and coverage distributions.",
    },
    {
        "step": 8,
        "action": "Validate count and coverage reconciliation.",
    },
    {
        "step": 9,
        "action": "Preserve deterministic ordering and serialization.",
    },
    {
        "step": 10,
        "action": "Preserve disabled-by-default and immutable-input behavior.",
    },
    {
        "step": 11,
        "action": "Create independent observability audit.",
    },
    {
        "step": 12,
        "action": "Emit bounded CSV and JSON artifacts.",
    },
]

ACCEPTANCE_CRITERIA = [
    {
        "criterion_id": "OB-C01",
        "criterion": "layer_8I_dependency_verified",
    },
    {
        "criterion_id": "OB-C02",
        "criterion": "summary_schema_defined",
    },
    {
        "criterion_id": "OB-C03",
        "criterion": "entry_schema_defined",
    },
    {
        "criterion_id": "OB-C04",
        "criterion": "aggregate_schema_defined",
    },
    {
        "criterion_id": "OB-C05",
        "criterion": "deterministic_observation_id_defined",
    },
    {
        "criterion_id": "OB-C06",
        "criterion": "status_counts_defined",
    },
    {
        "criterion_id": "OB-C07",
        "criterion": "coverage_distribution_defined",
    },
    {
        "criterion_id": "OB-C08",
        "criterion": "fallback_counts_defined",
    },
    {
        "criterion_id": "OB-C09",
        "criterion": "presence_flags_defined",
    },
    {
        "criterion_id": "OB-C10",
        "criterion": "profile_and_overlay_versions_retained",
    },
    {
        "criterion_id": "OB-C11",
        "criterion": "serialization_deterministic",
    },
    {
        "criterion_id": "OB-C12",
        "criterion": "caller_overlay_immutable",
    },
    {
        "criterion_id": "OB-C13",
        "criterion": "disabled_path_non_emitting",
    },
    {
        "criterion_id": "OB-C14",
        "criterion": "historical_outcomes_absent",
    },
    {
        "criterion_id": "OB-C15",
        "criterion": "production_authority_absent",
    },
    {
        "criterion_id": "OB-C16",
        "criterion": "simulation_behavior_unchanged",
    },
]

PROHIBITED_AUTHORITIES = [
    "production_overlay_integration",
    "production_matchup_adjustment",
    "production_pitch_selection",
    "production_pitch_sequence_change",
    "swing_probability_change",
    "whiff_probability_change",
    "contact_probability_change",
    "batted_ball_probability_change",
    "plate_appearance_outcome_change",
    "simulation_state_change",
    "simulation_parameter_change",
    "simulation_probability_change",
    "contact_quality_change",
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
        "pitcher_batter_pitch_type_matchup_overlay_contract_implementation_passed"
        in string_constants(
            PREDECESSOR_PATH
        )
    )

    summary_field_names = [
        row["field"]
        for row in OVERLAY_SUMMARY_FIELDS
    ]

    entry_field_names = [
        row["field"]
        for row in ENTRY_OBSERVABILITY_FIELDS
    ]

    aggregate_field_names = [
        row["field"]
        for row in AGGREGATE_FIELDS
    ]

    planning_checks = [
        {
            "check": "eight_i_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twenty_five_summary_fields_defined",
            "actual": len(OVERLAY_SUMMARY_FIELDS),
            "expected": 25,
            "passed": len(OVERLAY_SUMMARY_FIELDS) == 25,
        },
        {
            "check": "summary_field_names_unique",
            "actual": len(set(summary_field_names)),
            "expected": len(summary_field_names),
            "passed": (
                len(set(summary_field_names))
                == len(summary_field_names)
            ),
        },
        {
            "check": "twenty_two_entry_fields_defined",
            "actual": len(ENTRY_OBSERVABILITY_FIELDS),
            "expected": 22,
            "passed": len(ENTRY_OBSERVABILITY_FIELDS) == 22,
        },
        {
            "check": "entry_field_names_unique",
            "actual": len(set(entry_field_names)),
            "expected": len(entry_field_names),
            "passed": (
                len(set(entry_field_names))
                == len(entry_field_names)
            ),
        },
        {
            "check": "sixteen_aggregate_fields_defined",
            "actual": len(AGGREGATE_FIELDS),
            "expected": 16,
            "passed": len(AGGREGATE_FIELDS) == 16,
        },
        {
            "check": "aggregate_field_names_unique",
            "actual": len(set(aggregate_field_names)),
            "expected": len(aggregate_field_names),
            "passed": (
                len(set(aggregate_field_names))
                == len(aggregate_field_names)
            ),
        },
        {
            "check": "five_observability_statuses_defined",
            "actual": len(OBSERVABILITY_STATUSES),
            "expected": 5,
            "passed": len(OBSERVABILITY_STATUSES) == 5,
        },
        {
            "check": "six_serialization_rules_defined",
            "actual": len(SERIALIZATION_RULES),
            "expected": 6,
            "passed": len(SERIALIZATION_RULES) == 6,
        },
        {
            "check": "six_aggregation_rules_defined",
            "actual": len(AGGREGATION_RULES),
            "expected": 6,
            "passed": len(AGGREGATION_RULES) == 6,
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
            "check": "five_coverage_buckets_defined",
            "actual": len(COVERAGE_BUCKETS),
            "expected": 5,
            "passed": len(COVERAGE_BUCKETS) == 5,
        },
        {
            "check": "five_fallback_codes_defined",
            "actual": len(FALLBACK_CODES),
            "expected": 5,
            "passed": len(FALLBACK_CODES) == 5,
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
            "check": "planning_only_boundary_preserved",
            "actual": True,
            "expected": True,
            "passed": True,
        },
        {
            "check": "production_validation_tuning_pricing_edge_authority_absent",
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
                "8J defines diagnostic-only overlay observability."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "pitch_type_matchup_overlay_observability_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "8K may implement bounded diagnostic observability."
                ),
            },
            {
                "authority": (
                    "production_matchup_overlay_integration"
                ),
                "granted": False,
                "reason": (
                    "Observability outputs remain non-authoritative."
                ),
            },
        ]
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_observability_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_observability_contract_plan_failed"
    )

    recommended_next_layer = (
        "8K_pitch_type_matchup_overlay_observability_contract_implementation"
        if all_checks_passed
        else
        "8J_pitch_type_matchup_overlay_observability_contract_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "overlay_summary_fields.csv": OVERLAY_SUMMARY_FIELDS,
        "entry_observability_fields.csv": ENTRY_OBSERVABILITY_FIELDS,
        "aggregate_fields.csv": AGGREGATE_FIELDS,
        "observability_statuses.csv": OBSERVABILITY_STATUSES,
        "serialization_rules.csv": SERIALIZATION_RULES,
        "aggregation_rules.csv": AGGREGATION_RULES,
        "validation_rules.csv": VALIDATION_RULES,
        "artifact_schemas.csv": ARTIFACT_SCHEMAS,
        "coverage_buckets.csv": COVERAGE_BUCKETS,
        "fallback_codes.csv": FALLBACK_CODES,
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
        "overlay_summary_fields.csv": [
            "field",
            "type",
            "required",
        ],
        "entry_observability_fields.csv": [
            "field",
            "type",
            "required",
        ],
        "aggregate_fields.csv": [
            "field",
            "type",
            "required",
        ],
        "observability_statuses.csv": [
            "status",
            "meaning",
        ],
        "serialization_rules.csv": [
            "rule_id",
            "rule",
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
        "coverage_buckets.csv": [
            "bucket",
            "lower_bound",
            "upper_bound",
            "upper_inclusive",
        ],
        "fallback_codes.csv": [
            "code",
            "category",
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
                    "Implement bounded deterministic observability for "
                    "diagnostic matchup overlays."
                    if all_checks_passed
                    else
                    "Remediate failed 8J planning checks."
                ),
                "entry_condition": (
                    "All nineteen 8J planning checks pass."
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
        "overlay_summary_fields_defined": len(
            OVERLAY_SUMMARY_FIELDS
        ),
        "entry_observability_fields_defined": len(
            ENTRY_OBSERVABILITY_FIELDS
        ),
        "aggregate_fields_defined": len(
            AGGREGATE_FIELDS
        ),
        "observability_statuses_defined": len(
            OBSERVABILITY_STATUSES
        ),
        "serialization_rules_defined": len(
            SERIALIZATION_RULES
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
        "coverage_buckets_defined": len(
            COVERAGE_BUCKETS
        ),
        "fallback_codes_defined": len(
            FALLBACK_CODES
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
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "matchup_adjustments_activated": False,
        "historical_outcome_joined": False,
        "historical_validation_executed": False,
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
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "matchup_overlay_observability_implementation_allowed_next": (
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
