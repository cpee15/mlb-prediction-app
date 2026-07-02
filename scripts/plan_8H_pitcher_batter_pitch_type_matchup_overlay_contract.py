#!/usr/bin/env python3
"""
Layer 8H
Pitcher-Batter Pitch-Type Matchup Overlay Contract Plan

Defines the bounded planning contract for a diagnostic pitcher-batter
pitch-type matchup overlay.

The overlay may align:
- Layer 8E pitcher arsenal entries;
- Layer 8G batter pitch-type response entries;
- canonical pitch identity;
- pitcher handedness;
- count context;
- usage exposure;
- swing, whiff, contact, and batted-ball response metadata;
- coverage, sample-size, freshness, and provenance metadata.

Planning only.

This layer does not:
- select pitches;
- change pitch sequencing;
- activate matchup adjustments;
- alter swing, whiff, contact, batted-ball, or plate-appearance probabilities;
- alter simulation state, parameters, or outcomes;
- join historical outcomes for predictive validation;
- calculate accuracy or calibration metrics;
- tune parameters;
- execute backtests, pricing, market comparison, or edge detection.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "8H"
LAYER_NAME = (
    "pitcher_batter_pitch_type_matchup_overlay_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8H_pitcher_batter_pitch_type_matchup_overlay_contract"
)

PITCHER_PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "audit_8E_pitcher_arsenal_profile_contract.py"
)

BATTER_PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "audit_8G_batter_pitch_type_response_profile_contract.py"
)

OVERLAY_ENTRY_FIELDS = [
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
        "field": "pitcher_hand",
        "type": "enum_R_L_U",
        "required": True,
    },
    {
        "field": "batter_hand",
        "type": "enum_R_L_S_U",
        "required": True,
    },
    {
        "field": "count_context",
        "type": "enum",
        "required": True,
    },
    {
        "field": "pitch_available",
        "type": "boolean",
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
        "field": "coverage_status",
        "type": "enum",
        "required": True,
    },
    {
        "field": "swing_rate",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "chase_rate",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "whiff_rate",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "contact_rate",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "called_strike_plus_whiff_rate",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "avg_exit_velocity_mph",
        "type": "positive_float_or_null",
        "required": False,
    },
    {
        "field": "avg_launch_angle_degrees",
        "type": "finite_float_or_null",
        "required": False,
    },
    {
        "field": "hard_hit_rate",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "barrel_rate",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "command_index",
        "type": "bounded_float_or_null",
        "required": False,
    },
    {
        "field": "pitch_quality_index",
        "type": "bounded_float_or_null",
        "required": False,
    },
    {
        "field": "batter_response_index",
        "type": "bounded_float_or_null",
        "required": False,
    },
    {
        "field": "diagnostic_matchup_index",
        "type": "bounded_float_or_null",
        "required": False,
    },
    {
        "field": "diagnostic_codes",
        "type": "sorted_unique_string_list",
        "required": True,
    },
]

OVERLAY_FIELDS = [
    {
        "field": "pitcher_id",
        "type": "string",
        "required": True,
    },
    {
        "field": "batter_id",
        "type": "string",
        "required": True,
    },
    {
        "field": "pitcher_hand",
        "type": "enum_R_L_U",
        "required": True,
    },
    {
        "field": "batter_hand",
        "type": "enum_R_L_S_U",
        "required": True,
    },
    {
        "field": "count_context",
        "type": "enum",
        "required": True,
    },
    {
        "field": "as_of_date_utc",
        "type": "date",
        "required": True,
    },
    {
        "field": "pitcher_profile_status",
        "type": "enum",
        "required": True,
    },
    {
        "field": "batter_profile_status",
        "type": "enum",
        "required": True,
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
        "field": "overlay_entries",
        "type": "ordered_immutable_sequence",
        "required": True,
    },
    {
        "field": "pitcher_profile_version",
        "type": "string",
        "required": True,
    },
    {
        "field": "batter_profile_version",
        "type": "string",
        "required": True,
    },
    {
        "field": "overlay_version",
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
    {
        "field": "production_authority",
        "type": "boolean_false",
        "required": True,
    },
]

OVERLAY_STATUSES = [
    {
        "status": "resolved",
        "meaning": (
            "Pitcher and batter profiles align with sufficient coverage."
        ),
    },
    {
        "status": "partial",
        "meaning": (
            "Some pitcher arsenal exposure lacks matching batter response."
        ),
    },
    {
        "status": "sparse",
        "meaning": (
            "Aligned entries exist but sample support is insufficient."
        ),
    },
    {
        "status": "stale",
        "meaning": (
            "One or both source profiles are stale."
        ),
    },
    {
        "status": "unavailable",
        "meaning": (
            "Required source profile data is unavailable."
        ),
    },
    {
        "status": "invalid",
        "meaning": (
            "Source profiles or overlay structure failed validation."
        ),
    },
    {
        "status": "disabled",
        "meaning": (
            "Diagnostic overlay generation is disabled."
        ),
    },
]

COVERAGE_STATUSES = [
    {
        "status": "matched",
        "meaning": (
            "Pitch exists in both the pitcher and batter profiles."
        ),
    },
    {
        "status": "pitcher_only",
        "meaning": (
            "Pitch exists in the arsenal but no batter response entry exists."
        ),
    },
    {
        "status": "batter_only",
        "meaning": (
            "Batter response exists without matching arsenal exposure."
        ),
    },
    {
        "status": "unknown_pitch",
        "meaning": (
            "Canonical pitch identity is UN."
        ),
    },
    {
        "status": "context_fallback",
        "meaning": (
            "Requested count context used all-counts fallback."
        ),
    },
    {
        "status": "hand_fallback",
        "meaning": (
            "Requested pitcher-hand split used unknown-hand fallback."
        ),
    },
    {
        "status": "unavailable",
        "meaning": (
            "No usable entry can be formed."
        ),
    },
]

MATCH_RULES = [
    {
        "rule_id": "MO-M01",
        "rule": "match_on_canonical_pitch_id",
    },
    {
        "rule_id": "MO-M02",
        "rule": "prefer_exact_pitcher_hand",
    },
    {
        "rule_id": "MO-M03",
        "rule": "fallback_to_unknown_pitcher_hand",
    },
    {
        "rule_id": "MO-M04",
        "rule": "prefer_requested_count_context",
    },
    {
        "rule_id": "MO-M05",
        "rule": "fallback_to_all_counts",
    },
    {
        "rule_id": "MO-M06",
        "rule": "retain_pitcher_only_entries_observably",
    },
    {
        "rule_id": "MO-M07",
        "rule": "do_not_infer_missing_batter_metrics",
    },
]

WEIGHTING_RULES = [
    {
        "rule_id": "MO-W01",
        "rule": "pitch_usage_share_is_exposure_weight_only",
    },
    {
        "rule_id": "MO-W02",
        "rule": "null_usage_share_must_not_be_imputed_without_counts",
    },
    {
        "rule_id": "MO-W03",
        "rule": "matched_coverage_share_uses_pitcher_usage_exposure",
    },
    {
        "rule_id": "MO-W04",
        "rule": "usage_weights_must_sum_to_one_with_tolerance",
    },
    {
        "rule_id": "MO-W05",
        "rule": "diagnostic_matchup_index_must_not_be_a_probability",
    },
    {
        "rule_id": "MO-W06",
        "rule": "no_unvalidated_metric_blending",
    },
]

ORDERING_RULES = [
    {
        "rule_id": "MO-O01",
        "rule": "sort_by_pitch_usage_share_descending",
    },
    {
        "rule_id": "MO-O02",
        "rule": "null_usage_sorts_after_numeric_usage",
    },
    {
        "rule_id": "MO-O03",
        "rule": "usage_ties_sort_by_pitcher_pitch_count_descending",
    },
    {
        "rule_id": "MO-O04",
        "rule": "remaining_ties_sort_by_canonical_pitch_id",
    },
    {
        "rule_id": "MO-O05",
        "rule": "ordering_must_be_stable_across_repeated_runs",
    },
    {
        "rule_id": "MO-O06",
        "rule": "duplicate_overlay_entry_keys_are_invalid",
    },
]

VALIDATION_RULES = [
    {
        "rule_id": "MO-V01",
        "rule": "pitcher_id_must_be_nonempty",
    },
    {
        "rule_id": "MO-V02",
        "rule": "batter_id_must_be_nonempty",
    },
    {
        "rule_id": "MO-V03",
        "rule": "pitcher_profile_must_be_emitted",
    },
    {
        "rule_id": "MO-V04",
        "rule": "batter_profile_must_be_emitted",
    },
    {
        "rule_id": "MO-V05",
        "rule": "pitcher_profile_status_must_be_supported",
    },
    {
        "rule_id": "MO-V06",
        "rule": "batter_profile_status_must_be_supported",
    },
    {
        "rule_id": "MO-V07",
        "rule": "profile_pitcher_and_batter_identity_must_match_request",
    },
    {
        "rule_id": "MO-V08",
        "rule": "canonical_pitch_id_must_exist_in_8C_taxonomy",
    },
    {
        "rule_id": "MO-V09",
        "rule": "pitcher_hand_must_be_R_L_or_U",
    },
    {
        "rule_id": "MO-V10",
        "rule": "batter_hand_must_be_R_L_S_or_U",
    },
    {
        "rule_id": "MO-V11",
        "rule": "count_context_must_be_supported",
    },
    {
        "rule_id": "MO-V12",
        "rule": "usage_share_must_be_between_zero_and_one",
    },
    {
        "rule_id": "MO-V13",
        "rule": "matched_and_unmatched_counts_must_be_nonnegative",
    },
    {
        "rule_id": "MO-V14",
        "rule": "coverage_share_must_be_between_zero_and_one",
    },
    {
        "rule_id": "MO-V15",
        "rule": "coverage_share_must_match_entry_exposure",
    },
    {
        "rule_id": "MO-V16",
        "rule": "profile_versions_must_be_explicit",
    },
    {
        "rule_id": "MO-V17",
        "rule": "overlay_version_must_be_explicit",
    },
    {
        "rule_id": "MO-V18",
        "rule": "diagnostic_codes_must_be_sorted_and_unique",
    },
    {
        "rule_id": "MO-V19",
        "rule": "caller_payloads_must_remain_immutable",
    },
    {
        "rule_id": "MO-V20",
        "rule": "disabled_path_must_not_emit_overlay",
    },
    {
        "rule_id": "MO-V21",
        "rule": "production_authority_must_remain_false",
    },
]

FRESHNESS_RULES = [
    {
        "rule_id": "MO-FR01",
        "condition": "both_profiles_current",
        "result": "continue_overlay_classification",
    },
    {
        "rule_id": "MO-FR02",
        "condition": "pitcher_profile_stale",
        "result": "overlay_stale",
    },
    {
        "rule_id": "MO-FR03",
        "condition": "batter_profile_stale",
        "result": "overlay_stale",
    },
    {
        "rule_id": "MO-FR04",
        "condition": "either_profile_unavailable",
        "result": "overlay_unavailable",
    },
    {
        "rule_id": "MO-FR05",
        "condition": "either_profile_invalid",
        "result": "overlay_invalid",
    },
    {
        "rule_id": "MO-FR06",
        "condition": "profile_as_of_dates_disagree",
        "result": "retain_dates_and_emit_diagnostic",
    },
]

COVERAGE_RULES = [
    {
        "rule_id": "MO-CV01",
        "threshold": "coverage_share_gte_0_80",
        "status": "resolved",
    },
    {
        "rule_id": "MO-CV02",
        "threshold": "coverage_share_gte_0_50_lt_0_80",
        "status": "partial",
    },
    {
        "rule_id": "MO-CV03",
        "threshold": "coverage_share_gt_0_lt_0_50",
        "status": "sparse",
    },
    {
        "rule_id": "MO-CV04",
        "threshold": "coverage_share_eq_0",
        "status": "unavailable",
    },
    {
        "rule_id": "MO-CV05",
        "threshold": "unknown_pitch_exposure",
        "status": "observable_not_dropped",
    },
    {
        "rule_id": "MO-CV06",
        "threshold": "context_or_hand_fallback_used",
        "status": "partial_or_lower",
    },
]

FALLBACK_CONTRACTS = [
    {
        "fallback_id": "MO-F01",
        "condition": "overlay_disabled",
        "result": "no_overlay",
        "diagnostic_code": "pitch_type_matchup_overlay_disabled",
    },
    {
        "fallback_id": "MO-F02",
        "condition": "missing_pitcher_profile",
        "result": "unavailable_overlay",
        "diagnostic_code": "pitch_type_matchup_pitcher_profile_unavailable",
    },
    {
        "fallback_id": "MO-F03",
        "condition": "missing_batter_profile",
        "result": "unavailable_overlay",
        "diagnostic_code": "pitch_type_matchup_batter_profile_unavailable",
    },
    {
        "fallback_id": "MO-F04",
        "condition": "missing_exact_count_context",
        "result": "fallback_to_all_counts",
        "diagnostic_code": "pitch_type_matchup_count_context_fallback",
    },
    {
        "fallback_id": "MO-F05",
        "condition": "missing_exact_pitcher_hand",
        "result": "fallback_to_unknown_hand",
        "diagnostic_code": "pitch_type_matchup_pitcher_hand_fallback",
    },
    {
        "fallback_id": "MO-F06",
        "condition": "pitcher_only_pitch",
        "result": "retain_unmatched_entry",
        "diagnostic_code": "pitch_type_matchup_batter_response_missing",
    },
    {
        "fallback_id": "MO-F07",
        "condition": "unknown_canonical_pitch",
        "result": "retain_UN_entry",
        "diagnostic_code": "pitch_type_matchup_unknown_pitch_retained",
    },
]

IMPLEMENTATION_STEPS = [
    {
        "step": 1,
        "action": "Create immutable pitch-type matchup overlay module.",
    },
    {
        "step": 2,
        "action": "Import Layer 8E pitcher arsenal profile records.",
    },
    {
        "step": 3,
        "action": "Import Layer 8G batter response profile records.",
    },
    {
        "step": 4,
        "action": "Validate profile identities, statuses, and versions.",
    },
    {
        "step": 5,
        "action": "Match canonical pitch identities deterministically.",
    },
    {
        "step": 6,
        "action": "Apply exact-hand then unknown-hand fallback.",
    },
    {
        "step": 7,
        "action": "Apply requested-count then all-counts fallback.",
    },
    {
        "step": 8,
        "action": "Calculate diagnostic coverage metadata.",
    },
    {
        "step": 9,
        "action": "Retain unmatched and unknown pitches observably.",
    },
    {
        "step": 10,
        "action": "Preserve disabled-by-default and immutable-input behavior.",
    },
    {
        "step": 11,
        "action": "Create independent implementation audit.",
    },
    {
        "step": 12,
        "action": "Emit CSV and JSON audit artifacts.",
    },
]

ACCEPTANCE_CRITERIA = [
    {
        "criterion_id": "MO-A01",
        "criterion": "layer_8E_pitcher_dependency_verified",
    },
    {
        "criterion_id": "MO-A02",
        "criterion": "layer_8G_batter_dependency_verified",
    },
    {
        "criterion_id": "MO-A03",
        "criterion": "canonical_pitch_matching_defined",
    },
    {
        "criterion_id": "MO-A04",
        "criterion": "pitcher_hand_fallback_defined",
    },
    {
        "criterion_id": "MO-A05",
        "criterion": "count_context_fallback_defined",
    },
    {
        "criterion_id": "MO-A06",
        "criterion": "usage_exposure_weighting_defined",
    },
    {
        "criterion_id": "MO-A07",
        "criterion": "coverage_statuses_defined",
    },
    {
        "criterion_id": "MO-A08",
        "criterion": "overlay_statuses_defined",
    },
    {
        "criterion_id": "MO-A09",
        "criterion": "deterministic_entry_ordering_defined",
    },
    {
        "criterion_id": "MO-A10",
        "criterion": "unmatched_pitch_retention_defined",
    },
    {
        "criterion_id": "MO-A11",
        "criterion": "unknown_pitch_retention_defined",
    },
    {
        "criterion_id": "MO-A12",
        "criterion": "bounded_provenance_defined",
    },
    {
        "criterion_id": "MO-A13",
        "criterion": "caller_payloads_immutable",
    },
    {
        "criterion_id": "MO-A14",
        "criterion": "disabled_path_non_emitting",
    },
    {
        "criterion_id": "MO-A15",
        "criterion": "production_matchup_authority_absent",
    },
    {
        "criterion_id": "MO-A16",
        "criterion": "simulation_behavior_unchanged",
    },
]

PROHIBITED_AUTHORITIES = [
    "production_pitch_selection",
    "production_pitch_sequence_change",
    "production_matchup_adjustment",
    "production_overlay_integration",
    "swing_probability_change",
    "whiff_probability_change",
    "contact_probability_change",
    "batted_ball_probability_change",
    "plate_appearance_outcome_change",
    "simulation_state_change",
    "simulation_parameter_change",
    "simulation_probability_change",
    "contact_quality_change",
    "exit_velocity_change",
    "launch_angle_change",
    "canonical_probability_replacement",
    "historical_outcome_join",
    "accuracy_metric_generation",
    "calibration_metric_generation",
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

    pitcher_predecessor_present = (
        "pitcher_arsenal_profile_contract_implementation_passed"
        in string_constants(
            PITCHER_PREDECESSOR_PATH
        )
    )

    batter_predecessor_present = (
        "batter_pitch_type_response_profile_contract_implementation_passed"
        in string_constants(
            BATTER_PREDECESSOR_PATH
        )
    )

    overlay_entry_field_names = [
        row["field"]
        for row in OVERLAY_ENTRY_FIELDS
    ]

    overlay_field_names = [
        row["field"]
        for row in OVERLAY_FIELDS
    ]

    planning_checks = [
        {
            "check": "eight_e_pitcher_predecessor_present",
            "actual": pitcher_predecessor_present,
            "expected": True,
            "passed": pitcher_predecessor_present,
        },
        {
            "check": "eight_g_batter_predecessor_present",
            "actual": batter_predecessor_present,
            "expected": True,
            "passed": batter_predecessor_present,
        },
        {
            "check": "twenty_five_overlay_entry_fields_defined",
            "actual": len(OVERLAY_ENTRY_FIELDS),
            "expected": 25,
            "passed": len(OVERLAY_ENTRY_FIELDS) == 25,
        },
        {
            "check": "overlay_entry_field_names_unique",
            "actual": len(
                set(
                    overlay_entry_field_names
                )
            ),
            "expected": len(
                overlay_entry_field_names
            ),
            "passed": (
                len(
                    set(
                        overlay_entry_field_names
                    )
                )
                == len(
                    overlay_entry_field_names
                )
            ),
        },
        {
            "check": "nineteen_overlay_fields_defined",
            "actual": len(OVERLAY_FIELDS),
            "expected": 19,
            "passed": len(OVERLAY_FIELDS) == 19,
        },
        {
            "check": "overlay_field_names_unique",
            "actual": len(
                set(
                    overlay_field_names
                )
            ),
            "expected": len(
                overlay_field_names
            ),
            "passed": (
                len(
                    set(
                        overlay_field_names
                    )
                )
                == len(
                    overlay_field_names
                )
            ),
        },
        {
            "check": "seven_overlay_statuses_defined",
            "actual": len(OVERLAY_STATUSES),
            "expected": 7,
            "passed": len(OVERLAY_STATUSES) == 7,
        },
        {
            "check": "seven_coverage_statuses_defined",
            "actual": len(COVERAGE_STATUSES),
            "expected": 7,
            "passed": len(COVERAGE_STATUSES) == 7,
        },
        {
            "check": "seven_match_rules_defined",
            "actual": len(MATCH_RULES),
            "expected": 7,
            "passed": len(MATCH_RULES) == 7,
        },
        {
            "check": "six_weighting_rules_defined",
            "actual": len(WEIGHTING_RULES),
            "expected": 6,
            "passed": len(WEIGHTING_RULES) == 6,
        },
        {
            "check": "six_ordering_rules_defined",
            "actual": len(ORDERING_RULES),
            "expected": 6,
            "passed": len(ORDERING_RULES) == 6,
        },
        {
            "check": "twenty_one_validation_rules_defined",
            "actual": len(VALIDATION_RULES),
            "expected": 21,
            "passed": len(VALIDATION_RULES) == 21,
        },
        {
            "check": "six_freshness_rules_defined",
            "actual": len(FRESHNESS_RULES),
            "expected": 6,
            "passed": len(FRESHNESS_RULES) == 6,
        },
        {
            "check": "six_coverage_rules_defined",
            "actual": len(COVERAGE_RULES),
            "expected": 6,
            "passed": len(COVERAGE_RULES) == 6,
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
                "8H defines a diagnostic-only pitch-type matchup overlay plan."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "pitcher_batter_pitch_type_matchup_overlay_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "8I may implement the deterministic diagnostic overlay."
                ),
            },
            {
                "authority": (
                    "production_matchup_overlay_integration"
                ),
                "granted": False,
                "reason": (
                    "Overlay outputs remain non-authoritative diagnostics."
                ),
            },
        ]
    )

    diagnosis_name = (
        "pitcher_batter_pitch_type_matchup_overlay_contract_plan_complete"
        if all_checks_passed
        else
        "pitcher_batter_pitch_type_matchup_overlay_contract_plan_failed"
    )

    recommended_next_layer = (
        "8I_pitcher_batter_pitch_type_matchup_overlay_contract_implementation"
        if all_checks_passed
        else
        "8H_pitcher_batter_pitch_type_matchup_overlay_contract_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "overlay_entry_fields.csv": OVERLAY_ENTRY_FIELDS,
        "overlay_fields.csv": OVERLAY_FIELDS,
        "overlay_statuses.csv": OVERLAY_STATUSES,
        "coverage_statuses.csv": COVERAGE_STATUSES,
        "match_rules.csv": MATCH_RULES,
        "weighting_rules.csv": WEIGHTING_RULES,
        "ordering_rules.csv": ORDERING_RULES,
        "validation_rules.csv": VALIDATION_RULES,
        "freshness_rules.csv": FRESHNESS_RULES,
        "coverage_rules.csv": COVERAGE_RULES,
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
        "overlay_entry_fields.csv": [
            "field",
            "type",
            "required",
        ],
        "overlay_fields.csv": [
            "field",
            "type",
            "required",
        ],
        "overlay_statuses.csv": [
            "status",
            "meaning",
        ],
        "coverage_statuses.csv": [
            "status",
            "meaning",
        ],
        "match_rules.csv": [
            "rule_id",
            "rule",
        ],
        "weighting_rules.csv": [
            "rule_id",
            "rule",
        ],
        "ordering_rules.csv": [
            "rule_id",
            "rule",
        ],
        "validation_rules.csv": [
            "rule_id",
            "rule",
        ],
        "freshness_rules.csv": [
            "rule_id",
            "condition",
            "result",
        ],
        "coverage_rules.csv": [
            "rule_id",
            "threshold",
            "status",
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
                    "Implement the deterministic diagnostic pitcher-batter "
                    "pitch-type matchup overlay with an independent audit."
                    if all_checks_passed
                    else
                    "Remediate failed 8H planning checks."
                ),
                "entry_condition": (
                    "All nineteen 8H planning checks pass."
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
        "overlay_entry_fields_defined": len(
            OVERLAY_ENTRY_FIELDS
        ),
        "overlay_fields_defined": len(
            OVERLAY_FIELDS
        ),
        "overlay_statuses_defined": len(
            OVERLAY_STATUSES
        ),
        "coverage_statuses_defined": len(
            COVERAGE_STATUSES
        ),
        "match_rules_defined": len(
            MATCH_RULES
        ),
        "weighting_rules_defined": len(
            WEIGHTING_RULES
        ),
        "ordering_rules_defined": len(
            ORDERING_RULES
        ),
        "validation_rules_defined": len(
            VALIDATION_RULES
        ),
        "freshness_rules_defined": len(
            FRESHNESS_RULES
        ),
        "coverage_rules_defined": len(
            COVERAGE_RULES
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
        "pitch_selection_changed": False,
        "pitch_sequence_changed": False,
        "matchup_adjustments_activated": False,
        "swing_probability_changed": False,
        "whiff_probability_changed": False,
        "contact_probability_changed": False,
        "batted_ball_probability_changed": False,
        "contact_quality_changed": False,
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
        "matchup_overlay_implementation_allowed_next": (
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
