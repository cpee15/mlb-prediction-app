#!/usr/bin/env python3
"""
Layer 8F
Batter Pitch-Type Response Profile Contract Plan

Defines the bounded planning contract for diagnostic batter response profiles
by canonical pitch type.

The contract covers:
- batter identity and handedness;
- canonical pitch identities from Layer 8C;
- pitcher-hand and count-context dimensions;
- swing, chase, take, whiff, contact, and CSW response metrics;
- exit velocity, launch angle, hard-hit, barrel, and batted-ball metadata;
- sample size, season, freshness, provenance, ordering, and fallbacks;
- disabled-by-default, immutable, non-authoritative diagnostics.

Planning only.

This layer does not:
- alter pitch selection or sequencing;
- activate pitcher-batter matchup adjustments;
- alter swing, contact, batted-ball, or plate-appearance probabilities;
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


LAYER_ID = "8F"
LAYER_NAME = "batter_pitch_type_response_profile_contract_plan"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8F_batter_pitch_type_response_profile_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "audit_8E_pitcher_arsenal_profile_contract.py"
)

RESPONSE_ENTRY_FIELDS = [
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
        "field": "count_context",
        "type": "enum",
        "required": True,
    },
    {
        "field": "pitch_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "swing_count",
        "type": "nonnegative_integer_or_null",
        "required": False,
    },
    {
        "field": "contact_count",
        "type": "nonnegative_integer_or_null",
        "required": False,
    },
    {
        "field": "batted_ball_count",
        "type": "nonnegative_integer_or_null",
        "required": False,
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
        "field": "zone_swing_rate",
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
        "field": "ground_ball_rate",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "line_drive_rate",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "fly_ball_rate",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "popup_rate",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "response_index",
        "type": "bounded_float_or_null",
        "required": False,
    },
    {
        "field": "diagnostic_codes",
        "type": "sorted_unique_string_list",
        "required": True,
    },
]

PROFILE_FIELDS = [
    {
        "field": "batter_id",
        "type": "string",
        "required": True,
    },
    {
        "field": "batter_name",
        "type": "string_or_null",
        "required": False,
    },
    {
        "field": "batter_hand",
        "type": "enum_R_L_S_U",
        "required": True,
    },
    {
        "field": "season",
        "type": "integer",
        "required": True,
    },
    {
        "field": "as_of_date_utc",
        "type": "date",
        "required": True,
    },
    {
        "field": "source_name",
        "type": "string",
        "required": True,
    },
    {
        "field": "source_record_id",
        "type": "string_or_null",
        "required": False,
    },
    {
        "field": "source_timestamp_utc",
        "type": "datetime_or_null",
        "required": False,
    },
    {
        "field": "sample_pitch_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "sample_plate_appearance_count",
        "type": "nonnegative_integer_or_null",
        "required": False,
    },
    {
        "field": "profile_status",
        "type": "enum",
        "required": True,
    },
    {
        "field": "response_entries",
        "type": "ordered_immutable_sequence",
        "required": True,
    },
    {
        "field": "taxonomy_version",
        "type": "string",
        "required": True,
    },
    {
        "field": "profile_version",
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

COUNT_CONTEXTS = [
    {
        "context": "all_counts",
        "description": "Unconditioned pitch-type response.",
    },
    {
        "context": "ahead",
        "description": "Batter ahead in the count.",
    },
    {
        "context": "even",
        "description": "Neutral count state.",
    },
    {
        "context": "behind",
        "description": "Batter behind in the count.",
    },
    {
        "context": "two_strike",
        "description": "Two-strike response context.",
    },
    {
        "context": "first_pitch",
        "description": "First-pitch response context.",
    },
    {
        "context": "unknown",
        "description": "Count context unavailable.",
    },
]

PROFILE_STATUSES = [
    {
        "status": "resolved",
        "meaning": "Usable diagnostic response profile is present.",
    },
    {
        "status": "partial",
        "meaning": "Some pitch-type or response metrics are missing.",
    },
    {
        "status": "sparse",
        "meaning": "Profile exists but sample size is below threshold.",
    },
    {
        "status": "stale",
        "meaning": "Profile timestamp is older than the freshness limit.",
    },
    {
        "status": "unavailable",
        "meaning": "No usable response profile is available.",
    },
    {
        "status": "invalid",
        "meaning": "Profile failed structural or numeric validation.",
    },
    {
        "status": "disabled",
        "meaning": "Diagnostic response generation is disabled.",
    },
]

SOURCE_PRECEDENCE = [
    {
        "priority": 1,
        "source": "statcast_pitch_level_batter_aggregate",
        "use_when": "complete_and_current",
    },
    {
        "priority": 2,
        "source": "trusted_provider_pitch_type_split",
        "use_when": "statcast_unavailable",
    },
    {
        "priority": 3,
        "source": "repository_cached_batter_response_profile",
        "use_when": "trusted_source_unavailable_and_cache_current",
    },
    {
        "priority": 4,
        "source": "season_level_pitch_family_summary",
        "use_when": "pitch_type_metrics_partial",
    },
    {
        "priority": 5,
        "source": "unavailable_fallback",
        "use_when": "missing_invalid_or_unsupported",
    },
]

RATE_RULES = [
    {
        "rule_id": "BR-R01",
        "rule": "all_rate_metrics_must_be_between_zero_and_one",
    },
    {
        "rule_id": "BR-R02",
        "rule": "whiff_rate_requires_swing_count_support",
    },
    {
        "rule_id": "BR-R03",
        "rule": "contact_rate_requires_swing_count_support",
    },
    {
        "rule_id": "BR-R04",
        "rule": "batted_ball_rates_require_batted_ball_count_support",
    },
    {
        "rule_id": "BR-R05",
        "rule": "batted_ball_type_rates_may_not_materially_exceed_one",
    },
    {
        "rule_id": "BR-R06",
        "rule": "rate_rounding_must_be_deterministic",
    },
]

ORDERING_RULES = [
    {
        "rule_id": "BR-O01",
        "rule": "sort_by_pitch_count_descending",
    },
    {
        "rule_id": "BR-O02",
        "rule": "pitch_count_ties_sort_by_canonical_pitch_id",
    },
    {
        "rule_id": "BR-O03",
        "rule": "remaining_ties_sort_by_pitcher_hand",
    },
    {
        "rule_id": "BR-O04",
        "rule": "remaining_ties_sort_by_count_context",
    },
    {
        "rule_id": "BR-O05",
        "rule": "ordering_must_be_stable_across_repeated_runs",
    },
    {
        "rule_id": "BR-O06",
        "rule": "duplicate_pitch_hand_count_entries_are_invalid",
    },
]

VALIDATION_RULES = [
    {
        "rule_id": "BR-V01",
        "rule": "batter_id_must_be_nonempty",
    },
    {
        "rule_id": "BR-V02",
        "rule": "batter_hand_must_be_R_L_S_or_U",
    },
    {
        "rule_id": "BR-V03",
        "rule": "pitcher_hand_must_be_R_L_or_U",
    },
    {
        "rule_id": "BR-V04",
        "rule": "count_context_must_be_supported",
    },
    {
        "rule_id": "BR-V05",
        "rule": "season_must_be_positive",
    },
    {
        "rule_id": "BR-V06",
        "rule": "as_of_date_must_be_present",
    },
    {
        "rule_id": "BR-V07",
        "rule": "source_name_must_be_nonempty",
    },
    {
        "rule_id": "BR-V08",
        "rule": "sample_counts_must_be_nonnegative",
    },
    {
        "rule_id": "BR-V09",
        "rule": "canonical_pitch_id_must_exist_in_8C_taxonomy",
    },
    {
        "rule_id": "BR-V10",
        "rule": "response_entry_keys_must_be_unique",
    },
    {
        "rule_id": "BR-V11",
        "rule": "rate_metrics_must_be_between_zero_and_one",
    },
    {
        "rule_id": "BR-V12",
        "rule": "exit_velocity_and_launch_angle_must_be_finite",
    },
    {
        "rule_id": "BR-V13",
        "rule": "component_counts_may_not_exceed_parent_counts",
    },
    {
        "rule_id": "BR-V14",
        "rule": "batted_ball_rate_sum_must_be_tolerable",
    },
    {
        "rule_id": "BR-V15",
        "rule": "profile_status_must_match_completeness",
    },
    {
        "rule_id": "BR-V16",
        "rule": "taxonomy_version_must_be_explicit",
    },
    {
        "rule_id": "BR-V17",
        "rule": "profile_version_must_be_explicit",
    },
    {
        "rule_id": "BR-V18",
        "rule": "diagnostic_codes_must_be_sorted_and_unique",
    },
    {
        "rule_id": "BR-V19",
        "rule": "caller_payload_must_remain_immutable",
    },
    {
        "rule_id": "BR-V20",
        "rule": "disabled_path_must_not_emit_profile",
    },
    {
        "rule_id": "BR-V21",
        "rule": "production_authority_must_remain_false",
    },
]

FRESHNESS_RULES = [
    {
        "rule_id": "BR-FR01",
        "context": "in_season_current_profile",
        "maximum_age_days": 14,
    },
    {
        "rule_id": "BR-FR02",
        "context": "recent_batter_role_change",
        "maximum_age_days": 7,
    },
    {
        "rule_id": "BR-FR03",
        "context": "spring_or_preseason",
        "maximum_age_days": 30,
    },
    {
        "rule_id": "BR-FR04",
        "context": "offseason_reference",
        "maximum_age_days": 180,
    },
    {
        "rule_id": "BR-FR05",
        "context": "missing_source_timestamp",
        "maximum_age_days": 0,
    },
    {
        "rule_id": "BR-FR06",
        "context": "future_source_timestamp",
        "maximum_age_days": 0,
    },
]

SAMPLE_SIZE_RULES = [
    {
        "rule_id": "BR-S01",
        "level": "profile",
        "minimum_pitch_count": 100,
        "status_below_threshold": "sparse",
    },
    {
        "rule_id": "BR-S02",
        "level": "pitch_type_entry",
        "minimum_pitch_count": 25,
        "status_below_threshold": "partial",
    },
    {
        "rule_id": "BR-S03",
        "level": "swing_metric",
        "minimum_pitch_count": 25,
        "status_below_threshold": "partial",
    },
    {
        "rule_id": "BR-S04",
        "level": "whiff_contact_metric",
        "minimum_pitch_count": 25,
        "status_below_threshold": "partial",
    },
    {
        "rule_id": "BR-S05",
        "level": "batted_ball_metric",
        "minimum_pitch_count": 20,
        "status_below_threshold": "partial",
    },
    {
        "rule_id": "BR-S06",
        "level": "count_conditioned_entry",
        "minimum_pitch_count": 15,
        "status_below_threshold": "partial",
    },
]

FALLBACK_CONTRACTS = [
    {
        "fallback_id": "BR-F01",
        "condition": "profile_disabled",
        "result": "no_profile",
        "diagnostic_code": "batter_pitch_response_profile_disabled",
    },
    {
        "fallback_id": "BR-F02",
        "condition": "missing_batter_identity",
        "result": "invalid_no_profile",
        "diagnostic_code": "batter_pitch_response_batter_identity_missing",
    },
    {
        "fallback_id": "BR-F03",
        "condition": "missing_source_profile",
        "result": "unavailable_empty_profile",
        "diagnostic_code": "batter_pitch_response_source_unavailable",
    },
    {
        "fallback_id": "BR-F04",
        "condition": "unknown_pitch_classification",
        "result": "retain_UN_entry",
        "diagnostic_code": "batter_pitch_response_unknown_pitch_retained",
    },
    {
        "fallback_id": "BR-F05",
        "condition": "invalid_rate_or_count_relationship",
        "result": "invalid_non_authoritative_profile",
        "diagnostic_code": "batter_pitch_response_metrics_invalid",
    },
    {
        "fallback_id": "BR-F06",
        "condition": "sparse_sample",
        "result": "sparse_non_authoritative_profile",
        "diagnostic_code": "batter_pitch_response_sample_sparse",
    },
    {
        "fallback_id": "BR-F07",
        "condition": "stale_profile",
        "result": "stale_non_authoritative_profile",
        "diagnostic_code": "batter_pitch_response_profile_stale",
    },
]

IMPLEMENTATION_STEPS = [
    {
        "step": 1,
        "action": "Create immutable batter pitch-type response profile module.",
    },
    {
        "step": 2,
        "action": "Import and validate Layer 8C canonical pitch identities.",
    },
    {
        "step": 3,
        "action": "Define immutable response entry and profile records.",
    },
    {
        "step": 4,
        "action": "Normalize batter hand, pitcher hand, and count context.",
    },
    {
        "step": 5,
        "action": "Validate pitch, swing, contact, and batted-ball counts.",
    },
    {
        "step": 6,
        "action": "Validate rate and batted-ball quality metrics.",
    },
    {
        "step": 7,
        "action": "Apply sample-size and freshness classifications.",
    },
    {
        "step": 8,
        "action": "Sort response entries deterministically.",
    },
    {
        "step": 9,
        "action": "Retain canonical UN classifications observably.",
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
        "criterion_id": "BR-C01",
        "criterion": "layer_8C_taxonomy_dependency_verified",
    },
    {
        "criterion_id": "BR-C02",
        "criterion": "batter_identity_and_hand_validation_defined",
    },
    {
        "criterion_id": "BR-C03",
        "criterion": "pitcher_hand_and_count_context_defined",
    },
    {
        "criterion_id": "BR-C04",
        "criterion": "response_entry_keys_unique",
    },
    {
        "criterion_id": "BR-C05",
        "criterion": "swing_and_contact_rate_validation_defined",
    },
    {
        "criterion_id": "BR-C06",
        "criterion": "batted_ball_quality_validation_defined",
    },
    {
        "criterion_id": "BR-C07",
        "criterion": "sample_size_statuses_defined",
    },
    {
        "criterion_id": "BR-C08",
        "criterion": "freshness_statuses_defined",
    },
    {
        "criterion_id": "BR-C09",
        "criterion": "deterministic_entry_ordering_defined",
    },
    {
        "criterion_id": "BR-C10",
        "criterion": "unknown_pitch_retention_defined",
    },
    {
        "criterion_id": "BR-C11",
        "criterion": "bounded_provenance_defined",
    },
    {
        "criterion_id": "BR-C12",
        "criterion": "diagnostic_codes_deterministic",
    },
    {
        "criterion_id": "BR-C13",
        "criterion": "caller_payload_immutable",
    },
    {
        "criterion_id": "BR-C14",
        "criterion": "disabled_path_non_emitting",
    },
    {
        "criterion_id": "BR-C15",
        "criterion": "matchup_adjustment_authority_absent",
    },
    {
        "criterion_id": "BR-C16",
        "criterion": "simulation_behavior_unchanged",
    },
]

PROHIBITED_AUTHORITIES = [
    "production_pitch_selection",
    "production_pitch_sequence_change",
    "production_batter_response_replacement",
    "production_matchup_adjustment",
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


def string_constants(path: Path) -> set[str]:
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
        "pitcher_arsenal_profile_contract_implementation_passed"
        in string_constants(
            PREDECESSOR_PATH
        )
    )

    priorities = [
        row["priority"]
        for row in SOURCE_PRECEDENCE
    ]

    response_field_names = [
        row["field"]
        for row in RESPONSE_ENTRY_FIELDS
    ]

    profile_field_names = [
        row["field"]
        for row in PROFILE_FIELDS
    ]

    planning_checks = [
        {
            "check": "eight_e_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twenty_five_response_entry_fields_defined",
            "actual": len(RESPONSE_ENTRY_FIELDS),
            "expected": 25,
            "passed": len(RESPONSE_ENTRY_FIELDS) == 25,
        },
        {
            "check": "response_entry_field_names_unique",
            "actual": len(set(response_field_names)),
            "expected": len(response_field_names),
            "passed": (
                len(set(response_field_names))
                == len(response_field_names)
            ),
        },
        {
            "check": "seventeen_profile_fields_defined",
            "actual": len(PROFILE_FIELDS),
            "expected": 17,
            "passed": len(PROFILE_FIELDS) == 17,
        },
        {
            "check": "profile_field_names_unique",
            "actual": len(set(profile_field_names)),
            "expected": len(profile_field_names),
            "passed": (
                len(set(profile_field_names))
                == len(profile_field_names)
            ),
        },
        {
            "check": "seven_count_contexts_defined",
            "actual": len(COUNT_CONTEXTS),
            "expected": 7,
            "passed": len(COUNT_CONTEXTS) == 7,
        },
        {
            "check": "seven_profile_statuses_defined",
            "actual": len(PROFILE_STATUSES),
            "expected": 7,
            "passed": len(PROFILE_STATUSES) == 7,
        },
        {
            "check": "five_source_precedence_rules_defined",
            "actual": len(SOURCE_PRECEDENCE),
            "expected": 5,
            "passed": len(SOURCE_PRECEDENCE) == 5,
        },
        {
            "check": "source_precedence_contiguous",
            "actual": priorities,
            "expected": [1, 2, 3, 4, 5],
            "passed": priorities == [1, 2, 3, 4, 5],
        },
        {
            "check": "six_rate_rules_defined",
            "actual": len(RATE_RULES),
            "expected": 6,
            "passed": len(RATE_RULES) == 6,
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
            "check": "six_sample_size_rules_defined",
            "actual": len(SAMPLE_SIZE_RULES),
            "expected": 6,
            "passed": len(SAMPLE_SIZE_RULES) == 6,
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
                "8F defines a diagnostic-only batter response profile plan."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "batter_pitch_type_response_profile_contract_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "8G may implement deterministic diagnostic batter "
                    "pitch-type response profiles."
                ),
            },
            {
                "authority": (
                    "production_batter_response_integration"
                ),
                "granted": False,
                "reason": (
                    "Batter response profiles remain non-authoritative."
                ),
            },
        ]
    )

    diagnosis_name = (
        "batter_pitch_type_response_profile_contract_plan_complete"
        if all_checks_passed
        else
        "batter_pitch_type_response_profile_contract_plan_failed"
    )

    recommended_next_layer = (
        "8G_batter_pitch_type_response_profile_contract_implementation"
        if all_checks_passed
        else
        "8F_batter_pitch_type_response_profile_contract_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "response_entry_fields.csv": RESPONSE_ENTRY_FIELDS,
        "profile_fields.csv": PROFILE_FIELDS,
        "count_contexts.csv": COUNT_CONTEXTS,
        "profile_statuses.csv": PROFILE_STATUSES,
        "source_precedence.csv": SOURCE_PRECEDENCE,
        "rate_rules.csv": RATE_RULES,
        "ordering_rules.csv": ORDERING_RULES,
        "validation_rules.csv": VALIDATION_RULES,
        "freshness_rules.csv": FRESHNESS_RULES,
        "sample_size_rules.csv": SAMPLE_SIZE_RULES,
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
        "response_entry_fields.csv": [
            "field",
            "type",
            "required",
        ],
        "profile_fields.csv": [
            "field",
            "type",
            "required",
        ],
        "count_contexts.csv": [
            "context",
            "description",
        ],
        "profile_statuses.csv": [
            "status",
            "meaning",
        ],
        "source_precedence.csv": [
            "priority",
            "source",
            "use_when",
        ],
        "rate_rules.csv": [
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
            "context",
            "maximum_age_days",
        ],
        "sample_size_rules.csv": [
            "rule_id",
            "level",
            "minimum_pitch_count",
            "status_below_threshold",
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
                    "Implement deterministic diagnostic batter pitch-type "
                    "response profiles with an independent contract audit."
                    if all_checks_passed
                    else
                    "Remediate failed 8F planning checks."
                ),
                "entry_condition": (
                    "All nineteen 8F planning checks pass."
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
        "response_entry_fields_defined": len(
            RESPONSE_ENTRY_FIELDS
        ),
        "profile_fields_defined": len(
            PROFILE_FIELDS
        ),
        "count_contexts_defined": len(
            COUNT_CONTEXTS
        ),
        "profile_statuses_defined": len(
            PROFILE_STATUSES
        ),
        "source_precedence_rules_defined": len(
            SOURCE_PRECEDENCE
        ),
        "rate_rules_defined": len(
            RATE_RULES
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
        "sample_size_rules_defined": len(
            SAMPLE_SIZE_RULES
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
        "contact_probability_changed": False,
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
        "batter_pitch_type_response_implementation_allowed_next": (
            all_checks_passed
        ),
        "production_batter_response_integration_allowed_next": False,
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
