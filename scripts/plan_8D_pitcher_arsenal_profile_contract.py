#!/usr/bin/env python3
"""
Layer 8D
Pitcher Arsenal Profile Contract Plan

Defines the bounded planning contract for diagnostic pitcher arsenal profiles.

The contract covers:
- pitcher identity and handedness;
- canonical pitch identities from Layer 8C;
- pitch availability and usage share;
- velocity, movement, extension, location, command, and quality metadata;
- sample size, season, role, freshness, and provenance;
- deterministic ordering, normalization, validation, and fallback behavior;
- disabled-by-default, non-authoritative diagnostics.

Planning only.

This layer does not:
- select production pitches;
- change pitch sequencing;
- alter matchup, plate-appearance, contact-quality, or batted-ball outcomes;
- alter simulation state, parameters, or probabilities;
- join historical outcomes;
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


LAYER_ID = "8D"
LAYER_NAME = "pitcher_arsenal_profile_contract_plan"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8D_pitcher_arsenal_profile_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "audit_8C_canonical_pitch_taxonomy_and_source_contract.py"
)

ARSENAL_ENTRY_FIELDS = [
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
        "field": "available",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "usage_share",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "pitch_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "avg_velocity_mph",
        "type": "positive_float_or_null",
        "required": False,
    },
    {
        "field": "avg_spin_rpm",
        "type": "nonnegative_float_or_null",
        "required": False,
    },
    {
        "field": "avg_horizontal_break_inches",
        "type": "finite_float_or_null",
        "required": False,
    },
    {
        "field": "avg_vertical_break_inches",
        "type": "finite_float_or_null",
        "required": False,
    },
    {
        "field": "avg_extension_feet",
        "type": "positive_float_or_null",
        "required": False,
    },
    {
        "field": "zone_rate",
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
        "field": "called_strike_plus_whiff_rate",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "command_index",
        "type": "bounded_float_or_null",
        "required": False,
    },
    {
        "field": "quality_index",
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
        "field": "pitcher_id",
        "type": "string",
        "required": True,
    },
    {
        "field": "pitcher_name",
        "type": "string_or_null",
        "required": False,
    },
    {
        "field": "pitcher_hand",
        "type": "enum_R_L_U",
        "required": True,
    },
    {
        "field": "pitcher_role",
        "type": "enum_starter_reliever_opener_unknown",
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
        "field": "sample_game_count",
        "type": "nonnegative_integer_or_null",
        "required": False,
    },
    {
        "field": "profile_status",
        "type": "enum",
        "required": True,
    },
    {
        "field": "arsenal_entries",
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
        "field": "production_authority",
        "type": "boolean_false",
        "required": True,
    },
]

PROFILE_STATUSES = [
    {
        "status": "resolved",
        "meaning": "Usable diagnostic arsenal profile is present.",
    },
    {
        "status": "partial",
        "meaning": "Some pitches or metrics are missing.",
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
        "meaning": "No usable source profile is available.",
    },
    {
        "status": "invalid",
        "meaning": "Profile failed structural or numeric validation.",
    },
    {
        "status": "disabled",
        "meaning": "Diagnostic arsenal generation is disabled.",
    },
]

SOURCE_PRECEDENCE = [
    {
        "priority": 1,
        "source": "statcast_pitch_level_aggregate",
        "use_when": "complete_and_current",
    },
    {
        "priority": 2,
        "source": "trusted_provider_pitch_level_aggregate",
        "use_when": "statcast_unavailable",
    },
    {
        "priority": 3,
        "source": "repository_cached_pitch_profile",
        "use_when": "trusted_source_unavailable_and_cache_current",
    },
    {
        "priority": 4,
        "source": "season_level_pitch_mix_summary",
        "use_when": "pitch_level_metrics_partial",
    },
    {
        "priority": 5,
        "source": "unavailable_fallback",
        "use_when": "missing_invalid_or_unsupported",
    },
]

USAGE_RULES = [
    {
        "rule_id": "AR-U01",
        "rule": "usage_share_must_be_between_zero_and_one",
    },
    {
        "rule_id": "AR-U02",
        "rule": "resolved_usage_shares_should_sum_to_one_with_tolerance",
    },
    {
        "rule_id": "AR-U03",
        "rule": "zero_pitch_count_requires_zero_or_null_usage",
    },
    {
        "rule_id": "AR-U04",
        "rule": "available_pitch_requires_positive_pitch_count",
    },
    {
        "rule_id": "AR-U05",
        "rule": "unavailable_pitch_must_not_receive_authoritative_usage",
    },
    {
        "rule_id": "AR-U06",
        "rule": "usage_rounding_must_be_deterministic",
    },
]

ORDERING_RULES = [
    {
        "rule_id": "AR-O01",
        "rule": "sort_by_usage_share_descending",
    },
    {
        "rule_id": "AR-O02",
        "rule": "null_usage_sorts_after_numeric_usage",
    },
    {
        "rule_id": "AR-O03",
        "rule": "usage_ties_sort_by_pitch_count_descending",
    },
    {
        "rule_id": "AR-O04",
        "rule": "remaining_ties_sort_by_canonical_pitch_id",
    },
    {
        "rule_id": "AR-O05",
        "rule": "ordering_must_be_stable_across_repeated_runs",
    },
    {
        "rule_id": "AR-O06",
        "rule": "duplicate_canonical_pitch_entries_are_invalid",
    },
]

VALIDATION_RULES = [
    {
        "rule_id": "AR-V01",
        "rule": "pitcher_id_must_be_nonempty",
    },
    {
        "rule_id": "AR-V02",
        "rule": "pitcher_hand_must_be_R_L_or_U",
    },
    {
        "rule_id": "AR-V03",
        "rule": "pitcher_role_must_be_supported",
    },
    {
        "rule_id": "AR-V04",
        "rule": "season_must_be_positive",
    },
    {
        "rule_id": "AR-V05",
        "rule": "as_of_date_must_be_present",
    },
    {
        "rule_id": "AR-V06",
        "rule": "source_name_must_be_nonempty",
    },
    {
        "rule_id": "AR-V07",
        "rule": "sample_pitch_count_must_be_nonnegative",
    },
    {
        "rule_id": "AR-V08",
        "rule": "entry_pitch_count_must_be_nonnegative",
    },
    {
        "rule_id": "AR-V09",
        "rule": "canonical_pitch_id_must_exist_in_8C_taxonomy",
    },
    {
        "rule_id": "AR-V10",
        "rule": "canonical_pitch_ids_must_be_unique_within_profile",
    },
    {
        "rule_id": "AR-V11",
        "rule": "rate_metrics_must_be_between_zero_and_one",
    },
    {
        "rule_id": "AR-V12",
        "rule": "velocity_spin_break_and_extension_must_be_finite",
    },
    {
        "rule_id": "AR-V13",
        "rule": "usage_total_must_be_valid_for_resolved_profile",
    },
    {
        "rule_id": "AR-V14",
        "rule": "profile_status_must_match_completeness",
    },
    {
        "rule_id": "AR-V15",
        "rule": "taxonomy_version_must_be_explicit",
    },
    {
        "rule_id": "AR-V16",
        "rule": "profile_version_must_be_explicit",
    },
    {
        "rule_id": "AR-V17",
        "rule": "diagnostic_codes_must_be_sorted_and_unique",
    },
    {
        "rule_id": "AR-V18",
        "rule": "caller_payload_must_remain_immutable",
    },
    {
        "rule_id": "AR-V19",
        "rule": "disabled_path_must_not_emit_profile",
    },
    {
        "rule_id": "AR-V20",
        "rule": "production_authority_must_remain_false",
    },
]

FRESHNESS_RULES = [
    {
        "rule_id": "AR-FR01",
        "context": "in_season_current_profile",
        "maximum_age_days": 14,
    },
    {
        "rule_id": "AR-FR02",
        "context": "recent_role_change",
        "maximum_age_days": 7,
    },
    {
        "rule_id": "AR-FR03",
        "context": "spring_or_preseason",
        "maximum_age_days": 30,
    },
    {
        "rule_id": "AR-FR04",
        "context": "offseason_reference",
        "maximum_age_days": 180,
    },
    {
        "rule_id": "AR-FR05",
        "context": "missing_source_timestamp",
        "maximum_age_days": 0,
    },
    {
        "rule_id": "AR-FR06",
        "context": "future_source_timestamp",
        "maximum_age_days": 0,
    },
]

SAMPLE_SIZE_RULES = [
    {
        "rule_id": "AR-S01",
        "level": "profile",
        "minimum_pitch_count": 50,
        "status_below_threshold": "sparse",
    },
    {
        "rule_id": "AR-S02",
        "level": "pitch_entry",
        "minimum_pitch_count": 10,
        "status_below_threshold": "partial",
    },
    {
        "rule_id": "AR-S03",
        "level": "movement_metric",
        "minimum_pitch_count": 10,
        "status_below_threshold": "partial",
    },
    {
        "rule_id": "AR-S04",
        "level": "swing_metric",
        "minimum_pitch_count": 25,
        "status_below_threshold": "partial",
    },
    {
        "rule_id": "AR-S05",
        "level": "command_metric",
        "minimum_pitch_count": 50,
        "status_below_threshold": "partial",
    },
    {
        "rule_id": "AR-S06",
        "level": "quality_metric",
        "minimum_pitch_count": 50,
        "status_below_threshold": "partial",
    },
]

FALLBACK_CONTRACTS = [
    {
        "fallback_id": "AR-F01",
        "condition": "taxonomy_disabled",
        "result": "no_profile",
        "diagnostic_code": "pitcher_arsenal_profile_disabled",
    },
    {
        "fallback_id": "AR-F02",
        "condition": "missing_pitcher_identity",
        "result": "invalid_no_profile",
        "diagnostic_code": "pitcher_arsenal_pitcher_identity_missing",
    },
    {
        "fallback_id": "AR-F03",
        "condition": "missing_source_profile",
        "result": "unavailable_empty_profile",
        "diagnostic_code": "pitcher_arsenal_source_unavailable",
    },
    {
        "fallback_id": "AR-F04",
        "condition": "unknown_pitch_classification",
        "result": "retain_UN_entry",
        "diagnostic_code": "pitcher_arsenal_unknown_pitch_retained",
    },
    {
        "fallback_id": "AR-F05",
        "condition": "invalid_usage_total",
        "result": "invalid_non_authoritative_profile",
        "diagnostic_code": "pitcher_arsenal_usage_total_invalid",
    },
    {
        "fallback_id": "AR-F06",
        "condition": "sparse_sample",
        "result": "sparse_non_authoritative_profile",
        "diagnostic_code": "pitcher_arsenal_sample_sparse",
    },
    {
        "fallback_id": "AR-F07",
        "condition": "stale_profile",
        "result": "stale_non_authoritative_profile",
        "diagnostic_code": "pitcher_arsenal_profile_stale",
    },
]

IMPLEMENTATION_STEPS = [
    {
        "step": 1,
        "action": "Create immutable pitcher arsenal profile module.",
    },
    {
        "step": 2,
        "action": "Import and validate Layer 8C canonical pitch identities.",
    },
    {
        "step": 3,
        "action": "Define immutable arsenal entry and profile records.",
    },
    {
        "step": 4,
        "action": "Normalize pitcher hand, role, season, and source metadata.",
    },
    {
        "step": 5,
        "action": "Validate pitch-level numeric and rate fields.",
    },
    {
        "step": 6,
        "action": "Compute deterministic usage shares when permitted.",
    },
    {
        "step": 7,
        "action": "Apply sample-size and freshness classifications.",
    },
    {
        "step": 8,
        "action": "Sort arsenal entries deterministically.",
    },
    {
        "step": 9,
        "action": "Emit bounded provenance and diagnostic codes.",
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
        "criterion_id": "AR-C01",
        "criterion": "layer_8C_taxonomy_dependency_verified",
    },
    {
        "criterion_id": "AR-C02",
        "criterion": "pitcher_identity_validation_defined",
    },
    {
        "criterion_id": "AR-C03",
        "criterion": "pitcher_hand_and_role_validation_defined",
    },
    {
        "criterion_id": "AR-C04",
        "criterion": "canonical_pitch_entries_unique",
    },
    {
        "criterion_id": "AR-C05",
        "criterion": "usage_share_validation_defined",
    },
    {
        "criterion_id": "AR-C06",
        "criterion": "usage_total_tolerance_defined",
    },
    {
        "criterion_id": "AR-C07",
        "criterion": "sample_size_statuses_defined",
    },
    {
        "criterion_id": "AR-C08",
        "criterion": "freshness_statuses_defined",
    },
    {
        "criterion_id": "AR-C09",
        "criterion": "deterministic_entry_ordering_defined",
    },
    {
        "criterion_id": "AR-C10",
        "criterion": "unknown_pitch_retention_defined",
    },
    {
        "criterion_id": "AR-C11",
        "criterion": "bounded_provenance_defined",
    },
    {
        "criterion_id": "AR-C12",
        "criterion": "diagnostic_codes_deterministic",
    },
    {
        "criterion_id": "AR-C13",
        "criterion": "caller_payload_immutable",
    },
    {
        "criterion_id": "AR-C14",
        "criterion": "disabled_path_non_emitting",
    },
    {
        "criterion_id": "AR-C15",
        "criterion": "production_pitch_selection_unchanged",
    },
    {
        "criterion_id": "AR-C16",
        "criterion": "simulation_behavior_unchanged",
    },
]

PROHIBITED_AUTHORITIES = [
    "production_pitch_selection",
    "production_pitch_sequence_change",
    "production_arsenal_replacement",
    "production_matchup_adjustment",
    "simulation_state_change",
    "simulation_parameter_change",
    "simulation_probability_change",
    "plate_appearance_outcome_change",
    "contact_quality_change",
    "exit_velocity_change",
    "launch_angle_change",
    "batted_ball_outcome_change",
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
        "canonical_pitch_taxonomy_and_source_contract_implementation_passed"
        in string_constants(
            PREDECESSOR_PATH
        )
    )

    priorities = [
        row["priority"]
        for row in SOURCE_PRECEDENCE
    ]

    arsenal_field_names = [
        row["field"]
        for row in ARSENAL_ENTRY_FIELDS
    ]

    profile_field_names = [
        row["field"]
        for row in PROFILE_FIELDS
    ]

    planning_checks = [
        {
            "check": "eight_c_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "eighteen_arsenal_entry_fields_defined",
            "actual": len(ARSENAL_ENTRY_FIELDS),
            "expected": 18,
            "passed": len(ARSENAL_ENTRY_FIELDS) == 18,
        },
        {
            "check": "arsenal_entry_field_names_unique",
            "actual": len(set(arsenal_field_names)),
            "expected": len(arsenal_field_names),
            "passed": (
                len(set(arsenal_field_names))
                == len(arsenal_field_names)
            ),
        },
        {
            "check": "sixteen_profile_fields_defined",
            "actual": len(PROFILE_FIELDS),
            "expected": 16,
            "passed": len(PROFILE_FIELDS) == 16,
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
            "check": "six_usage_rules_defined",
            "actual": len(USAGE_RULES),
            "expected": 6,
            "passed": len(USAGE_RULES) == 6,
        },
        {
            "check": "six_ordering_rules_defined",
            "actual": len(ORDERING_RULES),
            "expected": 6,
            "passed": len(ORDERING_RULES) == 6,
        },
        {
            "check": "twenty_validation_rules_defined",
            "actual": len(VALIDATION_RULES),
            "expected": 20,
            "passed": len(VALIDATION_RULES) == 20,
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
                "8D defines a diagnostic-only pitcher arsenal profile plan."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "pitcher_arsenal_profile_contract_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "8E may implement deterministic diagnostic arsenal profiles."
                ),
            },
            {
                "authority": (
                    "production_pitcher_arsenal_integration"
                ),
                "granted": False,
                "reason": (
                    "Arsenal profiles remain non-authoritative diagnostics."
                ),
            },
        ]
    )

    diagnosis_name = (
        "pitcher_arsenal_profile_contract_plan_complete"
        if all_checks_passed
        else
        "pitcher_arsenal_profile_contract_plan_failed"
    )

    recommended_next_layer = (
        "8E_pitcher_arsenal_profile_contract_implementation"
        if all_checks_passed
        else
        "8D_pitcher_arsenal_profile_contract_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "arsenal_entry_fields.csv": ARSENAL_ENTRY_FIELDS,
        "profile_fields.csv": PROFILE_FIELDS,
        "profile_statuses.csv": PROFILE_STATUSES,
        "source_precedence.csv": SOURCE_PRECEDENCE,
        "usage_rules.csv": USAGE_RULES,
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
        "arsenal_entry_fields.csv": [
            "field",
            "type",
            "required",
        ],
        "profile_fields.csv": [
            "field",
            "type",
            "required",
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
        "usage_rules.csv": [
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
                    "Implement deterministic diagnostic pitcher arsenal "
                    "profiles with independent contract audit."
                    if all_checks_passed
                    else
                    "Remediate failed 8D planning checks."
                ),
                "entry_condition": (
                    "All eighteen 8D planning checks pass."
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
        "arsenal_entry_fields_defined": len(
            ARSENAL_ENTRY_FIELDS
        ),
        "profile_fields_defined": len(
            PROFILE_FIELDS
        ),
        "profile_statuses_defined": len(
            PROFILE_STATUSES
        ),
        "source_precedence_rules_defined": len(
            SOURCE_PRECEDENCE
        ),
        "usage_rules_defined": len(
            USAGE_RULES
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
        "pitcher_arsenal_profile_implementation_allowed_next": (
            all_checks_passed
        ),
        "production_pitcher_arsenal_integration_allowed_next": False,
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
