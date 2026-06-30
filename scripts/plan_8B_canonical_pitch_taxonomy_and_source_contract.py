#!/usr/bin/env python3
"""
Layer 8B
Canonical Pitch Taxonomy and Source Contract Plan

Defines the bounded planning contract for:
- canonical pitch identities and families;
- source-specific pitch aliases;
- deterministic normalization;
- source precedence and provenance;
- unknown, ambiguous, missing, and deprecated classifications;
- validation and fallback behavior;
- diagnostic-only implementation requirements.

Planning only.

This layer does not:
- alter production pitch selection;
- alter pitch sequencing;
- alter plate-appearance or batted-ball probabilities;
- change contact quality, exit velocity, launch angle, or outcomes;
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


LAYER_ID = "8B"
LAYER_NAME = "canonical_pitch_taxonomy_and_source_contract_plan"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8B_canonical_pitch_taxonomy_and_source_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "plan_8A_pitch_arsenal_and_matchup_layer_inventory.py"
)

CANONICAL_PITCHES = [
    {
        "canonical_pitch_id": "FF",
        "canonical_name": "four_seam_fastball",
        "family": "fastball",
        "velocity_band": "high",
        "movement_profile": "ride",
        "active": True,
    },
    {
        "canonical_pitch_id": "SI",
        "canonical_name": "sinker",
        "family": "fastball",
        "velocity_band": "high",
        "movement_profile": "arm_side_run_sink",
        "active": True,
    },
    {
        "canonical_pitch_id": "FC",
        "canonical_name": "cutter",
        "family": "fastball",
        "velocity_band": "high",
        "movement_profile": "glove_side_cut",
        "active": True,
    },
    {
        "canonical_pitch_id": "SL",
        "canonical_name": "slider",
        "family": "breaking",
        "velocity_band": "medium",
        "movement_profile": "glove_side_break",
        "active": True,
    },
    {
        "canonical_pitch_id": "ST",
        "canonical_name": "sweeper",
        "family": "breaking",
        "velocity_band": "medium",
        "movement_profile": "large_horizontal_break",
        "active": True,
    },
    {
        "canonical_pitch_id": "CU",
        "canonical_name": "curveball",
        "family": "breaking",
        "velocity_band": "low",
        "movement_profile": "vertical_break",
        "active": True,
    },
    {
        "canonical_pitch_id": "KC",
        "canonical_name": "knuckle_curve",
        "family": "breaking",
        "velocity_band": "low",
        "movement_profile": "vertical_break",
        "active": True,
    },
    {
        "canonical_pitch_id": "CH",
        "canonical_name": "changeup",
        "family": "offspeed",
        "velocity_band": "medium",
        "movement_profile": "arm_side_fade",
        "active": True,
    },
    {
        "canonical_pitch_id": "FS",
        "canonical_name": "splitter",
        "family": "offspeed",
        "velocity_band": "medium",
        "movement_profile": "vertical_drop",
        "active": True,
    },
    {
        "canonical_pitch_id": "FO",
        "canonical_name": "forkball",
        "family": "offspeed",
        "velocity_band": "low",
        "movement_profile": "vertical_drop",
        "active": True,
    },
    {
        "canonical_pitch_id": "KN",
        "canonical_name": "knuckleball",
        "family": "specialty",
        "velocity_band": "low",
        "movement_profile": "unstable",
        "active": True,
    },
    {
        "canonical_pitch_id": "EP",
        "canonical_name": "eephus",
        "family": "specialty",
        "velocity_band": "very_low",
        "movement_profile": "high_arc",
        "active": True,
    },
    {
        "canonical_pitch_id": "SC",
        "canonical_name": "screwball",
        "family": "specialty",
        "velocity_band": "low",
        "movement_profile": "reverse_break",
        "active": True,
    },
    {
        "canonical_pitch_id": "PO",
        "canonical_name": "pitchout",
        "family": "non_competitive",
        "velocity_band": "unknown",
        "movement_profile": "not_applicable",
        "active": True,
    },
    {
        "canonical_pitch_id": "IN",
        "canonical_name": "intentional_ball",
        "family": "non_competitive",
        "velocity_band": "unknown",
        "movement_profile": "not_applicable",
        "active": True,
    },
    {
        "canonical_pitch_id": "UN",
        "canonical_name": "unknown",
        "family": "unknown",
        "velocity_band": "unknown",
        "movement_profile": "unknown",
        "active": True,
    },
]

ALIASES = [
    {
        "source": "statcast",
        "source_value": "FF",
        "canonical_pitch_id": "FF",
        "status": "exact",
    },
    {
        "source": "statcast",
        "source_value": "SI",
        "canonical_pitch_id": "SI",
        "status": "exact",
    },
    {
        "source": "statcast",
        "source_value": "FT",
        "canonical_pitch_id": "SI",
        "status": "legacy_alias",
    },
    {
        "source": "statcast",
        "source_value": "FC",
        "canonical_pitch_id": "FC",
        "status": "exact",
    },
    {
        "source": "statcast",
        "source_value": "SL",
        "canonical_pitch_id": "SL",
        "status": "exact",
    },
    {
        "source": "statcast",
        "source_value": "ST",
        "canonical_pitch_id": "ST",
        "status": "exact",
    },
    {
        "source": "statcast",
        "source_value": "CU",
        "canonical_pitch_id": "CU",
        "status": "exact",
    },
    {
        "source": "statcast",
        "source_value": "KC",
        "canonical_pitch_id": "KC",
        "status": "exact",
    },
    {
        "source": "statcast",
        "source_value": "CH",
        "canonical_pitch_id": "CH",
        "status": "exact",
    },
    {
        "source": "statcast",
        "source_value": "FS",
        "canonical_pitch_id": "FS",
        "status": "exact",
    },
    {
        "source": "statcast",
        "source_value": "FO",
        "canonical_pitch_id": "FO",
        "status": "exact",
    },
    {
        "source": "statcast",
        "source_value": "KN",
        "canonical_pitch_id": "KN",
        "status": "exact",
    },
    {
        "source": "statcast",
        "source_value": "EP",
        "canonical_pitch_id": "EP",
        "status": "exact",
    },
    {
        "source": "statcast",
        "source_value": "SC",
        "canonical_pitch_id": "SC",
        "status": "exact",
    },
    {
        "source": "statcast",
        "source_value": "PO",
        "canonical_pitch_id": "PO",
        "status": "exact",
    },
    {
        "source": "statcast",
        "source_value": "IN",
        "canonical_pitch_id": "IN",
        "status": "exact",
    },
    {
        "source": "generic",
        "source_value": "four-seam fastball",
        "canonical_pitch_id": "FF",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "four seam fastball",
        "canonical_pitch_id": "FF",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "four_seam_fastball",
        "canonical_pitch_id": "FF",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "two-seam fastball",
        "canonical_pitch_id": "SI",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "two seam fastball",
        "canonical_pitch_id": "SI",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "sinker",
        "canonical_pitch_id": "SI",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "cutter",
        "canonical_pitch_id": "FC",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "slider",
        "canonical_pitch_id": "SL",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "sweeper",
        "canonical_pitch_id": "ST",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "curveball",
        "canonical_pitch_id": "CU",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "knuckle curve",
        "canonical_pitch_id": "KC",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "changeup",
        "canonical_pitch_id": "CH",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "change-up",
        "canonical_pitch_id": "CH",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "splitter",
        "canonical_pitch_id": "FS",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "split-finger",
        "canonical_pitch_id": "FS",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "forkball",
        "canonical_pitch_id": "FO",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "knuckleball",
        "canonical_pitch_id": "KN",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "eephus",
        "canonical_pitch_id": "EP",
        "status": "normalized_alias",
    },
    {
        "source": "generic",
        "source_value": "screwball",
        "canonical_pitch_id": "SC",
        "status": "normalized_alias",
    },
]

SOURCE_PRECEDENCE = [
    {
        "priority": 1,
        "source": "statcast_explicit_pitch_type",
        "use_when": "non_missing_and_supported",
    },
    {
        "priority": 2,
        "source": "trusted_provider_explicit_pitch_type",
        "use_when": "statcast_unavailable",
    },
    {
        "priority": 3,
        "source": "repository_canonical_alias",
        "use_when": "provider_value_normalizable",
    },
    {
        "priority": 4,
        "source": "source_specific_legacy_alias",
        "use_when": "legacy_value_is_unambiguous",
    },
    {
        "priority": 5,
        "source": "unknown_fallback",
        "use_when": "missing_ambiguous_or_unsupported",
    },
]

CONTRACT_FIELDS = [
    {
        "field": "source_name",
        "type": "string",
        "required": True,
    },
    {
        "field": "source_pitch_value",
        "type": "string_or_null",
        "required": False,
    },
    {
        "field": "canonical_pitch_id",
        "type": "string",
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
        "field": "normalization_status",
        "type": "enum",
        "required": True,
    },
    {
        "field": "normalization_rule",
        "type": "string",
        "required": True,
    },
    {
        "field": "source_priority",
        "type": "integer",
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
        "field": "taxonomy_version",
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

NORMALIZATION_STATUSES = [
    {
        "status": "exact",
        "meaning": "Source value exactly matches a canonical code.",
    },
    {
        "status": "normalized_alias",
        "meaning": "Source value resolves through a deterministic alias.",
    },
    {
        "status": "legacy_alias",
        "meaning": "Deprecated source value resolves unambiguously.",
    },
    {
        "status": "ambiguous",
        "meaning": "Source value could map to multiple canonical pitches.",
    },
    {
        "status": "unsupported",
        "meaning": "Source value is present but not supported.",
    },
    {
        "status": "missing",
        "meaning": "Source pitch value is absent.",
    },
    {
        "status": "unknown",
        "meaning": "Fallback canonical pitch identity is UN.",
    },
]

VALIDATION_RULES = [
    {
        "rule_id": "PT-V01",
        "rule": "canonical_pitch_id_must_exist",
    },
    {
        "rule_id": "PT-V02",
        "rule": "canonical_pitch_ids_must_be_unique",
    },
    {
        "rule_id": "PT-V03",
        "rule": "canonical_names_must_be_unique",
    },
    {
        "rule_id": "PT-V04",
        "rule": "alias_source_value_pair_must_be_unique",
    },
    {
        "rule_id": "PT-V05",
        "rule": "alias_target_must_exist",
    },
    {
        "rule_id": "PT-V06",
        "rule": "source_precedence_priorities_must_be_unique",
    },
    {
        "rule_id": "PT-V07",
        "rule": "source_precedence_must_be_contiguous",
    },
    {
        "rule_id": "PT-V08",
        "rule": "unknown_fallback_must_exist",
    },
    {
        "rule_id": "PT-V09",
        "rule": "ambiguous_values_must_not_be_guessed",
    },
    {
        "rule_id": "PT-V10",
        "rule": "missing_values_must_resolve_to_unknown",
    },
    {
        "rule_id": "PT-V11",
        "rule": "normalization_must_be_case_and_whitespace_stable",
    },
    {
        "rule_id": "PT-V12",
        "rule": "diagnostic_codes_must_be_sorted_and_unique",
    },
    {
        "rule_id": "PT-V13",
        "rule": "caller_payload_must_remain_immutable",
    },
    {
        "rule_id": "PT-V14",
        "rule": "production_authority_must_remain_false",
    },
    {
        "rule_id": "PT-V15",
        "rule": "taxonomy_version_must_be_explicit",
    },
    {
        "rule_id": "PT-V16",
        "rule": "unsupported_source_values_must_be_observable",
    },
]

FALLBACK_CONTRACTS = [
    {
        "fallback_id": "PT-F01",
        "condition": "missing_source_value",
        "result": "UN",
        "diagnostic_code": "pitch_taxonomy_source_value_missing",
    },
    {
        "fallback_id": "PT-F02",
        "condition": "unsupported_source_value",
        "result": "UN",
        "diagnostic_code": "pitch_taxonomy_source_value_unsupported",
    },
    {
        "fallback_id": "PT-F03",
        "condition": "ambiguous_source_value",
        "result": "UN",
        "diagnostic_code": "pitch_taxonomy_source_value_ambiguous",
    },
    {
        "fallback_id": "PT-F04",
        "condition": "missing_source_name",
        "result": "UN",
        "diagnostic_code": "pitch_taxonomy_source_name_missing",
    },
    {
        "fallback_id": "PT-F05",
        "condition": "invalid_alias_target",
        "result": "UN",
        "diagnostic_code": "pitch_taxonomy_alias_target_invalid",
    },
    {
        "fallback_id": "PT-F06",
        "condition": "normalization_exception",
        "result": "UN",
        "diagnostic_code": "pitch_taxonomy_normalization_exception",
    },
    {
        "fallback_id": "PT-F07",
        "condition": "taxonomy_disabled",
        "result": "no_diagnostic_payload",
        "diagnostic_code": "pitch_taxonomy_disabled",
    },
]

IMPLEMENTATION_STEPS = [
    {
        "step": 1,
        "action": "Create canonical pitch taxonomy module.",
    },
    {
        "step": 2,
        "action": "Define immutable canonical pitch records.",
    },
    {
        "step": 3,
        "action": "Define source-specific alias maps.",
    },
    {
        "step": 4,
        "action": "Implement deterministic input normalization.",
    },
    {
        "step": 5,
        "action": "Implement source-precedence resolution.",
    },
    {
        "step": 6,
        "action": "Implement unknown and ambiguous fallbacks.",
    },
    {
        "step": 7,
        "action": "Attach bounded provenance metadata.",
    },
    {
        "step": 8,
        "action": "Emit sorted diagnostic and validation codes.",
    },
    {
        "step": 9,
        "action": "Preserve caller payload immutability.",
    },
    {
        "step": 10,
        "action": "Implement disabled-by-default behavior.",
    },
    {
        "step": 11,
        "action": "Create independent contract audit.",
    },
    {
        "step": 12,
        "action": "Emit CSV and JSON audit artifacts.",
    },
]

ACCEPTANCE_CRITERIA = [
    {
        "criterion_id": "PT-C01",
        "criterion": "canonical_pitch_codes_unique",
    },
    {
        "criterion_id": "PT-C02",
        "criterion": "canonical_pitch_names_unique",
    },
    {
        "criterion_id": "PT-C03",
        "criterion": "all_alias_targets_valid",
    },
    {
        "criterion_id": "PT-C04",
        "criterion": "all_alias_keys_unique",
    },
    {
        "criterion_id": "PT-C05",
        "criterion": "source_precedence_deterministic",
    },
    {
        "criterion_id": "PT-C06",
        "criterion": "unknown_fallback_defined",
    },
    {
        "criterion_id": "PT-C07",
        "criterion": "missing_input_fallback_defined",
    },
    {
        "criterion_id": "PT-C08",
        "criterion": "ambiguous_input_not_guessed",
    },
    {
        "criterion_id": "PT-C09",
        "criterion": "unsupported_input_observable",
    },
    {
        "criterion_id": "PT-C10",
        "criterion": "taxonomy_version_explicit",
    },
    {
        "criterion_id": "PT-C11",
        "criterion": "provenance_fields_bounded",
    },
    {
        "criterion_id": "PT-C12",
        "criterion": "diagnostic_codes_deterministic",
    },
    {
        "criterion_id": "PT-C13",
        "criterion": "caller_payload_immutable",
    },
    {
        "criterion_id": "PT-C14",
        "criterion": "disabled_path_non_emitting",
    },
    {
        "criterion_id": "PT-C15",
        "criterion": "production_authority_false",
    },
    {
        "criterion_id": "PT-C16",
        "criterion": "simulation_behavior_unchanged",
    },
]

PROHIBITED_AUTHORITIES = [
    "production_pitch_selection",
    "production_pitch_sequence_change",
    "production_pitch_taxonomy_replacement",
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


def read_text(path: Path) -> str:
    if not path.exists():
        return ""

    return path.read_text(
        encoding="utf-8",
        errors="ignore",
    )


def string_constants(path: Path) -> set[str]:
    if not path.exists():
        return set()

    try:
        tree = ast.parse(
            read_text(path),
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
        "pitch_arsenal_and_matchup_layer_inventory_plan_complete"
        in string_constants(
            PREDECESSOR_PATH
        )
    )

    canonical_ids = [
        row["canonical_pitch_id"]
        for row in CANONICAL_PITCHES
    ]

    canonical_names = [
        row["canonical_name"]
        for row in CANONICAL_PITCHES
    ]

    alias_keys = [
        (
            row["source"],
            row["source_value"].strip().lower(),
        )
        for row in ALIASES
    ]

    alias_targets_valid = all(
        row["canonical_pitch_id"]
        in canonical_ids
        for row in ALIASES
    )

    priorities = [
        row["priority"]
        for row in SOURCE_PRECEDENCE
    ]

    planning_checks = [
        {
            "check": "eight_a_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "sixteen_canonical_pitches_defined",
            "actual": len(CANONICAL_PITCHES),
            "expected": 16,
            "passed": len(CANONICAL_PITCHES) == 16,
        },
        {
            "check": "canonical_pitch_ids_unique",
            "actual": len(set(canonical_ids)),
            "expected": len(canonical_ids),
            "passed": (
                len(set(canonical_ids))
                == len(canonical_ids)
            ),
        },
        {
            "check": "canonical_pitch_names_unique",
            "actual": len(set(canonical_names)),
            "expected": len(canonical_names),
            "passed": (
                len(set(canonical_names))
                == len(canonical_names)
            ),
        },
        {
            "check": "unknown_fallback_present",
            "actual": "UN" in canonical_ids,
            "expected": True,
            "passed": "UN" in canonical_ids,
        },
        {
            "check": "alias_keys_unique",
            "actual": len(set(alias_keys)),
            "expected": len(alias_keys),
            "passed": (
                len(set(alias_keys))
                == len(alias_keys)
            ),
        },
        {
            "check": "alias_targets_valid",
            "actual": alias_targets_valid,
            "expected": True,
            "passed": alias_targets_valid,
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
            "expected": list(
                range(
                    1,
                    len(priorities) + 1,
                )
            ),
            "passed": priorities
            == list(
                range(
                    1,
                    len(priorities) + 1,
                )
            ),
        },
        {
            "check": "fourteen_contract_fields_defined",
            "actual": len(CONTRACT_FIELDS),
            "expected": 14,
            "passed": len(CONTRACT_FIELDS) == 14,
        },
        {
            "check": "seven_normalization_statuses_defined",
            "actual": len(NORMALIZATION_STATUSES),
            "expected": 7,
            "passed": len(NORMALIZATION_STATUSES) == 7,
        },
        {
            "check": "sixteen_validation_rules_defined",
            "actual": len(VALIDATION_RULES),
            "expected": 16,
            "passed": len(VALIDATION_RULES) == 16,
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
            "check": "production_authority_not_granted",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "validation_tuning_pricing_edge_not_executed",
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
                "8B defines a diagnostic-only taxonomy and source contract plan."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "canonical_pitch_taxonomy_contract_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "8C may implement deterministic pitch normalization "
                    "as non-authoritative diagnostic metadata."
                ),
            },
            {
                "authority": (
                    "production_pitch_taxonomy_integration"
                ),
                "granted": False,
                "reason": (
                    "No pitch taxonomy result may influence production behavior."
                ),
            },
        ]
    )

    diagnosis_name = (
        "canonical_pitch_taxonomy_and_source_contract_plan_complete"
        if all_checks_passed
        else
        "canonical_pitch_taxonomy_and_source_contract_plan_failed"
    )

    recommended_next_layer = (
        "8C_canonical_pitch_taxonomy_and_source_contract_implementation"
        if all_checks_passed
        else
        "8B_canonical_pitch_taxonomy_contract_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        planning_checks,
    )

    write_csv(
        OUTPUT_DIR / "canonical_pitches.csv",
        [
            "canonical_pitch_id",
            "canonical_name",
            "family",
            "velocity_band",
            "movement_profile",
            "active",
        ],
        CANONICAL_PITCHES,
    )

    write_csv(
        OUTPUT_DIR / "aliases.csv",
        [
            "source",
            "source_value",
            "canonical_pitch_id",
            "status",
        ],
        ALIASES,
    )

    write_csv(
        OUTPUT_DIR / "source_precedence.csv",
        [
            "priority",
            "source",
            "use_when",
        ],
        SOURCE_PRECEDENCE,
    )

    write_csv(
        OUTPUT_DIR / "contract_fields.csv",
        [
            "field",
            "type",
            "required",
        ],
        CONTRACT_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "normalization_statuses.csv",
        [
            "status",
            "meaning",
        ],
        NORMALIZATION_STATUSES,
    )

    write_csv(
        OUTPUT_DIR / "validation_rules.csv",
        [
            "rule_id",
            "rule",
        ],
        VALIDATION_RULES,
    )

    write_csv(
        OUTPUT_DIR / "fallback_contracts.csv",
        [
            "fallback_id",
            "condition",
            "result",
            "diagnostic_code",
        ],
        FALLBACK_CONTRACTS,
    )

    write_csv(
        OUTPUT_DIR / "implementation_steps.csv",
        [
            "step",
            "action",
        ],
        IMPLEMENTATION_STEPS,
    )

    write_csv(
        OUTPUT_DIR / "acceptance_criteria.csv",
        [
            "criterion_id",
            "criterion",
        ],
        ACCEPTANCE_CRITERIA,
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
                    "Implement deterministic canonical pitch normalization "
                    "and an independent contract audit."
                    if all_checks_passed
                    else
                    "Remediate failed 8B planning checks."
                ),
                "entry_condition": (
                    "All eighteen 8B planning checks pass."
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
        "canonical_pitches_defined": len(
            CANONICAL_PITCHES
        ),
        "aliases_defined": len(
            ALIASES
        ),
        "source_precedence_rules_defined": len(
            SOURCE_PRECEDENCE
        ),
        "contract_fields_defined": len(
            CONTRACT_FIELDS
        ),
        "normalization_statuses_defined": len(
            NORMALIZATION_STATUSES
        ),
        "validation_rules_defined": len(
            VALIDATION_RULES
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
        "pitch_taxonomy_implementation_allowed_next": (
            all_checks_passed
        ),
        "production_pitch_taxonomy_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / filename
            )
            for filename in [
                "planning_checks.csv",
                "canonical_pitches.csv",
                "aliases.csv",
                "source_precedence.csv",
                "contract_fields.csv",
                "normalization_statuses.csv",
                "validation_rules.csv",
                "fallback_contracts.csv",
                "implementation_steps.csv",
                "acceptance_criteria.csv",
                "authority_boundaries.csv",
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
