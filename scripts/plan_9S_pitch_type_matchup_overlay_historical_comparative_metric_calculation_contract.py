#!/usr/bin/env python3
"""
Layer 9S
Pitch-Type Matchup Overlay Historical Comparative Metric Calculation Contract Plan

Plans the bounded metric-calculation contract for eligible Layer 9R historical
baseline/augmented comparison pairs.

Planning only.

This layer defines:

- authorized deterministic metric formulas;
- prediction/outcome type compatibility;
- paired baseline/augmented delta conventions;
- aggregation, support, clipping, missingness, and lineage rules;
- deterministic metric-record fields, identities, statuses, and artifacts;
- authority boundaries for Layer 9T.

This layer does not:

- calculate any comparative metric;
- estimate uncertainty or statistical significance;
- declare superiority, equivalence, activation, or production readiness;
- execute backtests, dataset splits, model training, tuning, or threshold selection;
- modify production probabilities, simulations, pricing, markets, or bets.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9S"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "metric_calculation_contract_plan"
)

PLAN_VERSION = (
    "layer_9S_historical_comparative_metric_calculation_contract_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9S_pitch_type_matchup_overlay_"
    "historical_comparative_metric_calculation_contract_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9R_pitch_type_matchup_overlay_"
    "historical_comparative_evaluation_contract.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9R_historical_comparative_evaluation_contract_v1"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "evaluation_contract_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_comparative_metric_calculation_contract_planning"
)


METRIC_DEFINITIONS = [
    {
        "metric_id": "HCMET-M01",
        "metric_name": "brier_score",
        "metric_family": "proper_scoring_rule",
        "prediction_value_type": "probability",
        "outcome_type": "binary",
        "formula": "(prediction_value - outcome_value) ** 2",
        "aggregation": "mean",
        "better_direction": "lower",
        "pair_delta_formula": (
            "augmented_metric_value - baseline_metric_value"
        ),
    },
    {
        "metric_id": "HCMET-M02",
        "metric_name": "log_loss",
        "metric_family": "proper_scoring_rule",
        "prediction_value_type": "probability",
        "outcome_type": "binary",
        "formula": (
            "-(outcome_value * ln(clipped_prediction) + "
            "(1 - outcome_value) * ln(1 - clipped_prediction))"
        ),
        "aggregation": "mean",
        "better_direction": "lower",
        "pair_delta_formula": (
            "augmented_metric_value - baseline_metric_value"
        ),
    },
    {
        "metric_id": "HCMET-M03",
        "metric_name": "absolute_error",
        "metric_family": "absolute_error",
        "prediction_value_type": "numeric",
        "outcome_type": "numeric",
        "formula": "abs(prediction_value - outcome_value)",
        "aggregation": "mean",
        "better_direction": "lower",
        "pair_delta_formula": (
            "augmented_metric_value - baseline_metric_value"
        ),
    },
    {
        "metric_id": "HCMET-M04",
        "metric_name": "squared_error",
        "metric_family": "squared_error",
        "prediction_value_type": "numeric",
        "outcome_type": "numeric",
        "formula": "(prediction_value - outcome_value) ** 2",
        "aggregation": "mean",
        "better_direction": "lower",
        "pair_delta_formula": (
            "augmented_metric_value - baseline_metric_value"
        ),
    },
    {
        "metric_id": "HCMET-M05",
        "metric_name": "root_mean_squared_error",
        "metric_family": "squared_error",
        "prediction_value_type": "numeric",
        "outcome_type": "numeric",
        "formula": "sqrt(mean(squared_error))",
        "aggregation": "root_of_mean",
        "better_direction": "lower",
        "pair_delta_formula": (
            "augmented_metric_value - baseline_metric_value"
        ),
    },
    {
        "metric_id": "HCMET-M06",
        "metric_name": "pair_count",
        "metric_family": "coverage",
        "prediction_value_type": "any",
        "outcome_type": "any",
        "formula": "count(comparison_eligible == true)",
        "aggregation": "count",
        "better_direction": "descriptive_only",
        "pair_delta_formula": "not_applicable",
    },
    {
        "metric_id": "HCMET-M07",
        "metric_name": "eligible_pair_rate",
        "metric_family": "coverage",
        "prediction_value_type": "any",
        "outcome_type": "any",
        "formula": "eligible_pair_count / candidate_pair_count",
        "aggregation": "ratio",
        "better_direction": "descriptive_only",
        "pair_delta_formula": "not_applicable",
    },
    {
        "metric_id": "HCMET-M08",
        "metric_name": "missing_pair_rate",
        "metric_family": "coverage",
        "prediction_value_type": "any",
        "outcome_type": "any",
        "formula": "excluded_missing_pair_count / candidate_pair_count",
        "aggregation": "ratio",
        "better_direction": "descriptive_only",
        "pair_delta_formula": "not_applicable",
    },
]

PAIR_DELTA_RULES = [
    {
        "rule_id": "HCMET-D01",
        "rule": (
            "For lower-is-better metrics, negative augmented-minus-baseline "
            "delta indicates lower augmented loss."
        ),
    },
    {
        "rule_id": "HCMET-D02",
        "rule": (
            "For higher-is-better metrics, positive augmented-minus-baseline "
            "delta indicates higher augmented score."
        ),
    },
    {
        "rule_id": "HCMET-D03",
        "rule": (
            "Descriptive coverage metrics do not emit performance deltas."
        ),
    },
    {
        "rule_id": "HCMET-D04",
        "rule": (
            "Metric direction is descriptive and does not authorize "
            "a superiority declaration."
        ),
    },
]

INPUT_COMPATIBILITY_RULES = [
    {
        "rule_id": "HCMET-I01",
        "rule": "comparison_status_must_equal_paired_eligible",
    },
    {
        "rule_id": "HCMET-I02",
        "rule": "comparison_eligible_must_equal_true",
    },
    {
        "rule_id": "HCMET-I03",
        "rule": "baseline_and_augmented_prediction_values_must_be_finite",
    },
    {
        "rule_id": "HCMET-I04",
        "rule": "outcome_value_must_be_finite_for_numeric_metrics",
    },
    {
        "rule_id": "HCMET-I05",
        "rule": "binary_metrics_require_outcome_value_in_zero_or_one",
    },
    {
        "rule_id": "HCMET-I06",
        "rule": "probability_metrics_require_prediction_values_in_zero_to_one",
    },
    {
        "rule_id": "HCMET-I07",
        "rule": "baseline_and_augmented_prediction_value_types_must_match",
    },
    {
        "rule_id": "HCMET-I08",
        "rule": "comparison_record_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HCMET-I09",
        "rule": "comparison_record_id_must_be_unique",
    },
]

CLIPPING_RULES = [
    {
        "rule_id": "HCMET-C01",
        "metric_name": "log_loss",
        "lower_bound": "1e-15",
        "upper_bound": "1 - 1e-15",
        "rule": "clip_probability_before_logarithm_only",
    },
    {
        "rule_id": "HCMET-C02",
        "metric_name": "all_other_metrics",
        "lower_bound": "not_applicable",
        "upper_bound": "not_applicable",
        "rule": "do_not_clip",
    },
    {
        "rule_id": "HCMET-C03",
        "metric_name": "all_metrics",
        "lower_bound": "not_applicable",
        "upper_bound": "not_applicable",
        "rule": "preserve_original_prediction_values_in_outputs",
    },
]

AGGREGATION_LEVELS = [
    {
        "aggregation_id": "HCMET-A01",
        "aggregation_name": "overall",
        "grouping_fields": "",
        "required": True,
    },
    {
        "aggregation_id": "HCMET-A02",
        "aggregation_name": "target_id",
        "grouping_fields": "target_id",
        "required": True,
    },
    {
        "aggregation_id": "HCMET-A03",
        "aggregation_name": "event_level",
        "grouping_fields": "event_level",
        "required": True,
    },
    {
        "aggregation_id": "HCMET-A04",
        "aggregation_name": "target_event_level",
        "grouping_fields": "target_id|event_level",
        "required": True,
    },
    {
        "aggregation_id": "HCMET-A05",
        "aggregation_name": "game_date",
        "grouping_fields": "game_date",
        "required": False,
    },
    {
        "aggregation_id": "HCMET-A06",
        "aggregation_name": "model_contract_pair",
        "grouping_fields": (
            "baseline_model_contract_version|"
            "augmented_model_contract_version"
        ),
        "required": True,
    },
    {
        "aggregation_id": "HCMET-A07",
        "aggregation_name": "overlay_contract_version",
        "grouping_fields": "augmented_overlay_contract_version",
        "required": True,
    },
]

SUPPORT_RULES = [
    {
        "rule_id": "HCMET-S01",
        "rule": "every_metric_record_must_include_candidate_pair_count",
    },
    {
        "rule_id": "HCMET-S02",
        "rule": "every_metric_record_must_include_eligible_pair_count",
    },
    {
        "rule_id": "HCMET-S03",
        "rule": "every_metric_record_must_include_excluded_pair_count",
    },
    {
        "rule_id": "HCMET-S04",
        "rule": "performance_metrics_require_minimum_eligible_pair_count",
    },
    {
        "rule_id": "HCMET-S05",
        "rule": "minimum_eligible_pair_count_is_versioned_as_two_for_contract_tests",
    },
    {
        "rule_id": "HCMET-S06",
        "rule": "insufficient_support_records_must_be_explicitly_suppressed",
    },
    {
        "rule_id": "HCMET-S07",
        "rule": "coverage_metrics_may_emit_with_zero_eligible_pairs",
    },
    {
        "rule_id": "HCMET-S08",
        "rule": "suppression_must_not_remove_counts_from_reconciliation",
    },
]

METRIC_RECORD_FIELDS = [
    {"ordinal": 1, "field": "metric_contract_version"},
    {"ordinal": 2, "field": "metric_record_id"},
    {"ordinal": 3, "field": "metric_name"},
    {"ordinal": 4, "field": "metric_family"},
    {"ordinal": 5, "field": "aggregation_name"},
    {"ordinal": 6, "field": "aggregation_key"},
    {"ordinal": 7, "field": "target_id"},
    {"ordinal": 8, "field": "event_level"},
    {"ordinal": 9, "field": "game_date"},
    {"ordinal": 10, "field": "baseline_model_contract_version"},
    {"ordinal": 11, "field": "augmented_model_contract_version"},
    {"ordinal": 12, "field": "augmented_overlay_contract_version"},
    {"ordinal": 13, "field": "candidate_pair_count"},
    {"ordinal": 14, "field": "eligible_pair_count"},
    {"ordinal": 15, "field": "excluded_pair_count"},
    {"ordinal": 16, "field": "baseline_metric_value"},
    {"ordinal": 17, "field": "augmented_metric_value"},
    {"ordinal": 18, "field": "augmented_minus_baseline_delta"},
    {"ordinal": 19, "field": "better_direction"},
    {"ordinal": 20, "field": "minimum_support_required"},
    {"ordinal": 21, "field": "support_satisfied"},
    {"ordinal": 22, "field": "metric_status"},
    {"ordinal": 23, "field": "metric_exclusion_codes"},
    {"ordinal": 24, "field": "source_comparison_digest"},
    {"ordinal": 25, "field": "metric_identity_digest"},
    {"ordinal": 26, "field": "metric_record_digest"},
]

METRIC_STATUSES = [
    {
        "status": "metric_eligible",
        "metric_value_emitted": True,
    },
    {
        "status": "coverage_metric_eligible",
        "metric_value_emitted": True,
    },
    {
        "status": "insufficient_support",
        "metric_value_emitted": False,
    },
    {
        "status": "prediction_value_type_incompatible",
        "metric_value_emitted": False,
    },
    {
        "status": "outcome_type_incompatible",
        "metric_value_emitted": False,
    },
    {
        "status": "prediction_value_invalid",
        "metric_value_emitted": False,
    },
    {
        "status": "outcome_value_invalid",
        "metric_value_emitted": False,
    },
    {
        "status": "comparison_lineage_invalid",
        "metric_value_emitted": False,
    },
    {
        "status": "metric_definition_invalid",
        "metric_value_emitted": False,
    },
]

EXCLUSION_CODES = [
    {
        "code": "historical_metric_insufficient_support",
        "category": "support",
    },
    {
        "code": "historical_metric_prediction_value_type_incompatible",
        "category": "contract",
    },
    {
        "code": "historical_metric_outcome_type_incompatible",
        "category": "contract",
    },
    {
        "code": "historical_metric_prediction_value_invalid",
        "category": "value",
    },
    {
        "code": "historical_metric_outcome_value_invalid",
        "category": "value",
    },
    {
        "code": "historical_metric_probability_out_of_bounds",
        "category": "value",
    },
    {
        "code": "historical_metric_binary_outcome_invalid",
        "category": "value",
    },
    {
        "code": "historical_metric_comparison_lineage_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_metric_duplicate_comparison_identity",
        "category": "cardinality",
    },
    {
        "code": "historical_metric_definition_invalid",
        "category": "definition",
    },
    {
        "code": "historical_metric_aggregation_invalid",
        "category": "aggregation",
    },
    {
        "code": "historical_metric_source_record_invalid",
        "category": "source",
    },
]

DETERMINISM_RULES = [
    {
        "rule_id": "HCMET-R01",
        "rule": "metric_records_must_be_independent_of_input_order",
    },
    {
        "rule_id": "HCMET-R02",
        "rule": "group_keys_must_use_canonical_json_serialization",
    },
    {
        "rule_id": "HCMET-R03",
        "rule": "floating_point_outputs_must_use_stable_rounding",
    },
    {
        "rule_id": "HCMET-R04",
        "rule": "metric_records_must_use_canonical_sort_order",
    },
    {
        "rule_id": "HCMET-R05",
        "rule": "all_metric_records_must_have_sha256_lineage",
    },
]

ORDERING_FIELDS = [
    {"ordinal": 1, "field": "aggregation_name"},
    {"ordinal": 2, "field": "aggregation_key"},
    {"ordinal": 3, "field": "metric_family"},
    {"ordinal": 4, "field": "metric_name"},
    {"ordinal": 5, "field": "target_id"},
    {"ordinal": 6, "field": "event_level"},
    {"ordinal": 7, "field": "game_date"},
    {"ordinal": 8, "field": "metric_record_id"},
]

IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "verify_layer_9s_plan_and_layer_9r_predecessor",
    },
    {
        "ordinal": 2,
        "step": "replay_layer_9r_comparative_evaluation_records",
    },
    {
        "ordinal": 3,
        "step": "validate_metric_definition_catalog",
    },
    {
        "ordinal": 4,
        "step": "validate_prediction_and_outcome_type_compatibility",
    },
    {
        "ordinal": 5,
        "step": "validate_probability_bounds_and_log_loss_clipping",
    },
    {
        "ordinal": 6,
        "step": "construct_canonical_aggregation_groups",
    },
    {
        "ordinal": 7,
        "step": "calculate_baseline_and_augmented_metric_values",
    },
    {
        "ordinal": 8,
        "step": "calculate_paired_augmented_minus_baseline_deltas",
    },
    {
        "ordinal": 9,
        "step": "calculate_coverage_counts_and_rates",
    },
    {
        "ordinal": 10,
        "step": "apply_minimum_support_and_suppression_rules",
    },
    {
        "ordinal": 11,
        "step": "derive_metric_identity_and_record_digests",
    },
    {
        "ordinal": 12,
        "step": "reconcile_candidate_eligible_and_excluded_counts",
    },
    {
        "ordinal": 13,
        "step": "replay_metric_calculation_under_reversed_input_order",
    },
    {
        "ordinal": 14,
        "step": "write_temporary_diagnostic_artifacts_only",
    },
]

DIAGNOSTIC_ARTIFACTS = [
    {"artifact": "planning_checks.csv"},
    {"artifact": "metric_definitions.csv"},
    {"artifact": "pair_delta_rules.csv"},
    {"artifact": "input_compatibility_rules.csv"},
    {"artifact": "clipping_rules.csv"},
    {"artifact": "aggregation_levels.csv"},
    {"artifact": "support_rules.csv"},
    {"artifact": "metric_record_field_contract.csv"},
    {"artifact": "metric_statuses.csv"},
    {"artifact": "exclusion_code_catalog.csv"},
    {"artifact": "determinism_rules.csv"},
    {"artifact": "ordering_fields.csv"},
    {"artifact": "implementation_steps.csv"},
    {"artifact": "authority_boundaries.csv"},
    {"artifact": "metric_calculation_plan_summary.json"},
    {"artifact": "diagnosis.json"},
]

PROHIBITED_AUTHORITIES = [
    "augmented_prediction_generation",
    "backtest_execution",
    "baseline_prediction_generation",
    "bet_recommendation",
    "canonical_probability_authority_change",
    "dataset_split_execution",
    "edge_detection",
    "historical_comparative_metric_calculation",
    "market_comparison",
    "model_training",
    "parameter_tuning",
    "pricing",
    "production_historical_prediction_materialization",
    "production_matchup_activation",
    "production_overlay_integration",
    "simulation_probability_change",
    "simulation_state_change",
    "statistical_significance_evaluation",
    "superiority_declaration",
    "threshold_tuning",
    "uncertainty_estimation",
]


def canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_payload(payload: Any) -> str:
    return hashlib.sha256(
        canonical_json_bytes(payload)
    ).hexdigest()


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


def write_csv(
    path: Path,
    fieldnames: Sequence[str],
    rows: Iterable[Mapping[str, Any]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(fieldnames),
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
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


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_constants = string_constants(
        PREDECESSOR_PATH
    )

    predecessor_verified = (
        PREDECESSOR_PATH.exists()
        and EXPECTED_PREDECESSOR_VERSION
        in predecessor_constants
        and EXPECTED_PREDECESSOR_DIAGNOSIS
        in predecessor_constants
        and EXPECTED_PREDECESSOR_AUTHORITY
        in predecessor_constants
    )

    metric_names = [
        row["metric_name"]
        for row in METRIC_DEFINITIONS
    ]

    field_names = [
        row["field"]
        for row in METRIC_RECORD_FIELDS
    ]

    checks = [
        {
            "check": "nine_r_predecessor_verified",
            "actual": predecessor_verified,
            "expected": True,
            "passed": predecessor_verified,
        },
        {
            "check": "eight_metric_definitions_defined",
            "actual": len(METRIC_DEFINITIONS),
            "expected": 8,
            "passed": (
                len(METRIC_DEFINITIONS) == 8
                and len(set(metric_names)) == 8
            ),
        },
        {
            "check": "four_pair_delta_rules_defined",
            "actual": len(PAIR_DELTA_RULES),
            "expected": 4,
            "passed": len(PAIR_DELTA_RULES) == 4,
        },
        {
            "check": "nine_input_compatibility_rules_defined",
            "actual": len(INPUT_COMPATIBILITY_RULES),
            "expected": 9,
            "passed": (
                len(INPUT_COMPATIBILITY_RULES) == 9
            ),
        },
        {
            "check": "three_clipping_rules_defined",
            "actual": len(CLIPPING_RULES),
            "expected": 3,
            "passed": len(CLIPPING_RULES) == 3,
        },
        {
            "check": "seven_aggregation_levels_defined",
            "actual": len(AGGREGATION_LEVELS),
            "expected": 7,
            "passed": len(AGGREGATION_LEVELS) == 7,
        },
        {
            "check": "eight_support_rules_defined",
            "actual": len(SUPPORT_RULES),
            "expected": 8,
            "passed": len(SUPPORT_RULES) == 8,
        },
        {
            "check": "twenty_six_metric_record_fields_defined",
            "actual": len(METRIC_RECORD_FIELDS),
            "expected": 26,
            "passed": (
                len(METRIC_RECORD_FIELDS) == 26
                and len(set(field_names)) == 26
            ),
        },
        {
            "check": "nine_metric_statuses_defined",
            "actual": len(METRIC_STATUSES),
            "expected": 9,
            "passed": len(METRIC_STATUSES) == 9,
        },
        {
            "check": "twelve_exclusion_codes_defined",
            "actual": len(EXCLUSION_CODES),
            "expected": 12,
            "passed": len(EXCLUSION_CODES) == 12,
        },
        {
            "check": "five_determinism_rules_defined",
            "actual": len(DETERMINISM_RULES),
            "expected": 5,
            "passed": len(DETERMINISM_RULES) == 5,
        },
        {
            "check": "eight_ordering_fields_defined",
            "actual": len(ORDERING_FIELDS),
            "expected": 8,
            "passed": len(ORDERING_FIELDS) == 8,
        },
        {
            "check": "fourteen_implementation_steps_defined",
            "actual": len(IMPLEMENTATION_STEPS),
            "expected": 14,
            "passed": len(IMPLEMENTATION_STEPS) == 14,
        },
        {
            "check": "sixteen_diagnostic_artifacts_defined",
            "actual": len(DIAGNOSTIC_ARTIFACTS),
            "expected": 16,
            "passed": len(DIAGNOSTIC_ARTIFACTS) == 16,
        },
        {
            "check": "brier_and_log_loss_defined",
            "actual": True,
            "expected": True,
            "passed": {
                "brier_score",
                "log_loss",
            }.issubset(set(metric_names)),
        },
        {
            "check": "error_metrics_defined",
            "actual": True,
            "expected": True,
            "passed": {
                "absolute_error",
                "squared_error",
                "root_mean_squared_error",
            }.issubset(set(metric_names)),
        },
        {
            "check": "coverage_metrics_defined",
            "actual": True,
            "expected": True,
            "passed": {
                "pair_count",
                "eligible_pair_rate",
                "missing_pair_rate",
            }.issubset(set(metric_names)),
        },
        {
            "check": "log_loss_clipping_defined",
            "actual": True,
            "expected": True,
            "passed": any(
                row["metric_name"] == "log_loss"
                and row["lower_bound"] == "1e-15"
                for row in CLIPPING_RULES
            ),
        },
        {
            "check": "minimum_support_versioned",
            "actual": True,
            "expected": True,
            "passed": any(
                row["rule"]
                == (
                    "minimum_eligible_pair_count_is_"
                    "versioned_as_two_for_contract_tests"
                )
                for row in SUPPORT_RULES
            ),
        },
        {
            "check": "metric_calculation_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "uncertainty_not_estimated",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "superiority_not_declared",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "production_and_betting_authority_absent",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
    ]

    all_checks_passed = all(
        bool(row["passed"])
        for row in checks
    )

    plan_digest = sha256_payload(
        {
            "plan_version": PLAN_VERSION,
            "metric_definitions": METRIC_DEFINITIONS,
            "pair_delta_rules": PAIR_DELTA_RULES,
            "input_compatibility_rules": (
                INPUT_COMPATIBILITY_RULES
            ),
            "clipping_rules": CLIPPING_RULES,
            "aggregation_levels": AGGREGATION_LEVELS,
            "support_rules": SUPPORT_RULES,
            "metric_record_fields": (
                METRIC_RECORD_FIELDS
            ),
            "metric_statuses": METRIC_STATUSES,
            "exclusion_codes": EXCLUSION_CODES,
            "determinism_rules": DETERMINISM_RULES,
            "ordering_fields": ORDERING_FIELDS,
            "implementation_steps": IMPLEMENTATION_STEPS,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_comparative_"
        "metric_calculation_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_comparative_"
        "metric_calculation_contract_plan_failed"
    )

    next_layer = (
        "9T_pitch_type_matchup_overlay_historical_comparative_"
        "metric_calculation_contract_implementation"
        if all_checks_passed
        else
        "9S_pitch_type_matchup_overlay_historical_comparative_"
        "metric_calculation_contract_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "metric_definitions.csv",
        [
            "metric_id",
            "metric_name",
            "metric_family",
            "prediction_value_type",
            "outcome_type",
            "formula",
            "aggregation",
            "better_direction",
            "pair_delta_formula",
        ],
        METRIC_DEFINITIONS,
    )

    write_csv(
        OUTPUT_DIR / "pair_delta_rules.csv",
        [
            "rule_id",
            "rule",
        ],
        PAIR_DELTA_RULES,
    )

    write_csv(
        OUTPUT_DIR
        / "input_compatibility_rules.csv",
        [
            "rule_id",
            "rule",
        ],
        INPUT_COMPATIBILITY_RULES,
    )

    write_csv(
        OUTPUT_DIR / "clipping_rules.csv",
        [
            "rule_id",
            "metric_name",
            "lower_bound",
            "upper_bound",
            "rule",
        ],
        CLIPPING_RULES,
    )

    write_csv(
        OUTPUT_DIR / "aggregation_levels.csv",
        [
            "aggregation_id",
            "aggregation_name",
            "grouping_fields",
            "required",
        ],
        AGGREGATION_LEVELS,
    )

    write_csv(
        OUTPUT_DIR / "support_rules.csv",
        [
            "rule_id",
            "rule",
        ],
        SUPPORT_RULES,
    )

    write_csv(
        OUTPUT_DIR
        / "metric_record_field_contract.csv",
        [
            "ordinal",
            "field",
        ],
        METRIC_RECORD_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "metric_statuses.csv",
        [
            "status",
            "metric_value_emitted",
        ],
        METRIC_STATUSES,
    )

    write_csv(
        OUTPUT_DIR
        / "exclusion_code_catalog.csv",
        [
            "code",
            "category",
        ],
        EXCLUSION_CODES,
    )

    write_csv(
        OUTPUT_DIR / "determinism_rules.csv",
        [
            "rule_id",
            "rule",
        ],
        DETERMINISM_RULES,
    )

    write_csv(
        OUTPUT_DIR / "ordering_fields.csv",
        [
            "ordinal",
            "field",
        ],
        ORDERING_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "implementation_steps.csv",
        [
            "ordinal",
            "step",
        ],
        IMPLEMENTATION_STEPS,
    )

    write_csv(
        OUTPUT_DIR / "authority_boundaries.csv",
        [
            "authority",
            "granted",
            "reason",
        ],
        [
            {
                "authority": authority,
                "granted": False,
                "reason": (
                    "Layer 9S is planning-only and grants "
                    "no metric execution, uncertainty, "
                    "superiority, production, market, or "
                    "betting authority."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_comparative_metric_"
                    "calculation_contract_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "Layer 9T may calculate only the bounded "
                    "deterministic temporary diagnostic metrics "
                    "defined by Layer 9S."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_verified": predecessor_verified,
        "metric_definitions": len(
            METRIC_DEFINITIONS
        ),
        "pair_delta_rules": len(
            PAIR_DELTA_RULES
        ),
        "input_compatibility_rules": len(
            INPUT_COMPATIBILITY_RULES
        ),
        "clipping_rules": len(
            CLIPPING_RULES
        ),
        "aggregation_levels": len(
            AGGREGATION_LEVELS
        ),
        "support_rules": len(
            SUPPORT_RULES
        ),
        "metric_record_fields": len(
            METRIC_RECORD_FIELDS
        ),
        "metric_statuses": len(
            METRIC_STATUSES
        ),
        "exclusion_codes": len(
            EXCLUSION_CODES
        ),
        "determinism_rules": len(
            DETERMINISM_RULES
        ),
        "ordering_fields": len(
            ORDERING_FIELDS
        ),
        "implementation_steps": len(
            IMPLEMENTATION_STEPS
        ),
        "planning_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "planning_checks_required": len(
            checks
        ),
        "plan_digest": plan_digest,
        "metric_records_materialized": 0,
        "comparative_metrics_calculated": 0,
        "uncertainty_estimates_calculated": 0,
        "superiority_decisions_emitted": 0,
        "production_predictions_generated": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed": (
            all_checks_passed
        ),
        "recommended_next_layer": (
            next_layer
        ),
    }

    write_json(
        OUTPUT_DIR
        / "metric_calculation_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": (
            all_checks_passed
        ),
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_comparative_metric_"
            "calculation_contract_implementation"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": sorted(
            PROHIBITED_AUTHORITIES
        ),
        "recommended_next_layer": (
            next_layer
        ),
        "output_directory": str(
            OUTPUT_DIR.relative_to(ROOT)
        ),
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(
        f"Layer: {LAYER_ID} — {LAYER_NAME}"
    )
    print(
        f"Plan version: {PLAN_VERSION}"
    )
    print(
        "Predecessor verified: "
        f"{predecessor_verified}"
    )
    print(
        "Planning checks passed: "
        f"{summary['planning_checks_passed']}/"
        f"{summary['planning_checks_required']}"
    )
    print(
        "Metric definitions: "
        f"{len(METRIC_DEFINITIONS)}"
    )
    print(
        "Pair delta rules: "
        f"{len(PAIR_DELTA_RULES)}"
    )
    print(
        "Input compatibility rules: "
        f"{len(INPUT_COMPATIBILITY_RULES)}"
    )
    print(
        f"Clipping rules: {len(CLIPPING_RULES)}"
    )
    print(
        "Aggregation levels: "
        f"{len(AGGREGATION_LEVELS)}"
    )
    print(
        f"Support rules: {len(SUPPORT_RULES)}"
    )
    print(
        "Metric record fields: "
        f"{len(METRIC_RECORD_FIELDS)}"
    )
    print(
        "Metric statuses: "
        f"{len(METRIC_STATUSES)}"
    )
    print(
        f"Exclusion codes: {len(EXCLUSION_CODES)}"
    )
    print(
        "Determinism rules: "
        f"{len(DETERMINISM_RULES)}"
    )
    print(
        f"Ordering fields: {len(ORDERING_FIELDS)}"
    )
    print(
        "Implementation steps: "
        f"{len(IMPLEMENTATION_STEPS)}"
    )
    print(
        "Metric records materialized: 0"
    )
    print(
        "Comparative metrics calculated: 0"
    )
    print(
        "Uncertainty estimates calculated: 0"
    )
    print(
        "Superiority decisions emitted: 0"
    )
    print(
        "Production predictions generated: 0"
    )
    print(
        "Production probabilities changed: 0"
    )
    print(
        "Market comparisons executed: 0"
    )
    print(
        "Betting edges calculated: 0"
    )
    print(
        f"Diagnosis: {diagnosis_name}"
    )
    print(
        "Authority granted: "
        f"{diagnosis['authority_granted']}"
    )
    print(
        "Recommended next layer: "
        f"{next_layer}"
    )
    print(
        "Artifacts: "
        f"{OUTPUT_DIR.relative_to(ROOT)}"
    )

    if not all_checks_passed:
        failed_checks = [
            row["check"]
            for row in checks
            if not row["passed"]
        ]

        print(
            "FAILED CHECKS: "
            + ", ".join(failed_checks)
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
