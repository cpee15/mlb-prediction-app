#!/usr/bin/env python3
"""
Layer 9Q
Pitch-Type Matchup Overlay Historical Comparative Evaluation Contract Plan

Plans the bounded contract for comparing paired historical baseline and
augmented prediction/outcome records produced under Layer 9P.

Planning only.

This layer defines:

- paired baseline/augmented comparison eligibility;
- comparison grains, identities, fields, statuses, and exclusions;
- metric families that a later implementation may calculate;
- aggregation and minimum-support boundaries;
- deterministic ordering, lineage, and replay requirements;
- authority boundaries for Layer 9R.

This layer does not:

- calculate predictive, calibration, discrimination, or incremental-value metrics;
- declare the augmented overlay better or worse;
- execute dataset splits, backtests, training, tuning, or threshold selection;
- generate production predictions;
- modify production probabilities, simulations, pricing, markets, or bets.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9Q"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "evaluation_contract_plan"
)

PLAN_VERSION = (
    "layer_9Q_historical_comparative_evaluation_contract_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9Q_pitch_type_matchup_overlay_"
    "historical_comparative_evaluation_contract_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9P_pitch_type_matchup_overlay_"
    "historical_prediction_outcome_join_contract.py"
)

EXPECTED_PREDECESSOR_JOIN_VERSION = (
    "layer_9P_historical_prediction_outcome_join_contract_v1"
)

EXPECTED_PREDECESSOR_PREDICTION_VERSION = (
    "layer_9P_synthetic_historical_prediction_contract_v1"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_prediction_"
    "outcome_join_contract_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_comparative_evaluation_contract_planning"
)


COMPARISON_GRAINS = [
    {
        "grain_id": "HCOMP-G01",
        "event_level": "event",
        "comparison_identity": (
            "target_id + game_id + event_sequence"
        ),
    },
    {
        "grain_id": "HCOMP-G02",
        "event_level": "plate_appearance",
        "comparison_identity": (
            "target_id + game_id + plate_appearance_id"
        ),
    },
    {
        "grain_id": "HCOMP-G03",
        "event_level": "pitch",
        "comparison_identity": (
            "target_id + game_id + plate_appearance_id + pitch_id"
        ),
    },
    {
        "grain_id": "HCOMP-G04",
        "event_level": "contact",
        "comparison_identity": (
            "target_id + game_id + plate_appearance_id + pitch_id"
        ),
    },
]

PAIRING_RULES = [
    {
        "rule_id": "HCOMP-P01",
        "rule": "baseline_join_status_must_be_matched_eligible",
    },
    {
        "rule_id": "HCOMP-P02",
        "rule": "augmented_join_status_must_be_matched_eligible",
    },
    {
        "rule_id": "HCOMP-P03",
        "rule": "baseline_and_augmented_evaluation_row_id_must_match",
    },
    {
        "rule_id": "HCOMP-P04",
        "rule": "baseline_and_augmented_target_id_must_match",
    },
    {
        "rule_id": "HCOMP-P05",
        "rule": "baseline_and_augmented_event_level_must_match",
    },
    {
        "rule_id": "HCOMP-P06",
        "rule": "baseline_and_augmented_game_id_must_match",
    },
    {
        "rule_id": "HCOMP-P07",
        "rule": "baseline_and_augmented_event_identity_must_match",
    },
    {
        "rule_id": "HCOMP-P08",
        "rule": "baseline_and_augmented_outcome_value_must_match",
    },
    {
        "rule_id": "HCOMP-P09",
        "rule": "baseline_and_augmented_outcome_availability_must_match",
    },
    {
        "rule_id": "HCOMP-P10",
        "rule": "baseline_prediction_variant_must_equal_baseline",
    },
    {
        "rule_id": "HCOMP-P11",
        "rule": "augmented_prediction_variant_must_equal_augmented",
    },
    {
        "rule_id": "HCOMP-P12",
        "rule": "prediction_value_types_must_match",
    },
    {
        "rule_id": "HCOMP-P13",
        "rule": "evaluation_row_digest_must_match",
    },
    {
        "rule_id": "HCOMP-P14",
        "rule": "each_evaluation_row_may_have_at_most_one_pair",
    },
    {
        "rule_id": "HCOMP-P15",
        "rule": "unpaired_or_invalid_rows_must_be_explicitly_classified",
    },
]

COMPARISON_FIELDS = [
    {
        "ordinal": 1,
        "field": "comparative_evaluation_contract_version",
        "role": "Pins comparison semantics.",
    },
    {
        "ordinal": 2,
        "field": "comparison_record_id",
        "role": "Stable identity for one baseline/augmented comparison pair.",
    },
    {
        "ordinal": 3,
        "field": "evaluation_row_id",
        "role": "Links both predictions to the same evaluation row.",
    },
    {
        "ordinal": 4,
        "field": "target_id",
        "role": "Identifies the evaluated target.",
    },
    {
        "ordinal": 5,
        "field": "event_level",
        "role": "Defines comparison grain.",
    },
    {
        "ordinal": 6,
        "field": "game_id",
        "role": "Scopes the pair to one game.",
    },
    {
        "ordinal": 7,
        "field": "game_date",
        "role": "Supports chronological aggregation.",
    },
    {
        "ordinal": 8,
        "field": "scheduled_start_utc",
        "role": "Preserves event-time context.",
    },
    {
        "ordinal": 9,
        "field": "plate_appearance_id",
        "role": "Conditional lower-grain identity.",
    },
    {
        "ordinal": 10,
        "field": "pitch_id",
        "role": "Conditional pitch/contact identity.",
    },
    {
        "ordinal": 11,
        "field": "event_sequence",
        "role": "Preserves deterministic event ordering.",
    },
    {
        "ordinal": 12,
        "field": "baseline_prediction_record_id",
        "role": "Links the baseline prediction.",
    },
    {
        "ordinal": 13,
        "field": "baseline_prediction_value",
        "role": "Stores the baseline value without judging it.",
    },
    {
        "ordinal": 14,
        "field": "baseline_model_contract_version",
        "role": "Pins baseline model semantics.",
    },
    {
        "ordinal": 15,
        "field": "baseline_prediction_provenance_digest",
        "role": "Preserves baseline lineage.",
    },
    {
        "ordinal": 16,
        "field": "augmented_prediction_record_id",
        "role": "Links the augmented prediction.",
    },
    {
        "ordinal": 17,
        "field": "augmented_prediction_value",
        "role": "Stores the augmented value without judging it.",
    },
    {
        "ordinal": 18,
        "field": "augmented_model_contract_version",
        "role": "Pins augmented model semantics.",
    },
    {
        "ordinal": 19,
        "field": "augmented_overlay_contract_version",
        "role": "Pins overlay semantics.",
    },
    {
        "ordinal": 20,
        "field": "augmented_prediction_provenance_digest",
        "role": "Preserves augmented lineage.",
    },
    {
        "ordinal": 21,
        "field": "prediction_value_type",
        "role": "Defines interpretation of both prediction values.",
    },
    {
        "ordinal": 22,
        "field": "outcome_value",
        "role": "Stores the common terminal outcome.",
    },
    {
        "ordinal": 23,
        "field": "outcome_available_at_utc",
        "role": "Preserves outcome availability lineage.",
    },
    {
        "ordinal": 24,
        "field": "evaluation_row_digest",
        "role": "Pins the shared evaluation source row.",
    },
    {
        "ordinal": 25,
        "field": "baseline_join_record_digest",
        "role": "Pins the baseline prediction/outcome join row.",
    },
    {
        "ordinal": 26,
        "field": "augmented_join_record_digest",
        "role": "Pins the augmented prediction/outcome join row.",
    },
    {
        "ordinal": 27,
        "field": "comparison_identity_digest",
        "role": "Pins pair identity.",
    },
    {
        "ordinal": 28,
        "field": "comparison_status",
        "role": "Classifies pair eligibility.",
    },
    {
        "ordinal": 29,
        "field": "comparison_eligible",
        "role": "Indicates whether later metric calculation is allowed.",
    },
    {
        "ordinal": 30,
        "field": "comparison_exclusion_codes",
        "role": "Explains ineligibility deterministically.",
    },
    {
        "ordinal": 31,
        "field": "comparison_record_digest",
        "role": "Provides immutable comparison-row integrity.",
    },
]

COMPARISON_STATUSES = [
    {
        "status": "paired_eligible",
        "comparison_eligible": True,
    },
    {
        "status": "baseline_prediction_missing",
        "comparison_eligible": False,
    },
    {
        "status": "augmented_prediction_missing",
        "comparison_eligible": False,
    },
    {
        "status": "baseline_join_ineligible",
        "comparison_eligible": False,
    },
    {
        "status": "augmented_join_ineligible",
        "comparison_eligible": False,
    },
    {
        "status": "evaluation_identity_mismatch",
        "comparison_eligible": False,
    },
    {
        "status": "outcome_mismatch",
        "comparison_eligible": False,
    },
    {
        "status": "prediction_value_type_mismatch",
        "comparison_eligible": False,
    },
    {
        "status": "lineage_mismatch",
        "comparison_eligible": False,
    },
    {
        "status": "duplicate_baseline",
        "comparison_eligible": False,
    },
    {
        "status": "duplicate_augmented",
        "comparison_eligible": False,
    },
    {
        "status": "many_to_many_detected",
        "comparison_eligible": False,
    },
]

EXCLUSION_CODES = [
    {
        "code": "historical_comparison_baseline_prediction_missing",
        "category": "missingness",
    },
    {
        "code": "historical_comparison_augmented_prediction_missing",
        "category": "missingness",
    },
    {
        "code": "historical_comparison_baseline_join_ineligible",
        "category": "eligibility",
    },
    {
        "code": "historical_comparison_augmented_join_ineligible",
        "category": "eligibility",
    },
    {
        "code": "historical_comparison_evaluation_identity_mismatch",
        "category": "identity",
    },
    {
        "code": "historical_comparison_event_identity_mismatch",
        "category": "identity",
    },
    {
        "code": "historical_comparison_outcome_mismatch",
        "category": "outcome",
    },
    {
        "code": "historical_comparison_outcome_availability_mismatch",
        "category": "outcome",
    },
    {
        "code": "historical_comparison_prediction_value_type_mismatch",
        "category": "contract",
    },
    {
        "code": "historical_comparison_evaluation_lineage_mismatch",
        "category": "lineage",
    },
    {
        "code": "historical_comparison_prediction_lineage_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_comparison_join_lineage_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_comparison_duplicate_baseline",
        "category": "cardinality",
    },
    {
        "code": "historical_comparison_duplicate_augmented",
        "category": "cardinality",
    },
    {
        "code": "historical_comparison_many_to_many_detected",
        "category": "cardinality",
    },
    {
        "code": "historical_comparison_source_record_invalid",
        "category": "source",
    },
]

METRIC_FAMILIES = [
    {
        "metric_family_id": "HCOMP-M01",
        "metric_family": "proper_scoring_rules",
        "allowed_metrics": (
            "brier_score|log_loss"
        ),
        "calculation_authorized_here": False,
    },
    {
        "metric_family_id": "HCOMP-M02",
        "metric_family": "absolute_error",
        "allowed_metrics": (
            "mean_absolute_error"
        ),
        "calculation_authorized_here": False,
    },
    {
        "metric_family_id": "HCOMP-M03",
        "metric_family": "squared_error",
        "allowed_metrics": (
            "mean_squared_error|root_mean_squared_error"
        ),
        "calculation_authorized_here": False,
    },
    {
        "metric_family_id": "HCOMP-M04",
        "metric_family": "calibration",
        "allowed_metrics": (
            "calibration_intercept|calibration_slope|"
            "expected_calibration_error"
        ),
        "calculation_authorized_here": False,
    },
    {
        "metric_family_id": "HCOMP-M05",
        "metric_family": "discrimination",
        "allowed_metrics": (
            "roc_auc|pr_auc"
        ),
        "calculation_authorized_here": False,
    },
    {
        "metric_family_id": "HCOMP-M06",
        "metric_family": "paired_incremental_value",
        "allowed_metrics": (
            "augmented_minus_baseline_loss|"
            "baseline_minus_augmented_score"
        ),
        "calculation_authorized_here": False,
    },
    {
        "metric_family_id": "HCOMP-M07",
        "metric_family": "coverage",
        "allowed_metrics": (
            "pair_count|eligible_pair_rate|missing_pair_rate"
        ),
        "calculation_authorized_here": False,
    },
]

METRIC_DIRECTION_RULES = [
    {
        "rule_id": "HCOMP-D01",
        "metric": "brier_score",
        "better_direction": "lower",
    },
    {
        "rule_id": "HCOMP-D02",
        "metric": "log_loss",
        "better_direction": "lower",
    },
    {
        "rule_id": "HCOMP-D03",
        "metric": "mean_absolute_error",
        "better_direction": "lower",
    },
    {
        "rule_id": "HCOMP-D04",
        "metric": "mean_squared_error",
        "better_direction": "lower",
    },
    {
        "rule_id": "HCOMP-D05",
        "metric": "root_mean_squared_error",
        "better_direction": "lower",
    },
    {
        "rule_id": "HCOMP-D06",
        "metric": "expected_calibration_error",
        "better_direction": "lower",
    },
    {
        "rule_id": "HCOMP-D07",
        "metric": "roc_auc",
        "better_direction": "higher",
    },
    {
        "rule_id": "HCOMP-D08",
        "metric": "pr_auc",
        "better_direction": "higher",
    },
]

AGGREGATION_DIMENSIONS = [
    {
        "ordinal": 1,
        "dimension": "overall",
        "required": True,
    },
    {
        "ordinal": 2,
        "dimension": "target_id",
        "required": True,
    },
    {
        "ordinal": 3,
        "dimension": "event_level",
        "required": True,
    },
    {
        "ordinal": 4,
        "dimension": "game_date",
        "required": False,
    },
    {
        "ordinal": 5,
        "dimension": "model_contract_version_pair",
        "required": True,
    },
    {
        "ordinal": 6,
        "dimension": "overlay_contract_version",
        "required": True,
    },
]

SUPPORT_RULES = [
    {
        "rule_id": "HCOMP-S01",
        "rule": "every_reported_cell_must_include_pair_count",
    },
    {
        "rule_id": "HCOMP-S02",
        "rule": "zero_support_cells_must_not_emit_metrics",
    },
    {
        "rule_id": "HCOMP-S03",
        "rule": "minimum_support_threshold_must_be_explicit_and_versioned",
    },
    {
        "rule_id": "HCOMP-S04",
        "rule": "suppressed_cells_must_remain_counted_in_coverage_reconciliation",
    },
    {
        "rule_id": "HCOMP-S05",
        "rule": "target_and_event_level_cells_must_remain_separately_reportable",
    },
    {
        "rule_id": "HCOMP-S06",
        "rule": "no_global_superiority_claim_may_ignore_support_or_missingness",
    },
]

UNCERTAINTY_BOUNDARIES = [
    {
        "boundary_id": "HCOMP-U01",
        "rule": (
            "Layer 9Q defines no confidence interval, bootstrap, "
            "permutation, or significance calculation."
        ),
    },
    {
        "boundary_id": "HCOMP-U02",
        "rule": (
            "Any later uncertainty method must preserve game-level "
            "dependence rather than resampling rows independently."
        ),
    },
    {
        "boundary_id": "HCOMP-U03",
        "rule": (
            "Any later superiority decision threshold requires a "
            "separate explicit authority grant."
        ),
    },
    {
        "boundary_id": "HCOMP-U04",
        "rule": (
            "Absence of statistical evidence must not be interpreted "
            "as equivalence."
        ),
    },
]

CARDINALITY_RULES = [
    {
        "rule_id": "HCOMP-C01",
        "rule": "one_evaluation_row_to_zero_or_one_baseline_record",
    },
    {
        "rule_id": "HCOMP-C02",
        "rule": "one_evaluation_row_to_zero_or_one_augmented_record",
    },
    {
        "rule_id": "HCOMP-C03",
        "rule": "one_evaluation_row_to_zero_or_one_comparison_pair",
    },
    {
        "rule_id": "HCOMP-C04",
        "rule": "comparison_record_id_must_be_unique",
    },
    {
        "rule_id": "HCOMP-C05",
        "rule": "duplicate_variant_records_must_be_excluded",
    },
    {
        "rule_id": "HCOMP-C06",
        "rule": "many_to_many_pairing_is_prohibited",
    },
    {
        "rule_id": "HCOMP-C07",
        "rule": "source_pair_and_exclusion_counts_must_reconcile",
    },
]

ORDERING_FIELDS = [
    {
        "ordinal": 1,
        "field": "game_date",
    },
    {
        "ordinal": 2,
        "field": "scheduled_start_utc",
    },
    {
        "ordinal": 3,
        "field": "game_id",
    },
    {
        "ordinal": 4,
        "field": "event_sequence",
    },
    {
        "ordinal": 5,
        "field": "target_id",
    },
    {
        "ordinal": 6,
        "field": "event_level",
    },
    {
        "ordinal": 7,
        "field": "comparison_record_id",
    },
]

IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "verify_layer_9q_plan_and_layer_9p_predecessor",
    },
    {
        "ordinal": 2,
        "step": "replay_layer_9p_prediction_outcome_join_records",
    },
    {
        "ordinal": 3,
        "step": "partition_records_by_prediction_variant",
    },
    {
        "ordinal": 4,
        "step": "index_baseline_and_augmented_rows_by_evaluation_row_id",
    },
    {
        "ordinal": 5,
        "step": "detect_duplicate_variant_and_many_to_many_identities",
    },
    {
        "ordinal": 6,
        "step": "pair_baseline_and_augmented_rows",
    },
    {
        "ordinal": 7,
        "step": "validate_common_event_identity_and_outcome",
    },
    {
        "ordinal": 8,
        "step": "validate_prediction_value_type_and_lineage",
    },
    {
        "ordinal": 9,
        "step": "classify_every_pair_and_unpaired_record",
    },
    {
        "ordinal": 10,
        "step": "derive_comparison_identity_and_record_digests",
    },
    {
        "ordinal": 11,
        "step": "validate_metric_family_and_direction_catalogs",
    },
    {
        "ordinal": 12,
        "step": "validate_aggregation_and_support_boundaries",
    },
    {
        "ordinal": 13,
        "step": "replay_pairing_under_reversed_input_order",
    },
    {
        "ordinal": 14,
        "step": "write_temporary_diagnostic_artifacts_only",
    },
]

DIAGNOSTIC_ARTIFACTS = [
    {
        "artifact": "planning_checks.csv",
    },
    {
        "artifact": "comparison_grains.csv",
    },
    {
        "artifact": "pairing_rules.csv",
    },
    {
        "artifact": "comparison_field_contract.csv",
    },
    {
        "artifact": "comparison_statuses.csv",
    },
    {
        "artifact": "exclusion_code_catalog.csv",
    },
    {
        "artifact": "metric_families.csv",
    },
    {
        "artifact": "metric_direction_rules.csv",
    },
    {
        "artifact": "aggregation_dimensions.csv",
    },
    {
        "artifact": "support_rules.csv",
    },
    {
        "artifact": "uncertainty_boundaries.csv",
    },
    {
        "artifact": "cardinality_rules.csv",
    },
    {
        "artifact": "ordering_fields.csv",
    },
    {
        "artifact": "implementation_steps.csv",
    },
    {
        "artifact": "authority_boundaries.csv",
    },
    {
        "artifact": "comparative_evaluation_plan_summary.json",
    },
    {
        "artifact": "diagnosis.json",
    },
]

PROHIBITED_AUTHORITIES = [
    "accuracy_evaluation",
    "augmented_prediction_generation",
    "backtest_execution",
    "baseline_prediction_generation",
    "bet_recommendation",
    "calibration_evaluation",
    "canonical_probability_authority_change",
    "dataset_split_execution",
    "edge_detection",
    "historical_comparative_metric_calculation",
    "historical_outcome_collection_execution",
    "historical_outcome_fetch_execution",
    "incremental_value_evaluation",
    "market_comparison",
    "model_training",
    "parameter_tuning",
    "predictive_metric_calculation",
    "pricing",
    "production_historical_evaluation_dataset_materialization",
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
        and EXPECTED_PREDECESSOR_JOIN_VERSION
        in predecessor_constants
        and EXPECTED_PREDECESSOR_PREDICTION_VERSION
        in predecessor_constants
        and EXPECTED_PREDECESSOR_DIAGNOSIS
        in predecessor_constants
        and EXPECTED_PREDECESSOR_AUTHORITY
        in predecessor_constants
    )

    comparison_field_names = [
        row["field"]
        for row in COMPARISON_FIELDS
    ]

    checks = [
        {
            "check": "nine_p_predecessor_verified",
            "actual": predecessor_verified,
            "expected": True,
            "passed": predecessor_verified,
        },
        {
            "check": "four_comparison_grains_defined",
            "actual": len(COMPARISON_GRAINS),
            "expected": 4,
            "passed": len(COMPARISON_GRAINS) == 4,
        },
        {
            "check": "fifteen_pairing_rules_defined",
            "actual": len(PAIRING_RULES),
            "expected": 15,
            "passed": len(PAIRING_RULES) == 15,
        },
        {
            "check": "thirty_one_comparison_fields_defined",
            "actual": len(COMPARISON_FIELDS),
            "expected": 31,
            "passed": (
                len(COMPARISON_FIELDS) == 31
                and len(set(comparison_field_names)) == 31
            ),
        },
        {
            "check": "twelve_comparison_statuses_defined",
            "actual": len(COMPARISON_STATUSES),
            "expected": 12,
            "passed": len(COMPARISON_STATUSES) == 12,
        },
        {
            "check": "sixteen_exclusion_codes_defined",
            "actual": len(EXCLUSION_CODES),
            "expected": 16,
            "passed": len(EXCLUSION_CODES) == 16,
        },
        {
            "check": "seven_metric_families_defined",
            "actual": len(METRIC_FAMILIES),
            "expected": 7,
            "passed": len(METRIC_FAMILIES) == 7,
        },
        {
            "check": "eight_metric_direction_rules_defined",
            "actual": len(METRIC_DIRECTION_RULES),
            "expected": 8,
            "passed": len(METRIC_DIRECTION_RULES) == 8,
        },
        {
            "check": "six_aggregation_dimensions_defined",
            "actual": len(AGGREGATION_DIMENSIONS),
            "expected": 6,
            "passed": len(AGGREGATION_DIMENSIONS) == 6,
        },
        {
            "check": "six_support_rules_defined",
            "actual": len(SUPPORT_RULES),
            "expected": 6,
            "passed": len(SUPPORT_RULES) == 6,
        },
        {
            "check": "four_uncertainty_boundaries_defined",
            "actual": len(UNCERTAINTY_BOUNDARIES),
            "expected": 4,
            "passed": len(UNCERTAINTY_BOUNDARIES) == 4,
        },
        {
            "check": "seven_cardinality_rules_defined",
            "actual": len(CARDINALITY_RULES),
            "expected": 7,
            "passed": len(CARDINALITY_RULES) == 7,
        },
        {
            "check": "seven_ordering_fields_defined",
            "actual": len(ORDERING_FIELDS),
            "expected": 7,
            "passed": len(ORDERING_FIELDS) == 7,
        },
        {
            "check": "fourteen_implementation_steps_defined",
            "actual": len(IMPLEMENTATION_STEPS),
            "expected": 14,
            "passed": len(IMPLEMENTATION_STEPS) == 14,
        },
        {
            "check": "seventeen_diagnostic_artifacts_defined",
            "actual": len(DIAGNOSTIC_ARTIFACTS),
            "expected": 17,
            "passed": len(DIAGNOSTIC_ARTIFACTS) == 17,
        },
        {
            "check": "baseline_and_augmented_pairing_required",
            "actual": True,
            "expected": True,
            "passed": all(
                rule in {
                    row["rule"]
                    for row in PAIRING_RULES
                }
                for rule in {
                    "baseline_join_status_must_be_matched_eligible",
                    "augmented_join_status_must_be_matched_eligible",
                    "baseline_and_augmented_evaluation_row_id_must_match",
                }
            ),
        },
        {
            "check": "paired_outcome_consistency_required",
            "actual": True,
            "expected": True,
            "passed": all(
                rule in {
                    row["rule"]
                    for row in PAIRING_RULES
                }
                for rule in {
                    "baseline_and_augmented_outcome_value_must_match",
                    "baseline_and_augmented_outcome_availability_must_match",
                }
            ),
        },
        {
            "check": "many_to_many_pairing_prohibited",
            "actual": True,
            "expected": True,
            "passed": any(
                row["rule"]
                == "many_to_many_pairing_is_prohibited"
                for row in CARDINALITY_RULES
            ),
        },
        {
            "check": "metric_calculation_not_authorized",
            "actual": sum(
                bool(row["calculation_authorized_here"])
                for row in METRIC_FAMILIES
            ),
            "expected": 0,
            "passed": all(
                not row["calculation_authorized_here"]
                for row in METRIC_FAMILIES
            ),
        },
        {
            "check": "minimum_support_boundary_defined",
            "actual": True,
            "expected": True,
            "passed": any(
                row["rule"]
                == "minimum_support_threshold_must_be_explicit_and_versioned"
                for row in SUPPORT_RULES
            ),
        },
        {
            "check": "game_level_dependence_boundary_defined",
            "actual": True,
            "expected": True,
            "passed": any(
                "game-level dependence"
                in row["rule"]
                for row in UNCERTAINTY_BOUNDARIES
            ),
        },
        {
            "check": "comparative_metrics_not_calculated",
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
            "comparison_grains": COMPARISON_GRAINS,
            "pairing_rules": PAIRING_RULES,
            "comparison_fields": COMPARISON_FIELDS,
            "comparison_statuses": COMPARISON_STATUSES,
            "exclusion_codes": EXCLUSION_CODES,
            "metric_families": METRIC_FAMILIES,
            "metric_direction_rules": METRIC_DIRECTION_RULES,
            "aggregation_dimensions": AGGREGATION_DIMENSIONS,
            "support_rules": SUPPORT_RULES,
            "uncertainty_boundaries": UNCERTAINTY_BOUNDARIES,
            "cardinality_rules": CARDINALITY_RULES,
            "ordering_fields": ORDERING_FIELDS,
            "implementation_steps": IMPLEMENTATION_STEPS,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_comparative_"
        "evaluation_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_comparative_"
        "evaluation_contract_plan_failed"
    )

    next_layer = (
        "9R_pitch_type_matchup_overlay_historical_comparative_"
        "evaluation_contract_implementation"
        if all_checks_passed
        else
        "9Q_pitch_type_matchup_overlay_historical_comparative_"
        "evaluation_contract_plan_remediation"
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
        OUTPUT_DIR / "comparison_grains.csv",
        [
            "grain_id",
            "event_level",
            "comparison_identity",
        ],
        COMPARISON_GRAINS,
    )

    write_csv(
        OUTPUT_DIR / "pairing_rules.csv",
        [
            "rule_id",
            "rule",
        ],
        PAIRING_RULES,
    )

    write_csv(
        OUTPUT_DIR / "comparison_field_contract.csv",
        [
            "ordinal",
            "field",
            "role",
        ],
        COMPARISON_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "comparison_statuses.csv",
        [
            "status",
            "comparison_eligible",
        ],
        COMPARISON_STATUSES,
    )

    write_csv(
        OUTPUT_DIR / "exclusion_code_catalog.csv",
        [
            "code",
            "category",
        ],
        EXCLUSION_CODES,
    )

    write_csv(
        OUTPUT_DIR / "metric_families.csv",
        [
            "metric_family_id",
            "metric_family",
            "allowed_metrics",
            "calculation_authorized_here",
        ],
        METRIC_FAMILIES,
    )

    write_csv(
        OUTPUT_DIR / "metric_direction_rules.csv",
        [
            "rule_id",
            "metric",
            "better_direction",
        ],
        METRIC_DIRECTION_RULES,
    )

    write_csv(
        OUTPUT_DIR / "aggregation_dimensions.csv",
        [
            "ordinal",
            "dimension",
            "required",
        ],
        AGGREGATION_DIMENSIONS,
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
        OUTPUT_DIR / "uncertainty_boundaries.csv",
        [
            "boundary_id",
            "rule",
        ],
        UNCERTAINTY_BOUNDARIES,
    )

    write_csv(
        OUTPUT_DIR / "cardinality_rules.csv",
        [
            "rule_id",
            "rule",
        ],
        CARDINALITY_RULES,
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
                    "Layer 9Q is planning-only and grants no "
                    "metric calculation, uncertainty estimation, "
                    "superiority declaration, production, market, "
                    "or betting authority."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_comparative_evaluation_contract_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "Layer 9R may implement deterministic pairing, "
                    "classification, lineage, support, and metric-catalog "
                    "validation without calculating comparative metrics."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_verified": predecessor_verified,
        "comparison_grains": len(COMPARISON_GRAINS),
        "pairing_rules": len(PAIRING_RULES),
        "comparison_fields": len(COMPARISON_FIELDS),
        "comparison_statuses": len(COMPARISON_STATUSES),
        "exclusion_codes": len(EXCLUSION_CODES),
        "metric_families": len(METRIC_FAMILIES),
        "metric_direction_rules": len(METRIC_DIRECTION_RULES),
        "aggregation_dimensions": len(AGGREGATION_DIMENSIONS),
        "support_rules": len(SUPPORT_RULES),
        "uncertainty_boundaries": len(UNCERTAINTY_BOUNDARIES),
        "cardinality_rules": len(CARDINALITY_RULES),
        "ordering_fields": len(ORDERING_FIELDS),
        "implementation_steps": len(IMPLEMENTATION_STEPS),
        "planning_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "planning_checks_required": len(checks),
        "plan_digest": plan_digest,
        "comparison_pairs_materialized": 0,
        "comparative_metrics_calculated": 0,
        "uncertainty_estimates_calculated": 0,
        "superiority_decisions_emitted": 0,
        "dataset_splits_executed": 0,
        "production_predictions_generated": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed": all_checks_passed,
        "recommended_next_layer": next_layer,
    }

    write_json(
        OUTPUT_DIR
        / "comparative_evaluation_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_comparative_evaluation_contract_implementation"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": sorted(
            PROHIBITED_AUTHORITIES
        ),
        "recommended_next_layer": next_layer,
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
        "Comparison grains: "
        f"{len(COMPARISON_GRAINS)}"
    )
    print(
        f"Pairing rules: {len(PAIRING_RULES)}"
    )
    print(
        "Comparison fields: "
        f"{len(COMPARISON_FIELDS)}"
    )
    print(
        "Comparison statuses: "
        f"{len(COMPARISON_STATUSES)}"
    )
    print(
        f"Exclusion codes: {len(EXCLUSION_CODES)}"
    )
    print(
        f"Metric families: {len(METRIC_FAMILIES)}"
    )
    print(
        "Metric direction rules: "
        f"{len(METRIC_DIRECTION_RULES)}"
    )
    print(
        "Aggregation dimensions: "
        f"{len(AGGREGATION_DIMENSIONS)}"
    )
    print(
        f"Support rules: {len(SUPPORT_RULES)}"
    )
    print(
        "Uncertainty boundaries: "
        f"{len(UNCERTAINTY_BOUNDARIES)}"
    )
    print(
        "Cardinality rules: "
        f"{len(CARDINALITY_RULES)}"
    )
    print(
        f"Ordering fields: {len(ORDERING_FIELDS)}"
    )
    print(
        "Implementation steps: "
        f"{len(IMPLEMENTATION_STEPS)}"
    )
    print(
        "Comparison pairs materialized: 0"
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
        "Dataset splits executed: 0"
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
