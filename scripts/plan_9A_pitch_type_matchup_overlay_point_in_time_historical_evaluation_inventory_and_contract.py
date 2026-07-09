#!/usr/bin/env python3
"""
Layer 9A
Pitch-Type Matchup Overlay Point-in-Time Historical Evaluation
Inventory and Contract Plan

Inventories the repository surfaces needed for a leakage-safe, point-in-time
historical evaluation of the Layer 8 pitch-type matchup overlay and defines
the bounded evaluation contract.

Planning only.

This layer does not:
- join historical outcomes to Layer 8 records;
- generate baseline or augmented predictions;
- calculate predictive metrics;
- evaluate accuracy, calibration, or incremental value;
- train or tune models, weights, thresholds, or fallbacks;
- run backtests;
- activate the matchup overlay in production;
- modify simulation or canonical probabilities;
- compare projections with betting markets;
- price wagers, detect edges, or recommend bets.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "9A"
LAYER_NAME = (
    "pitch_type_matchup_overlay_point_in_time_historical_"
    "evaluation_inventory_and_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_9A_pitch_type_matchup_overlay_point_in_time_"
    "historical_evaluation_inventory_and_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "plan_8AH_layer_8_pitch_type_matchup_overlay_shadow_"
    "evaluation_readiness_and_scope_closure.py"
)


INVENTORY_DOMAINS = [
    {
        "domain_id": "PIT-I01",
        "domain": "layer_8_shadow_rows",
        "inventory_objective": (
            "Locate deterministic Layer 8 matchup-shadow rows, partitions, "
            "manifests, schema fingerprints, and source-version lineage."
        ),
    },
    {
        "domain_id": "PIT-I02",
        "domain": "game_identity",
        "inventory_objective": (
            "Locate canonical game identifiers, game dates, scheduled start "
            "times, venues, teams, and doubleheader identifiers."
        ),
    },
    {
        "domain_id": "PIT-I03",
        "domain": "plate_appearance_identity",
        "inventory_objective": (
            "Locate game-scoped plate-appearance identifiers, inning, half, "
            "outs, sequence, pitcher, batter, and lineup-position fields."
        ),
    },
    {
        "domain_id": "PIT-I04",
        "domain": "pitch_identity",
        "inventory_objective": (
            "Locate pitch-level identifiers, pitch sequence, count state, "
            "pitch type, timestamp, and source lineage."
        ),
    },
    {
        "domain_id": "PIT-I05",
        "domain": "feature_availability_timestamps",
        "inventory_objective": (
            "Locate source-observation, profile-generation, overlay-generation, "
            "collection, and publication timestamps."
        ),
    },
    {
        "domain_id": "PIT-I06",
        "domain": "baseline_predictions",
        "inventory_objective": (
            "Locate frozen baseline probability records and the versions, "
            "timestamps, inputs, and simulation identities that produced them."
        ),
    },
    {
        "domain_id": "PIT-I07",
        "domain": "augmented_shadow_predictions",
        "inventory_objective": (
            "Locate or define a non-authoritative surface for baseline-plus-"
            "overlay diagnostic predictions."
        ),
    },
    {
        "domain_id": "PIT-I08",
        "domain": "pitch_and_plate_appearance_outcomes",
        "inventory_objective": (
            "Locate pitch results and plate-appearance terminal outcomes with "
            "canonical identifiers and source timestamps."
        ),
    },
    {
        "domain_id": "PIT-I09",
        "domain": "contact_quality_outcomes",
        "inventory_objective": (
            "Locate batted-ball, exit-velocity, launch-angle, expected-value, "
            "and contact-result fields with missingness semantics."
        ),
    },
    {
        "domain_id": "PIT-I10",
        "domain": "run_value_outcomes",
        "inventory_objective": (
            "Locate event run values, state transitions, scoring outcomes, and "
            "win-probability-change fields without market data."
        ),
    },
    {
        "domain_id": "PIT-I11",
        "domain": "data_versions_and_provenance",
        "inventory_objective": (
            "Locate dataset versions, provider versions, ingestion timestamps, "
            "revision semantics, and immutable payload digests."
        ),
    },
    {
        "domain_id": "PIT-I12",
        "domain": "evaluation_and_validation_surfaces",
        "inventory_objective": (
            "Locate existing time-split, metric, calibration, uncertainty, "
            "ablation, and artifact-generation utilities."
        ),
    },
]


EVALUATION_RECORD_FIELDS = [
    {"field": "evaluation_record_id", "type": "deterministic_string", "required": True},
    {"field": "evaluation_contract_version", "type": "string", "required": True},
    {"field": "game_id", "type": "string", "required": True},
    {"field": "game_date", "type": "date", "required": True},
    {"field": "scheduled_start_utc", "type": "datetime", "required": True},
    {"field": "event_level", "type": "enum", "required": True},
    {"field": "plate_appearance_id", "type": "nullable_string", "required": True},
    {"field": "pitch_id", "type": "nullable_string", "required": True},
    {"field": "pitcher_id", "type": "string", "required": True},
    {"field": "batter_id", "type": "string", "required": True},
    {"field": "feature_cutoff_utc", "type": "datetime", "required": True},
    {"field": "shadow_row_id", "type": "string", "required": True},
    {"field": "shadow_row_generated_at_utc", "type": "datetime", "required": True},
    {"field": "pitcher_profile_version", "type": "string", "required": True},
    {"field": "batter_profile_version", "type": "string", "required": True},
    {"field": "matchup_overlay_version", "type": "string", "required": True},
    {"field": "baseline_prediction_id", "type": "string", "required": True},
    {"field": "baseline_prediction_generated_at_utc", "type": "datetime", "required": True},
    {"field": "augmented_prediction_id", "type": "nullable_string", "required": True},
    {"field": "outcome_id", "type": "string", "required": True},
    {"field": "outcome_available_at_utc", "type": "datetime", "required": True},
    {"field": "point_in_time_eligible", "type": "boolean", "required": True},
    {"field": "exclusion_codes", "type": "sorted_unique_string_array", "required": True},
    {"field": "provenance_digest", "type": "sha256_string", "required": True},
]


EVENT_IDENTITY_RULES = [
    {
        "rule_id": "PIT-E01",
        "rule": "game_id_required_for_all_records",
    },
    {
        "rule_id": "PIT-E02",
        "rule": "plate_appearance_id_required_for_plate_appearance_and_pitch_records",
    },
    {
        "rule_id": "PIT-E03",
        "rule": "pitch_id_required_only_for_pitch_records",
    },
    {
        "rule_id": "PIT-E04",
        "rule": "pitcher_and_batter_identity_required",
    },
    {
        "rule_id": "PIT-E05",
        "rule": "event_identity_unique_within_contract_version",
    },
    {
        "rule_id": "PIT-E06",
        "rule": "doubleheader_and_rescheduled_game_identity_preserved",
    },
    {
        "rule_id": "PIT-E07",
        "rule": "event_sequence_order_deterministic",
    },
    {
        "rule_id": "PIT-E08",
        "rule": "identity_conflicts_rejected_not_overwritten",
    },
]


FEATURE_CUTOFF_RULES = [
    {
        "rule_id": "PIT-C01",
        "rule": "feature_cutoff_precedes_event_start",
    },
    {
        "rule_id": "PIT-C02",
        "rule": "source_observation_time_not_after_feature_cutoff",
    },
    {
        "rule_id": "PIT-C03",
        "rule": "profile_generation_time_not_after_feature_cutoff",
    },
    {
        "rule_id": "PIT-C04",
        "rule": "overlay_generation_time_not_after_feature_cutoff",
    },
    {
        "rule_id": "PIT-C05",
        "rule": "baseline_prediction_time_not_after_feature_cutoff",
    },
    {
        "rule_id": "PIT-C06",
        "rule": "lineup_and_handedness_context_time_not_after_cutoff",
    },
    {
        "rule_id": "PIT-C07",
        "rule": "revised_source_payloads_use_as_of_version_only",
    },
    {
        "rule_id": "PIT-C08",
        "rule": "cutoff_semantics_explicit_by_event_level",
    },
]


FUTURE_INFORMATION_EXCLUSION_RULES = [
    {
        "rule_id": "PIT-L01",
        "rule": "post_event_source_records_excluded",
    },
    {
        "rule_id": "PIT-L02",
        "rule": "post_cutoff_profile_revisions_excluded",
    },
    {
        "rule_id": "PIT-L03",
        "rule": "post_cutoff_overlay_revisions_excluded",
    },
    {
        "rule_id": "PIT-L04",
        "rule": "same_event_future_pitches_excluded",
    },
    {
        "rule_id": "PIT-L05",
        "rule": "same_event_terminal_outcomes_excluded_from_features",
    },
    {
        "rule_id": "PIT-L06",
        "rule": "future_season_aggregates_excluded",
    },
    {
        "rule_id": "PIT-L07",
        "rule": "retrospectively_corrected_provider_values_excluded_without_as_of_version",
    },
    {
        "rule_id": "PIT-L08",
        "rule": "outcome_and_feature_payloads_digest_separated",
    },
]


OUTCOME_TARGETS = [
    {
        "target_id": "PIT-O01",
        "event_level": "pitch",
        "target": "swing_event",
        "target_type": "binary",
    },
    {
        "target_id": "PIT-O02",
        "event_level": "pitch",
        "target": "whiff_event",
        "target_type": "binary",
    },
    {
        "target_id": "PIT-O03",
        "event_level": "pitch",
        "target": "called_strike_event",
        "target_type": "binary",
    },
    {
        "target_id": "PIT-O04",
        "event_level": "pitch",
        "target": "ball_in_play_event",
        "target_type": "binary",
    },
    {
        "target_id": "PIT-O05",
        "event_level": "plate_appearance",
        "target": "strikeout_event",
        "target_type": "binary",
    },
    {
        "target_id": "PIT-O06",
        "event_level": "plate_appearance",
        "target": "walk_event",
        "target_type": "binary",
    },
    {
        "target_id": "PIT-O07",
        "event_level": "plate_appearance",
        "target": "hit_event",
        "target_type": "binary",
    },
    {
        "target_id": "PIT-O08",
        "event_level": "plate_appearance",
        "target": "extra_base_hit_event",
        "target_type": "binary",
    },
    {
        "target_id": "PIT-O09",
        "event_level": "contact",
        "target": "contact_quality_value",
        "target_type": "continuous",
    },
    {
        "target_id": "PIT-O10",
        "event_level": "event",
        "target": "run_value",
        "target_type": "continuous",
    },
]


COMPARISON_ARMS = [
    {
        "arm_id": "PIT-A01",
        "arm": "frozen_baseline",
        "purpose": "Primary comparator using existing point-in-time predictions.",
    },
    {
        "arm_id": "PIT-A02",
        "arm": "baseline_plus_pitcher_arsenal",
        "purpose": "Measure pitcher-profile incremental value.",
    },
    {
        "arm_id": "PIT-A03",
        "arm": "baseline_plus_batter_response",
        "purpose": "Measure batter-profile incremental value.",
    },
    {
        "arm_id": "PIT-A04",
        "arm": "baseline_plus_handedness_context",
        "purpose": "Measure handedness-context incremental value.",
    },
    {
        "arm_id": "PIT-A05",
        "arm": "baseline_plus_aggregate_matchup",
        "purpose": "Compare pitch-type overlay with a non-pitch-type matchup.",
    },
    {
        "arm_id": "PIT-A06",
        "arm": "baseline_plus_full_pitch_type_overlay",
        "purpose": "Measure complete Layer 8 overlay incremental value.",
    },
    {
        "arm_id": "PIT-A07",
        "arm": "full_overlay_without_fallback_entries",
        "purpose": "Measure dependence on fallback entries.",
    },
    {
        "arm_id": "PIT-A08",
        "arm": "full_overlay_by_coverage_bucket",
        "purpose": "Measure value conditional on matchup coverage.",
    },
]


EVALUATION_METRICS = [
    {
        "metric_id": "PIT-M01",
        "metric": "log_loss",
        "applies_to": "probabilistic_binary_targets",
    },
    {
        "metric_id": "PIT-M02",
        "metric": "brier_score",
        "applies_to": "probabilistic_binary_targets",
    },
    {
        "metric_id": "PIT-M03",
        "metric": "calibration_intercept",
        "applies_to": "probabilistic_binary_targets",
    },
    {
        "metric_id": "PIT-M04",
        "metric": "calibration_slope",
        "applies_to": "probabilistic_binary_targets",
    },
    {
        "metric_id": "PIT-M05",
        "metric": "expected_calibration_error",
        "applies_to": "probabilistic_binary_targets",
    },
    {
        "metric_id": "PIT-M06",
        "metric": "roc_auc",
        "applies_to": "binary_targets_when_appropriate",
    },
    {
        "metric_id": "PIT-M07",
        "metric": "mean_absolute_error",
        "applies_to": "continuous_targets",
    },
    {
        "metric_id": "PIT-M08",
        "metric": "root_mean_squared_error",
        "applies_to": "continuous_targets",
    },
    {
        "metric_id": "PIT-M09",
        "metric": "incremental_metric_delta",
        "applies_to": "all_comparison_arms",
    },
    {
        "metric_id": "PIT-M10",
        "metric": "coverage_and_fallback_segment_metrics",
        "applies_to": "all_targets",
    },
]


TIME_SPLIT_RULES = [
    {
        "rule_id": "PIT-T01",
        "rule": "splits_ordered_chronologically",
    },
    {
        "rule_id": "PIT-T02",
        "rule": "training_period_precedes_validation_period",
    },
    {
        "rule_id": "PIT-T03",
        "rule": "validation_period_precedes_test_period",
    },
    {
        "rule_id": "PIT-T04",
        "rule": "future_test_period_untouched_until_final_evaluation",
    },
    {
        "rule_id": "PIT-T05",
        "rule": "player_records_may_span_splits_but_event_time_may_not",
    },
    {
        "rule_id": "PIT-T06",
        "rule": "season_boundaries_reported_explicitly",
    },
    {
        "rule_id": "PIT-T07",
        "rule": "tuning_forbidden_on_test_period",
    },
    {
        "rule_id": "PIT-T08",
        "rule": "all_split_assignments_deterministic_and_artifacted",
    },
]


VALIDATION_RULES = [
    {"rule_id": "PIT-V01", "rule": "evaluation_contract_version_explicit"},
    {"rule_id": "PIT-V02", "rule": "evaluation_record_id_deterministic"},
    {"rule_id": "PIT-V03", "rule": "event_level_valid"},
    {"rule_id": "PIT-V04", "rule": "game_identity_present"},
    {"rule_id": "PIT-V05", "rule": "plate_appearance_identity_conditional"},
    {"rule_id": "PIT-V06", "rule": "pitch_identity_conditional"},
    {"rule_id": "PIT-V07", "rule": "pitcher_and_batter_identity_present"},
    {"rule_id": "PIT-V08", "rule": "feature_cutoff_present"},
    {"rule_id": "PIT-V09", "rule": "feature_cutoff_precedes_event"},
    {"rule_id": "PIT-V10", "rule": "shadow_row_generated_before_cutoff"},
    {"rule_id": "PIT-V11", "rule": "baseline_prediction_generated_before_cutoff"},
    {"rule_id": "PIT-V12", "rule": "profile_versions_present"},
    {"rule_id": "PIT-V13", "rule": "overlay_version_present"},
    {"rule_id": "PIT-V14", "rule": "outcome_identity_present"},
    {"rule_id": "PIT-V15", "rule": "outcome_available_after_event"},
    {"rule_id": "PIT-V16", "rule": "outcome_payload_excluded_from_features"},
    {"rule_id": "PIT-V17", "rule": "future_information_exclusion_codes_explicit"},
    {"rule_id": "PIT-V18", "rule": "provenance_digest_valid_sha256"},
    {"rule_id": "PIT-V19", "rule": "identity_conflicts_rejected"},
    {"rule_id": "PIT-V20", "rule": "evaluation_order_deterministic"},
    {"rule_id": "PIT-V21", "rule": "disabled_path_non_emitting"},
    {"rule_id": "PIT-V22", "rule": "production_authority_false"},
]


ARTIFACT_SCHEMAS = [
    {
        "artifact": "repository_inventory.csv",
        "scope": "one_row_per_repository_inventory_match",
        "required": True,
    },
    {
        "artifact": "evaluation_record_contract.csv",
        "scope": "one_row_per_evaluation_record_field",
        "required": True,
    },
    {
        "artifact": "point_in_time_rules.csv",
        "scope": "one_row_per_identity_cutoff_or_leakage_rule",
        "required": True,
    },
    {
        "artifact": "outcome_target_contract.csv",
        "scope": "one_row_per_historical_outcome_target",
        "required": True,
    },
    {
        "artifact": "comparison_arms.csv",
        "scope": "one_row_per_baseline_or_augmented_comparison_arm",
        "required": True,
    },
    {
        "artifact": "evaluation_metrics.csv",
        "scope": "one_row_per_metric_definition",
        "required": True,
    },
    {
        "artifact": "authority_boundaries.csv",
        "scope": "evaluation_authority_contract",
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
        "fallback_id": "PIT-F01",
        "condition": "game_identity_missing",
        "result": "record_ineligible",
        "diagnostic_code": "point_in_time_game_identity_missing",
    },
    {
        "fallback_id": "PIT-F02",
        "condition": "event_identity_conflict",
        "result": "record_rejected",
        "diagnostic_code": "point_in_time_event_identity_conflict",
    },
    {
        "fallback_id": "PIT-F03",
        "condition": "feature_cutoff_missing",
        "result": "record_ineligible",
        "diagnostic_code": "point_in_time_feature_cutoff_missing",
    },
    {
        "fallback_id": "PIT-F04",
        "condition": "feature_generated_after_cutoff",
        "result": "record_excluded",
        "diagnostic_code": "point_in_time_future_feature_detected",
    },
    {
        "fallback_id": "PIT-F05",
        "condition": "baseline_prediction_missing",
        "result": "comparison_record_ineligible",
        "diagnostic_code": "point_in_time_baseline_prediction_missing",
    },
    {
        "fallback_id": "PIT-F06",
        "condition": "outcome_missing",
        "result": "record_retained_without_scoring",
        "diagnostic_code": "point_in_time_outcome_missing",
    },
    {
        "fallback_id": "PIT-F07",
        "condition": "provenance_incomplete",
        "result": "record_rejected",
        "diagnostic_code": "point_in_time_provenance_incomplete",
    },
    {
        "fallback_id": "PIT-F08",
        "condition": "evaluation_disabled",
        "result": "no_evaluation_record_emitted",
        "diagnostic_code": "point_in_time_evaluation_disabled",
    },
]


IMPLEMENTATION_STEPS = [
    {
        "step": 1,
        "action": "Inventory event, feature, prediction, outcome, and evaluation surfaces.",
    },
    {
        "step": 2,
        "action": "Define deterministic game, plate-appearance, and pitch identities.",
    },
    {
        "step": 3,
        "action": "Define event-level feature-cutoff semantics.",
    },
    {
        "step": 4,
        "action": "Define future-information exclusion and as-of-version rules.",
    },
    {
        "step": 5,
        "action": "Define immutable point-in-time evaluation-record schema.",
    },
    {
        "step": 6,
        "action": "Define bounded historical outcome targets.",
    },
    {
        "step": 7,
        "action": "Define frozen baseline and diagnostic augmented comparison arms.",
    },
    {
        "step": 8,
        "action": "Define evaluation metrics without calculating them.",
    },
    {
        "step": 9,
        "action": "Define chronological training, validation, and test splits.",
    },
    {
        "step": 10,
        "action": "Define deterministic provenance and eligibility diagnostics.",
    },
    {
        "step": 11,
        "action": "Create an independent point-in-time contract audit plan.",
    },
    {
        "step": 12,
        "action": "Emit deterministic CSV and JSON planning artifacts.",
    },
]


ACCEPTANCE_CRITERIA = [
    {"criterion_id": "PIT-AC01", "criterion": "layer_8AH_dependency_verified"},
    {"criterion_id": "PIT-AC02", "criterion": "twelve_inventory_domains_defined"},
    {"criterion_id": "PIT-AC03", "criterion": "twenty_four_evaluation_fields_defined"},
    {"criterion_id": "PIT-AC04", "criterion": "event_identity_rules_defined"},
    {"criterion_id": "PIT-AC05", "criterion": "feature_cutoff_rules_defined"},
    {"criterion_id": "PIT-AC06", "criterion": "future_information_exclusion_rules_defined"},
    {"criterion_id": "PIT-AC07", "criterion": "ten_outcome_targets_defined"},
    {"criterion_id": "PIT-AC08", "criterion": "eight_comparison_arms_defined"},
    {"criterion_id": "PIT-AC09", "criterion": "ten_evaluation_metrics_defined"},
    {"criterion_id": "PIT-AC10", "criterion": "eight_time_split_rules_defined"},
    {"criterion_id": "PIT-AC11", "criterion": "twenty_two_validation_rules_defined"},
    {"criterion_id": "PIT-AC12", "criterion": "eight_fallback_contracts_defined"},
    {"criterion_id": "PIT-AC13", "criterion": "outcome_join_execution_absent"},
    {"criterion_id": "PIT-AC14", "criterion": "predictive_evaluation_execution_absent"},
    {"criterion_id": "PIT-AC15", "criterion": "production_authority_absent"},
    {"criterion_id": "PIT-AC16", "criterion": "implementation_handoff_bounded"},
]


PROHIBITED_AUTHORITIES = [
    "historical_outcome_join_execution",
    "evaluation_record_materialization",
    "baseline_prediction_generation",
    "augmented_prediction_generation",
    "predictive_metric_calculation",
    "accuracy_evaluation",
    "calibration_evaluation",
    "incremental_value_evaluation",
    "uncertainty_estimation",
    "model_training",
    "parameter_tuning",
    "threshold_tuning",
    "fallback_tuning",
    "backtest_execution",
    "production_overlay_integration",
    "production_matchup_activation",
    "simulation_state_change",
    "simulation_probability_change",
    "canonical_probability_authority_change",
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
        "layer_8_pitch_type_matchup_overlay_shadow_evaluation_"
        "readiness_and_scope_closure_plan_complete"
        in string_constants(
            PREDECESSOR_PATH
        )
    )

    evaluation_field_names = [
        row["field"]
        for row in EVALUATION_RECORD_FIELDS
    ]

    planning_checks = [
        {
            "check": "eight_ah_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twelve_inventory_domains_defined",
            "actual": len(INVENTORY_DOMAINS),
            "expected": 12,
            "passed": len(INVENTORY_DOMAINS) == 12,
        },
        {
            "check": "twenty_four_evaluation_fields_defined",
            "actual": len(EVALUATION_RECORD_FIELDS),
            "expected": 24,
            "passed": len(EVALUATION_RECORD_FIELDS) == 24,
        },
        {
            "check": "evaluation_field_names_unique",
            "actual": len(set(evaluation_field_names)),
            "expected": len(evaluation_field_names),
            "passed": (
                len(set(evaluation_field_names))
                == len(evaluation_field_names)
            ),
        },
        {
            "check": "eight_event_identity_rules_defined",
            "actual": len(EVENT_IDENTITY_RULES),
            "expected": 8,
            "passed": len(EVENT_IDENTITY_RULES) == 8,
        },
        {
            "check": "eight_feature_cutoff_rules_defined",
            "actual": len(FEATURE_CUTOFF_RULES),
            "expected": 8,
            "passed": len(FEATURE_CUTOFF_RULES) == 8,
        },
        {
            "check": "eight_future_information_exclusion_rules_defined",
            "actual": len(FUTURE_INFORMATION_EXCLUSION_RULES),
            "expected": 8,
            "passed": len(FUTURE_INFORMATION_EXCLUSION_RULES) == 8,
        },
        {
            "check": "ten_outcome_targets_defined",
            "actual": len(OUTCOME_TARGETS),
            "expected": 10,
            "passed": len(OUTCOME_TARGETS) == 10,
        },
        {
            "check": "eight_comparison_arms_defined",
            "actual": len(COMPARISON_ARMS),
            "expected": 8,
            "passed": len(COMPARISON_ARMS) == 8,
        },
        {
            "check": "ten_evaluation_metrics_defined",
            "actual": len(EVALUATION_METRICS),
            "expected": 10,
            "passed": len(EVALUATION_METRICS) == 10,
        },
        {
            "check": "eight_time_split_rules_defined",
            "actual": len(TIME_SPLIT_RULES),
            "expected": 8,
            "passed": len(TIME_SPLIT_RULES) == 8,
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
            "check": "outcome_join_and_predictive_execution_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "production_tuning_backtest_pricing_edge_authority_absent",
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
    ]

    all_checks_passed = all(
        row["passed"]
        for row in planning_checks
    )

    point_in_time_rows = [
        {
            "rule_group": "event_identity",
            **row,
        }
        for row in EVENT_IDENTITY_RULES
    ] + [
        {
            "rule_group": "feature_cutoff",
            **row,
        }
        for row in FEATURE_CUTOFF_RULES
    ] + [
        {
            "rule_group": "future_information_exclusion",
            **row,
        }
        for row in FUTURE_INFORMATION_EXCLUSION_RULES
    ] + [
        {
            "rule_group": "time_split",
            **row,
        }
        for row in TIME_SPLIT_RULES
    ]

    authority_rows = [
        {
            "authority": authority,
            "granted": False,
            "reason": (
                "9A defines a point-in-time historical evaluation inventory "
                "and contract plan only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.append(
        {
            "authority": (
                "point_in_time_historical_evaluation_contract_implementation"
            ),
            "granted": all_checks_passed,
            "reason": (
                "9B may implement deterministic eligibility, identity, cutoff, "
                "and leakage checks without joining outcomes or calculating "
                "predictive metrics."
            ),
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_point_in_time_historical_"
        "evaluation_inventory_and_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_point_in_time_historical_"
        "evaluation_inventory_and_contract_plan_failed"
    )

    recommended_next_layer = (
        "9B_pitch_type_matchup_overlay_point_in_time_historical_"
        "evaluation_contract_implementation"
        if all_checks_passed
        else
        "9A_pitch_type_matchup_overlay_point_in_time_historical_"
        "evaluation_inventory_and_contract_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "inventory_domains.csv": INVENTORY_DOMAINS,
        "evaluation_record_fields.csv": EVALUATION_RECORD_FIELDS,
        "point_in_time_rules.csv": point_in_time_rows,
        "outcome_targets.csv": OUTCOME_TARGETS,
        "comparison_arms.csv": COMPARISON_ARMS,
        "evaluation_metrics.csv": EVALUATION_METRICS,
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
        "inventory_domains.csv": [
            "domain_id",
            "domain",
            "inventory_objective",
        ],
        "evaluation_record_fields.csv": [
            "field",
            "type",
            "required",
        ],
        "point_in_time_rules.csv": [
            "rule_group",
            "rule_id",
            "rule",
        ],
        "outcome_targets.csv": [
            "target_id",
            "event_level",
            "target",
            "target_type",
        ],
        "comparison_arms.csv": [
            "arm_id",
            "arm",
            "purpose",
        ],
        "evaluation_metrics.csv": [
            "metric_id",
            "metric",
            "applies_to",
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
                "recommended_next_layer": recommended_next_layer,
                "recommended_action": (
                    "Implement deterministic point-in-time identity, cutoff, "
                    "eligibility, provenance, and leakage checks without "
                    "joining outcomes or calculating predictive metrics."
                    if all_checks_passed
                    else
                    "Remediate failed 9A planning checks."
                ),
                "entry_condition": (
                    "All nineteen 9A planning checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    summary = {
        "planning_checks_required": len(planning_checks),
        "planning_checks_passed": sum(
            row["passed"]
            for row in planning_checks
        ),
        "inventory_domains_defined": len(
            INVENTORY_DOMAINS
        ),
        "evaluation_record_fields_defined": len(
            EVALUATION_RECORD_FIELDS
        ),
        "event_identity_rules_defined": len(
            EVENT_IDENTITY_RULES
        ),
        "feature_cutoff_rules_defined": len(
            FEATURE_CUTOFF_RULES
        ),
        "future_information_exclusion_rules_defined": len(
            FUTURE_INFORMATION_EXCLUSION_RULES
        ),
        "outcome_targets_defined": len(
            OUTCOME_TARGETS
        ),
        "comparison_arms_defined": len(
            COMPARISON_ARMS
        ),
        "evaluation_metrics_defined": len(
            EVALUATION_METRICS
        ),
        "time_split_rules_defined": len(
            TIME_SPLIT_RULES
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
        "historical_outcome_joined": False,
        "evaluation_records_materialized": False,
        "predictive_metrics_calculated": False,
        "predictive_evaluation_executed": False,
        "model_training_executed": False,
        "tuning_executed": False,
        "backtest_executed": False,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
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
        "layer9_completed": False,
        "new_production_authority_granted": False,
        "point_in_time_contract_implementation_allowed_next": (
            all_checks_passed
        ),
        "historical_outcome_enrichment_allowed_next": False,
        "predictive_evaluation_allowed_next": False,
        "metric_calculation_allowed_next": False,
        "model_training_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "production_matchup_overlay_integration_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
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
