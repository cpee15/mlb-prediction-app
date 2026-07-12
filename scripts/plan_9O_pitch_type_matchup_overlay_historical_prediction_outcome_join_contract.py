#!/usr/bin/env python3
"""
Layer 9O
Pitch-Type Matchup Overlay Historical Prediction/Outcome Join Contract Plan

Plans the bounded contract for joining immutable historical prediction records
to Layer 9N historical evaluation rows.

Planning only.

This layer does not:

- generate baseline or augmented predictions;
- join production predictions to outcomes;
- calculate accuracy, calibration, discrimination, or incremental value;
- execute dataset splits or backtests;
- train or tune models, parameters, thresholds, or weights;
- modify production probabilities, simulations, pricing, markets, or bets.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9O"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_prediction_"
    "outcome_join_contract_plan"
)

PLAN_VERSION = (
    "layer_9O_historical_prediction_outcome_join_contract_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9O_pitch_type_matchup_overlay_"
    "historical_prediction_outcome_join_contract_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9N_pitch_type_matchup_overlay_"
    "historical_evaluation_dataset_contract.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9N_historical_evaluation_dataset_contract_v1"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_evaluation_"
    "dataset_contract_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_prediction_outcome_join_contract_planning"
)

PREDICTION_VARIANTS = [
    {
        "variant_id": "HPRED-V01",
        "prediction_variant": "baseline",
        "description": (
            "Historical canonical prediction produced without "
            "the pitch-type matchup overlay."
        ),
    },
    {
        "variant_id": "HPRED-V02",
        "prediction_variant": "augmented",
        "description": (
            "Historical candidate prediction produced with the "
            "pitch-type matchup overlay under a separately pinned contract."
        ),
    },
]

JOIN_GRAINS = [
    {
        "grain_id": "HPJOIN-G01",
        "event_level": "event",
        "identity": (
            "prediction_variant + target_id + game_id + event_sequence"
        ),
    },
    {
        "grain_id": "HPJOIN-G02",
        "event_level": "plate_appearance",
        "identity": (
            "prediction_variant + target_id + game_id + plate_appearance_id"
        ),
    },
    {
        "grain_id": "HPJOIN-G03",
        "event_level": "pitch",
        "identity": (
            "prediction_variant + target_id + game_id + "
            "plate_appearance_id + pitch_id"
        ),
    },
    {
        "grain_id": "HPJOIN-G04",
        "event_level": "contact",
        "identity": (
            "prediction_variant + target_id + game_id + "
            "plate_appearance_id + pitch_id"
        ),
    },
]

PREDICTION_FIELDS = [
    {
        "ordinal": 1,
        "field": "prediction_contract_version",
        "required": True,
        "role": "Pins the historical prediction schema.",
    },
    {
        "ordinal": 2,
        "field": "prediction_record_id",
        "required": True,
        "role": "Stable identity for one historical prediction.",
    },
    {
        "ordinal": 3,
        "field": "prediction_variant",
        "required": True,
        "role": "Separates baseline from augmented predictions.",
    },
    {
        "ordinal": 4,
        "field": "target_id",
        "required": True,
        "role": "Identifies the prediction target.",
    },
    {
        "ordinal": 5,
        "field": "event_level",
        "required": True,
        "role": "Defines prediction grain.",
    },
    {
        "ordinal": 6,
        "field": "game_id",
        "required": True,
        "role": "Scopes the prediction to one game.",
    },
    {
        "ordinal": 7,
        "field": "plate_appearance_id",
        "required": "conditional",
        "role": "Required below event grain.",
    },
    {
        "ordinal": 8,
        "field": "pitch_id",
        "required": "conditional",
        "role": "Required for pitch and contact grains.",
    },
    {
        "ordinal": 9,
        "field": "event_sequence",
        "required": True,
        "role": "Provides deterministic within-game ordering.",
    },
    {
        "ordinal": 10,
        "field": "prediction_generated_at_utc",
        "required": True,
        "role": "Records the point-in-time prediction cutoff.",
    },
    {
        "ordinal": 11,
        "field": "prediction_value",
        "required": True,
        "role": "Stores the prediction without evaluating it.",
    },
    {
        "ordinal": 12,
        "field": "prediction_value_type",
        "required": True,
        "role": "Defines probability, expected value, score, or target-specific type.",
    },
    {
        "ordinal": 13,
        "field": "model_artifact_id",
        "required": True,
        "role": "Pins the model artifact used.",
    },
    {
        "ordinal": 14,
        "field": "model_contract_version",
        "required": True,
        "role": "Pins model semantics.",
    },
    {
        "ordinal": 15,
        "field": "feature_contract_version",
        "required": True,
        "role": "Pins prediction feature semantics.",
    },
    {
        "ordinal": 16,
        "field": "overlay_contract_version",
        "required": "variant_conditional",
        "role": "Required only for augmented predictions.",
    },
    {
        "ordinal": 17,
        "field": "prediction_provenance_digest",
        "required": True,
        "role": "Provides immutable prediction lineage.",
    },
]

JOINED_FIELDS = [
    {
        "ordinal": 1,
        "field": "prediction_outcome_join_contract_version",
    },
    {
        "ordinal": 2,
        "field": "prediction_outcome_join_id",
    },
    {
        "ordinal": 3,
        "field": "evaluation_row_id",
    },
    {
        "ordinal": 4,
        "field": "prediction_record_id",
    },
    {
        "ordinal": 5,
        "field": "prediction_variant",
    },
    {
        "ordinal": 6,
        "field": "target_id",
    },
    {
        "ordinal": 7,
        "field": "event_level",
    },
    {
        "ordinal": 8,
        "field": "game_id",
    },
    {
        "ordinal": 9,
        "field": "plate_appearance_id",
    },
    {
        "ordinal": 10,
        "field": "pitch_id",
    },
    {
        "ordinal": 11,
        "field": "event_sequence",
    },
    {
        "ordinal": 12,
        "field": "prediction_generated_at_utc",
    },
    {
        "ordinal": 13,
        "field": "prediction_value",
    },
    {
        "ordinal": 14,
        "field": "prediction_value_type",
    },
    {
        "ordinal": 15,
        "field": "outcome_value",
    },
    {
        "ordinal": 16,
        "field": "outcome_available_at_utc",
    },
    {
        "ordinal": 17,
        "field": "evaluation_dataset_version",
    },
    {
        "ordinal": 18,
        "field": "prediction_contract_version",
    },
    {
        "ordinal": 19,
        "field": "model_contract_version",
    },
    {
        "ordinal": 20,
        "field": "feature_contract_version",
    },
    {
        "ordinal": 21,
        "field": "overlay_contract_version",
    },
    {
        "ordinal": 22,
        "field": "evaluation_row_digest",
    },
    {
        "ordinal": 23,
        "field": "prediction_provenance_digest",
    },
    {
        "ordinal": 24,
        "field": "prediction_outcome_join_identity_digest",
    },
    {
        "ordinal": 25,
        "field": "prediction_outcome_join_status",
    },
    {
        "ordinal": 26,
        "field": "prediction_outcome_join_eligible",
    },
    {
        "ordinal": 27,
        "field": "prediction_outcome_join_exclusion_codes",
    },
    {
        "ordinal": 28,
        "field": "prediction_outcome_join_record_digest",
    },
]

JOIN_RULES = [
    {
        "rule_id": "HPJOIN-R01",
        "rule": "evaluation_row_must_be_evaluation_eligible",
    },
    {
        "rule_id": "HPJOIN-R02",
        "rule": "prediction_and_evaluation_target_id_must_match",
    },
    {
        "rule_id": "HPJOIN-R03",
        "rule": "prediction_and_evaluation_event_level_must_match",
    },
    {
        "rule_id": "HPJOIN-R04",
        "rule": "prediction_and_evaluation_game_id_must_match",
    },
    {
        "rule_id": "HPJOIN-R05",
        "rule": "event_identity_must_match_at_declared_grain",
    },
    {
        "rule_id": "HPJOIN-R06",
        "rule": "prediction_generated_at_must_precede_scheduled_start",
    },
    {
        "rule_id": "HPJOIN-R07",
        "rule": "prediction_generated_at_must_precede_outcome_availability",
    },
    {
        "rule_id": "HPJOIN-R08",
        "rule": "prediction_contract_version_must_match",
    },
    {
        "rule_id": "HPJOIN-R09",
        "rule": "prediction_provenance_digest_must_be_valid",
    },
    {
        "rule_id": "HPJOIN-R10",
        "rule": "evaluation_row_digest_must_be_valid",
    },
    {
        "rule_id": "HPJOIN-R11",
        "rule": "baseline_and_augmented_rows_must_remain_distinct",
    },
    {
        "rule_id": "HPJOIN-R12",
        "rule": "augmented_prediction_requires_overlay_contract_version",
    },
    {
        "rule_id": "HPJOIN-R13",
        "rule": "baseline_prediction_must_not_claim_overlay_contract",
    },
    {
        "rule_id": "HPJOIN-R14",
        "rule": "one_prediction_record_to_zero_or_one_evaluation_row",
    },
    {
        "rule_id": "HPJOIN-R15",
        "rule": "one_evaluation_row_to_zero_or_one_prediction_per_variant",
    },
    {
        "rule_id": "HPJOIN-R16",
        "rule": "all_unmatched_and_invalid_rows_must_be_explicitly_classified",
    },
]

JOIN_STATUSES = [
    {
        "status": "matched_eligible",
        "join_eligible": True,
    },
    {
        "status": "evaluation_row_ineligible",
        "join_eligible": False,
    },
    {
        "status": "prediction_without_evaluation_row",
        "join_eligible": False,
    },
    {
        "status": "evaluation_row_without_baseline_prediction",
        "join_eligible": False,
    },
    {
        "status": "evaluation_row_without_augmented_prediction",
        "join_eligible": False,
    },
    {
        "status": "prediction_identity_mismatch",
        "join_eligible": False,
    },
    {
        "status": "prediction_point_in_time_violation",
        "join_eligible": False,
    },
    {
        "status": "prediction_contract_mismatch",
        "join_eligible": False,
    },
    {
        "status": "prediction_lineage_invalid",
        "join_eligible": False,
    },
    {
        "status": "prediction_variant_contract_invalid",
        "join_eligible": False,
    },
    {
        "status": "duplicate_prediction_identity",
        "join_eligible": False,
    },
    {
        "status": "many_to_many_detected",
        "join_eligible": False,
    },
]

EXCLUSION_CODES = [
    {
        "code": "historical_prediction_outcome_evaluation_row_ineligible",
        "category": "evaluation",
    },
    {
        "code": "historical_prediction_outcome_prediction_missing",
        "category": "missingness",
    },
    {
        "code": "historical_prediction_outcome_evaluation_row_missing",
        "category": "missingness",
    },
    {
        "code": "historical_prediction_outcome_identity_mismatch",
        "category": "identity",
    },
    {
        "code": "historical_prediction_outcome_point_in_time_violation",
        "category": "time",
    },
    {
        "code": "historical_prediction_outcome_contract_version_mismatch",
        "category": "contract",
    },
    {
        "code": "historical_prediction_outcome_prediction_lineage_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_prediction_outcome_evaluation_lineage_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_prediction_outcome_variant_contract_invalid",
        "category": "variant",
    },
    {
        "code": "historical_prediction_outcome_duplicate_prediction_identity",
        "category": "cardinality",
    },
    {
        "code": "historical_prediction_outcome_duplicate_evaluation_identity",
        "category": "cardinality",
    },
    {
        "code": "historical_prediction_outcome_many_to_many_detected",
        "category": "cardinality",
    },
    {
        "code": "historical_prediction_outcome_source_record_invalid",
        "category": "source",
    },
]

CARDINALITY_RULES = [
    {
        "rule_id": "HPJOIN-C01",
        "rule": "prediction_record_id_must_be_unique",
    },
    {
        "rule_id": "HPJOIN-C02",
        "rule": "evaluation_row_id_must_be_unique",
    },
    {
        "rule_id": "HPJOIN-C03",
        "rule": "evaluation_row_and_variant_pair_must_be_unique",
    },
    {
        "rule_id": "HPJOIN-C04",
        "rule": "prediction_identity_must_map_to_at_most_one_evaluation_row",
    },
    {
        "rule_id": "HPJOIN-C05",
        "rule": "evaluation_identity_must_map_to_at_most_one_prediction_per_variant",
    },
    {
        "rule_id": "HPJOIN-C06",
        "rule": "many_to_many_join_is_prohibited",
    },
    {
        "rule_id": "HPJOIN-C07",
        "rule": "all_source_counts_must_reconcile",
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
        "field": "prediction_variant",
    },
    {
        "ordinal": 7,
        "field": "prediction_outcome_join_id",
    },
]

IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "verify_layer_9o_plan_and_layer_9n_predecessor",
    },
    {
        "ordinal": 2,
        "step": "replay_layer_9n_historical_evaluation_dataset",
    },
    {
        "ordinal": 3,
        "step": "create_deterministic_synthetic_prediction_fixtures",
    },
    {
        "ordinal": 4,
        "step": "validate_prediction_contract_and_variant_semantics",
    },
    {
        "ordinal": 5,
        "step": "validate_prediction_point_in_time_boundaries",
    },
    {
        "ordinal": 6,
        "step": "index_predictions_and_evaluation_rows_by_grain",
    },
    {
        "ordinal": 7,
        "step": "detect_duplicate_and_many_to_many_identities",
    },
    {
        "ordinal": 8,
        "step": "join_each_prediction_to_zero_or_one_evaluation_row",
    },
    {
        "ordinal": 9,
        "step": "classify_unmatched_evaluation_rows_by_prediction_variant",
    },
    {
        "ordinal": 10,
        "step": "derive_join_identity_and_record_digests",
    },
    {
        "ordinal": 11,
        "step": "replay_join_under_reversed_input_order",
    },
    {
        "ordinal": 12,
        "step": "reconcile_source_join_and_exclusion_counts",
    },
    {
        "ordinal": 13,
        "step": "write_temporary_diagnostic_artifacts_only",
    },
]

DIAGNOSTIC_ARTIFACTS = [
    {
        "artifact": "planning_checks.csv",
    },
    {
        "artifact": "prediction_variants.csv",
    },
    {
        "artifact": "join_grains.csv",
    },
    {
        "artifact": "prediction_field_contract.csv",
    },
    {
        "artifact": "joined_field_contract.csv",
    },
    {
        "artifact": "join_rules.csv",
    },
    {
        "artifact": "join_statuses.csv",
    },
    {
        "artifact": "exclusion_code_catalog.csv",
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
        "artifact": "prediction_outcome_join_plan_summary.json",
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
    "historical_outcome_collection_execution",
    "historical_outcome_fetch_execution",
    "historical_outcome_prediction_join_execution",
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
        and EXPECTED_PREDECESSOR_VERSION
        in predecessor_constants
        and EXPECTED_PREDECESSOR_DIAGNOSIS
        in predecessor_constants
        and EXPECTED_PREDECESSOR_AUTHORITY
        in predecessor_constants
    )

    checks = [
        {
            "check": "nine_n_predecessor_verified",
            "actual": predecessor_verified,
            "expected": True,
            "passed": predecessor_verified,
        },
        {
            "check": "two_prediction_variants_defined",
            "actual": len(PREDICTION_VARIANTS),
            "expected": 2,
            "passed": (
                len(PREDICTION_VARIANTS) == 2
                and {
                    row["prediction_variant"]
                    for row in PREDICTION_VARIANTS
                }
                == {
                    "baseline",
                    "augmented",
                }
            ),
        },
        {
            "check": "four_join_grains_defined",
            "actual": len(JOIN_GRAINS),
            "expected": 4,
            "passed": len(JOIN_GRAINS) == 4,
        },
        {
            "check": "seventeen_prediction_fields_defined",
            "actual": len(PREDICTION_FIELDS),
            "expected": 17,
            "passed": (
                len(PREDICTION_FIELDS) == 17
                and len({
                    row["field"]
                    for row in PREDICTION_FIELDS
                }) == 17
            ),
        },
        {
            "check": "twenty_eight_joined_fields_defined",
            "actual": len(JOINED_FIELDS),
            "expected": 28,
            "passed": (
                len(JOINED_FIELDS) == 28
                and len({
                    row["field"]
                    for row in JOINED_FIELDS
                }) == 28
            ),
        },
        {
            "check": "sixteen_join_rules_defined",
            "actual": len(JOIN_RULES),
            "expected": 16,
            "passed": len(JOIN_RULES) == 16,
        },
        {
            "check": "twelve_join_statuses_defined",
            "actual": len(JOIN_STATUSES),
            "expected": 12,
            "passed": len(JOIN_STATUSES) == 12,
        },
        {
            "check": "thirteen_exclusion_codes_defined",
            "actual": len(EXCLUSION_CODES),
            "expected": 13,
            "passed": len(EXCLUSION_CODES) == 13,
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
            "check": "thirteen_implementation_steps_defined",
            "actual": len(IMPLEMENTATION_STEPS),
            "expected": 13,
            "passed": len(IMPLEMENTATION_STEPS) == 13,
        },
        {
            "check": "fourteen_diagnostic_artifacts_defined",
            "actual": len(DIAGNOSTIC_ARTIFACTS),
            "expected": 14,
            "passed": len(DIAGNOSTIC_ARTIFACTS) == 14,
        },
        {
            "check": "baseline_and_augmented_remain_distinct",
            "actual": True,
            "expected": True,
            "passed": any(
                row["rule"]
                == "baseline_and_augmented_rows_must_remain_distinct"
                for row in JOIN_RULES
            ),
        },
        {
            "check": "point_in_time_boundaries_defined",
            "actual": 2,
            "expected": 2,
            "passed": sum(
                "prediction_generated_at"
                in row["rule"]
                for row in JOIN_RULES
            ) == 2,
        },
        {
            "check": "many_to_many_join_prohibited",
            "actual": True,
            "expected": True,
            "passed": any(
                row["rule"]
                == "many_to_many_join_is_prohibited"
                for row in CARDINALITY_RULES
            ),
        },
        {
            "check": "missing_predictions_explicitly_classified",
            "actual": True,
            "expected": True,
            "passed": all(
                status
                in {
                    row["status"]
                    for row in JOIN_STATUSES
                }
                for status in {
                    "evaluation_row_without_baseline_prediction",
                    "evaluation_row_without_augmented_prediction",
                }
            ),
        },
        {
            "check": "prediction_generation_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "prediction_outcome_join_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "predictive_metrics_not_calculated",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "dataset_split_not_executed",
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
            "prediction_variants": PREDICTION_VARIANTS,
            "join_grains": JOIN_GRAINS,
            "prediction_fields": PREDICTION_FIELDS,
            "joined_fields": JOINED_FIELDS,
            "join_rules": JOIN_RULES,
            "join_statuses": JOIN_STATUSES,
            "exclusion_codes": EXCLUSION_CODES,
            "cardinality_rules": CARDINALITY_RULES,
            "ordering_fields": ORDERING_FIELDS,
            "implementation_steps": IMPLEMENTATION_STEPS,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_prediction_"
        "outcome_join_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_prediction_"
        "outcome_join_contract_plan_failed"
    )

    next_layer = (
        "9P_pitch_type_matchup_overlay_historical_prediction_"
        "outcome_join_contract_implementation"
        if all_checks_passed
        else
        "9O_pitch_type_matchup_overlay_historical_prediction_"
        "outcome_join_contract_plan_remediation"
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
        OUTPUT_DIR / "prediction_variants.csv",
        [
            "variant_id",
            "prediction_variant",
            "description",
        ],
        PREDICTION_VARIANTS,
    )

    write_csv(
        OUTPUT_DIR / "join_grains.csv",
        [
            "grain_id",
            "event_level",
            "identity",
        ],
        JOIN_GRAINS,
    )

    write_csv(
        OUTPUT_DIR / "prediction_field_contract.csv",
        [
            "ordinal",
            "field",
            "required",
            "role",
        ],
        PREDICTION_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "joined_field_contract.csv",
        [
            "ordinal",
            "field",
        ],
        JOINED_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "join_rules.csv",
        [
            "rule_id",
            "rule",
        ],
        JOIN_RULES,
    )

    write_csv(
        OUTPUT_DIR / "join_statuses.csv",
        [
            "status",
            "join_eligible",
        ],
        JOIN_STATUSES,
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
                    "Layer 9O is planning-only and grants no "
                    "prediction generation, join execution, evaluation, "
                    "production, market, or betting authority."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_prediction_outcome_join_contract_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "Layer 9P may implement the bounded deterministic "
                    "temporary diagnostic join contract planned here."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_verified": predecessor_verified,
        "prediction_variants": len(PREDICTION_VARIANTS),
        "join_grains": len(JOIN_GRAINS),
        "prediction_fields": len(PREDICTION_FIELDS),
        "joined_fields": len(JOINED_FIELDS),
        "join_rules": len(JOIN_RULES),
        "join_statuses": len(JOIN_STATUSES),
        "exclusion_codes": len(EXCLUSION_CODES),
        "cardinality_rules": len(CARDINALITY_RULES),
        "ordering_fields": len(ORDERING_FIELDS),
        "implementation_steps": len(IMPLEMENTATION_STEPS),
        "planning_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "planning_checks_required": len(checks),
        "plan_digest": plan_digest,
        "prediction_records_generated": 0,
        "prediction_outcome_joins_executed": 0,
        "predictive_metrics_calculated": 0,
        "dataset_splits_executed": 0,
        "production_records_materialized": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed": all_checks_passed,
        "recommended_next_layer": next_layer,
    }

    write_json(
        OUTPUT_DIR
        / "prediction_outcome_join_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_prediction_outcome_join_contract_implementation"
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
        "Prediction variants: "
        f"{len(PREDICTION_VARIANTS)}"
    )
    print(
        f"Join grains: {len(JOIN_GRAINS)}"
    )
    print(
        "Prediction fields: "
        f"{len(PREDICTION_FIELDS)}"
    )
    print(
        f"Joined fields: {len(JOINED_FIELDS)}"
    )
    print(
        f"Join rules: {len(JOIN_RULES)}"
    )
    print(
        f"Join statuses: {len(JOIN_STATUSES)}"
    )
    print(
        f"Exclusion codes: {len(EXCLUSION_CODES)}"
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
        "Prediction records generated: 0"
    )
    print(
        "Prediction/outcome joins executed: 0"
    )
    print(
        "Predictive metrics calculated: 0"
    )
    print(
        "Dataset splits executed: 0"
    )
    print(
        "Production records materialized: 0"
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
