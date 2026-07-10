#!/usr/bin/env python3
"""
Layer 9M
Pitch-Type Matchup Overlay Historical Evaluation Dataset Contract Plan

Plans the bounded contract for transforming Layer 9L diagnostic
feature/outcome join records into a deterministic historical evaluation dataset.

Planning only.

This layer does not:

- materialize a production historical evaluation dataset;
- join predictions to outcomes;
- calculate accuracy, calibration, discrimination, or incremental value;
- create train, validation, or test samples for model fitting;
- train or tune models, parameters, thresholds, or weights;
- alter production probabilities, simulations, markets, pricing, or bets.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9M"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_evaluation_"
    "dataset_contract_plan"
)

DATASET_PLAN_VERSION = (
    "layer_9M_historical_evaluation_dataset_contract_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9M_pitch_type_matchup_overlay_"
    "historical_evaluation_dataset_contract_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9L_pitch_type_matchup_overlay_"
    "historical_outcome_feature_join_contract.py"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_outcome_"
    "feature_join_contract_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_evaluation_dataset_contract_planning"
)

EXPECTED_JOIN_CONTRACT_VERSION = (
    "layer_9L_historical_outcome_feature_join_contract_v1"
)

EXPECTED_FEATURE_CONTRACT_VERSION = (
    "layer_9L_synthetic_historical_feature_contract_v1"
)

EXPECTED_OUTCOME_CONTRACT_VERSION = (
    "layer_9D_historical_outcome_contract_v1"
)

VALID_EVENT_LEVELS = (
    "event",
    "plate_appearance",
    "pitch",
    "contact",
)

DATASET_GRAINS = [
    {
        "grain_id": "HDSET-G01",
        "event_level": "event",
        "primary_identity": (
            "target_id + game_id + event_sequence"
        ),
        "allowed_join_statuses": (
            "matched_eligible"
        ),
    },
    {
        "grain_id": "HDSET-G02",
        "event_level": "plate_appearance",
        "primary_identity": (
            "target_id + game_id + plate_appearance_id"
        ),
        "allowed_join_statuses": (
            "matched_eligible"
        ),
    },
    {
        "grain_id": "HDSET-G03",
        "event_level": "pitch",
        "primary_identity": (
            "target_id + game_id + plate_appearance_id + pitch_id"
        ),
        "allowed_join_statuses": (
            "matched_eligible"
        ),
    },
    {
        "grain_id": "HDSET-G04",
        "event_level": "contact",
        "primary_identity": (
            "target_id + game_id + plate_appearance_id + pitch_id"
        ),
        "allowed_join_statuses": (
            "matched_eligible"
        ),
    },
]

DATASET_FIELDS = [
    {
        "ordinal": 1,
        "field": "evaluation_dataset_version",
        "type": "string",
        "required": True,
        "role": "Pins every row to one immutable dataset contract.",
    },
    {
        "ordinal": 2,
        "field": "evaluation_row_id",
        "type": "deterministic_string",
        "required": True,
        "role": "Stable identity derived from row grain and source contracts.",
    },
    {
        "ordinal": 3,
        "field": "target_id",
        "type": "string",
        "required": True,
        "role": "Identifies the historical outcome target.",
    },
    {
        "ordinal": 4,
        "field": "event_level",
        "type": "enum",
        "required": True,
        "role": "Defines the observational grain.",
    },
    {
        "ordinal": 5,
        "field": "game_id",
        "type": "string",
        "required": True,
        "role": "Scopes the row to one game.",
    },
    {
        "ordinal": 6,
        "field": "game_date",
        "type": "date",
        "required": True,
        "role": "Supports chronological ordering and partitioning.",
    },
    {
        "ordinal": 7,
        "field": "scheduled_start_utc",
        "type": "datetime",
        "required": True,
        "role": "Defines the event-time boundary.",
    },
    {
        "ordinal": 8,
        "field": "plate_appearance_id",
        "type": "nullable_string",
        "required": "conditional",
        "role": "Required below game-event grain.",
    },
    {
        "ordinal": 9,
        "field": "pitch_id",
        "type": "nullable_string",
        "required": "conditional",
        "role": "Required for pitch and contact grains.",
    },
    {
        "ordinal": 10,
        "field": "event_sequence",
        "type": "integer",
        "required": True,
        "role": "Preserves deterministic within-game order.",
    },
    {
        "ordinal": 11,
        "field": "feature_row_id",
        "type": "string",
        "required": True,
        "role": "Links to the historical feature artifact.",
    },
    {
        "ordinal": 12,
        "field": "feature_as_of_utc",
        "type": "datetime",
        "required": True,
        "role": "Records the feature information cutoff.",
    },
    {
        "ordinal": 13,
        "field": "feature_contract_version",
        "type": "string",
        "required": True,
        "role": "Pins the historical feature schema.",
    },
    {
        "ordinal": 14,
        "field": "feature_provenance_digest",
        "type": "sha256_string",
        "required": True,
        "role": "Provides immutable feature lineage.",
    },
    {
        "ordinal": 15,
        "field": "historical_outcome_id",
        "type": "string",
        "required": True,
        "role": "Links to the historical outcome record.",
    },
    {
        "ordinal": 16,
        "field": "outcome_value",
        "type": "target_defined",
        "required": True,
        "role": "Stores the terminal label without coercing target semantics.",
    },
    {
        "ordinal": 17,
        "field": "outcome_available_at_utc",
        "type": "datetime",
        "required": True,
        "role": "Proves the outcome became available after feature generation.",
    },
    {
        "ordinal": 18,
        "field": "historical_outcome_contract_version",
        "type": "string",
        "required": True,
        "role": "Pins the historical outcome schema.",
    },
    {
        "ordinal": 19,
        "field": "outcome_provenance_digest",
        "type": "sha256_string",
        "required": True,
        "role": "Provides immutable outcome lineage.",
    },
    {
        "ordinal": 20,
        "field": "join_contract_version",
        "type": "string",
        "required": True,
        "role": "Pins the feature/outcome join semantics.",
    },
    {
        "ordinal": 21,
        "field": "join_identity_digest",
        "type": "sha256_string",
        "required": True,
        "role": "Proves the join identity used.",
    },
    {
        "ordinal": 22,
        "field": "joined_record_digest",
        "type": "sha256_string",
        "required": True,
        "role": "Pins the joined source record.",
    },
    {
        "ordinal": 23,
        "field": "evaluation_eligible",
        "type": "boolean",
        "required": True,
        "role": "Records row-level dataset eligibility.",
    },
    {
        "ordinal": 24,
        "field": "evaluation_exclusion_codes",
        "type": "sorted_unique_string_array",
        "required": True,
        "role": "Explains every excluded row deterministically.",
    },
    {
        "ordinal": 25,
        "field": "evaluation_row_digest",
        "type": "sha256_string",
        "required": True,
        "role": "Provides immutable evaluation-row integrity.",
    },
]

ELIGIBILITY_RULES = [
    {
        "rule_id": "HDSET-E01",
        "rule": "join_status_must_be_matched_eligible",
    },
    {
        "rule_id": "HDSET-E02",
        "rule": "source_join_evaluation_eligible_must_be_true",
    },
    {
        "rule_id": "HDSET-E03",
        "rule": "outcome_missing_must_be_false",
    },
    {
        "rule_id": "HDSET-E04",
        "rule": "historical_outcome_eligible_must_be_true",
    },
    {
        "rule_id": "HDSET-E05",
        "rule": "feature_as_of_must_precede_scheduled_start",
    },
    {
        "rule_id": "HDSET-E06",
        "rule": "feature_as_of_must_precede_outcome_availability",
    },
    {
        "rule_id": "HDSET-E07",
        "rule": "source_contract_versions_must_match",
    },
    {
        "rule_id": "HDSET-E08",
        "rule": "event_identity_must_match_event_level",
    },
    {
        "rule_id": "HDSET-E09",
        "rule": "source_digests_must_be_valid_sha256",
    },
    {
        "rule_id": "HDSET-E10",
        "rule": "evaluation_row_identity_must_be_unique",
    },
    {
        "rule_id": "HDSET-E11",
        "rule": "outcome_fields_must_not_enter_feature_payload",
    },
    {
        "rule_id": "HDSET-E12",
        "rule": "all_exclusions_must_be_explicit_and_deterministic",
    },
]

EXCLUSION_CODES = [
    {
        "code": "historical_evaluation_join_not_eligible",
        "category": "join",
    },
    {
        "code": "historical_evaluation_outcome_missing",
        "category": "outcome",
    },
    {
        "code": "historical_evaluation_outcome_ineligible",
        "category": "outcome",
    },
    {
        "code": "historical_evaluation_point_in_time_violation",
        "category": "time",
    },
    {
        "code": "historical_evaluation_contract_version_mismatch",
        "category": "contract",
    },
    {
        "code": "historical_evaluation_event_identity_invalid",
        "category": "identity",
    },
    {
        "code": "historical_evaluation_feature_lineage_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_evaluation_outcome_lineage_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_evaluation_join_lineage_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_evaluation_duplicate_row_identity",
        "category": "cardinality",
    },
    {
        "code": "historical_evaluation_outcome_field_in_features",
        "category": "leakage",
    },
    {
        "code": "historical_evaluation_source_record_invalid",
        "category": "source",
    },
]

PARTITION_FIELDS = [
    {
        "ordinal": 1,
        "field": "game_date",
        "purpose": "Primary chronological partition boundary.",
    },
    {
        "ordinal": 2,
        "field": "target_id",
        "purpose": "Prevents mixing target semantics.",
    },
    {
        "ordinal": 3,
        "field": "event_level",
        "purpose": "Prevents mixing observational grains.",
    },
]

ORDERING_FIELDS = [
    {
        "ordinal": 1,
        "field": "game_date",
        "direction": "ascending",
    },
    {
        "ordinal": 2,
        "field": "scheduled_start_utc",
        "direction": "ascending",
    },
    {
        "ordinal": 3,
        "field": "game_id",
        "direction": "ascending",
    },
    {
        "ordinal": 4,
        "field": "event_sequence",
        "direction": "ascending",
    },
    {
        "ordinal": 5,
        "field": "target_id",
        "direction": "ascending",
    },
    {
        "ordinal": 6,
        "field": "evaluation_row_id",
        "direction": "ascending",
    },
]

SPLIT_BOUNDARIES = [
    {
        "boundary_id": "HDSET-S01",
        "name": "chronological_only",
        "rule": (
            "Any future train, validation, or test split must be chronological; "
            "random row-level splitting is prohibited."
        ),
    },
    {
        "boundary_id": "HDSET-S02",
        "name": "game_atomicity",
        "rule": (
            "All rows from one game must remain in the same split."
        ),
    },
    {
        "boundary_id": "HDSET-S03",
        "name": "target_reporting",
        "rule": (
            "Target-level results must remain separately reportable."
        ),
    },
    {
        "boundary_id": "HDSET-S04",
        "name": "event_level_reporting",
        "rule": (
            "Event-level results must remain separately reportable."
        ),
    },
    {
        "boundary_id": "HDSET-S05",
        "name": "no_split_execution",
        "rule": (
            "Layer 9M plans split boundaries but does not assign rows to splits."
        ),
    },
]

CARDINALITY_RULES = [
    {
        "rule_id": "HDSET-C01",
        "rule": "one_joined_record_to_zero_or_one_evaluation_row",
    },
    {
        "rule_id": "HDSET-C02",
        "rule": "one_evaluation_row_id_to_exactly_one_row",
    },
    {
        "rule_id": "HDSET-C03",
        "rule": "one_join_identity_digest_to_zero_or_one_eligible_row",
    },
    {
        "rule_id": "HDSET-C04",
        "rule": "duplicate_source_identity_must_be_excluded",
    },
    {
        "rule_id": "HDSET-C05",
        "rule": "excluded_rows_must_not_be_silently_dropped",
    },
    {
        "rule_id": "HDSET-C06",
        "rule": "dataset_counts_must_reconcile_to_source_join_counts",
    },
]

DATASET_STATUSES = [
    {
        "status": "evaluation_eligible",
        "included": True,
    },
    {
        "status": "excluded_join_ineligible",
        "included": False,
    },
    {
        "status": "excluded_missing_outcome",
        "included": False,
    },
    {
        "status": "excluded_outcome_ineligible",
        "included": False,
    },
    {
        "status": "excluded_point_in_time_violation",
        "included": False,
    },
    {
        "status": "excluded_contract_mismatch",
        "included": False,
    },
    {
        "status": "excluded_identity_invalid",
        "included": False,
    },
    {
        "status": "excluded_lineage_invalid",
        "included": False,
    },
    {
        "status": "excluded_duplicate_identity",
        "included": False,
    },
    {
        "status": "excluded_leakage_detected",
        "included": False,
    },
]

DIAGNOSTIC_ARTIFACTS = [
    {
        "artifact": "planning_checks.csv",
        "purpose": "Machine-readable Layer 9M planning validation.",
    },
    {
        "artifact": "dataset_grains.csv",
        "purpose": "Supported historical evaluation grains.",
    },
    {
        "artifact": "dataset_field_contract.csv",
        "purpose": "Immutable evaluation dataset row schema.",
    },
    {
        "artifact": "eligibility_rules.csv",
        "purpose": "Row inclusion and exclusion rules.",
    },
    {
        "artifact": "exclusion_code_catalog.csv",
        "purpose": "Deterministic row exclusion reasons.",
    },
    {
        "artifact": "partition_fields.csv",
        "purpose": "Dataset partition dimensions.",
    },
    {
        "artifact": "ordering_fields.csv",
        "purpose": "Canonical deterministic row ordering.",
    },
    {
        "artifact": "split_boundaries.csv",
        "purpose": "Future chronological split constraints.",
    },
    {
        "artifact": "cardinality_rules.csv",
        "purpose": "Source-to-dataset cardinality requirements.",
    },
    {
        "artifact": "dataset_statuses.csv",
        "purpose": "Allowed row classification states.",
    },
    {
        "artifact": "authority_boundaries.csv",
        "purpose": "Explicitly granted and withheld authorities.",
    },
    {
        "artifact": "dataset_contract_summary.json",
        "purpose": "High-level planning summary.",
    },
    {
        "artifact": "diagnosis.json",
        "purpose": "Terminal diagnosis and next-layer authority.",
    },
]

IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "verify_layer_9m_plan_and_layer_9l_predecessor",
    },
    {
        "ordinal": 2,
        "step": "load_layer_9l_joined_diagnostic_records",
    },
    {
        "ordinal": 3,
        "step": "validate_source_contract_versions",
    },
    {
        "ordinal": 4,
        "step": "classify_every_source_join_record",
    },
    {
        "ordinal": 5,
        "step": "derive_deterministic_evaluation_row_identity",
    },
    {
        "ordinal": 6,
        "step": "validate_point_in_time_eligibility",
    },
    {
        "ordinal": 7,
        "step": "validate_feature_outcome_and_join_lineage",
    },
    {
        "ordinal": 8,
        "step": "preserve_explicit_exclusion_rows",
    },
    {
        "ordinal": 9,
        "step": "enforce_cardinality_and_count_reconciliation",
    },
    {
        "ordinal": 10,
        "step": "sort_rows_in_canonical_order",
    },
    {
        "ordinal": 11,
        "step": "replay_materialization_for_determinism",
    },
    {
        "ordinal": 12,
        "step": "write_temporary_diagnostic_artifacts_only",
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
    "historical_evaluation_dataset_materialization",
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
        and EXPECTED_PREDECESSOR_DIAGNOSIS
        in predecessor_constants
        and EXPECTED_PREDECESSOR_AUTHORITY
        in predecessor_constants
        and EXPECTED_JOIN_CONTRACT_VERSION
        in predecessor_constants
        and EXPECTED_FEATURE_CONTRACT_VERSION
        in predecessor_constants
        and EXPECTED_OUTCOME_CONTRACT_VERSION
        in predecessor_constants
    )

    dataset_field_names = [
        row["field"]
        for row in DATASET_FIELDS
    ]

    eligibility_rule_ids = [
        row["rule_id"]
        for row in ELIGIBILITY_RULES
    ]

    exclusion_code_names = [
        row["code"]
        for row in EXCLUSION_CODES
    ]

    checks = [
        {
            "check": "nine_l_predecessor_verified",
            "actual": predecessor_verified,
            "expected": True,
            "passed": predecessor_verified,
        },
        {
            "check": "four_dataset_grains_defined",
            "actual": len(DATASET_GRAINS),
            "expected": 4,
            "passed": (
                len(DATASET_GRAINS) == 4
                and {
                    row["event_level"]
                    for row in DATASET_GRAINS
                }
                == set(VALID_EVENT_LEVELS)
            ),
        },
        {
            "check": "twenty_five_dataset_fields_defined",
            "actual": len(DATASET_FIELDS),
            "expected": 25,
            "passed": (
                len(DATASET_FIELDS) == 25
                and len(set(dataset_field_names)) == 25
            ),
        },
        {
            "check": "twelve_eligibility_rules_defined",
            "actual": len(ELIGIBILITY_RULES),
            "expected": 12,
            "passed": (
                len(ELIGIBILITY_RULES) == 12
                and len(set(eligibility_rule_ids)) == 12
            ),
        },
        {
            "check": "twelve_exclusion_codes_defined",
            "actual": len(EXCLUSION_CODES),
            "expected": 12,
            "passed": (
                len(EXCLUSION_CODES) == 12
                and len(set(exclusion_code_names)) == 12
            ),
        },
        {
            "check": "three_partition_fields_defined",
            "actual": len(PARTITION_FIELDS),
            "expected": 3,
            "passed": len(PARTITION_FIELDS) == 3,
        },
        {
            "check": "six_ordering_fields_defined",
            "actual": len(ORDERING_FIELDS),
            "expected": 6,
            "passed": len(ORDERING_FIELDS) == 6,
        },
        {
            "check": "five_split_boundaries_defined",
            "actual": len(SPLIT_BOUNDARIES),
            "expected": 5,
            "passed": len(SPLIT_BOUNDARIES) == 5,
        },
        {
            "check": "six_cardinality_rules_defined",
            "actual": len(CARDINALITY_RULES),
            "expected": 6,
            "passed": len(CARDINALITY_RULES) == 6,
        },
        {
            "check": "ten_dataset_statuses_defined",
            "actual": len(DATASET_STATUSES),
            "expected": 10,
            "passed": len(DATASET_STATUSES) == 10,
        },
        {
            "check": "thirteen_diagnostic_artifacts_defined",
            "actual": len(DIAGNOSTIC_ARTIFACTS),
            "expected": 13,
            "passed": len(DIAGNOSTIC_ARTIFACTS) == 13,
        },
        {
            "check": "twelve_implementation_steps_defined",
            "actual": len(IMPLEMENTATION_STEPS),
            "expected": 12,
            "passed": len(IMPLEMENTATION_STEPS) == 12,
        },
        {
            "check": "evaluation_row_identity_defined",
            "actual": (
                "evaluation_row_id"
                in dataset_field_names
            ),
            "expected": True,
            "passed": (
                "evaluation_row_id"
                in dataset_field_names
            ),
        },
        {
            "check": "feature_cutoff_preserved",
            "actual": (
                "feature_as_of_utc"
                in dataset_field_names
            ),
            "expected": True,
            "passed": (
                "feature_as_of_utc"
                in dataset_field_names
            ),
        },
        {
            "check": "outcome_availability_preserved",
            "actual": (
                "outcome_available_at_utc"
                in dataset_field_names
            ),
            "expected": True,
            "passed": (
                "outcome_available_at_utc"
                in dataset_field_names
            ),
        },
        {
            "check": "three_source_lineage_digests_preserved",
            "actual": sum(
                field
                in dataset_field_names
                for field in {
                    "feature_provenance_digest",
                    "outcome_provenance_digest",
                    "joined_record_digest",
                }
            ),
            "expected": 3,
            "passed": all(
                field
                in dataset_field_names
                for field in {
                    "feature_provenance_digest",
                    "outcome_provenance_digest",
                    "joined_record_digest",
                }
            ),
        },
        {
            "check": "exclusion_rows_preserved",
            "actual": (
                "evaluation_exclusion_codes"
                in dataset_field_names
            ),
            "expected": True,
            "passed": (
                "evaluation_exclusion_codes"
                in dataset_field_names
            ),
        },
        {
            "check": "chronological_split_boundary_defined",
            "actual": any(
                row["name"] == "chronological_only"
                for row in SPLIT_BOUNDARIES
            ),
            "expected": True,
            "passed": any(
                row["name"] == "chronological_only"
                for row in SPLIT_BOUNDARIES
            ),
        },
        {
            "check": "game_atomicity_boundary_defined",
            "actual": any(
                row["name"] == "game_atomicity"
                for row in SPLIT_BOUNDARIES
            ),
            "expected": True,
            "passed": any(
                row["name"] == "game_atomicity"
                for row in SPLIT_BOUNDARIES
            ),
        },
        {
            "check": "dataset_materialization_not_executed",
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
            "dataset_plan_version": DATASET_PLAN_VERSION,
            "dataset_grains": DATASET_GRAINS,
            "dataset_fields": DATASET_FIELDS,
            "eligibility_rules": ELIGIBILITY_RULES,
            "exclusion_codes": EXCLUSION_CODES,
            "partition_fields": PARTITION_FIELDS,
            "ordering_fields": ORDERING_FIELDS,
            "split_boundaries": SPLIT_BOUNDARIES,
            "cardinality_rules": CARDINALITY_RULES,
            "dataset_statuses": DATASET_STATUSES,
            "implementation_steps": IMPLEMENTATION_STEPS,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_evaluation_"
        "dataset_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_evaluation_"
        "dataset_contract_plan_failed"
    )

    next_layer = (
        "9N_pitch_type_matchup_overlay_historical_evaluation_"
        "dataset_contract_implementation"
        if all_checks_passed
        else
        "9M_pitch_type_matchup_overlay_historical_evaluation_"
        "dataset_contract_plan_remediation"
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
        OUTPUT_DIR / "dataset_grains.csv",
        [
            "grain_id",
            "event_level",
            "primary_identity",
            "allowed_join_statuses",
        ],
        DATASET_GRAINS,
    )

    write_csv(
        OUTPUT_DIR / "dataset_field_contract.csv",
        [
            "ordinal",
            "field",
            "type",
            "required",
            "role",
        ],
        DATASET_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "eligibility_rules.csv",
        [
            "rule_id",
            "rule",
        ],
        ELIGIBILITY_RULES,
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
        OUTPUT_DIR / "partition_fields.csv",
        [
            "ordinal",
            "field",
            "purpose",
        ],
        PARTITION_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "ordering_fields.csv",
        [
            "ordinal",
            "field",
            "direction",
        ],
        ORDERING_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "split_boundaries.csv",
        [
            "boundary_id",
            "name",
            "rule",
        ],
        SPLIT_BOUNDARIES,
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
        OUTPUT_DIR / "dataset_statuses.csv",
        [
            "status",
            "included",
        ],
        DATASET_STATUSES,
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
                    "Layer 9M is planning-only and grants no "
                    "execution, evaluation, production, market, "
                    "or betting authority."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_evaluation_dataset_contract_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "Layer 9N may implement the bounded temporary "
                    "diagnostic dataset contract defined by Layer 9M."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "dataset_plan_version": DATASET_PLAN_VERSION,
        "predecessor_verified": predecessor_verified,
        "dataset_grains": len(DATASET_GRAINS),
        "dataset_fields": len(DATASET_FIELDS),
        "eligibility_rules": len(ELIGIBILITY_RULES),
        "exclusion_codes": len(EXCLUSION_CODES),
        "partition_fields": len(PARTITION_FIELDS),
        "ordering_fields": len(ORDERING_FIELDS),
        "split_boundaries": len(SPLIT_BOUNDARIES),
        "cardinality_rules": len(CARDINALITY_RULES),
        "dataset_statuses": len(DATASET_STATUSES),
        "implementation_steps": len(IMPLEMENTATION_STEPS),
        "planning_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "planning_checks_required": len(checks),
        "plan_digest": plan_digest,
        "dataset_records_materialized": 0,
        "dataset_splits_executed": 0,
        "prediction_outcome_joins_executed": 0,
        "predictive_metrics_calculated": 0,
        "production_records_materialized": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed": all_checks_passed,
        "recommended_next_layer": next_layer,
    }

    write_json(
        OUTPUT_DIR / "dataset_contract_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_evaluation_dataset_contract_implementation"
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
        "Dataset plan version: "
        f"{DATASET_PLAN_VERSION}"
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
        f"Dataset grains: {len(DATASET_GRAINS)}"
    )
    print(
        f"Dataset fields: {len(DATASET_FIELDS)}"
    )
    print(
        f"Eligibility rules: {len(ELIGIBILITY_RULES)}"
    )
    print(
        f"Exclusion codes: {len(EXCLUSION_CODES)}"
    )
    print(
        f"Partition fields: {len(PARTITION_FIELDS)}"
    )
    print(
        f"Ordering fields: {len(ORDERING_FIELDS)}"
    )
    print(
        f"Split boundaries: {len(SPLIT_BOUNDARIES)}"
    )
    print(
        f"Cardinality rules: {len(CARDINALITY_RULES)}"
    )
    print(
        f"Dataset statuses: {len(DATASET_STATUSES)}"
    )
    print(
        f"Implementation steps: {len(IMPLEMENTATION_STEPS)}"
    )
    print(
        "Dataset records materialized: 0"
    )
    print(
        "Dataset splits executed: 0"
    )
    print(
        "Prediction/outcome joins executed: 0"
    )
    print(
        "Predictive metrics calculated: 0"
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
