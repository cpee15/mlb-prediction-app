#!/usr/bin/env python3
"""
Layer 9K
Pitch-Type Matchup Overlay Historical Outcome Feature Join Contract Plan

Plans the bounded point-in-time contract for joining historical outcome records
to historical feature artifacts.

Planning only.

This layer does not:

- execute feature/outcome joins;
- execute prediction/outcome joins;
- fetch external historical outcomes;
- materialize production datasets;
- calculate predictive or evaluation metrics;
- train or tune models;
- modify production probabilities, simulations, pricing, or betting behavior.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9K"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_"
    "feature_join_contract_plan"
)

JOIN_PLAN_VERSION = (
    "layer_9K_historical_outcome_feature_join_contract_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9K_pitch_type_matchup_overlay_"
    "historical_outcome_feature_join_contract_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9J_pitch_type_matchup_overlay_"
    "historical_outcome_fixture_replay_contract.py"
)

OUTCOME_CONTRACT_PATH = (
    ROOT
    / "scripts"
    / "audit_9D_pitch_type_matchup_overlay_"
    "historical_outcome_contract.py"
)

POINT_IN_TIME_CONTRACT_PATH = (
    ROOT
    / "scripts"
    / "audit_9B_pitch_type_matchup_overlay_"
    "point_in_time_historical_evaluation_contract.py"
)

CORPUS_DIR = (
    ROOT
    / "tests"
    / "fixtures"
    / "historical_outcomes"
    / "layer_9H"
)

MANIFEST_PATH = CORPUS_DIR / "manifest.json"
EXPECTED_OUTCOME_RECORDS_PATH = (
    CORPUS_DIR / "expected_outcome_records.jsonl"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_outcome_"
    "fixture_replay_contract_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_outcome_feature_join_contract_planning"
)

EXPECTED_OUTCOME_CONTRACT_VERSION = (
    "layer_9D_historical_outcome_contract_v1"
)

EXPECTED_POINT_IN_TIME_DIAGNOSIS = (
    "pitch_type_matchup_overlay_point_in_time_historical_"
    "evaluation_contract_implementation_complete"
)


JOIN_GRAINS = [
    {
        "grain_id": "HJOIN-G01",
        "event_level": "pitch",
        "feature_grain": "pitch",
        "outcome_grain": "pitch",
        "primary_identity": "pitch_id",
        "secondary_identity": (
            "game_id + plate_appearance_id + event_sequence"
        ),
        "cardinality": "one_feature_to_zero_or_one_outcome",
    },
    {
        "grain_id": "HJOIN-G02",
        "event_level": "contact",
        "feature_grain": "contact",
        "outcome_grain": "contact",
        "primary_identity": "pitch_id",
        "secondary_identity": (
            "game_id + plate_appearance_id + event_sequence"
        ),
        "cardinality": "one_feature_to_zero_or_one_outcome",
    },
    {
        "grain_id": "HJOIN-G03",
        "event_level": "plate_appearance",
        "feature_grain": "plate_appearance",
        "outcome_grain": "plate_appearance",
        "primary_identity": "plate_appearance_id",
        "secondary_identity": (
            "game_id + event_sequence"
        ),
        "cardinality": "one_feature_to_zero_or_one_outcome",
    },
    {
        "grain_id": "HJOIN-G04",
        "event_level": "event",
        "feature_grain": "game_event",
        "outcome_grain": "event",
        "primary_identity": (
            "game_id + event_sequence + target_id"
        ),
        "secondary_identity": (
            "provider + provider_event_id + target_id"
        ),
        "cardinality": "one_feature_to_zero_or_one_outcome",
    },
]


JOIN_KEY_FIELDS = [
    {
        "ordinal": 1,
        "field": "target_id",
        "required": True,
        "role": (
            "Prevents cross-target joins between different historical labels."
        ),
    },
    {
        "ordinal": 2,
        "field": "event_level",
        "required": True,
        "role": (
            "Prevents joining records across incompatible observational grains."
        ),
    },
    {
        "ordinal": 3,
        "field": "game_id",
        "required": True,
        "role": "Scopes all identities to a single game.",
    },
    {
        "ordinal": 4,
        "field": "plate_appearance_id",
        "required": "conditional",
        "role": (
            "Required for pitch, contact, and plate-appearance targets."
        ),
    },
    {
        "ordinal": 5,
        "field": "pitch_id",
        "required": "conditional",
        "role": "Required for pitch and contact targets.",
    },
    {
        "ordinal": 6,
        "field": "event_sequence",
        "required": True,
        "role": (
            "Provides deterministic within-game ordering and fallback identity."
        ),
    },
    {
        "ordinal": 7,
        "field": "historical_outcome_contract_version",
        "required": True,
        "role": (
            "Prevents silent joins across incompatible outcome contracts."
        ),
    },
]


POINT_IN_TIME_FIELDS = [
    {
        "field": "feature_as_of_utc",
        "source": "historical feature artifact",
        "required": True,
        "rule": (
            "Must represent the latest information available when the feature "
            "was generated."
        ),
    },
    {
        "field": "scheduled_start_utc",
        "source": "feature and outcome records",
        "required": True,
        "rule": (
            "Feature-side and outcome-side scheduled start timestamps must agree."
        ),
    },
    {
        "field": "event_occurred_at_utc",
        "source": "historical outcome record",
        "required": False,
        "rule": (
            "May be null only when the outcome contract permits a missing event time."
        ),
    },
    {
        "field": "source_observed_at_utc",
        "source": "historical outcome record",
        "required": True,
        "rule": (
            "Must not be used as a feature input or influence feature generation."
        ),
    },
    {
        "field": "source_published_at_utc",
        "source": "historical outcome record",
        "required": False,
        "rule": (
            "Must remain outcome-side provenance and cannot enter predictors."
        ),
    },
    {
        "field": "outcome_available_at_utc",
        "source": "historical outcome record",
        "required": True,
        "rule": (
            "Must be strictly later than the feature cutoff for eligible "
            "evaluation rows unless the governing target contract states otherwise."
        ),
    },
]


LEAKAGE_GUARDS = [
    {
        "guard_id": "HJOIN-L01",
        "guard": (
            "No outcome value, eligibility flag, exclusion code, provider "
            "revision, or outcome provenance field may appear in feature inputs."
        ),
    },
    {
        "guard_id": "HJOIN-L02",
        "guard": (
            "Feature generation must complete before outcome_available_at_utc."
        ),
    },
    {
        "guard_id": "HJOIN-L03",
        "guard": (
            "Outcome-side source_observed_at_utc and source_published_at_utc "
            "must not determine feature selection or feature values."
        ),
    },
    {
        "guard_id": "HJOIN-L04",
        "guard": (
            "The join must not use future provider revisions unavailable at the "
            "feature cutoff."
        ),
    },
    {
        "guard_id": "HJOIN-L05",
        "guard": (
            "The join must not select a feature row based on whether an outcome "
            "exists, is missing, or is eligible."
        ),
    },
    {
        "guard_id": "HJOIN-L06",
        "guard": (
            "Feature deduplication must occur independently of outcome values."
        ),
    },
    {
        "guard_id": "HJOIN-L07",
        "guard": (
            "Outcome records marked identity_conflict or revision_conflict must "
            "remain excluded according to the Layer 9D contract."
        ),
    },
    {
        "guard_id": "HJOIN-L08",
        "guard": (
            "Game-incomplete or non-final outcome records must not be promoted "
            "to eligible evaluation rows."
        ),
    },
    {
        "guard_id": "HJOIN-L09",
        "guard": (
            "A missing historical outcome must remain distinguishable from an "
            "unmatched join."
        ),
    },
    {
        "guard_id": "HJOIN-L10",
        "guard": (
            "All joins must be deterministic under stable input ordering."
        ),
    },
]


CARDINALITY_RULES = [
    {
        "rule_id": "HJOIN-C01",
        "rule": (
            "A feature row may join to at most one historical outcome record "
            "for a target_id."
        ),
    },
    {
        "rule_id": "HJOIN-C02",
        "rule": (
            "An eligible historical outcome record may join to at most one "
            "feature row for the declared feature contract version."
        ),
    },
    {
        "rule_id": "HJOIN-C03",
        "rule": (
            "Duplicate feature identities must fail before outcome attachment."
        ),
    },
    {
        "rule_id": "HJOIN-C04",
        "rule": (
            "Duplicate final historical outcome identities must fail rather "
            "than resolve by arbitrary row order."
        ),
    },
    {
        "rule_id": "HJOIN-C05",
        "rule": (
            "Multiple provider revisions may exist in source history, but only "
            "the contract-selected final revision may enter the join."
        ),
    },
    {
        "rule_id": "HJOIN-C06",
        "rule": (
            "One feature row may carry multiple targets only as separate "
            "target_id-specific joined rows."
        ),
    },
]


JOIN_STATUSES = [
    {
        "status": "matched_eligible",
        "meaning": (
            "Exactly one feature row and one eligible historical outcome match."
        ),
        "evaluation_eligible": True,
    },
    {
        "status": "matched_ineligible",
        "meaning": (
            "Exactly one outcome matches, but the outcome contract marks it "
            "ineligible."
        ),
        "evaluation_eligible": False,
    },
    {
        "status": "matched_missing_outcome",
        "meaning": (
            "An outcome record matches and explicitly represents a missing label."
        ),
        "evaluation_eligible": False,
    },
    {
        "status": "feature_without_outcome",
        "meaning": (
            "A valid feature row has no corresponding historical outcome record."
        ),
        "evaluation_eligible": False,
    },
    {
        "status": "outcome_without_feature",
        "meaning": (
            "A valid historical outcome record has no corresponding feature row."
        ),
        "evaluation_eligible": False,
    },
    {
        "status": "duplicate_feature_identity",
        "meaning": "More than one feature row shares the declared join identity.",
        "evaluation_eligible": False,
    },
    {
        "status": "duplicate_outcome_identity",
        "meaning": "More than one outcome row shares the declared final identity.",
        "evaluation_eligible": False,
    },
    {
        "status": "point_in_time_violation",
        "meaning": (
            "The feature cutoff does not precede outcome availability."
        ),
        "evaluation_eligible": False,
    },
    {
        "status": "contract_version_mismatch",
        "meaning": (
            "Feature or outcome contract versions are incompatible."
        ),
        "evaluation_eligible": False,
    },
    {
        "status": "identity_mismatch",
        "meaning": (
            "Required target, grain, game, plate appearance, pitch, or sequence "
            "identity is missing or inconsistent."
        ),
        "evaluation_eligible": False,
    },
]


FAILURE_CODES = [
    {
        "failure_code": "historical_outcome_feature_join_feature_contract_missing",
        "failure_class": "discovery",
    },
    {
        "failure_code": "historical_outcome_feature_join_outcome_contract_missing",
        "failure_class": "discovery",
    },
    {
        "failure_code": "historical_outcome_feature_join_contract_version_mismatch",
        "failure_class": "contract",
    },
    {
        "failure_code": "historical_outcome_feature_join_required_key_missing",
        "failure_class": "identity",
    },
    {
        "failure_code": "historical_outcome_feature_join_event_level_mismatch",
        "failure_class": "identity",
    },
    {
        "failure_code": "historical_outcome_feature_join_target_mismatch",
        "failure_class": "identity",
    },
    {
        "failure_code": "historical_outcome_feature_join_game_mismatch",
        "failure_class": "identity",
    },
    {
        "failure_code": "historical_outcome_feature_join_duplicate_feature_identity",
        "failure_class": "cardinality",
    },
    {
        "failure_code": "historical_outcome_feature_join_duplicate_outcome_identity",
        "failure_class": "cardinality",
    },
    {
        "failure_code": "historical_outcome_feature_join_many_to_many_detected",
        "failure_class": "cardinality",
    },
    {
        "failure_code": "historical_outcome_feature_join_point_in_time_violation",
        "failure_class": "leakage",
    },
    {
        "failure_code": "historical_outcome_feature_join_outcome_field_in_features",
        "failure_class": "leakage",
    },
    {
        "failure_code": "historical_outcome_feature_join_future_revision_selected",
        "failure_class": "leakage",
    },
    {
        "failure_code": "historical_outcome_feature_join_ineligible_outcome_promoted",
        "failure_class": "eligibility",
    },
    {
        "failure_code": "historical_outcome_feature_join_missing_outcome_conflated",
        "failure_class": "semantics",
    },
    {
        "failure_code": "historical_outcome_feature_join_not_deterministic",
        "failure_class": "determinism",
    },
]


OUTPUT_FIELDS = [
    {
        "ordinal": 1,
        "field": "join_contract_version",
        "source": "join contract",
    },
    {
        "ordinal": 2,
        "field": "feature_contract_version",
        "source": "feature artifact",
    },
    {
        "ordinal": 3,
        "field": "historical_outcome_contract_version",
        "source": "outcome record",
    },
    {
        "ordinal": 4,
        "field": "target_id",
        "source": "join identity",
    },
    {
        "ordinal": 5,
        "field": "event_level",
        "source": "join identity",
    },
    {
        "ordinal": 6,
        "field": "game_id",
        "source": "join identity",
    },
    {
        "ordinal": 7,
        "field": "plate_appearance_id",
        "source": "join identity",
    },
    {
        "ordinal": 8,
        "field": "pitch_id",
        "source": "join identity",
    },
    {
        "ordinal": 9,
        "field": "event_sequence",
        "source": "join identity",
    },
    {
        "ordinal": 10,
        "field": "feature_as_of_utc",
        "source": "feature artifact",
    },
    {
        "ordinal": 11,
        "field": "outcome_available_at_utc",
        "source": "outcome record",
    },
    {
        "ordinal": 12,
        "field": "historical_outcome_id",
        "source": "outcome record",
    },
    {
        "ordinal": 13,
        "field": "outcome_value",
        "source": "outcome record",
    },
    {
        "ordinal": 14,
        "field": "outcome_missing",
        "source": "outcome record",
    },
    {
        "ordinal": 15,
        "field": "historical_outcome_eligible",
        "source": "outcome record",
    },
    {
        "ordinal": 16,
        "field": "exclusion_codes",
        "source": "outcome record",
    },
    {
        "ordinal": 17,
        "field": "join_status",
        "source": "join contract",
    },
    {
        "ordinal": 18,
        "field": "evaluation_eligible",
        "source": "join contract",
    },
    {
        "ordinal": 19,
        "field": "join_identity_digest",
        "source": "join contract",
    },
    {
        "ordinal": 20,
        "field": "joined_record_digest",
        "source": "join contract",
    },
]


DIAGNOSTIC_ARTIFACTS = [
    {
        "ordinal": 1,
        "artifact": "join_contract_summary.json",
    },
    {
        "ordinal": 2,
        "artifact": "join_grains.csv",
    },
    {
        "ordinal": 3,
        "artifact": "join_key_fields.csv",
    },
    {
        "ordinal": 4,
        "artifact": "point_in_time_fields.csv",
    },
    {
        "ordinal": 5,
        "artifact": "leakage_guards.csv",
    },
    {
        "ordinal": 6,
        "artifact": "cardinality_rules.csv",
    },
    {
        "ordinal": 7,
        "artifact": "join_statuses.csv",
    },
    {
        "ordinal": 8,
        "artifact": "failure_codes.csv",
    },
    {
        "ordinal": 9,
        "artifact": "joined_output_fields.csv",
    },
    {
        "ordinal": 10,
        "artifact": "planning_checks.csv",
    },
    {
        "ordinal": 11,
        "artifact": "authority_boundaries.csv",
    },
    {
        "ordinal": 12,
        "artifact": "diagnosis.json",
    },
]


IMPLEMENTATION_STEPS = [
    {
        "step": 1,
        "action": (
            "Discover the historical feature artifact contract and immutable "
            "Layer 9D historical outcome contract."
        ),
    },
    {
        "step": 2,
        "action": (
            "Validate compatible feature, outcome, and join contract versions."
        ),
    },
    {
        "step": 3,
        "action": (
            "Normalize target_id, event_level, game, plate appearance, pitch, "
            "and sequence identities independently on both sides."
        ),
    },
    {
        "step": 4,
        "action": (
            "Validate feature-side and outcome-side uniqueness before joining."
        ),
    },
    {
        "step": 5,
        "action": (
            "Select only contract-valid final historical outcome revisions."
        ),
    },
    {
        "step": 6,
        "action": (
            "Apply point-in-time guards without inspecting outcome values."
        ),
    },
    {
        "step": 7,
        "action": (
            "Execute a target-specific left join from features to outcomes."
        ),
    },
    {
        "step": 8,
        "action": (
            "Run a reciprocal outcome-to-feature coverage audit."
        ),
    },
    {
        "step": 9,
        "action": (
            "Assign stable join statuses without conflating unmatched and "
            "explicitly missing outcomes."
        ),
    },
    {
        "step": 10,
        "action": (
            "Calculate deterministic identity and joined-record digests."
        ),
    },
    {
        "step": 11,
        "action": (
            "Replay the join under reordered inputs and require equivalent output."
        ),
    },
    {
        "step": 12,
        "action": (
            "Write temporary diagnostics and grant only bounded join-contract "
            "implementation authority."
        ),
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
    "edge_detection",
    "feature_outcome_join_execution",
    "historical_outcome_collection_execution",
    "historical_outcome_fetch_execution",
    "historical_outcome_prediction_join_execution",
    "incremental_value_evaluation",
    "market_comparison",
    "model_training",
    "parameter_tuning",
    "predictive_metric_calculation",
    "pricing",
    "production_historical_outcome_materialization",
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


def read_json(path: Path) -> Any:
    return json.loads(
        path.read_text(
            encoding="utf-8",
        )
    )


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    with path.open(
        "r",
        encoding="utf-8",
    ) as handle:
        for line in handle:
            stripped = line.strip()

            if stripped:
                rows.append(
                    json.loads(
                        stripped
                    )
                )

    return rows


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
            fieldnames=list(
                fieldnames
            ),
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_constants = string_constants(
        PREDECESSOR_PATH
    )

    outcome_constants = string_constants(
        OUTCOME_CONTRACT_PATH
    )

    point_in_time_constants = string_constants(
        POINT_IN_TIME_CONTRACT_PATH
    )

    predecessor_verified = (
        PREDECESSOR_PATH.exists()
        and EXPECTED_PREDECESSOR_DIAGNOSIS
        in predecessor_constants
        and EXPECTED_PREDECESSOR_AUTHORITY
        in predecessor_constants
    )

    outcome_contract_verified = (
        OUTCOME_CONTRACT_PATH.exists()
        and EXPECTED_OUTCOME_CONTRACT_VERSION
        in outcome_constants
    )

    point_in_time_contract_verified = (
        POINT_IN_TIME_CONTRACT_PATH.exists()
        and EXPECTED_POINT_IN_TIME_DIAGNOSIS
        in point_in_time_constants
    )

    manifest: dict[str, Any] = {}
    outcome_rows: list[
        dict[str, Any]
    ] = []

    if (
        MANIFEST_PATH.exists()
        and EXPECTED_OUTCOME_RECORDS_PATH.exists()
    ):
        manifest = read_json(
            MANIFEST_PATH
        )
        outcome_rows = read_jsonl(
            EXPECTED_OUTCOME_RECORDS_PATH
        )

    outcome_target_ids = sorted(
        {
            str(
                row.get(
                    "outcome_record",
                    {},
                ).get(
                    "target_id"
                )
            )
            for row in outcome_rows
            if isinstance(
                row.get(
                    "outcome_record"
                ),
                dict,
            )
        }
    )

    outcome_event_levels = sorted(
        {
            str(
                row.get(
                    "outcome_record",
                    {},
                ).get(
                    "event_level"
                )
            )
            for row in outcome_rows
            if isinstance(
                row.get(
                    "outcome_record"
                ),
                dict,
            )
        }
    )

    checks = [
        {
            "check": "nine_j_predecessor_verified",
            "actual": predecessor_verified,
            "expected": True,
            "passed": predecessor_verified,
        },
        {
            "check": "historical_outcome_contract_verified",
            "actual": outcome_contract_verified,
            "expected": True,
            "passed": outcome_contract_verified,
        },
        {
            "check": "point_in_time_contract_verified",
            "actual": point_in_time_contract_verified,
            "expected": True,
            "passed": point_in_time_contract_verified,
        },
        {
            "check": "fixture_manifest_present",
            "actual": MANIFEST_PATH.exists(),
            "expected": True,
            "passed": MANIFEST_PATH.exists(),
        },
        {
            "check": "expected_outcome_records_present",
            "actual": (
                EXPECTED_OUTCOME_RECORDS_PATH.exists()
            ),
            "expected": True,
            "passed": (
                EXPECTED_OUTCOME_RECORDS_PATH.exists()
            ),
        },
        {
            "check": "twenty_nine_outcome_records_discovered",
            "actual": len(
                outcome_rows
            ),
            "expected": 29,
            "passed": len(
                outcome_rows
            )
            == 29,
        },
        {
            "check": "all_ten_targets_discovered",
            "actual": outcome_target_ids,
            "expected": [
                f"HOUT-O{number:02d}"
                for number in range(
                    1,
                    11,
                )
            ],
            "passed": outcome_target_ids
            == [
                f"HOUT-O{number:02d}"
                for number in range(
                    1,
                    11,
                )
            ],
        },
        {
            "check": "four_event_levels_discovered",
            "actual": outcome_event_levels,
            "expected": [
                "contact",
                "event",
                "pitch",
                "plate_appearance",
            ],
            "passed": outcome_event_levels
            == [
                "contact",
                "event",
                "pitch",
                "plate_appearance",
            ],
        },
        {
            "check": "four_join_grains_defined",
            "actual": len(
                JOIN_GRAINS
            ),
            "expected": 4,
            "passed": len(
                JOIN_GRAINS
            )
            == 4,
        },
        {
            "check": "seven_join_key_fields_defined",
            "actual": len(
                JOIN_KEY_FIELDS
            ),
            "expected": 7,
            "passed": len(
                JOIN_KEY_FIELDS
            )
            == 7,
        },
        {
            "check": "six_point_in_time_fields_defined",
            "actual": len(
                POINT_IN_TIME_FIELDS
            ),
            "expected": 6,
            "passed": len(
                POINT_IN_TIME_FIELDS
            )
            == 6,
        },
        {
            "check": "ten_leakage_guards_defined",
            "actual": len(
                LEAKAGE_GUARDS
            ),
            "expected": 10,
            "passed": len(
                LEAKAGE_GUARDS
            )
            == 10,
        },
        {
            "check": "six_cardinality_rules_defined",
            "actual": len(
                CARDINALITY_RULES
            ),
            "expected": 6,
            "passed": len(
                CARDINALITY_RULES
            )
            == 6,
        },
        {
            "check": "ten_join_statuses_defined",
            "actual": len(
                JOIN_STATUSES
            ),
            "expected": 10,
            "passed": len(
                JOIN_STATUSES
            )
            == 10,
        },
        {
            "check": "sixteen_failure_codes_defined",
            "actual": len(
                FAILURE_CODES
            ),
            "expected": 16,
            "passed": len(
                FAILURE_CODES
            )
            == 16,
        },
        {
            "check": "twenty_output_fields_defined",
            "actual": len(
                OUTPUT_FIELDS
            ),
            "expected": 20,
            "passed": len(
                OUTPUT_FIELDS
            )
            == 20,
        },
        {
            "check": "twelve_diagnostic_artifacts_defined",
            "actual": len(
                DIAGNOSTIC_ARTIFACTS
            ),
            "expected": 12,
            "passed": len(
                DIAGNOSTIC_ARTIFACTS
            )
            == 12,
        },
        {
            "check": "twelve_implementation_steps_defined",
            "actual": len(
                IMPLEMENTATION_STEPS
            ),
            "expected": 12,
            "passed": len(
                IMPLEMENTATION_STEPS
            )
            == 12,
        },
        {
            "check": "target_id_is_required_join_key",
            "actual": any(
                row["field"]
                == "target_id"
                and row["required"]
                is True
                for row in JOIN_KEY_FIELDS
            ),
            "expected": True,
            "passed": any(
                row["field"]
                == "target_id"
                and row["required"]
                is True
                for row in JOIN_KEY_FIELDS
            ),
        },
        {
            "check": "outcome_availability_guard_defined",
            "actual": any(
                row["field"]
                == "outcome_available_at_utc"
                for row in POINT_IN_TIME_FIELDS
            ),
            "expected": True,
            "passed": any(
                row["field"]
                == "outcome_available_at_utc"
                for row in POINT_IN_TIME_FIELDS
            ),
        },
        {
            "check": "missing_outcome_distinction_defined",
            "actual": any(
                row["status"]
                == "matched_missing_outcome"
                for row in JOIN_STATUSES
            )
            and any(
                row["status"]
                == "feature_without_outcome"
                for row in JOIN_STATUSES
            ),
            "expected": True,
            "passed": any(
                row["status"]
                == "matched_missing_outcome"
                for row in JOIN_STATUSES
            )
            and any(
                row["status"]
                == "feature_without_outcome"
                for row in JOIN_STATUSES
            ),
        },
        {
            "check": "feature_outcome_join_not_executed",
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
        bool(
            row["passed"]
        )
        for row in checks
    )

    plan_digest = sha256_payload(
        {
            "join_plan_version": (
                JOIN_PLAN_VERSION
            ),
            "join_grains": JOIN_GRAINS,
            "join_key_fields": (
                JOIN_KEY_FIELDS
            ),
            "point_in_time_fields": (
                POINT_IN_TIME_FIELDS
            ),
            "leakage_guards": (
                LEAKAGE_GUARDS
            ),
            "cardinality_rules": (
                CARDINALITY_RULES
            ),
            "join_statuses": (
                JOIN_STATUSES
            ),
            "failure_codes": (
                FAILURE_CODES
            ),
            "output_fields": (
                OUTPUT_FIELDS
            ),
            "implementation_steps": (
                IMPLEMENTATION_STEPS
            ),
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_"
        "feature_join_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_"
        "feature_join_contract_plan_failed"
    )

    next_layer = (
        "9L_pitch_type_matchup_overlay_historical_outcome_"
        "feature_join_contract_implementation"
        if all_checks_passed
        else
        "9K_pitch_type_matchup_overlay_historical_outcome_"
        "feature_join_contract_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "join_grains.csv",
        [
            "grain_id",
            "event_level",
            "feature_grain",
            "outcome_grain",
            "primary_identity",
            "secondary_identity",
            "cardinality",
        ],
        JOIN_GRAINS,
    )

    write_csv(
        OUTPUT_DIR / "join_key_fields.csv",
        [
            "ordinal",
            "field",
            "required",
            "role",
        ],
        JOIN_KEY_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "point_in_time_fields.csv",
        [
            "field",
            "source",
            "required",
            "rule",
        ],
        POINT_IN_TIME_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "leakage_guards.csv",
        [
            "guard_id",
            "guard",
        ],
        LEAKAGE_GUARDS,
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
        OUTPUT_DIR / "join_statuses.csv",
        [
            "status",
            "meaning",
            "evaluation_eligible",
        ],
        JOIN_STATUSES,
    )

    write_csv(
        OUTPUT_DIR / "failure_codes.csv",
        [
            "failure_code",
            "failure_class",
        ],
        FAILURE_CODES,
    )

    write_csv(
        OUTPUT_DIR / "joined_output_fields.csv",
        [
            "ordinal",
            "field",
            "source",
        ],
        OUTPUT_FIELDS,
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
                    "Layer 9K plans a bounded "
                    "historical feature/outcome join only."
                ),
            }
            for authority in (
                PROHIBITED_AUTHORITIES
            )
        ]
        + [
            {
                "authority": (
                    "historical_outcome_feature_"
                    "join_contract_implementation"
                ),
                "granted": (
                    all_checks_passed
                ),
                "reason": (
                    "Layer 9L may implement a local "
                    "diagnostic join contract within "
                    "the planned identity, cardinality, "
                    "and point-in-time boundaries."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "join_plan_version": (
            JOIN_PLAN_VERSION
        ),
        "predecessor_verified": (
            predecessor_verified
        ),
        "outcome_contract_verified": (
            outcome_contract_verified
        ),
        "point_in_time_contract_verified": (
            point_in_time_contract_verified
        ),
        "outcome_records_discovered": len(
            outcome_rows
        ),
        "targets_discovered": len(
            outcome_target_ids
        ),
        "event_levels_discovered": len(
            outcome_event_levels
        ),
        "join_grains": len(
            JOIN_GRAINS
        ),
        "join_key_fields": len(
            JOIN_KEY_FIELDS
        ),
        "point_in_time_fields": len(
            POINT_IN_TIME_FIELDS
        ),
        "leakage_guards": len(
            LEAKAGE_GUARDS
        ),
        "cardinality_rules": len(
            CARDINALITY_RULES
        ),
        "join_statuses": len(
            JOIN_STATUSES
        ),
        "failure_codes": len(
            FAILURE_CODES
        ),
        "output_fields": len(
            OUTPUT_FIELDS
        ),
        "implementation_steps": len(
            IMPLEMENTATION_STEPS
        ),
        "planning_checks_passed": sum(
            bool(
                row["passed"]
            )
            for row in checks
        ),
        "planning_checks_required": len(
            checks
        ),
        "plan_digest": plan_digest,
        "external_records_fetched": 0,
        "feature_outcome_joins_executed": 0,
        "prediction_joins_executed": 0,
        "predictive_metrics_calculated": 0,
        "production_records_materialized": 0,
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
        / "join_contract_summary.json",
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
            "historical_outcome_feature_join_contract_implementation"
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
            OUTPUT_DIR.relative_to(
                ROOT
            )
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
        "Join plan version: "
        f"{JOIN_PLAN_VERSION}"
    )
    print(
        "Predecessor verified: "
        f"{predecessor_verified}"
    )
    print(
        "Outcome contract verified: "
        f"{outcome_contract_verified}"
    )
    print(
        "Point-in-time contract verified: "
        f"{point_in_time_contract_verified}"
    )
    print(
        "Planning checks passed: "
        f"{summary['planning_checks_passed']}/"
        f"{summary['planning_checks_required']}"
    )
    print(
        "Outcome records discovered: "
        f"{summary['outcome_records_discovered']}"
    )
    print(
        "Targets discovered: "
        f"{summary['targets_discovered']}"
    )
    print(
        "Event levels discovered: "
        f"{summary['event_levels_discovered']}"
    )
    print(
        "Join grains: "
        f"{summary['join_grains']}"
    )
    print(
        "Join key fields: "
        f"{summary['join_key_fields']}"
    )
    print(
        "Point-in-time fields: "
        f"{summary['point_in_time_fields']}"
    )
    print(
        "Leakage guards: "
        f"{summary['leakage_guards']}"
    )
    print(
        "Cardinality rules: "
        f"{summary['cardinality_rules']}"
    )
    print(
        "Join statuses: "
        f"{summary['join_statuses']}"
    )
    print(
        "Failure codes: "
        f"{summary['failure_codes']}"
    )
    print(
        "Output fields: "
        f"{summary['output_fields']}"
    )
    print(
        "Implementation steps: "
        f"{summary['implementation_steps']}"
    )
    print(
        "External historical outcome records fetched: 0"
    )
    print(
        "Feature/outcome joins executed: 0"
    )
    print(
        "Prediction joins executed: 0"
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
            str(
                row["check"]
            )
            for row in checks
            if not row["passed"]
        ]

        print(
            "FAILED CHECKS: "
            + ", ".join(
                failed_checks
            )
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
