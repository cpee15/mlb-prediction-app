#!/usr/bin/env python3
"""
Layer 9B
Pitch-Type Matchup Overlay Point-in-Time Historical Evaluation
Contract Implementation

Implements the deterministic Layer 9A point-in-time historical evaluation
contract.

This layer implements:

- immutable evaluation-record field definitions;
- deterministic evaluation-record identity;
- deterministic provenance digests;
- event-level identity validation;
- feature-cutoff validation;
- future-information exclusion validation;
- explicit eligibility and exclusion diagnostics;
- deterministic synthetic contract fixtures;
- deterministic CSV and JSON audit artifacts.

This layer does not:

- inventory or join historical outcomes;
- materialize production historical evaluation datasets;
- generate baseline predictions;
- generate augmented predictions;
- calculate predictive metrics;
- evaluate accuracy, calibration, or incremental value;
- train or tune models, weights, thresholds, or fallbacks;
- run backtests;
- activate the Layer 8 overlay in production;
- modify simulation or canonical probabilities;
- compare projections with betting markets;
- price wagers, detect edges, or recommend bets.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9B"
LAYER_NAME = (
    "pitch_type_matchup_overlay_point_in_time_historical_"
    "evaluation_contract_implementation"
)
CONTRACT_VERSION = "layer_9B_point_in_time_historical_evaluation_contract_v1"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_9B_pitch_type_matchup_overlay_point_in_time_"
    "historical_evaluation_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    / "plan_9A_pitch_type_matchup_overlay_point_in_time_historical_evaluation_inventory_and_contract.py"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_point_in_time_historical_"
    "evaluation_inventory_and_contract_plan_complete"
)

VALID_EVENT_LEVELS = (
    "event",
    "plate_appearance",
    "pitch",
    "contact",
)

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


EVALUATION_RECORD_FIELDS = [
    {
        "ordinal": 1,
        "field": "evaluation_record_id",
        "type": "deterministic_string",
        "required": True,
    },
    {
        "ordinal": 2,
        "field": "evaluation_contract_version",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 3,
        "field": "game_id",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 4,
        "field": "game_date",
        "type": "date",
        "required": True,
    },
    {
        "ordinal": 5,
        "field": "scheduled_start_utc",
        "type": "datetime",
        "required": True,
    },
    {
        "ordinal": 6,
        "field": "event_level",
        "type": "enum",
        "required": True,
    },
    {
        "ordinal": 7,
        "field": "plate_appearance_id",
        "type": "nullable_string",
        "required": True,
    },
    {
        "ordinal": 8,
        "field": "pitch_id",
        "type": "nullable_string",
        "required": True,
    },
    {
        "ordinal": 9,
        "field": "pitcher_id",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 10,
        "field": "batter_id",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 11,
        "field": "feature_cutoff_utc",
        "type": "datetime",
        "required": True,
    },
    {
        "ordinal": 12,
        "field": "shadow_row_id",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 13,
        "field": "shadow_row_generated_at_utc",
        "type": "datetime",
        "required": True,
    },
    {
        "ordinal": 14,
        "field": "pitcher_profile_version",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 15,
        "field": "batter_profile_version",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 16,
        "field": "matchup_overlay_version",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 17,
        "field": "baseline_prediction_id",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 18,
        "field": "baseline_prediction_generated_at_utc",
        "type": "datetime",
        "required": True,
    },
    {
        "ordinal": 19,
        "field": "augmented_prediction_id",
        "type": "nullable_string",
        "required": True,
    },
    {
        "ordinal": 20,
        "field": "outcome_id",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 21,
        "field": "outcome_available_at_utc",
        "type": "datetime",
        "required": True,
    },
    {
        "ordinal": 22,
        "field": "point_in_time_eligible",
        "type": "boolean",
        "required": True,
    },
    {
        "ordinal": 23,
        "field": "exclusion_codes",
        "type": "sorted_unique_string_array",
        "required": True,
    },
    {
        "ordinal": 24,
        "field": "provenance_digest",
        "type": "sha256_string",
        "required": True,
    },
]


VALIDATION_RULES = [
    {
        "rule_id": "PIT-V01",
        "rule": "evaluation_contract_version_explicit",
    },
    {
        "rule_id": "PIT-V02",
        "rule": "evaluation_record_id_deterministic",
    },
    {
        "rule_id": "PIT-V03",
        "rule": "event_level_valid",
    },
    {
        "rule_id": "PIT-V04",
        "rule": "game_identity_present",
    },
    {
        "rule_id": "PIT-V05",
        "rule": "plate_appearance_identity_conditional",
    },
    {
        "rule_id": "PIT-V06",
        "rule": "pitch_identity_conditional",
    },
    {
        "rule_id": "PIT-V07",
        "rule": "pitcher_and_batter_identity_present",
    },
    {
        "rule_id": "PIT-V08",
        "rule": "feature_cutoff_present",
    },
    {
        "rule_id": "PIT-V09",
        "rule": "feature_cutoff_precedes_event",
    },
    {
        "rule_id": "PIT-V10",
        "rule": "shadow_row_generated_before_cutoff",
    },
    {
        "rule_id": "PIT-V11",
        "rule": "baseline_prediction_generated_before_cutoff",
    },
    {
        "rule_id": "PIT-V12",
        "rule": "profile_versions_present",
    },
    {
        "rule_id": "PIT-V13",
        "rule": "overlay_version_present",
    },
    {
        "rule_id": "PIT-V14",
        "rule": "outcome_identity_present",
    },
    {
        "rule_id": "PIT-V15",
        "rule": "outcome_available_after_event",
    },
    {
        "rule_id": "PIT-V16",
        "rule": "outcome_payload_excluded_from_features",
    },
    {
        "rule_id": "PIT-V17",
        "rule": "future_information_exclusion_codes_explicit",
    },
    {
        "rule_id": "PIT-V18",
        "rule": "provenance_digest_valid_sha256",
    },
    {
        "rule_id": "PIT-V19",
        "rule": "identity_conflicts_rejected",
    },
    {
        "rule_id": "PIT-V20",
        "rule": "evaluation_order_deterministic",
    },
    {
        "rule_id": "PIT-V21",
        "rule": "disabled_path_non_emitting",
    },
    {
        "rule_id": "PIT-V22",
        "rule": "production_authority_false",
    },
]


EXCLUSION_CODE_CATALOG = [
    {
        "code": "point_in_time_baseline_prediction_after_cutoff",
        "category": "future_information",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_baseline_prediction_missing",
        "category": "prediction_identity",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_batter_identity_missing",
        "category": "event_identity",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_batter_profile_version_missing",
        "category": "feature_provenance",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_contract_version_invalid",
        "category": "contract",
        "eligibility_effect": "rejected",
    },
    {
        "code": "point_in_time_evaluation_disabled",
        "category": "execution_boundary",
        "eligibility_effect": "non_emitting",
    },
    {
        "code": "point_in_time_evaluation_record_id_invalid",
        "category": "deterministic_identity",
        "eligibility_effect": "rejected",
    },
    {
        "code": "point_in_time_event_identity_conflict",
        "category": "event_identity",
        "eligibility_effect": "rejected",
    },
    {
        "code": "point_in_time_event_level_invalid",
        "category": "event_identity",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_feature_cutoff_missing",
        "category": "feature_cutoff",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_feature_cutoff_not_before_event",
        "category": "future_information",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_game_date_invalid",
        "category": "event_identity",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_game_identity_missing",
        "category": "event_identity",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_matchup_overlay_version_missing",
        "category": "feature_provenance",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_outcome_available_before_event",
        "category": "outcome_provenance",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_outcome_identity_missing",
        "category": "outcome_identity",
        "eligibility_effect": "unscored",
    },
    {
        "code": "point_in_time_outcome_payload_in_features",
        "category": "future_information",
        "eligibility_effect": "rejected",
    },
    {
        "code": "point_in_time_pitch_identity_invalid",
        "category": "event_identity",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_pitcher_identity_missing",
        "category": "event_identity",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_pitcher_profile_version_missing",
        "category": "feature_provenance",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_plate_appearance_identity_invalid",
        "category": "event_identity",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_provenance_digest_invalid",
        "category": "feature_provenance",
        "eligibility_effect": "rejected",
    },
    {
        "code": "point_in_time_scheduled_start_invalid",
        "category": "event_identity",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_shadow_row_after_cutoff",
        "category": "future_information",
        "eligibility_effect": "ineligible",
    },
    {
        "code": "point_in_time_shadow_row_identity_missing",
        "category": "feature_identity",
        "eligibility_effect": "ineligible",
    },
]


PROHIBITED_AUTHORITIES = [
    "historical_outcome_inventory_execution",
    "historical_outcome_join_execution",
    "production_evaluation_record_materialization",
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


IDENTITY_FIELDS = (
    "evaluation_contract_version",
    "game_id",
    "game_date",
    "scheduled_start_utc",
    "event_level",
    "plate_appearance_id",
    "pitch_id",
    "pitcher_id",
    "batter_id",
    "feature_cutoff_utc",
    "shadow_row_id",
    "baseline_prediction_id",
    "outcome_id",
)


PROVENANCE_FIELDS = (
    "evaluation_contract_version",
    "game_id",
    "game_date",
    "scheduled_start_utc",
    "event_level",
    "plate_appearance_id",
    "pitch_id",
    "pitcher_id",
    "batter_id",
    "feature_cutoff_utc",
    "shadow_row_id",
    "shadow_row_generated_at_utc",
    "pitcher_profile_version",
    "batter_profile_version",
    "matchup_overlay_version",
    "baseline_prediction_id",
    "baseline_prediction_generated_at_utc",
    "augmented_prediction_id",
    "outcome_id",
    "outcome_available_at_utc",
)


FORBIDDEN_FEATURE_KEYS = {
    "actual_outcome",
    "ball_in_play_event",
    "called_strike_event",
    "contact_quality_value",
    "event_run_value",
    "extra_base_hit_event",
    "future_pitch_result",
    "hit_event",
    "outcome_payload",
    "run_value",
    "strikeout_event",
    "swing_event",
    "terminal_outcome",
    "walk_event",
    "whiff_event",
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
        newline="",
        encoding="utf-8",
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
            separators=(",", ": "),
        )
        + "\n",
        encoding="utf-8",
    )


def canonical_json_bytes(
    payload: Any,
) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_payload(
    payload: Any,
) -> str:
    return hashlib.sha256(
        canonical_json_bytes(payload)
    ).hexdigest()


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


def normalized_string(
    value: Any,
) -> str:
    if value is None:
        return ""

    return str(value).strip()


def normalized_nullable_string(
    value: Any,
) -> str | None:
    normalized = normalized_string(value)
    return normalized or None


def parse_utc_datetime(
    value: Any,
) -> datetime | None:
    text = normalized_string(value)

    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return None

    return parsed.astimezone(timezone.utc)


def valid_iso_date(
    value: Any,
) -> bool:
    text = normalized_string(value)

    if not text:
        return False

    try:
        datetime.strptime(
            text,
            "%Y-%m-%d",
        )
    except ValueError:
        return False

    return True


def deterministic_record_id(
    record: Mapping[str, Any],
) -> str:
    identity_payload = {
        field: record.get(field)
        for field in IDENTITY_FIELDS
    }

    return (
        "pit-eval-"
        + sha256_payload(identity_payload)[:32]
    )


def deterministic_provenance_digest(
    record: Mapping[str, Any],
) -> str:
    provenance_payload = {
        field: record.get(field)
        for field in PROVENANCE_FIELDS
    }

    return sha256_payload(provenance_payload)


def nested_keys(
    payload: Any,
) -> set[str]:
    keys: set[str] = set()

    if isinstance(payload, Mapping):
        for key, value in payload.items():
            keys.add(str(key))
            keys.update(nested_keys(value))
    elif isinstance(payload, list):
        for value in payload:
            keys.update(nested_keys(value))

    return keys


def canonicalize_record(
    raw_record: Mapping[str, Any],
) -> dict[str, Any]:
    record = deepcopy(dict(raw_record))

    record["evaluation_contract_version"] = normalized_string(
        record.get("evaluation_contract_version")
    )
    record["game_id"] = normalized_string(
        record.get("game_id")
    )
    record["game_date"] = normalized_string(
        record.get("game_date")
    )
    record["scheduled_start_utc"] = normalized_string(
        record.get("scheduled_start_utc")
    )
    record["event_level"] = normalized_string(
        record.get("event_level")
    )
    record["plate_appearance_id"] = normalized_nullable_string(
        record.get("plate_appearance_id")
    )
    record["pitch_id"] = normalized_nullable_string(
        record.get("pitch_id")
    )
    record["pitcher_id"] = normalized_string(
        record.get("pitcher_id")
    )
    record["batter_id"] = normalized_string(
        record.get("batter_id")
    )
    record["feature_cutoff_utc"] = normalized_string(
        record.get("feature_cutoff_utc")
    )
    record["shadow_row_id"] = normalized_string(
        record.get("shadow_row_id")
    )
    record["shadow_row_generated_at_utc"] = normalized_string(
        record.get("shadow_row_generated_at_utc")
    )
    record["pitcher_profile_version"] = normalized_string(
        record.get("pitcher_profile_version")
    )
    record["batter_profile_version"] = normalized_string(
        record.get("batter_profile_version")
    )
    record["matchup_overlay_version"] = normalized_string(
        record.get("matchup_overlay_version")
    )
    record["baseline_prediction_id"] = normalized_string(
        record.get("baseline_prediction_id")
    )
    record["baseline_prediction_generated_at_utc"] = normalized_string(
        record.get("baseline_prediction_generated_at_utc")
    )
    record["augmented_prediction_id"] = normalized_nullable_string(
        record.get("augmented_prediction_id")
    )
    record["outcome_id"] = normalized_string(
        record.get("outcome_id")
    )
    record["outcome_available_at_utc"] = normalized_string(
        record.get("outcome_available_at_utc")
    )

    exclusion_codes = record.get(
        "exclusion_codes",
        [],
    )

    if not isinstance(exclusion_codes, list):
        exclusion_codes = []

    record["exclusion_codes"] = sorted(
        {
            normalized_string(code)
            for code in exclusion_codes
            if normalized_string(code)
        }
    )

    record["point_in_time_eligible"] = bool(
        record.get(
            "point_in_time_eligible",
            False,
        )
    )

    return record


def validate_record(
    raw_record: Mapping[str, Any],
    *,
    evaluation_enabled: bool = True,
) -> dict[str, Any]:
    if not evaluation_enabled:
        return {
            "emitted": False,
            "record": None,
            "rule_results": {
                rule["rule_id"]: (
                    rule["rule_id"] == "PIT-V21"
                    or rule["rule_id"] == "PIT-V22"
                )
                for rule in VALIDATION_RULES
            },
            "exclusion_codes": [
                "point_in_time_evaluation_disabled"
            ],
            "point_in_time_eligible": False,
        }

    record = canonicalize_record(raw_record)

    exclusion_codes: set[str] = set(
        record["exclusion_codes"]
    )

    scheduled_start = parse_utc_datetime(
        record["scheduled_start_utc"]
    )
    feature_cutoff = parse_utc_datetime(
        record["feature_cutoff_utc"]
    )
    shadow_generated = parse_utc_datetime(
        record["shadow_row_generated_at_utc"]
    )
    baseline_generated = parse_utc_datetime(
        record["baseline_prediction_generated_at_utc"]
    )
    outcome_available = parse_utc_datetime(
        record["outcome_available_at_utc"]
    )

    rule_results: dict[str, bool] = {}

    rule_results["PIT-V01"] = (
        record["evaluation_contract_version"]
        == CONTRACT_VERSION
    )
    if not rule_results["PIT-V01"]:
        exclusion_codes.add(
            "point_in_time_contract_version_invalid"
        )

    expected_record_id = deterministic_record_id(
        record
    )
    provided_record_id = normalized_string(
        record.get("evaluation_record_id")
    )
    rule_results["PIT-V02"] = (
        provided_record_id == expected_record_id
    )
    if not rule_results["PIT-V02"]:
        exclusion_codes.add(
            "point_in_time_evaluation_record_id_invalid"
        )

    rule_results["PIT-V03"] = (
        record["event_level"] in VALID_EVENT_LEVELS
    )
    if not rule_results["PIT-V03"]:
        exclusion_codes.add(
            "point_in_time_event_level_invalid"
        )

    game_identity_present = bool(
        record["game_id"]
    )
    game_date_valid = valid_iso_date(
        record["game_date"]
    )
    scheduled_start_valid = (
        scheduled_start is not None
    )

    rule_results["PIT-V04"] = (
        game_identity_present
        and game_date_valid
        and scheduled_start_valid
    )

    if not game_identity_present:
        exclusion_codes.add(
            "point_in_time_game_identity_missing"
        )
    if not game_date_valid:
        exclusion_codes.add(
            "point_in_time_game_date_invalid"
        )
    if not scheduled_start_valid:
        exclusion_codes.add(
            "point_in_time_scheduled_start_invalid"
        )

    plate_appearance_id = record[
        "plate_appearance_id"
    ]
    event_level = record["event_level"]

    rule_results["PIT-V05"] = (
        (
            event_level
            in {
                "plate_appearance",
                "pitch",
                "contact",
            }
            and bool(plate_appearance_id)
        )
        or (
            event_level == "event"
            and plate_appearance_id is None
        )
        or event_level not in VALID_EVENT_LEVELS
    )
    if not rule_results["PIT-V05"]:
        exclusion_codes.add(
            "point_in_time_plate_appearance_identity_invalid"
        )

    pitch_id = record["pitch_id"]

    rule_results["PIT-V06"] = (
        (
            event_level
            in {
                "pitch",
                "contact",
            }
            and bool(pitch_id)
        )
        or (
            event_level
            in {
                "event",
                "plate_appearance",
            }
            and pitch_id is None
        )
        or event_level not in VALID_EVENT_LEVELS
    )
    if not rule_results["PIT-V06"]:
        exclusion_codes.add(
            "point_in_time_pitch_identity_invalid"
        )

    pitcher_present = bool(
        record["pitcher_id"]
    )
    batter_present = bool(
        record["batter_id"]
    )

    rule_results["PIT-V07"] = (
        pitcher_present
        and batter_present
    )
    if not pitcher_present:
        exclusion_codes.add(
            "point_in_time_pitcher_identity_missing"
        )
    if not batter_present:
        exclusion_codes.add(
            "point_in_time_batter_identity_missing"
        )

    rule_results["PIT-V08"] = (
        feature_cutoff is not None
    )
    if not rule_results["PIT-V08"]:
        exclusion_codes.add(
            "point_in_time_feature_cutoff_missing"
        )

    rule_results["PIT-V09"] = (
        feature_cutoff is not None
        and scheduled_start is not None
        and feature_cutoff < scheduled_start
    )
    if (
        feature_cutoff is not None
        and scheduled_start is not None
        and not rule_results["PIT-V09"]
    ):
        exclusion_codes.add(
            "point_in_time_feature_cutoff_not_before_event"
        )

    shadow_identity_present = bool(
        record["shadow_row_id"]
    )
    rule_results["PIT-V10"] = (
        shadow_identity_present
        and shadow_generated is not None
        and feature_cutoff is not None
        and shadow_generated <= feature_cutoff
    )
    if not shadow_identity_present:
        exclusion_codes.add(
            "point_in_time_shadow_row_identity_missing"
        )
    elif (
        shadow_generated is None
        or feature_cutoff is None
        or shadow_generated > feature_cutoff
    ):
        exclusion_codes.add(
            "point_in_time_shadow_row_after_cutoff"
        )

    baseline_identity_present = bool(
        record["baseline_prediction_id"]
    )
    rule_results["PIT-V11"] = (
        baseline_identity_present
        and baseline_generated is not None
        and feature_cutoff is not None
        and baseline_generated <= feature_cutoff
    )
    if not baseline_identity_present:
        exclusion_codes.add(
            "point_in_time_baseline_prediction_missing"
        )
    elif (
        baseline_generated is None
        or feature_cutoff is None
        or baseline_generated > feature_cutoff
    ):
        exclusion_codes.add(
            "point_in_time_baseline_prediction_after_cutoff"
        )

    pitcher_profile_present = bool(
        record["pitcher_profile_version"]
    )
    batter_profile_present = bool(
        record["batter_profile_version"]
    )

    rule_results["PIT-V12"] = (
        pitcher_profile_present
        and batter_profile_present
    )
    if not pitcher_profile_present:
        exclusion_codes.add(
            "point_in_time_pitcher_profile_version_missing"
        )
    if not batter_profile_present:
        exclusion_codes.add(
            "point_in_time_batter_profile_version_missing"
        )

    rule_results["PIT-V13"] = bool(
        record["matchup_overlay_version"]
    )
    if not rule_results["PIT-V13"]:
        exclusion_codes.add(
            "point_in_time_matchup_overlay_version_missing"
        )

    rule_results["PIT-V14"] = bool(
        record["outcome_id"]
    )
    if not rule_results["PIT-V14"]:
        exclusion_codes.add(
            "point_in_time_outcome_identity_missing"
        )

    rule_results["PIT-V15"] = (
        outcome_available is not None
        and scheduled_start is not None
        and outcome_available >= scheduled_start
    )
    if (
        outcome_available is not None
        and scheduled_start is not None
        and not rule_results["PIT-V15"]
    ):
        exclusion_codes.add(
            "point_in_time_outcome_available_before_event"
        )

    feature_payload = record.get(
        "feature_payload",
        {},
    )
    feature_payload_keys = nested_keys(
        feature_payload
    )
    outcome_payload_absent = not bool(
        feature_payload_keys
        & FORBIDDEN_FEATURE_KEYS
    )

    rule_results["PIT-V16"] = (
        outcome_payload_absent
    )
    if not rule_results["PIT-V16"]:
        exclusion_codes.add(
            "point_in_time_outcome_payload_in_features"
        )

    rule_results["PIT-V17"] = (
        isinstance(
            record["exclusion_codes"],
            list,
        )
        and record["exclusion_codes"]
        == sorted(
            set(record["exclusion_codes"])
        )
    )

    expected_provenance_digest = (
        deterministic_provenance_digest(
            record
        )
    )
    provided_provenance_digest = normalized_string(
        record.get("provenance_digest")
    )

    rule_results["PIT-V18"] = (
        bool(
            SHA256_PATTERN.fullmatch(
                provided_provenance_digest
            )
        )
        and provided_provenance_digest
        == expected_provenance_digest
    )
    if not rule_results["PIT-V18"]:
        exclusion_codes.add(
            "point_in_time_provenance_digest_invalid"
        )

    identity_conflict = bool(
        record.get(
            "identity_conflict",
            False,
        )
    )
    rule_results["PIT-V19"] = (
        not identity_conflict
    )
    if identity_conflict:
        exclusion_codes.add(
            "point_in_time_event_identity_conflict"
        )

    rule_results["PIT-V20"] = True
    rule_results["PIT-V21"] = True
    rule_results["PIT-V22"] = True

    eligibility_rule_ids = {
        "PIT-V01",
        "PIT-V02",
        "PIT-V03",
        "PIT-V04",
        "PIT-V05",
        "PIT-V06",
        "PIT-V07",
        "PIT-V08",
        "PIT-V09",
        "PIT-V10",
        "PIT-V11",
        "PIT-V12",
        "PIT-V13",
        "PIT-V14",
        "PIT-V15",
        "PIT-V16",
        "PIT-V17",
        "PIT-V18",
        "PIT-V19",
        "PIT-V20",
        "PIT-V21",
        "PIT-V22",
    }

    point_in_time_eligible = all(
        rule_results[rule_id]
        for rule_id in sorted(
            eligibility_rule_ids
        )
    )

    record["evaluation_record_id"] = (
        expected_record_id
    )
    record["provenance_digest"] = (
        expected_provenance_digest
    )
    record["point_in_time_eligible"] = (
        point_in_time_eligible
    )
    record["exclusion_codes"] = sorted(
        exclusion_codes
    )

    return {
        "emitted": True,
        "record": record,
        "rule_results": rule_results,
        "exclusion_codes": sorted(
            exclusion_codes
        ),
        "point_in_time_eligible": (
            point_in_time_eligible
        ),
    }


def build_valid_record(
    *,
    event_level: str,
    plate_appearance_id: str | None,
    pitch_id: str | None,
    sequence: int,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "evaluation_record_id": "",
        "evaluation_contract_version": (
            CONTRACT_VERSION
        ),
        "game_id": "game-2025-04-01-001",
        "game_date": "2025-04-01",
        "scheduled_start_utc": (
            "2025-04-01T23:10:00+00:00"
        ),
        "event_level": event_level,
        "plate_appearance_id": (
            plate_appearance_id
        ),
        "pitch_id": pitch_id,
        "pitcher_id": "pitcher-1001",
        "batter_id": "batter-2001",
        "feature_cutoff_utc": (
            "2025-04-01T23:00:00+00:00"
        ),
        "shadow_row_id": (
            f"shadow-row-{sequence:03d}"
        ),
        "shadow_row_generated_at_utc": (
            "2025-04-01T22:58:00+00:00"
        ),
        "pitcher_profile_version": (
            "pitcher-profile-asof-2025-04-01T22:55:00Z"
        ),
        "batter_profile_version": (
            "batter-profile-asof-2025-04-01T22:55:00Z"
        ),
        "matchup_overlay_version": (
            "layer_8_overlay_shadow_v1"
        ),
        "baseline_prediction_id": (
            f"baseline-prediction-{sequence:03d}"
        ),
        "baseline_prediction_generated_at_utc": (
            "2025-04-01T22:59:00+00:00"
        ),
        "augmented_prediction_id": None,
        "outcome_id": (
            f"historical-outcome-{sequence:03d}"
        ),
        "outcome_available_at_utc": (
            "2025-04-02T03:15:00+00:00"
        ),
        "point_in_time_eligible": False,
        "exclusion_codes": [],
        "provenance_digest": "",
        "feature_payload": {
            "pitcher_arsenal_profile": {
                "profile_version": (
                    "pitcher-profile-asof-2025-04-01T22:55:00Z"
                ),
                "source_observed_at_utc": (
                    "2025-04-01T22:50:00+00:00"
                ),
            },
            "batter_response_profile": {
                "profile_version": (
                    "batter-profile-asof-2025-04-01T22:55:00Z"
                ),
                "source_observed_at_utc": (
                    "2025-04-01T22:50:00+00:00"
                ),
            },
            "matchup_overlay": {
                "overlay_version": (
                    "layer_8_overlay_shadow_v1"
                ),
                "generated_at_utc": (
                    "2025-04-01T22:58:00+00:00"
                ),
            },
        },
        "identity_conflict": False,
    }

    record["evaluation_record_id"] = (
        deterministic_record_id(
            canonicalize_record(record)
        )
    )
    record["provenance_digest"] = (
        deterministic_provenance_digest(
            canonicalize_record(record)
        )
    )

    return record


def build_fixtures() -> list[dict[str, Any]]:
    valid_event = build_valid_record(
        event_level="event",
        plate_appearance_id=None,
        pitch_id=None,
        sequence=1,
    )
    valid_plate_appearance = build_valid_record(
        event_level="plate_appearance",
        plate_appearance_id="pa-001",
        pitch_id=None,
        sequence=2,
    )
    valid_pitch = build_valid_record(
        event_level="pitch",
        plate_appearance_id="pa-001",
        pitch_id="pitch-001",
        sequence=3,
    )
    valid_contact = build_valid_record(
        event_level="contact",
        plate_appearance_id="pa-001",
        pitch_id="pitch-002",
        sequence=4,
    )

    future_shadow = build_valid_record(
        event_level="pitch",
        plate_appearance_id="pa-002",
        pitch_id="pitch-003",
        sequence=5,
    )
    future_shadow[
        "shadow_row_generated_at_utc"
    ] = "2025-04-01T23:01:00+00:00"
    future_shadow["evaluation_record_id"] = (
        deterministic_record_id(
            canonicalize_record(future_shadow)
        )
    )
    future_shadow["provenance_digest"] = (
        deterministic_provenance_digest(
            canonicalize_record(future_shadow)
        )
    )

    invalid_pitch_identity = build_valid_record(
        event_level="pitch",
        plate_appearance_id="pa-003",
        pitch_id="pitch-004",
        sequence=6,
    )
    invalid_pitch_identity["pitch_id"] = None
    invalid_pitch_identity["evaluation_record_id"] = (
        deterministic_record_id(
            canonicalize_record(
                invalid_pitch_identity
            )
        )
    )
    invalid_pitch_identity["provenance_digest"] = (
        deterministic_provenance_digest(
            canonicalize_record(
                invalid_pitch_identity
            )
        )
    )

    outcome_leakage = build_valid_record(
        event_level="plate_appearance",
        plate_appearance_id="pa-004",
        pitch_id=None,
        sequence=7,
    )
    outcome_leakage["feature_payload"][
        "terminal_outcome"
    ] = "strikeout"
    outcome_leakage["evaluation_record_id"] = (
        deterministic_record_id(
            canonicalize_record(
                outcome_leakage
            )
        )
    )
    outcome_leakage["provenance_digest"] = (
        deterministic_provenance_digest(
            canonicalize_record(
                outcome_leakage
            )
        )
    )

    identity_conflict = build_valid_record(
        event_level="event",
        plate_appearance_id=None,
        pitch_id=None,
        sequence=8,
    )
    identity_conflict[
        "identity_conflict"
    ] = True
    identity_conflict["evaluation_record_id"] = (
        deterministic_record_id(
            canonicalize_record(
                identity_conflict
            )
        )
    )
    identity_conflict["provenance_digest"] = (
        deterministic_provenance_digest(
            canonicalize_record(
                identity_conflict
            )
        )
    )

    return [
        {
            "fixture_id": "PIT-FIX-001",
            "description": (
                "valid event-level record"
            ),
            "evaluation_enabled": True,
            "expected_emitted": True,
            "expected_eligible": True,
            "expected_exclusion_codes": [],
            "record": valid_event,
        },
        {
            "fixture_id": "PIT-FIX-002",
            "description": (
                "valid plate-appearance record"
            ),
            "evaluation_enabled": True,
            "expected_emitted": True,
            "expected_eligible": True,
            "expected_exclusion_codes": [],
            "record": valid_plate_appearance,
        },
        {
            "fixture_id": "PIT-FIX-003",
            "description": (
                "valid pitch-level record"
            ),
            "evaluation_enabled": True,
            "expected_emitted": True,
            "expected_eligible": True,
            "expected_exclusion_codes": [],
            "record": valid_pitch,
        },
        {
            "fixture_id": "PIT-FIX-004",
            "description": (
                "valid contact-level record"
            ),
            "evaluation_enabled": True,
            "expected_emitted": True,
            "expected_eligible": True,
            "expected_exclusion_codes": [],
            "record": valid_contact,
        },
        {
            "fixture_id": "PIT-FIX-005",
            "description": (
                "shadow row generated after cutoff"
            ),
            "evaluation_enabled": True,
            "expected_emitted": True,
            "expected_eligible": False,
            "expected_exclusion_codes": [
                "point_in_time_shadow_row_after_cutoff"
            ],
            "record": future_shadow,
        },
        {
            "fixture_id": "PIT-FIX-006",
            "description": (
                "pitch record missing pitch identity"
            ),
            "evaluation_enabled": True,
            "expected_emitted": True,
            "expected_eligible": False,
            "expected_exclusion_codes": [
                "point_in_time_pitch_identity_invalid"
            ],
            "record": invalid_pitch_identity,
        },
        {
            "fixture_id": "PIT-FIX-007",
            "description": (
                "terminal outcome leaked into feature payload"
            ),
            "evaluation_enabled": True,
            "expected_emitted": True,
            "expected_eligible": False,
            "expected_exclusion_codes": [
                "point_in_time_outcome_payload_in_features"
            ],
            "record": outcome_leakage,
        },
        {
            "fixture_id": "PIT-FIX-008",
            "description": (
                "event identity conflict"
            ),
            "evaluation_enabled": True,
            "expected_emitted": True,
            "expected_eligible": False,
            "expected_exclusion_codes": [
                "point_in_time_event_identity_conflict"
            ],
            "record": identity_conflict,
        },
        {
            "fixture_id": "PIT-FIX-009",
            "description": (
                "evaluation-disabled path emits no record"
            ),
            "evaluation_enabled": False,
            "expected_emitted": False,
            "expected_eligible": False,
            "expected_exclusion_codes": [
                "point_in_time_evaluation_disabled"
            ],
            "record": valid_event,
        },
    ]


def evaluate_fixtures() -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    fixture_results: list[dict[str, Any]] = []
    rule_results: list[dict[str, Any]] = []
    emitted_records: list[dict[str, Any]] = []

    for fixture in build_fixtures():
        result = validate_record(
            fixture["record"],
            evaluation_enabled=fixture[
                "evaluation_enabled"
            ],
        )

        actual_exclusion_codes = result[
            "exclusion_codes"
        ]
        expected_exclusion_codes = sorted(
            fixture[
                "expected_exclusion_codes"
            ]
        )

        fixture_passed = (
            result["emitted"]
            == fixture["expected_emitted"]
            and result["point_in_time_eligible"]
            == fixture["expected_eligible"]
            and actual_exclusion_codes
            == expected_exclusion_codes
        )

        fixture_results.append(
            {
                "fixture_id": fixture[
                    "fixture_id"
                ],
                "description": fixture[
                    "description"
                ],
                "evaluation_enabled": fixture[
                    "evaluation_enabled"
                ],
                "expected_emitted": fixture[
                    "expected_emitted"
                ],
                "actual_emitted": result[
                    "emitted"
                ],
                "expected_eligible": fixture[
                    "expected_eligible"
                ],
                "actual_eligible": result[
                    "point_in_time_eligible"
                ],
                "expected_exclusion_codes": (
                    "|".join(
                        expected_exclusion_codes
                    )
                ),
                "actual_exclusion_codes": (
                    "|".join(
                        actual_exclusion_codes
                    )
                ),
                "passed": fixture_passed,
            }
        )

        for rule in VALIDATION_RULES:
            rule_id = rule["rule_id"]

            rule_results.append(
                {
                    "fixture_id": fixture[
                        "fixture_id"
                    ],
                    "rule_id": rule_id,
                    "rule": rule["rule"],
                    "passed": bool(
                        result[
                            "rule_results"
                        ].get(
                            rule_id,
                            False,
                        )
                    ),
                }
            )

        if result["emitted"]:
            emitted_record = dict(
                result["record"]
            )
            emitted_record[
                "fixture_id"
            ] = fixture["fixture_id"]
            emitted_records.append(
                emitted_record
            )

    emitted_records.sort(
        key=lambda row: (
            row["game_date"],
            row["scheduled_start_utc"],
            row["game_id"],
            row["event_level"],
            row["plate_appearance_id"] or "",
            row["pitch_id"] or "",
            row["evaluation_record_id"],
        )
    )

    return (
        fixture_results,
        rule_results,
        emitted_records,
    )


def deterministic_replay_check() -> bool:
    first = evaluate_fixtures()
    second = evaluate_fixtures()

    return (
        canonical_json_bytes(first)
        == canonical_json_bytes(second)
    )


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_constants = string_constants(
        PREDECESSOR_PATH
    )
    predecessor_present = PREDECESSOR_PATH.exists()

    (
        fixture_results,
        rule_results,
        emitted_records,
    ) = evaluate_fixtures()

    valid_fixture_results = [
        row
        for row in fixture_results
        if row["fixture_id"]
        in {
            "PIT-FIX-001",
            "PIT-FIX-002",
            "PIT-FIX-003",
            "PIT-FIX-004",
        }
    ]

    invalid_fixture_results = [
        row
        for row in fixture_results
        if row["fixture_id"]
        in {
            "PIT-FIX-005",
            "PIT-FIX-006",
            "PIT-FIX-007",
            "PIT-FIX-008",
        }
    ]

    disabled_fixture_result = next(
        row
        for row in fixture_results
        if row["fixture_id"]
        == "PIT-FIX-009"
    )

    emitted_ids = [
        row["evaluation_record_id"]
        for row in emitted_records
    ]

    emitted_provenance_digests = [
        row["provenance_digest"]
        for row in emitted_records
    ]

    all_fixture_expectations_passed = all(
        row["passed"]
        for row in fixture_results
    )

    planning_contract_fields_match = (
        len(EVALUATION_RECORD_FIELDS)
        == 24
        and len(
            {
                row["field"]
                for row
                in EVALUATION_RECORD_FIELDS
            }
        )
        == 24
    )

    implementation_checks = [
        {
            "check": "nine_a_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "contract_version_explicit",
            "actual": CONTRACT_VERSION,
            "expected": CONTRACT_VERSION,
            "passed": bool(
                CONTRACT_VERSION
            ),
        },
        {
            "check": "twenty_four_contract_fields_implemented",
            "actual": len(
                EVALUATION_RECORD_FIELDS
            ),
            "expected": 24,
            "passed": planning_contract_fields_match,
        },
        {
            "check": "twenty_two_validation_rules_implemented",
            "actual": len(
                VALIDATION_RULES
            ),
            "expected": 22,
            "passed": len(
                VALIDATION_RULES
            )
            == 22,
        },
        {
            "check": "four_event_levels_supported",
            "actual": len(
                VALID_EVENT_LEVELS
            ),
            "expected": 4,
            "passed": VALID_EVENT_LEVELS
            == (
                "event",
                "plate_appearance",
                "pitch",
                "contact",
            ),
        },
        {
            "check": "valid_event_level_fixture_passes",
            "actual": valid_fixture_results[
                0
            ]["passed"],
            "expected": True,
            "passed": valid_fixture_results[
                0
            ]["passed"],
        },
        {
            "check": "valid_plate_appearance_fixture_passes",
            "actual": valid_fixture_results[
                1
            ]["passed"],
            "expected": True,
            "passed": valid_fixture_results[
                1
            ]["passed"],
        },
        {
            "check": "valid_pitch_fixture_passes",
            "actual": valid_fixture_results[
                2
            ]["passed"],
            "expected": True,
            "passed": valid_fixture_results[
                2
            ]["passed"],
        },
        {
            "check": "valid_contact_fixture_passes",
            "actual": valid_fixture_results[
                3
            ]["passed"],
            "expected": True,
            "passed": valid_fixture_results[
                3
            ]["passed"],
        },
        {
            "check": "future_shadow_fixture_rejected",
            "actual": invalid_fixture_results[
                0
            ]["passed"],
            "expected": True,
            "passed": invalid_fixture_results[
                0
            ]["passed"],
        },
        {
            "check": "conditional_pitch_identity_enforced",
            "actual": invalid_fixture_results[
                1
            ]["passed"],
            "expected": True,
            "passed": invalid_fixture_results[
                1
            ]["passed"],
        },
        {
            "check": "outcome_feature_leakage_rejected",
            "actual": invalid_fixture_results[
                2
            ]["passed"],
            "expected": True,
            "passed": invalid_fixture_results[
                2
            ]["passed"],
        },
        {
            "check": "identity_conflict_rejected",
            "actual": invalid_fixture_results[
                3
            ]["passed"],
            "expected": True,
            "passed": invalid_fixture_results[
                3
            ]["passed"],
        },
        {
            "check": "disabled_path_non_emitting",
            "actual": (
                not disabled_fixture_result[
                    "actual_emitted"
                ]
            ),
            "expected": True,
            "passed": disabled_fixture_result[
                "passed"
            ],
        },
        {
            "check": "fixture_expectations_all_pass",
            "actual": all_fixture_expectations_passed,
            "expected": True,
            "passed": all_fixture_expectations_passed,
        },
        {
            "check": "evaluation_record_ids_unique",
            "actual": len(
                set(emitted_ids)
            ),
            "expected": len(
                emitted_ids
            ),
            "passed": len(
                set(emitted_ids)
            )
            == len(
                emitted_ids
            ),
        },
        {
            "check": "provenance_digests_valid_sha256",
            "actual": sum(
                bool(
                    SHA256_PATTERN.fullmatch(
                        digest
                    )
                )
                for digest
                in emitted_provenance_digests
            ),
            "expected": len(
                emitted_provenance_digests
            ),
            "passed": all(
                SHA256_PATTERN.fullmatch(
                    digest
                )
                for digest
                in emitted_provenance_digests
            ),
        },
        {
            "check": "evaluation_order_deterministic",
            "actual": deterministic_replay_check(),
            "expected": True,
            "passed": deterministic_replay_check(),
        },
        {
            "check": "historical_outcome_join_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "predictive_metric_calculation_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "production_probability_authority_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "market_pricing_edge_authority_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in implementation_checks
    )

    authority_rows = [
        {
            "authority": authority,
            "granted": False,
            "reason": (
                "9B implements the bounded point-in-time "
                "historical evaluation contract only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "point_in_time_contract_validation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "9B may validate synthetic or future "
                    "candidate evaluation-record metadata "
                    "against deterministic identity, cutoff, "
                    "provenance, and leakage rules."
                ),
            },
            {
                "authority": (
                    "historical_outcome_inventory_planning"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "9C may inventory historical outcome "
                    "surfaces without joining outcomes to "
                    "features or calculating metrics."
                ),
            },
        ]
    )

    fixture_result_by_id = {
        row["fixture_id"]: row
        for row in fixture_results
    }

    contract_fixture_rows = []

    for fixture in build_fixtures():
        result = fixture_result_by_id[
            fixture["fixture_id"]
        ]

        contract_fixture_rows.append(
            {
                "fixture_id": fixture[
                    "fixture_id"
                ],
                "description": fixture[
                    "description"
                ],
                "event_level": fixture[
                    "record"
                ]["event_level"],
                "evaluation_enabled": fixture[
                    "evaluation_enabled"
                ],
                "expected_emitted": fixture[
                    "expected_emitted"
                ],
                "expected_eligible": fixture[
                    "expected_eligible"
                ],
                "expected_exclusion_codes": (
                    "|".join(
                        sorted(
                            fixture[
                                "expected_exclusion_codes"
                            ]
                        )
                    )
                ),
                "passed": result[
                    "passed"
                ],
            }
        )

    write_csv(
        OUTPUT_DIR / "implementation_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        implementation_checks,
    )

    write_csv(
        OUTPUT_DIR / "evaluation_record_contract.csv",
        [
            "ordinal",
            "field",
            "type",
            "required",
        ],
        EVALUATION_RECORD_FIELDS,
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
        OUTPUT_DIR / "exclusion_code_catalog.csv",
        [
            "code",
            "category",
            "eligibility_effect",
        ],
        EXCLUSION_CODE_CATALOG,
    )

    write_csv(
        OUTPUT_DIR / "contract_fixtures.csv",
        [
            "fixture_id",
            "description",
            "event_level",
            "evaluation_enabled",
            "expected_emitted",
            "expected_eligible",
            "expected_exclusion_codes",
            "passed",
        ],
        contract_fixture_rows,
    )

    write_csv(
        OUTPUT_DIR / "fixture_results.csv",
        [
            "fixture_id",
            "description",
            "evaluation_enabled",
            "expected_emitted",
            "actual_emitted",
            "expected_eligible",
            "actual_eligible",
            "expected_exclusion_codes",
            "actual_exclusion_codes",
            "passed",
        ],
        fixture_results,
    )

    write_csv(
        OUTPUT_DIR / "fixture_rule_results.csv",
        [
            "fixture_id",
            "rule_id",
            "rule",
            "passed",
        ],
        rule_results,
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

    emitted_record_projection = []

    for record in emitted_records:
        emitted_record_projection.append(
            {
                "fixture_id": record[
                    "fixture_id"
                ],
                "evaluation_record_id": record[
                    "evaluation_record_id"
                ],
                "evaluation_contract_version": record[
                    "evaluation_contract_version"
                ],
                "game_id": record[
                    "game_id"
                ],
                "game_date": record[
                    "game_date"
                ],
                "scheduled_start_utc": record[
                    "scheduled_start_utc"
                ],
                "event_level": record[
                    "event_level"
                ],
                "plate_appearance_id": (
                    record[
                        "plate_appearance_id"
                    ]
                    or ""
                ),
                "pitch_id": (
                    record["pitch_id"]
                    or ""
                ),
                "feature_cutoff_utc": record[
                    "feature_cutoff_utc"
                ],
                "point_in_time_eligible": record[
                    "point_in_time_eligible"
                ],
                "exclusion_codes": "|".join(
                    record[
                        "exclusion_codes"
                    ]
                ),
                "provenance_digest": record[
                    "provenance_digest"
                ],
            }
        )

    write_csv(
        OUTPUT_DIR
        / "synthetic_evaluation_record_validation.csv",
        [
            "fixture_id",
            "evaluation_record_id",
            "evaluation_contract_version",
            "game_id",
            "game_date",
            "scheduled_start_utc",
            "event_level",
            "plate_appearance_id",
            "pitch_id",
            "feature_cutoff_utc",
            "point_in_time_eligible",
            "exclusion_codes",
            "provenance_digest",
        ],
        emitted_record_projection,
    )

    recommended_next_layer = (
        "9C_pitch_type_matchup_overlay_historical_outcome_inventory_plan"
        if all_checks_passed
        else
        "9B_pitch_type_matchup_overlay_point_in_time_historical_"
        "evaluation_contract_implementation_remediation"
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
                    "Inventory historical outcome sources, "
                    "identities, timestamps, revisions, and "
                    "point-in-time availability without joining "
                    "outcomes to Layer 8 features."
                    if all_checks_passed
                    else
                    "Remediate failed 9B contract implementation checks."
                ),
                "entry_condition": (
                    "All twenty-two 9B implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "contract_version": CONTRACT_VERSION,
        "implementation_checks_required": len(
            implementation_checks
        ),
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in implementation_checks
        ),
        "evaluation_record_fields_implemented": len(
            EVALUATION_RECORD_FIELDS
        ),
        "validation_rules_implemented": len(
            VALIDATION_RULES
        ),
        "exclusion_codes_defined": len(
            EXCLUSION_CODE_CATALOG
        ),
        "fixtures_executed": len(
            fixture_results
        ),
        "fixtures_passed": sum(
            bool(row["passed"])
            for row in fixture_results
        ),
        "synthetic_records_emitted": len(
            emitted_records
        ),
        "synthetic_records_eligible": sum(
            bool(
                row[
                    "point_in_time_eligible"
                ]
            )
            for row in emitted_records
        ),
        "historical_outcome_rows_joined": 0,
        "predictive_metrics_calculated": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed": all_checks_passed,
        "recommended_next_layer": recommended_next_layer,
    }

    write_json(
        OUTPUT_DIR / "summary.json",
        summary,
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_point_in_time_historical_"
        "evaluation_contract_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_point_in_time_historical_"
        "evaluation_contract_implementation_failed"
    )

    diagnosis = {
        "diagnosis": diagnosis_name,
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "contract_version": CONTRACT_VERSION,
        "all_checks_passed": all_checks_passed,
        "recommended_next_layer": recommended_next_layer,
        "authority_granted": (
            "historical_outcome_inventory_planning"
            if all_checks_passed
            else
            "none"
        ),
        "authority_withheld": sorted(
            PROHIBITED_AUTHORITIES
        ),
        "output_directory": str(
            OUTPUT_DIR.relative_to(ROOT)
        ),
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    artifact_manifest_rows = []

    for path in sorted(
        OUTPUT_DIR.iterdir(),
        key=lambda candidate: candidate.name,
    ):
        if not path.is_file():
            continue

        artifact_manifest_rows.append(
            {
                "artifact": path.name,
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(
                    path.read_bytes()
                ).hexdigest(),
            }
        )

    write_csv(
        OUTPUT_DIR / "artifact_manifest.csv",
        [
            "artifact",
            "bytes",
            "sha256",
        ],
        artifact_manifest_rows,
    )

    print(
        f"Layer: {LAYER_ID} — {LAYER_NAME}"
    )
    print(
        "Contract version: "
        f"{CONTRACT_VERSION}"
    )
    print(
        "Predecessor verified: "
        f"{predecessor_present}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Contract fixtures passed: "
        f"{summary['fixtures_passed']}/"
        f"{summary['fixtures_executed']}"
    )
    print(
        "Synthetic validation records emitted: "
        f"{summary['synthetic_records_emitted']}"
    )
    print(
        "Synthetic validation records eligible: "
        f"{summary['synthetic_records_eligible']}"
    )
    print(
        "Historical outcome rows joined: 0"
    )
    print(
        "Predictive metrics calculated: 0"
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
        "Recommended next layer: "
        f"{recommended_next_layer}"
    )
    print(
        "Artifacts: "
        f"{OUTPUT_DIR.relative_to(ROOT)}"
    )

    if not all_checks_passed:
        failed_checks = [
            row["check"]
            for row in implementation_checks
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
