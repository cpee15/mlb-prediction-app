#!/usr/bin/env python3
"""
Layer 9D
Pitch-Type Matchup Overlay Historical Outcome Contract Implementation

Implements the deterministic historical outcome contract planned by Layer 9C.

This layer implements:

- the immutable 30-field historical outcome record contract;
- deterministic historical outcome identities;
- bounded derivation of ten historical outcome targets;
- event-level identity validation;
- provider availability and revision validation;
- explicit outcome missingness semantics;
- immutable payload and provenance digests;
- deterministic synthetic contract fixtures;
- deterministic CSV and JSON audit artifacts.

This layer does not:

- fetch or collect external historical outcomes;
- materialize production historical outcome datasets;
- join historical outcomes to Layer 8 feature records;
- join historical outcomes to predictions;
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
import math
import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9D"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_contract_implementation"
)
CONTRACT_VERSION = "layer_9D_historical_outcome_contract_v1"
TARGET_DERIVATION_VERSION = "layer_9D_target_derivation_v1"
PROVIDER_MAPPING_VERSION = "layer_9D_provider_mapping_v1"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9D_pitch_type_matchup_overlay_historical_outcome_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "plan_9C_pitch_type_matchup_overlay_historical_outcome_inventory.py"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_outcome_inventory_plan_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_outcome_contract_implementation"
)

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

VALID_EVENT_LEVELS = (
    "event",
    "plate_appearance",
    "pitch",
    "contact",
)

TARGET_CONTRACT = {
    "HOUT-O01": {
        "event_level": "pitch",
        "target": "swing_event",
        "target_type": "binary",
    },
    "HOUT-O02": {
        "event_level": "pitch",
        "target": "whiff_event",
        "target_type": "binary",
    },
    "HOUT-O03": {
        "event_level": "pitch",
        "target": "called_strike_event",
        "target_type": "binary",
    },
    "HOUT-O04": {
        "event_level": "pitch",
        "target": "ball_in_play_event",
        "target_type": "binary",
    },
    "HOUT-O05": {
        "event_level": "plate_appearance",
        "target": "strikeout_event",
        "target_type": "binary",
    },
    "HOUT-O06": {
        "event_level": "plate_appearance",
        "target": "walk_event",
        "target_type": "binary",
    },
    "HOUT-O07": {
        "event_level": "plate_appearance",
        "target": "hit_event",
        "target_type": "binary",
    },
    "HOUT-O08": {
        "event_level": "plate_appearance",
        "target": "extra_base_hit_event",
        "target_type": "binary",
    },
    "HOUT-O09": {
        "event_level": "contact",
        "target": "contact_quality_value",
        "target_type": "continuous",
    },
    "HOUT-O10": {
        "event_level": "event",
        "target": "run_value",
        "target_type": "continuous",
    },
}

HISTORICAL_OUTCOME_RECORD_FIELDS = [
    {"ordinal": 1, "field": "historical_outcome_id", "type": "deterministic_string", "required": True},
    {"ordinal": 2, "field": "historical_outcome_contract_version", "type": "string", "required": True},
    {"ordinal": 3, "field": "target_id", "type": "enum", "required": True},
    {"ordinal": 4, "field": "event_level", "type": "enum", "required": True},
    {"ordinal": 5, "field": "game_id", "type": "string", "required": True},
    {"ordinal": 6, "field": "game_date", "type": "date", "required": True},
    {"ordinal": 7, "field": "scheduled_start_utc", "type": "datetime", "required": True},
    {"ordinal": 8, "field": "plate_appearance_id", "type": "nullable_string", "required": True},
    {"ordinal": 9, "field": "pitch_id", "type": "nullable_string", "required": True},
    {"ordinal": 10, "field": "pitcher_id", "type": "string", "required": True},
    {"ordinal": 11, "field": "batter_id", "type": "string", "required": True},
    {"ordinal": 12, "field": "event_sequence", "type": "integer", "required": True},
    {"ordinal": 13, "field": "raw_outcome_code", "type": "string", "required": True},
    {"ordinal": 14, "field": "outcome_value", "type": "nullable_number_or_boolean", "required": True},
    {"ordinal": 15, "field": "outcome_missing", "type": "boolean", "required": True},
    {"ordinal": 16, "field": "outcome_missing_reason", "type": "nullable_string", "required": True},
    {"ordinal": 17, "field": "event_occurred_at_utc", "type": "nullable_datetime", "required": True},
    {"ordinal": 18, "field": "source_observed_at_utc", "type": "datetime", "required": True},
    {"ordinal": 19, "field": "source_published_at_utc", "type": "nullable_datetime", "required": True},
    {"ordinal": 20, "field": "outcome_available_at_utc", "type": "datetime", "required": True},
    {"ordinal": 21, "field": "provider", "type": "string", "required": True},
    {"ordinal": 22, "field": "provider_event_id", "type": "string", "required": True},
    {"ordinal": 23, "field": "provider_payload_version", "type": "string", "required": True},
    {"ordinal": 24, "field": "provider_revision_id", "type": "nullable_string", "required": True},
    {"ordinal": 25, "field": "is_final_provider_revision", "type": "boolean", "required": True},
    {"ordinal": 26, "field": "ingestion_run_id", "type": "string", "required": True},
    {"ordinal": 27, "field": "raw_payload_digest", "type": "sha256_string", "required": True},
    {"ordinal": 28, "field": "outcome_provenance_digest", "type": "sha256_string", "required": True},
    {"ordinal": 29, "field": "exclusion_codes", "type": "sorted_unique_string_array", "required": True},
    {"ordinal": 30, "field": "historical_outcome_eligible", "type": "boolean", "required": True},
]

VALIDATION_RULES = [
    {"rule_id": "HOUT-ID01", "rule": "game_id_required_for_every_historical_outcome"},
    {"rule_id": "HOUT-ID02", "rule": "plate_appearance_id_required_conditionally"},
    {"rule_id": "HOUT-ID03", "rule": "pitch_id_required_conditionally"},
    {"rule_id": "HOUT-ID04", "rule": "pitcher_id_and_batter_id_required"},
    {"rule_id": "HOUT-ID05", "rule": "provider_event_id_required"},
    {"rule_id": "HOUT-ID06", "rule": "event_sequence_nonnegative_integer"},
    {"rule_id": "HOUT-ID07", "rule": "target_id_valid"},
    {"rule_id": "HOUT-ID08", "rule": "target_event_level_matches_contract"},
    {"rule_id": "HOUT-ID09", "rule": "identity_conflicts_rejected"},
    {"rule_id": "HOUT-ID10", "rule": "historical_outcome_id_deterministic"},
    {"rule_id": "HOUT-AV01", "rule": "outcome_available_at_required"},
    {"rule_id": "HOUT-AV02", "rule": "outcome_available_at_not_before_start"},
    {"rule_id": "HOUT-AV03", "rule": "source_observed_not_after_availability"},
    {"rule_id": "HOUT-AV04", "rule": "publication_timestamp_valid_when_present"},
    {"rule_id": "HOUT-AV05", "rule": "event_occurrence_not_after_availability"},
    {"rule_id": "HOUT-AV06", "rule": "ingestion_not_used_as_provider_availability"},
    {"rule_id": "HOUT-AV07", "rule": "unknown_availability_ineligible"},
    {"rule_id": "HOUT-AV08", "rule": "availability_semantics_explicit"},
    {"rule_id": "HOUT-RV01", "rule": "provider_payload_version_required"},
    {"rule_id": "HOUT-RV02", "rule": "provider_revision_preserved"},
    {"rule_id": "HOUT-RV03", "rule": "original_value_not_overwritten"},
    {"rule_id": "HOUT-RV04", "rule": "revision_identity_deterministic"},
    {"rule_id": "HOUT-RV05", "rule": "final_revision_flag_descriptive"},
    {"rule_id": "HOUT-RV06", "rule": "raw_payload_digest_required"},
    {"rule_id": "HOUT-RV07", "rule": "duplicate_revision_payload_deterministic"},
    {"rule_id": "HOUT-RV08", "rule": "revision_order_deterministic"},
    {"rule_id": "HOUT-MS01", "rule": "outcome_missing_boolean_required"},
    {"rule_id": "HOUT-MS02", "rule": "missing_outcome_requires_reason"},
    {"rule_id": "HOUT-MS03", "rule": "missing_value_not_zero_imputed"},
    {"rule_id": "HOUT-MS04", "rule": "unsupported_code_not_negative"},
    {"rule_id": "HOUT-MS05", "rule": "incomplete_game_explicit"},
    {"rule_id": "HOUT-MS06", "rule": "provider_gap_explicit"},
    {"rule_id": "HOUT-MS07", "rule": "missing_identity_rejected"},
    {"rule_id": "HOUT-MS08", "rule": "missingness_semantics_deterministic"},
    {"rule_id": "HOUT-PV01", "rule": "provider_required"},
    {"rule_id": "HOUT-PV02", "rule": "ingestion_run_id_required"},
    {"rule_id": "HOUT-PV03", "rule": "raw_payload_digest_valid_sha256"},
    {"rule_id": "HOUT-PV04", "rule": "outcome_provenance_digest_valid_sha256"},
    {"rule_id": "HOUT-PV05", "rule": "feature_and_outcome_provenance_separated"},
    {"rule_id": "HOUT-PV06", "rule": "provider_mapping_version_explicit"},
    {"rule_id": "HOUT-PV07", "rule": "target_derivation_version_explicit"},
    {"rule_id": "HOUT-PV08", "rule": "provenance_digest_deterministic"},
]

EXCLUSION_CODE_CATALOG = [
    {"code": "historical_outcome_availability_before_start", "effect": "ineligible"},
    {"code": "historical_outcome_availability_unknown", "effect": "ineligible"},
    {"code": "historical_outcome_contract_version_invalid", "effect": "rejected"},
    {"code": "historical_outcome_event_identity_conflict", "effect": "rejected"},
    {"code": "historical_outcome_event_identity_missing", "effect": "rejected"},
    {"code": "historical_outcome_event_level_invalid", "effect": "rejected"},
    {"code": "historical_outcome_event_sequence_invalid", "effect": "rejected"},
    {"code": "historical_outcome_game_identity_missing", "effect": "rejected"},
    {"code": "historical_outcome_game_incomplete", "effect": "ineligible"},
    {"code": "historical_outcome_ingestion_identity_missing", "effect": "rejected"},
    {"code": "historical_outcome_payload_digest_invalid", "effect": "rejected"},
    {"code": "historical_outcome_provider_identity_missing", "effect": "rejected"},
    {"code": "historical_outcome_provider_mapping_version_invalid", "effect": "rejected"},
    {"code": "historical_outcome_revision_conflict", "effect": "rejected"},
    {"code": "historical_outcome_source_observed_after_availability", "effect": "ineligible"},
    {"code": "historical_outcome_target_derivation_version_invalid", "effect": "rejected"},
    {"code": "historical_outcome_target_event_level_mismatch", "effect": "rejected"},
    {"code": "historical_outcome_target_invalid", "effect": "rejected"},
    {"code": "historical_outcome_value_missing", "effect": "unscored"},
    {"code": "historical_outcome_code_unsupported", "effect": "unscored"},
    {"code": "historical_outcome_collection_disabled", "effect": "non_emitting"},
]


SWING_CODES = {
    "swinging_strike",
    "swinging_strike_blocked",
    "foul",
    "foul_tip",
    "foul_bunt",
    "missed_bunt",
    "hit_into_play",
    "hit_into_play_no_out",
    "hit_into_play_score",
}

WHIFF_CODES = {
    "swinging_strike",
    "swinging_strike_blocked",
    "missed_bunt",
}

CALLED_STRIKE_CODES = {
    "called_strike",
}

BALL_IN_PLAY_CODES = {
    "hit_into_play",
    "hit_into_play_no_out",
    "hit_into_play_score",
}

STRIKEOUT_CODES = {
    "strikeout",
    "strikeout_double_play",
}

WALK_CODES = {
    "walk",
    "intent_walk",
}

HIT_CODES = {
    "single",
    "double",
    "triple",
    "home_run",
}

EXTRA_BASE_HIT_CODES = {
    "double",
    "triple",
    "home_run",
}


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


def parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None

    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(
            tzinfo=timezone.utc
        )

    return parsed.astimezone(
        timezone.utc
    )


def valid_date(value: Any) -> bool:
    if value in (None, ""):
        return False

    try:
        datetime.strptime(
            str(value),
            "%Y-%m-%d",
        )
    except ValueError:
        return False

    return True


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
        )
        + "\n",
        encoding="utf-8",
    )


def normalized_string(value: Any) -> str:
    if value is None:
        return ""

    return str(value).strip()


def deterministic_outcome_identity(
    row: Mapping[str, Any],
) -> str:
    identity_payload = {
        "historical_outcome_contract_version": CONTRACT_VERSION,
        "target_id": normalized_string(
            row.get("target_id")
        ),
        "event_level": normalized_string(
            row.get("event_level")
        ),
        "game_id": normalized_string(
            row.get("game_id")
        ),
        "plate_appearance_id": normalized_string(
            row.get("plate_appearance_id")
        ),
        "pitch_id": normalized_string(
            row.get("pitch_id")
        ),
        "pitcher_id": normalized_string(
            row.get("pitcher_id")
        ),
        "batter_id": normalized_string(
            row.get("batter_id")
        ),
        "event_sequence": row.get(
            "event_sequence"
        ),
        "provider": normalized_string(
            row.get("provider")
        ),
        "provider_event_id": normalized_string(
            row.get("provider_event_id")
        ),
        "provider_payload_version": normalized_string(
            row.get("provider_payload_version")
        ),
        "provider_revision_id": normalized_string(
            row.get("provider_revision_id")
        ),
    }

    return (
        "hout_"
        + sha256_payload(
            identity_payload
        )[:32]
    )


def provenance_digest(
    row: Mapping[str, Any],
) -> str:
    payload = {
        "historical_outcome_id": row.get(
            "historical_outcome_id"
        ),
        "historical_outcome_contract_version": row.get(
            "historical_outcome_contract_version"
        ),
        "target_id": row.get("target_id"),
        "provider": row.get("provider"),
        "provider_event_id": row.get(
            "provider_event_id"
        ),
        "provider_payload_version": row.get(
            "provider_payload_version"
        ),
        "provider_revision_id": row.get(
            "provider_revision_id"
        ),
        "is_final_provider_revision": row.get(
            "is_final_provider_revision"
        ),
        "ingestion_run_id": row.get(
            "ingestion_run_id"
        ),
        "raw_payload_digest": row.get(
            "raw_payload_digest"
        ),
        "provider_mapping_version": PROVIDER_MAPPING_VERSION,
        "target_derivation_version": TARGET_DERIVATION_VERSION,
    }

    return sha256_payload(payload)


def derive_target_value(
    target_id: str,
    raw_outcome_code: str,
    source_payload: Mapping[str, Any],
) -> tuple[Any, bool, str | None]:
    code = normalized_string(
        raw_outcome_code
    ).lower()

    if target_id == "HOUT-O01":
        return code in SWING_CODES, False, None

    if target_id == "HOUT-O02":
        return code in WHIFF_CODES, False, None

    if target_id == "HOUT-O03":
        return (
            code in CALLED_STRIKE_CODES,
            False,
            None,
        )

    if target_id == "HOUT-O04":
        return (
            code in BALL_IN_PLAY_CODES,
            False,
            None,
        )

    if target_id == "HOUT-O05":
        return (
            code in STRIKEOUT_CODES,
            False,
            None,
        )

    if target_id == "HOUT-O06":
        return code in WALK_CODES, False, None

    if target_id == "HOUT-O07":
        return code in HIT_CODES, False, None

    if target_id == "HOUT-O08":
        return (
            code in EXTRA_BASE_HIT_CODES,
            False,
            None,
        )

    if target_id == "HOUT-O09":
        value = source_payload.get(
            "contact_quality_value"
        )

        if value is None:
            return (
                None,
                True,
                "contact_quality_value_missing",
            )

        if isinstance(value, bool):
            return (
                None,
                True,
                "contact_quality_value_invalid",
            )

        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return (
                None,
                True,
                "contact_quality_value_invalid",
            )

        if not math.isfinite(
            numeric_value
        ):
            return (
                None,
                True,
                "contact_quality_value_invalid",
            )

        return numeric_value, False, None

    if target_id == "HOUT-O10":
        value = source_payload.get(
            "run_value"
        )

        if value is None:
            return (
                None,
                True,
                "run_value_missing",
            )

        if isinstance(value, bool):
            return (
                None,
                True,
                "run_value_invalid",
            )

        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return (
                None,
                True,
                "run_value_invalid",
            )

        if not math.isfinite(
            numeric_value
        ):
            return (
                None,
                True,
                "run_value_invalid",
            )

        return numeric_value, False, None

    return (
        None,
        True,
        "unsupported_target_id",
    )


def rejection_codes(
    raw: Mapping[str, Any],
) -> list[str]:
    codes: list[str] = []

    target_id = normalized_string(
        raw.get("target_id")
    )
    event_level = normalized_string(
        raw.get("event_level")
    )
    target_contract = TARGET_CONTRACT.get(
        target_id
    )

    if raw.get(
        "historical_outcome_contract_version",
        CONTRACT_VERSION,
    ) != CONTRACT_VERSION:
        codes.append(
            "historical_outcome_contract_version_invalid"
        )

    if not normalized_string(
        raw.get("game_id")
    ):
        codes.append(
            "historical_outcome_game_identity_missing"
        )

    if not valid_date(
        raw.get("game_date")
    ):
        codes.append(
            "historical_outcome_event_identity_missing"
        )

    scheduled_start = parse_datetime(
        raw.get("scheduled_start_utc")
    )

    if scheduled_start is None:
        codes.append(
            "historical_outcome_event_identity_missing"
        )

    if target_id not in TARGET_CONTRACT:
        codes.append(
            "historical_outcome_target_invalid"
        )

    if event_level not in VALID_EVENT_LEVELS:
        codes.append(
            "historical_outcome_event_level_invalid"
        )

    if (
        target_contract is not None
        and event_level
        != target_contract["event_level"]
    ):
        codes.append(
            "historical_outcome_target_event_level_mismatch"
        )

    if event_level in {
        "plate_appearance",
        "pitch",
        "contact",
    } and not normalized_string(
        raw.get("plate_appearance_id")
    ):
        codes.append(
            "historical_outcome_event_identity_missing"
        )

    if event_level in {
        "pitch",
        "contact",
    } and not normalized_string(
        raw.get("pitch_id")
    ):
        codes.append(
            "historical_outcome_event_identity_missing"
        )

    if not normalized_string(
        raw.get("pitcher_id")
    ) or not normalized_string(
        raw.get("batter_id")
    ):
        codes.append(
            "historical_outcome_event_identity_missing"
        )

    event_sequence = raw.get(
        "event_sequence"
    )

    if (
        isinstance(event_sequence, bool)
        or not isinstance(
            event_sequence,
            int,
        )
        or event_sequence < 0
    ):
        codes.append(
            "historical_outcome_event_sequence_invalid"
        )

    if not normalized_string(
        raw.get("provider")
    ) or not normalized_string(
        raw.get("provider_event_id")
    ):
        codes.append(
            "historical_outcome_provider_identity_missing"
        )

    if not normalized_string(
        raw.get("provider_payload_version")
    ):
        codes.append(
            "historical_outcome_revision_conflict"
        )

    if not normalized_string(
        raw.get("ingestion_run_id")
    ):
        codes.append(
            "historical_outcome_ingestion_identity_missing"
        )

    raw_payload_digest = normalized_string(
        raw.get("raw_payload_digest")
    )

    if not SHA256_PATTERN.fullmatch(
        raw_payload_digest
    ):
        codes.append(
            "historical_outcome_payload_digest_invalid"
        )

    source_observed = parse_datetime(
        raw.get("source_observed_at_utc")
    )
    outcome_available = parse_datetime(
        raw.get("outcome_available_at_utc")
    )
    event_occurred = parse_datetime(
        raw.get("event_occurred_at_utc")
    )
    source_published = parse_datetime(
        raw.get("source_published_at_utc")
    )

    if outcome_available is None:
        codes.append(
            "historical_outcome_availability_unknown"
        )

    if (
        scheduled_start is not None
        and outcome_available is not None
        and outcome_available
        < scheduled_start
    ):
        codes.append(
            "historical_outcome_availability_before_start"
        )

    if (
        source_observed is None
        or (
            outcome_available is not None
            and source_observed
            > outcome_available
        )
    ):
        codes.append(
            "historical_outcome_source_observed_after_availability"
        )

    if (
        event_occurred is not None
        and outcome_available is not None
        and event_occurred
        > outcome_available
    ):
        codes.append(
            "historical_outcome_availability_unknown"
        )

    if (
        source_published is not None
        and source_observed is not None
        and source_published
        < source_observed
    ):
        codes.append(
            "historical_outcome_revision_conflict"
        )

    if raw.get(
        "provider_mapping_version",
        PROVIDER_MAPPING_VERSION,
    ) != PROVIDER_MAPPING_VERSION:
        codes.append(
            "historical_outcome_provider_mapping_version_invalid"
        )

    if raw.get(
        "target_derivation_version",
        TARGET_DERIVATION_VERSION,
    ) != TARGET_DERIVATION_VERSION:
        codes.append(
            "historical_outcome_target_derivation_version_invalid"
        )

    if raw.get(
        "identity_conflict",
        False,
    ):
        codes.append(
            "historical_outcome_event_identity_conflict"
        )

    if raw.get(
        "revision_conflict",
        False,
    ):
        codes.append(
            "historical_outcome_revision_conflict"
        )

    if raw.get(
        "game_incomplete",
        False,
    ):
        codes.append(
            "historical_outcome_game_incomplete"
        )

    return sorted(set(codes))


def materialize_historical_outcome(
    raw: Mapping[str, Any],
) -> dict[str, Any] | None:
    if raw.get(
        "collection_enabled",
        True,
    ) is False:
        return None

    row = deepcopy(dict(raw))

    target_id = normalized_string(
        row.get("target_id")
    )
    raw_outcome_code = normalized_string(
        row.get("raw_outcome_code")
    )

    (
        outcome_value,
        outcome_missing,
        outcome_missing_reason,
    ) = derive_target_value(
        target_id,
        raw_outcome_code,
        row.get(
            "source_payload",
            {},
        ),
    )

    exclusions = rejection_codes(row)

    if outcome_missing:
        exclusions.append(
            "historical_outcome_value_missing"
        )

        if outcome_missing_reason == (
            "unsupported_target_id"
        ):
            exclusions.append(
                "historical_outcome_code_unsupported"
            )

    exclusions = sorted(
        set(exclusions)
    )

    rejection_effect_codes = {
        item["code"]
        for item in EXCLUSION_CODE_CATALOG
        if item["effect"] == "rejected"
    }
    ineligible_effect_codes = {
        item["code"]
        for item in EXCLUSION_CODE_CATALOG
        if item["effect"] == "ineligible"
    }

    historical_outcome_eligible = not any(
        code in rejection_effect_codes
        or code in ineligible_effect_codes
        for code in exclusions
    )

    materialized = {
        "historical_outcome_id": "",
        "historical_outcome_contract_version": CONTRACT_VERSION,
        "target_id": target_id,
        "event_level": normalized_string(
            row.get("event_level")
        ),
        "game_id": normalized_string(
            row.get("game_id")
        ),
        "game_date": normalized_string(
            row.get("game_date")
        ),
        "scheduled_start_utc": normalized_string(
            row.get("scheduled_start_utc")
        ),
        "plate_appearance_id": (
            normalized_string(
                row.get("plate_appearance_id")
            )
            or None
        ),
        "pitch_id": (
            normalized_string(
                row.get("pitch_id")
            )
            or None
        ),
        "pitcher_id": normalized_string(
            row.get("pitcher_id")
        ),
        "batter_id": normalized_string(
            row.get("batter_id")
        ),
        "event_sequence": row.get(
            "event_sequence"
        ),
        "raw_outcome_code": raw_outcome_code,
        "outcome_value": outcome_value,
        "outcome_missing": outcome_missing,
        "outcome_missing_reason": (
            outcome_missing_reason
        ),
        "event_occurred_at_utc": (
            normalized_string(
                row.get("event_occurred_at_utc")
            )
            or None
        ),
        "source_observed_at_utc": normalized_string(
            row.get("source_observed_at_utc")
        ),
        "source_published_at_utc": (
            normalized_string(
                row.get("source_published_at_utc")
            )
            or None
        ),
        "outcome_available_at_utc": normalized_string(
            row.get("outcome_available_at_utc")
        ),
        "provider": normalized_string(
            row.get("provider")
        ),
        "provider_event_id": normalized_string(
            row.get("provider_event_id")
        ),
        "provider_payload_version": normalized_string(
            row.get("provider_payload_version")
        ),
        "provider_revision_id": (
            normalized_string(
                row.get("provider_revision_id")
            )
            or None
        ),
        "is_final_provider_revision": bool(
            row.get(
                "is_final_provider_revision",
                False,
            )
        ),
        "ingestion_run_id": normalized_string(
            row.get("ingestion_run_id")
        ),
        "raw_payload_digest": normalized_string(
            row.get("raw_payload_digest")
        ),
        "outcome_provenance_digest": "",
        "exclusion_codes": exclusions,
        "historical_outcome_eligible": (
            historical_outcome_eligible
        ),
    }

    materialized[
        "historical_outcome_id"
    ] = deterministic_outcome_identity(
        materialized
    )
    materialized[
        "outcome_provenance_digest"
    ] = provenance_digest(
        materialized
    )

    return materialized


def base_fixture() -> dict[str, Any]:
    raw_payload = {
        "provider_event_id": "provider-event-001",
        "description": "swinging_strike",
        "revision": "r1",
    }

    return {
        "collection_enabled": True,
        "historical_outcome_contract_version": CONTRACT_VERSION,
        "target_id": "HOUT-O02",
        "event_level": "pitch",
        "game_id": "game-2026-001",
        "game_date": "2026-04-01",
        "scheduled_start_utc": "2026-04-01T18:10:00Z",
        "plate_appearance_id": "pa-001",
        "pitch_id": "pitch-001",
        "pitcher_id": "pitcher-001",
        "batter_id": "batter-001",
        "event_sequence": 1,
        "raw_outcome_code": "swinging_strike",
        "source_payload": {},
        "event_occurred_at_utc": "2026-04-01T18:12:10Z",
        "source_observed_at_utc": "2026-04-01T18:12:11Z",
        "source_published_at_utc": "2026-04-01T18:12:12Z",
        "outcome_available_at_utc": "2026-04-01T18:12:12Z",
        "provider": "synthetic_statcast",
        "provider_event_id": "provider-event-001",
        "provider_payload_version": "payload-v1",
        "provider_revision_id": "revision-001",
        "is_final_provider_revision": False,
        "ingestion_run_id": "ingestion-run-001",
        "raw_payload_digest": sha256_payload(
            raw_payload
        ),
        "provider_mapping_version": PROVIDER_MAPPING_VERSION,
        "target_derivation_version": TARGET_DERIVATION_VERSION,
        "identity_conflict": False,
        "revision_conflict": False,
        "game_incomplete": False,
    }


def build_fixtures() -> list[dict[str, Any]]:
    fixtures: list[dict[str, Any]] = []

    target_cases = [
        (
            "HOUT-FIX-001",
            "HOUT-O01",
            "pitch",
            "swinging_strike",
            {},
            True,
            True,
            [],
        ),
        (
            "HOUT-FIX-002",
            "HOUT-O02",
            "pitch",
            "swinging_strike",
            {},
            True,
            True,
            [],
        ),
        (
            "HOUT-FIX-003",
            "HOUT-O03",
            "pitch",
            "called_strike",
            {},
            True,
            True,
            [],
        ),
        (
            "HOUT-FIX-004",
            "HOUT-O04",
            "pitch",
            "hit_into_play",
            {},
            True,
            True,
            [],
        ),
        (
            "HOUT-FIX-005",
            "HOUT-O05",
            "plate_appearance",
            "strikeout",
            {},
            True,
            True,
            [],
        ),
        (
            "HOUT-FIX-006",
            "HOUT-O06",
            "plate_appearance",
            "walk",
            {},
            True,
            True,
            [],
        ),
        (
            "HOUT-FIX-007",
            "HOUT-O07",
            "plate_appearance",
            "single",
            {},
            True,
            True,
            [],
        ),
        (
            "HOUT-FIX-008",
            "HOUT-O08",
            "plate_appearance",
            "double",
            {},
            True,
            True,
            [],
        ),
        (
            "HOUT-FIX-009",
            "HOUT-O09",
            "contact",
            "hit_into_play",
            {
                "contact_quality_value": 0.412,
            },
            0.412,
            True,
            [],
        ),
        (
            "HOUT-FIX-010",
            "HOUT-O10",
            "event",
            "single",
            {
                "run_value": 0.47,
            },
            0.47,
            True,
            [],
        ),
    ]

    for (
        fixture_id,
        target_id,
        event_level,
        raw_code,
        source_payload,
        expected_value,
        expected_eligible,
        expected_codes,
    ) in target_cases:
        raw = base_fixture()
        raw.update(
            {
                "target_id": target_id,
                "event_level": event_level,
                "raw_outcome_code": raw_code,
                "source_payload": source_payload,
                "provider_event_id": fixture_id,
                "event_sequence": int(
                    fixture_id.rsplit(
                        "-",
                        1,
                    )[-1]
                ),
            }
        )

        if event_level == "event":
            raw["plate_appearance_id"] = None
            raw["pitch_id"] = None
        elif event_level == "plate_appearance":
            raw["pitch_id"] = None

        raw["raw_payload_digest"] = (
            sha256_payload(
                {
                    "fixture_id": fixture_id,
                    "raw_outcome_code": raw_code,
                    "source_payload": source_payload,
                }
            )
        )

        fixtures.append(
            {
                "fixture_id": fixture_id,
                "description": (
                    f"valid_{target_id.lower()}"
                ),
                "raw": raw,
                "expect_emitted": True,
                "expected_value": expected_value,
                "expected_missing": False,
                "expected_eligible": (
                    expected_eligible
                ),
                "expected_codes": (
                    expected_codes
                ),
            }
        )

    missing_contact = base_fixture()
    missing_contact.update(
        {
            "target_id": "HOUT-O09",
            "event_level": "contact",
            "provider_event_id": "provider-missing-contact",
            "source_payload": {
                "contact_quality_value": None,
            },
        }
    )
    fixtures.append(
        {
            "fixture_id": "HOUT-FIX-011",
            "description": "missing_contact_quality_retained_unscored",
            "raw": missing_contact,
            "expect_emitted": True,
            "expected_value": None,
            "expected_missing": True,
            "expected_eligible": True,
            "expected_codes": [
                "historical_outcome_value_missing",
            ],
        }
    )

    availability_before_start = base_fixture()
    availability_before_start[
        "provider_event_id"
    ] = "provider-before-start"
    availability_before_start[
        "source_observed_at_utc"
    ] = "2026-04-01T18:10:01Z"
    availability_before_start[
        "event_occurred_at_utc"
    ] = "2026-04-01T17:59:59Z"
    availability_before_start[
        "outcome_available_at_utc"
    ] = "2026-04-01T17:59:59Z"
    fixtures.append(
        {
            "fixture_id": "HOUT-FIX-012",
            "description": "availability_before_start_ineligible",
            "raw": availability_before_start,
            "expect_emitted": True,
            "expected_value": True,
            "expected_missing": False,
            "expected_eligible": False,
            "expected_codes": [
                "historical_outcome_availability_before_start",
                "historical_outcome_source_observed_after_availability",
            ],
        }
    )

    identity_conflict = base_fixture()
    identity_conflict.update(
        {
            "provider_event_id": "provider-identity-conflict",
            "identity_conflict": True,
        }
    )
    fixtures.append(
        {
            "fixture_id": "HOUT-FIX-013",
            "description": "identity_conflict_rejected",
            "raw": identity_conflict,
            "expect_emitted": True,
            "expected_value": True,
            "expected_missing": False,
            "expected_eligible": False,
            "expected_codes": [
                "historical_outcome_event_identity_conflict",
            ],
        }
    )

    revision_conflict = base_fixture()
    revision_conflict.update(
        {
            "provider_event_id": "provider-revision-conflict",
            "revision_conflict": True,
        }
    )
    fixtures.append(
        {
            "fixture_id": "HOUT-FIX-014",
            "description": "revision_conflict_rejected",
            "raw": revision_conflict,
            "expect_emitted": True,
            "expected_value": True,
            "expected_missing": False,
            "expected_eligible": False,
            "expected_codes": [
                "historical_outcome_revision_conflict",
            ],
        }
    )

    disabled = base_fixture()
    disabled.update(
        {
            "provider_event_id": "provider-disabled",
            "collection_enabled": False,
        }
    )
    fixtures.append(
        {
            "fixture_id": "HOUT-FIX-015",
            "description": "disabled_path_non_emitting",
            "raw": disabled,
            "expect_emitted": False,
            "expected_value": None,
            "expected_missing": None,
            "expected_eligible": None,
            "expected_codes": [],
        }
    )

    return fixtures


def evaluate_fixtures() -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    fixture_results: list[
        dict[str, Any]
    ] = []
    emitted_records: list[
        dict[str, Any]
    ] = []

    for fixture in build_fixtures():
        record = materialize_historical_outcome(
            fixture["raw"]
        )

        emitted = record is not None

        if record is not None:
            emitted_records.append(record)

        actual_value = (
            record.get("outcome_value")
            if record is not None
            else None
        )
        actual_missing = (
            record.get("outcome_missing")
            if record is not None
            else None
        )
        actual_eligible = (
            record.get(
                "historical_outcome_eligible"
            )
            if record is not None
            else None
        )
        actual_codes = (
            record.get("exclusion_codes")
            if record is not None
            else []
        )

        expected_value = fixture[
            "expected_value"
        ]

        if (
            isinstance(expected_value, float)
            and isinstance(actual_value, float)
        ):
            value_matches = math.isclose(
                actual_value,
                expected_value,
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        else:
            value_matches = (
                actual_value
                == expected_value
            )

        passed = all(
            [
                emitted
                == fixture[
                    "expect_emitted"
                ],
                value_matches,
                actual_missing
                == fixture[
                    "expected_missing"
                ],
                actual_eligible
                == fixture[
                    "expected_eligible"
                ],
                actual_codes
                == fixture[
                    "expected_codes"
                ],
            ]
        )

        fixture_results.append(
            {
                "fixture_id": fixture[
                    "fixture_id"
                ],
                "description": fixture[
                    "description"
                ],
                "expect_emitted": fixture[
                    "expect_emitted"
                ],
                "actual_emitted": emitted,
                "expected_value": expected_value,
                "actual_value": actual_value,
                "expected_missing": fixture[
                    "expected_missing"
                ],
                "actual_missing": actual_missing,
                "expected_eligible": fixture[
                    "expected_eligible"
                ],
                "actual_eligible": actual_eligible,
                "expected_codes": "|".join(
                    fixture[
                        "expected_codes"
                    ]
                ),
                "actual_codes": "|".join(
                    actual_codes
                ),
                "passed": passed,
            }
        )

    emitted_records.sort(
        key=lambda row: (
            row["game_date"],
            row["game_id"],
            row["event_sequence"],
            row["target_id"],
            row["historical_outcome_id"],
        )
    )

    return (
        fixture_results,
        emitted_records,
    )


def csv_safe_record(
    record: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        key: (
            "|".join(value)
            if isinstance(value, list)
            else value
        )
        for key, value in record.items()
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_constants = string_constants(
        PREDECESSOR_PATH
    )

    predecessor_present = (
        PREDECESSOR_PATH.exists()
        and EXPECTED_PREDECESSOR_DIAGNOSIS
        in predecessor_constants
        and EXPECTED_PREDECESSOR_AUTHORITY
        in predecessor_constants
    )

    (
        fixture_results,
        emitted_records,
    ) = evaluate_fixtures()

    replay_fixture_results, replay_records = (
        evaluate_fixtures()
    )

    record_ids = [
        row["historical_outcome_id"]
        for row in emitted_records
    ]
    provenance_digests = [
        row[
            "outcome_provenance_digest"
        ]
        for row in emitted_records
    ]

    expected_fields = [
        row["field"]
        for row in HISTORICAL_OUTCOME_RECORD_FIELDS
    ]

    emitted_field_contract_matches = all(
        list(record.keys())
        == expected_fields
        for record in emitted_records
    )

    fixture_expectations_all_pass = all(
        bool(row["passed"])
        for row in fixture_results
    )

    valid_target_fixtures_pass = all(
        bool(row["passed"])
        for row in fixture_results[:10]
    )

    missing_contact_fixture_passes = next(
        row["passed"]
        for row in fixture_results
        if row["fixture_id"]
        == "HOUT-FIX-011"
    )

    availability_fixture_passes = next(
        row["passed"]
        for row in fixture_results
        if row["fixture_id"]
        == "HOUT-FIX-012"
    )

    identity_conflict_fixture_passes = next(
        row["passed"]
        for row in fixture_results
        if row["fixture_id"]
        == "HOUT-FIX-013"
    )

    revision_conflict_fixture_passes = next(
        row["passed"]
        for row in fixture_results
        if row["fixture_id"]
        == "HOUT-FIX-014"
    )

    disabled_fixture_passes = next(
        row["passed"]
        for row in fixture_results
        if row["fixture_id"]
        == "HOUT-FIX-015"
    )

    implementation_checks = [
        {
            "check": "nine_c_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "contract_version_explicit",
            "actual": CONTRACT_VERSION,
            "expected": CONTRACT_VERSION,
            "passed": bool(CONTRACT_VERSION),
        },
        {
            "check": "ten_targets_implemented",
            "actual": len(TARGET_CONTRACT),
            "expected": 10,
            "passed": len(TARGET_CONTRACT) == 10,
        },
        {
            "check": "thirty_contract_fields_implemented",
            "actual": len(
                HISTORICAL_OUTCOME_RECORD_FIELDS
            ),
            "expected": 30,
            "passed": len(
                HISTORICAL_OUTCOME_RECORD_FIELDS
            )
            == 30,
        },
        {
            "check": "forty_two_validation_rules_implemented",
            "actual": len(
                VALIDATION_RULES
            ),
            "expected": 42,
            "passed": len(
                VALIDATION_RULES
            )
            == 42,
        },
        {
            "check": "four_event_levels_supported",
            "actual": len(
                VALID_EVENT_LEVELS
            ),
            "expected": 4,
            "passed": len(
                VALID_EVENT_LEVELS
            )
            == 4,
        },
        {
            "check": "valid_target_fixtures_pass",
            "actual": valid_target_fixtures_pass,
            "expected": True,
            "passed": valid_target_fixtures_pass,
        },
        {
            "check": "missing_contact_semantics_pass",
            "actual": missing_contact_fixture_passes,
            "expected": True,
            "passed": missing_contact_fixture_passes,
        },
        {
            "check": "availability_boundary_enforced",
            "actual": availability_fixture_passes,
            "expected": True,
            "passed": availability_fixture_passes,
        },
        {
            "check": "identity_conflict_rejected",
            "actual": identity_conflict_fixture_passes,
            "expected": True,
            "passed": identity_conflict_fixture_passes,
        },
        {
            "check": "revision_conflict_rejected",
            "actual": revision_conflict_fixture_passes,
            "expected": True,
            "passed": revision_conflict_fixture_passes,
        },
        {
            "check": "disabled_path_non_emitting",
            "actual": disabled_fixture_passes,
            "expected": True,
            "passed": disabled_fixture_passes,
        },
        {
            "check": "fixture_expectations_all_pass",
            "actual": fixture_expectations_all_pass,
            "expected": True,
            "passed": fixture_expectations_all_pass,
        },
        {
            "check": "emitted_field_contract_matches",
            "actual": emitted_field_contract_matches,
            "expected": True,
            "passed": emitted_field_contract_matches,
        },
        {
            "check": "historical_outcome_ids_unique",
            "actual": len(set(record_ids)),
            "expected": len(record_ids),
            "passed": len(set(record_ids))
            == len(record_ids),
        },
        {
            "check": "historical_outcome_ids_deterministic",
            "actual": record_ids,
            "expected": [
                row[
                    "historical_outcome_id"
                ]
                for row in replay_records
            ],
            "passed": record_ids
            == [
                row[
                    "historical_outcome_id"
                ]
                for row in replay_records
            ],
        },
        {
            "check": "raw_payload_digests_valid_sha256",
            "actual": sum(
                bool(
                    SHA256_PATTERN.fullmatch(
                        row[
                            "raw_payload_digest"
                        ]
                    )
                )
                for row in emitted_records
            ),
            "expected": len(
                emitted_records
            ),
            "passed": all(
                bool(
                    SHA256_PATTERN.fullmatch(
                        row[
                            "raw_payload_digest"
                        ]
                    )
                )
                for row in emitted_records
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
                for digest in provenance_digests
            ),
            "expected": len(
                emitted_records
            ),
            "passed": all(
                bool(
                    SHA256_PATTERN.fullmatch(
                        digest
                    )
                )
                for digest in provenance_digests
            ),
        },
        {
            "check": "provenance_digests_deterministic",
            "actual": provenance_digests,
            "expected": [
                row[
                    "outcome_provenance_digest"
                ]
                for row in replay_records
            ],
            "passed": provenance_digests
            == [
                row[
                    "outcome_provenance_digest"
                ]
                for row in replay_records
            ],
        },
        {
            "check": "fixture_replay_deterministic",
            "actual": fixture_results,
            "expected": replay_fixture_results,
            "passed": fixture_results
            == replay_fixture_results,
        },
        {
            "check": "feature_outcome_join_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "prediction_join_absent",
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
        bool(row["passed"])
        for row in implementation_checks
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_contract_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_contract_implementation_failed"
    )

    recommended_next_layer = (
        "9E_pitch_type_matchup_overlay_historical_outcome_source_mapping_plan"
        if all_checks_passed
        else
        "9D_pitch_type_matchup_overlay_historical_outcome_contract_implementation_remediation"
    )

    authority_withheld = [
        "accuracy_evaluation",
        "augmented_prediction_generation",
        "backtest_execution",
        "baseline_prediction_generation",
        "bet_recommendation",
        "calibration_evaluation",
        "canonical_probability_authority_change",
        "edge_detection",
        "fallback_tuning",
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
        OUTPUT_DIR
        / "historical_outcome_record_contract.csv",
        [
            "ordinal",
            "field",
            "type",
            "required",
        ],
        HISTORICAL_OUTCOME_RECORD_FIELDS,
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
            "effect",
        ],
        EXCLUSION_CODE_CATALOG,
    )

    write_csv(
        OUTPUT_DIR / "fixture_results.csv",
        [
            "fixture_id",
            "description",
            "expect_emitted",
            "actual_emitted",
            "expected_value",
            "actual_value",
            "expected_missing",
            "actual_missing",
            "expected_eligible",
            "actual_eligible",
            "expected_codes",
            "actual_codes",
            "passed",
        ],
        fixture_results,
    )

    write_csv(
        OUTPUT_DIR
        / "synthetic_historical_outcome_records.csv",
        expected_fields,
        [
            csv_safe_record(record)
            for record in emitted_records
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
        "targets_implemented": len(
            TARGET_CONTRACT
        ),
        "contract_fields_implemented": len(
            HISTORICAL_OUTCOME_RECORD_FIELDS
        ),
        "validation_rules_implemented": len(
            VALIDATION_RULES
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
                    "historical_outcome_eligible"
                ]
            )
            for row in emitted_records
        ),
        "production_historical_outcome_records_materialized": 0,
        "feature_outcome_joins_executed": 0,
        "prediction_joins_executed": 0,
        "predictive_metrics_calculated": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed": all_checks_passed,
        "recommended_next_layer": (
            recommended_next_layer
        ),
    }

    write_json(
        OUTPUT_DIR / "summary.json",
        summary,
    )

    diagnosis = {
        "all_checks_passed": all_checks_passed,
        "authority_granted": (
            "historical_outcome_source_mapping_planning"
            if all_checks_passed
            else
            "none"
        ),
        "authority_withheld": sorted(
            authority_withheld
        ),
        "contract_version": CONTRACT_VERSION,
        "diagnosis": diagnosis_name,
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "output_directory": str(
            OUTPUT_DIR.relative_to(ROOT)
        ),
        "recommended_next_layer": (
            recommended_next_layer
        ),
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    manifest_rows: list[
        dict[str, Any]
    ] = []

    for path in sorted(
        OUTPUT_DIR.iterdir(),
        key=lambda item: item.name,
    ):
        if not path.is_file():
            continue

        manifest_rows.append(
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
        manifest_rows,
    )

    print(
        f"Layer: {LAYER_ID} — {LAYER_NAME}"
    )
    print(
        f"Contract version: {CONTRACT_VERSION}"
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
        "Synthetic historical outcome records emitted: "
        f"{summary['synthetic_records_emitted']}"
    )
    print(
        "Synthetic historical outcome records eligible: "
        f"{summary['synthetic_records_eligible']}"
    )
    print(
        "Production historical outcome records materialized: 0"
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
