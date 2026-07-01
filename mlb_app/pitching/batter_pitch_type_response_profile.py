"""
Diagnostic-only batter pitch-type response profile contract.

Builds deterministic, immutable batter response profiles by canonical pitch
type, pitcher handedness, and count context.

This module does not:
- alter pitch selection or sequencing;
- activate production pitcher-batter matchup adjustments;
- alter swing, whiff, contact, batted-ball, or plate-appearance probabilities;
- alter simulation state, parameters, or outcomes;
- grant production authority.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
import math
from typing import Any, Mapping, Sequence

from mlb_app.pitching.canonical_pitch_taxonomy import (
    CANONICAL_PITCHES,
    TAXONOMY_VERSION,
)


PROFILE_VERSION = "8G-v1"
PROFILE_MINIMUM_PITCH_COUNT = 100
ENTRY_MINIMUM_PITCH_COUNT = 25
CURRENT_PROFILE_MAXIMUM_AGE_DAYS = 14
BATTED_BALL_RATE_SUM_TOLERANCE = 0.001

SUPPORTED_BATTER_HANDS = frozenset(
    {"R", "L", "S", "U"}
)
SUPPORTED_PITCHER_HANDS = frozenset(
    {"R", "L", "U"}
)
SUPPORTED_COUNT_CONTEXTS = frozenset(
    {
        "all_counts",
        "ahead",
        "even",
        "behind",
        "two_strike",
        "first_pitch",
        "unknown",
    }
)


@dataclass(frozen=True)
class BatterPitchTypeResponseEntry:
    canonical_pitch_id: str
    canonical_pitch_name: str
    canonical_family: str
    pitcher_hand: str
    count_context: str
    pitch_count: int
    swing_count: int | None
    contact_count: int | None
    batted_ball_count: int | None
    swing_rate: float | None
    chase_rate: float | None
    zone_swing_rate: float | None
    whiff_rate: float | None
    contact_rate: float | None
    called_strike_plus_whiff_rate: float | None
    avg_exit_velocity_mph: float | None
    avg_launch_angle_degrees: float | None
    hard_hit_rate: float | None
    barrel_rate: float | None
    ground_ball_rate: float | None
    line_drive_rate: float | None
    fly_ball_rate: float | None
    popup_rate: float | None
    response_index: float | None
    diagnostic_codes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["diagnostic_codes"] = list(
            self.diagnostic_codes
        )
        return payload


@dataclass(frozen=True)
class BatterPitchTypeResponseProfile:
    emitted: bool
    reason: str
    batter_id: str | None
    batter_name: str | None
    batter_hand: str | None
    season: int | None
    as_of_date_utc: str | None
    source_name: str | None
    source_record_id: str | None
    source_timestamp_utc: str | None
    source_priority: int | None
    sample_pitch_count: int
    sample_plate_appearance_count: int | None
    profile_status: str
    response_entries: tuple[
        BatterPitchTypeResponseEntry,
        ...,
    ]
    taxonomy_version: str
    profile_version: str
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    production_authority: bool = False
    production_behavior_changed: bool = False
    simulation_behavior_changed: bool = False
    pitch_selection_changed: bool = False
    pitch_sequence_changed: bool = False
    matchup_adjustment_activated: bool = False
    swing_probability_changed: bool = False
    whiff_probability_changed: bool = False
    contact_probability_changed: bool = False
    batted_ball_probability_changed: bool = False
    contact_quality_changed: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["response_entries"] = [
            entry.to_dict()
            for entry in self.response_entries
        ]
        payload["diagnostic_codes"] = list(
            self.diagnostic_codes
        )
        payload["validation_errors"] = list(
            self.validation_errors
        )
        return payload


def _sorted_unique_strings(
    values: Sequence[str],
) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                value
                for value in values
                if isinstance(value, str)
                and value
            }
        )
    )


def _finite_or_none(
    value: Any,
) -> float | None:
    if value is None:
        return None

    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(converted):
        return None

    return converted


def _rate_or_none(
    value: Any,
) -> float | None:
    converted = _finite_or_none(
        value
    )

    if converted is None:
        return None

    if not 0.0 <= converted <= 1.0:
        return None

    return converted


def _positive_or_none(
    value: Any,
) -> float | None:
    converted = _finite_or_none(
        value
    )

    if (
        converted is None
        or converted <= 0.0
    ):
        return None

    return converted


def _nonnegative_int_or_none(
    value: Any,
) -> int | None:
    if value is None:
        return None

    try:
        converted = int(value)
    except (TypeError, ValueError):
        return None

    if converted < 0:
        return None

    return converted


def _normalize_batter_hand(
    value: Any,
) -> str:
    normalized = str(
        value or "U"
    ).strip().upper()

    if normalized not in SUPPORTED_BATTER_HANDS:
        return "U"

    return normalized


def _normalize_pitcher_hand(
    value: Any,
) -> str:
    normalized = str(
        value or "U"
    ).strip().upper()

    if normalized not in SUPPORTED_PITCHER_HANDS:
        return "U"

    return normalized


def _normalize_count_context(
    value: Any,
) -> str:
    normalized = str(
        value or "unknown"
    ).strip().lower()

    if normalized not in SUPPORTED_COUNT_CONTEXTS:
        return "unknown"

    return normalized


def _parse_date(
    value: Any,
) -> date | None:
    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    if isinstance(value, str):
        try:
            return date.fromisoformat(
                value[:10]
            )
        except ValueError:
            return None

    return None


def _parse_datetime(
    value: Any,
) -> datetime | None:
    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        try:
            return datetime.fromisoformat(
                value
            )
        except ValueError:
            return None

    return None


def _source_priority(
    source_name: str,
) -> int:
    normalized = source_name.strip().lower()

    if normalized in {
        "statcast",
        "statcast_pitch_level_batter_aggregate",
        "baseball_savant",
        "baseball savant",
    }:
        return 1

    if normalized in {
        "trusted_provider",
        "trusted_provider_pitch_type_split",
        "provider",
    }:
        return 2

    if normalized in {
        "repository_cache",
        "repository_cached_batter_response_profile",
        "cache",
    }:
        return 3

    if normalized in {
        "season_summary",
        "season_level_pitch_family_summary",
    }:
        return 4

    return 5


def _disabled_profile(
    payload: Mapping[str, Any],
) -> BatterPitchTypeResponseProfile:
    return BatterPitchTypeResponseProfile(
        emitted=False,
        reason="profile_disabled",
        batter_id=payload.get(
            "batter_id"
        ),
        batter_name=payload.get(
            "batter_name"
        ),
        batter_hand=None,
        season=None,
        as_of_date_utc=None,
        source_name=payload.get(
            "source_name"
        ),
        source_record_id=payload.get(
            "source_record_id"
        ),
        source_timestamp_utc=None,
        source_priority=None,
        sample_pitch_count=0,
        sample_plate_appearance_count=None,
        profile_status="disabled",
        response_entries=(),
        taxonomy_version=TAXONOMY_VERSION,
        profile_version=PROFILE_VERSION,
        diagnostic_codes=(
            "batter_pitch_response_profile_disabled",
        ),
        validation_errors=(),
    )


def build_batter_pitch_type_response_profile(
    payload: Mapping[str, Any],
) -> BatterPitchTypeResponseProfile:
    copied_payload = dict(payload)

    if not bool(
        copied_payload.get(
            "enabled",
            False,
        )
    ):
        return _disabled_profile(
            copied_payload
        )

    validation_errors: list[str] = []
    diagnostic_codes: list[str] = []

    batter_id_raw = copied_payload.get(
        "batter_id"
    )
    batter_id = (
        str(batter_id_raw).strip()
        if batter_id_raw is not None
        else ""
    )

    if not batter_id:
        validation_errors.append(
            "batter_pitch_response_batter_identity_missing"
        )

    batter_name_raw = copied_payload.get(
        "batter_name"
    )
    batter_name = (
        str(batter_name_raw).strip()
        if batter_name_raw is not None
        else None
    )

    batter_hand = _normalize_batter_hand(
        copied_payload.get(
            "batter_hand"
        )
    )

    if batter_hand == "U":
        diagnostic_codes.append(
            "batter_pitch_response_batter_hand_unknown"
        )

    try:
        season = int(
            copied_payload.get(
                "season"
            )
        )
    except (TypeError, ValueError):
        season = 0

    if season <= 0:
        validation_errors.append(
            "batter_pitch_response_season_invalid"
        )

    as_of_date = _parse_date(
        copied_payload.get(
            "as_of_date_utc"
        )
    )

    if as_of_date is None:
        validation_errors.append(
            "batter_pitch_response_as_of_date_invalid"
        )

    source_name_raw = copied_payload.get(
        "source_name"
    )
    source_name = (
        str(source_name_raw).strip()
        if source_name_raw is not None
        else ""
    )

    if not source_name:
        validation_errors.append(
            "batter_pitch_response_source_name_missing"
        )

    source_timestamp = _parse_datetime(
        copied_payload.get(
            "source_timestamp_utc"
        )
    )

    source_priority = (
        _source_priority(source_name)
        if source_name
        else 5
    )

    raw_entries = copied_payload.get(
        "response_entries",
        [],
    )

    if not isinstance(
        raw_entries,
        Sequence,
    ) or isinstance(
        raw_entries,
        (str, bytes),
    ):
        raw_entries = []
        validation_errors.append(
            "batter_pitch_response_entries_invalid"
        )

    parsed_entries: list[
        BatterPitchTypeResponseEntry
    ] = []

    seen_keys: set[
        tuple[str, str, str]
    ] = set()

    total_pitch_count = 0

    rate_fields = (
        "swing_rate",
        "chase_rate",
        "zone_swing_rate",
        "whiff_rate",
        "contact_rate",
        "called_strike_plus_whiff_rate",
        "hard_hit_rate",
        "barrel_rate",
        "ground_ball_rate",
        "line_drive_rate",
        "fly_ball_rate",
        "popup_rate",
    )

    for raw_entry in raw_entries:
        if not isinstance(
            raw_entry,
            Mapping,
        ):
            validation_errors.append(
                "batter_pitch_response_entry_invalid"
            )
            continue

        canonical_pitch_id = str(
            raw_entry.get(
                "canonical_pitch_id",
                "UN",
            )
        ).strip().upper()

        if canonical_pitch_id not in CANONICAL_PITCHES:
            canonical_pitch_id = "UN"
            diagnostic_codes.append(
                "batter_pitch_response_unknown_pitch_retained"
            )

        pitcher_hand = _normalize_pitcher_hand(
            raw_entry.get(
                "pitcher_hand"
            )
        )
        count_context = _normalize_count_context(
            raw_entry.get(
                "count_context"
            )
        )

        entry_key = (
            canonical_pitch_id,
            pitcher_hand,
            count_context,
        )

        if entry_key in seen_keys:
            validation_errors.append(
                "batter_pitch_response_duplicate_entry"
            )
            continue

        seen_keys.add(
            entry_key
        )

        canonical = CANONICAL_PITCHES[
            canonical_pitch_id
        ]

        try:
            pitch_count = int(
                raw_entry.get(
                    "pitch_count",
                    0,
                )
            )
        except (TypeError, ValueError):
            pitch_count = -1

        if pitch_count < 0:
            validation_errors.append(
                "batter_pitch_response_pitch_count_invalid"
            )
            pitch_count = 0

        total_pitch_count += pitch_count

        swing_count = _nonnegative_int_or_none(
            raw_entry.get(
                "swing_count"
            )
        )
        contact_count = _nonnegative_int_or_none(
            raw_entry.get(
                "contact_count"
            )
        )
        batted_ball_count = _nonnegative_int_or_none(
            raw_entry.get(
                "batted_ball_count"
            )
        )

        for field_name, field_value in (
            (
                "swing_count",
                swing_count,
            ),
            (
                "contact_count",
                contact_count,
            ),
            (
                "batted_ball_count",
                batted_ball_count,
            ),
        ):
            if (
                raw_entry.get(field_name)
                is not None
                and field_value is None
            ):
                validation_errors.append(
                    f"batter_pitch_response_{field_name}_invalid"
                )

        if (
            swing_count is not None
            and swing_count > pitch_count
        ):
            validation_errors.append(
                "batter_pitch_response_swing_count_exceeds_pitch_count"
            )

        if (
            contact_count is not None
            and swing_count is not None
            and contact_count > swing_count
        ):
            validation_errors.append(
                "batter_pitch_response_contact_count_exceeds_swing_count"
            )

        if (
            batted_ball_count is not None
            and contact_count is not None
            and batted_ball_count > contact_count
        ):
            validation_errors.append(
                "batter_pitch_response_batted_ball_count_exceeds_contact_count"
            )

        parsed_rates: dict[
            str,
            float | None,
        ] = {}

        entry_diagnostics: list[str] = []

        for field_name in rate_fields:
            parsed_value = _rate_or_none(
                raw_entry.get(
                    field_name
                )
            )
            parsed_rates[field_name] = (
                parsed_value
            )

            if (
                raw_entry.get(field_name)
                is not None
                and parsed_value is None
            ):
                validation_errors.append(
                    f"batter_pitch_response_{field_name}_invalid"
                )
                entry_diagnostics.append(
                    f"batter_pitch_response_{field_name}_invalid"
                )

        if (
            parsed_rates["whiff_rate"]
            is not None
            and (
                swing_count is None
                or swing_count == 0
            )
        ):
            validation_errors.append(
                "batter_pitch_response_whiff_without_swing_support"
            )

        if (
            parsed_rates["contact_rate"]
            is not None
            and (
                swing_count is None
                or swing_count == 0
            )
        ):
            validation_errors.append(
                "batter_pitch_response_contact_without_swing_support"
            )

        batted_ball_rates = [
            parsed_rates[
                "ground_ball_rate"
            ],
            parsed_rates[
                "line_drive_rate"
            ],
            parsed_rates[
                "fly_ball_rate"
            ],
            parsed_rates[
                "popup_rate"
            ],
        ]

        if (
            any(
                value is not None
                for value in batted_ball_rates
            )
            and (
                batted_ball_count is None
                or batted_ball_count == 0
            )
        ):
            validation_errors.append(
                "batter_pitch_response_batted_ball_rates_without_support"
            )

        batted_ball_rate_total = sum(
            value or 0.0
            for value in batted_ball_rates
        )

        if (
            batted_ball_rate_total
            > 1.0
            + BATTED_BALL_RATE_SUM_TOLERANCE
        ):
            validation_errors.append(
                "batter_pitch_response_batted_ball_rate_total_invalid"
            )

        if pitcher_hand == "U":
            entry_diagnostics.append(
                "batter_pitch_response_pitcher_hand_unknown"
            )

        if count_context == "unknown":
            entry_diagnostics.append(
                "batter_pitch_response_count_context_unknown"
            )

        if (
            pitch_count
            < ENTRY_MINIMUM_PITCH_COUNT
        ):
            entry_diagnostics.append(
                "batter_pitch_response_entry_sample_sparse"
            )

        parsed_entries.append(
            BatterPitchTypeResponseEntry(
                canonical_pitch_id=canonical.canonical_pitch_id,
                canonical_pitch_name=canonical.canonical_name,
                canonical_family=canonical.family,
                pitcher_hand=pitcher_hand,
                count_context=count_context,
                pitch_count=pitch_count,
                swing_count=swing_count,
                contact_count=contact_count,
                batted_ball_count=batted_ball_count,
                swing_rate=parsed_rates[
                    "swing_rate"
                ],
                chase_rate=parsed_rates[
                    "chase_rate"
                ],
                zone_swing_rate=parsed_rates[
                    "zone_swing_rate"
                ],
                whiff_rate=parsed_rates[
                    "whiff_rate"
                ],
                contact_rate=parsed_rates[
                    "contact_rate"
                ],
                called_strike_plus_whiff_rate=parsed_rates[
                    "called_strike_plus_whiff_rate"
                ],
                avg_exit_velocity_mph=_positive_or_none(
                    raw_entry.get(
                        "avg_exit_velocity_mph"
                    )
                ),
                avg_launch_angle_degrees=_finite_or_none(
                    raw_entry.get(
                        "avg_launch_angle_degrees"
                    )
                ),
                hard_hit_rate=parsed_rates[
                    "hard_hit_rate"
                ],
                barrel_rate=parsed_rates[
                    "barrel_rate"
                ],
                ground_ball_rate=parsed_rates[
                    "ground_ball_rate"
                ],
                line_drive_rate=parsed_rates[
                    "line_drive_rate"
                ],
                fly_ball_rate=parsed_rates[
                    "fly_ball_rate"
                ],
                popup_rate=parsed_rates[
                    "popup_rate"
                ],
                response_index=_finite_or_none(
                    raw_entry.get(
                        "response_index"
                    )
                ),
                diagnostic_codes=_sorted_unique_strings(
                    entry_diagnostics
                ),
            )
        )

    if not parsed_entries:
        diagnostic_codes.append(
            "batter_pitch_response_source_unavailable"
        )

    parsed_entries.sort(
        key=lambda entry: (
            -entry.pitch_count,
            entry.canonical_pitch_id,
            entry.pitcher_hand,
            entry.count_context,
        )
    )

    requested_sample_pitch_count = (
        copied_payload.get(
            "sample_pitch_count"
        )
    )

    if requested_sample_pitch_count is None:
        sample_pitch_count = total_pitch_count
    else:
        try:
            sample_pitch_count = int(
                requested_sample_pitch_count
            )
        except (TypeError, ValueError):
            sample_pitch_count = -1

    if sample_pitch_count < 0:
        validation_errors.append(
            "batter_pitch_response_sample_pitch_count_invalid"
        )
        sample_pitch_count = 0

    sample_pa_raw = copied_payload.get(
        "sample_plate_appearance_count"
    )

    if sample_pa_raw is None:
        sample_pa_count = None
    else:
        try:
            sample_pa_count = int(
                sample_pa_raw
            )
        except (TypeError, ValueError):
            sample_pa_count = -1

        if sample_pa_count < 0:
            validation_errors.append(
                "batter_pitch_response_sample_pa_count_invalid"
            )
            sample_pa_count = None

    stale = False

    if (
        as_of_date is not None
        and source_timestamp is not None
    ):
        age_days = (
            as_of_date
            - source_timestamp.date()
        ).days

        if age_days < 0:
            validation_errors.append(
                "batter_pitch_response_source_timestamp_future"
            )
        elif (
            age_days
            > CURRENT_PROFILE_MAXIMUM_AGE_DAYS
        ):
            stale = True
            diagnostic_codes.append(
                "batter_pitch_response_profile_stale"
            )
    elif source_timestamp is None:
        diagnostic_codes.append(
            "batter_pitch_response_source_timestamp_missing"
        )

    if validation_errors:
        profile_status = "invalid"
        reason = "profile_invalid"
    elif not parsed_entries:
        profile_status = "unavailable"
        reason = "profile_unavailable"
    elif stale:
        profile_status = "stale"
        reason = "profile_stale"
    elif (
        sample_pitch_count
        < PROFILE_MINIMUM_PITCH_COUNT
    ):
        profile_status = "sparse"
        reason = "profile_sparse"
        diagnostic_codes.append(
            "batter_pitch_response_sample_sparse"
        )
    elif any(
        entry.pitch_count
        < ENTRY_MINIMUM_PITCH_COUNT
        for entry in parsed_entries
    ):
        profile_status = "partial"
        reason = "profile_partial"
    else:
        profile_status = "resolved"
        reason = "profile_resolved"

    return BatterPitchTypeResponseProfile(
        emitted=True,
        reason=reason,
        batter_id=(
            batter_id
            if batter_id
            else None
        ),
        batter_name=batter_name,
        batter_hand=batter_hand,
        season=(
            season
            if season > 0
            else None
        ),
        as_of_date_utc=(
            as_of_date.isoformat()
            if as_of_date is not None
            else None
        ),
        source_name=(
            source_name
            if source_name
            else None
        ),
        source_record_id=copied_payload.get(
            "source_record_id"
        ),
        source_timestamp_utc=(
            source_timestamp.isoformat()
            if source_timestamp is not None
            else None
        ),
        source_priority=source_priority,
        sample_pitch_count=sample_pitch_count,
        sample_plate_appearance_count=(
            sample_pa_count
        ),
        profile_status=profile_status,
        response_entries=tuple(
            parsed_entries
        ),
        taxonomy_version=TAXONOMY_VERSION,
        profile_version=PROFILE_VERSION,
        diagnostic_codes=_sorted_unique_strings(
            diagnostic_codes
        ),
        validation_errors=_sorted_unique_strings(
            validation_errors
        ),
    )
