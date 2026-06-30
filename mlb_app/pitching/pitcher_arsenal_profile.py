"""
Diagnostic-only pitcher arsenal profile contract.

Builds deterministic, immutable pitcher arsenal profiles using the canonical
pitch taxonomy from Layer 8C.

This module does not:
- select pitches;
- alter pitch sequencing;
- alter matchup or simulation probabilities;
- alter contact quality or batted-ball outcomes;
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


PROFILE_VERSION = "8E-v1"
USAGE_TOTAL_TOLERANCE = 0.001
PROFILE_MINIMUM_PITCH_COUNT = 50
ENTRY_MINIMUM_PITCH_COUNT = 10
CURRENT_PROFILE_MAXIMUM_AGE_DAYS = 14

SUPPORTED_HANDS = frozenset({"R", "L", "U"})
SUPPORTED_ROLES = frozenset(
    {
        "starter",
        "reliever",
        "opener",
        "unknown",
    }
)


@dataclass(frozen=True)
class PitcherArsenalEntry:
    canonical_pitch_id: str
    canonical_pitch_name: str
    canonical_family: str
    available: bool
    usage_share: float | None
    pitch_count: int
    avg_velocity_mph: float | None
    avg_spin_rpm: float | None
    avg_horizontal_break_inches: float | None
    avg_vertical_break_inches: float | None
    avg_extension_feet: float | None
    zone_rate: float | None
    chase_rate: float | None
    whiff_rate: float | None
    called_strike_plus_whiff_rate: float | None
    command_index: float | None
    quality_index: float | None
    diagnostic_codes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["diagnostic_codes"] = list(
            self.diagnostic_codes
        )
        return payload


@dataclass(frozen=True)
class PitcherArsenalProfile:
    emitted: bool
    reason: str
    pitcher_id: str | None
    pitcher_name: str | None
    pitcher_hand: str | None
    pitcher_role: str | None
    season: int | None
    as_of_date_utc: str | None
    source_name: str | None
    source_record_id: str | None
    source_timestamp_utc: str | None
    source_priority: int | None
    sample_pitch_count: int
    sample_game_count: int | None
    profile_status: str
    arsenal_entries: tuple[PitcherArsenalEntry, ...]
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
    contact_quality_changed: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["arsenal_entries"] = [
            entry.to_dict()
            for entry in self.arsenal_entries
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


def _finite_or_none(value: Any) -> float | None:
    if value is None:
        return None

    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(converted):
        return None

    return converted


def _rate_or_none(value: Any) -> float | None:
    converted = _finite_or_none(value)

    if converted is None:
        return None

    if not 0.0 <= converted <= 1.0:
        return None

    return converted


def _positive_or_none(value: Any) -> float | None:
    converted = _finite_or_none(value)

    if converted is None or converted <= 0.0:
        return None

    return converted


def _nonnegative_or_none(
    value: Any,
) -> float | None:
    converted = _finite_or_none(value)

    if converted is None or converted < 0.0:
        return None

    return converted


def _normalize_hand(value: Any) -> str:
    normalized = str(value or "U").strip().upper()

    if normalized not in SUPPORTED_HANDS:
        return "U"

    return normalized


def _normalize_role(value: Any) -> str:
    normalized = str(
        value or "unknown"
    ).strip().lower()

    if normalized not in SUPPORTED_ROLES:
        return "unknown"

    return normalized


def _parse_date(value: Any) -> date | None:
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


def _source_priority(source_name: str) -> int:
    normalized = source_name.strip().lower()

    if normalized in {
        "statcast",
        "statcast_pitch_level_aggregate",
        "baseball_savant",
        "baseball savant",
    }:
        return 1

    if normalized in {
        "trusted_provider",
        "trusted_provider_pitch_level_aggregate",
        "provider",
    }:
        return 2

    if normalized in {
        "repository_cache",
        "repository_cached_pitch_profile",
        "cache",
    }:
        return 3

    if normalized in {
        "season_summary",
        "season_level_pitch_mix_summary",
    }:
        return 4

    return 5


def _disabled_profile(
    payload: Mapping[str, Any],
) -> PitcherArsenalProfile:
    return PitcherArsenalProfile(
        emitted=False,
        reason="profile_disabled",
        pitcher_id=payload.get("pitcher_id"),
        pitcher_name=payload.get("pitcher_name"),
        pitcher_hand=None,
        pitcher_role=None,
        season=None,
        as_of_date_utc=None,
        source_name=payload.get("source_name"),
        source_record_id=payload.get(
            "source_record_id"
        ),
        source_timestamp_utc=None,
        source_priority=None,
        sample_pitch_count=0,
        sample_game_count=None,
        profile_status="disabled",
        arsenal_entries=(),
        taxonomy_version=TAXONOMY_VERSION,
        profile_version=PROFILE_VERSION,
        diagnostic_codes=(
            "pitcher_arsenal_profile_disabled",
        ),
        validation_errors=(),
    )


def build_pitcher_arsenal_profile(
    payload: Mapping[str, Any],
) -> PitcherArsenalProfile:
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

    pitcher_id_raw = copied_payload.get(
        "pitcher_id"
    )
    pitcher_id = (
        str(pitcher_id_raw).strip()
        if pitcher_id_raw is not None
        else ""
    )

    if not pitcher_id:
        validation_errors.append(
            "pitcher_arsenal_pitcher_identity_missing"
        )

    pitcher_name_raw = copied_payload.get(
        "pitcher_name"
    )
    pitcher_name = (
        str(pitcher_name_raw).strip()
        if pitcher_name_raw is not None
        else None
    )

    pitcher_hand = _normalize_hand(
        copied_payload.get(
            "pitcher_hand"
        )
    )

    if pitcher_hand == "U":
        diagnostic_codes.append(
            "pitcher_arsenal_pitcher_hand_unknown"
        )

    pitcher_role = _normalize_role(
        copied_payload.get(
            "pitcher_role"
        )
    )

    if pitcher_role == "unknown":
        diagnostic_codes.append(
            "pitcher_arsenal_pitcher_role_unknown"
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
            "pitcher_arsenal_season_invalid"
        )

    as_of_date = _parse_date(
        copied_payload.get(
            "as_of_date_utc"
        )
    )

    if as_of_date is None:
        validation_errors.append(
            "pitcher_arsenal_as_of_date_invalid"
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
            "pitcher_arsenal_source_name_missing"
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
        "arsenal_entries",
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
            "pitcher_arsenal_entries_invalid"
        )

    parsed_entries: list[
        PitcherArsenalEntry
    ] = []

    seen_pitch_ids: set[str] = set()
    total_pitch_count = 0

    for raw_entry in raw_entries:
        if not isinstance(
            raw_entry,
            Mapping,
        ):
            validation_errors.append(
                "pitcher_arsenal_entry_invalid"
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
                "pitcher_arsenal_unknown_pitch_retained"
            )

        if canonical_pitch_id in seen_pitch_ids:
            validation_errors.append(
                "pitcher_arsenal_duplicate_pitch"
            )
            continue

        seen_pitch_ids.add(
            canonical_pitch_id
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
                "pitcher_arsenal_pitch_count_invalid"
            )
            pitch_count = 0

        total_pitch_count += pitch_count

        usage_share = _rate_or_none(
            raw_entry.get(
                "usage_share"
            )
        )

        entry_diagnostics: list[str] = []

        if (
            raw_entry.get("usage_share")
            is not None
            and usage_share is None
        ):
            entry_diagnostics.append(
                "pitcher_arsenal_usage_share_invalid"
            )
            validation_errors.append(
                "pitcher_arsenal_usage_share_invalid"
            )

        available = bool(
            raw_entry.get(
                "available",
                pitch_count > 0,
            )
        )

        if available and pitch_count == 0:
            entry_diagnostics.append(
                "pitcher_arsenal_available_without_pitches"
            )
            validation_errors.append(
                "pitcher_arsenal_available_without_pitches"
            )

        if (
            pitch_count
            < ENTRY_MINIMUM_PITCH_COUNT
        ):
            entry_diagnostics.append(
                "pitcher_arsenal_entry_sample_sparse"
            )

        parsed_entries.append(
            PitcherArsenalEntry(
                canonical_pitch_id=canonical.canonical_pitch_id,
                canonical_pitch_name=canonical.canonical_name,
                canonical_family=canonical.family,
                available=available,
                usage_share=usage_share,
                pitch_count=pitch_count,
                avg_velocity_mph=_positive_or_none(
                    raw_entry.get(
                        "avg_velocity_mph"
                    )
                ),
                avg_spin_rpm=_nonnegative_or_none(
                    raw_entry.get(
                        "avg_spin_rpm"
                    )
                ),
                avg_horizontal_break_inches=_finite_or_none(
                    raw_entry.get(
                        "avg_horizontal_break_inches"
                    )
                ),
                avg_vertical_break_inches=_finite_or_none(
                    raw_entry.get(
                        "avg_vertical_break_inches"
                    )
                ),
                avg_extension_feet=_positive_or_none(
                    raw_entry.get(
                        "avg_extension_feet"
                    )
                ),
                zone_rate=_rate_or_none(
                    raw_entry.get(
                        "zone_rate"
                    )
                ),
                chase_rate=_rate_or_none(
                    raw_entry.get(
                        "chase_rate"
                    )
                ),
                whiff_rate=_rate_or_none(
                    raw_entry.get(
                        "whiff_rate"
                    )
                ),
                called_strike_plus_whiff_rate=_rate_or_none(
                    raw_entry.get(
                        "called_strike_plus_whiff_rate"
                    )
                ),
                command_index=_finite_or_none(
                    raw_entry.get(
                        "command_index"
                    )
                ),
                quality_index=_finite_or_none(
                    raw_entry.get(
                        "quality_index"
                    )
                ),
                diagnostic_codes=_sorted_unique_strings(
                    entry_diagnostics
                ),
            )
        )

    if not parsed_entries:
        diagnostic_codes.append(
            "pitcher_arsenal_source_unavailable"
        )

    explicit_usage_present = any(
        entry.usage_share is not None
        for entry in parsed_entries
    )

    if (
        parsed_entries
        and not explicit_usage_present
        and total_pitch_count > 0
    ):
        parsed_entries = [
            PitcherArsenalEntry(
                **{
                    **entry.__dict__,
                    "usage_share": round(
                        entry.pitch_count
                        / total_pitch_count,
                        6,
                    ),
                }
            )
            for entry in parsed_entries
        ]
        diagnostic_codes.append(
            "pitcher_arsenal_usage_derived_from_counts"
        )

    usage_values = [
        entry.usage_share
        for entry in parsed_entries
        if entry.usage_share is not None
    ]

    usage_total = sum(
        usage_values
    )

    if (
        usage_values
        and abs(
            usage_total - 1.0
        )
        > USAGE_TOTAL_TOLERANCE
    ):
        validation_errors.append(
            "pitcher_arsenal_usage_total_invalid"
        )

    parsed_entries.sort(
        key=lambda entry: (
            entry.usage_share is None,
            -(
                entry.usage_share
                if entry.usage_share is not None
                else 0.0
            ),
            -entry.pitch_count,
            entry.canonical_pitch_id,
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
            "pitcher_arsenal_sample_pitch_count_invalid"
        )
        sample_pitch_count = 0

    sample_game_count_raw = (
        copied_payload.get(
            "sample_game_count"
        )
    )

    if sample_game_count_raw is None:
        sample_game_count = None
    else:
        try:
            sample_game_count = int(
                sample_game_count_raw
            )
        except (TypeError, ValueError):
            sample_game_count = -1

        if sample_game_count < 0:
            validation_errors.append(
                "pitcher_arsenal_sample_game_count_invalid"
            )
            sample_game_count = None

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
                "pitcher_arsenal_source_timestamp_future"
            )
        elif (
            age_days
            > CURRENT_PROFILE_MAXIMUM_AGE_DAYS
        ):
            stale = True
            diagnostic_codes.append(
                "pitcher_arsenal_profile_stale"
            )
    elif source_timestamp is None:
        diagnostic_codes.append(
            "pitcher_arsenal_source_timestamp_missing"
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
            "pitcher_arsenal_sample_sparse"
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

    return PitcherArsenalProfile(
        emitted=True,
        reason=reason,
        pitcher_id=(
            pitcher_id
            if pitcher_id
            else None
        ),
        pitcher_name=pitcher_name,
        pitcher_hand=pitcher_hand,
        pitcher_role=pitcher_role,
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
        sample_game_count=sample_game_count,
        profile_status=profile_status,
        arsenal_entries=tuple(
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
