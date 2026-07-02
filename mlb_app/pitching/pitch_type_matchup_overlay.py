"""
Diagnostic-only pitcher-batter pitch-type matchup overlay.

Aligns Layer 8E pitcher arsenal profiles with Layer 8G batter pitch-type
response profiles. The output is immutable and non-authoritative.

This module does not alter:
- pitch selection or sequencing;
- swing, whiff, contact, or batted-ball probabilities;
- plate-appearance probabilities;
- simulation state, parameters, or outcomes;
- production matchup behavior.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable

from mlb_app.pitching.batter_pitch_type_response_profile import (
    BatterPitchTypeResponseEntry,
    BatterPitchTypeResponseProfile,
)
from mlb_app.pitching.pitcher_arsenal_profile import (
    PitcherArsenalEntry,
    PitcherArsenalProfile,
)


OVERLAY_VERSION = "8I-v1"

SUPPORTED_PROFILE_STATUSES = frozenset(
    {
        "resolved",
        "partial",
        "sparse",
        "stale",
        "unavailable",
        "invalid",
        "disabled",
    }
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
class PitchTypeMatchupOverlayEntry:
    canonical_pitch_id: str
    canonical_pitch_name: str
    canonical_family: str
    pitcher_hand: str
    batter_hand: str
    count_context: str
    pitch_available: bool
    pitch_usage_share: float | None
    pitcher_pitch_count: int
    batter_pitch_count: int
    coverage_status: str
    swing_rate: float | None
    chase_rate: float | None
    whiff_rate: float | None
    contact_rate: float | None
    called_strike_plus_whiff_rate: float | None
    avg_exit_velocity_mph: float | None
    avg_launch_angle_degrees: float | None
    hard_hit_rate: float | None
    barrel_rate: float | None
    command_index: float | None
    pitch_quality_index: float | None
    batter_response_index: float | None
    diagnostic_matchup_index: float | None
    diagnostic_codes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["diagnostic_codes"] = list(
            self.diagnostic_codes
        )
        return payload


@dataclass(frozen=True)
class PitchTypeMatchupOverlay:
    emitted: bool
    reason: str
    pitcher_id: str | None
    batter_id: str | None
    pitcher_hand: str | None
    batter_hand: str | None
    count_context: str | None
    as_of_date_utc: str | None
    pitcher_profile_status: str | None
    batter_profile_status: str | None
    overlay_status: str
    coverage_share: float
    matched_pitch_count: int
    unmatched_pitch_count: int
    overlay_entries: tuple[
        PitchTypeMatchupOverlayEntry,
        ...,
    ]
    pitcher_profile_version: str | None
    batter_profile_version: str | None
    overlay_version: str
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
        payload["overlay_entries"] = [
            entry.to_dict()
            for entry in self.overlay_entries
        ]
        payload["diagnostic_codes"] = list(
            self.diagnostic_codes
        )
        payload["validation_errors"] = list(
            self.validation_errors
        )
        return payload


def _sorted_unique_strings(
    values: Iterable[str],
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


def _disabled_overlay(
    pitcher_profile: PitcherArsenalProfile | None,
    batter_profile: BatterPitchTypeResponseProfile | None,
    count_context: str,
) -> PitchTypeMatchupOverlay:
    return PitchTypeMatchupOverlay(
        emitted=False,
        reason="overlay_disabled",
        pitcher_id=(
            pitcher_profile.pitcher_id
            if pitcher_profile is not None
            else None
        ),
        batter_id=(
            batter_profile.batter_id
            if batter_profile is not None
            else None
        ),
        pitcher_hand=(
            pitcher_profile.pitcher_hand
            if pitcher_profile is not None
            else None
        ),
        batter_hand=(
            batter_profile.batter_hand
            if batter_profile is not None
            else None
        ),
        count_context=count_context,
        as_of_date_utc=None,
        pitcher_profile_status=(
            pitcher_profile.profile_status
            if pitcher_profile is not None
            else None
        ),
        batter_profile_status=(
            batter_profile.profile_status
            if batter_profile is not None
            else None
        ),
        overlay_status="disabled",
        coverage_share=0.0,
        matched_pitch_count=0,
        unmatched_pitch_count=0,
        overlay_entries=(),
        pitcher_profile_version=(
            pitcher_profile.profile_version
            if pitcher_profile is not None
            else None
        ),
        batter_profile_version=(
            batter_profile.profile_version
            if batter_profile is not None
            else None
        ),
        overlay_version=OVERLAY_VERSION,
        diagnostic_codes=(
            "pitch_type_matchup_overlay_disabled",
        ),
        validation_errors=(),
    )


def _select_batter_entry(
    entries: tuple[
        BatterPitchTypeResponseEntry,
        ...,
    ],
    canonical_pitch_id: str,
    pitcher_hand: str,
    count_context: str,
) -> tuple[
    BatterPitchTypeResponseEntry | None,
    tuple[str, ...],
]:
    candidates = [
        entry
        for entry in entries
        if entry.canonical_pitch_id
        == canonical_pitch_id
    ]

    exact = [
        entry
        for entry in candidates
        if entry.pitcher_hand == pitcher_hand
        and entry.count_context == count_context
    ]

    if exact:
        return exact[0], ()

    unknown_hand = [
        entry
        for entry in candidates
        if entry.pitcher_hand == "U"
        and entry.count_context == count_context
    ]

    if unknown_hand:
        return (
            unknown_hand[0],
            (
                "pitch_type_matchup_pitcher_hand_fallback",
            ),
        )

    all_counts = [
        entry
        for entry in candidates
        if entry.pitcher_hand == pitcher_hand
        and entry.count_context == "all_counts"
    ]

    if all_counts:
        return (
            all_counts[0],
            (
                "pitch_type_matchup_count_context_fallback",
            ),
        )

    unknown_all_counts = [
        entry
        for entry in candidates
        if entry.pitcher_hand == "U"
        and entry.count_context == "all_counts"
    ]

    if unknown_all_counts:
        return (
            unknown_all_counts[0],
            (
                "pitch_type_matchup_count_context_fallback",
                "pitch_type_matchup_pitcher_hand_fallback",
            ),
        )

    return None, ()


def _coverage_status(
    pitch: PitcherArsenalEntry,
    matched_entry: BatterPitchTypeResponseEntry | None,
    fallback_codes: tuple[str, ...],
) -> str:
    if pitch.canonical_pitch_id == "UN":
        return "unknown_pitch"

    if matched_entry is None:
        return "pitcher_only"

    if (
        "pitch_type_matchup_pitcher_hand_fallback"
        in fallback_codes
    ):
        return "hand_fallback"

    if (
        "pitch_type_matchup_count_context_fallback"
        in fallback_codes
    ):
        return "context_fallback"

    return "matched"


def build_pitch_type_matchup_overlay(
    pitcher_profile: PitcherArsenalProfile | None,
    batter_profile: BatterPitchTypeResponseProfile | None,
    *,
    count_context: str = "all_counts",
    enabled: bool = False,
) -> PitchTypeMatchupOverlay:
    normalized_context = str(
        count_context or "unknown"
    ).strip().lower()

    if normalized_context not in SUPPORTED_COUNT_CONTEXTS:
        normalized_context = "unknown"

    if not enabled:
        return _disabled_overlay(
            pitcher_profile,
            batter_profile,
            normalized_context,
        )

    validation_errors: list[str] = []
    diagnostic_codes: list[str] = []

    if pitcher_profile is None:
        validation_errors.append(
            "pitch_type_matchup_pitcher_profile_unavailable"
        )

    if batter_profile is None:
        validation_errors.append(
            "pitch_type_matchup_batter_profile_unavailable"
        )

    if validation_errors:
        return PitchTypeMatchupOverlay(
            emitted=True,
            reason="overlay_unavailable",
            pitcher_id=None,
            batter_id=None,
            pitcher_hand=None,
            batter_hand=None,
            count_context=normalized_context,
            as_of_date_utc=None,
            pitcher_profile_status=None,
            batter_profile_status=None,
            overlay_status="unavailable",
            coverage_share=0.0,
            matched_pitch_count=0,
            unmatched_pitch_count=0,
            overlay_entries=(),
            pitcher_profile_version=None,
            batter_profile_version=None,
            overlay_version=OVERLAY_VERSION,
            diagnostic_codes=_sorted_unique_strings(
                diagnostic_codes
            ),
            validation_errors=_sorted_unique_strings(
                validation_errors
            ),
        )

    assert pitcher_profile is not None
    assert batter_profile is not None

    if not pitcher_profile.emitted:
        validation_errors.append(
            "pitch_type_matchup_pitcher_profile_not_emitted"
        )

    if not batter_profile.emitted:
        validation_errors.append(
            "pitch_type_matchup_batter_profile_not_emitted"
        )

    if (
        pitcher_profile.profile_status
        not in SUPPORTED_PROFILE_STATUSES
    ):
        validation_errors.append(
            "pitch_type_matchup_pitcher_profile_status_invalid"
        )

    if (
        batter_profile.profile_status
        not in SUPPORTED_PROFILE_STATUSES
    ):
        validation_errors.append(
            "pitch_type_matchup_batter_profile_status_invalid"
        )

    if not pitcher_profile.pitcher_id:
        validation_errors.append(
            "pitch_type_matchup_pitcher_identity_missing"
        )

    if not batter_profile.batter_id:
        validation_errors.append(
            "pitch_type_matchup_batter_identity_missing"
        )

    if not pitcher_profile.profile_version:
        validation_errors.append(
            "pitch_type_matchup_pitcher_profile_version_missing"
        )

    if not batter_profile.profile_version:
        validation_errors.append(
            "pitch_type_matchup_batter_profile_version_missing"
        )

    if (
        pitcher_profile.as_of_date_utc
        != batter_profile.as_of_date_utc
    ):
        diagnostic_codes.append(
            "pitch_type_matchup_profile_dates_disagree"
        )

    overlay_entries: list[
        PitchTypeMatchupOverlayEntry
    ] = []

    matched_pitch_count = 0
    unmatched_pitch_count = 0
    matched_usage = 0.0
    total_usage = 0.0
    matched_count_exposure = 0
    total_count_exposure = 0
    fallback_used = False

    for pitch in pitcher_profile.arsenal_entries:
        matched_entry, fallback_codes = (
            _select_batter_entry(
                batter_profile.response_entries,
                pitch.canonical_pitch_id,
                pitcher_profile.pitcher_hand or "U",
                normalized_context,
            )
        )

        coverage_status = _coverage_status(
            pitch,
            matched_entry,
            fallback_codes,
        )

        entry_diagnostics = list(
            fallback_codes
        )

        if pitch.canonical_pitch_id == "UN":
            entry_diagnostics.append(
                "pitch_type_matchup_unknown_pitch_retained"
            )

        if matched_entry is None:
            entry_diagnostics.append(
                "pitch_type_matchup_batter_response_missing"
            )
            unmatched_pitch_count += 1
        else:
            matched_pitch_count += 1

        if fallback_codes:
            fallback_used = True
            diagnostic_codes.extend(
                fallback_codes
            )

        if pitch.usage_share is not None:
            total_usage += pitch.usage_share

            if matched_entry is not None:
                matched_usage += pitch.usage_share

        total_count_exposure += pitch.pitch_count

        if matched_entry is not None:
            matched_count_exposure += (
                pitch.pitch_count
            )

        overlay_entries.append(
            PitchTypeMatchupOverlayEntry(
                canonical_pitch_id=(
                    pitch.canonical_pitch_id
                ),
                canonical_pitch_name=(
                    pitch.canonical_pitch_name
                ),
                canonical_family=(
                    pitch.canonical_family
                ),
                pitcher_hand=(
                    pitcher_profile.pitcher_hand
                    or "U"
                ),
                batter_hand=(
                    batter_profile.batter_hand
                    or "U"
                ),
                count_context=normalized_context,
                pitch_available=pitch.available,
                pitch_usage_share=(
                    pitch.usage_share
                ),
                pitcher_pitch_count=(
                    pitch.pitch_count
                ),
                batter_pitch_count=(
                    matched_entry.pitch_count
                    if matched_entry is not None
                    else 0
                ),
                coverage_status=coverage_status,
                swing_rate=(
                    matched_entry.swing_rate
                    if matched_entry is not None
                    else None
                ),
                chase_rate=(
                    matched_entry.chase_rate
                    if matched_entry is not None
                    else None
                ),
                whiff_rate=(
                    matched_entry.whiff_rate
                    if matched_entry is not None
                    else None
                ),
                contact_rate=(
                    matched_entry.contact_rate
                    if matched_entry is not None
                    else None
                ),
                called_strike_plus_whiff_rate=(
                    matched_entry.called_strike_plus_whiff_rate
                    if matched_entry is not None
                    else None
                ),
                avg_exit_velocity_mph=(
                    matched_entry.avg_exit_velocity_mph
                    if matched_entry is not None
                    else None
                ),
                avg_launch_angle_degrees=(
                    matched_entry.avg_launch_angle_degrees
                    if matched_entry is not None
                    else None
                ),
                hard_hit_rate=(
                    matched_entry.hard_hit_rate
                    if matched_entry is not None
                    else None
                ),
                barrel_rate=(
                    matched_entry.barrel_rate
                    if matched_entry is not None
                    else None
                ),
                command_index=pitch.command_index,
                pitch_quality_index=(
                    pitch.quality_index
                ),
                batter_response_index=(
                    matched_entry.response_index
                    if matched_entry is not None
                    else None
                ),
                diagnostic_matchup_index=None,
                diagnostic_codes=(
                    _sorted_unique_strings(
                        entry_diagnostics
                    )
                ),
            )
        )

    overlay_entries.sort(
        key=lambda entry: (
            entry.pitch_usage_share is None,
            -(
                entry.pitch_usage_share
                if entry.pitch_usage_share
                is not None
                else 0.0
            ),
            -entry.pitcher_pitch_count,
            entry.canonical_pitch_id,
        )
    )

    if total_usage > 0.0:
        coverage_share = (
            matched_usage / total_usage
        )
    elif total_count_exposure > 0:
        coverage_share = (
            matched_count_exposure
            / total_count_exposure
        )
    else:
        coverage_share = 0.0

    coverage_share = round(
        max(
            0.0,
            min(
                1.0,
                coverage_share,
            ),
        ),
        6,
    )

    source_statuses = {
        pitcher_profile.profile_status,
        batter_profile.profile_status,
    }

    if validation_errors or "invalid" in source_statuses:
        overlay_status = "invalid"
        reason = "overlay_invalid"
    elif (
        not pitcher_profile.emitted
        or not batter_profile.emitted
        or "unavailable" in source_statuses
        or "disabled" in source_statuses
    ):
        overlay_status = "unavailable"
        reason = "overlay_unavailable"
    elif "stale" in source_statuses:
        overlay_status = "stale"
        reason = "overlay_stale"
    elif coverage_share >= 0.8:
        overlay_status = "resolved"
        reason = "overlay_resolved"
    elif coverage_share >= 0.5:
        overlay_status = "partial"
        reason = "overlay_partial"
    elif coverage_share > 0.0:
        overlay_status = "sparse"
        reason = "overlay_sparse"
    else:
        overlay_status = "unavailable"
        reason = "overlay_unavailable"

    if (
        fallback_used
        and overlay_status == "resolved"
    ):
        overlay_status = "partial"
        reason = "overlay_partial"

    as_of_dates = [
        value
        for value in (
            pitcher_profile.as_of_date_utc,
            batter_profile.as_of_date_utc,
        )
        if value
    ]

    as_of_date_utc = (
        max(as_of_dates)
        if as_of_dates
        else None
    )

    return PitchTypeMatchupOverlay(
        emitted=True,
        reason=reason,
        pitcher_id=pitcher_profile.pitcher_id,
        batter_id=batter_profile.batter_id,
        pitcher_hand=pitcher_profile.pitcher_hand,
        batter_hand=batter_profile.batter_hand,
        count_context=normalized_context,
        as_of_date_utc=as_of_date_utc,
        pitcher_profile_status=(
            pitcher_profile.profile_status
        ),
        batter_profile_status=(
            batter_profile.profile_status
        ),
        overlay_status=overlay_status,
        coverage_share=coverage_share,
        matched_pitch_count=matched_pitch_count,
        unmatched_pitch_count=unmatched_pitch_count,
        overlay_entries=tuple(
            overlay_entries
        ),
        pitcher_profile_version=(
            pitcher_profile.profile_version
        ),
        batter_profile_version=(
            batter_profile.profile_version
        ),
        overlay_version=OVERLAY_VERSION,
        diagnostic_codes=_sorted_unique_strings(
            diagnostic_codes
        ),
        validation_errors=_sorted_unique_strings(
            validation_errors
        ),
    )
