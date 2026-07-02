"""
Diagnostic-only observability for Layer 8I pitch-type matchup overlays.

Creates deterministic immutable summary, entry, and aggregate records without
changing production, probability, matchup, or simulation behavior.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any, Iterable, Sequence

from mlb_app.pitching.pitch_type_matchup_overlay import (
    PitchTypeMatchupOverlay,
)


OBSERVABILITY_VERSION = "8K-v1"

SUPPORTED_OVERLAY_STATUSES = frozenset(
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

SUPPORTED_COVERAGE_STATUSES = frozenset(
    {
        "matched",
        "pitcher_only",
        "batter_only",
        "unknown_pitch",
        "context_fallback",
        "hand_fallback",
        "unavailable",
    }
)

TRACKED_FALLBACK_CODES = (
    "pitch_type_matchup_count_context_fallback",
    "pitch_type_matchup_pitcher_hand_fallback",
    "pitch_type_matchup_batter_response_missing",
    "pitch_type_matchup_unknown_pitch_retained",
    "pitch_type_matchup_profile_dates_disagree",
)

FALLBACK_ENTRY_CODES = frozenset(
    {
        "pitch_type_matchup_count_context_fallback",
        "pitch_type_matchup_pitcher_hand_fallback",
    }
)


@dataclass(frozen=True)
class MatchupOverlayObservation:
    observation_id: str
    pitcher_id: str | None
    batter_id: str | None
    pitcher_hand: str | None
    batter_hand: str | None
    count_context: str | None
    as_of_date_utc: str | None
    overlay_status: str
    coverage_share: float
    matched_pitch_count: int
    unmatched_pitch_count: int
    overlay_entry_count: int
    matched_usage_share: float | None
    unmatched_usage_share: float | None
    fallback_entry_count: int
    unknown_pitch_entry_count: int
    pitcher_only_entry_count: int
    pitcher_profile_status: str | None
    batter_profile_status: str | None
    pitcher_profile_version: str | None
    batter_profile_version: str | None
    overlay_version: str
    observability_version: str
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    production_authority: bool = False
    production_behavior_changed: bool = False
    simulation_behavior_changed: bool = False
    historical_outcomes_joined: bool = False
    predictive_evaluation_executed: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["diagnostic_codes"] = list(
            self.diagnostic_codes
        )
        payload["validation_errors"] = list(
            self.validation_errors
        )
        return payload


@dataclass(frozen=True)
class MatchupOverlayEntryObservation:
    observation_id: str
    entry_ordinal: int
    canonical_pitch_id: str
    canonical_pitch_name: str
    canonical_family: str
    coverage_status: str
    pitch_usage_share: float | None
    pitcher_pitch_count: int
    batter_pitch_count: int
    fallback_used: bool
    response_available: bool
    unknown_pitch: bool
    swing_rate_present: bool
    whiff_rate_present: bool
    contact_rate_present: bool
    hard_hit_rate_present: bool
    barrel_rate_present: bool
    command_index_present: bool
    pitch_quality_index_present: bool
    batter_response_index_present: bool
    diagnostic_matchup_index_present: bool
    diagnostic_codes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["diagnostic_codes"] = list(
            self.diagnostic_codes
        )
        return payload


@dataclass(frozen=True)
class MatchupOverlayObservationBundle:
    emitted: bool
    reason: str
    observability_status: str
    summary: MatchupOverlayObservation | None
    entries: tuple[
        MatchupOverlayEntryObservation,
        ...,
    ]
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    observability_version: str
    production_authority: bool = False
    production_behavior_changed: bool = False
    simulation_behavior_changed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "emitted": self.emitted,
            "reason": self.reason,
            "observability_status": (
                self.observability_status
            ),
            "summary": (
                self.summary.to_dict()
                if self.summary is not None
                else None
            ),
            "entries": [
                entry.to_dict()
                for entry in self.entries
            ],
            "diagnostic_codes": list(
                self.diagnostic_codes
            ),
            "validation_errors": list(
                self.validation_errors
            ),
            "observability_version": (
                self.observability_version
            ),
            "production_authority": (
                self.production_authority
            ),
            "production_behavior_changed": (
                self.production_behavior_changed
            ),
            "simulation_behavior_changed": (
                self.simulation_behavior_changed
            ),
        }


@dataclass(frozen=True)
class MatchupOverlayObservationAggregate:
    overlay_count: int
    emitted_overlay_count: int
    disabled_overlay_count: int
    resolved_overlay_count: int
    partial_overlay_count: int
    sparse_overlay_count: int
    stale_overlay_count: int
    unavailable_overlay_count: int
    invalid_overlay_count: int
    mean_coverage_share: float | None
    minimum_coverage_share: float | None
    maximum_coverage_share: float | None
    fallback_overlay_count: int
    pitcher_only_overlay_count: int
    unknown_pitch_overlay_count: int
    observability_version: str
    production_authority: bool = False
    production_behavior_changed: bool = False
    simulation_behavior_changed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


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


def _observation_id(
    overlay: PitchTypeMatchupOverlay,
) -> str:
    identity = {
        "pitcher_id": overlay.pitcher_id,
        "batter_id": overlay.batter_id,
        "pitcher_hand": overlay.pitcher_hand,
        "batter_hand": overlay.batter_hand,
        "count_context": overlay.count_context,
        "as_of_date_utc": overlay.as_of_date_utc,
        "overlay_version": overlay.overlay_version,
    }

    serialized = json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
    )

    digest = hashlib.sha256(
        serialized.encode("utf-8")
    ).hexdigest()[:20]

    return f"matchup-overlay-{digest}"


def _usage_shares(
    overlay: PitchTypeMatchupOverlay,
) -> tuple[float | None, float | None]:
    numeric_entries = [
        entry
        for entry in overlay.overlay_entries
        if entry.pitch_usage_share is not None
    ]

    if not numeric_entries:
        return None, None

    matched = sum(
        entry.pitch_usage_share or 0.0
        for entry in numeric_entries
        if entry.coverage_status
        not in {
            "pitcher_only",
            "unavailable",
        }
    )

    unmatched = sum(
        entry.pitch_usage_share or 0.0
        for entry in numeric_entries
        if entry.coverage_status
        in {
            "pitcher_only",
            "unavailable",
        }
    )

    return (
        round(matched, 6),
        round(unmatched, 6),
    )


def observe_pitch_type_matchup_overlay(
    overlay: PitchTypeMatchupOverlay | None,
    *,
    enabled: bool = False,
) -> MatchupOverlayObservationBundle:
    if not enabled:
        return MatchupOverlayObservationBundle(
            emitted=False,
            reason="observability_disabled",
            observability_status="disabled",
            summary=None,
            entries=(),
            diagnostic_codes=(
                "matchup_overlay_observability_disabled",
            ),
            validation_errors=(),
            observability_version=(
                OBSERVABILITY_VERSION
            ),
        )

    if overlay is None:
        return MatchupOverlayObservationBundle(
            emitted=True,
            reason="observation_invalid",
            observability_status="invalid",
            summary=None,
            entries=(),
            diagnostic_codes=(
                "matchup_overlay_observation_missing",
            ),
            validation_errors=(
                "matchup_overlay_observation_missing",
            ),
            observability_version=(
                OBSERVABILITY_VERSION
            ),
        )

    validation_errors: list[str] = []
    diagnostic_codes: list[str] = list(
        overlay.diagnostic_codes
    )

    if (
        overlay.overlay_status
        not in SUPPORTED_OVERLAY_STATUSES
    ):
        validation_errors.append(
            "matchup_overlay_status_invalid"
        )

    if not 0.0 <= overlay.coverage_share <= 1.0:
        validation_errors.append(
            "matchup_overlay_coverage_invalid"
        )

    if overlay.matched_pitch_count < 0:
        validation_errors.append(
            "matchup_overlay_matched_count_invalid"
        )

    if overlay.unmatched_pitch_count < 0:
        validation_errors.append(
            "matchup_overlay_unmatched_count_invalid"
        )

    observation_id = _observation_id(
        overlay
    )

    entry_records: list[
        MatchupOverlayEntryObservation
    ] = []

    fallback_entry_count = 0
    unknown_pitch_entry_count = 0
    pitcher_only_entry_count = 0

    for ordinal, entry in enumerate(
        overlay.overlay_entries
    ):
        entry_diagnostics = (
            _sorted_unique_strings(
                entry.diagnostic_codes
            )
        )

        fallback_used = any(
            code in FALLBACK_ENTRY_CODES
            for code in entry_diagnostics
        )

        if fallback_used:
            fallback_entry_count += 1

        unknown_pitch = (
            entry.canonical_pitch_id == "UN"
        )

        if unknown_pitch:
            unknown_pitch_entry_count += 1

        if entry.coverage_status == "pitcher_only":
            pitcher_only_entry_count += 1

        if (
            entry.coverage_status
            not in SUPPORTED_COVERAGE_STATUSES
        ):
            validation_errors.append(
                "matchup_overlay_coverage_status_invalid"
            )

        if entry.pitcher_pitch_count < 0:
            validation_errors.append(
                "matchup_overlay_pitcher_pitch_count_invalid"
            )

        if entry.batter_pitch_count < 0:
            validation_errors.append(
                "matchup_overlay_batter_pitch_count_invalid"
            )

        if (
            entry.pitch_usage_share is not None
            and not (
                0.0
                <= entry.pitch_usage_share
                <= 1.0
            )
        ):
            validation_errors.append(
                "matchup_overlay_usage_share_invalid"
            )

        response_available = (
            entry.batter_pitch_count > 0
            or any(
                value is not None
                for value in (
                    entry.swing_rate,
                    entry.whiff_rate,
                    entry.contact_rate,
                    entry.hard_hit_rate,
                    entry.barrel_rate,
                    entry.batter_response_index,
                )
            )
        )

        entry_records.append(
            MatchupOverlayEntryObservation(
                observation_id=observation_id,
                entry_ordinal=ordinal,
                canonical_pitch_id=(
                    entry.canonical_pitch_id
                ),
                canonical_pitch_name=(
                    entry.canonical_pitch_name
                ),
                canonical_family=(
                    entry.canonical_family
                ),
                coverage_status=(
                    entry.coverage_status
                ),
                pitch_usage_share=(
                    entry.pitch_usage_share
                ),
                pitcher_pitch_count=(
                    entry.pitcher_pitch_count
                ),
                batter_pitch_count=(
                    entry.batter_pitch_count
                ),
                fallback_used=fallback_used,
                response_available=(
                    response_available
                ),
                unknown_pitch=unknown_pitch,
                swing_rate_present=(
                    entry.swing_rate is not None
                ),
                whiff_rate_present=(
                    entry.whiff_rate is not None
                ),
                contact_rate_present=(
                    entry.contact_rate is not None
                ),
                hard_hit_rate_present=(
                    entry.hard_hit_rate is not None
                ),
                barrel_rate_present=(
                    entry.barrel_rate is not None
                ),
                command_index_present=(
                    entry.command_index is not None
                ),
                pitch_quality_index_present=(
                    entry.pitch_quality_index
                    is not None
                ),
                batter_response_index_present=(
                    entry.batter_response_index
                    is not None
                ),
                diagnostic_matchup_index_present=(
                    entry.diagnostic_matchup_index
                    is not None
                ),
                diagnostic_codes=entry_diagnostics,
            )
        )

    matched_usage, unmatched_usage = (
        _usage_shares(
            overlay
        )
    )

    if (
        matched_usage is not None
        and unmatched_usage is not None
        and matched_usage + unmatched_usage
        > 1.000001
    ):
        validation_errors.append(
            "matchup_overlay_usage_total_invalid"
        )

    if not overlay.emitted:
        diagnostic_codes.append(
            "matchup_overlay_not_emitted"
        )

    if not overlay.overlay_entries:
        diagnostic_codes.append(
            "matchup_overlay_entries_empty"
        )

    if validation_errors:
        observability_status = "invalid"
        reason = "observation_invalid"
    elif not overlay.emitted:
        observability_status = "partial"
        reason = "observation_status_only"
    elif not overlay.overlay_entries:
        observability_status = "empty"
        reason = "observation_empty"
    elif (
        overlay.pitcher_id is None
        or overlay.batter_id is None
        or overlay.as_of_date_utc is None
    ):
        observability_status = "partial"
        reason = "observation_partial"
    else:
        observability_status = "complete"
        reason = "observation_complete"

    summary = MatchupOverlayObservation(
        observation_id=observation_id,
        pitcher_id=overlay.pitcher_id,
        batter_id=overlay.batter_id,
        pitcher_hand=overlay.pitcher_hand,
        batter_hand=overlay.batter_hand,
        count_context=overlay.count_context,
        as_of_date_utc=overlay.as_of_date_utc,
        overlay_status=overlay.overlay_status,
        coverage_share=round(
            overlay.coverage_share,
            6,
        ),
        matched_pitch_count=(
            overlay.matched_pitch_count
        ),
        unmatched_pitch_count=(
            overlay.unmatched_pitch_count
        ),
        overlay_entry_count=len(
            entry_records
        ),
        matched_usage_share=matched_usage,
        unmatched_usage_share=unmatched_usage,
        fallback_entry_count=(
            fallback_entry_count
        ),
        unknown_pitch_entry_count=(
            unknown_pitch_entry_count
        ),
        pitcher_only_entry_count=(
            pitcher_only_entry_count
        ),
        pitcher_profile_status=(
            overlay.pitcher_profile_status
        ),
        batter_profile_status=(
            overlay.batter_profile_status
        ),
        pitcher_profile_version=(
            overlay.pitcher_profile_version
        ),
        batter_profile_version=(
            overlay.batter_profile_version
        ),
        overlay_version=overlay.overlay_version,
        observability_version=(
            OBSERVABILITY_VERSION
        ),
        diagnostic_codes=(
            _sorted_unique_strings(
                diagnostic_codes
            )
        ),
        validation_errors=(
            _sorted_unique_strings(
                validation_errors
            )
        ),
    )

    return MatchupOverlayObservationBundle(
        emitted=True,
        reason=reason,
        observability_status=(
            observability_status
        ),
        summary=summary,
        entries=tuple(entry_records),
        diagnostic_codes=(
            summary.diagnostic_codes
        ),
        validation_errors=(
            summary.validation_errors
        ),
        observability_version=(
            OBSERVABILITY_VERSION
        ),
    )


def aggregate_matchup_overlay_observations(
    bundles: Sequence[
        MatchupOverlayObservationBundle
    ],
) -> MatchupOverlayObservationAggregate:
    overlay_count = len(bundles)

    summaries = [
        bundle.summary
        for bundle in bundles
        if bundle.summary is not None
    ]

    emitted_overlay_count = sum(
        1
        for bundle in bundles
        if bundle.emitted
    )

    disabled_overlay_count = sum(
        1
        for bundle in bundles
        if bundle.observability_status
        == "disabled"
    )

    status_counts = {
        status: sum(
            1
            for summary in summaries
            if summary.overlay_status
            == status
        )
        for status in (
            "resolved",
            "partial",
            "sparse",
            "stale",
            "unavailable",
            "invalid",
        )
    }

    coverage_values = [
        summary.coverage_share
        for summary in summaries
    ]

    if coverage_values:
        mean_coverage = round(
            sum(coverage_values)
            / len(coverage_values),
            6,
        )
        minimum_coverage = min(
            coverage_values
        )
        maximum_coverage = max(
            coverage_values
        )
    else:
        mean_coverage = None
        minimum_coverage = None
        maximum_coverage = None

    fallback_overlay_count = sum(
        1
        for summary in summaries
        if summary.fallback_entry_count > 0
        or any(
            code
            in summary.diagnostic_codes
            for code in TRACKED_FALLBACK_CODES
        )
    )

    pitcher_only_overlay_count = sum(
        1
        for summary in summaries
        if summary.pitcher_only_entry_count
        > 0
    )

    unknown_pitch_overlay_count = sum(
        1
        for summary in summaries
        if summary.unknown_pitch_entry_count
        > 0
    )

    return MatchupOverlayObservationAggregate(
        overlay_count=overlay_count,
        emitted_overlay_count=(
            emitted_overlay_count
        ),
        disabled_overlay_count=(
            disabled_overlay_count
        ),
        resolved_overlay_count=(
            status_counts["resolved"]
        ),
        partial_overlay_count=(
            status_counts["partial"]
        ),
        sparse_overlay_count=(
            status_counts["sparse"]
        ),
        stale_overlay_count=(
            status_counts["stale"]
        ),
        unavailable_overlay_count=(
            status_counts["unavailable"]
        ),
        invalid_overlay_count=(
            status_counts["invalid"]
        ),
        mean_coverage_share=mean_coverage,
        minimum_coverage_share=(
            minimum_coverage
        ),
        maximum_coverage_share=(
            maximum_coverage
        ),
        fallback_overlay_count=(
            fallback_overlay_count
        ),
        pitcher_only_overlay_count=(
            pitcher_only_overlay_count
        ),
        unknown_pitch_overlay_count=(
            unknown_pitch_overlay_count
        ),
        observability_version=(
            OBSERVABILITY_VERSION
        ),
    )


def coverage_bucket(
    coverage_share: float,
) -> str:
    if coverage_share == 0.0:
        return "coverage_0"

    if 0.0 < coverage_share < 0.5:
        return "coverage_gt_0_lt_0_5"

    if 0.5 <= coverage_share < 0.8:
        return "coverage_gte_0_5_lt_0_8"

    if 0.8 <= coverage_share < 1.0:
        return "coverage_gte_0_8_lt_1"

    return "coverage_1"
