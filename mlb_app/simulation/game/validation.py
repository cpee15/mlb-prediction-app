"""Validation for canonical full-game orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from .contracts import CanonicalGameResult


@dataclass(frozen=True)
class CanonicalGameValidation:
    event_sequences_contiguous: bool
    half_order_valid: bool
    score_continuity_valid: bool
    batting_order_continuity_valid: bool
    final_state_matches: bool
    completion_rule_valid: bool
    warnings: Tuple[str, ...]

    @property
    def passed(self) -> bool:
        return (
            self.event_sequences_contiguous
            and self.half_order_valid
            and self.score_continuity_valid
            and self.batting_order_continuity_valid
            and self.final_state_matches
            and self.completion_rule_valid
        )


def validate_canonical_game(
    result: CanonicalGameResult,
) -> CanonicalGameValidation:
    events = result.events

    event_sequences_contiguous = tuple(
        event.sequence for event in events
    ) == tuple(range(len(events)))

    half_order_valid = all(
        _expected_next_half(previous, current)
        for previous, current in zip(
            result.halves,
            result.halves[1:],
        )
    )

    score_continuity_valid = all(
        (
            current.initial_state.away_score
            == previous.final_state.away_score
            and current.initial_state.home_score
            == previous.final_state.home_score
        )
        for previous, current in zip(
            result.halves,
            result.halves[1:],
        )
    )

    batting_order_continuity_valid = (
        _batting_order_continuity(result)
    )

    final_state_matches = (
        result.final_state
        == result.halves[-1].final_state
    )

    completion_rule_valid = (
        _completion_rule_valid(result)
    )

    warnings = []

    if not event_sequences_contiguous:
        warnings.append(
            "noncontiguous_event_sequence"
        )
    if not half_order_valid:
        warnings.append("invalid_half_order")
    if not score_continuity_valid:
        warnings.append(
            "score_discontinuity_between_halves"
        )
    if not batting_order_continuity_valid:
        warnings.append(
            "batting_order_discontinuity"
        )
    if not final_state_matches:
        warnings.append(
            "final_state_mismatch"
        )
    if not completion_rule_valid:
        warnings.append(
            "invalid_game_completion"
        )

    return CanonicalGameValidation(
        event_sequences_contiguous=(
            event_sequences_contiguous
        ),
        half_order_valid=half_order_valid,
        score_continuity_valid=(
            score_continuity_valid
        ),
        batting_order_continuity_valid=(
            batting_order_continuity_valid
        ),
        final_state_matches=final_state_matches,
        completion_rule_valid=completion_rule_valid,
        warnings=tuple(warnings),
    )


def _expected_next_half(previous, current) -> bool:
    if previous.half == "top":
        return (
            current.inning == previous.inning
            and current.half == "bottom"
        )

    return (
        current.inning == previous.inning + 1
        and current.half == "top"
    )


def _batting_order_continuity(
    result: CanonicalGameResult,
) -> bool:
    expected = {
        "away": 0,
        "home": 0,
    }

    for half in result.halves:
        side = (
            "away"
            if half.half == "top"
            else "home"
        )

        if half.batting_order_start != expected[side]:
            return False

        expected[side] = half.batting_order_end

    return True


def _completion_rule_valid(
    result: CanonicalGameResult,
) -> bool:
    reason = result.completion_reason.value
    away = result.away_score
    home = result.home_score
    final_half = result.halves[-1]

    if reason == "extra_innings_cap_tie":
        return away == home

    if reason == "home_lead_after_top":
        return (
            final_half.half == "top"
            and home > away
        )

    if reason == "walk_off":
        return (
            final_half.half == "bottom"
            and final_half.ended_by_walk_off
            and home > away
        )

    if reason in {
        "regulation",
        "extra_innings",
    }:
        return (
            final_half.half == "bottom"
            and final_half.final_state.outs == 3
            and away != home
        )

    return False
