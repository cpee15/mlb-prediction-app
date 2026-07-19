"""Construct canonical state transitions from explicit play records."""

from __future__ import annotations

from dataclasses import replace
from typing import Tuple

from .contracts import (
    Base,
    GameState,
    OutRecord,
    PlayEvent,
    RunnerMovement,
)
from .event_validation import (
    validate_resolved_play_components,
)
from .attribution import PlayAttribution
from .scoring_rules import counted_scorers


def build_play_event(
    *,
    sequence: int,
    event_type: str,
    batter_id: str,
    state_before: GameState,
    runner_movements: Tuple[RunnerMovement, ...],
    outs_recorded: Tuple[OutRecord, ...] = (),
    attribution: PlayAttribution = PlayAttribution(),
) -> PlayEvent:
    """Build and validate a canonical play from explicit records."""

    if state_before.outs >= 3:
        raise ValueError(
            "cannot resolve a play after three outs"
        )

    runs_scored = counted_scorers(
        outs_before=state_before.outs,
        outs_recorded=outs_recorded,
        batter_id=batter_id,
        runner_movements=runner_movements,
    )

    counted_movements = tuple(
        movement
        for movement in runner_movements
        if (
            not movement.scored
            or movement.runner_id in runs_scored
        )
    )

    bases = [None, None, None]

    for movement in counted_movements:
        if movement.scored or movement.is_out:
            continue

        if movement.end_base is None:
            raise ValueError(
                "surviving movement requires an ending base"
            )

        if movement.end_base is Base.HOME:
            raise ValueError(
                "non-scoring movement cannot end at home"
            )

        index = int(movement.end_base) - 1

        if bases[index] is not None:
            raise ValueError(
                "multiple runners cannot occupy one base"
            )

        bases[index] = movement.runner_id

    runs = len(runs_scored)

    if state_before.half == "top":
        away_score = state_before.away_score + runs
        home_score = state_before.home_score
    else:
        away_score = state_before.away_score
        home_score = state_before.home_score + runs

    state_after = replace(
        state_before,
        outs=min(
            3,
            state_before.outs + len(outs_recorded),
        ),
        bases=tuple(bases),
        away_score=away_score,
        home_score=home_score,
        batting_order_index=(
            state_before.batting_order_index + 1
        ) % 9,
        plate_appearance_number=(
            state_before.plate_appearance_number + 1
        ),
    )

    validate_resolved_play_components(
        state_before=state_before,
        state_after=state_after,
        runner_movements=counted_movements,
        outs_recorded=outs_recorded,
        runs_scored=runs_scored,
    )

    return PlayEvent(
        sequence=sequence,
        event_type=event_type,
        batter_id=batter_id,
        state_before=state_before,
        state_after=state_after,
        runner_movements=counted_movements,
        outs_recorded=outs_recorded,
        runs_scored=runs_scored,
        attribution=attribution,
    )
