"""Construct canonical non-plate-appearance baserunning events."""

from __future__ import annotations

from dataclasses import replace
from typing import Optional, Tuple

from .contracts import (
    Base,
    GameState,
    OutRecord,
    PlayEvent,
    RunnerMovement,
)
from .event_validation import validate_resolved_play_components


_SUPPORTED_EVENT_TYPES = frozenset(
    {
        "stolen_base",
        "caught_stealing",
    }
)

_SUPPORTED_TRANSITIONS = frozenset(
    {
        (Base.FIRST, Base.SECOND),
        (Base.SECOND, Base.THIRD),
    }
)


def build_baserunning_event(
    *,
    sequence: int,
    event_type: str,
    batter_id: str,
    runner_id: str,
    state_before: GameState,
    origin_base: Base,
    target_base: Base,
    pitcher_id: Optional[str] = None,
) -> PlayEvent:
    """Resolve one deterministic steal or caught-stealing event."""

    if state_before.outs >= 3:
        raise ValueError(
            "cannot resolve baserunning after three outs"
        )

    if event_type not in _SUPPORTED_EVENT_TYPES:
        raise ValueError(
            f"unsupported baserunning event_type: {event_type}"
        )

    if (
        origin_base,
        target_base,
    ) not in _SUPPORTED_TRANSITIONS:
        raise ValueError(
            "unsupported baserunning base transition"
        )

    if state_before.runner_on(origin_base) != runner_id:
        raise ValueError(
            "runner does not occupy origin base"
        )

    if state_before.runner_on(target_base) is not None:
        raise ValueError(
            "target base must be unoccupied"
        )

    caught_stealing = event_type == "caught_stealing"
    movements = _build_movements(
        state_before=state_before,
        runner_id=runner_id,
        origin_base=origin_base,
        target_base=target_base,
        caught_stealing=caught_stealing,
    )

    outs_recorded: Tuple[OutRecord, ...] = ()
    if caught_stealing:
        outs_recorded = (
            OutRecord(
                runner_id=runner_id,
                out_number=state_before.outs + 1,
                reason="caught_stealing",
            ),
        )

    bases = [None, None, None]
    for movement in movements:
        if movement.is_out:
            continue

        if movement.end_base is None:
            raise ValueError(
                "surviving runner requires an ending base"
            )

        bases[int(movement.end_base) - 1] = (
            movement.runner_id
        )

    state_after = replace(
        state_before,
        outs=state_before.outs + len(outs_recorded),
        bases=tuple(bases),
    )

    validate_resolved_play_components(
        state_before=state_before,
        state_after=state_after,
        runner_movements=movements,
        outs_recorded=outs_recorded,
        runs_scored=(),
    )

    return PlayEvent(
        sequence=sequence,
        event_type=event_type,
        batter_id=batter_id,
        pitcher_id=pitcher_id,
        state_before=state_before,
        state_after=state_after,
        runner_movements=movements,
        outs_recorded=outs_recorded,
        is_plate_appearance=False,
    )


def _build_movements(
    *,
    state_before: GameState,
    runner_id: str,
    origin_base: Base,
    target_base: Base,
    caught_stealing: bool,
) -> Tuple[RunnerMovement, ...]:
    movements = []

    for base in (
        Base.FIRST,
        Base.SECOND,
        Base.THIRD,
    ):
        occupant = state_before.runner_on(base)
        if occupant is None:
            continue

        if occupant == runner_id:
            movements.append(
                RunnerMovement(
                    runner_id=runner_id,
                    start_base=origin_base,
                    end_base=(
                        None
                        if caught_stealing
                        else target_base
                    ),
                    is_out=caught_stealing,
                )
            )
            continue

        movements.append(
            RunnerMovement(
                runner_id=occupant,
                start_base=base,
                end_base=base,
            )
        )

    return tuple(movements)
