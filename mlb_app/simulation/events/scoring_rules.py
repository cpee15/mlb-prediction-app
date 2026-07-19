"""Deterministic scoring-rule contracts for canonical plays."""

from __future__ import annotations

from typing import Tuple

from .attribution import PlayAttribution
from .contracts import OutRecord, RunnerMovement


def third_out_is_force_or_batter_runner_out(
    *,
    outs_before: int,
    outs_recorded: Tuple[OutRecord, ...],
    batter_id: str,
    runner_movements: Tuple[RunnerMovement, ...],
) -> bool:
    """Return whether timing rules cancel all runs on the play."""

    if outs_before + len(outs_recorded) < 3:
        return False

    third_out_number = 3

    third_out = next(
        (
            record
            for record in outs_recorded
            if record.out_number == third_out_number
        ),
        None,
    )

    if third_out is None:
        return False

    if third_out.reason in {
        "force_out",
        "batter_runner_out_before_first",
    }:
        return True

    batter_movement = next(
        (
            movement
            for movement in runner_movements
            if movement.runner_id == batter_id
        ),
        None,
    )

    return bool(
        batter_movement
        and batter_movement.is_out
        and batter_movement.start_base.value == 0
    )


def counted_scorers(
    *,
    outs_before: int,
    outs_recorded: Tuple[OutRecord, ...],
    batter_id: str,
    runner_movements: Tuple[RunnerMovement, ...],
) -> Tuple[str, ...]:
    """Apply force and batter-runner third-out run rules."""

    if third_out_is_force_or_batter_runner_out(
        outs_before=outs_before,
        outs_recorded=outs_recorded,
        batter_id=batter_id,
        runner_movements=runner_movements,
    ):
        return ()

    return tuple(
        movement.runner_id
        for movement in runner_movements
        if movement.scored
    )
