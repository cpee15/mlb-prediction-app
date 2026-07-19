"""Cross-contract validation for canonical resolved plays."""

from __future__ import annotations

from typing import Tuple

from .contracts import (
    Base,
    GameState,
    OutRecord,
    RunnerMovement,
)
from .legal_transitions import validate_runner_movements


def validate_resolved_play_components(
    *,
    state_before: GameState,
    state_after: GameState,
    runner_movements: Tuple[RunnerMovement, ...],
    outs_recorded: Tuple[OutRecord, ...],
    runs_scored: Tuple[str, ...],
) -> None:
    """Verify that every base, out, and run change is explained."""

    validate_runner_movements(runner_movements)

    expected_out_delta = state_after.outs - state_before.outs
    if expected_out_delta != len(outs_recorded):
        raise ValueError(
            "outs delta must match explicit out records"
        )

    scoring_movements = tuple(
        movement.runner_id
        for movement in runner_movements
        if movement.scored
    )

    if runs_scored != scoring_movements:
        raise ValueError(
            "runs_scored must match scoring movements"
        )

    out_runner_ids = {
        record.runner_id
        for record in outs_recorded
    }

    movement_out_runner_ids = {
        movement.runner_id
        for movement in runner_movements
        if movement.is_out
    }

    if out_runner_ids != movement_out_runner_ids:
        raise ValueError(
            "out movements and out records must identify "
            "the same runners"
        )

    expected_bases = [None, None, None]

    for movement in runner_movements:
        if movement.scored or movement.is_out:
            continue

        if movement.end_base is Base.HOME:
            raise ValueError(
                "non-scoring runner cannot survive at home"
            )

        if movement.end_base is None:
            raise ValueError(
                "surviving runner requires an ending base"
            )

        index = int(movement.end_base) - 1

        if expected_bases[index] is not None:
            raise ValueError(
                "multiple surviving runners occupy one base"
            )

        expected_bases[index] = movement.runner_id

    if tuple(expected_bases) != state_after.bases:
        raise ValueError(
            "state_after bases are not explained by movements"
        )
