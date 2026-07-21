"""Legal destination enumeration for baseline runner advancement."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional, Tuple

from .batted_ball import BattedBallContext, BattedBallType
from .contracts import Base, GameState, RunnerMovement


SUPPORTED_ADVANCEMENT_OUTCOMES = frozenset(
    {
        "single",
        "double",
        "triple",
        "out",
    }
)


@dataclass(frozen=True)
class LegalRunnerDestinations:
    """Legal destinations available to one runner."""

    runner_id: str
    start_base: Base
    destinations: Tuple[Base, ...]

    def __post_init__(self) -> None:
        if not self.runner_id:
            raise ValueError("runner_id is required")
        if not self.destinations:
            raise ValueError(
                "at least one legal destination is required"
            )
        if len(self.destinations) != len(set(self.destinations)):
            raise ValueError(
                "legal destinations cannot contain duplicates"
            )


def enumerate_legal_runner_destinations(
    *,
    state: GameState,
    batter_id: str,
    primary_outcome: str,
    context: BattedBallContext,
) -> Tuple[LegalRunnerDestinations, ...]:
    """Enumerate destinations without selecting probabilities."""

    if not batter_id:
        raise ValueError("batter_id is required")

    outcome = primary_outcome.strip().lower()
    if outcome not in SUPPORTED_ADVANCEMENT_OUTCOMES:
        raise ValueError(
            f"unsupported advancement outcome: {primary_outcome}"
        )

    legal = []

    if state.third is not None:
        destinations = _third_base_destinations(
            outcome=outcome,
            context=context,
        )
        legal.append(
            LegalRunnerDestinations(
                runner_id=state.third,
                start_base=Base.THIRD,
                destinations=destinations,
            )
        )

    if state.second is not None:
        destinations = _second_base_destinations(
            outcome=outcome,
            context=context,
        )
        legal.append(
            LegalRunnerDestinations(
                runner_id=state.second,
                start_base=Base.SECOND,
                destinations=destinations,
            )
        )

    if state.first is not None:
        destinations = _first_base_destinations(
            outcome=outcome,
        )
        legal.append(
            LegalRunnerDestinations(
                runner_id=state.first,
                start_base=Base.FIRST,
                destinations=destinations,
            )
        )

    if outcome == "single":
        legal.append(
            LegalRunnerDestinations(
                runner_id=batter_id,
                start_base=Base.HOME,
                destinations=(Base.FIRST,),
            )
        )
    elif outcome == "double":
        legal.append(
            LegalRunnerDestinations(
                runner_id=batter_id,
                start_base=Base.HOME,
                destinations=(Base.SECOND,),
            )
        )
    elif outcome == "triple":
        legal.append(
            LegalRunnerDestinations(
                runner_id=batter_id,
                start_base=Base.HOME,
                destinations=(Base.THIRD,),
            )
        )

    return tuple(legal)


def validate_runner_movements(
    movements: Tuple[RunnerMovement, ...],
) -> None:
    """Reject duplicate runners and occupied-base collisions."""

    runner_ids = tuple(
        movement.runner_id
        for movement in movements
    )
    if len(runner_ids) != len(set(runner_ids)):
        raise ValueError(
            "a runner cannot appear in multiple movements"
        )

    occupied_destinations = tuple(
        movement.end_base
        for movement in movements
        if (
            movement.end_base is not None
            and movement.end_base is not Base.HOME
            and not movement.is_out
        )
    )

    if len(occupied_destinations) != len(
        set(occupied_destinations)
    ):
        raise ValueError(
            "multiple runners cannot occupy the same base"
        )


def _third_base_destinations(
    *,
    outcome: str,
    context: BattedBallContext,
) -> Tuple[Base, ...]:
    if outcome in {
        "single",
        "double",
        "triple",
    }:
        return (Base.HOME,)

    if context.batted_ball_type in {
        BattedBallType.FLY_BALL,
        BattedBallType.LINE_DRIVE,
        BattedBallType.POPUP,
        BattedBallType.GROUND_BALL,
    }:
        return (Base.THIRD, Base.HOME)

    return (Base.THIRD,)


def _second_base_destinations(
    *,
    outcome: str,
    context: BattedBallContext,
) -> Tuple[Base, ...]:
    if outcome == "single":
        return (Base.THIRD, Base.HOME)
    if outcome in {"double", "triple"}:
        return (Base.HOME,)

    if context.batted_ball_type in {
        BattedBallType.FLY_BALL,
        BattedBallType.LINE_DRIVE,
        BattedBallType.POPUP,
        BattedBallType.GROUND_BALL,
    }:
        return (Base.SECOND, Base.THIRD)

    return (Base.SECOND,)


def _first_base_destinations(
    *,
    outcome: str,
) -> Tuple[Base, ...]:
    if outcome == "single":
        return (Base.SECOND, Base.THIRD)
    if outcome == "double":
        return (Base.THIRD, Base.HOME)
    if outcome == "triple":
        return (Base.HOME,)

    # Force plays and fielder's choices are Layer 10E concerns.
    return (Base.FIRST,)
