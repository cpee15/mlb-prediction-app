"""Immutable contracts for event-driven baseball simulation."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Iterable, Optional, Tuple


class Base(IntEnum):
    """Canonical base identifiers used by runner movements."""

    HOME = 0
    FIRST = 1
    SECOND = 2
    THIRD = 3


@dataclass(frozen=True)
class GameState:
    """Immutable state required to resolve and replay a half-inning.

    Bases are stored in first/second/third order. A value of ``None``
    means the base is empty. Runner identifiers are intentionally
    opaque strings so this contract remains independent of player
    storage and projection models.
    """

    inning: int = 1
    half: str = "top"
    outs: int = 0
    bases: Tuple[Optional[str], Optional[str], Optional[str]] = (
        None,
        None,
        None,
    )
    away_score: int = 0
    home_score: int = 0
    batting_order_index: int = 0
    plate_appearance_number: int = 0

    def __post_init__(self) -> None:
        if self.inning < 1:
            raise ValueError("inning must be at least 1")
        if self.half not in {"top", "bottom"}:
            raise ValueError("half must be 'top' or 'bottom'")
        if not 0 <= self.outs <= 3:
            raise ValueError("outs must be between 0 and 3")
        if len(self.bases) != 3:
            raise ValueError("bases must contain first, second, and third")
        if self.away_score < 0 or self.home_score < 0:
            raise ValueError("scores cannot be negative")
        if self.batting_order_index < 0:
            raise ValueError("batting_order_index cannot be negative")
        if self.plate_appearance_number < 0:
            raise ValueError("plate_appearance_number cannot be negative")

        occupied = tuple(runner for runner in self.bases if runner is not None)
        if len(occupied) != len(set(occupied)):
            raise ValueError("a runner cannot occupy multiple bases")

    @property
    def first(self) -> Optional[str]:
        return self.bases[0]

    @property
    def second(self) -> Optional[str]:
        return self.bases[1]

    @property
    def third(self) -> Optional[str]:
        return self.bases[2]

    @property
    def batting_score(self) -> int:
        return self.away_score if self.half == "top" else self.home_score

    def runner_on(self, base: Base) -> Optional[str]:
        if base == Base.HOME:
            raise ValueError("home plate is not an occupiable base")
        return self.bases[int(base) - 1]


@dataclass(frozen=True)
class RunnerMovement:
    """One runner's movement caused by a single play."""

    runner_id: str
    start_base: Base
    end_base: Optional[Base]
    scored: bool = False
    is_out: bool = False
    is_forced: bool = False

    def __post_init__(self) -> None:
        if not self.runner_id:
            raise ValueError("runner_id is required")
        if self.scored and self.is_out:
            raise ValueError("a runner cannot both score and be out")
        if self.scored and self.end_base is not Base.HOME:
            raise ValueError("a scoring movement must end at home")
        if self.is_out and self.end_base is not None:
            raise ValueError("an out movement cannot occupy an ending base")
        if not self.scored and not self.is_out and self.end_base is None:
            raise ValueError(
                "a surviving non-scoring runner requires an ending base"
            )


@dataclass(frozen=True)
class OutRecord:
    """An out charged during a play."""

    runner_id: str
    out_number: int
    reason: str

    def __post_init__(self) -> None:
        if not self.runner_id:
            raise ValueError("runner_id is required")
        if not 1 <= self.out_number <= 3:
            raise ValueError("out_number must be between 1 and 3")
        if not self.reason:
            raise ValueError("reason is required")


@dataclass(frozen=True)
class PlayEvent:
    """Canonical append-only record for one resolved baseball play."""

    sequence: int
    event_type: str
    batter_id: str
    state_before: GameState
    state_after: GameState
    runner_movements: Tuple[RunnerMovement, ...] = field(default_factory=tuple)
    outs_recorded: Tuple[OutRecord, ...] = field(default_factory=tuple)
    runs_scored: Tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.sequence < 0:
            raise ValueError("sequence cannot be negative")
        if not self.event_type:
            raise ValueError("event_type is required")
        if not self.batter_id:
            raise ValueError("batter_id is required")

        if self.state_after.plate_appearance_number != (
            self.state_before.plate_appearance_number + 1
        ):
            raise ValueError(
                "a plate-appearance event must increment "
                "plate_appearance_number exactly once"
            )

        if self.state_after.batting_order_index != (
            self.state_before.batting_order_index + 1
        ) % 9:
            raise ValueError(
                "a plate-appearance event must advance batting order once"
            )

        score_delta = (
            self.state_after.away_score - self.state_before.away_score
            + self.state_after.home_score - self.state_before.home_score
        )
        if score_delta != len(self.runs_scored):
            raise ValueError(
                "score change must equal the number of recorded runs"
            )

        movement_scorers = tuple(
            movement.runner_id
            for movement in self.runner_movements
            if movement.scored
        )
        if movement_scorers != self.runs_scored:
            raise ValueError(
                "runs_scored must match scoring runner movements in order"
            )

        expected_outs = (
            self.state_after.outs - self.state_before.outs
        )
        if expected_outs != len(self.outs_recorded):
            raise ValueError(
                "outs change must equal the number of out records"
            )


@dataclass(frozen=True)
class PlayLedger:
    """Immutable append-only sequence of canonical play events."""

    initial_state: GameState
    events: Tuple[PlayEvent, ...] = field(default_factory=tuple)

    @property
    def current_state(self) -> GameState:
        if not self.events:
            return self.initial_state
        return self.events[-1].state_after

    def append(self, event: PlayEvent) -> "PlayLedger":
        expected_sequence = len(self.events)

        if event.sequence != expected_sequence:
            raise ValueError(
                f"expected event sequence {expected_sequence}, "
                f"received {event.sequence}"
            )

        if event.state_before != self.current_state:
            raise ValueError(
                "event state_before does not match ledger current_state"
            )

        return PlayLedger(
            initial_state=self.initial_state,
            events=self.events + (event,),
        )

    @classmethod
    def from_events(
        cls,
        initial_state: GameState,
        events: Iterable[PlayEvent],
    ) -> "PlayLedger":
        ledger = cls(initial_state=initial_state)
        for event in events:
            ledger = ledger.append(event)
        return ledger
