"""Deterministic play resolution for BB, HBP, and HR outcomes."""

from __future__ import annotations

from dataclasses import replace
from typing import List, Optional, Tuple

from .contracts import (
    Base,
    GameState,
    PlayEvent,
    RunnerMovement,
)


class DeterministicPlayResolver:
    """Resolve plate appearances whose runner movement is deterministic."""

    SUPPORTED_EVENTS = frozenset({"bb", "hbp", "hr"})

    def resolve(
        self,
        *,
        state: GameState,
        event_type: str,
        batter_id: str,
        sequence: int,
    ) -> PlayEvent:
        normalized_event = event_type.lower().strip()

        if normalized_event not in self.SUPPORTED_EVENTS:
            raise ValueError(
                f"unsupported deterministic event_type: {event_type}"
            )
        if state.outs >= 3:
            raise ValueError("cannot resolve a plate appearance with 3 outs")
        if not batter_id:
            raise ValueError("batter_id is required")
        if batter_id in {
            runner for runner in state.bases if runner is not None
        }:
            raise ValueError("batter is already present on base")

        if normalized_event in {"bb", "hbp"}:
            return self._resolve_forced_award(
                state=state,
                event_type=normalized_event,
                batter_id=batter_id,
                sequence=sequence,
            )

        return self._resolve_home_run(
            state=state,
            batter_id=batter_id,
            sequence=sequence,
        )

    @staticmethod
    def _next_pa_state(
        state: GameState,
        *,
        bases: Tuple[Optional[str], Optional[str], Optional[str]],
        runs: int,
    ) -> GameState:
        if state.half == "top":
            away_score = state.away_score + runs
            home_score = state.home_score
        else:
            away_score = state.away_score
            home_score = state.home_score + runs

        return replace(
            state,
            bases=bases,
            away_score=away_score,
            home_score=home_score,
            batting_order_index=(state.batting_order_index + 1) % 9,
            plate_appearance_number=state.plate_appearance_number + 1,
        )

    def _resolve_forced_award(
        self,
        *,
        state: GameState,
        event_type: str,
        batter_id: str,
        sequence: int,
    ) -> PlayEvent:
        first, second, third = state.bases
        movements: List[RunnerMovement] = []
        scorers: List[str] = []

        new_first: Optional[str] = batter_id
        new_second = second
        new_third = third

        # Only contiguous occupied bases are forced by the batter award.
        if first is not None:
            movements.append(
                RunnerMovement(
                    runner_id=first,
                    start_base=Base.FIRST,
                    end_base=Base.SECOND,
                    is_forced=True,
                )
            )
            new_second = first

            if second is not None:
                movements.append(
                    RunnerMovement(
                        runner_id=second,
                        start_base=Base.SECOND,
                        end_base=Base.THIRD,
                        is_forced=True,
                    )
                )
                new_third = second

                if third is not None:
                    movements.append(
                        RunnerMovement(
                            runner_id=third,
                            start_base=Base.THIRD,
                            end_base=Base.HOME,
                            scored=True,
                            is_forced=True,
                        )
                    )
                    scorers.append(third)

        movements.append(
            RunnerMovement(
                runner_id=batter_id,
                start_base=Base.HOME,
                end_base=Base.FIRST,
                is_forced=True,
            )
        )

        state_after = self._next_pa_state(
            state,
            bases=(new_first, new_second, new_third),
            runs=len(scorers),
        )

        return PlayEvent(
            sequence=sequence,
            event_type=event_type,
            batter_id=batter_id,
            state_before=state,
            state_after=state_after,
            runner_movements=tuple(movements),
            runs_scored=tuple(scorers),
        )

    def _resolve_home_run(
        self,
        *,
        state: GameState,
        batter_id: str,
        sequence: int,
    ) -> PlayEvent:
        movements: List[RunnerMovement] = []
        scorers: List[str] = []

        for base, runner_id in (
            (Base.FIRST, state.first),
            (Base.SECOND, state.second),
            (Base.THIRD, state.third),
        ):
            if runner_id is None:
                continue
            movements.append(
                RunnerMovement(
                    runner_id=runner_id,
                    start_base=base,
                    end_base=Base.HOME,
                    scored=True,
                )
            )
            scorers.append(runner_id)

        movements.append(
            RunnerMovement(
                runner_id=batter_id,
                start_base=Base.HOME,
                end_base=Base.HOME,
                scored=True,
            )
        )
        scorers.append(batter_id)

        state_after = self._next_pa_state(
            state,
            bases=(None, None, None),
            runs=len(scorers),
        )

        return PlayEvent(
            sequence=sequence,
            event_type="hr",
            batter_id=batter_id,
            state_before=state,
            state_after=state_after,
            runner_movements=tuple(movements),
            runs_scored=tuple(scorers),
        )
