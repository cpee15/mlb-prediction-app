"""Deterministic multi-out and scoring-rule play resolution."""

from __future__ import annotations

from .contracts import (
    Base,
    GameState,
    OutRecord,
    PlayEvent,
    RunnerMovement,
)
from .play_resolution import build_play_event
from .attribution import (
    ErrorType,
    PlayAttribution,
    SacrificeType,
)


class MultiOutPlayResolver:
    """Resolve explicitly selected scoring-rule play families."""

    SUPPORTED_EVENTS = frozenset(
        {
            "ground_ball_double_play",
            "ground_ball_fielders_choice",
            "caught_fly",
            "sacrifice_fly",
            "sacrifice_bunt",
            "reached_on_error",
        }
    )

    def resolve(
        self,
        *,
        state: GameState,
        event_type: str,
        batter_id: str,
        sequence: int,
        error_fielder_id: str | None = None,
        error_type: ErrorType | None = None,
    ) -> PlayEvent:
        normalized = event_type.strip().lower()

        if normalized not in self.SUPPORTED_EVENTS:
            raise ValueError(
                f"unsupported multi-out event_type: {event_type}"
            )

        if not batter_id:
            raise ValueError("batter_id is required")

        if state.outs >= 3:
            raise ValueError(
                "cannot resolve a play after three outs"
            )

        if normalized == "ground_ball_double_play":
            return self._ground_ball_double_play(
                state=state,
                batter_id=batter_id,
                sequence=sequence,
            )

        if normalized == "ground_ball_fielders_choice":
            return self._fielders_choice(
                state=state,
                batter_id=batter_id,
                sequence=sequence,
            )

        if normalized == "caught_fly":
            return self._caught_fly(
                state=state,
                batter_id=batter_id,
                sequence=sequence,
                sacrifice=False,
            )

        if normalized == "sacrifice_fly":
            return self._caught_fly(
                state=state,
                batter_id=batter_id,
                sequence=sequence,
                sacrifice=True,
            )

        if normalized == "sacrifice_bunt":
            return self._sacrifice_bunt(
                state=state,
                batter_id=batter_id,
                sequence=sequence,
            )

        return self._reached_on_error(
            state=state,
            batter_id=batter_id,
            sequence=sequence,
            error_fielder_id=error_fielder_id,
            error_type=error_type,
        )

    @staticmethod
    def _ground_ball_double_play(
        *,
        state: GameState,
        batter_id: str,
        sequence: int,
    ) -> PlayEvent:
        if state.first is None:
            raise ValueError(
                "ground-ball double play requires a runner on first"
            )

        if state.outs >= 2:
            raise ValueError(
                "double play requires fewer than two outs"
            )

        movements = []

        if state.third is not None:
            movements.append(
                RunnerMovement(
                    runner_id=state.third,
                    start_base=Base.THIRD,
                    end_base=Base.HOME,
                    scored=True,
                )
            )

        if state.second is not None:
            movements.append(
                RunnerMovement(
                    runner_id=state.second,
                    start_base=Base.SECOND,
                    end_base=Base.THIRD,
                )
            )

        movements.extend(
            [
                RunnerMovement(
                    runner_id=state.first,
                    start_base=Base.FIRST,
                    end_base=None,
                    is_out=True,
                    is_forced=True,
                ),
                RunnerMovement(
                    runner_id=batter_id,
                    start_base=Base.HOME,
                    end_base=None,
                    is_out=True,
                ),
            ]
        )

        outs = (
            OutRecord(
                runner_id=state.first,
                out_number=state.outs + 1,
                reason="force_out",
            ),
            OutRecord(
                runner_id=batter_id,
                out_number=state.outs + 2,
                reason="batter_runner_out_before_first",
            ),
        )

        return build_play_event(
            sequence=sequence,
            event_type="ground_ball_double_play",
            batter_id=batter_id,
            state_before=state,
            runner_movements=tuple(movements),
            outs_recorded=outs,
        )

    @staticmethod
    def _fielders_choice(
        *,
        state: GameState,
        batter_id: str,
        sequence: int,
    ) -> PlayEvent:
        if state.first is None:
            raise ValueError(
                "fielder's choice requires a runner on first"
            )

        movements = []

        if state.third is not None:
            movements.append(
                RunnerMovement(
                    runner_id=state.third,
                    start_base=Base.THIRD,
                    end_base=Base.HOME,
                    scored=True,
                )
            )

        if state.second is not None:
            movements.append(
                RunnerMovement(
                    runner_id=state.second,
                    start_base=Base.SECOND,
                    end_base=Base.THIRD,
                    is_forced=True,
                )
            )

        movements.extend(
            [
                RunnerMovement(
                    runner_id=state.first,
                    start_base=Base.FIRST,
                    end_base=None,
                    is_out=True,
                    is_forced=True,
                ),
                RunnerMovement(
                    runner_id=batter_id,
                    start_base=Base.HOME,
                    end_base=Base.FIRST,
                    is_forced=True,
                ),
            ]
        )

        outs = (
            OutRecord(
                runner_id=state.first,
                out_number=state.outs + 1,
                reason="force_out",
            ),
        )

        return build_play_event(
            sequence=sequence,
            event_type="ground_ball_fielders_choice",
            batter_id=batter_id,
            state_before=state,
            runner_movements=tuple(movements),
            outs_recorded=outs,
        )

    @staticmethod
    def _caught_fly(
        *,
        state: GameState,
        batter_id: str,
        sequence: int,
        sacrifice: bool,
    ) -> PlayEvent:
        movements = []

        if state.first is not None:
            movements.append(
                RunnerMovement(
                    runner_id=state.first,
                    start_base=Base.FIRST,
                    end_base=Base.FIRST,
                )
            )

        if state.second is not None:
            movements.append(
                RunnerMovement(
                    runner_id=state.second,
                    start_base=Base.SECOND,
                    end_base=Base.SECOND,
                )
            )

        scoring_runner = (
            state.third
            if sacrifice and state.outs < 2
            else None
        )

        if state.third is not None:
            movements.append(
                RunnerMovement(
                    runner_id=state.third,
                    start_base=Base.THIRD,
                    end_base=(
                        Base.HOME
                        if scoring_runner
                        else Base.THIRD
                    ),
                    scored=bool(scoring_runner),
                )
            )

        movements.append(
            RunnerMovement(
                runner_id=batter_id,
                start_base=Base.HOME,
                end_base=None,
                is_out=True,
            )
        )

        outs = (
            OutRecord(
                runner_id=batter_id,
                out_number=state.outs + 1,
                reason="caught_fly",
            ),
        )

        attribution = PlayAttribution(
            rbi_credited_to=(
                batter_id
                if scoring_runner
                else None
            ),
            rbi_count=1 if scoring_runner else 0,
            sacrifice_type=(
                SacrificeType.FLY
                if sacrifice and scoring_runner
                else None
            ),
        )

        return build_play_event(
            sequence=sequence,
            event_type=(
                "sacrifice_fly"
                if sacrifice
                else "caught_fly"
            ),
            batter_id=batter_id,
            state_before=state,
            runner_movements=tuple(movements),
            outs_recorded=outs,
            attribution=attribution,
        )

    @staticmethod
    def _sacrifice_bunt(
        *,
        state: GameState,
        batter_id: str,
        sequence: int,
    ) -> PlayEvent:
        if state.outs >= 2:
            raise ValueError(
                "sacrifice bunt requires fewer than two outs"
            )

        movements = []

        if state.third is not None:
            movements.append(
                RunnerMovement(
                    runner_id=state.third,
                    start_base=Base.THIRD,
                    end_base=Base.HOME,
                    scored=True,
                )
            )

        if state.second is not None:
            movements.append(
                RunnerMovement(
                    runner_id=state.second,
                    start_base=Base.SECOND,
                    end_base=Base.THIRD,
                    is_forced=True,
                )
            )

        if state.first is not None:
            movements.append(
                RunnerMovement(
                    runner_id=state.first,
                    start_base=Base.FIRST,
                    end_base=Base.SECOND,
                    is_forced=True,
                )
            )

        movements.append(
            RunnerMovement(
                runner_id=batter_id,
                start_base=Base.HOME,
                end_base=None,
                is_out=True,
            )
        )

        outs = (
            OutRecord(
                runner_id=batter_id,
                out_number=state.outs + 1,
                reason="sacrifice_bunt",
            ),
        )

        scored = state.third is not None

        attribution = PlayAttribution(
            rbi_credited_to=batter_id if scored else None,
            rbi_count=1 if scored else 0,
            sacrifice_type=SacrificeType.BUNT,
        )

        return build_play_event(
            sequence=sequence,
            event_type="sacrifice_bunt",
            batter_id=batter_id,
            state_before=state,
            runner_movements=tuple(movements),
            outs_recorded=outs,
            attribution=attribution,
        )

    @staticmethod
    def _reached_on_error(
        *,
        state: GameState,
        batter_id: str,
        sequence: int,
        error_fielder_id: str | None,
        error_type: ErrorType | None,
    ) -> PlayEvent:
        if not error_fielder_id or error_type is None:
            raise ValueError(
                "reached_on_error requires error attribution"
            )

        movements = []

        if state.third is not None:
            movements.append(
                RunnerMovement(
                    runner_id=state.third,
                    start_base=Base.THIRD,
                    end_base=Base.HOME,
                    scored=True,
                )
            )

        if state.second is not None:
            movements.append(
                RunnerMovement(
                    runner_id=state.second,
                    start_base=Base.SECOND,
                    end_base=Base.HOME,
                    scored=True,
                )
            )

        if state.first is not None:
            movements.append(
                RunnerMovement(
                    runner_id=state.first,
                    start_base=Base.FIRST,
                    end_base=Base.SECOND,
                )
            )

        movements.append(
            RunnerMovement(
                runner_id=batter_id,
                start_base=Base.HOME,
                end_base=Base.FIRST,
            )
        )

        return build_play_event(
            sequence=sequence,
            event_type="reached_on_error",
            batter_id=batter_id,
            state_before=state,
            runner_movements=tuple(movements),
            attribution=PlayAttribution(
                error_fielder_id=error_fielder_id,
                error_type=error_type,
            ),
        )
