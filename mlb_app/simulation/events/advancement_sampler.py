"""Sample legal baseline runner advancement decisions."""

from __future__ import annotations

from dataclasses import dataclass, field
import random
from typing import Optional, Tuple

from .advancement_model import (
    AdvancementProbability,
    BaselineRunnerAdvancementModel,
)
from .advancement_version import (
    BASELINE_RUNNER_ADVANCEMENT_METADATA,
    AdvancementModelMetadata,
)
from .batted_ball import BattedBallContext, BattedBallType
from .contracts import Base, GameState, RunnerMovement
from .legal_transitions import (
    enumerate_legal_runner_destinations,
    validate_runner_movements,
)


@dataclass(frozen=True)
class RunnerAdvancementResult:
    """Resolved movement decisions plus probability provenance."""

    movements: Tuple[RunnerMovement, ...]
    probabilities_used: Tuple[
        AdvancementProbability,
        ...,
    ] = field(default_factory=tuple)
    metadata: AdvancementModelMetadata = (
        BASELINE_RUNNER_ADVANCEMENT_METADATA
    )

    def __post_init__(self) -> None:
        validate_runner_movements(self.movements)

    @property
    def runs_scored(self) -> Tuple[str, ...]:
        return tuple(
            movement.runner_id
            for movement in self.movements
            if movement.scored
        )


class BaselineRunnerAdvancementSampler:
    """Choose among legal baseline runner destinations."""

    def __init__(
        self,
        *,
        rng: Optional[random.Random] = None,
        model: Optional[
            BaselineRunnerAdvancementModel
        ] = None,
    ) -> None:
        self._rng = rng or random.Random()
        self._model = (
            model
            or BaselineRunnerAdvancementModel()
        )

    def sample(
        self,
        *,
        state: GameState,
        batter_id: str,
        primary_outcome: str,
        context: BattedBallContext,
    ) -> RunnerAdvancementResult:
        outcome = primary_outcome.strip().lower()

        enumerate_legal_runner_destinations(
            state=state,
            batter_id=batter_id,
            primary_outcome=outcome,
            context=context,
        )

        if outcome == "single":
            return self._sample_single(
                state=state,
                batter_id=batter_id,
                context=context,
            )

        if outcome == "double":
            return self._sample_double(
                state=state,
                batter_id=batter_id,
                context=context,
            )

        if outcome == "triple":
            return self._sample_triple(
                state=state,
                batter_id=batter_id,
            )

        if outcome == "out":
            return self._sample_out(
                state=state,
                context=context,
            )

        raise RuntimeError(
            "legal transition enumeration accepted "
            "an unreachable outcome"
        )

    def _sample_single(
        self,
        *,
        state: GameState,
        batter_id: str,
        context: BattedBallContext,
    ) -> RunnerAdvancementResult:
        movements = []
        decisions = []
        third_occupied = False

        if state.third is not None:
            movements.append(
                _movement(
                    runner_id=state.third,
                    start=Base.THIRD,
                    end=Base.HOME,
                )
            )

        if state.second is not None:
            decision = self._model.probability(
                "single_runner_second_scores",
                context=context,
            )
            decisions.append(decision)

            if self._roll(decision):
                movements.append(
                    _movement(
                        runner_id=state.second,
                        start=Base.SECOND,
                        end=Base.HOME,
                    )
                )
            else:
                third_occupied = True
                movements.append(
                    _movement(
                        runner_id=state.second,
                        start=Base.SECOND,
                        end=Base.THIRD,
                    )
                )

        if state.first is not None:
            decision = self._model.probability(
                "single_runner_first_to_third",
                context=context,
            )
            decisions.append(decision)

            if (
                not third_occupied
                and self._roll(decision)
            ):
                third_occupied = True
                end_base = Base.THIRD
            else:
                end_base = Base.SECOND

            movements.append(
                _movement(
                    runner_id=state.first,
                    start=Base.FIRST,
                    end=end_base,
                )
            )

        movements.append(
            _movement(
                runner_id=batter_id,
                start=Base.HOME,
                end=Base.FIRST,
                forced=True,
            )
        )

        return RunnerAdvancementResult(
            movements=tuple(movements),
            probabilities_used=tuple(decisions),
        )

    def _sample_double(
        self,
        *,
        state: GameState,
        batter_id: str,
        context: BattedBallContext,
    ) -> RunnerAdvancementResult:
        movements = []
        decisions = []

        if state.third is not None:
            movements.append(
                _movement(
                    runner_id=state.third,
                    start=Base.THIRD,
                    end=Base.HOME,
                )
            )

        if state.second is not None:
            movements.append(
                _movement(
                    runner_id=state.second,
                    start=Base.SECOND,
                    end=Base.HOME,
                )
            )

        if state.first is not None:
            decision = self._model.probability(
                "double_runner_first_scores",
                context=context,
            )
            decisions.append(decision)

            end_base = (
                Base.HOME
                if self._roll(decision)
                else Base.THIRD
            )
            movements.append(
                _movement(
                    runner_id=state.first,
                    start=Base.FIRST,
                    end=end_base,
                )
            )

        movements.append(
            _movement(
                runner_id=batter_id,
                start=Base.HOME,
                end=Base.SECOND,
                forced=True,
            )
        )

        return RunnerAdvancementResult(
            movements=tuple(movements),
            probabilities_used=tuple(decisions),
        )

    def _sample_triple(
        self,
        *,
        state: GameState,
        batter_id: str,
    ) -> RunnerAdvancementResult:
        movements = []

        for start_base, runner_id in (
            (Base.THIRD, state.third),
            (Base.SECOND, state.second),
            (Base.FIRST, state.first),
        ):
            if runner_id is None:
                continue

            movements.append(
                _movement(
                    runner_id=runner_id,
                    start=start_base,
                    end=Base.HOME,
                )
            )

        movements.append(
            _movement(
                runner_id=batter_id,
                start=Base.HOME,
                end=Base.THIRD,
                forced=True,
            )
        )

        return RunnerAdvancementResult(
            movements=tuple(movements),
        )

    def _sample_out(
        self,
        *,
        state: GameState,
        context: BattedBallContext,
    ) -> RunnerAdvancementResult:
        movements = []
        decisions = []
        third_vacated = state.third is None

        if state.third is not None:
            decision_name = (
                "groundout_runner_third_scores"
                if context.batted_ball_type
                is BattedBallType.GROUND_BALL
                else "caught_ball_runner_third_scores"
            )
            decision = self._model.probability(
                decision_name,
                context=context,
            )
            decisions.append(decision)

            if (
                state.outs < 2
                and self._roll(decision)
            ):
                third_vacated = True
                end_base = Base.HOME
            else:
                third_vacated = False
                end_base = Base.THIRD

            movements.append(
                _movement(
                    runner_id=state.third,
                    start=Base.THIRD,
                    end=end_base,
                )
            )

        if state.second is not None:
            decision_name = (
                "groundout_runner_second_to_third"
                if context.batted_ball_type
                is BattedBallType.GROUND_BALL
                else (
                    "caught_ball_runner_second_to_third"
                )
            )
            decision = self._model.probability(
                decision_name,
                context=context,
            )
            decisions.append(decision)

            if (
                state.outs < 2
                and third_vacated
                and self._roll(decision)
            ):
                end_base = Base.THIRD
            else:
                end_base = Base.SECOND

            movements.append(
                _movement(
                    runner_id=state.second,
                    start=Base.SECOND,
                    end=end_base,
                )
            )

        if state.first is not None:
            movements.append(
                _movement(
                    runner_id=state.first,
                    start=Base.FIRST,
                    end=Base.FIRST,
                )
            )

        return RunnerAdvancementResult(
            movements=tuple(movements),
            probabilities_used=tuple(decisions),
        )

    def _roll(
        self,
        decision: AdvancementProbability,
    ) -> bool:
        return (
            self._rng.random()
            < decision.probability
        )


def _movement(
    *,
    runner_id: str,
    start: Base,
    end: Base,
    forced: bool = False,
) -> RunnerMovement:
    return RunnerMovement(
        runner_id=runner_id,
        start_base=start,
        end_base=end,
        scored=end is Base.HOME,
        is_forced=forced,
    )
