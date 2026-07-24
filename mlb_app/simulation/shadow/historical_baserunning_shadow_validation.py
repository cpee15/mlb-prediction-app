"""Collect aligned historical baserunning shadow validations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Tuple

from .baserunning_output_validation import (
    validate_canonical_baserunning_shadow_outputs,
)
from .historical_baserunning_game_materialization import (
    CanonicalHistoricalBaserunningShadowGame,
)
from .mlb_play_by_play_baserunning_source import (
    CanonicalMlbPlayByPlayBaserunningSnapshot,
)
from .production_execution import (
    CanonicalProductionShadowExecution,
)


CANONICAL_HISTORICAL_BASERUNNING_SHADOW_COLLECTION_VERSION = (
    "canonical_historical_baserunning_shadow_collection_v1"
)


@dataclass(frozen=True)
class CanonicalHistoricalBaserunningExecutionGame:
    game_pk: int
    game_date: str
    execution: CanonicalProductionShadowExecution

    def __post_init__(self) -> None:
        if (
            not isinstance(self.game_pk, int)
            or isinstance(self.game_pk, bool)
            or self.game_pk <= 0
        ):
            raise ValueError(
                "game_pk must be a positive integer"
            )

        if not isinstance(self.game_date, str):
            raise TypeError(
                "game_date must be a string"
            )

        parsed_date = date.fromisoformat(
            self.game_date
        )
        if parsed_date.isoformat() != self.game_date:
            raise ValueError(
                "game_date must use ISO format"
            )

        if not isinstance(
            self.execution,
            CanonicalProductionShadowExecution,
        ):
            raise TypeError(
                "execution must be "
                "CanonicalProductionShadowExecution"
            )


def collect_historical_baserunning_shadow_validations(
    *,
    execution_games: Tuple[
        CanonicalHistoricalBaserunningExecutionGame,
        ...,
    ],
    observed: CanonicalMlbPlayByPlayBaserunningSnapshot,
) -> Tuple[
    CanonicalHistoricalBaserunningShadowGame,
    ...,
]:
    """
    Validate one production-shaped shadow execution per completed game.

    Exact game/date coverage is required. Failed, blocked, unavailable, or
    incomplete executions do not become historical calibration evidence.
    """

    if not isinstance(execution_games, tuple):
        raise TypeError(
            "execution_games must be a tuple"
        )
    if not execution_games:
        raise ValueError(
            "execution_games must contain records"
        )

    for value in execution_games:
        if not isinstance(
            value,
            CanonicalHistoricalBaserunningExecutionGame,
        ):
            raise TypeError(
                "execution_games must contain "
                "CanonicalHistoricalBaserunningExecutionGame"
            )

    if not isinstance(
        observed,
        CanonicalMlbPlayByPlayBaserunningSnapshot,
    ):
        raise TypeError(
            "observed must be "
            "CanonicalMlbPlayByPlayBaserunningSnapshot"
        )

    executions_by_id = {}
    for value in execution_games:
        if value.game_pk in executions_by_id:
            raise ValueError(
                "historical execution game identifiers "
                "must be unique"
            )
        executions_by_id[value.game_pk] = value

    observed_by_id = {
        value.game_pk: value
        for value in observed.games
    }

    if set(executions_by_id) != set(observed_by_id):
        raise ValueError(
            "historical executions must exactly match "
            "observed play-by-play games"
        )

    collected = []
    for game_pk in sorted(
        executions_by_id,
        key=lambda value: (
            executions_by_id[value].game_date,
            value,
        ),
    ):
        execution_game = executions_by_id[game_pk]
        observed_game = observed_by_id[game_pk]

        if (
            execution_game.game_date
            != observed_game.game_date
        ):
            raise ValueError(
                "historical execution game_date must "
                "match observed official game_date"
            )

        validation = (
            validate_canonical_baserunning_shadow_outputs(
                execution_game.execution
            )
        )

        if not validation.ready:
            raise ValueError(
                "historical baserunning shadow validation "
                f"unavailable for game_pk {game_pk}: "
                + (
                    validation.error_message
                    or validation.status
                )
            )

        collected.append(
            CanonicalHistoricalBaserunningShadowGame(
                game_pk=game_pk,
                game_date=execution_game.game_date,
                validation=validation,
            )
        )

    return tuple(collected)
