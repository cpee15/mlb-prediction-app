"""Materialize aligned historical baserunning game records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional, Tuple

from .baserunning_calibration_payload import (
    CanonicalHistoricalBaserunningGame,
)
from .baserunning_output_validation import (
    CanonicalBaserunningOutputValidation,
)
from .mlb_play_by_play_baserunning_source import (
    CanonicalMlbPlayByPlayBaserunningSnapshot,
)
from .statcast_baserunning_source import (
    CanonicalStatcastBaserunningOutcome,
)


CANONICAL_HISTORICAL_BASERUNNING_SHADOW_GAME_VERSION = (
    "canonical_historical_baserunning_shadow_game_v1"
)
CANONICAL_HISTORICAL_BASERUNNING_MATERIALIZATION_VERSION = (
    "canonical_historical_baserunning_materialization_v1"
)

CANONICAL_PLAY_BY_PLAY_BASERUNNING_MATERIALIZATION_VERSION = (
    "canonical_play_by_play_baserunning_materialization_v1"
)


@dataclass(frozen=True)
class CanonicalHistoricalBaserunningShadowGame:
    game_pk: int
    game_date: str
    validation: CanonicalBaserunningOutputValidation
    record_version: str = (
        CANONICAL_HISTORICAL_BASERUNNING_SHADOW_GAME_VERSION
    )

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
            self.validation,
            CanonicalBaserunningOutputValidation,
        ):
            raise TypeError(
                "validation must be "
                "CanonicalBaserunningOutputValidation"
            )

        if not self.validation.ready:
            raise ValueError(
                "historical shadow validation must be ready"
            )

        if self.record_version != (
            CANONICAL_HISTORICAL_BASERUNNING_SHADOW_GAME_VERSION
        ):
            raise ValueError(
                "unsupported historical baserunning "
                "shadow game version"
            )


def _outcome_identity(
    value: CanonicalStatcastBaserunningOutcome,
) -> Tuple[
    int,
    int,
    int,
    str,
    str,
    str,
    str,
]:
    if value.game_pk is None:
        raise ValueError(
            "Statcast outcome requires game_pk"
        )
    if value.at_bat_number is None:
        raise ValueError(
            "Statcast outcome requires at_bat_number"
        )
    if value.pitch_number is None:
        raise ValueError(
            "Statcast outcome requires pitch_number"
        )

    return (
        value.game_pk,
        value.at_bat_number,
        value.pitch_number,
        value.runner_id,
        value.event_type,
        value.origin_base,
        value.target_base,
    )


def materialize_historical_baserunning_game_records(
    *,
    shadow_games: Tuple[
        CanonicalHistoricalBaserunningShadowGame,
        ...,
    ],
    outcomes: Tuple[
        CanonicalStatcastBaserunningOutcome,
        ...,
    ],
    observed_source_version: str,
) -> Tuple[
    CanonicalHistoricalBaserunningGame,
    ...,
]:
    """
    Join per-game shadow validations to exact observed SB/CS outcomes.

    Games without an observed attempt remain explicit zero-activity records.
    No data is fetched, persisted, simulated, or activated.
    """

    if not isinstance(shadow_games, tuple):
        raise TypeError(
            "shadow_games must be a tuple"
        )
    if not shadow_games:
        raise ValueError(
            "shadow_games must contain records"
        )

    for value in shadow_games:
        if not isinstance(
            value,
            CanonicalHistoricalBaserunningShadowGame,
        ):
            raise TypeError(
                "shadow_games must contain "
                "CanonicalHistoricalBaserunningShadowGame"
            )

    if not isinstance(outcomes, tuple):
        raise TypeError(
            "outcomes must be a tuple"
        )

    for value in outcomes:
        if not isinstance(
            value,
            CanonicalStatcastBaserunningOutcome,
        ):
            raise TypeError(
                "outcomes must contain "
                "CanonicalStatcastBaserunningOutcome"
            )

    if (
        not isinstance(observed_source_version, str)
        or not observed_source_version.strip()
        or observed_source_version == "unavailable"
    ):
        raise ValueError(
            "observed_source_version must identify "
            "an available source"
        )

    game_ids = tuple(
        value.game_pk
        for value in shadow_games
    )
    if len(game_ids) != len(set(game_ids)):
        raise ValueError(
            "historical shadow game identifiers "
            "must be unique"
        )

    shadow_game_ids = set(game_ids)
    outcome_identities = tuple(
        _outcome_identity(value)
        for value in outcomes
    )

    if len(outcome_identities) != len(
        set(outcome_identities)
    ):
        raise ValueError(
            "Statcast outcome identifiers must be unique"
        )

    for value in outcomes:
        if value.game_pk not in shadow_game_ids:
            raise ValueError(
                "Statcast outcome game_pk must match "
                "a historical shadow game"
            )
        if value.source_version != (
            observed_source_version
        ):
            raise ValueError(
                "Statcast outcome source_version must "
                "match observed_source_version"
            )

    ordered_games = tuple(
        sorted(
            shadow_games,
            key=lambda value: (
                value.game_date,
                value.game_pk,
            ),
        )
    )

    records = []
    for shadow_game in ordered_games:
        game_outcomes = tuple(
            value
            for value in outcomes
            if value.game_pk == shadow_game.game_pk
        )

        records.append(
            CanonicalHistoricalBaserunningGame(
                game_pk=shadow_game.game_pk,
                game_date=shadow_game.game_date,
                validation=shadow_game.validation,
                observed_stolen_bases=sum(
                    value.event_type == "stolen_base"
                    for value in game_outcomes
                ),
                observed_caught_stealing=sum(
                    value.event_type == "caught_stealing"
                    for value in game_outcomes
                ),
                observed_source_version=(
                    observed_source_version
                ),
            )
        )

    return tuple(records)


def materialize_play_by_play_baserunning_game_records(
    *,
    shadow_games: Tuple[
        CanonicalHistoricalBaserunningShadowGame,
        ...,
    ],
    observed: CanonicalMlbPlayByPlayBaserunningSnapshot,
) -> Tuple[
    CanonicalHistoricalBaserunningGame,
    ...,
]:
    """
    Join complete MLB play-by-play totals to shadow validations.

    Exact game identity and official-date coverage are required. Statcast
    description outcomes are intentionally not used as calibration truth.
    """

    if not isinstance(shadow_games, tuple):
        raise TypeError(
            "shadow_games must be a tuple"
        )
    if not shadow_games:
        raise ValueError(
            "shadow_games must contain records"
        )

    for value in shadow_games:
        if not isinstance(
            value,
            CanonicalHistoricalBaserunningShadowGame,
        ):
            raise TypeError(
                "shadow_games must contain "
                "CanonicalHistoricalBaserunningShadowGame"
            )

    if not isinstance(
        observed,
        CanonicalMlbPlayByPlayBaserunningSnapshot,
    ):
        raise TypeError(
            "observed must be "
            "CanonicalMlbPlayByPlayBaserunningSnapshot"
        )

    shadow_by_id = {}
    for value in shadow_games:
        if value.game_pk in shadow_by_id:
            raise ValueError(
                "historical shadow game identifiers "
                "must be unique"
            )
        shadow_by_id[value.game_pk] = value

    observed_by_id = {
        value.game_pk: value
        for value in observed.games
    }

    if set(shadow_by_id) != set(observed_by_id):
        raise ValueError(
            "shadow games must exactly match "
            "observed play-by-play games"
        )

    records = []
    for game_pk in sorted(
        shadow_by_id,
        key=lambda value: (
            shadow_by_id[value].game_date,
            value,
        ),
    ):
        shadow_game = shadow_by_id[game_pk]
        observed_game = observed_by_id[game_pk]

        if (
            shadow_game.game_date
            != observed_game.game_date
        ):
            raise ValueError(
                "shadow game_date must match "
                "observed official game_date"
            )

        records.append(
            CanonicalHistoricalBaserunningGame(
                game_pk=game_pk,
                game_date=shadow_game.game_date,
                validation=shadow_game.validation,
                observed_stolen_bases=(
                    observed_game.stolen_bases
                ),
                observed_caught_stealing=(
                    observed_game.caught_stealing
                ),
                observed_source_version=(
                    observed.source_version
                ),
            )
        )

    if sum(
        value.observed_stolen_bases
        for value in records
    ) != observed.stolen_bases:
        raise ValueError(
            "materialized stolen-base total must "
            "match observed snapshot"
        )

    if sum(
        value.observed_caught_stealing
        for value in records
    ) != observed.caught_stealing:
        raise ValueError(
            "materialized caught-stealing total must "
            "match observed snapshot"
        )

    return tuple(records)
