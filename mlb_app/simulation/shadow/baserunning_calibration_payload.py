"""Assemble immutable historical baserunning calibration inputs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Tuple

from .baserunning_calibration_artifact import (
    CANONICAL_BASERUNNING_CALIBRATION_INPUT_VERSION,
)
from .baserunning_calibration_gate import (
    CanonicalBaserunningCalibrationPolicy,
)
from .baserunning_output_validation import (
    CanonicalBaserunningOutputValidation,
)


CANONICAL_HISTORICAL_BASERUNNING_GAME_VERSION = (
    "canonical_historical_baserunning_game_v1"
)
CANONICAL_BASERUNNING_CALIBRATION_PAYLOAD_VERSION = (
    "canonical_baserunning_calibration_payload_v1"
)


@dataclass(frozen=True)
class CanonicalHistoricalBaserunningGame:
    game_pk: int
    game_date: str
    validation: CanonicalBaserunningOutputValidation
    observed_stolen_bases: int
    observed_caught_stealing: int
    observed_source_version: str
    record_version: str = (
        CANONICAL_HISTORICAL_BASERUNNING_GAME_VERSION
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

        parsed_date = date.fromisoformat(self.game_date)
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
                "historical game validation must be ready"
            )

        for field_name in (
            "observed_stolen_bases",
            "observed_caught_stealing",
        ):
            value = getattr(self, field_name)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                raise ValueError(
                    f"{field_name} must be a "
                    "nonnegative integer"
                )

        if (
            not isinstance(
                self.observed_source_version,
                str,
            )
            or not self.observed_source_version.strip()
            or self.observed_source_version
            == "unavailable"
        ):
            raise ValueError(
                "observed_source_version must identify "
                "an available source"
            )

        if self.record_version != (
            CANONICAL_HISTORICAL_BASERUNNING_GAME_VERSION
        ):
            raise ValueError(
                "unsupported historical baserunning "
                "game version"
            )


def _iso_date(
    value: Any,
    field_name: str,
) -> date:
    if not isinstance(value, str):
        raise TypeError(
            f"{field_name} must be a string"
        )

    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError(
            f"{field_name} must use ISO format"
        )

    return parsed


def _validation_payload(
    value: CanonicalBaserunningOutputValidation,
) -> Dict[str, Any]:
    return {
        "status": value.status,
        "simulation_count": value.simulation_count,
        "catalog_digest": value.catalog_digest,
        "runner_projection_count": (
            value.runner_projection_count
        ),
        "stolen_base_mean_total": (
            value.stolen_base_mean_total
        ),
        "caught_stealing_mean_total": (
            value.caught_stealing_mean_total
        ),
        "warnings": list(value.warnings),
        "error_message": value.error_message,
    }


def _policy_payload(
    value: CanonicalBaserunningCalibrationPolicy,
) -> Dict[str, Any]:
    return {
        "minimum_game_count": value.minimum_game_count,
        "maximum_stolen_base_error_per_game": (
            value.maximum_stolen_base_error_per_game
        ),
        "maximum_caught_stealing_error_per_game": (
            value.maximum_caught_stealing_error_per_game
        ),
        "maximum_attempt_error_per_game": (
            value.maximum_attempt_error_per_game
        ),
        "maximum_success_rate_absolute_error": (
            value.maximum_success_rate_absolute_error
        ),
        "policy_version": value.policy_version,
    }


def assemble_historical_baserunning_calibration_payload(
    *,
    window_start: str,
    window_end: str,
    games: Tuple[
        CanonicalHistoricalBaserunningGame,
        ...,
    ],
    policy: CanonicalBaserunningCalibrationPolicy,
) -> Dict[str, Any]:
    """
    Assemble the exact offline artifact input from aligned game records.

    The function performs no fetching, persistence, simulation execution,
    calibration approval, or production activation.
    """

    start_date = _iso_date(
        window_start,
        "window_start",
    )
    end_date = _iso_date(
        window_end,
        "window_end",
    )

    if end_date < start_date:
        raise ValueError(
            "window_end must not precede window_start"
        )

    if not isinstance(games, tuple):
        raise TypeError(
            "games must be a tuple"
        )

    if not games:
        raise ValueError(
            "games must contain historical records"
        )

    for value in games:
        if not isinstance(
            value,
            CanonicalHistoricalBaserunningGame,
        ):
            raise TypeError(
                "games must contain "
                "CanonicalHistoricalBaserunningGame"
            )

    if not isinstance(
        policy,
        CanonicalBaserunningCalibrationPolicy,
    ):
        raise TypeError(
            "policy must be "
            "CanonicalBaserunningCalibrationPolicy"
        )

    game_ids = tuple(
        value.game_pk
        for value in games
    )
    if len(game_ids) != len(set(game_ids)):
        raise ValueError(
            "historical game identifiers must be unique"
        )

    source_versions = {
        value.observed_source_version
        for value in games
    }
    if len(source_versions) != 1:
        raise ValueError(
            "observed source versions must be identical"
        )

    ordered_games = tuple(
        sorted(
            games,
            key=lambda value: (
                value.game_date,
                value.game_pk,
            ),
        )
    )

    for value in ordered_games:
        record_date = date.fromisoformat(
            value.game_date
        )
        if not (
            start_date
            <= record_date
            <= end_date
        ):
            raise ValueError(
                "historical game date must fall "
                "within calibration window"
            )

    return {
        "schema_version": (
            CANONICAL_BASERUNNING_CALIBRATION_INPUT_VERSION
        ),
        "window": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
        "validations": [
            _validation_payload(value.validation)
            for value in ordered_games
        ],
        "observed": {
            "game_count": len(ordered_games),
            "stolen_bases": sum(
                value.observed_stolen_bases
                for value in ordered_games
            ),
            "caught_stealing": sum(
                value.observed_caught_stealing
                for value in ordered_games
            ),
            "source_version": next(
                iter(source_versions)
            ),
        },
        "policy": _policy_payload(policy),
    }
