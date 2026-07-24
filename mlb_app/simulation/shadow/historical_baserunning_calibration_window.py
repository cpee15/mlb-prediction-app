"""Execute one aligned historical baserunning calibration window."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple

from .baserunning_calibration_artifact import (
    CanonicalBaserunningCalibrationArtifact,
    execute_baserunning_calibration_artifact,
)
from .baserunning_calibration_gate import (
    CanonicalBaserunningCalibrationPolicy,
)
from .baserunning_calibration_payload import (
    assemble_historical_baserunning_calibration_payload,
)
from .historical_baserunning_game_materialization import (
    CanonicalHistoricalBaserunningShadowGame,
    materialize_historical_baserunning_game_records,
)
from .statcast_baserunning_source import (
    decode_statcast_baserunning_outcomes,
)


CANONICAL_HISTORICAL_BASERUNNING_WINDOW_VERSION = (
    "canonical_historical_baserunning_window_v1"
)

_VALID_STATUSES = {
    "ready",
    "unavailable",
    "error",
}


@dataclass(frozen=True)
class CanonicalHistoricalBaserunningWindowExecution:
    status: str = "unavailable"
    window_start: Optional[str] = None
    window_end: Optional[str] = None
    game_count: int = 0
    statcast_row_count: int = 0
    observed_outcome_count: int = 0
    artifact: Optional[
        CanonicalBaserunningCalibrationArtifact
    ] = None
    error_message: Optional[str] = None
    execution_version: str = (
        CANONICAL_HISTORICAL_BASERUNNING_WINDOW_VERSION
    )

    def __post_init__(self) -> None:
        if self.status not in _VALID_STATUSES:
            raise ValueError(
                "unsupported historical baserunning "
                "window status"
            )

        for field_name in (
            "game_count",
            "statcast_row_count",
            "observed_outcome_count",
        ):
            if getattr(self, field_name) < 0:
                raise ValueError(
                    f"{field_name} must be nonnegative"
                )

        if self.status == "ready" and (
            self.artifact is None
            or not self.artifact.ready
        ):
            raise ValueError(
                "ready window execution requires "
                "ready artifact"
            )

        if self.execution_version != (
            CANONICAL_HISTORICAL_BASERUNNING_WINDOW_VERSION
        ):
            raise ValueError(
                "unsupported historical baserunning "
                "window version"
            )

    @property
    def ready(self) -> bool:
        return self.status == "ready"

    @property
    def calibration_gate_passed(self) -> bool:
        return bool(
            self.artifact is not None
            and self.artifact.calibration_gate_passed
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.execution_version,
            "status": self.status,
            "ready": self.ready,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "game_count": self.game_count,
            "statcast_row_count": (
                self.statcast_row_count
            ),
            "observed_outcome_count": (
                self.observed_outcome_count
            ),
            "calibration_gate_passed": (
                self.calibration_gate_passed
            ),
            "artifact": (
                self.artifact.to_diagnostics()
                if self.artifact is not None
                else None
            ),
            "error_message": self.error_message,
            "external_fetch_performed": False,
            "persistence_performed": False,
            "activation_permitted": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def _game_pk(row: Mapping[str, Any]) -> int:
    value = row.get("game_pk")

    if value is None or isinstance(value, bool):
        raise ValueError(
            "Statcast row requires game_pk"
        )

    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Statcast row requires integer game_pk"
        ) from exc

    if (
        not math.isfinite(numeric)
        or not numeric.is_integer()
        or numeric <= 0
    ):
        raise ValueError(
            "Statcast row requires integer game_pk"
        )

    return int(numeric)


def execute_historical_baserunning_calibration_window(
    *,
    window_start: str,
    window_end: str,
    shadow_games: Tuple[
        CanonicalHistoricalBaserunningShadowGame,
        ...,
    ],
    statcast_rows: Tuple[
        Mapping[str, Any],
        ...,
    ],
    policy: CanonicalBaserunningCalibrationPolicy,
    observed_source_version: str,
) -> CanonicalHistoricalBaserunningWindowExecution:
    """
    Execute the complete offline calibration pipeline for one window.

    Every shadow game must have Statcast row coverage, including games with
    zero decoded SB/CS outcomes. No fetching, persistence, or activation is
    performed.
    """

    game_count = 0
    statcast_row_count = 0
    observed_outcome_count = 0

    try:
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

        if not isinstance(statcast_rows, tuple):
            raise TypeError(
                "statcast_rows must be a tuple"
            )
        if not statcast_rows:
            raise ValueError(
                "statcast_rows must contain records"
            )

        game_count = len(shadow_games)
        statcast_row_count = len(statcast_rows)

        shadow_game_ids = {
            value.game_pk
            for value in shadow_games
        }
        row_game_ids = set()
        outcomes = []

        for row in statcast_rows:
            if not isinstance(row, Mapping):
                raise TypeError(
                    "statcast_rows must contain mappings"
                )

            row_game_pk = _game_pk(row)
            row_game_ids.add(row_game_pk)

            if row_game_pk not in shadow_game_ids:
                raise ValueError(
                    "Statcast row game_pk must match "
                    "a historical shadow game"
                )

            outcomes.extend(
                decode_statcast_baserunning_outcomes(
                    row
                )
            )

        missing_game_ids = (
            shadow_game_ids - row_game_ids
        )
        if missing_game_ids:
            raise ValueError(
                "every historical shadow game must "
                "have Statcast row coverage"
            )

        observed_outcome_count = len(outcomes)

        game_records = (
            materialize_historical_baserunning_game_records(
                shadow_games=shadow_games,
                outcomes=tuple(outcomes),
                observed_source_version=(
                    observed_source_version
                ),
            )
        )

        payload = (
            assemble_historical_baserunning_calibration_payload(
                window_start=window_start,
                window_end=window_end,
                games=game_records,
                policy=policy,
            )
        )
        artifact = execute_baserunning_calibration_artifact(
            payload
        )
    except (
        KeyError,
        TypeError,
        ValueError,
    ) as exc:
        return CanonicalHistoricalBaserunningWindowExecution(
            status="error",
            window_start=window_start,
            window_end=window_end,
            game_count=game_count,
            statcast_row_count=statcast_row_count,
            observed_outcome_count=(
                observed_outcome_count
            ),
            error_message=str(exc),
        )

    return CanonicalHistoricalBaserunningWindowExecution(
        status=artifact.status,
        window_start=window_start,
        window_end=window_end,
        game_count=len(game_records),
        statcast_row_count=statcast_row_count,
        observed_outcome_count=(
            observed_outcome_count
        ),
        artifact=artifact,
        error_message=artifact.error_message,
    )
