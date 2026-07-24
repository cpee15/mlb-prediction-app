"""Adapt Baseball Savant sprint speed into canonical evidence."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Iterable, Mapping, Optional, Tuple


CANONICAL_RUNNER_SPRINT_SPEED_SOURCE_VERSION = (
    "baseball_savant_sprint_speed_v1"
)
CANONICAL_RUNNER_SPRINT_SPEED_NORMALIZATION_VERSION = (
    "canonical_runner_sprint_speed_normalization_v1"
)

SPRINT_SPEED_FLOOR_FT_PER_SECOND = 23.0
SPRINT_SPEED_ELITE_FT_PER_SECOND = 30.0


def _identifier(value: Any) -> Optional[str]:
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return str(int(value))

    text = str(value).strip()

    if text.lower() in {
        "",
        "<na>",
        "nan",
        "none",
        "null",
    }:
        return None

    try:
        numeric = float(text)
    except ValueError:
        return text

    if math.isnan(numeric):
        return None

    if numeric.is_integer():
        return str(int(numeric))

    return text


def _sprint_speed(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None

    try:
        result = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(result) or result <= 0.0:
        return None

    return result


def normalize_runner_sprint_speed(
    sprint_speed_ft_per_second: float,
) -> float:
    """
    Normalize observed sprint speed onto the canonical unit interval.

    The versioned policy maps 23 ft/s to zero and the Baseball Savant
    elite-speed threshold of 30 ft/s to one, clamping observations
    outside that interval while preserving the raw measurement.
    """

    speed = _sprint_speed(
        sprint_speed_ft_per_second
    )

    if speed is None:
        raise ValueError(
            "sprint_speed_ft_per_second must be positive "
            "and finite"
        )

    normalized = (
        speed - SPRINT_SPEED_FLOOR_FT_PER_SECOND
    ) / (
        SPRINT_SPEED_ELITE_FT_PER_SECOND
        - SPRINT_SPEED_FLOOR_FT_PER_SECOND
    )

    return round(
        min(1.0, max(0.0, normalized)),
        6,
    )


@dataclass(frozen=True)
class CanonicalRunnerSprintSpeedObservation:
    """One sourced Baseball Savant runner-speed observation."""

    runner_id: str
    sprint_speed_ft_per_second: float
    source_version: str = (
        CANONICAL_RUNNER_SPRINT_SPEED_SOURCE_VERSION
    )
    normalization_version: str = (
        CANONICAL_RUNNER_SPRINT_SPEED_NORMALIZATION_VERSION
    )

    def __post_init__(self) -> None:
        if not self.runner_id:
            raise ValueError(
                "runner_id is required"
            )

        if _sprint_speed(
            self.sprint_speed_ft_per_second
        ) is None:
            raise ValueError(
                "sprint_speed_ft_per_second must be "
                "positive and finite"
            )

        if self.source_version != (
            CANONICAL_RUNNER_SPRINT_SPEED_SOURCE_VERSION
        ):
            raise ValueError(
                "unsupported runner sprint-speed source "
                "version"
            )

        if self.normalization_version != (
            CANONICAL_RUNNER_SPRINT_SPEED_NORMALIZATION_VERSION
        ):
            raise ValueError(
                "unsupported runner sprint-speed "
                "normalization version"
            )

    @property
    def speed_score(self) -> float:
        return normalize_runner_sprint_speed(
            self.sprint_speed_ft_per_second
        )


def decode_baseball_savant_sprint_speed_rows(
    rows: Iterable[Mapping[str, Any]],
) -> Tuple[
    CanonicalRunnerSprintSpeedObservation,
    ...,
]:
    """
    Decode complete Baseball Savant sprint-speed rows.

    Rows missing an exact player identity or sprint-speed measurement
    are omitted. Duplicate runner identities are rejected instead of
    selecting an observation implicitly.
    """

    observations = {}

    for row in rows:
        if not isinstance(row, Mapping):
            raise TypeError(
                "each sprint-speed row must be a mapping"
            )

        runner_id = _identifier(
            row.get("player_id")
        )
        speed = _sprint_speed(
            row.get("sprint_speed")
        )

        if runner_id is None or speed is None:
            continue

        if runner_id in observations:
            raise ValueError(
                "runner sprint-speed identifiers must "
                "be unique"
            )

        observations[runner_id] = (
            CanonicalRunnerSprintSpeedObservation(
                runner_id=runner_id,
                sprint_speed_ft_per_second=speed,
            )
        )

    return tuple(
        observations[runner_id]
        for runner_id in sorted(observations)
    )
