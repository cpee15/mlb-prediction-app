"""Adapt Baseball Savant catcher pop time into canonical evidence."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Iterable, Mapping, Optional, Tuple


CANONICAL_CATCHER_POP_TIME_SOURCE_VERSION = (
    "baseball_savant_catcher_pop_time_v1"
)
CANONICAL_CATCHER_POP_TIME_NORMALIZATION_VERSION = (
    "canonical_catcher_pop_time_normalization_v1"
)

ELITE_POP_TIME_SECONDS = 1.80
SLOW_POP_TIME_SECONDS = 2.10


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


def _pop_time(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None

    try:
        result = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(result) or result <= 0.0:
        return None

    return result


def normalize_catcher_pop_time(
    pop_time_seconds: float,
) -> float:
    """
    Normalize observed pop time onto the canonical unit interval.

    The versioned policy maps 2.10 seconds to zero and 1.80 seconds
    to one. Lower pop times are better. Observations outside the
    interval are clamped while the raw measurement is preserved.
    """

    pop_time = _pop_time(
        pop_time_seconds
    )

    if pop_time is None:
        raise ValueError(
            "pop_time_seconds must be positive and finite"
        )

    normalized = (
        SLOW_POP_TIME_SECONDS - pop_time
    ) / (
        SLOW_POP_TIME_SECONDS
        - ELITE_POP_TIME_SECONDS
    )

    return round(
        min(1.0, max(0.0, normalized)),
        6,
    )


@dataclass(frozen=True)
class CanonicalCatcherPopTimeObservation:
    """One sourced Baseball Savant catcher pop-time observation."""

    catcher_id: str
    pop_time_seconds: float
    source_version: str = (
        CANONICAL_CATCHER_POP_TIME_SOURCE_VERSION
    )
    normalization_version: str = (
        CANONICAL_CATCHER_POP_TIME_NORMALIZATION_VERSION
    )

    def __post_init__(self) -> None:
        if not self.catcher_id:
            raise ValueError(
                "catcher_id is required"
            )

        if _pop_time(
            self.pop_time_seconds
        ) is None:
            raise ValueError(
                "pop_time_seconds must be positive and finite"
            )

        if self.source_version != (
            CANONICAL_CATCHER_POP_TIME_SOURCE_VERSION
        ):
            raise ValueError(
                "unsupported catcher pop-time source version"
            )

        if self.normalization_version != (
            CANONICAL_CATCHER_POP_TIME_NORMALIZATION_VERSION
        ):
            raise ValueError(
                "unsupported catcher pop-time "
                "normalization version"
            )

    @property
    def pop_time_score(self) -> float:
        return normalize_catcher_pop_time(
            self.pop_time_seconds
        )


def decode_baseball_savant_catcher_pop_time_rows(
    rows: Iterable[Mapping[str, Any]],
) -> Tuple[
    CanonicalCatcherPopTimeObservation,
    ...,
]:
    """
    Decode complete Baseball Savant catcher pop-time rows.

    The source contract uses Baseball Savant's `player_id` and
    `pop_2b_sba` fields. Missing measurements are omitted and duplicate
    catcher identities are rejected instead of resolved implicitly.
    """

    observations = {}

    for row in rows:
        if not isinstance(row, Mapping):
            raise TypeError(
                "each catcher pop-time row must be a mapping"
            )

        catcher_id = _identifier(
            row.get("player_id")
        )
        pop_time = _pop_time(
            row.get("pop_2b_sba")
        )

        if catcher_id is None or pop_time is None:
            continue

        if catcher_id in observations:
            raise ValueError(
                "catcher pop-time identifiers must be unique"
            )

        observations[catcher_id] = (
            CanonicalCatcherPopTimeObservation(
                catcher_id=catcher_id,
                pop_time_seconds=pop_time,
            )
        )

    return tuple(
        observations[catcher_id]
        for catcher_id in sorted(observations)
    )
