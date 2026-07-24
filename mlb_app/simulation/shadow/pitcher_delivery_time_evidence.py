"""Adapt explicit pitcher delivery-time measurements."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Mapping, Tuple


CANONICAL_PITCHER_DELIVERY_TIME_SOURCE_VERSION = (
    "observed_pitcher_delivery_time_v1"
)
CANONICAL_PITCHER_DELIVERY_TIME_NORMALIZATION_VERSION = (
    "canonical_pitcher_delivery_time_normalization_v1"
)

FAST_DELIVERY_TIME_SECONDS = 1.20
SLOW_DELIVERY_TIME_SECONDS = 1.60


def normalize_pitcher_delivery_time(
    seconds_to_plate: float,
) -> float:
    """
    Normalize measured seconds to plate onto a unit score.

    Faster delivery receives a higher score. Values outside the explicit
    versioned bounds are clamped. Missing or invalid measurements are not
    converted to neutral values.
    """

    if not isinstance(
        seconds_to_plate,
        (int, float),
    ):
        raise TypeError(
            "seconds_to_plate must be numeric"
        )

    value = float(seconds_to_plate)

    if not math.isfinite(value) or value <= 0.0:
        raise ValueError(
            "seconds_to_plate must be positive and finite"
        )

    span = (
        SLOW_DELIVERY_TIME_SECONDS
        - FAST_DELIVERY_TIME_SECONDS
    )
    score = (
        SLOW_DELIVERY_TIME_SECONDS
        - value
    ) / span

    return round(
        min(max(score, 0.0), 1.0),
        6,
    )


@dataclass(frozen=True)
class CanonicalPitcherDeliveryTimeObservation:
    """One explicit measured pitcher delivery time."""

    pitcher_id: str
    seconds_to_plate: float
    source_version: str = (
        CANONICAL_PITCHER_DELIVERY_TIME_SOURCE_VERSION
    )
    normalization_version: str = (
        CANONICAL_PITCHER_DELIVERY_TIME_NORMALIZATION_VERSION
    )

    def __post_init__(self) -> None:
        if not self.pitcher_id:
            raise ValueError(
                "pitcher_id is required"
            )

        normalize_pitcher_delivery_time(
            self.seconds_to_plate
        )

        if (
            not self.source_version
            or self.source_version == "unavailable"
        ):
            raise ValueError(
                "source_version must identify "
                "an available source"
            )

        if self.normalization_version != (
            CANONICAL_PITCHER_DELIVERY_TIME_NORMALIZATION_VERSION
        ):
            raise ValueError(
                "unsupported pitcher delivery-time "
                "normalization version"
            )

    @property
    def delivery_time_score(self) -> float:
        return normalize_pitcher_delivery_time(
            self.seconds_to_plate
        )


def decode_pitcher_delivery_time_rows(
    rows: Tuple[
        Mapping[str, Any],
        ...,
    ],
) -> Tuple[
    CanonicalPitcherDeliveryTimeObservation,
    ...,
]:
    """
    Decode explicit source-agnostic pitcher timing rows.

    Required fields are pitcher_id and seconds_to_plate. The optional
    source_version must identify the actual upstream measurement source.
    Duplicate pitcher identities are rejected rather than averaged.
    """

    if not isinstance(rows, tuple):
        raise TypeError(
            "rows must be a tuple"
        )

    observations = []

    for row in rows:
        if not isinstance(row, Mapping):
            raise TypeError(
                "rows must contain mappings"
            )

        pitcher_id = row.get("pitcher_id")
        seconds_to_plate = row.get(
            "seconds_to_plate"
        )

        if pitcher_id in (None, ""):
            raise ValueError(
                "pitcher_id is required"
            )

        if seconds_to_plate is None:
            raise ValueError(
                "seconds_to_plate is required"
            )

        observations.append(
            CanonicalPitcherDeliveryTimeObservation(
                pitcher_id=str(pitcher_id),
                seconds_to_plate=float(
                    seconds_to_plate
                ),
                source_version=str(
                    row.get("source_version")
                    or CANONICAL_PITCHER_DELIVERY_TIME_SOURCE_VERSION
                ),
            )
        )

    identifiers = [
        value.pitcher_id
        for value in observations
    ]

    if len(identifiers) != len(set(identifiers)):
        raise ValueError(
            "pitcher delivery-time identifiers "
            "must be unique"
        )

    return tuple(observations)
