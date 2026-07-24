"""Adapt explicit runner fatigue and injury-limit evidence."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Mapping, Tuple


CANONICAL_RUNNER_AVAILABILITY_EVIDENCE_VERSION = (
    "canonical_runner_availability_evidence_v1"
)


@dataclass(frozen=True)
class CanonicalRunnerAvailabilityObservation:
    """One explicitly sourced runner availability snapshot."""

    runner_id: str
    fatigue_index: float
    injury_limit_flag: bool
    source_version: str
    evidence_version: str = (
        CANONICAL_RUNNER_AVAILABILITY_EVIDENCE_VERSION
    )

    def __post_init__(self) -> None:
        if not isinstance(self.runner_id, str):
            raise TypeError(
                "runner_id must be a string"
            )
        if not self.runner_id.strip():
            raise ValueError(
                "runner_id must identify a runner"
            )

        if not isinstance(
            self.fatigue_index,
            (int, float),
        ):
            raise TypeError(
                "fatigue_index must be numeric"
            )

        fatigue = float(self.fatigue_index)
        if (
            not math.isfinite(fatigue)
            or not 0.0 <= fatigue <= 1.0
        ):
            raise ValueError(
                "fatigue_index must be finite and "
                "between 0 and 1"
            )

        if not isinstance(
            self.injury_limit_flag,
            bool,
        ):
            raise TypeError(
                "injury_limit_flag must be boolean"
            )

        if not isinstance(
            self.source_version,
            str,
        ):
            raise TypeError(
                "source_version must be a string"
            )
        if (
            not self.source_version.strip()
            or self.source_version == "unavailable"
        ):
            raise ValueError(
                "source_version must identify "
                "an available source"
            )

        if self.evidence_version != (
            CANONICAL_RUNNER_AVAILABILITY_EVIDENCE_VERSION
        ):
            raise ValueError(
                "evidence_version must identify the canonical "
                "runner availability evidence contract"
            )

    @property
    def fatigue_score(self) -> float:
        return round(
            float(self.fatigue_index),
            6,
        )


def decode_runner_availability_rows(
    rows: Tuple[
        Mapping[str, Any],
        ...,
    ],
) -> Tuple[
    CanonicalRunnerAvailabilityObservation,
    ...,
]:
    """
    Decode explicit runner fatigue and injury-limit rows.

    All fields and source provenance are required. Missing availability
    evidence is not interpreted as rested or unrestricted.
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

        runner_id = row.get("runner_id")
        fatigue_index = row.get(
            "fatigue_index"
        )
        injury_limit_flag = row.get(
            "injury_limit_flag"
        )
        source_version = row.get(
            "source_version"
        )

        if runner_id in (None, ""):
            raise ValueError(
                "runner_id is required"
            )
        if fatigue_index is None:
            raise ValueError(
                "fatigue_index is required"
            )
        if injury_limit_flag is None:
            raise ValueError(
                "injury_limit_flag is required"
            )
        if not isinstance(
            injury_limit_flag,
            bool,
        ):
            raise TypeError(
                "injury_limit_flag must be boolean"
            )
        if source_version in (
            None,
            "",
            "unavailable",
        ):
            raise ValueError(
                "source_version must identify "
                "an available source"
            )

        observations.append(
            CanonicalRunnerAvailabilityObservation(
                runner_id=str(runner_id),
                fatigue_index=float(
                    fatigue_index
                ),
                injury_limit_flag=(
                    injury_limit_flag
                ),
                source_version=str(
                    source_version
                ),
            )
        )

    identifiers = [
        value.runner_id
        for value in observations
    ]

    if len(identifiers) != len(set(identifiers)):
        raise ValueError(
            "runner availability identifiers "
            "must be unique"
        )

    return tuple(observations)
