"""Adapt explicit runner lead-quality evidence."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Mapping, Tuple


CANONICAL_RUNNER_LEAD_QUALITY_EVIDENCE_VERSION = (
    "canonical_runner_lead_quality_evidence_v1"
)


@dataclass(frozen=True)
class CanonicalRunnerLeadQualityObservation:
    """One explicitly sourced runner lead-quality score."""

    runner_id: str
    lead_quality: float
    source_version: str
    evidence_version: str = (
        CANONICAL_RUNNER_LEAD_QUALITY_EVIDENCE_VERSION
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
            self.lead_quality,
            (int, float),
        ):
            raise TypeError(
                "lead_quality must be numeric"
            )

        value = float(self.lead_quality)
        if (
            not math.isfinite(value)
            or not 0.0 <= value <= 1.0
        ):
            raise ValueError(
                "lead_quality must be finite and "
                "between 0 and 1"
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
            CANONICAL_RUNNER_LEAD_QUALITY_EVIDENCE_VERSION
        ):
            raise ValueError(
                "evidence_version must identify the canonical "
                "runner lead-quality evidence contract"
            )

    @property
    def lead_quality_score(self) -> float:
        return round(
            float(self.lead_quality),
            6,
        )


def decode_runner_lead_quality_rows(
    rows: Tuple[
        Mapping[str, Any],
        ...,
    ],
) -> Tuple[
    CanonicalRunnerLeadQualityObservation,
    ...,
]:
    """
    Decode explicitly normalized runner lead-quality rows.

    The upstream source must supply runner identity, a unit-interval
    lead-quality score, and source provenance. Missing evidence is not
    converted to a neutral score.
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
        lead_quality = row.get("lead_quality")
        source_version = row.get(
            "source_version"
        )

        if runner_id in (None, ""):
            raise ValueError(
                "runner_id is required"
            )
        if lead_quality is None:
            raise ValueError(
                "lead_quality is required"
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
            CanonicalRunnerLeadQualityObservation(
                runner_id=str(runner_id),
                lead_quality=float(
                    lead_quality
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
            "runner lead-quality identifiers "
            "must be unique"
        )

    return tuple(observations)
