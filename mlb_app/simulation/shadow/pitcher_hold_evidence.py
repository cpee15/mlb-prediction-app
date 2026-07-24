"""Canonical pitcher hold evidence derived from exact attempt exposure."""

from dataclasses import dataclass
import math
from typing import Tuple

from .statcast_baserunning_source import (
    CanonicalStatcastPitcherBaserunningCounts,
)


CANONICAL_PITCHER_HOLD_EVIDENCE_VERSION = (
    "canonical_pitcher_hold_evidence_v1"
)
CANONICAL_PITCHER_HOLD_NORMALIZATION_VERSION = (
    "canonical_pitcher_hold_normalization_v1"
)


@dataclass(frozen=True)
class CanonicalPitcherHoldObservation:
    """Observed pitcher steal-attempt deterrence."""

    pitcher_id: str
    eligible_opportunities: int
    steal_attempts_against: int
    source_version: str
    evidence_version: str = (
        CANONICAL_PITCHER_HOLD_EVIDENCE_VERSION
    )
    normalization_version: str = (
        CANONICAL_PITCHER_HOLD_NORMALIZATION_VERSION
    )

    def __post_init__(self) -> None:
        if not isinstance(self.pitcher_id, str):
            raise TypeError("pitcher_id must be a string")
        if not self.pitcher_id.strip():
            raise ValueError("pitcher_id must identify a pitcher")

        if not isinstance(self.eligible_opportunities, int):
            raise TypeError(
                "eligible_opportunities must be an integer"
            )
        if self.eligible_opportunities <= 0:
            raise ValueError(
                "eligible_opportunities must be positive"
            )

        if not isinstance(self.steal_attempts_against, int):
            raise TypeError(
                "steal_attempts_against must be an integer"
            )
        if not (
            0
            <= self.steal_attempts_against
            <= self.eligible_opportunities
        ):
            raise ValueError(
                "steal_attempts_against must be between zero "
                "and eligible_opportunities"
            )

        if not isinstance(self.source_version, str):
            raise TypeError("source_version must be a string")
        if not self.source_version.strip():
            raise ValueError(
                "source_version must identify an available source"
            )

        if (
            self.evidence_version
            != CANONICAL_PITCHER_HOLD_EVIDENCE_VERSION
        ):
            raise ValueError(
                "evidence_version must identify the canonical "
                "pitcher hold evidence contract"
            )
        if (
            self.normalization_version
            != CANONICAL_PITCHER_HOLD_NORMALIZATION_VERSION
        ):
            raise ValueError(
                "normalization_version must identify the canonical "
                "pitcher hold normalization"
            )

    @property
    def attempt_rate(self) -> float:
        return round(
            self.steal_attempts_against
            / self.eligible_opportunities,
            6,
        )

    @property
    def hold_score(self) -> float:
        value = 1.0 - self.attempt_rate
        if not math.isfinite(value):
            raise ValueError("hold_score must be finite")
        return round(min(1.0, max(0.0, value)), 6)


def adapt_statcast_pitcher_hold_evidence(
    counts: Tuple[
        CanonicalStatcastPitcherBaserunningCounts,
        ...,
    ],
) -> Tuple[CanonicalPitcherHoldObservation, ...]:
    """
    Convert exact Statcast exposure into pitcher hold evidence.

    Stolen bases and caught stealing both represent attempts against
    the pitcher. Caught stealing does not provide extra pitcher credit,
    and pickoff behavior remains a separate evidence contract.
    """

    if not isinstance(counts, tuple):
        raise TypeError("counts must be a tuple")

    if any(
        not isinstance(
            value,
            CanonicalStatcastPitcherBaserunningCounts,
        )
        for value in counts
    ):
        raise TypeError(
            "counts must contain "
            "CanonicalStatcastPitcherBaserunningCounts"
        )

    pitcher_ids = [
        value.pitcher_id
        for value in counts
    ]
    if len(pitcher_ids) != len(set(pitcher_ids)):
        raise ValueError(
            "pitcher baserunning count identifiers must be unique"
        )

    return tuple(
        CanonicalPitcherHoldObservation(
            pitcher_id=value.pitcher_id,
            eligible_opportunities=(
                value.eligible_opportunities
            ),
            steal_attempts_against=(
                value.stolen_bases_allowed
                + value.caught_stealing
            ),
            source_version=value.source_version,
        )
        for value in counts
        if value.eligible_opportunities > 0
    )
