"""Adapt observed pitcher baserunning evidence into profiles."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib

from mlb_app.simulation.game import (
    CanonicalPitcherBaserunningProfile,
)


CANONICAL_PITCHER_BASERUNNING_EVIDENCE_VERSION = (
    "canonical_pitcher_baserunning_evidence_v1"
)


def _validate_count(
    *,
    name: str,
    value: int,
) -> None:
    if not isinstance(value, int):
        raise TypeError(
            f"{name} must be an integer"
        )
    if value < 0:
        raise ValueError(
            f"{name} must be nonnegative"
        )


def _validate_rate(
    *,
    name: str,
    value: float,
) -> None:
    if not isinstance(value, (int, float)):
        raise TypeError(
            f"{name} must be numeric"
        )
    if not 0.0 <= float(value) <= 1.0:
        raise ValueError(
            f"{name} must be between 0 and 1"
        )


@dataclass(frozen=True)
class CanonicalPitcherBaserunningObservation:
    """
    Immutable event-level hold and pickoff evidence for one pitcher.

    Eligible opportunities must come from source events with a steal-capable
    runner aboard. Batters faced or innings pitched are not substitutes.
    """

    pitcher_id: str
    eligible_pickoff_opportunities: int
    pickoff_attempts: int
    successful_pickoffs: int
    hold_score: float
    delivery_time_score: float
    source_version: str = (
        CANONICAL_PITCHER_BASERUNNING_EVIDENCE_VERSION
    )

    def __post_init__(self) -> None:
        if not self.pitcher_id:
            raise ValueError(
                "pitcher_id is required"
            )

        for name, value in (
            (
                "eligible_pickoff_opportunities",
                self.eligible_pickoff_opportunities,
            ),
            (
                "pickoff_attempts",
                self.pickoff_attempts,
            ),
            (
                "successful_pickoffs",
                self.successful_pickoffs,
            ),
        ):
            _validate_count(
                name=name,
                value=value,
            )

        if (
            self.pickoff_attempts
            > self.eligible_pickoff_opportunities
        ):
            raise ValueError(
                "pickoff attempts cannot exceed "
                "eligible opportunities"
            )

        if (
            self.successful_pickoffs
            > self.pickoff_attempts
        ):
            raise ValueError(
                "successful pickoffs cannot exceed attempts"
            )

        for name, value in (
            ("hold_score", self.hold_score),
            (
                "delivery_time_score",
                self.delivery_time_score,
            ),
        ):
            _validate_rate(
                name=name,
                value=value,
            )

        if not self.source_version:
            raise ValueError(
                "source_version is required"
            )

    @property
    def pickoff_attempt_rate(self) -> float:
        if self.eligible_pickoff_opportunities == 0:
            return 0.0

        return round(
            self.pickoff_attempts
            / self.eligible_pickoff_opportunities,
            6,
        )

    @property
    def pickoff_success_rate(self) -> float:
        if self.pickoff_attempts == 0:
            return 0.0

        return round(
            self.successful_pickoffs
            / self.pickoff_attempts,
            6,
        )

    @property
    def digest(self) -> str:
        parts = (
            self.source_version,
            self.pitcher_id,
            str(self.eligible_pickoff_opportunities),
            str(self.pickoff_attempts),
            str(self.successful_pickoffs),
            repr(float(self.hold_score)),
            repr(float(self.delivery_time_score)),
        )

        return hashlib.sha256(
            "\x1f".join(parts).encode("utf-8")
        ).hexdigest()

    def to_profile(
        self,
    ) -> CanonicalPitcherBaserunningProfile:
        return CanonicalPitcherBaserunningProfile(
            pitcher_id=self.pitcher_id,
            hold_score=float(self.hold_score),
            delivery_time_score=float(
                self.delivery_time_score
            ),
            pickoff_attempt_rate=(
                self.pickoff_attempt_rate
            ),
            pickoff_success_rate=(
                self.pickoff_success_rate
            ),
        )


def adapt_observed_pitcher_baserunning_evidence(
    observation: CanonicalPitcherBaserunningObservation,
) -> CanonicalPitcherBaserunningProfile:
    """Return one deterministic canonical pitcher profile."""

    if not isinstance(
        observation,
        CanonicalPitcherBaserunningObservation,
    ):
        raise TypeError(
            "observation must be a "
            "CanonicalPitcherBaserunningObservation"
        )

    return observation.to_profile()
