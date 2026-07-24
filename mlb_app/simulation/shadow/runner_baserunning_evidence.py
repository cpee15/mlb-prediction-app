"""Adapt observed runner opportunities into canonical profiles."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib

from mlb_app.simulation.game import (
    CanonicalRunnerBaserunningProfile,
)


CANONICAL_RUNNER_BASERUNNING_EVIDENCE_VERSION = (
    "canonical_runner_baserunning_evidence_v1"
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
class CanonicalRunnerBaserunningObservation:
    """
    Immutable observed opportunity evidence for one runner.

    Eligible opportunities must be computed from event-level source data.
    Plate appearances or times on base are not accepted as substitutes.
    """

    runner_id: str
    eligible_opportunities: int
    stolen_bases: int
    caught_stealing: int
    speed_score: float
    lead_quality: float
    fatigue_index: float
    injury_limit_flag: bool = False
    source_version: str = (
        CANONICAL_RUNNER_BASERUNNING_EVIDENCE_VERSION
    )

    def __post_init__(self) -> None:
        if not self.runner_id:
            raise ValueError(
                "runner_id is required"
            )

        for name, value in (
            (
                "eligible_opportunities",
                self.eligible_opportunities,
            ),
            ("stolen_bases", self.stolen_bases),
            (
                "caught_stealing",
                self.caught_stealing,
            ),
        ):
            _validate_count(
                name=name,
                value=value,
            )

        attempts = (
            self.stolen_bases
            + self.caught_stealing
        )

        if attempts > self.eligible_opportunities:
            raise ValueError(
                "attempts cannot exceed eligible opportunities"
            )

        for name, value in (
            ("speed_score", self.speed_score),
            ("lead_quality", self.lead_quality),
            ("fatigue_index", self.fatigue_index),
        ):
            _validate_rate(
                name=name,
                value=value,
            )

        if not isinstance(
            self.injury_limit_flag,
            bool,
        ):
            raise TypeError(
                "injury_limit_flag must be boolean"
            )

        if not self.source_version:
            raise ValueError(
                "source_version is required"
            )

    @property
    def attempts(self) -> int:
        return (
            self.stolen_bases
            + self.caught_stealing
        )

    @property
    def attempt_rate(self) -> float:
        if self.eligible_opportunities == 0:
            return 0.0

        return round(
            self.attempts
            / self.eligible_opportunities,
            6,
        )

    @property
    def success_rate(self) -> float:
        if self.attempts == 0:
            return 0.0

        return round(
            self.stolen_bases
            / self.attempts,
            6,
        )

    @property
    def digest(self) -> str:
        parts = (
            self.source_version,
            self.runner_id,
            str(self.eligible_opportunities),
            str(self.stolen_bases),
            str(self.caught_stealing),
            repr(float(self.speed_score)),
            repr(float(self.lead_quality)),
            repr(float(self.fatigue_index)),
            repr(self.injury_limit_flag),
        )

        return hashlib.sha256(
            "\x1f".join(parts).encode("utf-8")
        ).hexdigest()

    def to_profile(
        self,
    ) -> CanonicalRunnerBaserunningProfile:
        return CanonicalRunnerBaserunningProfile(
            runner_id=self.runner_id,
            speed_score=float(self.speed_score),
            attempt_rate=self.attempt_rate,
            success_rate=self.success_rate,
            lead_quality=float(self.lead_quality),
            fatigue_index=float(self.fatigue_index),
            injury_limit_flag=(
                self.injury_limit_flag
            ),
        )


def adapt_observed_runner_baserunning_evidence(
    observation: CanonicalRunnerBaserunningObservation,
) -> CanonicalRunnerBaserunningProfile:
    """Return one deterministic canonical runner profile."""

    if not isinstance(
        observation,
        CanonicalRunnerBaserunningObservation,
    ):
        raise TypeError(
            "observation must be a "
            "CanonicalRunnerBaserunningObservation"
        )

    return observation.to_profile()
