"""Adapt observed catcher baserunning evidence into profiles."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib

from mlb_app.simulation.game import (
    CanonicalCatcherBaserunningProfile,
)


CANONICAL_CATCHER_BASERUNNING_EVIDENCE_VERSION = (
    "canonical_catcher_baserunning_evidence_v1"
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
class CanonicalCatcherBaserunningObservation:
    """
    Immutable event-level throwing evidence for one catcher.

    Steal attempts against and caught-stealing outcomes must be attributed
    to this catcher. Team-level totals are not accepted as substitutes.
    """

    catcher_id: str
    team_side: str
    steal_attempts_against: int
    caught_stealing: int
    pop_time_score: float
    source_version: str = (
        CANONICAL_CATCHER_BASERUNNING_EVIDENCE_VERSION
    )

    def __post_init__(self) -> None:
        if not self.catcher_id:
            raise ValueError(
                "catcher_id is required"
            )

        if self.team_side not in {
            "away",
            "home",
        }:
            raise ValueError(
                "team_side must be away or home"
            )

        for name, value in (
            (
                "steal_attempts_against",
                self.steal_attempts_against,
            ),
            (
                "caught_stealing",
                self.caught_stealing,
            ),
        ):
            _validate_count(
                name=name,
                value=value,
            )

        if (
            self.caught_stealing
            > self.steal_attempts_against
        ):
            raise ValueError(
                "caught stealing cannot exceed "
                "steal attempts against"
            )

        _validate_rate(
            name="pop_time_score",
            value=self.pop_time_score,
        )

        if not self.source_version:
            raise ValueError(
                "source_version is required"
            )

    @property
    def throwing_score(self) -> float:
        if self.steal_attempts_against == 0:
            return 0.0

        return round(
            self.caught_stealing
            / self.steal_attempts_against,
            6,
        )

    @property
    def digest(self) -> str:
        parts = (
            self.source_version,
            self.catcher_id,
            self.team_side,
            str(self.steal_attempts_against),
            str(self.caught_stealing),
            repr(float(self.pop_time_score)),
        )

        return hashlib.sha256(
            "\x1f".join(parts).encode("utf-8")
        ).hexdigest()

    def to_profile(
        self,
    ) -> CanonicalCatcherBaserunningProfile:
        return CanonicalCatcherBaserunningProfile(
            catcher_id=self.catcher_id,
            team_side=self.team_side,
            throwing_score=self.throwing_score,
            pop_time_score=float(
                self.pop_time_score
            ),
        )


def adapt_observed_catcher_baserunning_evidence(
    observation: CanonicalCatcherBaserunningObservation,
) -> CanonicalCatcherBaserunningProfile:
    """Return one deterministic canonical catcher profile."""

    if not isinstance(
        observation,
        CanonicalCatcherBaserunningObservation,
    ):
        raise TypeError(
            "observation must be a "
            "CanonicalCatcherBaserunningObservation"
        )

    return observation.to_profile()
