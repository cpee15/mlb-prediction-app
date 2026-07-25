"""Materialize calibration-only historical baserunning profiles."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Tuple

from mlb_app.simulation.game import (
    CanonicalBaserunningEvidenceCatalog,
    CanonicalCatcherBaserunningProfile,
    CanonicalPitcherBaserunningProfile,
    CanonicalRunnerBaserunningProfile,
)

from .historical_baserunning_replay_evidence_source import (
    HISTORICAL_BASERUNNING_CALIBRATION_PROXY_POLICY,
    HISTORICAL_BASERUNNING_EVIDENCE_QUALITY,
)


CANONICAL_HISTORICAL_BASERUNNING_PROFILE_MATERIALIZATION_VERSION = (
    "canonical_historical_baserunning_profile_materialization_v1"
)

_DEFAULT_RUNNER_ATTEMPT_RATE = 0.05
_DEFAULT_RUNNER_SUCCESS_RATE = 0.75
_DEFAULT_PITCHER_ALLOWED_ATTEMPT_RATE = 0.05
_DEFAULT_PICKOFF_SUCCESS_RATE = 0.10
_DEFAULT_CATCHER_THROWING_RATE = 0.25


def _identifier(value: Any, name: str) -> str:
    if value in (None, "") or isinstance(value, bool):
        raise ValueError(f"{name} is required")

    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{name} must be a positive integer string"
        ) from exc

    if parsed <= 0 or str(parsed) != str(value):
        raise ValueError(
            f"{name} must be a positive integer string"
        )

    return str(parsed)


def _count(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(
            f"{name} must be a nonnegative integer"
        )

    try:
        parsed = int(value)
        parsed_float = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{name} must be a nonnegative integer"
        ) from exc

    if parsed < 0 or parsed_float != parsed:
        raise ValueError(
            f"{name} must be a nonnegative integer"
        )

    return parsed


def _clamp(value: float) -> float:
    return min(max(float(value), 0.0), 1.0)


def _rate(
    numerator: int,
    denominator: int,
    fallback: float,
) -> float:
    if denominator <= 0:
        return fallback

    return _clamp(numerator / denominator)


@dataclass(frozen=True)
class CanonicalHistoricalRunnerBaserunningCounts:
    runner_id: str
    opportunity_count: int
    stolen_bases: int
    caught_stealing: int

    def __post_init__(self) -> None:
        _identifier(self.runner_id, "runner_id")

        for name, value in (
            ("opportunity_count", self.opportunity_count),
            ("stolen_bases", self.stolen_bases),
            ("caught_stealing", self.caught_stealing),
        ):
            _count(value, name)

        if (
            self.stolen_bases + self.caught_stealing
            > self.opportunity_count
        ):
            raise ValueError(
                "runner attempts cannot exceed opportunities"
            )

    @property
    def attempt_count(self) -> int:
        return self.stolen_bases + self.caught_stealing

    @property
    def sample_available(self) -> bool:
        return self.opportunity_count > 0


@dataclass(frozen=True)
class CanonicalHistoricalPitcherBaserunningCounts:
    pitcher_id: str
    batters_faced: int
    stolen_bases_allowed: int
    caught_stealing: int
    pickoffs: int

    def __post_init__(self) -> None:
        _identifier(self.pitcher_id, "pitcher_id")

        for name, value in (
            ("batters_faced", self.batters_faced),
            (
                "stolen_bases_allowed",
                self.stolen_bases_allowed,
            ),
            ("caught_stealing", self.caught_stealing),
            ("pickoffs", self.pickoffs),
        ):
            _count(value, name)

    @property
    def attempt_count(self) -> int:
        return (
            self.stolen_bases_allowed
            + self.caught_stealing
        )

    @property
    def sample_available(self) -> bool:
        return self.batters_faced > 0


@dataclass(frozen=True)
class CanonicalHistoricalCatcherBaserunningCounts:
    catcher_id: str
    team_side: str
    stolen_bases_allowed: int
    caught_stealing: int

    def __post_init__(self) -> None:
        _identifier(self.catcher_id, "catcher_id")

        if self.team_side not in {"away", "home"}:
            raise ValueError(
                "team_side must be away or home"
            )

        _count(
            self.stolen_bases_allowed,
            "stolen_bases_allowed",
        )
        _count(
            self.caught_stealing,
            "caught_stealing",
        )

    @property
    def attempt_count(self) -> int:
        return (
            self.stolen_bases_allowed
            + self.caught_stealing
        )

    @property
    def sample_available(self) -> bool:
        return self.attempt_count > 0


@dataclass(frozen=True)
class CanonicalHistoricalBaserunningCatalogMaterialization:
    catalog: CanonicalBaserunningEvidenceCatalog
    direct_evidence_count: int
    proxy_evidence_count: int
    fallback_evidence_count: int
    materialization_version: str = (
        CANONICAL_HISTORICAL_BASERUNNING_PROFILE_MATERIALIZATION_VERSION
    )

    def __post_init__(self) -> None:
        if not isinstance(
            self.catalog,
            CanonicalBaserunningEvidenceCatalog,
        ):
            raise TypeError(
                "catalog must be "
                "CanonicalBaserunningEvidenceCatalog"
            )

        for name, value in (
            (
                "direct_evidence_count",
                self.direct_evidence_count,
            ),
            (
                "proxy_evidence_count",
                self.proxy_evidence_count,
            ),
            (
                "fallback_evidence_count",
                self.fallback_evidence_count,
            ),
        ):
            _count(value, name)

        if self.materialization_version != (
            CANONICAL_HISTORICAL_BASERUNNING_PROFILE_MATERIALIZATION_VERSION
        ):
            raise ValueError(
                "unsupported historical baserunning "
                "profile materialization version"
            )

    @property
    def evidence_counts(self) -> Tuple[int, int, int]:
        return (
            self.direct_evidence_count,
            self.proxy_evidence_count,
            self.fallback_evidence_count,
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.materialization_version,
            "ready": True,
            "catalog_digest": self.catalog.digest,
            "runner_count": len(self.catalog.runners),
            "pitcher_count": len(self.catalog.pitchers),
            "catcher_count": 2,
            "direct_evidence_count": (
                self.direct_evidence_count
            ),
            "proxy_evidence_count": (
                self.proxy_evidence_count
            ),
            "fallback_evidence_count": (
                self.fallback_evidence_count
            ),
            "evidence_quality": (
                HISTORICAL_BASERUNNING_EVIDENCE_QUALITY
            ),
            "tracking_proxy_policy": (
                HISTORICAL_BASERUNNING_CALIBRATION_PROXY_POLICY
            ),
            "direct_outcome_evidence": True,
            "tracking_observations_available": False,
            "zero_sample_rows_labeled_direct": False,
            "target_game_outcomes_used": False,
            "future_data_permitted": False,
            "activation_permitted": False,
            "authoritative_source": "legacy",
        }


def _runner_priors(
    values: Tuple[
        CanonicalHistoricalRunnerBaserunningCounts,
        ...,
    ],
) -> Tuple[float, float]:
    opportunities = sum(
        value.opportunity_count
        for value in values
    )
    attempts = sum(
        value.attempt_count
        for value in values
    )
    stolen_bases = sum(
        value.stolen_bases
        for value in values
    )

    return (
        _rate(
            attempts,
            opportunities,
            _DEFAULT_RUNNER_ATTEMPT_RATE,
        ),
        _rate(
            stolen_bases,
            attempts,
            _DEFAULT_RUNNER_SUCCESS_RATE,
        ),
    )


def _pitcher_priors(
    values: Tuple[
        CanonicalHistoricalPitcherBaserunningCounts,
        ...,
    ],
) -> Tuple[float, float]:
    batters_faced = sum(
        value.batters_faced
        for value in values
    )
    attempts = sum(
        value.attempt_count
        for value in values
    )
    pickoffs = sum(
        value.pickoffs
        for value in values
    )

    return (
        _rate(
            attempts,
            batters_faced,
            _DEFAULT_PITCHER_ALLOWED_ATTEMPT_RATE,
        ),
        _rate(
            pickoffs,
            pickoffs + attempts,
            _DEFAULT_PICKOFF_SUCCESS_RATE,
        ),
    )


def _catcher_prior(
    values: Tuple[
        CanonicalHistoricalCatcherBaserunningCounts,
        ...,
    ],
) -> float:
    attempts = sum(
        value.attempt_count
        for value in values
    )
    caught = sum(
        value.caught_stealing
        for value in values
    )

    return _rate(
        caught,
        attempts,
        _DEFAULT_CATCHER_THROWING_RATE,
    )


def materialize_historical_baserunning_profiles(
    *,
    required_runner_ids: Tuple[str, ...],
    required_pitcher_ids: Tuple[str, ...],
    runner_counts: Tuple[
        CanonicalHistoricalRunnerBaserunningCounts,
        ...,
    ],
    pitcher_counts: Tuple[
        CanonicalHistoricalPitcherBaserunningCounts,
        ...,
    ],
    away_catcher_counts: (
        CanonicalHistoricalCatcherBaserunningCounts
    ),
    home_catcher_counts: (
        CanonicalHistoricalCatcherBaserunningCounts
    ),
) -> CanonicalHistoricalBaserunningCatalogMaterialization:
    """
    Convert cutoff-safe direct counts into one complete replay catalog.

    Attempt, success, hold, and throwing rates use direct historical
    outcomes. Tracking-only fields use explicit calibration proxies.
    Zero-sample identities use window priors and are counted as fallbacks.
    """

    runner_ids = tuple(
        _identifier(value, "runner_id")
        for value in required_runner_ids
    )
    pitcher_ids = tuple(
        _identifier(value, "pitcher_id")
        for value in required_pitcher_ids
    )

    if len(runner_ids) != len(set(runner_ids)):
        raise ValueError(
            "required runner identifiers must be unique"
        )
    if len(pitcher_ids) != len(set(pitcher_ids)):
        raise ValueError(
            "required pitcher identifiers must be unique"
        )

    runners_by_id = {
        value.runner_id: value
        for value in runner_counts
    }
    pitchers_by_id = {
        value.pitcher_id: value
        for value in pitcher_counts
    }

    if len(runners_by_id) != len(runner_counts):
        raise ValueError(
            "runner count identifiers must be unique"
        )
    if len(pitchers_by_id) != len(pitcher_counts):
        raise ValueError(
            "pitcher count identifiers must be unique"
        )
    if set(runners_by_id) != set(runner_ids):
        raise ValueError(
            "runner counts must exactly cover required runners"
        )
    if set(pitchers_by_id) != set(pitcher_ids):
        raise ValueError(
            "pitcher counts must exactly cover required pitchers"
        )

    if away_catcher_counts.team_side != "away":
        raise ValueError(
            "away catcher counts must use away side"
        )
    if home_catcher_counts.team_side != "home":
        raise ValueError(
            "home catcher counts must use home side"
        )

    runner_attempt_prior, runner_success_prior = (
        _runner_priors(runner_counts)
    )
    pitcher_attempt_prior, pickoff_success_prior = (
        _pitcher_priors(pitcher_counts)
    )
    catcher_prior = _catcher_prior(
        (
            away_catcher_counts,
            home_catcher_counts,
        )
    )

    runner_profiles = []
    for runner_id in runner_ids:
        counts = runners_by_id[runner_id]
        attempt_rate = _rate(
            counts.attempt_count,
            counts.opportunity_count,
            runner_attempt_prior,
        )
        success_rate = _rate(
            counts.stolen_bases,
            counts.attempt_count,
            runner_success_prior,
        )

        runner_profiles.append(
            CanonicalRunnerBaserunningProfile(
                runner_id=runner_id,
                speed_score=_clamp(
                    attempt_rate / 0.15
                ),
                attempt_rate=attempt_rate,
                success_rate=success_rate,
                lead_quality=success_rate,
                fatigue_index=0.0,
                injury_limit_flag=False,
            )
        )

    pitcher_profiles = []
    for pitcher_id in pitcher_ids:
        counts = pitchers_by_id[pitcher_id]
        allowed_attempt_rate = _rate(
            counts.attempt_count,
            counts.batters_faced,
            pitcher_attempt_prior,
        )
        hold_score = 1.0 - _clamp(
            allowed_attempt_rate / 0.10
        )
        pickoff_success_rate = _rate(
            counts.pickoffs,
            counts.pickoffs + counts.attempt_count,
            pickoff_success_prior,
        )

        pitcher_profiles.append(
            CanonicalPitcherBaserunningProfile(
                pitcher_id=pitcher_id,
                hold_score=hold_score,
                delivery_time_score=hold_score,
                pickoff_attempt_rate=_rate(
                    counts.pickoffs,
                    counts.batters_faced,
                    0.0,
                ),
                pickoff_success_rate=(
                    pickoff_success_rate
                ),
            )
        )

    def catcher_profile(
        counts: (
            CanonicalHistoricalCatcherBaserunningCounts
        ),
    ) -> CanonicalCatcherBaserunningProfile:
        throwing_score = _rate(
            counts.caught_stealing,
            counts.attempt_count,
            catcher_prior,
        )

        return CanonicalCatcherBaserunningProfile(
            catcher_id=counts.catcher_id,
            team_side=counts.team_side,
            throwing_score=throwing_score,
            pop_time_score=throwing_score,
        )

    catalog = CanonicalBaserunningEvidenceCatalog(
        runners=tuple(runner_profiles),
        pitchers=tuple(pitcher_profiles),
        away_catcher=catcher_profile(
            away_catcher_counts
        ),
        home_catcher=catcher_profile(
            home_catcher_counts
        ),
    )

    direct_count = sum(
        value.sample_available
        for value in runner_counts
    )
    direct_count += sum(
        value.sample_available
        for value in pitcher_counts
    )
    direct_count += sum(
        value.sample_available
        for value in (
            away_catcher_counts,
            home_catcher_counts,
        )
    )

    identity_count = (
        len(runner_counts)
        + len(pitcher_counts)
        + 2
    )
    fallback_count = identity_count - direct_count

    proxy_count = (
        len(runner_counts) * 3
        + len(pitcher_counts) * 2
        + 2
    )

    return CanonicalHistoricalBaserunningCatalogMaterialization(
        catalog=catalog,
        direct_evidence_count=direct_count,
        proxy_evidence_count=proxy_count,
        fallback_evidence_count=fallback_count,
    )
