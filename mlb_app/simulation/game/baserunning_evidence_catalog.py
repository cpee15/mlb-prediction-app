"""Versioned immutable baserunning evidence catalog."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Callable, Optional, Tuple

from mlb_app.simulation.events import Base

from .baserunning_evidence_adapter import (
    CanonicalBaserunningStateProvider,
)
from .baserunning_resolver import (
    CanonicalBaserunningEvidenceQuery,
)


CANONICAL_BASERUNNING_EVIDENCE_CATALOG_VERSION = (
    "canonical_baserunning_evidence_catalog_v1"
)

_BASE_NAMES = {
    Base.FIRST: "first",
    Base.SECOND: "second",
    Base.THIRD: "third",
}


def _validate_rate(
    *,
    name: str,
    value: float,
) -> None:
    if not 0.0 <= value <= 1.0:
        raise ValueError(
            f"{name} must be between 0 and 1"
        )


@dataclass(frozen=True)
class CanonicalRunnerBaserunningProfile:
    """Complete runner evidence consumed by the evaluator."""

    runner_id: str
    speed_score: float
    attempt_rate: float
    success_rate: float
    lead_quality: float
    fatigue_index: float
    injury_limit_flag: bool = False

    def __post_init__(self) -> None:
        if not self.runner_id:
            raise ValueError(
                "runner_id is required"
            )

        for name, value in (
            ("speed_score", self.speed_score),
            ("attempt_rate", self.attempt_rate),
            ("success_rate", self.success_rate),
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


@dataclass(frozen=True)
class CanonicalPitcherBaserunningProfile:
    """Complete pitcher hold and pickoff evidence."""

    pitcher_id: str
    hold_score: float
    delivery_time_score: float
    pickoff_attempt_rate: float
    pickoff_success_rate: float

    def __post_init__(self) -> None:
        if not self.pitcher_id:
            raise ValueError(
                "pitcher_id is required"
            )

        for name, value in (
            ("hold_score", self.hold_score),
            (
                "delivery_time_score",
                self.delivery_time_score,
            ),
            (
                "pickoff_attempt_rate",
                self.pickoff_attempt_rate,
            ),
            (
                "pickoff_success_rate",
                self.pickoff_success_rate,
            ),
        ):
            _validate_rate(
                name=name,
                value=value,
            )


@dataclass(frozen=True)
class CanonicalCatcherBaserunningProfile:
    """Complete catcher throwing evidence for one team."""

    catcher_id: str
    team_side: str
    throwing_score: float
    pop_time_score: float

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

        _validate_rate(
            name="throwing_score",
            value=self.throwing_score,
        )
        _validate_rate(
            name="pop_time_score",
            value=self.pop_time_score,
        )


@dataclass(frozen=True)
class CanonicalBaserunningEvidenceCatalog:
    """Immutable complete profiles available for one matchup."""

    runners: Tuple[
        CanonicalRunnerBaserunningProfile,
        ...,
    ]
    pitchers: Tuple[
        CanonicalPitcherBaserunningProfile,
        ...,
    ]
    away_catcher: CanonicalCatcherBaserunningProfile
    home_catcher: CanonicalCatcherBaserunningProfile
    catalog_version: str = (
        CANONICAL_BASERUNNING_EVIDENCE_CATALOG_VERSION
    )

    def __post_init__(self) -> None:
        if self.away_catcher.team_side != "away":
            raise ValueError(
                "away_catcher must use away team side"
            )
        if self.home_catcher.team_side != "home":
            raise ValueError(
                "home_catcher must use home team side"
            )

        runner_ids = tuple(
            profile.runner_id
            for profile in self.runners
        )
        if len(runner_ids) != len(set(runner_ids)):
            raise ValueError(
                "runner profile identifiers must be unique"
            )

        pitcher_ids = tuple(
            profile.pitcher_id
            for profile in self.pitchers
        )
        if len(pitcher_ids) != len(set(pitcher_ids)):
            raise ValueError(
                "pitcher profile identifiers must be unique"
            )

        if self.catalog_version != (
            CANONICAL_BASERUNNING_EVIDENCE_CATALOG_VERSION
        ):
            raise ValueError(
                "unsupported baserunning evidence catalog version"
            )

    @property
    def digest(self) -> str:
        """Return deterministic provenance for the complete catalog."""

        parts = [
            self.catalog_version,
        ]

        for profile in sorted(
            self.runners,
            key=lambda value: value.runner_id,
        ):
            parts.extend(
                (
                    "runner",
                    profile.runner_id,
                    repr(profile.speed_score),
                    repr(profile.attempt_rate),
                    repr(profile.success_rate),
                    repr(profile.lead_quality),
                    repr(profile.fatigue_index),
                    repr(profile.injury_limit_flag),
                )
            )

        for profile in sorted(
            self.pitchers,
            key=lambda value: value.pitcher_id,
        ):
            parts.extend(
                (
                    "pitcher",
                    profile.pitcher_id,
                    repr(profile.hold_score),
                    repr(profile.delivery_time_score),
                    repr(profile.pickoff_attempt_rate),
                    repr(profile.pickoff_success_rate),
                )
            )

        for profile in (
            self.away_catcher,
            self.home_catcher,
        ):
            parts.extend(
                (
                    "catcher",
                    profile.team_side,
                    profile.catcher_id,
                    repr(profile.throwing_score),
                    repr(profile.pop_time_score),
                )
            )

        return hashlib.sha256(
            "\x1f".join(parts).encode("utf-8")
        ).hexdigest()

    def runner_profile(
        self,
        runner_id: str,
    ) -> Optional[CanonicalRunnerBaserunningProfile]:
        return next(
            (
                profile
                for profile in self.runners
                if profile.runner_id == runner_id
            ),
            None,
        )

    def pitcher_profile(
        self,
        pitcher_id: str,
    ) -> Optional[CanonicalPitcherBaserunningProfile]:
        return next(
            (
                profile
                for profile in self.pitchers
                if profile.pitcher_id == pitcher_id
            ),
            None,
        )

    def fielding_catcher(
        self,
        half: str,
    ) -> CanonicalCatcherBaserunningProfile:
        if half == "top":
            return self.home_catcher
        if half == "bottom":
            return self.away_catcher
        raise ValueError(
            "half must be top or bottom"
        )


CanonicalActivePitcherProvider = Callable[
    [CanonicalBaserunningEvidenceQuery],
    Optional[str],
]


@dataclass(frozen=True)
class CanonicalBaserunningCatalogStateProvider:
    """Assemble evaluator state from immutable matchup profiles."""

    catalog: CanonicalBaserunningEvidenceCatalog
    active_pitcher_provider: CanonicalActivePitcherProvider

    def __post_init__(self) -> None:
        if not isinstance(
            self.catalog,
            CanonicalBaserunningEvidenceCatalog,
        ):
            raise TypeError(
                "catalog must be "
                "CanonicalBaserunningEvidenceCatalog"
            )
        if not callable(self.active_pitcher_provider):
            raise TypeError(
                "active_pitcher_provider must be callable"
            )

    def __call__(
        self,
        query: CanonicalBaserunningEvidenceQuery,
    ):
        if not isinstance(
            query,
            CanonicalBaserunningEvidenceQuery,
        ):
            return None

        runner = self.catalog.runner_profile(
            query.runner_id
        )
        if runner is None:
            return None

        try:
            pitcher_id = self.active_pitcher_provider(
                query
            )
        except Exception:
            return None

        if not pitcher_id:
            return None

        pitcher = self.catalog.pitcher_profile(
            str(pitcher_id)
        )
        if pitcher is None:
            return None

        catcher = self.catalog.fielding_catcher(
            query.state.half
        )
        state = query.state

        score_margin = (
            state.away_score - state.home_score
            if state.half == "top"
            else state.home_score - state.away_score
        )

        return {
            "inning": state.inning,
            "half": state.half,
            "outs": state.outs,
            "base_state": {
                "first": state.first is not None,
                "second": state.second is not None,
                "third": state.third is not None,
            },
            "score_margin": score_margin,
            "runner": {
                "runner_id": runner.runner_id,
                "evidence_complete": True,
                "speed_score": runner.speed_score,
                "attempt_rate": runner.attempt_rate,
                "success_rate": runner.success_rate,
                "lead_quality": runner.lead_quality,
                "fatigue_index": runner.fatigue_index,
                "injury_limit_flag": (
                    runner.injury_limit_flag
                ),
            },
            "origin_base": _BASE_NAMES[
                query.origin_base
            ],
            "target_base": _BASE_NAMES[
                query.target_base
            ],
            "pitcher": {
                "pitcher_id": pitcher.pitcher_id,
                "evidence_complete": True,
                "hold_score": pitcher.hold_score,
                "delivery_time_score": (
                    pitcher.delivery_time_score
                ),
                "pickoff_attempt_rate": (
                    pitcher.pickoff_attempt_rate
                ),
                "pickoff_success_rate": (
                    pitcher.pickoff_success_rate
                ),
            },
            "catcher": {
                "catcher_id": catcher.catcher_id,
                "evidence_complete": True,
                "throwing_score": (
                    catcher.throwing_score
                ),
                "pop_time_score": (
                    catcher.pop_time_score
                ),
            },
        }


def build_canonical_baserunning_state_provider(
    *,
    catalog: CanonicalBaserunningEvidenceCatalog,
    active_pitcher_provider: CanonicalActivePitcherProvider,
) -> CanonicalBaserunningStateProvider:
    """Build an explicit fail-open catalog-backed provider."""

    return CanonicalBaserunningCatalogStateProvider(
        catalog=catalog,
        active_pitcher_provider=active_pitcher_provider,
    )
