"""Versioned production prior for calibrated baserunning evidence."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Mapping, Tuple

from mlb_app.simulation.game import (
    CanonicalBaserunningEvidenceCatalog,
    CanonicalCatcherBaserunningProfile,
    CanonicalPitcherBaserunningProfile,
    CanonicalRunnerBaserunningProfile,
)

from .baserunning_evidence_discovery import (
    CanonicalShadowBaserunningEvidenceDiscovery,
)
from .historical_baserunning_replay_evidence_source import (
    CanonicalHistoricalBaserunningReplayEvidenceWindow,
)


CANONICAL_BASERUNNING_PRODUCTION_PRIOR_VERSION = (
    "canonical_baserunning_production_prior_v1"
)
BASERUNNING_PRODUCTION_FALLBACK_POLICY_VERSION = (
    "baserunning_production_fallback_policy_v1"
)

_FALLBACK_RUNNER_ATTEMPT_RATE = 0.05
_FALLBACK_RUNNER_SUCCESS_RATE = 0.75
_FALLBACK_PITCHER_ALLOWED_ATTEMPT_RATE = 0.05
_FALLBACK_PICKOFF_SUCCESS_RATE = 0.10
_FALLBACK_CATCHER_THROWING_RATE = 0.25

DEFAULT_BASERUNNING_PRODUCTION_PRIOR_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "baserunning"
    / "production_prior.json"
)


def _sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _identifier(value: Any, name: str) -> str:
    if value in (None, "") or isinstance(value, bool):
        raise ValueError(f"{name} is required")

    normalized = str(value).strip()

    if not normalized:
        raise ValueError(f"{name} is required")

    return normalized


@dataclass(frozen=True)
class CanonicalBaserunningProductionPriorCatcher:
    catcher_id: str
    throwing_score: float
    pop_time_score: float

    def __post_init__(self) -> None:
        _identifier(self.catcher_id, "catcher_id")

        for name, value in (
            ("throwing_score", self.throwing_score),
            ("pop_time_score", self.pop_time_score),
        ):
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not 0.0 <= float(value) <= 1.0
            ):
                raise ValueError(
                    f"{name} must be between 0 and 1"
                )


@dataclass(frozen=True)
class CanonicalBaserunningProductionPrior:
    source_through_date: str
    runners: Tuple[
        CanonicalRunnerBaserunningProfile,
        ...,
    ]
    pitchers: Tuple[
        CanonicalPitcherBaserunningProfile,
        ...,
    ]
    catchers: Tuple[
        CanonicalBaserunningProductionPriorCatcher,
        ...,
    ]
    direct_evidence_count: int
    proxy_evidence_count: int
    fallback_evidence_count: int
    prior_version: str = (
        CANONICAL_BASERUNNING_PRODUCTION_PRIOR_VERSION
    )

    def __post_init__(self) -> None:
        if not self.source_through_date:
            raise ValueError(
                "source_through_date is required"
            )

        for name, values, value_type, id_name in (
            (
                "runners",
                self.runners,
                CanonicalRunnerBaserunningProfile,
                "runner_id",
            ),
            (
                "pitchers",
                self.pitchers,
                CanonicalPitcherBaserunningProfile,
                "pitcher_id",
            ),
            (
                "catchers",
                self.catchers,
                CanonicalBaserunningProductionPriorCatcher,
                "catcher_id",
            ),
        ):
            if not isinstance(values, tuple):
                raise TypeError(
                    f"{name} must be a tuple"
                )
            if any(
                not isinstance(value, value_type)
                for value in values
            ):
                raise TypeError(
                    f"{name} must contain canonical values"
                )

            identifiers = tuple(
                getattr(value, id_name)
                for value in values
            )
            if len(identifiers) != len(set(identifiers)):
                raise ValueError(
                    f"{name} identifiers must be unique"
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
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                raise ValueError(
                    f"{name} must be nonnegative"
                )

        if self.prior_version != (
            CANONICAL_BASERUNNING_PRODUCTION_PRIOR_VERSION
        ):
            raise ValueError(
                "unsupported baserunning production "
                "prior version"
            )

    def _content(self) -> Dict[str, Any]:
        return {
            "schema_version": self.prior_version,
            "source_through_date": (
                self.source_through_date
            ),
            "runners": [
                {
                    "runner_id": value.runner_id,
                    "speed_score": value.speed_score,
                    "attempt_rate": value.attempt_rate,
                    "success_rate": value.success_rate,
                    "lead_quality": value.lead_quality,
                    "fatigue_index": value.fatigue_index,
                    "injury_limit_flag": (
                        value.injury_limit_flag
                    ),
                }
                for value in self.runners
            ],
            "pitchers": [
                {
                    "pitcher_id": value.pitcher_id,
                    "hold_score": value.hold_score,
                    "delivery_time_score": (
                        value.delivery_time_score
                    ),
                    "pickoff_attempt_rate": (
                        value.pickoff_attempt_rate
                    ),
                    "pickoff_success_rate": (
                        value.pickoff_success_rate
                    ),
                }
                for value in self.pitchers
            ],
            "catchers": [
                {
                    "catcher_id": value.catcher_id,
                    "throwing_score": (
                        value.throwing_score
                    ),
                    "pop_time_score": (
                        value.pop_time_score
                    ),
                }
                for value in self.catchers
            ],
            "direct_evidence_count": (
                self.direct_evidence_count
            ),
            "proxy_evidence_count": (
                self.proxy_evidence_count
            ),
            "fallback_evidence_count": (
                self.fallback_evidence_count
            ),
        }

    @property
    def digest(self) -> str:
        return _sha256(self._content())

    def to_payload(self) -> Dict[str, Any]:
        payload = self._content()
        payload["artifact_digest"] = self.digest
        return payload

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.prior_version,
            "source_through_date": (
                self.source_through_date
            ),
            "runner_count": len(self.runners),
            "pitcher_count": len(self.pitchers),
            "catcher_count": len(self.catchers),
            "direct_evidence_count": (
                self.direct_evidence_count
            ),
            "proxy_evidence_count": (
                self.proxy_evidence_count
            ),
            "fallback_evidence_count": (
                self.fallback_evidence_count
            ),
            "artifact_digest": self.digest,
            "production_activation": False,
            "authoritative_source": "legacy",
        }

    def discover_matchup(
        self,
        *,
        required_runner_ids: Tuple[str, ...],
        required_pitcher_ids: Tuple[str, ...],
        away_catcher_id: str,
        home_catcher_id: str,
        allow_fallback_profiles: bool = False,
    ) -> CanonicalShadowBaserunningEvidenceDiscovery:
        if not isinstance(
            allow_fallback_profiles,
            bool,
        ):
            raise TypeError(
                "allow_fallback_profiles must be a bool"
            )
        runner_ids = tuple(
            _identifier(value, "runner_id")
            for value in required_runner_ids
        )
        pitcher_ids = tuple(
            _identifier(value, "pitcher_id")
            for value in required_pitcher_ids
        )
        away_id = _identifier(
            away_catcher_id,
            "away_catcher_id",
        )
        home_id = _identifier(
            home_catcher_id,
            "home_catcher_id",
        )

        runners_by_id = {
            value.runner_id: value
            for value in self.runners
        }
        pitchers_by_id = {
            value.pitcher_id: value
            for value in self.pitchers
        }
        catchers_by_id = {
            value.catcher_id: value
            for value in self.catchers
        }

        available_runners = tuple(
            runners_by_id[value]
            for value in runner_ids
            if value in runners_by_id
        )
        available_pitchers = tuple(
            pitchers_by_id[value]
            for value in pitcher_ids
            if value in pitchers_by_id
        )

        away = catchers_by_id.get(away_id)
        home = catchers_by_id.get(home_id)

        missing_runner_ids = tuple(
            value
            for value in runner_ids
            if value not in runners_by_id
        )
        missing_pitcher_ids = tuple(
            value
            for value in pitcher_ids
            if value not in pitchers_by_id
        )
        missing_catcher_ids = tuple(
            value
            for value in (away_id, home_id)
            if value not in catchers_by_id
        )

        complete = not (
            missing_runner_ids
            or missing_pitcher_ids
            or missing_catcher_ids
        )

        if not complete and not allow_fallback_profiles:
            return CanonicalShadowBaserunningEvidenceDiscovery(
                requested_runner_count=len(runner_ids),
                available_runner_count=len(
                    available_runners
                ),
                requested_pitcher_count=len(
                    pitcher_ids
                ),
                available_pitcher_count=len(
                    available_pitchers
                ),
                status="unavailable",
                observation_digest=self.digest,
            )

        if allow_fallback_profiles:
            for runner_id in missing_runner_ids:
                runners_by_id[runner_id] = (
                    CanonicalRunnerBaserunningProfile(
                        runner_id=runner_id,
                        speed_score=(
                            _FALLBACK_RUNNER_ATTEMPT_RATE
                            / 0.15
                        ),
                        attempt_rate=(
                            _FALLBACK_RUNNER_ATTEMPT_RATE
                        ),
                        success_rate=(
                            _FALLBACK_RUNNER_SUCCESS_RATE
                        ),
                        lead_quality=(
                            _FALLBACK_RUNNER_SUCCESS_RATE
                        ),
                        fatigue_index=0.0,
                        injury_limit_flag=False,
                    )
                )

            fallback_hold_score = 1.0 - (
                _FALLBACK_PITCHER_ALLOWED_ATTEMPT_RATE
                / 0.10
            )
            for pitcher_id in missing_pitcher_ids:
                pitchers_by_id[pitcher_id] = (
                    CanonicalPitcherBaserunningProfile(
                        pitcher_id=pitcher_id,
                        hold_score=fallback_hold_score,
                        delivery_time_score=(
                            fallback_hold_score
                        ),
                        pickoff_attempt_rate=0.0,
                        pickoff_success_rate=(
                            _FALLBACK_PICKOFF_SUCCESS_RATE
                        ),
                    )
                )

            for catcher_id in missing_catcher_ids:
                catchers_by_id[catcher_id] = (
                    CanonicalBaserunningProductionPriorCatcher(
                        catcher_id=catcher_id,
                        throwing_score=(
                            _FALLBACK_CATCHER_THROWING_RATE
                        ),
                        pop_time_score=(
                            _FALLBACK_CATCHER_THROWING_RATE
                        ),
                    )
                )

            away = catchers_by_id[away_id]
            home = catchers_by_id[home_id]

        catalog = CanonicalBaserunningEvidenceCatalog(
            runners=tuple(
                runners_by_id[value]
                for value in runner_ids
            ),
            pitchers=tuple(
                pitchers_by_id[value]
                for value in pitcher_ids
            ),
            away_catcher=(
                CanonicalCatcherBaserunningProfile(
                    catcher_id=away.catcher_id,
                    team_side="away",
                    throwing_score=(
                        away.throwing_score
                    ),
                    pop_time_score=(
                        away.pop_time_score
                    ),
                )
            ),
            home_catcher=(
                CanonicalCatcherBaserunningProfile(
                    catcher_id=home.catcher_id,
                    team_side="home",
                    throwing_score=(
                        home.throwing_score
                    ),
                    pop_time_score=(
                        home.pop_time_score
                    ),
                )
            ),
        )

        return CanonicalShadowBaserunningEvidenceDiscovery(
            catalog=catalog,
            requested_runner_count=len(runner_ids),
            available_runner_count=len(runner_ids),
            requested_pitcher_count=len(pitcher_ids),
            available_pitcher_count=len(
                pitcher_ids
            ),
            status="ready",
            observation_digest=(
                _sha256(
                    {
                        "production_prior_digest": (
                            self.digest
                        ),
                        "catalog_digest": catalog.digest,
                        "fallback_runner_ids": (
                            missing_runner_ids
                        ),
                        "fallback_pitcher_ids": (
                            missing_pitcher_ids
                        ),
                        "fallback_catcher_ids": (
                            missing_catcher_ids
                        ),
                        "fallback_policy_version": (
                            BASERUNNING_PRODUCTION_FALLBACK_POLICY_VERSION
                            if not complete
                            else None
                        ),
                    }
                )
                if not complete
                else self.digest
            ),
            fallback_runner_count=len(
                missing_runner_ids
            ),
            fallback_pitcher_count=len(
                missing_pitcher_ids
            ),
            fallback_catcher_count=len(
                missing_catcher_ids
            ),
            fallback_policy_version=(
                BASERUNNING_PRODUCTION_FALLBACK_POLICY_VERSION
                if not complete
                else None
            ),
        )



def build_baserunning_production_prior(
    evidence: (
        CanonicalHistoricalBaserunningReplayEvidenceWindow
    ),
) -> CanonicalBaserunningProductionPrior:
    """
    Materialize a deterministic production prior.

    Games are ordered by their cutoff-safe statistics date.
    The latest available profile wins for each identity.
    Catcher profiles are stored without matchup-specific team
    assignment and are reassigned during matchup discovery.
    """

    if not isinstance(
        evidence,
        CanonicalHistoricalBaserunningReplayEvidenceWindow,
    ):
        raise TypeError(
            "evidence must be a "
            "CanonicalHistoricalBaserunningReplayEvidenceWindow"
        )

    runners = {}
    pitchers = {}
    catchers = {}

    ordered_games = sorted(
        evidence.games,
        key=lambda value: (
            value.statistics_through_date,
            value.game_date,
            value.game_pk,
        ),
    )

    for game in ordered_games:
        catalog = game.catalog

        for runner in catalog.runners:
            runners[runner.runner_id] = runner

        for pitcher in catalog.pitchers:
            pitchers[pitcher.pitcher_id] = pitcher

        for catcher in (
            catalog.away_catcher,
            catalog.home_catcher,
        ):
            catchers[catcher.catcher_id] = (
                CanonicalBaserunningProductionPriorCatcher(
                    catcher_id=catcher.catcher_id,
                    throwing_score=(
                        catcher.throwing_score
                    ),
                    pop_time_score=(
                        catcher.pop_time_score
                    ),
                )
            )

    return CanonicalBaserunningProductionPrior(
        source_through_date=max(
            value.statistics_through_date
            for value in ordered_games
        ),
        runners=tuple(
            runners[value]
            for value in sorted(runners)
        ),
        pitchers=tuple(
            pitchers[value]
            for value in sorted(pitchers)
        ),
        catchers=tuple(
            catchers[value]
            for value in sorted(catchers)
        ),
        direct_evidence_count=sum(
            value.direct_evidence_count
            for value in ordered_games
        ),
        proxy_evidence_count=sum(
            value.proxy_evidence_count
            for value in ordered_games
        ),
        fallback_evidence_count=sum(
            value.fallback_evidence_count
            for value in ordered_games
        ),
    )

def decode_baserunning_production_prior(
    payload: Mapping[str, Any],
) -> CanonicalBaserunningProductionPrior:
    if not isinstance(payload, Mapping):
        raise TypeError(
            "production-prior payload must be a mapping"
        )

    expected_digest = payload.get(
        "artifact_digest"
    )

    prior = CanonicalBaserunningProductionPrior(
        source_through_date=str(
            payload.get("source_through_date") or ""
        ),
        runners=tuple(
            CanonicalRunnerBaserunningProfile(
                **dict(value)
            )
            for value in payload.get("runners", ())
        ),
        pitchers=tuple(
            CanonicalPitcherBaserunningProfile(
                **dict(value)
            )
            for value in payload.get("pitchers", ())
        ),
        catchers=tuple(
            CanonicalBaserunningProductionPriorCatcher(
                **dict(value)
            )
            for value in payload.get("catchers", ())
        ),
        direct_evidence_count=int(
            payload.get("direct_evidence_count", 0)
        ),
        proxy_evidence_count=int(
            payload.get("proxy_evidence_count", 0)
        ),
        fallback_evidence_count=int(
            payload.get("fallback_evidence_count", 0)
        ),
        prior_version=str(
            payload.get("schema_version") or ""
        ),
    )

    if expected_digest != prior.digest:
        raise ValueError(
            "production-prior artifact digest mismatch"
        )

    return prior



@lru_cache(maxsize=4)
def load_baserunning_production_prior(
    path: Any = (
        DEFAULT_BASERUNNING_PRODUCTION_PRIOR_PATH
    ),
) -> CanonicalBaserunningProductionPrior:
    """
    Load and digest-verify one immutable production prior.

    Cache entries are keyed by the resolved artifact path. Missing,
    malformed, or tampered artifacts raise at this boundary so callers
    can fail open to legacy production authority.
    """

    artifact_path = Path(path).expanduser().resolve()
    payload = json.loads(
        artifact_path.read_text(
            encoding="utf-8"
        )
    )

    return decode_baserunning_production_prior(
        payload
    )
