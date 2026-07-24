"""Fail-open discovery of complete baserunning evidence catalogs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from mlb_app.simulation.game import (
    CanonicalBaserunningEvidenceCatalog,
    CanonicalCatcherBaserunningProfile,
    CanonicalPitcherBaserunningProfile,
    CanonicalRunnerBaserunningProfile,
)


CANONICAL_SHADOW_BASERUNNING_DISCOVERY_VERSION = (
    "canonical_shadow_baserunning_discovery_v1"
)

_VALID_STATUSES = {
    "ready",
    "unavailable",
    "error",
}


@dataclass(frozen=True)
class CanonicalShadowBaserunningEvidenceDiscovery:
    """Result of validating one matchup's complete evidence set."""

    catalog: Optional[
        CanonicalBaserunningEvidenceCatalog
    ] = None
    requested_runner_count: int = 0
    available_runner_count: int = 0
    requested_pitcher_count: int = 0
    available_pitcher_count: int = 0
    status: str = "unavailable"
    error_message: Optional[str] = None
    discovery_version: str = (
        CANONICAL_SHADOW_BASERUNNING_DISCOVERY_VERSION
    )

    def __post_init__(self) -> None:
        if self.status not in _VALID_STATUSES:
            raise ValueError(
                "unsupported baserunning discovery status"
            )

        for name, value in (
            (
                "requested_runner_count",
                self.requested_runner_count,
            ),
            (
                "available_runner_count",
                self.available_runner_count,
            ),
            (
                "requested_pitcher_count",
                self.requested_pitcher_count,
            ),
            (
                "available_pitcher_count",
                self.available_pitcher_count,
            ),
        ):
            if value < 0:
                raise ValueError(
                    f"{name} must be nonnegative"
                )

        if (
            self.catalog is not None
            and not isinstance(
                self.catalog,
                CanonicalBaserunningEvidenceCatalog,
            )
        ):
            raise TypeError(
                "catalog must be "
                "CanonicalBaserunningEvidenceCatalog or None"
            )

        if self.status == "ready":
            if self.catalog is None:
                raise ValueError(
                    "ready discovery requires a catalog"
                )
            if (
                self.available_runner_count
                != self.requested_runner_count
            ):
                raise ValueError(
                    "ready discovery requires complete runner evidence"
                )
            if (
                self.available_pitcher_count
                != self.requested_pitcher_count
            ):
                raise ValueError(
                    "ready discovery requires complete pitcher evidence"
                )
        elif self.catalog is not None:
            raise ValueError(
                "non-ready discovery cannot expose a catalog"
            )

        if self.discovery_version != (
            CANONICAL_SHADOW_BASERUNNING_DISCOVERY_VERSION
        ):
            raise ValueError(
                "unsupported baserunning discovery version"
            )

    @property
    def ready(self) -> bool:
        return (
            self.status == "ready"
            and self.catalog is not None
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.discovery_version,
            "status": self.status,
            "ready": self.ready,
            "requested_runner_count": (
                self.requested_runner_count
            ),
            "available_runner_count": (
                self.available_runner_count
            ),
            "requested_pitcher_count": (
                self.requested_pitcher_count
            ),
            "available_pitcher_count": (
                self.available_pitcher_count
            ),
            "catalog_digest": (
                self.catalog.digest
                if self.catalog is not None
                else None
            ),
            "error_message": self.error_message,
            "production_activation": False,
            "authoritative_source": "legacy",
        }


def _normalized_ids(
    values: Tuple[str, ...],
    *,
    name: str,
) -> Tuple[str, ...]:
    normalized = tuple(
        str(value).strip()
        for value in values
    )

    if any(not value for value in normalized):
        raise ValueError(
            f"{name} identifiers must be nonempty"
        )

    if len(normalized) != len(set(normalized)):
        raise ValueError(
            f"{name} identifiers must be unique"
        )

    return normalized


def discover_canonical_shadow_baserunning_evidence(
    *,
    required_runner_ids: Tuple[str, ...],
    required_pitcher_ids: Tuple[str, ...],
    runner_profiles: Tuple[
        CanonicalRunnerBaserunningProfile,
        ...,
    ] = (),
    pitcher_profiles: Tuple[
        CanonicalPitcherBaserunningProfile,
        ...,
    ] = (),
    away_catcher: Optional[
        CanonicalCatcherBaserunningProfile
    ] = None,
    home_catcher: Optional[
        CanonicalCatcherBaserunningProfile
    ] = None,
) -> CanonicalShadowBaserunningEvidenceDiscovery:
    """
    Build a catalog only when every required identity has evidence.

    Missing or invalid evidence fails open and never activates production
    behavior or changes legacy authority.
    """

    requested_runner_count = len(required_runner_ids)
    requested_pitcher_count = len(required_pitcher_ids)

    try:
        runner_ids = _normalized_ids(
            required_runner_ids,
            name="runner",
        )
        pitcher_ids = _normalized_ids(
            required_pitcher_ids,
            name="pitcher",
        )

        supplied_runners = {
            profile.runner_id: profile
            for profile in runner_profiles
        }
        supplied_pitchers = {
            profile.pitcher_id: profile
            for profile in pitcher_profiles
        }

        available_runners = tuple(
            supplied_runners[runner_id]
            for runner_id in runner_ids
            if runner_id in supplied_runners
        )
        available_pitchers = tuple(
            supplied_pitchers[pitcher_id]
            for pitcher_id in pitcher_ids
            if pitcher_id in supplied_pitchers
        )

        complete = (
            len(available_runners) == len(runner_ids)
            and len(available_pitchers) == len(pitcher_ids)
            and away_catcher is not None
            and home_catcher is not None
        )

        if not complete:
            return (
                CanonicalShadowBaserunningEvidenceDiscovery(
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
                )
            )

        catalog = CanonicalBaserunningEvidenceCatalog(
            runners=available_runners,
            pitchers=available_pitchers,
            away_catcher=away_catcher,
            home_catcher=home_catcher,
        )

        return CanonicalShadowBaserunningEvidenceDiscovery(
            catalog=catalog,
            requested_runner_count=len(runner_ids),
            available_runner_count=len(
                available_runners
            ),
            requested_pitcher_count=len(pitcher_ids),
            available_pitcher_count=len(
                available_pitchers
            ),
            status="ready",
        )
    except Exception as exc:
        return CanonicalShadowBaserunningEvidenceDiscovery(
            requested_runner_count=requested_runner_count,
            requested_pitcher_count=requested_pitcher_count,
            status="error",
            error_message=str(exc),
        )
