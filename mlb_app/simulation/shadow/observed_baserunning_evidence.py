"""Assemble observed baserunning evidence for discovery."""

from __future__ import annotations

from dataclasses import replace
import hashlib
from typing import Optional, Tuple

from .baserunning_evidence_discovery import (
    CanonicalShadowBaserunningEvidenceDiscovery,
    discover_canonical_shadow_baserunning_evidence,
)
from .catcher_baserunning_evidence import (
    CanonicalCatcherBaserunningObservation,
    adapt_observed_catcher_baserunning_evidence,
)
from .pitcher_baserunning_evidence import (
    CanonicalPitcherBaserunningObservation,
    adapt_observed_pitcher_baserunning_evidence,
)
from .runner_baserunning_evidence import (
    CanonicalRunnerBaserunningObservation,
    adapt_observed_runner_baserunning_evidence,
)


CANONICAL_OBSERVED_BASERUNNING_DIGEST_VERSION = (
    "canonical_observed_baserunning_digest_v1"
)


def _observation_digest(
    *,
    required_runner_ids: Tuple[str, ...],
    required_pitcher_ids: Tuple[str, ...],
    runner_observations: Tuple[
        CanonicalRunnerBaserunningObservation,
        ...,
    ],
    pitcher_observations: Tuple[
        CanonicalPitcherBaserunningObservation,
        ...,
    ],
    away_catcher_observation: Optional[
        CanonicalCatcherBaserunningObservation
    ],
    home_catcher_observation: Optional[
        CanonicalCatcherBaserunningObservation
    ],
) -> str:
    parts = [
        CANONICAL_OBSERVED_BASERUNNING_DIGEST_VERSION,
        "required_runners",
        *required_runner_ids,
        "required_pitchers",
        *required_pitcher_ids,
        "runner_observations",
        *(
            observation.digest
            for observation in runner_observations
        ),
        "pitcher_observations",
        *(
            observation.digest
            for observation in pitcher_observations
        ),
        "away_catcher",
        (
            away_catcher_observation.digest
            if away_catcher_observation is not None
            else "missing"
        ),
        "home_catcher",
        (
            home_catcher_observation.digest
            if home_catcher_observation is not None
            else "missing"
        ),
    ]

    return hashlib.sha256(
        "\x1f".join(parts).encode("utf-8")
    ).hexdigest()


def discover_observed_canonical_baserunning_evidence(
    *,
    required_runner_ids: Tuple[str, ...],
    required_pitcher_ids: Tuple[str, ...],
    runner_observations: Tuple[
        CanonicalRunnerBaserunningObservation,
        ...,
    ] = (),
    pitcher_observations: Tuple[
        CanonicalPitcherBaserunningObservation,
        ...,
    ] = (),
    away_catcher_observation: Optional[
        CanonicalCatcherBaserunningObservation
    ] = None,
    home_catcher_observation: Optional[
        CanonicalCatcherBaserunningObservation
    ] = None,
) -> CanonicalShadowBaserunningEvidenceDiscovery:
    """
    Adapt observations and discover one complete evidence catalog.

    Invalid observation contracts fail open through the canonical discovery
    result and never change legacy production authority.
    """

    try:
        runner_profiles = tuple(
            adapt_observed_runner_baserunning_evidence(
                observation
            )
            for observation in runner_observations
        )
        pitcher_profiles = tuple(
            adapt_observed_pitcher_baserunning_evidence(
                observation
            )
            for observation in pitcher_observations
        )
        away_catcher = (
            adapt_observed_catcher_baserunning_evidence(
                away_catcher_observation
            )
            if away_catcher_observation is not None
            else None
        )
        home_catcher = (
            adapt_observed_catcher_baserunning_evidence(
                home_catcher_observation
            )
            if home_catcher_observation is not None
            else None
        )
        observation_digest = _observation_digest(
            required_runner_ids=required_runner_ids,
            required_pitcher_ids=required_pitcher_ids,
            runner_observations=runner_observations,
            pitcher_observations=pitcher_observations,
            away_catcher_observation=(
                away_catcher_observation
            ),
            home_catcher_observation=(
                home_catcher_observation
            ),
        )
    except Exception as exc:
        return CanonicalShadowBaserunningEvidenceDiscovery(
            requested_runner_count=len(
                required_runner_ids
            ),
            requested_pitcher_count=len(
                required_pitcher_ids
            ),
            status="error",
            error_message=str(exc),
        )

    discovery = (
        discover_canonical_shadow_baserunning_evidence(
            required_runner_ids=required_runner_ids,
            required_pitcher_ids=required_pitcher_ids,
            runner_profiles=runner_profiles,
            pitcher_profiles=pitcher_profiles,
            away_catcher=away_catcher,
            home_catcher=home_catcher,
        )
    )

    return replace(
        discovery,
        observation_digest=observation_digest,
    )
