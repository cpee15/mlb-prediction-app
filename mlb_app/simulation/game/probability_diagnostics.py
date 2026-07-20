"""Canonical probability-resolution diagnostics collection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

from .probability import (
    CanonicalPlateAppearanceProbabilities,
    CanonicalPlateAppearanceProbabilityProvider,
    CanonicalPlateAppearanceQuery,
)
from .probability_fallback import (
    CanonicalProbabilityFallbackAdapter,
    CanonicalProbabilityFallbackResolution,
    CanonicalProbabilityFallbackTier,
)


CANONICAL_PROBABILITY_DIAGNOSTICS_VERSION = (
    "canonical_probability_diagnostics_v1"
)


@dataclass(frozen=True)
class CanonicalProbabilityResolutionObservation:
    """One observed exact-or-fallback probability resolution."""

    trial_index: int
    sequence: int
    inning: int
    half: str
    batter_id: str
    pitcher_id: str
    tier: CanonicalProbabilityFallbackTier
    source_identity: str | None
    exact_artifact_digest: str
    fallback_catalog_digest: str
    policy_version: str
    diagnostics_version: str = (
        CANONICAL_PROBABILITY_DIAGNOSTICS_VERSION
    )

    def __post_init__(self) -> None:
        if self.trial_index < 0:
            raise ValueError(
                "trial_index cannot be negative"
            )

        if self.sequence < 0:
            raise ValueError(
                "sequence cannot be negative"
            )

        if self.inning < 1:
            raise ValueError(
                "inning must be positive"
            )

        if self.half not in {
            "top",
            "bottom",
        }:
            raise ValueError(
                "half must be 'top' or 'bottom'"
            )

        if not self.batter_id:
            raise ValueError(
                "batter_id is required"
            )

        if not self.pitcher_id:
            raise ValueError(
                "pitcher_id is required"
            )

        if not isinstance(
            self.tier,
            CanonicalProbabilityFallbackTier,
        ):
            raise TypeError(
                "tier must be a canonical fallback tier"
            )

        if self.diagnostics_version != (
            CANONICAL_PROBABILITY_DIAGNOSTICS_VERSION
        ):
            raise ValueError(
                "unsupported probability diagnostics version"
            )

        _validate_digest(
            "exact_artifact_digest",
            self.exact_artifact_digest,
        )
        _validate_digest(
            "fallback_catalog_digest",
            self.fallback_catalog_digest,
        )

    @property
    def is_fallback(self) -> bool:
        return self.tier is not (
            CanonicalProbabilityFallbackTier.EXACT_MATCHUP
        )


@dataclass(frozen=True)
class CanonicalProbabilityTierUsage:
    """Aggregated resolution count for one tier."""

    tier: CanonicalProbabilityFallbackTier
    count: int

    def __post_init__(self) -> None:
        if not isinstance(
            self.tier,
            CanonicalProbabilityFallbackTier,
        ):
            raise TypeError(
                "tier must be a canonical fallback tier"
            )

        if isinstance(self.count, bool):
            raise TypeError(
                "count must be an integer"
            )

        if self.count < 0:
            raise ValueError(
                "count cannot be negative"
            )


@dataclass(frozen=True)
class CanonicalProbabilityResolutionDiagnostics:
    """Immutable deterministic diagnostics snapshot."""

    observations: Tuple[
        CanonicalProbabilityResolutionObservation,
        ...,
    ]
    tier_usage: Tuple[
        CanonicalProbabilityTierUsage,
        ...,
    ]
    diagnostics_version: str = (
        CANONICAL_PROBABILITY_DIAGNOSTICS_VERSION
    )

    def __post_init__(self) -> None:
        if self.diagnostics_version != (
            CANONICAL_PROBABILITY_DIAGNOSTICS_VERSION
        ):
            raise ValueError(
                "unsupported probability diagnostics version"
            )

        expected_tiers = tuple(
            CanonicalProbabilityFallbackTier
        )
        actual_tiers = tuple(
            usage.tier
            for usage in self.tier_usage
        )

        if actual_tiers != expected_tiers:
            raise ValueError(
                "tier_usage must contain every canonical "
                "fallback tier exactly once in canonical order"
            )

        if sum(
            usage.count
            for usage in self.tier_usage
        ) != len(self.observations):
            raise ValueError(
                "tier usage counts must reconcile to observations"
            )

    @property
    def total_resolutions(self) -> int:
        return len(self.observations)

    @property
    def exact_resolutions(self) -> int:
        return self.count_for(
            CanonicalProbabilityFallbackTier.EXACT_MATCHUP
        )

    @property
    def fallback_resolutions(self) -> int:
        return (
            self.total_resolutions
            - self.exact_resolutions
        )

    @property
    def fallback_rate(self) -> float:
        if self.total_resolutions == 0:
            return 0.0

        return (
            self.fallback_resolutions
            / self.total_resolutions
        )

    def count_for(
        self,
        tier: CanonicalProbabilityFallbackTier,
    ) -> int:
        for usage in self.tier_usage:
            if usage.tier is tier:
                return usage.count

        raise KeyError(tier)


class CanonicalProbabilityResolutionDiagnosticsCollector:
    """
    Collect resolution observations without changing provider decisions.

    A collector is intended to be owned by one canonical execution run.
    """

    def __init__(self) -> None:
        self._observations: List[
            CanonicalProbabilityResolutionObservation
        ] = []

    def record(
        self,
        resolution: CanonicalProbabilityFallbackResolution,
    ) -> None:
        if not isinstance(
            resolution,
            CanonicalProbabilityFallbackResolution,
        ):
            raise TypeError(
                "resolution must be a "
                "CanonicalProbabilityFallbackResolution"
            )

        query = resolution.probabilities.query

        self._observations.append(
            CanonicalProbabilityResolutionObservation(
                trial_index=query.trial_index,
                sequence=query.sequence,
                inning=query.state.inning,
                half=query.state.half,
                batter_id=query.batter_id,
                pitcher_id=query.pitcher_id,
                tier=resolution.tier,
                source_identity=resolution.source_identity,
                exact_artifact_digest=(
                    resolution.exact_artifact_digest
                ),
                fallback_catalog_digest=(
                    resolution.fallback_catalog_digest
                ),
                policy_version=resolution.policy_version,
            )
        )

    def snapshot(
        self,
    ) -> CanonicalProbabilityResolutionDiagnostics:
        observations = tuple(
            sorted(
                self._observations,
                key=lambda value: (
                    value.trial_index,
                    value.sequence,
                    value.inning,
                    value.half,
                    value.batter_id,
                    value.pitcher_id,
                    value.tier.value,
                    value.source_identity or "",
                ),
            )
        )

        usage = tuple(
            CanonicalProbabilityTierUsage(
                tier=tier,
                count=sum(
                    observation.tier is tier
                    for observation in observations
                ),
            )
            for tier in CanonicalProbabilityFallbackTier
        )

        return CanonicalProbabilityResolutionDiagnostics(
            observations=observations,
            tier_usage=usage,
        )


@dataclass(frozen=True)
class CanonicalProbabilityDiagnosticsProvider:
    """Observe a fallback adapter without changing its response."""

    fallback_adapter: CanonicalProbabilityFallbackAdapter
    collector: (
        CanonicalProbabilityResolutionDiagnosticsCollector
    )

    def __post_init__(self) -> None:
        if not isinstance(
            self.fallback_adapter,
            CanonicalProbabilityFallbackAdapter,
        ):
            raise TypeError(
                "fallback_adapter must be a "
                "CanonicalProbabilityFallbackAdapter"
            )

        if not isinstance(
            self.collector,
            CanonicalProbabilityResolutionDiagnosticsCollector,
        ):
            raise TypeError(
                "collector must be a probability-resolution "
                "diagnostics collector"
            )

    def __call__(
        self,
        query: CanonicalPlateAppearanceQuery,
    ) -> CanonicalPlateAppearanceProbabilities:
        resolution = self.fallback_adapter.resolve(
            query
        )
        self.collector.record(
            resolution
        )
        return resolution.probabilities


def build_canonical_probability_diagnostics_provider(
    *,
    fallback_adapter: CanonicalProbabilityFallbackAdapter,
    collector: (
        CanonicalProbabilityResolutionDiagnosticsCollector
    ),
) -> CanonicalPlateAppearanceProbabilityProvider:
    """Build an observational canonical probability provider."""

    return CanonicalProbabilityDiagnosticsProvider(
        fallback_adapter=fallback_adapter,
        collector=collector,
    )


def _validate_digest(
    name: str,
    digest: str,
) -> None:
    if (
        len(digest) != 64
        or any(
            character not in "0123456789abcdef"
            for character in digest
        )
    ):
        raise ValueError(
            f"{name} must be a SHA256 digest"
        )
