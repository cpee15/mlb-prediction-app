"""Fallback probability-catalog discovery for canonical readiness."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Dict, Mapping, Optional, Tuple

from mlb_app.simulation.game import (
    CANONICAL_PA_OUTCOME_ORDER,
    CanonicalOutcomeProbability,
    CanonicalPlateAppearanceOutcome,
    CanonicalProbabilityFallbackCatalog,
    CanonicalProbabilityFallbackRecord,
    CanonicalProbabilityFallbackTier,
    CanonicalProbabilityProviderIdentity,
)

from .probability_provider_discovery import (
    REQUIRED_WORKSPACE_MODELS,
)


CANONICAL_SHADOW_FALLBACK_CATALOG_DISCOVERY_VERSION = (
    "canonical_shadow_fallback_catalog_discovery_v1"
)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _canonical_distribution(
    value: Any,
) -> Optional[Tuple[CanonicalOutcomeProbability, ...]]:
    source = _mapping(value)

    required_source_keys = {
        "k",
        "bb",
        "hbp",
        "single",
        "double",
        "triple",
        "hr",
        "reached_on_error",
        "out",
    }

    if set(source.keys()) != required_source_keys:
        return None

    parsed: Dict[str, float] = {}

    for key in required_source_keys:
        raw = source.get(key)

        if isinstance(raw, bool):
            return None

        try:
            probability = float(raw)
        except (TypeError, ValueError):
            return None

        if (
            not math.isfinite(probability)
            or probability < 0.0
            or probability > 1.0
        ):
            return None

        parsed[key] = probability

    if abs(sum(parsed.values()) - 1.0) > 0.001:
        return None

    canonical_values = {
        CanonicalPlateAppearanceOutcome.OUT: (
            parsed["out"]
            + parsed["reached_on_error"]
        ),
        CanonicalPlateAppearanceOutcome.SINGLE: (
            parsed["single"]
        ),
        CanonicalPlateAppearanceOutcome.DOUBLE: (
            parsed["double"]
        ),
        CanonicalPlateAppearanceOutcome.TRIPLE: (
            parsed["triple"]
        ),
        CanonicalPlateAppearanceOutcome.HOME_RUN: (
            parsed["hr"]
        ),
        CanonicalPlateAppearanceOutcome.WALK: (
            parsed["bb"]
        ),
        CanonicalPlateAppearanceOutcome.HIT_BY_PITCH: (
            parsed["hbp"]
        ),
        CanonicalPlateAppearanceOutcome.STRIKEOUT: (
            parsed["k"]
        ),
    }

    total = sum(canonical_values.values())

    if total <= 0.0:
        return None

    normalized = {
        outcome: value / total
        for outcome, value in canonical_values.items()
    }

    return tuple(
        CanonicalOutcomeProbability(
            outcome=outcome,
            probability=normalized[outcome],
        )
        for outcome in CANONICAL_PA_OUTCOME_ORDER
    )


def _average_distributions(
    distributions: Tuple[
        Tuple[CanonicalOutcomeProbability, ...],
        ...,
    ],
) -> Tuple[CanonicalOutcomeProbability, ...]:
    count = len(distributions)

    if count == 0:
        raise ValueError(
            "at least one distribution is required"
        )

    totals = {
        outcome: 0.0
        for outcome in CANONICAL_PA_OUTCOME_ORDER
    }

    for distribution in distributions:
        for point in distribution:
            totals[point.outcome] += point.probability

    averaged = {
        outcome: totals[outcome] / count
        for outcome in CANONICAL_PA_OUTCOME_ORDER
    }

    total = sum(averaged.values())

    normalized = {
        outcome: averaged[outcome] / total
        for outcome in CANONICAL_PA_OUTCOME_ORDER
    }

    return tuple(
        CanonicalOutcomeProbability(
            outcome=outcome,
            probability=normalized[outcome],
        )
        for outcome in CANONICAL_PA_OUTCOME_ORDER
    )


@dataclass(frozen=True)
class CanonicalShadowFallbackCatalogDiscovery:
    catalog: Optional[
        CanonicalProbabilityFallbackCatalog
    ] = None
    source_model_count: int = 0
    required_model_count: int = len(
        REQUIRED_WORKSPACE_MODELS
    )
    missing_models: Tuple[str, ...] = ()
    invalid_models: Tuple[str, ...] = ()
    status: str = "unavailable"
    discovery_version: str = (
        CANONICAL_SHADOW_FALLBACK_CATALOG_DISCOVERY_VERSION
    )

    def __post_init__(self) -> None:
        if self.discovery_version != (
            CANONICAL_SHADOW_FALLBACK_CATALOG_DISCOVERY_VERSION
        ):
            raise ValueError(
                "unsupported canonical fallback-catalog "
                "discovery version"
            )

        if (
            self.catalog is not None
            and not isinstance(
                self.catalog,
                CanonicalProbabilityFallbackCatalog,
            )
        ):
            raise TypeError(
                "catalog must be a "
                "CanonicalProbabilityFallbackCatalog or None"
            )

    @property
    def ready(self) -> bool:
        return self.catalog is not None

    def readiness_workspace_fields(
        self,
    ) -> Dict[str, Any]:
        if self.catalog is None:
            return {}

        return {
            "canonicalProbabilityFallbackCatalog": {
                "schema_version": (
                    self.catalog.schema_version
                ),
                "provider_identity": (
                    self.catalog.provider.identity
                ),
                "digest": self.catalog.digest,
                "record_count": len(
                    self.catalog.records
                ),
                "tiers": [
                    record.tier.value
                    for record in self.catalog.records
                ],
            }
        }

    def to_diagnostics(self) -> Dict[str, Any]:
        catalog = self.catalog

        return {
            "schema_version": self.discovery_version,
            "status": self.status,
            "ready": self.ready,
            "source": (
                "averaged_production_team_context_pa_models"
            ),
            "source_model_count": (
                self.source_model_count
            ),
            "required_model_count": (
                self.required_model_count
            ),
            "missing_models": list(
                self.missing_models
            ),
            "invalid_models": list(
                self.invalid_models
            ),
            "catalog": (
                {
                    "schema_version": (
                        catalog.schema_version
                    ),
                    "provider_identity": (
                        catalog.provider.identity
                    ),
                    "digest": catalog.digest,
                    "record_count": len(
                        catalog.records
                    ),
                    "tiers": [
                        record.tier.value
                        for record in catalog.records
                    ],
                }
                if catalog is not None
                else None
            ),
            "global_record_only": True,
            "reached_on_error_mapping": (
                "folded_into_canonical_out"
            ),
            "probability_records_exposed": False,
            "exact_artifact_discovered": False,
            "activation_permitted": False,
            "authoritative_source": "legacy",
        }


def discover_canonical_shadow_fallback_catalog(
    *,
    workspace: Optional[Mapping[str, Any]],
    provider: Optional[
        CanonicalProbabilityProviderIdentity
    ],
) -> CanonicalShadowFallbackCatalogDiscovery:
    """
    Build one provider-bound global fallback from production team-context models.

    Team-context distributions cannot honestly satisfy batter- or pitcher-tier
    identity records. They are averaged into one global fallback distribution.
    """

    if provider is None:
        return CanonicalShadowFallbackCatalogDiscovery(
            status="blocked",
        )

    if not isinstance(
        provider,
        CanonicalProbabilityProviderIdentity,
    ):
        raise TypeError(
            "provider must be a "
            "CanonicalProbabilityProviderIdentity or None"
        )

    workspace_data = _mapping(workspace)
    missing_models = []
    invalid_models = []
    distributions = []

    for key in REQUIRED_WORKSPACE_MODELS:
        model = _mapping(
            workspace_data.get(key)
        )

        if not model:
            missing_models.append(key)
            continue

        distribution = _canonical_distribution(
            model.get("probabilities")
        )

        if distribution is None:
            invalid_models.append(key)
            continue

        distributions.append(distribution)

    if (
        missing_models
        or invalid_models
        or len(distributions)
        != len(REQUIRED_WORKSPACE_MODELS)
    ):
        return CanonicalShadowFallbackCatalogDiscovery(
            source_model_count=len(distributions),
            missing_models=tuple(missing_models),
            invalid_models=tuple(invalid_models),
            status=(
                "partial"
                if distributions
                else "unavailable"
            ),
        )

    global_distribution = _average_distributions(
        tuple(distributions)
    )

    catalog = CanonicalProbabilityFallbackCatalog(
        provider=provider,
        records=(
            CanonicalProbabilityFallbackRecord(
                tier=(
                    CanonicalProbabilityFallbackTier.GLOBAL
                ),
                identity=None,
                probabilities=global_distribution,
            ),
        ),
    )

    return CanonicalShadowFallbackCatalogDiscovery(
        catalog=catalog,
        source_model_count=len(distributions),
        status="ready",
    )
