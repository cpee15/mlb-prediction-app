"""Production PA probability-provider discovery for canonical readiness."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from mlb_app.simulation.game import (
    CanonicalProbabilityProviderIdentity,
)


CANONICAL_SHADOW_PROBABILITY_PROVIDER_DISCOVERY_VERSION = (
    "canonical_shadow_probability_provider_discovery_v1"
)

EXPECTED_PROBABILITY_KEYS = (
    "k",
    "bb",
    "hbp",
    "single",
    "double",
    "triple",
    "hr",
    "reached_on_error",
    "out",
)

REQUIRED_WORKSPACE_MODELS = (
    "awayPAOutcomeModel",
    "homePAOutcomeModel",
    "awayVsHomeBullpenPAOutcomeModel",
    "homeVsAwayBullpenPAOutcomeModel",
)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _valid_probability_distribution(
    value: Any,
) -> bool:
    probabilities = _mapping(value)

    if tuple(probabilities.keys()) != (
        EXPECTED_PROBABILITY_KEYS
    ):
        return False

    normalized = []

    for key in EXPECTED_PROBABILITY_KEYS:
        raw = probabilities.get(key)

        if isinstance(raw, bool):
            return False

        try:
            probability = float(raw)
        except (TypeError, ValueError):
            return False

        if (
            not math.isfinite(probability)
            or probability < 0.0
            or probability > 1.0
        ):
            return False

        normalized.append(probability)

    return abs(sum(normalized) - 1.0) <= 0.001


@dataclass(frozen=True)
class CanonicalShadowProbabilityProviderDiscovery:
    provider: Optional[
        CanonicalProbabilityProviderIdentity
    ] = None
    model_versions: Tuple[str, ...] = ()
    valid_model_count: int = 0
    required_model_count: int = len(
        REQUIRED_WORKSPACE_MODELS
    )
    missing_models: Tuple[str, ...] = ()
    invalid_models: Tuple[str, ...] = ()
    status: str = "unavailable"
    discovery_version: str = (
        CANONICAL_SHADOW_PROBABILITY_PROVIDER_DISCOVERY_VERSION
    )

    def __post_init__(self) -> None:
        if self.discovery_version != (
            CANONICAL_SHADOW_PROBABILITY_PROVIDER_DISCOVERY_VERSION
        ):
            raise ValueError(
                "unsupported canonical probability-provider "
                "discovery version"
            )

        if (
            self.provider is not None
            and not isinstance(
                self.provider,
                CanonicalProbabilityProviderIdentity,
            )
        ):
            raise TypeError(
                "provider must be a "
                "CanonicalProbabilityProviderIdentity or None"
            )

    @property
    def ready(self) -> bool:
        return self.provider is not None

    def readiness_workspace_fields(
        self,
    ) -> Dict[str, Any]:
        if self.provider is None:
            return {}

        return {
            "canonicalProbabilityProvider": {
                "provider_name": (
                    self.provider.provider_name
                ),
                "provider_version": (
                    self.provider.provider_version
                ),
                "artifact_id": (
                    self.provider.artifact_id
                ),
                "identity": self.provider.identity,
            }
        }

    def to_diagnostics(self) -> Dict[str, Any]:
        provider = self.provider

        return {
            "schema_version": self.discovery_version,
            "status": self.status,
            "ready": self.ready,
            "source": (
                "model_projections_workspace_pa_models"
            ),
            "provider": (
                {
                    "provider_name": (
                        provider.provider_name
                    ),
                    "provider_version": (
                        provider.provider_version
                    ),
                    "artifact_id": provider.artifact_id,
                    "identity": provider.identity,
                }
                if provider is not None
                else None
            ),
            "model_versions": list(
                self.model_versions
            ),
            "valid_model_count": (
                self.valid_model_count
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
            "probability_records_exposed": False,
            "artifact_discovered": False,
            "activation_permitted": False,
            "authoritative_source": "legacy",
        }


def discover_canonical_shadow_probability_provider(
    *,
    workspace: Optional[Mapping[str, Any]],
) -> CanonicalShadowProbabilityProviderDiscovery:
    """
    Discover a stable provider identity from production PA model outputs.

    Provider discovery proves only that one versioned implementation produced
    valid team-context PA distributions. It does not construct or imply an
    exact batter-pitcher probability artifact.
    """

    workspace_data = _mapping(workspace)

    missing_models = []
    invalid_models = []
    versions = []
    valid_model_count = 0

    for key in REQUIRED_WORKSPACE_MODELS:
        model = _mapping(
            workspace_data.get(key)
        )

        if not model:
            missing_models.append(key)
            continue

        version = str(
            model.get("model_version") or ""
        ).strip()

        if (
            not version
            or not _valid_probability_distribution(
                model.get("probabilities")
            )
        ):
            invalid_models.append(key)
            continue

        valid_model_count += 1
        versions.append(version)

    distinct_versions = tuple(
        sorted(set(versions))
    )

    provider = None
    status = "blocked"

    if (
        valid_model_count
        == len(REQUIRED_WORKSPACE_MODELS)
        and not missing_models
        and not invalid_models
        and len(distinct_versions) == 1
    ):
        provider = CanonicalProbabilityProviderIdentity(
            provider_name=(
                "model_projections_pa_outcome"
            ),
            provider_version=distinct_versions[0],
        )
        status = "ready"
    elif valid_model_count > 0:
        status = "partial"
    elif missing_models:
        status = "unavailable"

    return CanonicalShadowProbabilityProviderDiscovery(
        provider=provider,
        model_versions=distinct_versions,
        valid_model_count=valid_model_count,
        missing_models=tuple(missing_models),
        invalid_models=tuple(invalid_models),
        status=status,
    )
