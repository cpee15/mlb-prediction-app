"""Compose catalog-backed canonical baserunning resolvers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .baserunning_evidence_adapter import (
    build_canonical_baserunning_evidence_provider,
)
from .baserunning_evidence_catalog import (
    CanonicalBaserunningEvidenceCatalog,
    build_canonical_baserunning_state_provider,
)
from .baserunning_resolver import (
    CanonicalBaserunningEvidenceQuery,
    CanonicalBaserunningResolverAdapterFactory,
)
from .orchestrator import (
    BaserunningResolver,
    PlateAppearanceResolver,
)
from .trial_factory import (
    CanonicalCoupledBaserunningResolverFactory,
    CanonicalTrialResolverContext,
)


CANONICAL_BASERUNNING_COMPOSITION_VERSION = (
    "canonical_baserunning_composition_v1"
)


@dataclass(frozen=True)
class CanonicalCatalogBaserunningResolverFactory:
    """
    Compose one catalog-backed resolver with trial-owned pitcher state.

    Missing active-pitcher access remains fail-open through the catalog
    state provider and produces no baserunning event.
    """

    catalog: CanonicalBaserunningEvidenceCatalog
    composition_version: str = (
        CANONICAL_BASERUNNING_COMPOSITION_VERSION
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

        if self.composition_version != (
            CANONICAL_BASERUNNING_COMPOSITION_VERSION
        ):
            raise ValueError(
                "unsupported baserunning composition version"
            )

    def __call__(
        self,
        context: CanonicalTrialResolverContext,
        plate_appearance_resolver: PlateAppearanceResolver,
    ) -> BaserunningResolver:
        if not isinstance(
            context,
            CanonicalTrialResolverContext,
        ):
            raise TypeError(
                "context must be "
                "CanonicalTrialResolverContext"
            )

        if not callable(plate_appearance_resolver):
            raise TypeError(
                "plate_appearance_resolver must be callable"
            )

        def active_pitcher_provider(
            query: CanonicalBaserunningEvidenceQuery,
        ) -> Optional[str]:
            identity_provider = getattr(
                plate_appearance_resolver,
                "active_pitcher_id",
                None,
            )

            if not callable(identity_provider):
                return None

            return identity_provider(query.state)

        state_provider = (
            build_canonical_baserunning_state_provider(
                catalog=self.catalog,
                active_pitcher_provider=(
                    active_pitcher_provider
                ),
            )
        )
        evidence_provider = (
            build_canonical_baserunning_evidence_provider(
                state_provider=state_provider,
            )
        )

        return CanonicalBaserunningResolverAdapterFactory(
            evidence_provider=evidence_provider,
        )(context)


def build_canonical_catalog_baserunning_resolver_factory(
    *,
    catalog: CanonicalBaserunningEvidenceCatalog,
) -> CanonicalCoupledBaserunningResolverFactory:
    """Build an explicit coupled catalog-backed resolver factory."""

    return CanonicalCatalogBaserunningResolverFactory(
        catalog=catalog,
    )
