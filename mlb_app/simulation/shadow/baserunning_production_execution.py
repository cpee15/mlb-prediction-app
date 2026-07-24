"""
Inject discovered baserunning evidence into production-shadow execution.

This adapter is explicitly invoked and never changes legacy authority.
"""

from __future__ import annotations

from typing import Any

from .baserunning_evidence_discovery import (
    CanonicalShadowBaserunningEvidenceDiscovery,
)
from .production_execution import (
    CanonicalProductionShadowExecution,
    run_canonical_production_shadow,
)


CANONICAL_BASERUNNING_PRODUCTION_ADAPTER_VERSION = (
    "canonical_baserunning_production_adapter_v1"
)


def run_canonical_production_shadow_with_baserunning_discovery(
    *,
    baserunning_evidence_discovery: (
        CanonicalShadowBaserunningEvidenceDiscovery
    ),
    **production_inputs: Any,
) -> CanonicalProductionShadowExecution:
    """
    Run canonical production shadow with one discovered evidence catalog.

    A ready discovery injects its catalog. An unavailable discovery blocks
    this explicit baserunning execution. An error discovery fails open.
    Direct catalog injection through production_inputs is rejected so this
    boundary has one unambiguous evidence source.
    """

    if not isinstance(
        baserunning_evidence_discovery,
        CanonicalShadowBaserunningEvidenceDiscovery,
    ):
        return CanonicalProductionShadowExecution(
            status="error",
            error_type="TypeError",
            error_message=(
                "baserunning_evidence_discovery must be "
                "CanonicalShadowBaserunningEvidenceDiscovery"
            ),
        )

    if (
        "baserunning_evidence_catalog"
        in production_inputs
    ):
        return CanonicalProductionShadowExecution(
            status="error",
            error_type="ValueError",
            error_message=(
                "baserunning_evidence_catalog must be "
                "supplied through discovery"
            ),
        )

    if baserunning_evidence_discovery.status == "error":
        return CanonicalProductionShadowExecution(
            status="error",
            error_type=(
                "BaserunningEvidenceDiscoveryError"
            ),
            error_message=(
                baserunning_evidence_discovery.error_message
                or "baserunning evidence discovery failed"
            ),
        )

    if not baserunning_evidence_discovery.ready:
        return CanonicalProductionShadowExecution(
            status="blocked",
            error_type=(
                "BaserunningEvidenceUnavailable"
            ),
            error_message=(
                "complete baserunning evidence is unavailable"
            ),
        )

    catalog = baserunning_evidence_discovery.catalog

    if catalog is None:
        return CanonicalProductionShadowExecution(
            status="error",
            error_type="RuntimeError",
            error_message=(
                "ready baserunning discovery is missing "
                "its catalog"
            ),
        )

    return run_canonical_production_shadow(
        baserunning_evidence_catalog=catalog,
        **production_inputs,
    )
