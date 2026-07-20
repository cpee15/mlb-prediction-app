"""Atomic canonical shadow execution-bundle contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from mlb_app.simulation.game.probability_diagnostics import (
    CanonicalProbabilityResolutionDiagnostics,
)
from mlb_app.simulation.game.trials import (
    CanonicalTrialBatch,
)

from .trial_adapter import (
    canonical_trial_batch_to_shadow_payload,
)


CANONICAL_SHADOW_EXECUTION_BUNDLE_VERSION = (
    "canonical_shadow_execution_bundle_v1"
)


@dataclass(frozen=True)
class CanonicalShadowExecutionBundle:
    """
    Carry one canonical trial batch and its diagnostics atomically.

    The bundle is transport-only. It does not run simulations, mutate
    probability resolution, or alter legacy production authority.
    """

    trial_batch: CanonicalTrialBatch
    probability_resolution_diagnostics: (
        CanonicalProbabilityResolutionDiagnostics
    )
    bundle_version: str = (
        CANONICAL_SHADOW_EXECUTION_BUNDLE_VERSION
    )

    def __post_init__(self) -> None:
        if not isinstance(
            self.trial_batch,
            CanonicalTrialBatch,
        ):
            raise TypeError(
                "trial_batch must be a CanonicalTrialBatch"
            )

        if not isinstance(
            self.probability_resolution_diagnostics,
            CanonicalProbabilityResolutionDiagnostics,
        ):
            raise TypeError(
                "probability_resolution_diagnostics must be a "
                "CanonicalProbabilityResolutionDiagnostics"
            )

        if self.bundle_version != (
            CANONICAL_SHADOW_EXECUTION_BUNDLE_VERSION
        ):
            raise ValueError(
                "unsupported canonical shadow execution "
                "bundle version"
            )


@dataclass(frozen=True)
class CanonicalShadowExecutionMaterial:
    """Resolved atomic material for canonical shadow attachment."""

    canonical_payload: Dict[str, Any]
    probability_resolution_diagnostics: (
        CanonicalProbabilityResolutionDiagnostics
    )

    def __post_init__(self) -> None:
        if not isinstance(
            self.canonical_payload,
            dict,
        ):
            raise TypeError(
                "canonical_payload must be a dictionary"
            )

        if not isinstance(
            self.probability_resolution_diagnostics,
            CanonicalProbabilityResolutionDiagnostics,
        ):
            raise TypeError(
                "probability_resolution_diagnostics must be a "
                "CanonicalProbabilityResolutionDiagnostics"
            )


def canonical_shadow_execution_bundle_to_material(
    bundle: CanonicalShadowExecutionBundle,
) -> CanonicalShadowExecutionMaterial:
    """Adapt one immutable execution bundle for shadow attachment."""

    if not isinstance(
        bundle,
        CanonicalShadowExecutionBundle,
    ):
        raise TypeError(
            "bundle must be a CanonicalShadowExecutionBundle"
        )

    return CanonicalShadowExecutionMaterial(
        canonical_payload=(
            canonical_trial_batch_to_shadow_payload(
                bundle.trial_batch
            )
        ),
        probability_resolution_diagnostics=(
            bundle.probability_resolution_diagnostics
        ),
    )
