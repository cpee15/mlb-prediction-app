"""Atomic canonical shadow execution-bundle contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, TYPE_CHECKING

from mlb_app.simulation.game.probability_diagnostics import (
    CanonicalProbabilityResolutionDiagnostics,
)
from mlb_app.simulation.game.trials import (
    CanonicalTrialBatch,
)

from .trial_adapter import (
    canonical_trial_batch_to_shadow_payload,
)

if TYPE_CHECKING:
    from .input_assembly import (
        CanonicalShadowExecutionInputs,
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
    canonical_shadow_execution_inputs: Optional[
        "CanonicalShadowExecutionInputs"
    ] = None
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

        if (
            self.canonical_shadow_execution_inputs
            is not None
        ):
            from .input_assembly import (
                CanonicalShadowExecutionInputs,
            )

            if not isinstance(
                self.canonical_shadow_execution_inputs,
                CanonicalShadowExecutionInputs,
            ):
                raise TypeError(
                    "canonical_shadow_execution_inputs "
                    "must be CanonicalShadowExecutionInputs "
                    "or None"
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
    canonical_shadow_execution_inputs: Optional[
        "CanonicalShadowExecutionInputs"
    ] = None
    pitcher_appearance_sequence_audit: Optional[
        Dict[str, Any]
    ] = None

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

        if (
            self.canonical_shadow_execution_inputs
            is not None
        ):
            from .input_assembly import (
                CanonicalShadowExecutionInputs,
            )

            if not isinstance(
                self.canonical_shadow_execution_inputs,
                CanonicalShadowExecutionInputs,
            ):
                raise TypeError(
                    "canonical_shadow_execution_inputs "
                    "must be CanonicalShadowExecutionInputs "
                    "or None"
                )

        if (
            self.pitcher_appearance_sequence_audit
            is not None
            and not isinstance(
                self.pitcher_appearance_sequence_audit,
                dict,
            )
        ):
            raise TypeError(
                "pitcher_appearance_sequence_audit "
                "must be a dictionary or None"
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

    appearance_sequence_audit = None
    execution_inputs = (
        bundle.canonical_shadow_execution_inputs
    )

    if execution_inputs is not None:
        from .canonical_pitcher_appearance_sequence_audit import (
            audit_canonical_pitcher_appearance_sequence,
        )

        matchup_input = (
            execution_inputs.matchup_input
        )
        appearance_sequence_audit = (
            audit_canonical_pitcher_appearance_sequence(
                games=bundle.trial_batch.games,
                away_pitching_plan=(
                    matchup_input.away_pitching_plan
                ),
                home_pitching_plan=(
                    matchup_input.home_pitching_plan
                ),
            )
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
        canonical_shadow_execution_inputs=(
            execution_inputs
        ),
        pitcher_appearance_sequence_audit=(
            appearance_sequence_audit
        ),
    )
