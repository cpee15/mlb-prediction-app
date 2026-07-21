"""Fail-open production execution for canonical shadow trials."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from mlb_app.simulation.game import (
    CanonicalLineup,
    CanonicalMatchupInput,
    CanonicalPitchingPlan,
    CanonicalProbabilityFallbackPolicy,
    CanonicalProbabilityFallbackTier,
    build_canonical_trial_factory_input,
)

from .bullpen_discovery import (
    CanonicalShadowBullpenDiscovery,
)
from .execution_bundle import (
    CanonicalShadowExecutionMaterial,
    canonical_shadow_execution_bundle_to_material,
)
from .exact_artifact_discovery import (
    CanonicalShadowExactArtifactDiscovery,
)
from .fallback_catalog_discovery import (
    CanonicalShadowFallbackCatalogDiscovery,
)
from .input_assembly import (
    CanonicalShadowExecutionInputs,
    assemble_canonical_shadow_execution_inputs,
)
from .lineup_discovery import (
    CanonicalShadowLineupDiscovery,
)
from .probability_provider_discovery import (
    CanonicalShadowProbabilityProviderDiscovery,
)


CANONICAL_PRODUCTION_SHADOW_EXECUTION_VERSION = (
    "canonical_production_shadow_execution_v1"
)

DEFAULT_PRODUCTION_SHADOW_SIMULATION_COUNT = 25


@dataclass(frozen=True)
class CanonicalProductionShadowExecution:
    material: Optional[
        CanonicalShadowExecutionMaterial
    ] = None
    execution_inputs: Optional[
        CanonicalShadowExecutionInputs
    ] = None
    status: str = "not_run"
    simulation_count: int = 0
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    execution_version: str = (
        CANONICAL_PRODUCTION_SHADOW_EXECUTION_VERSION
    )

    def __post_init__(self) -> None:
        if self.execution_version != (
            CANONICAL_PRODUCTION_SHADOW_EXECUTION_VERSION
        ):
            raise ValueError(
                "unsupported canonical production shadow "
                "execution version"
            )

        if (
            self.material is not None
            and not isinstance(
                self.material,
                CanonicalShadowExecutionMaterial,
            )
        ):
            raise TypeError(
                "material must be "
                "CanonicalShadowExecutionMaterial or None"
            )

        if (
            self.execution_inputs is not None
            and not isinstance(
                self.execution_inputs,
                CanonicalShadowExecutionInputs,
            )
        ):
            raise TypeError(
                "execution_inputs must be "
                "CanonicalShadowExecutionInputs or None"
            )

    @property
    def executed(self) -> bool:
        return self.material is not None

    def to_diagnostics(self) -> Dict[str, Any]:
        payload = (
            self.material.canonical_payload
            if self.material is not None
            else {}
        )
        metadata = (
            payload.get("metadata")
            or payload.get("meta")
            or {}
        )

        return {
            "schema_version": self.execution_version,
            "status": self.status,
            "executed": self.executed,
            "simulation_count": self.simulation_count,
            "canonical_available": self.executed,
            "provider_identity": (
                self.execution_inputs.provider_identity
                if self.execution_inputs is not None
                else None
            ),
            "exact_artifact_digest": (
                self.execution_inputs
                .exact_artifact_digest
                if self.execution_inputs is not None
                else None
            ),
            "fallback_catalog_digest": (
                self.execution_inputs
                .fallback_catalog_digest
                if self.execution_inputs is not None
                else None
            ),
            "input_assembly_digest": (
                self.execution_inputs.assembly_digest
                if self.execution_inputs is not None
                else None
            ),
            "canonical_model_version": (
                metadata.get("model_version")
            ),
            "error_type": self.error_type,
            "error_message": self.error_message,
            "activation_permitted": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def _build_matchup_input(
    *,
    game_pk: int,
    lineups: CanonicalShadowLineupDiscovery,
    bullpens: CanonicalShadowBullpenDiscovery,
    provider_discovery: (
        CanonicalShadowProbabilityProviderDiscovery
    ),
) -> CanonicalMatchupInput:
    provider = provider_discovery.provider

    if provider is None:
        raise ValueError(
            "probability provider is unavailable"
        )

    away_starter = bullpens.away.starter_id
    home_starter = bullpens.home.starter_id

    if away_starter is None or home_starter is None:
        raise ValueError(
            "both scheduled starters are required"
        )

    return CanonicalMatchupInput(
        game_pk=int(game_pk),
        away_lineup=CanonicalLineup(
            team_side="away",
            player_ids=lineups.away_player_ids,
        ),
        home_lineup=CanonicalLineup(
            team_side="home",
            player_ids=lineups.home_player_ids,
        ),
        away_pitching_plan=CanonicalPitchingPlan(
            team_side="away",
            starter_id=away_starter,
            bullpen_pitcher_ids=(
                bullpens.away.bullpen_pitcher_ids
            ),
        ),
        home_pitching_plan=CanonicalPitchingPlan(
            team_side="home",
            starter_id=home_starter,
            bullpen_pitcher_ids=(
                bullpens.home.bullpen_pitcher_ids
            ),
        ),
        probability_provider=provider,
    )


def run_canonical_production_shadow(
    *,
    game_pk: Any,
    lineups: CanonicalShadowLineupDiscovery,
    bullpens: CanonicalShadowBullpenDiscovery,
    provider_discovery: (
        CanonicalShadowProbabilityProviderDiscovery
    ),
    exact_artifact_discovery: (
        CanonicalShadowExactArtifactDiscovery
    ),
    fallback_catalog_discovery: (
        CanonicalShadowFallbackCatalogDiscovery
    ),
    bootstrap_ready: bool,
    simulation_count: int = (
        DEFAULT_PRODUCTION_SHADOW_SIMULATION_COUNT
    ),
) -> CanonicalProductionShadowExecution:
    """
    Execute a small canonical batch when every production input is ready.

    Any assembly or execution failure remains fail-open and leaves legacy
    projections authoritative.
    """

    if not bootstrap_ready:
        return CanonicalProductionShadowExecution(
            status="blocked",
        )

    if not lineups.ready:
        return CanonicalProductionShadowExecution(
            status="blocked",
        )

    if not bullpens.ready:
        return CanonicalProductionShadowExecution(
            status="blocked",
        )

    exact_artifact = (
        exact_artifact_discovery.artifact
    )
    fallback_catalog = (
        fallback_catalog_discovery.catalog
    )

    if (
        exact_artifact is None
        or fallback_catalog is None
        or provider_discovery.provider is None
    ):
        return CanonicalProductionShadowExecution(
            status="blocked",
        )

    try:
        normalized_simulation_count = int(
            simulation_count
        )

        if normalized_simulation_count <= 0:
            raise ValueError(
                "simulation_count must be positive"
            )

        matchup_input = _build_matchup_input(
            game_pk=int(game_pk),
            lineups=lineups,
            bullpens=bullpens,
            provider_discovery=provider_discovery,
        )

        fallback_policy = (
            CanonicalProbabilityFallbackPolicy(
                tiers=(
                    CanonicalProbabilityFallbackTier
                    .EXACT_MATCHUP,
                    CanonicalProbabilityFallbackTier
                    .GLOBAL,
                )
            )
        )

        execution_inputs = (
            assemble_canonical_shadow_execution_inputs(
                matchup_input=matchup_input,
                exact_artifact=exact_artifact,
                fallback_catalog=fallback_catalog,
                fallback_policy=fallback_policy,
            )
        )

        factory_input = (
            build_canonical_trial_factory_input(
                game_pk=int(game_pk),
                config={
                    "simulation_count": (
                        normalized_simulation_count
                    ),
                    "canonical_model_version": (
                        "canonical-event-model-v1"
                    ),
                },
            )
        )

        bundle = execution_inputs.build_factory()(
            factory_input=factory_input,
        )

        material = (
            canonical_shadow_execution_bundle_to_material(
                bundle
            )
        )

        atomic_execution_inputs = (
            material.canonical_shadow_execution_inputs
        )

        if atomic_execution_inputs is None:
            raise RuntimeError(
                "canonical execution material is missing "
                "atomic execution inputs"
            )

        return CanonicalProductionShadowExecution(
            material=material,
            execution_inputs=atomic_execution_inputs,
            status="executed",
            simulation_count=(
                normalized_simulation_count
            ),
        )
    except Exception as exc:
        return CanonicalProductionShadowExecution(
            status="error",
            simulation_count=0,
            error_type=exc.__class__.__name__,
            error_message=str(exc),
        )
