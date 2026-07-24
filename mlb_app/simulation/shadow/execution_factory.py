"""Injected canonical shadow execution-bundle factory."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from mlb_app.simulation.box_score import (
    BatterDfsScoringRules,
    PitcherDfsScoringRules,
)
from mlb_app.simulation.game.contracts import (
    CanonicalGameConfig,
)
from mlb_app.simulation.game.factory_input import (
    CanonicalTrialFactoryInput,
)
from mlb_app.simulation.game.matchup_input import (
    CanonicalMatchupInput,
)
from mlb_app.simulation.game.bullpen_selector import (
    CanonicalBullpenPitcher,
    CanonicalBullpenRole,
)
from mlb_app.simulation.game.pa_resolver_factory import (
    CanonicalPlateAppearanceResolverFactory,
)
from mlb_app.simulation.game.probability_artifact import (
    CanonicalProbabilityArtifact,
)
from mlb_app.simulation.game.probability_diagnostics import (
    CanonicalProbabilityResolutionDiagnosticsCollector,
    build_canonical_probability_diagnostics_provider,
)
from mlb_app.simulation.game.probability_fallback import (
    CanonicalProbabilityFallbackAdapter,
    CanonicalProbabilityFallbackCatalog,
    CanonicalProbabilityFallbackPolicy,
)
from mlb_app.simulation.game.trial_factory import (
    CanonicalTrialExecutionPlan,
    run_canonical_trial_execution_plan,
)

from .execution_bundle import (
    CanonicalShadowExecutionBundle,
)


CANONICAL_SHADOW_EXECUTION_BUNDLE_FACTORY_VERSION = (
    "canonical_shadow_execution_bundle_factory_v1"
)


def _baseline_bullpen(
    pitcher_ids: tuple[str, ...],
) -> tuple[CanonicalBullpenPitcher, ...]:
    """Adapt an identity-only pitching plan into stable bullpen inputs."""

    return tuple(
        CanonicalBullpenPitcher(
            pitcher_id=pitcher_id,
            role=CanonicalBullpenRole.MIDDLE_RELIEF,
            appearance_priority=index,
        )
        for index, pitcher_id in enumerate(pitcher_ids)
    )


@dataclass(frozen=True)
class CanonicalShadowExecutionBundleFactory:
    """
    Compose one complete canonical shadow execution.

    This factory is dependency-injected only. It does not load artifacts,
    choose production defaults, mutate legacy output, or activate canonical
    authority.
    """

    matchup_input: CanonicalMatchupInput
    exact_artifact: CanonicalProbabilityArtifact
    fallback_catalog: CanonicalProbabilityFallbackCatalog
    fallback_policy: CanonicalProbabilityFallbackPolicy = field(
        default_factory=CanonicalProbabilityFallbackPolicy
    )
    game_config: CanonicalGameConfig = field(
        default_factory=CanonicalGameConfig
    )
    batter_dfs_rules: Optional[
        BatterDfsScoringRules
    ] = None
    pitcher_dfs_rules: Optional[
        PitcherDfsScoringRules
    ] = None
    factory_version: str = (
        CANONICAL_SHADOW_EXECUTION_BUNDLE_FACTORY_VERSION
    )

    def __post_init__(self) -> None:
        if not isinstance(
            self.matchup_input,
            CanonicalMatchupInput,
        ):
            raise TypeError(
                "matchup_input must be a CanonicalMatchupInput"
            )

        if not isinstance(
            self.exact_artifact,
            CanonicalProbabilityArtifact,
        ):
            raise TypeError(
                "exact_artifact must be a "
                "CanonicalProbabilityArtifact"
            )

        if not isinstance(
            self.fallback_catalog,
            CanonicalProbabilityFallbackCatalog,
        ):
            raise TypeError(
                "fallback_catalog must be a "
                "CanonicalProbabilityFallbackCatalog"
            )

        if not isinstance(
            self.fallback_policy,
            CanonicalProbabilityFallbackPolicy,
        ):
            raise TypeError(
                "fallback_policy must be a "
                "CanonicalProbabilityFallbackPolicy"
            )

        if not isinstance(
            self.game_config,
            CanonicalGameConfig,
        ):
            raise TypeError(
                "game_config must be a CanonicalGameConfig"
            )

        provider = (
            self.matchup_input.probability_provider
        )

        if self.exact_artifact.provider != provider:
            raise ValueError(
                "exact artifact provider must match "
                "matchup probability-provider identity"
            )

        if self.fallback_catalog.provider != provider:
            raise ValueError(
                "fallback catalog provider must match "
                "matchup probability-provider identity"
            )

        if self.factory_version != (
            CANONICAL_SHADOW_EXECUTION_BUNDLE_FACTORY_VERSION
        ):
            raise ValueError(
                "unsupported canonical shadow execution "
                "bundle factory version"
            )

    def __call__(
        self,
        *,
        factory_input: CanonicalTrialFactoryInput,
    ) -> CanonicalShadowExecutionBundle:
        if not isinstance(
            factory_input,
            CanonicalTrialFactoryInput,
        ):
            raise TypeError(
                "factory_input must be a "
                "CanonicalTrialFactoryInput"
            )

        if (
            factory_input.game_pk
            != self.matchup_input.game_pk
        ):
            raise ValueError(
                "factory-input game_pk must match "
                "canonical matchup input"
            )

        collector = (
            CanonicalProbabilityResolutionDiagnosticsCollector()
        )

        fallback_adapter = (
            CanonicalProbabilityFallbackAdapter(
                exact_artifact=self.exact_artifact,
                fallback_catalog=self.fallback_catalog,
                policy=self.fallback_policy,
            )
        )

        probability_provider = (
            build_canonical_probability_diagnostics_provider(
                fallback_adapter=fallback_adapter,
                collector=collector,
            )
        )

        resolver_factory = (
            CanonicalPlateAppearanceResolverFactory(
                probability_provider=probability_provider,
                away_bullpen=_baseline_bullpen(
                    self.matchup_input
                    .away_pitching_plan
                    .bullpen_pitcher_ids
                ),
                home_bullpen=_baseline_bullpen(
                    self.matchup_input
                    .home_pitching_plan
                    .bullpen_pitcher_ids
                ),
            )
        )

        plan = CanonicalTrialExecutionPlan(
            factory_input=factory_input,
            away_lineup=(
                self.matchup_input.away_lineup
            ),
            home_lineup=(
                self.matchup_input.home_lineup
            ),
            resolver_factory=resolver_factory,
            game_config=self.game_config,
            batter_dfs_rules=self.batter_dfs_rules,
            pitcher_dfs_rules=self.pitcher_dfs_rules,
            matchup_input=self.matchup_input,
        )

        trial_batch = (
            run_canonical_trial_execution_plan(
                plan
            )
        )

        from .input_assembly import (
            CanonicalShadowExecutionInputs,
        )

        execution_inputs = CanonicalShadowExecutionInputs(
            matchup_input=self.matchup_input,
            exact_artifact=self.exact_artifact,
            fallback_catalog=self.fallback_catalog,
            fallback_policy=self.fallback_policy,
            game_config=self.game_config,
            batter_dfs_rules=self.batter_dfs_rules,
            pitcher_dfs_rules=self.pitcher_dfs_rules,
        )

        return CanonicalShadowExecutionBundle(
            trial_batch=trial_batch,
            probability_resolution_diagnostics=(
                collector.snapshot()
            ),
            canonical_shadow_execution_inputs=(
                execution_inputs
            ),
        )


def build_canonical_shadow_execution_bundle_factory(
    *,
    matchup_input: CanonicalMatchupInput,
    exact_artifact: CanonicalProbabilityArtifact,
    fallback_catalog: CanonicalProbabilityFallbackCatalog,
    fallback_policy: Optional[
        CanonicalProbabilityFallbackPolicy
    ] = None,
    game_config: Optional[
        CanonicalGameConfig
    ] = None,
    batter_dfs_rules: Optional[
        BatterDfsScoringRules
    ] = None,
    pitcher_dfs_rules: Optional[
        PitcherDfsScoringRules
    ] = None,
) -> CanonicalShadowExecutionBundleFactory:
    """Build an explicit, non-default canonical shadow factory."""

    return CanonicalShadowExecutionBundleFactory(
        matchup_input=matchup_input,
        exact_artifact=exact_artifact,
        fallback_catalog=fallback_catalog,
        fallback_policy=(
            fallback_policy
            or CanonicalProbabilityFallbackPolicy()
        ),
        game_config=(
            game_config
            or CanonicalGameConfig()
        ),
        batter_dfs_rules=batter_dfs_rules,
        pitcher_dfs_rules=pitcher_dfs_rules,
    )
