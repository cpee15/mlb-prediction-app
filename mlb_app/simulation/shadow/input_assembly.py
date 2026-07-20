"""Production-facing canonical shadow input assembly contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
from typing import Optional

from mlb_app.simulation.box_score import (
    BatterDfsScoringRules,
    PitcherDfsScoringRules,
)
from mlb_app.simulation.game.contracts import (
    CanonicalGameConfig,
)
from mlb_app.simulation.game.matchup_input import (
    CanonicalMatchupInput,
)
from mlb_app.simulation.game.probability_artifact import (
    CanonicalProbabilityArtifact,
)
from mlb_app.simulation.game.probability_fallback import (
    CanonicalProbabilityFallbackCatalog,
    CanonicalProbabilityFallbackPolicy,
)

from .execution_factory import (
    CanonicalShadowExecutionBundleFactory,
    build_canonical_shadow_execution_bundle_factory,
)


CANONICAL_SHADOW_INPUT_ASSEMBLY_VERSION = (
    "canonical_shadow_input_assembly_v1"
)


@dataclass(frozen=True)
class CanonicalShadowExecutionInputs:
    """
    Immutable externally assembled canonical shadow inputs.

    This contract validates already-supplied inputs. It does not discover
    matchup data, load artifacts, read storage, select environment defaults,
    or activate canonical production authority.
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
    assembly_version: str = (
        CANONICAL_SHADOW_INPUT_ASSEMBLY_VERSION
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

        if (
            self.batter_dfs_rules is not None
            and not isinstance(
                self.batter_dfs_rules,
                BatterDfsScoringRules,
            )
        ):
            raise TypeError(
                "batter_dfs_rules must be "
                "BatterDfsScoringRules or None"
            )

        if (
            self.pitcher_dfs_rules is not None
            and not isinstance(
                self.pitcher_dfs_rules,
                PitcherDfsScoringRules,
            )
        ):
            raise TypeError(
                "pitcher_dfs_rules must be "
                "PitcherDfsScoringRules or None"
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

        if self.assembly_version != (
            CANONICAL_SHADOW_INPUT_ASSEMBLY_VERSION
        ):
            raise ValueError(
                "unsupported canonical shadow input "
                "assembly version"
            )

    @property
    def provider_identity(self) -> str:
        return (
            self.matchup_input
            .probability_provider
            .identity
        )

    @property
    def exact_artifact_digest(self) -> str:
        return self.exact_artifact.digest

    @property
    def fallback_catalog_digest(self) -> str:
        return self.fallback_catalog.digest

    @property
    def assembly_digest(self) -> str:
        """
        Return a deterministic digest of the complete assembled input set.

        This is input provenance only; it is not an authorization token and
        does not imply that canonical output is production authoritative.
        """

        away_plan = (
            self.matchup_input.away_pitching_plan
        )
        home_plan = (
            self.matchup_input.home_pitching_plan
        )

        parts = (
            self.assembly_version,
            str(self.matchup_input.game_pk),
            self.provider_identity,
            *self.matchup_input.away_lineup.player_ids,
            *self.matchup_input.home_lineup.player_ids,
            away_plan.starter_id,
            *away_plan.bullpen_pitcher_ids,
            home_plan.starter_id,
            *home_plan.bullpen_pitcher_ids,
            self.exact_artifact_digest,
            self.fallback_catalog_digest,
            self.fallback_policy.policy_version,
            *(
                tier.value
                for tier in self.fallback_policy.tiers
            ),
            str(self.game_config.regulation_innings),
            str(self.game_config.max_extra_innings),
            str(
                self.game_config
                .automatic_runner_enabled
            ),
            str(
                self.game_config
                .max_plate_appearances_per_half
            ),
            repr(self.batter_dfs_rules),
            repr(self.pitcher_dfs_rules),
        )

        return hashlib.sha256(
            "\x1f".join(parts).encode("utf-8")
        ).hexdigest()

    def build_factory(
        self,
    ) -> CanonicalShadowExecutionBundleFactory:
        """Delegate execution-factory construction to the existing path."""

        return build_canonical_shadow_execution_bundle_factory(
            matchup_input=self.matchup_input,
            exact_artifact=self.exact_artifact,
            fallback_catalog=self.fallback_catalog,
            fallback_policy=self.fallback_policy,
            game_config=self.game_config,
            batter_dfs_rules=self.batter_dfs_rules,
            pitcher_dfs_rules=self.pitcher_dfs_rules,
        )


def assemble_canonical_shadow_execution_inputs(
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
) -> CanonicalShadowExecutionInputs:
    """Assemble validated external inputs without discovery or activation."""

    return CanonicalShadowExecutionInputs(
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
