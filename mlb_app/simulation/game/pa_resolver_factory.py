"""Deterministic canonical plate-appearance resolver construction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from mlb_app.simulation.events import (
    GameState,
    PlayEvent,
)

from .matchup_input import CanonicalMatchupInput
from .orchestrator import PlateAppearanceResolver
from .outcome_resolution import (
    resolve_canonical_sampled_plate_appearance,
)
from .probability import (
    CanonicalPlateAppearanceProbabilities,
    CanonicalPlateAppearanceProbabilityProvider,
    CanonicalPlateAppearanceQuery,
    sample_canonical_plate_appearance,
)
from .trial_factory import (
    CanonicalTrialResolverContext,
    CanonicalTrialResolverFactory,
)


CANONICAL_PA_RESOLVER_FACTORY_VERSION = (
    "canonical_pa_resolver_factory_v1"
)


@dataclass(frozen=True)
class CanonicalPlateAppearanceResolverFactory:
    """
    Build one deterministic canonical PA resolver per trial.

    Pitcher substitutions are intentionally outside this contract.
    Each half-inning therefore uses the fixed starter from the matchup
    pitching plan.
    """

    probability_provider: (
        CanonicalPlateAppearanceProbabilityProvider
    )
    version: str = (
        CANONICAL_PA_RESOLVER_FACTORY_VERSION
    )

    def __post_init__(self) -> None:
        if not callable(self.probability_provider):
            raise TypeError(
                "probability_provider must be callable"
            )

        if self.version != (
            CANONICAL_PA_RESOLVER_FACTORY_VERSION
        ):
            raise ValueError(
                "unsupported canonical PA resolver "
                "factory version"
            )

    def __call__(
        self,
        context: CanonicalTrialResolverContext,
    ) -> PlateAppearanceResolver:
        if not isinstance(
            context,
            CanonicalTrialResolverContext,
        ):
            raise TypeError(
                "context must be a "
                "CanonicalTrialResolverContext"
            )

        matchup_input = context.matchup_input

        if matchup_input is None:
            raise ValueError(
                "canonical PA resolver requires "
                "matchup_input"
            )

        if not isinstance(
            matchup_input,
            CanonicalMatchupInput,
        ):
            raise TypeError(
                "matchup_input must be a "
                "CanonicalMatchupInput"
            )

        return _CanonicalPlateAppearanceResolver(
            context=context,
            matchup_input=matchup_input,
            probability_provider=(
                self.probability_provider
            ),
        )


@dataclass(frozen=True)
class _CanonicalPlateAppearanceResolver:
    """Fresh immutable resolver owned by exactly one trial."""

    context: CanonicalTrialResolverContext
    matchup_input: CanonicalMatchupInput
    probability_provider: (
        CanonicalPlateAppearanceProbabilityProvider
    )

    def __call__(
        self,
        state: GameState,
        batter_id: str,
        sequence: int,
    ) -> PlayEvent:
        if not isinstance(state, GameState):
            raise TypeError(
                "state must be a GameState"
            )

        pitcher_id = _fixed_pitcher_for_state(
            matchup_input=self.matchup_input,
            state=state,
        )

        query = CanonicalPlateAppearanceQuery(
            matchup_input=self.matchup_input,
            state=state,
            batter_id=batter_id,
            pitcher_id=pitcher_id,
            sequence=sequence,
            trial_index=self.context.trial_index,
            trial_seed=self.context.trial_seed,
        )

        probabilities = self.probability_provider(
            query
        )

        if not isinstance(
            probabilities,
            CanonicalPlateAppearanceProbabilities,
        ):
            raise TypeError(
                "probability provider must return "
                "CanonicalPlateAppearanceProbabilities"
            )

        if probabilities.query != query:
            raise ValueError(
                "probability provider returned a "
                "distribution for a different query"
            )

        sampled = sample_canonical_plate_appearance(
            probabilities
        )

        return (
            resolve_canonical_sampled_plate_appearance(
                sampled
            )
        )


def _fixed_pitcher_for_state(
    *,
    matchup_input: CanonicalMatchupInput,
    state: GameState,
) -> str:
    """
    Select the fixed starting pitcher for the fielding side.

    Top half: home starter.
    Bottom half: away starter.
    """

    if state.half == "top":
        return (
            matchup_input
            .home_pitching_plan
            .starter_id
        )

    if state.half == "bottom":
        return (
            matchup_input
            .away_pitching_plan
            .starter_id
        )

    raise ValueError(
        "state half must be 'top' or 'bottom'"
    )


def build_canonical_pa_resolver_factory(
    probability_provider: (
        CanonicalPlateAppearanceProbabilityProvider
    ),
) -> CanonicalTrialResolverFactory:
    """Build the execution-plan-compatible resolver factory."""

    return CanonicalPlateAppearanceResolverFactory(
        probability_provider=probability_provider,
    )
