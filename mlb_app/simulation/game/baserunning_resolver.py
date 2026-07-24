"""Fail-open canonical baserunning resolver adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from mlb_app.simulation.events import (
    Base,
    GameState,
    PlayEvent,
)

from .baserunning_outcome_resolution import (
    resolve_canonical_sampled_baserunning,
)
from .baserunning_sampling import (
    CanonicalBaserunningProbabilities,
    CanonicalBaserunningSamplingQuery,
    sample_canonical_baserunning,
)
from .orchestrator import BaserunningResolver
from .trial_factory import CanonicalTrialResolverContext


CANONICAL_BASERUNNING_RESOLVER_VERSION = (
    "canonical_baserunning_resolver_v1"
)


@dataclass(frozen=True)
class CanonicalBaserunningEvidenceQuery:
    """One legal steal opportunity requiring probability evidence."""

    state: GameState
    batter_id: str
    runner_id: str
    origin_base: Base
    target_base: Base

    def __post_init__(self) -> None:
        if not isinstance(self.state, GameState):
            raise TypeError(
                "state must be a GameState"
            )
        if not self.batter_id:
            raise ValueError(
                "batter_id is required"
            )
        if not self.runner_id:
            raise ValueError(
                "runner_id is required"
            )
        if (
            self.state.runner_on(self.origin_base)
            != self.runner_id
        ):
            raise ValueError(
                "runner does not occupy origin base"
            )
        if (
            self.state.runner_on(self.target_base)
            is not None
        ):
            raise ValueError(
                "target base must be unoccupied"
            )


@dataclass(frozen=True)
class CanonicalBaserunningEvidence:
    """Complete pitcher-aware probabilities for one opportunity."""

    pitcher_id: str
    attempt_probability: float
    success_probability: float
    probability_provenance: str

    def __post_init__(self) -> None:
        if not self.pitcher_id:
            raise ValueError(
                "pitcher_id is required"
            )

        for name, value in (
            (
                "attempt_probability",
                self.attempt_probability,
            ),
            (
                "success_probability",
                self.success_probability,
            ),
        ):
            if not 0.0 <= value <= 1.0:
                raise ValueError(
                    f"{name} must be between 0 and 1"
                )

        if not self.probability_provenance:
            raise ValueError(
                "probability_provenance is required"
            )


CanonicalBaserunningEvidenceProvider = Callable[
    [CanonicalBaserunningEvidenceQuery],
    Optional[CanonicalBaserunningEvidence],
]


@dataclass(frozen=True)
class CanonicalBaserunningResolverAdapterFactory:
    """Build one deterministic fail-open resolver per trial."""

    evidence_provider: CanonicalBaserunningEvidenceProvider
    resolver_version: str = (
        CANONICAL_BASERUNNING_RESOLVER_VERSION
    )

    def __post_init__(self) -> None:
        if not callable(self.evidence_provider):
            raise TypeError(
                "evidence_provider must be callable"
            )
        if self.resolver_version != (
            CANONICAL_BASERUNNING_RESOLVER_VERSION
        ):
            raise ValueError(
                "unsupported baserunning resolver version"
            )

    def __call__(
        self,
        context: CanonicalTrialResolverContext,
    ) -> BaserunningResolver:
        if not isinstance(
            context,
            CanonicalTrialResolverContext,
        ):
            raise TypeError(
                "context must be "
                "CanonicalTrialResolverContext"
            )

        def resolve(
            state: GameState,
            batter_id: str,
            sequence: int,
        ) -> Optional[PlayEvent]:
            opportunity = _select_opportunity(
                state=state,
                batter_id=batter_id,
            )

            if opportunity is None:
                return None

            try:
                evidence = self.evidence_provider(
                    opportunity
                )
            except Exception:
                return None

            if not isinstance(
                evidence,
                CanonicalBaserunningEvidence,
            ):
                return None

            probabilities = (
                CanonicalBaserunningProbabilities(
                    query=(
                        CanonicalBaserunningSamplingQuery(
                            game_pk=(
                                context.factory_input.game_pk
                            ),
                            trial_index=(
                                context.trial_index
                            ),
                            trial_seed=(
                                context.trial_seed
                            ),
                            sequence=sequence,
                            state=state,
                            batter_id=batter_id,
                            pitcher_id=(
                                evidence.pitcher_id
                            ),
                            runner_id=(
                                opportunity.runner_id
                            ),
                            origin_base=(
                                opportunity.origin_base
                            ),
                            target_base=(
                                opportunity.target_base
                            ),
                        )
                    ),
                    attempt_probability=(
                        evidence.attempt_probability
                    ),
                    success_probability=(
                        evidence.success_probability
                    ),
                    probability_provenance=(
                        evidence.probability_provenance
                    ),
                )
            )

            sampled = sample_canonical_baserunning(
                probabilities
            )
            return (
                resolve_canonical_sampled_baserunning(
                    sampled
                ).event
            )

        return resolve


def _select_opportunity(
    *,
    state: GameState,
    batter_id: str,
) -> Optional[CanonicalBaserunningEvidenceQuery]:
    """Select at most one legal lead-runner opportunity."""

    if state.outs >= 3:
        return None

    if (
        state.second is not None
        and state.third is None
    ):
        return CanonicalBaserunningEvidenceQuery(
            state=state,
            batter_id=batter_id,
            runner_id=state.second,
            origin_base=Base.SECOND,
            target_base=Base.THIRD,
        )

    if (
        state.first is not None
        and state.second is None
    ):
        return CanonicalBaserunningEvidenceQuery(
            state=state,
            batter_id=batter_id,
            runner_id=state.first,
            origin_base=Base.FIRST,
            target_base=Base.SECOND,
        )

    return None
