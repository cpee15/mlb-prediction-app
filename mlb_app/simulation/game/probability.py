"""Provider-neutral plate-appearance probability contracts."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import random
from typing import Callable, Tuple

from mlb_app.simulation.events import GameState

from .factory_input import MAX_CANONICAL_SEED
from .matchup_input import (
    CanonicalMatchupInput,
    CanonicalProbabilityProviderIdentity,
)


CANONICAL_PA_PROBABILITY_VERSION = (
    "canonical_pa_probability_v1"
)
CANONICAL_PA_SAMPLING_VERSION = (
    "canonical_pa_sampling_v1"
)
PROBABILITY_TOLERANCE = 0.000000001


class CanonicalPlateAppearanceOutcome(str, Enum):
    """Provider-neutral terminal plate-appearance categories."""

    OUT = "out"
    SINGLE = "single"
    DOUBLE = "double"
    TRIPLE = "triple"
    HOME_RUN = "hr"
    WALK = "bb"
    HIT_BY_PITCH = "hbp"
    STRIKEOUT = "k"


CANONICAL_PA_OUTCOME_ORDER = tuple(
    CanonicalPlateAppearanceOutcome
)


@dataclass(frozen=True)
class CanonicalOutcomeProbability:
    """One canonical outcome and its probability mass."""

    outcome: CanonicalPlateAppearanceOutcome
    probability: float

    def __post_init__(self) -> None:
        if not isinstance(
            self.outcome,
            CanonicalPlateAppearanceOutcome,
        ):
            raise TypeError(
                "outcome must be a "
                "CanonicalPlateAppearanceOutcome"
            )

        if not 0.0 <= self.probability <= 1.0:
            raise ValueError(
                "probability must be between 0 and 1"
            )


@dataclass(frozen=True)
class CanonicalPlateAppearanceQuery:
    """
    Immutable provider request for one plate appearance.

    This contract identifies the matchup and state but does not expose
    mutable orchestration or random-number-generator objects.
    """

    matchup_input: CanonicalMatchupInput
    state: GameState
    batter_id: str
    pitcher_id: str
    sequence: int
    trial_index: int
    trial_seed: int

    def __post_init__(self) -> None:
        if not isinstance(
            self.matchup_input,
            CanonicalMatchupInput,
        ):
            raise TypeError(
                "matchup_input must be a "
                "CanonicalMatchupInput"
            )

        if not isinstance(self.state, GameState):
            raise TypeError(
                "state must be a GameState"
            )

        if not self.batter_id:
            raise ValueError(
                "batter_id is required"
            )

        if not self.pitcher_id:
            raise ValueError(
                "pitcher_id is required"
            )

        batting_lineup = (
            self.matchup_input.away_lineup
            if self.state.half == "top"
            else self.matchup_input.home_lineup
        )

        fielding_plan = (
            self.matchup_input.home_pitching_plan
            if self.state.half == "top"
            else self.matchup_input.away_pitching_plan
        )

        if self.batter_id not in batting_lineup.player_ids:
            raise ValueError(
                "batter_id is not in the active batting lineup"
            )

        if (
            self.pitcher_id
            not in fielding_plan.available_pitcher_ids
        ):
            raise ValueError(
                "pitcher_id is not in the fielding "
                "team pitching plan"
            )

        if self.sequence < 0:
            raise ValueError(
                "sequence cannot be negative"
            )

        if self.trial_index < 0:
            raise ValueError(
                "trial_index cannot be negative"
            )

        if not 0 <= self.trial_seed <= MAX_CANONICAL_SEED:
            raise ValueError(
                "trial_seed is outside the supported range"
            )


@dataclass(frozen=True)
class CanonicalPlateAppearanceProbabilities:
    """
    Complete ordered probability distribution for one provider query.
    """

    query: CanonicalPlateAppearanceQuery
    probabilities: Tuple[
        CanonicalOutcomeProbability,
        ...,
    ]
    provider: CanonicalProbabilityProviderIdentity
    schema_version: str = (
        CANONICAL_PA_PROBABILITY_VERSION
    )

    def __post_init__(self) -> None:
        if not isinstance(
            self.query,
            CanonicalPlateAppearanceQuery,
        ):
            raise TypeError(
                "query must be a "
                "CanonicalPlateAppearanceQuery"
            )

        if not isinstance(
            self.provider,
            CanonicalProbabilityProviderIdentity,
        ):
            raise TypeError(
                "provider must be a "
                "CanonicalProbabilityProviderIdentity"
            )

        if (
            self.provider
            != self.query.matchup_input.probability_provider
        ):
            raise ValueError(
                "probability provider must match matchup identity"
            )

        if self.schema_version != (
            CANONICAL_PA_PROBABILITY_VERSION
        ):
            raise ValueError(
                "unsupported plate-appearance "
                "probability schema"
            )

        outcomes = tuple(
            point.outcome
            for point in self.probabilities
        )

        if outcomes != CANONICAL_PA_OUTCOME_ORDER:
            raise ValueError(
                "probabilities must contain every canonical "
                "outcome exactly once in canonical order"
            )

        total = sum(
            point.probability
            for point in self.probabilities
        )

        if abs(total - 1.0) > PROBABILITY_TOLERANCE:
            raise ValueError(
                "plate-appearance probabilities must sum to 1"
            )

    def probability_for(
        self,
        outcome: CanonicalPlateAppearanceOutcome,
    ) -> float:
        for point in self.probabilities:
            if point.outcome is outcome:
                return point.probability

        raise KeyError(outcome)


CanonicalPlateAppearanceProbabilityProvider = Callable[
    [CanonicalPlateAppearanceQuery],
    CanonicalPlateAppearanceProbabilities,
]


@dataclass(frozen=True)
class CanonicalSampledPlateAppearance:
    """Deterministic categorical sample from one PA distribution."""

    query: CanonicalPlateAppearanceQuery
    outcome: CanonicalPlateAppearanceOutcome
    draw: float
    sampling_seed: int
    sampling_version: str = (
        CANONICAL_PA_SAMPLING_VERSION
    )

    def __post_init__(self) -> None:
        if not 0.0 <= self.draw < 1.0:
            raise ValueError(
                "draw must be in the interval [0, 1)"
            )

        if not 0 <= self.sampling_seed <= MAX_CANONICAL_SEED:
            raise ValueError(
                "sampling_seed is outside the supported range"
            )

        if self.sampling_version != (
            CANONICAL_PA_SAMPLING_VERSION
        ):
            raise ValueError(
                "unsupported plate-appearance "
                "sampling version"
            )


def sample_canonical_plate_appearance(
    probabilities: CanonicalPlateAppearanceProbabilities,
) -> CanonicalSampledPlateAppearance:
    """
    Deterministically sample one categorical PA outcome.

    Sampling identity depends on the trial seed and immutable PA identity.
    No shared or mutable RNG is accepted.
    """

    if not isinstance(
        probabilities,
        CanonicalPlateAppearanceProbabilities,
    ):
        raise TypeError(
            "probabilities must be "
            "CanonicalPlateAppearanceProbabilities"
        )

    query = probabilities.query
    sampling_seed = derive_canonical_pa_sampling_seed(
        query=query,
        provider=probabilities.provider,
    )
    draw = random.Random(sampling_seed).random()

    cumulative = 0.0
    selected = None

    for point in probabilities.probabilities:
        cumulative += point.probability

        if draw < cumulative:
            selected = point.outcome
            break

    if selected is None:
        selected = probabilities.probabilities[-1].outcome

    return CanonicalSampledPlateAppearance(
        query=query,
        outcome=selected,
        draw=draw,
        sampling_seed=sampling_seed,
    )


def derive_canonical_pa_sampling_seed(
    *,
    query: CanonicalPlateAppearanceQuery,
    provider: CanonicalProbabilityProviderIdentity,
) -> int:
    """Derive stable RNG identity for one plate appearance."""

    if provider != query.matchup_input.probability_provider:
        raise ValueError(
            "provider must match matchup identity"
        )

    payload = "\x1f".join(
        (
            CANONICAL_PA_SAMPLING_VERSION,
            str(query.matchup_input.game_pk),
            str(query.trial_seed),
            str(query.trial_index),
            str(query.sequence),
            str(query.state.inning),
            query.state.half,
            str(query.state.outs),
            query.batter_id,
            query.pitcher_id,
            provider.identity,
        )
    ).encode("utf-8")

    digest = hashlib.sha256(payload).digest()

    return int.from_bytes(
        digest[:8],
        byteorder="big",
        signed=False,
    ) & MAX_CANONICAL_SEED
