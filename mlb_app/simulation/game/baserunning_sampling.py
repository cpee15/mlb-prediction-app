"""Deterministic canonical steal-attempt sampling."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import random
from typing import Optional

from mlb_app.simulation.events import Base, GameState

from .factory_input import MAX_CANONICAL_SEED


CANONICAL_BASERUNNING_SAMPLING_VERSION = (
    "canonical_baserunning_sampling_v1"
)

_SUPPORTED_TRANSITIONS = frozenset(
    {
        (Base.FIRST, Base.SECOND),
        (Base.SECOND, Base.THIRD),
    }
)


class CanonicalBaserunningOutcome(str, Enum):
    """Terminal result of one sampled steal opportunity."""

    HOLD = "hold"
    STOLEN_BASE = "stolen_base"
    CAUGHT_STEALING = "caught_stealing"


@dataclass(frozen=True)
class CanonicalBaserunningSamplingQuery:
    """Immutable identity and game state for one steal opportunity."""

    game_pk: int
    trial_index: int
    trial_seed: int
    sequence: int
    state: GameState
    batter_id: str
    pitcher_id: str
    runner_id: str
    origin_base: Base
    target_base: Base

    def __post_init__(self) -> None:
        if self.game_pk <= 0:
            raise ValueError("game_pk must be positive")
        if self.trial_index < 0:
            raise ValueError(
                "trial_index cannot be negative"
            )
        if not 0 <= self.trial_seed <= MAX_CANONICAL_SEED:
            raise ValueError(
                "trial_seed is outside the supported range"
            )
        if self.sequence < 0:
            raise ValueError(
                "sequence cannot be negative"
            )
        if not isinstance(self.state, GameState):
            raise TypeError(
                "state must be a GameState"
            )
        if self.state.outs >= 3:
            raise ValueError(
                "cannot sample baserunning after three outs"
            )
        if not self.batter_id:
            raise ValueError("batter_id is required")
        if not self.pitcher_id:
            raise ValueError("pitcher_id is required")
        if not self.runner_id:
            raise ValueError("runner_id is required")
        if (
            self.origin_base,
            self.target_base,
        ) not in _SUPPORTED_TRANSITIONS:
            raise ValueError(
                "unsupported baserunning base transition"
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
class CanonicalBaserunningProbabilities:
    """Attempt and success probabilities with source provenance."""

    query: CanonicalBaserunningSamplingQuery
    attempt_probability: float
    success_probability: float
    probability_provenance: str

    def __post_init__(self) -> None:
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


@dataclass(frozen=True)
class CanonicalSampledBaserunning:
    """One reproducible steal decision with complete RNG provenance."""

    probabilities: CanonicalBaserunningProbabilities
    outcome: CanonicalBaserunningOutcome
    attempt_seed: int
    attempt_draw: float
    success_seed: Optional[int] = None
    success_draw: Optional[float] = None
    sampling_version: str = (
        CANONICAL_BASERUNNING_SAMPLING_VERSION
    )

    def __post_init__(self) -> None:
        if not 0 <= self.attempt_seed <= MAX_CANONICAL_SEED:
            raise ValueError(
                "attempt_seed is outside the supported range"
            )
        if not 0.0 <= self.attempt_draw < 1.0:
            raise ValueError(
                "attempt_draw must be in [0, 1)"
            )

        if self.outcome is CanonicalBaserunningOutcome.HOLD:
            if (
                self.success_seed is not None
                or self.success_draw is not None
            ):
                raise ValueError(
                    "hold outcome cannot have a success draw"
                )
        else:
            if self.success_seed is None:
                raise ValueError(
                    "steal attempt requires success_seed"
                )
            if self.success_draw is None:
                raise ValueError(
                    "steal attempt requires success_draw"
                )
            if not (
                0
                <= self.success_seed
                <= MAX_CANONICAL_SEED
            ):
                raise ValueError(
                    "success_seed is outside the supported range"
                )
            if not 0.0 <= self.success_draw < 1.0:
                raise ValueError(
                    "success_draw must be in [0, 1)"
                )

        if self.sampling_version != (
            CANONICAL_BASERUNNING_SAMPLING_VERSION
        ):
            raise ValueError(
                "unsupported baserunning sampling version"
            )


def sample_canonical_baserunning(
    probabilities: CanonicalBaserunningProbabilities,
) -> CanonicalSampledBaserunning:
    """Sample attempt and success with independent derived RNGs."""

    if not isinstance(
        probabilities,
        CanonicalBaserunningProbabilities,
    ):
        raise TypeError(
            "probabilities must be "
            "CanonicalBaserunningProbabilities"
        )

    attempt_seed = derive_canonical_baserunning_seed(
        probabilities=probabilities,
        purpose="attempt",
    )
    attempt_draw = random.Random(
        attempt_seed
    ).random()

    if (
        attempt_draw
        >= probabilities.attempt_probability
    ):
        return CanonicalSampledBaserunning(
            probabilities=probabilities,
            outcome=CanonicalBaserunningOutcome.HOLD,
            attempt_seed=attempt_seed,
            attempt_draw=attempt_draw,
        )

    success_seed = derive_canonical_baserunning_seed(
        probabilities=probabilities,
        purpose="success",
    )
    success_draw = random.Random(
        success_seed
    ).random()

    outcome = (
        CanonicalBaserunningOutcome.STOLEN_BASE
        if (
            success_draw
            < probabilities.success_probability
        )
        else CanonicalBaserunningOutcome.CAUGHT_STEALING
    )

    return CanonicalSampledBaserunning(
        probabilities=probabilities,
        outcome=outcome,
        attempt_seed=attempt_seed,
        attempt_draw=attempt_draw,
        success_seed=success_seed,
        success_draw=success_draw,
    )


def derive_canonical_baserunning_seed(
    *,
    probabilities: CanonicalBaserunningProbabilities,
    purpose: str,
) -> int:
    """Derive stable independent RNG identity for one decision."""

    if not isinstance(
        probabilities,
        CanonicalBaserunningProbabilities,
    ):
        raise TypeError(
            "probabilities must be "
            "CanonicalBaserunningProbabilities"
        )
    if purpose not in {"attempt", "success"}:
        raise ValueError(
            "purpose must be attempt or success"
        )

    query = probabilities.query
    bases = tuple(
        runner_id or ""
        for runner_id in query.state.bases
    )
    payload = "\x1f".join(
        (
            CANONICAL_BASERUNNING_SAMPLING_VERSION,
            purpose,
            str(query.game_pk),
            str(query.trial_index),
            str(query.trial_seed),
            str(query.sequence),
            str(query.state.inning),
            query.state.half,
            str(query.state.outs),
            *bases,
            query.batter_id,
            query.pitcher_id,
            query.runner_id,
            str(int(query.origin_base)),
            str(int(query.target_base)),
            probabilities.probability_provenance,
        )
    ).encode("utf-8")

    digest = hashlib.sha256(payload).digest()
    return int.from_bytes(
        digest[:8],
        byteorder="big",
        signed=False,
    ) & MAX_CANONICAL_SEED
