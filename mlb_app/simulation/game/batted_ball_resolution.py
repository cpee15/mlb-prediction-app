"""Deterministic canonical batted-ball and runner advancement resolution."""

from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import random
from typing import Optional, Tuple

from mlb_app.simulation.events import (
    Base,
    BaselineBattedBallContextProvider,
    BaselineRunnerAdvancementSampler,
    BattedBallContext,
    OutRecord,
    PlayEvent,
    RunnerAdvancementResult,
    RunnerMovement,
    build_play_event,
)

from .factory_input import MAX_CANONICAL_SEED
from .probability import (
    CanonicalPlateAppearanceOutcome,
    CanonicalSampledPlateAppearance,
)


CANONICAL_BATTED_BALL_RESOLUTION_VERSION = (
    "canonical_batted_ball_resolution_v1"
)

CANONICAL_OUT_SUBTYPES: Tuple[str, ...] = (
    "groundout",
    "flyout",
    "lineout_popout",
    "other_out",
)

SUPPORTED_BATTED_BALL_ADVANCEMENT_OUTCOMES = frozenset(
    {
        CanonicalPlateAppearanceOutcome.OUT,
        CanonicalPlateAppearanceOutcome.SINGLE,
        CanonicalPlateAppearanceOutcome.DOUBLE,
    }
)


@dataclass(frozen=True)
class CanonicalBattedBallResolution:
    """Resolved event plus deterministic batted-ball provenance."""

    sampled: CanonicalSampledPlateAppearance
    event: PlayEvent
    context: BattedBallContext
    advancement: RunnerAdvancementResult
    context_seed: int
    advancement_seed: int
    outcome_subtype: Optional[str] = None
    resolution_version: str = (
        CANONICAL_BATTED_BALL_RESOLUTION_VERSION
    )

    def __post_init__(self) -> None:
        if self.event.state_before != self.sampled.query.state:
            raise ValueError(
                "resolved event must begin from sampled query state"
            )

        if self.event.batter_id != self.sampled.query.batter_id:
            raise ValueError(
                "resolved event batter must match sampled query"
            )

        if self.event.pitcher_id != self.sampled.query.pitcher_id:
            raise ValueError(
                "resolved event pitcher must match sampled query"
            )

        if self.context_seed < 0:
            raise ValueError(
                "context_seed cannot be negative"
            )

        if self.advancement_seed < 0:
            raise ValueError(
                "advancement_seed cannot be negative"
            )

        if self.resolution_version != (
            CANONICAL_BATTED_BALL_RESOLUTION_VERSION
        ):
            raise ValueError(
                "unsupported batted-ball resolution version"
            )


def resolve_canonical_batted_ball_outcome(
    sampled: CanonicalSampledPlateAppearance,
) -> CanonicalBattedBallResolution:
    """
    Resolve a sampled single, double, or batted-ball out.

    Context and advancement RNGs are independently derived from immutable
    sampled plate-appearance identity. No shared mutable RNG is accepted.
    """

    if not isinstance(
        sampled,
        CanonicalSampledPlateAppearance,
    ):
        raise TypeError(
            "sampled must be a "
            "CanonicalSampledPlateAppearance"
        )

    if (
        sampled.outcome
        not in SUPPORTED_BATTED_BALL_ADVANCEMENT_OUTCOMES
    ):
        raise ValueError(
            "sampled outcome is not supported by the "
            "batted-ball advancement resolver"
        )

    query = sampled.query
    outcome = sampled.outcome
    context_seed = derive_canonical_batted_ball_seed(
        sampled=sampled,
        purpose="context",
    )
    advancement_seed = derive_canonical_batted_ball_seed(
        sampled=sampled,
        purpose="advancement",
    )

    outcome_subtype = (
        _sample_outcome_subtype(context_seed)
        if outcome is CanonicalPlateAppearanceOutcome.OUT
        else None
    )

    context = BaselineBattedBallContextProvider(
        rng=random.Random(context_seed),
    ).sample(
        primary_outcome=outcome.value,
        outcome_subtype=outcome_subtype,
    )

    if context is None:
        raise RuntimeError(
            "batted-ball outcome produced no context"
        )

    advancement = BaselineRunnerAdvancementSampler(
        rng=random.Random(advancement_seed),
    ).sample(
        state=query.state,
        batter_id=query.batter_id,
        primary_outcome=outcome.value,
        context=context,
    )

    if outcome is CanonicalPlateAppearanceOutcome.OUT:
        movements = advancement.movements + (
            RunnerMovement(
                runner_id=query.batter_id,
                start_base=Base.HOME,
                end_base=None,
                is_out=True,
            ),
        )
        outs_recorded = (
            OutRecord(
                runner_id=query.batter_id,
                out_number=query.state.outs + 1,
                reason=outcome_subtype,
            ),
        )
    else:
        movements = advancement.movements
        outs_recorded = ()

    event = build_play_event(
        sequence=query.sequence,
        event_type=outcome.value,
        batter_id=query.batter_id,
        state_before=query.state,
        runner_movements=movements,
        outs_recorded=outs_recorded,
    )

    event = replace(
        event,
        pitcher_id=query.pitcher_id,
    )

    return CanonicalBattedBallResolution(
        sampled=sampled,
        event=event,
        context=context,
        advancement=advancement,
        context_seed=context_seed,
        advancement_seed=advancement_seed,
        outcome_subtype=outcome_subtype,
    )


def derive_canonical_batted_ball_seed(
    *,
    sampled: CanonicalSampledPlateAppearance,
    purpose: str,
) -> int:
    """Derive deterministic independent RNG identity for one stage."""

    if not isinstance(
        sampled,
        CanonicalSampledPlateAppearance,
    ):
        raise TypeError(
            "sampled must be a "
            "CanonicalSampledPlateAppearance"
        )

    if not purpose:
        raise ValueError(
            "purpose is required"
        )

    query = sampled.query

    payload = "\x1f".join(
        (
            CANONICAL_BATTED_BALL_RESOLUTION_VERSION,
            purpose,
            str(query.matchup_input.game_pk),
            str(query.trial_index),
            str(query.trial_seed),
            str(query.sequence),
            str(sampled.sampling_seed),
            sampled.outcome.value,
            query.batter_id,
            query.pitcher_id,
        )
    ).encode("utf-8")

    digest = hashlib.sha256(payload).digest()

    return int.from_bytes(
        digest[:8],
        byteorder="big",
        signed=False,
    ) & MAX_CANONICAL_SEED


def _sample_outcome_subtype(
    context_seed: int,
) -> str:
    rng = random.Random(
        context_seed ^ 0x5A5A5A5A
    )

    return CANONICAL_OUT_SUBTYPES[
        rng.randrange(
            len(CANONICAL_OUT_SUBTYPES)
        )
    ]
