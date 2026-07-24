"""Resolve sampled baserunning outcomes into canonical events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from mlb_app.simulation.events import (
    PlayEvent,
    build_baserunning_event,
)

from .baserunning_sampling import (
    CanonicalBaserunningOutcome,
    CanonicalSampledBaserunning,
)


CANONICAL_BASERUNNING_RESOLUTION_VERSION = (
    "canonical_baserunning_resolution_v1"
)


@dataclass(frozen=True)
class CanonicalBaserunningResolution:
    """Sampled decision paired with its optional ledger event."""

    sampled: CanonicalSampledBaserunning
    event: Optional[PlayEvent]
    resolution_version: str = (
        CANONICAL_BASERUNNING_RESOLUTION_VERSION
    )

    def __post_init__(self) -> None:
        if not isinstance(
            self.sampled,
            CanonicalSampledBaserunning,
        ):
            raise TypeError(
                "sampled must be CanonicalSampledBaserunning"
            )

        if (
            self.sampled.outcome
            is CanonicalBaserunningOutcome.HOLD
        ):
            if self.event is not None:
                raise ValueError(
                    "hold outcome cannot produce an event"
                )
        else:
            if self.event is None:
                raise ValueError(
                    "attempted steal must produce an event"
                )
            self._validate_event()

        if self.resolution_version != (
            CANONICAL_BASERUNNING_RESOLUTION_VERSION
        ):
            raise ValueError(
                "unsupported baserunning resolution version"
            )

    def _validate_event(self) -> None:
        if self.event is None:
            raise ValueError(
                "attempted steal must produce an event"
            )

        query = self.sampled.probabilities.query

        if self.event.event_type != self.sampled.outcome.value:
            raise ValueError(
                "event type must match sampled outcome"
            )
        if self.event.sequence != query.sequence:
            raise ValueError(
                "event sequence must match sampling query"
            )
        if self.event.batter_id != query.batter_id:
            raise ValueError(
                "event batter must match sampling query"
            )
        if self.event.pitcher_id != query.pitcher_id:
            raise ValueError(
                "event pitcher must match sampling query"
            )
        if self.event.state_before != query.state:
            raise ValueError(
                "event state must match sampling query"
            )
        if self.event.is_plate_appearance:
            raise ValueError(
                "baserunning event cannot be a plate appearance"
            )


def resolve_canonical_sampled_baserunning(
    sampled: CanonicalSampledBaserunning,
) -> CanonicalBaserunningResolution:
    """Convert one sampled steal decision into a ledger event."""

    if not isinstance(
        sampled,
        CanonicalSampledBaserunning,
    ):
        raise TypeError(
            "sampled must be CanonicalSampledBaserunning"
        )

    if sampled.outcome is CanonicalBaserunningOutcome.HOLD:
        return CanonicalBaserunningResolution(
            sampled=sampled,
            event=None,
        )

    query = sampled.probabilities.query
    event = build_baserunning_event(
        sequence=query.sequence,
        event_type=sampled.outcome.value,
        batter_id=query.batter_id,
        pitcher_id=query.pitcher_id,
        runner_id=query.runner_id,
        state_before=query.state,
        origin_base=query.origin_base,
        target_base=query.target_base,
    )

    return CanonicalBaserunningResolution(
        sampled=sampled,
        event=event,
    )
