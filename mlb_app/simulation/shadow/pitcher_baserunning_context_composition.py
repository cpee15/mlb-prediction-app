"""Compose complete canonical pitcher baserunning contexts."""

from __future__ import annotations

from typing import Tuple

from .pitcher_delivery_time_evidence import (
    CanonicalPitcherDeliveryTimeObservation,
)
from .pitcher_hold_evidence import (
    CanonicalPitcherHoldObservation,
)
from .statcast_baserunning_source import (
    CanonicalPitcherBaserunningContext,
)


CANONICAL_PITCHER_CONTEXT_COMPOSITION_VERSION = (
    "canonical_pitcher_context_composition_v1"
)


def compose_pitcher_baserunning_contexts(
    *,
    hold_observations: Tuple[
        CanonicalPitcherHoldObservation,
        ...,
    ],
    delivery_time_observations: Tuple[
        CanonicalPitcherDeliveryTimeObservation,
        ...,
    ],
) -> Tuple[
    CanonicalPitcherBaserunningContext,
    ...,
]:
    """
    Join explicit hold and delivery-time evidence by pitcher.

    A pitcher missing either evidence source is omitted. No neutral hold
    or delivery-time score is supplied. Output order follows the hold
    observations so composition remains deterministic.
    """

    if not isinstance(hold_observations, tuple):
        raise TypeError(
            "hold_observations must be a tuple"
        )
    if not isinstance(
        delivery_time_observations,
        tuple,
    ):
        raise TypeError(
            "delivery_time_observations must be a tuple"
        )

    if any(
        not isinstance(
            value,
            CanonicalPitcherHoldObservation,
        )
        for value in hold_observations
    ):
        raise TypeError(
            "hold_observations must contain "
            "CanonicalPitcherHoldObservation"
        )

    if any(
        not isinstance(
            value,
            CanonicalPitcherDeliveryTimeObservation,
        )
        for value in delivery_time_observations
    ):
        raise TypeError(
            "delivery_time_observations must contain "
            "CanonicalPitcherDeliveryTimeObservation"
        )

    hold_ids = [
        value.pitcher_id
        for value in hold_observations
    ]
    delivery_ids = [
        value.pitcher_id
        for value in delivery_time_observations
    ]

    if len(hold_ids) != len(set(hold_ids)):
        raise ValueError(
            "pitcher hold observation identifiers "
            "must be unique"
        )
    if len(delivery_ids) != len(set(delivery_ids)):
        raise ValueError(
            "pitcher delivery-time observation identifiers "
            "must be unique"
        )

    delivery_by_id = {
        value.pitcher_id: value
        for value in delivery_time_observations
    }

    contexts = []

    for hold in hold_observations:
        delivery = delivery_by_id.get(
            hold.pitcher_id
        )
        if delivery is None:
            continue

        contexts.append(
            CanonicalPitcherBaserunningContext(
                pitcher_id=hold.pitcher_id,
                hold_score=hold.hold_score,
                delivery_time_score=(
                    delivery.delivery_time_score
                ),
                context_source_version=(
                    f"{hold.source_version}+"
                    f"{hold.evidence_version}+"
                    f"{hold.normalization_version}+"
                    f"{delivery.source_version}+"
                    f"{delivery.normalization_version}+"
                    f"{CANONICAL_PITCHER_CONTEXT_COMPOSITION_VERSION}"
                ),
            )
        )

    return tuple(contexts)
