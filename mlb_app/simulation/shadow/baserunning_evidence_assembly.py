"""Assemble complete canonical baserunning evidence for shadow use."""

from __future__ import annotations

from typing import Tuple

from .baserunning_evidence_discovery import (
    CanonicalShadowBaserunningEvidenceDiscovery,
)
from .catcher_observation_composition import (
    CanonicalCatcherObservationComposition,
)
from .observed_baserunning_evidence import (
    discover_materialized_runner_baserunning_evidence,
)
from .pitcher_baserunning_context_composition import (
    compose_pitcher_baserunning_contexts,
)
from .pitcher_delivery_time_evidence import (
    CanonicalPitcherDeliveryTimeObservation,
)
from .pitcher_hold_evidence import (
    adapt_statcast_pitcher_hold_evidence,
)
from .runner_availability_evidence import (
    CanonicalRunnerAvailabilityObservation,
)
from .runner_baserunning_context_composition import (
    compose_runner_baserunning_contexts,
)
from .runner_lead_quality_evidence import (
    CanonicalRunnerLeadQualityObservation,
)
from .runner_sprint_speed_evidence import (
    CanonicalRunnerSprintSpeedObservation,
)
from .statcast_baserunning_source import (
    CanonicalStatcastPitcherBaserunningCounts,
    CanonicalStatcastPitcherPickoffCounts,
    CanonicalStatcastRunnerBaserunningCounts,
)


CANONICAL_BASERUNNING_EVIDENCE_ASSEMBLY_VERSION = (
    "canonical_baserunning_evidence_assembly_v1"
)


def _assembly_error(
    *,
    required_runner_ids: Tuple[str, ...],
    required_pitcher_ids: Tuple[str, ...],
    error: Exception,
) -> CanonicalShadowBaserunningEvidenceDiscovery:
    return CanonicalShadowBaserunningEvidenceDiscovery(
        requested_runner_count=len(
            required_runner_ids
        ),
        requested_pitcher_count=len(
            required_pitcher_ids
        ),
        status="error",
        error_message=str(error),
    )


def assemble_complete_canonical_baserunning_evidence(
    *,
    required_runner_ids: Tuple[str, ...],
    required_pitcher_ids: Tuple[str, ...],
    catcher_composition: (
        CanonicalCatcherObservationComposition
    ),
    runner_counts: Tuple[
        CanonicalStatcastRunnerBaserunningCounts,
        ...,
    ] = (),
    runner_sprint_speed_observations: Tuple[
        CanonicalRunnerSprintSpeedObservation,
        ...,
    ] = (),
    runner_lead_quality_observations: Tuple[
        CanonicalRunnerLeadQualityObservation,
        ...,
    ] = (),
    runner_availability_observations: Tuple[
        CanonicalRunnerAvailabilityObservation,
        ...,
    ] = (),
    pitcher_baserunning_counts: Tuple[
        CanonicalStatcastPitcherBaserunningCounts,
        ...,
    ] = (),
    pitcher_pickoff_counts: Tuple[
        CanonicalStatcastPitcherPickoffCounts,
        ...,
    ] = (),
    pitcher_delivery_time_observations: Tuple[
        CanonicalPitcherDeliveryTimeObservation,
        ...,
    ] = (),
) -> CanonicalShadowBaserunningEvidenceDiscovery:
    """
    Assemble all participant evidence into one canonical catalog.

    Runner context requires sprint speed, lead quality, and availability.
    Pitcher context requires exact attempt exposure and delivery time.
    Catcher observations must already be composed from confirmed assignments.

    Missing evidence is omitted by the underlying inner joins. Invalid inputs
    fail open. Legacy production authority remains unchanged.
    """

    try:
        runner_contexts = (
            compose_runner_baserunning_contexts(
                sprint_speed_observations=(
                    runner_sprint_speed_observations
                ),
                lead_quality_observations=(
                    runner_lead_quality_observations
                ),
                availability_observations=(
                    runner_availability_observations
                ),
            )
        )

        pitcher_hold_observations = (
            adapt_statcast_pitcher_hold_evidence(
                pitcher_baserunning_counts
            )
        )

        pitcher_contexts = (
            compose_pitcher_baserunning_contexts(
                hold_observations=(
                    pitcher_hold_observations
                ),
                delivery_time_observations=(
                    pitcher_delivery_time_observations
                ),
            )
        )
    except Exception as exc:
        return _assembly_error(
            required_runner_ids=required_runner_ids,
            required_pitcher_ids=required_pitcher_ids,
            error=exc,
        )

    return discover_materialized_runner_baserunning_evidence(
        required_runner_ids=required_runner_ids,
        required_pitcher_ids=required_pitcher_ids,
        catcher_composition=catcher_composition,
        runner_counts=runner_counts,
        runner_contexts=runner_contexts,
        pitcher_pickoff_counts=pitcher_pickoff_counts,
        pitcher_contexts=pitcher_contexts,
    )
