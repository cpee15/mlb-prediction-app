"""
Compose complete canonical runner baserunning contexts.

A context is emitted only when sprint-speed, lead-quality, and
availability evidence are all present for the same runner. Missing
evidence is never replaced with a neutral value.
"""

from typing import Tuple

from .runner_availability_evidence import (
    CanonicalRunnerAvailabilityObservation,
)
from .runner_lead_quality_evidence import (
    CanonicalRunnerLeadQualityObservation,
)
from .runner_sprint_speed_evidence import (
    CanonicalRunnerSprintSpeedObservation,
)
from .statcast_baserunning_source import (
    CanonicalRunnerBaserunningContext,
)


CANONICAL_RUNNER_CONTEXT_COMPOSITION_VERSION = (
    "canonical_runner_context_composition_v1"
)


def _validate_observations(
    *,
    values: tuple,
    expected_type: type,
    argument_name: str,
) -> None:
    if not isinstance(values, tuple):
        raise TypeError(
            f"{argument_name} must be a tuple"
        )

    if any(
        not isinstance(value, expected_type)
        for value in values
    ):
        raise TypeError(
            f"{argument_name} must contain "
            f"{expected_type.__name__}"
        )

    runner_ids = [
        value.runner_id
        for value in values
    ]
    if len(runner_ids) != len(set(runner_ids)):
        raise ValueError(
            f"{argument_name} runner identifiers "
            "must be unique"
        )


def _context_source_version(
    *,
    sprint_speed: CanonicalRunnerSprintSpeedObservation,
    lead_quality: CanonicalRunnerLeadQualityObservation,
    availability: CanonicalRunnerAvailabilityObservation,
) -> str:
    return "|".join(
        (
            sprint_speed.source_version,
            sprint_speed.normalization_version,
            lead_quality.source_version,
            lead_quality.evidence_version,
            availability.source_version,
            availability.evidence_version,
            CANONICAL_RUNNER_CONTEXT_COMPOSITION_VERSION,
        )
    )


def compose_runner_baserunning_contexts(
    *,
    sprint_speed_observations: Tuple[
        CanonicalRunnerSprintSpeedObservation,
        ...,
    ],
    lead_quality_observations: Tuple[
        CanonicalRunnerLeadQualityObservation,
        ...,
    ],
    availability_observations: Tuple[
        CanonicalRunnerAvailabilityObservation,
        ...,
    ],
) -> Tuple[
    CanonicalRunnerBaserunningContext,
    ...,
]:
    """
    Inner-join complete runner evidence into canonical contexts.

    Output order follows sprint-speed observation order so composition
    remains stable while incomplete runners are omitted.
    """

    _validate_observations(
        values=sprint_speed_observations,
        expected_type=(
            CanonicalRunnerSprintSpeedObservation
        ),
        argument_name="sprint_speed_observations",
    )
    _validate_observations(
        values=lead_quality_observations,
        expected_type=(
            CanonicalRunnerLeadQualityObservation
        ),
        argument_name="lead_quality_observations",
    )
    _validate_observations(
        values=availability_observations,
        expected_type=(
            CanonicalRunnerAvailabilityObservation
        ),
        argument_name="availability_observations",
    )

    lead_by_runner = {
        value.runner_id: value
        for value in lead_quality_observations
    }
    availability_by_runner = {
        value.runner_id: value
        for value in availability_observations
    }

    contexts = []
    for sprint_speed in sprint_speed_observations:
        lead_quality = lead_by_runner.get(
            sprint_speed.runner_id
        )
        availability = availability_by_runner.get(
            sprint_speed.runner_id
        )

        if (
            lead_quality is None
            or availability is None
        ):
            continue

        contexts.append(
            CanonicalRunnerBaserunningContext(
                runner_id=sprint_speed.runner_id,
                speed_score=sprint_speed.speed_score,
                lead_quality=(
                    lead_quality.lead_quality_score
                ),
                fatigue_index=(
                    availability.fatigue_score
                ),
                injury_limit_flag=(
                    availability.injury_limit_flag
                ),
                context_source_version=(
                    _context_source_version(
                        sprint_speed=sprint_speed,
                        lead_quality=lead_quality,
                        availability=availability,
                    )
                ),
            )
        )

    return tuple(contexts)
