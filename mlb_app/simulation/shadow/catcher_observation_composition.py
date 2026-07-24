"""Compose complete observed catcher baserunning evidence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from .catcher_assignment_discovery import (
    CanonicalCatcherAssignmentDiscovery,
)
from .catcher_baserunning_evidence import (
    CanonicalCatcherBaserunningObservation,
)
from .catcher_context_composition import (
    compose_catcher_baserunning_contexts,
)
from .catcher_pop_time_evidence import (
    CanonicalCatcherPopTimeObservation,
)
from .statcast_baserunning_source import (
    CanonicalStatcastCatcherBaserunningCounts,
    materialize_statcast_catcher_observations,
)


CANONICAL_CATCHER_OBSERVATION_COMPOSITION_VERSION = (
    "canonical_catcher_observation_composition_v1"
)

_VALID_STATUSES = {
    "ready",
    "unavailable",
    "error",
}


@dataclass(frozen=True)
class CanonicalCatcherObservationComposition:
    """Fail-open result of composing matchup catcher evidence."""

    observations: Tuple[
        CanonicalCatcherBaserunningObservation,
        ...,
    ] = ()
    assignment_count: int = 0
    count_record_count: int = 0
    pop_time_count: int = 0
    context_count: int = 0
    status: str = "unavailable"
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    composition_version: str = (
        CANONICAL_CATCHER_OBSERVATION_COMPOSITION_VERSION
    )

    def __post_init__(self) -> None:
        if self.status not in _VALID_STATUSES:
            raise ValueError(
                "unsupported catcher observation "
                "composition status"
            )

        for name, value in (
            ("assignment_count", self.assignment_count),
            ("count_record_count", self.count_record_count),
            ("pop_time_count", self.pop_time_count),
            ("context_count", self.context_count),
        ):
            if not isinstance(value, int):
                raise TypeError(
                    f"{name} must be an integer"
                )
            if value < 0:
                raise ValueError(
                    f"{name} must be nonnegative"
                )

        for value in self.observations:
            if not isinstance(
                value,
                CanonicalCatcherBaserunningObservation,
            ):
                raise TypeError(
                    "observations must contain "
                    "CanonicalCatcherBaserunningObservation"
                )

        if self.status == "ready":
            if len(self.observations) != 2:
                raise ValueError(
                    "ready composition requires two "
                    "catcher observations"
                )

            sides = {
                value.team_side
                for value in self.observations
            }

            if sides != {"away", "home"}:
                raise ValueError(
                    "ready composition requires away "
                    "and home catcher observations"
                )
        elif self.observations:
            raise ValueError(
                "non-ready composition cannot expose "
                "catcher observations"
            )

        if self.composition_version != (
            CANONICAL_CATCHER_OBSERVATION_COMPOSITION_VERSION
        ):
            raise ValueError(
                "unsupported catcher observation "
                "composition version"
            )

    @property
    def ready(self) -> bool:
        return (
            self.status == "ready"
            and len(self.observations) == 2
        )

    @property
    def away_observation(
        self,
    ) -> Optional[
        CanonicalCatcherBaserunningObservation
    ]:
        return next(
            (
                value
                for value in self.observations
                if value.team_side == "away"
            ),
            None,
        )

    @property
    def home_observation(
        self,
    ) -> Optional[
        CanonicalCatcherBaserunningObservation
    ]:
        return next(
            (
                value
                for value in self.observations
                if value.team_side == "home"
            ),
            None,
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.composition_version,
            "status": self.status,
            "ready": self.ready,
            "assignment_count": self.assignment_count,
            "count_record_count": self.count_record_count,
            "pop_time_count": self.pop_time_count,
            "context_count": self.context_count,
            "observation_count": len(
                self.observations
            ),
            "error_type": self.error_type,
            "error_message": self.error_message,
            "production_activation": False,
            "authoritative_source": "legacy",
        }


def compose_confirmed_catcher_observations(
    *,
    assignment_discovery: (
        CanonicalCatcherAssignmentDiscovery
    ),
    counts: Tuple[
        CanonicalStatcastCatcherBaserunningCounts,
        ...,
    ] = (),
    pop_times: Tuple[
        CanonicalCatcherPopTimeObservation,
        ...,
    ] = (),
) -> CanonicalCatcherObservationComposition:
    """
    Compose complete catcher observations for both matchup sides.

    Confirmed identity, exact outcome counts, and sourced pop time must
    all be present for both teams. Partial or invalid evidence fails open
    and cannot expose observations or activate production behavior.
    """

    if not isinstance(
        assignment_discovery,
        CanonicalCatcherAssignmentDiscovery,
    ):
        return CanonicalCatcherObservationComposition(
            status="error",
            error_type="TypeError",
            error_message=(
                "assignment_discovery must be "
                "CanonicalCatcherAssignmentDiscovery"
            ),
        )

    assignment_count = len(
        assignment_discovery.assignments
    )
    count_record_count = (
        len(counts)
        if isinstance(counts, tuple)
        else 0
    )
    pop_time_count = (
        len(pop_times)
        if isinstance(pop_times, tuple)
        else 0
    )

    if not assignment_discovery.ready:
        return CanonicalCatcherObservationComposition(
            assignment_count=assignment_count,
            count_record_count=count_record_count,
            pop_time_count=pop_time_count,
            status="unavailable",
        )

    try:
        if not isinstance(counts, tuple):
            raise TypeError(
                "counts must be a tuple"
            )

        if not isinstance(pop_times, tuple):
            raise TypeError(
                "pop_times must be a tuple"
            )

        contexts = compose_catcher_baserunning_contexts(
            assignments=(
                assignment_discovery.assignments
            ),
            pop_times=pop_times,
        )

        observations = (
            materialize_statcast_catcher_observations(
                counts=counts,
                contexts=contexts,
            )
        )

        observations_by_side = {
            value.team_side: value
            for value in observations
        }

        complete = (
            len(contexts) == 2
            and len(observations) == 2
            and set(observations_by_side)
            == {"away", "home"}
        )

        if not complete:
            return (
                CanonicalCatcherObservationComposition(
                    assignment_count=assignment_count,
                    count_record_count=(
                        count_record_count
                    ),
                    pop_time_count=pop_time_count,
                    context_count=len(contexts),
                    status="unavailable",
                )
            )

        ordered = (
            observations_by_side["away"],
            observations_by_side["home"],
        )

        return CanonicalCatcherObservationComposition(
            observations=ordered,
            assignment_count=assignment_count,
            count_record_count=count_record_count,
            pop_time_count=pop_time_count,
            context_count=len(contexts),
            status="ready",
        )
    except Exception as exc:
        return CanonicalCatcherObservationComposition(
            assignment_count=assignment_count,
            count_record_count=count_record_count,
            pop_time_count=pop_time_count,
            status="error",
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
