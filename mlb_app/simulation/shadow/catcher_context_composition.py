"""Compose exact catcher assignments with sourced pop-time evidence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from .catcher_pop_time_evidence import (
    CanonicalCatcherPopTimeObservation,
)
from .statcast_baserunning_source import (
    CanonicalCatcherBaserunningContext,
)


CANONICAL_CATCHER_CONTEXT_COMPOSITION_VERSION = (
    "canonical_catcher_context_composition_v1"
)


@dataclass(frozen=True)
class CanonicalCatcherTeamAssignment:
    """Exact active catcher assignment for one matchup side."""

    catcher_id: str
    team_side: str
    assignment_source_version: str = "unavailable"
    composition_version: str = (
        CANONICAL_CATCHER_CONTEXT_COMPOSITION_VERSION
    )

    def __post_init__(self) -> None:
        if not self.catcher_id:
            raise ValueError(
                "catcher_id is required"
            )

        if self.team_side not in {
            "away",
            "home",
        }:
            raise ValueError(
                "team_side must be away or home"
            )

        if (
            not self.assignment_source_version
            or self.assignment_source_version
            == "unavailable"
        ):
            raise ValueError(
                "assignment_source_version must identify "
                "an available source"
            )

        if self.composition_version != (
            CANONICAL_CATCHER_CONTEXT_COMPOSITION_VERSION
        ):
            raise ValueError(
                "unsupported catcher context "
                "composition version"
            )


def compose_catcher_baserunning_contexts(
    *,
    assignments: Tuple[
        CanonicalCatcherTeamAssignment,
        ...,
    ],
    pop_times: Tuple[
        CanonicalCatcherPopTimeObservation,
        ...,
    ],
) -> Tuple[
    CanonicalCatcherBaserunningContext,
    ...,
]:
    """
    Join active catcher assignments to exact pop-time evidence.

    A catcher without pop-time evidence is omitted. Pop-time evidence
    without an active matchup assignment is ignored. No catcher identity,
    team side, or measurement is inferred.
    """

    for value in assignments:
        if not isinstance(
            value,
            CanonicalCatcherTeamAssignment,
        ):
            raise TypeError(
                "assignments must contain "
                "CanonicalCatcherTeamAssignment"
            )

    for value in pop_times:
        if not isinstance(
            value,
            CanonicalCatcherPopTimeObservation,
        ):
            raise TypeError(
                "pop_times must contain "
                "CanonicalCatcherPopTimeObservation"
            )

    assignment_ids = [
        value.catcher_id
        for value in assignments
    ]
    assignment_sides = [
        value.team_side
        for value in assignments
    ]
    pop_time_ids = [
        value.catcher_id
        for value in pop_times
    ]

    if len(assignment_ids) != len(
        set(assignment_ids)
    ):
        raise ValueError(
            "catcher assignment identifiers must be unique"
        )

    if len(assignment_sides) != len(
        set(assignment_sides)
    ):
        raise ValueError(
            "catcher assignment team sides must be unique"
        )

    if len(pop_time_ids) != len(
        set(pop_time_ids)
    ):
        raise ValueError(
            "catcher pop-time identifiers must be unique"
        )

    pop_times_by_id = {
        value.catcher_id: value
        for value in pop_times
    }

    contexts = []

    for assignment in assignments:
        pop_time = pop_times_by_id.get(
            assignment.catcher_id
        )

        if pop_time is None:
            continue

        contexts.append(
            CanonicalCatcherBaserunningContext(
                catcher_id=assignment.catcher_id,
                team_side=assignment.team_side,
                pop_time_score=(
                    pop_time.pop_time_score
                ),
                context_source_version=(
                    f"{assignment.assignment_source_version}+"
                    f"{pop_time.source_version}+"
                    f"{pop_time.normalization_version}"
                ),
            )
        )

    return tuple(contexts)
