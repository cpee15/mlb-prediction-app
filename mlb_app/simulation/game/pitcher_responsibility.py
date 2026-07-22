"""Canonical pitcher responsibility for baserunners and scored runs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

from mlb_app.simulation.events import PlayEvent


CANONICAL_PITCHER_RESPONSIBILITY_VERSION = (
    "canonical_pitcher_responsibility_v1"
)


@dataclass(frozen=True)
class CanonicalRunnerResponsibility:
    """Pitcher responsible for one active baserunner."""

    runner_id: str
    responsible_pitcher_id: str
    reached_on_event_sequence: int
    reached_on_event_type: str
    schema_version: str = (
        CANONICAL_PITCHER_RESPONSIBILITY_VERSION
    )

    def __post_init__(self) -> None:
        if not self.runner_id:
            raise ValueError("runner_id is required")

        if not self.responsible_pitcher_id:
            raise ValueError(
                "responsible_pitcher_id is required"
            )

        if self.reached_on_event_sequence < 0:
            raise ValueError(
                "reached_on_event_sequence cannot be negative"
            )

        if not self.reached_on_event_type:
            raise ValueError(
                "reached_on_event_type is required"
            )

        if self.schema_version != (
            CANONICAL_PITCHER_RESPONSIBILITY_VERSION
        ):
            raise ValueError(
                "unsupported responsibility schema"
            )


@dataclass(frozen=True)
class CanonicalScoredRunResponsibility:
    """Responsibility attribution for one runner who scored."""

    runner_id: str
    responsible_pitcher_id: str
    pitcher_on_mound_id: str
    scoring_event_sequence: int
    scoring_event_type: str
    schema_version: str = (
        CANONICAL_PITCHER_RESPONSIBILITY_VERSION
    )

    def __post_init__(self) -> None:
        if not self.runner_id:
            raise ValueError("runner_id is required")

        if not self.responsible_pitcher_id:
            raise ValueError(
                "responsible_pitcher_id is required"
            )

        if not self.pitcher_on_mound_id:
            raise ValueError(
                "pitcher_on_mound_id is required"
            )

        if self.scoring_event_sequence < 0:
            raise ValueError(
                "scoring_event_sequence cannot be negative"
            )

        if not self.scoring_event_type:
            raise ValueError(
                "scoring_event_type is required"
            )

        if self.schema_version != (
            CANONICAL_PITCHER_RESPONSIBILITY_VERSION
        ):
            raise ValueError(
                "unsupported responsibility schema"
            )


class CanonicalPitcherResponsibilityLedger:
    """
    Mutable runner ledger owned by exactly one canonical trial.

    Responsibility is assigned when a batter becomes a runner and is
    preserved until that runner scores or is retired.
    """

    def __init__(self) -> None:
        self._active: Dict[
            str,
            CanonicalRunnerResponsibility,
        ] = {}
        self._scored: list[
            CanonicalScoredRunResponsibility
        ] = []

    def apply_event(
        self,
        event: PlayEvent,
    ) -> Tuple[
        CanonicalScoredRunResponsibility,
        ...,
    ]:
        if not isinstance(event, PlayEvent):
            raise TypeError(
                "event must be a PlayEvent"
            )

        if not event.pitcher_id:
            raise ValueError(
                "event pitcher_id is required"
            )

        scored_before = len(self._scored)

        for movement in event.runner_movements:
            runner_id = movement.runner_id

            if movement.start_base == 0:
                if runner_id in self._active:
                    raise ValueError(
                        "runner already has active "
                        "pitcher responsibility"
                    )

                self._active[runner_id] = (
                    CanonicalRunnerResponsibility(
                        runner_id=runner_id,
                        responsible_pitcher_id=(
                            event.pitcher_id
                        ),
                        reached_on_event_sequence=(
                            event.sequence
                        ),
                        reached_on_event_type=(
                            event.event_type
                        ),
                    )
                )

            if movement.is_out:
                self._active.pop(
                    runner_id,
                    None,
                )
                continue

            if movement.scored:
                responsibility = self._active.pop(
                    runner_id,
                    None,
                )

                if responsibility is None:
                    raise ValueError(
                        "scoring runner has no active "
                        "pitcher responsibility"
                    )

                self._scored.append(
                    CanonicalScoredRunResponsibility(
                        runner_id=runner_id,
                        responsible_pitcher_id=(
                            responsibility
                            .responsible_pitcher_id
                        ),
                        pitcher_on_mound_id=(
                            event.pitcher_id
                        ),
                        scoring_event_sequence=(
                            event.sequence
                        ),
                        scoring_event_type=(
                            event.event_type
                        ),
                    )
                )

        return tuple(
            self._scored[scored_before:]
        )

    def responsibility_for_runner(
        self,
        runner_id: str,
    ) -> CanonicalRunnerResponsibility | None:
        return self._active.get(runner_id)

    def active_responsibilities(
        self,
    ) -> Tuple[CanonicalRunnerResponsibility, ...]:
        return tuple(
            sorted(
                self._active.values(),
                key=lambda value: value.runner_id,
            )
        )

    def scored_run_responsibilities(
        self,
    ) -> Tuple[
        CanonicalScoredRunResponsibility,
        ...,
    ]:
        return tuple(self._scored)
