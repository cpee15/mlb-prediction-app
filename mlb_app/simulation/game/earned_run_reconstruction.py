"""Conservative canonical earned-run reconstruction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

from mlb_app.simulation.events import PlayEvent

from .pitcher_responsibility import (
    CanonicalRunnerResponsibility,
    CanonicalScoredRunResponsibility,
)


CANONICAL_EARNED_RUN_RECONSTRUCTION_VERSION = (
    "canonical_earned_run_reconstruction_v1"
)


@dataclass(frozen=True)
class CanonicalRunClassification:
    """Earned/unearned classification for one scored runner."""

    runner_id: str
    responsible_pitcher_id: str
    pitcher_on_mound_id: str
    earned: bool
    classification_reason: str
    reached_on_event_sequence: int
    scoring_event_sequence: int
    schema_version: str = (
        CANONICAL_EARNED_RUN_RECONSTRUCTION_VERSION
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

        if not self.classification_reason:
            raise ValueError(
                "classification_reason is required"
            )

        if self.reached_on_event_sequence < 0:
            raise ValueError(
                "reached_on_event_sequence cannot be negative"
            )

        if self.scoring_event_sequence < 0:
            raise ValueError(
                "scoring_event_sequence cannot be negative"
            )

        if self.schema_version != (
            CANONICAL_EARNED_RUN_RECONSTRUCTION_VERSION
        ):
            raise ValueError(
                "unsupported earned-run schema"
            )


@dataclass(frozen=True)
class CanonicalPitcherRunLine:
    """Reconstructed runs charged to one responsible pitcher."""

    pitcher_id: str
    runs_allowed: int
    earned_runs: int
    unearned_runs: int
    earned_run_status: str = "reconstructed"
    schema_version: str = (
        CANONICAL_EARNED_RUN_RECONSTRUCTION_VERSION
    )

    def __post_init__(self) -> None:
        if not self.pitcher_id:
            raise ValueError("pitcher_id is required")

        counters = (
            self.runs_allowed,
            self.earned_runs,
            self.unearned_runs,
        )

        if any(value < 0 for value in counters):
            raise ValueError(
                "run counters cannot be negative"
            )

        if self.runs_allowed != (
            self.earned_runs + self.unearned_runs
        ):
            raise ValueError(
                "runs_allowed must equal earned plus unearned"
            )

        if self.earned_run_status != "reconstructed":
            raise ValueError(
                "earned_run_status must be reconstructed"
            )

        if self.schema_version != (
            CANONICAL_EARNED_RUN_RECONSTRUCTION_VERSION
        ):
            raise ValueError(
                "unsupported earned-run schema"
            )


class CanonicalEarnedRunReconstructor:
    """
    Conservative earned-run classifier.

    Version one marks a run unearned only when the runner originally
    reached base on a play carrying explicit fielding-error attribution.
    It does not yet reconstruct hypothetical innings after errors.
    """

    def __init__(self) -> None:
        self._reach_events: Dict[str, PlayEvent] = {}
        self._automatic_runners: Dict[
            str,
            CanonicalRunnerResponsibility,
        ] = {}
        self._classifications: list[
            CanonicalRunClassification
        ] = []

    def record_automatic_runner(
        self,
        responsibility: CanonicalRunnerResponsibility,
    ) -> None:
        if not isinstance(
            responsibility,
            CanonicalRunnerResponsibility,
        ):
            raise TypeError(
                "responsibility must be a "
                "CanonicalRunnerResponsibility"
            )

        runner_id = responsibility.runner_id

        if (
            runner_id in self._reach_events
            or runner_id in self._automatic_runners
        ):
            raise ValueError(
                "runner reach is already recorded"
            )

        self._automatic_runners[
            runner_id
        ] = responsibility

    def record_runner_reach(
        self,
        *,
        responsibility: CanonicalRunnerResponsibility,
        event: PlayEvent,
    ) -> None:
        if not isinstance(
            responsibility,
            CanonicalRunnerResponsibility,
        ):
            raise TypeError(
                "responsibility must be a "
                "CanonicalRunnerResponsibility"
            )

        if not isinstance(event, PlayEvent):
            raise TypeError(
                "event must be a PlayEvent"
            )

        if responsibility.runner_id in self._reach_events:
            raise ValueError(
                "runner reach event is already recorded"
            )

        if responsibility.reached_on_event_sequence != (
            event.sequence
        ):
            raise ValueError(
                "responsibility reach sequence "
                "must match event sequence"
            )

        self._reach_events[
            responsibility.runner_id
        ] = event

    def classify_scored_run(
        self,
        responsibility: CanonicalScoredRunResponsibility,
    ) -> CanonicalRunClassification:
        if not isinstance(
            responsibility,
            CanonicalScoredRunResponsibility,
        ):
            raise TypeError(
                "responsibility must be a "
                "CanonicalScoredRunResponsibility"
            )

        reach_event = self._reach_events.pop(
            responsibility.runner_id,
            None,
        )
        automatic_runner = (
            self._automatic_runners.pop(
                responsibility.runner_id,
                None,
            )
        )

        if (
            reach_event is None
            and automatic_runner is None
        ):
            raise ValueError(
                "scored runner has no recorded reach event"
            )

        reached_on_error = bool(
            reach_event is not None
            and reach_event.attribution.error_fielder_id
        )
        is_automatic_runner = (
            automatic_runner is not None
        )

        classification = CanonicalRunClassification(
            runner_id=responsibility.runner_id,
            responsible_pitcher_id=(
                responsibility.responsible_pitcher_id
            ),
            pitcher_on_mound_id=(
                responsibility.pitcher_on_mound_id
            ),
            earned=(
                not reached_on_error
                and not is_automatic_runner
            ),
            classification_reason=(
                "automatic_runner"
                if is_automatic_runner
                else (
                    "reached_on_fielding_error"
                    if reached_on_error
                    else "no_explicit_error_on_reach"
                )
            ),
            reached_on_event_sequence=(
                automatic_runner
                .reached_on_event_sequence
                if automatic_runner is not None
                else reach_event.sequence
            ),
            scoring_event_sequence=(
                responsibility.scoring_event_sequence
            ),
        )

        self._classifications.append(
            classification
        )
        return classification

    def retire_runner(self, runner_id: str) -> None:
        self._reach_events.pop(
            runner_id,
            None,
        )
        self._automatic_runners.pop(
            runner_id,
            None,
        )

    def classifications(
        self,
    ) -> Tuple[CanonicalRunClassification, ...]:
        return tuple(self._classifications)

    def pitcher_run_lines(
        self,
    ) -> Tuple[CanonicalPitcherRunLine, ...]:
        totals: Dict[str, Dict[str, int]] = {}

        for classification in self._classifications:
            row = totals.setdefault(
                classification.responsible_pitcher_id,
                {
                    "runs_allowed": 0,
                    "earned_runs": 0,
                    "unearned_runs": 0,
                },
            )

            row["runs_allowed"] += 1

            if classification.earned:
                row["earned_runs"] += 1
            else:
                row["unearned_runs"] += 1

        return tuple(
            CanonicalPitcherRunLine(
                pitcher_id=pitcher_id,
                runs_allowed=row["runs_allowed"],
                earned_runs=row["earned_runs"],
                unearned_runs=row["unearned_runs"],
            )
            for pitcher_id, row in sorted(
                totals.items()
            )
        )
