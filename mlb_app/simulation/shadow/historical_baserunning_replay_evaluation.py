"""Evaluate historical baserunning shadow replays."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Dict, Tuple

from .baserunning_calibration_artifact import (
    CanonicalBaserunningCalibrationArtifact,
    execute_baserunning_calibration_artifact,
)
from .baserunning_calibration_gate import (
    CanonicalBaserunningCalibrationPolicy,
)
from .baserunning_calibration_payload import (
    CanonicalHistoricalBaserunningGame,
    assemble_historical_baserunning_calibration_payload,
)
from .historical_baserunning_game_materialization import (
    materialize_play_by_play_baserunning_game_records,
)
from .historical_baserunning_shadow_replay_execution import (
    CanonicalHistoricalBaserunningShadowReplayWindow,
)
from .historical_baserunning_shadow_validation import (
    CanonicalHistoricalBaserunningExecutionGame,
    collect_historical_baserunning_shadow_validations,
)
from .mlb_play_by_play_baserunning_source import (
    CanonicalMlbPlayByPlayBaserunningSnapshot,
)


CANONICAL_HISTORICAL_BASERUNNING_REPLAY_EVALUATION_VERSION = (
    "canonical_historical_baserunning_replay_evaluation_v1"
)
HISTORICAL_BASERUNNING_REPLAY_REVIEW_POLICY_VERSION = (
    "historical_baserunning_replay_review_policy_v1"
)


def build_historical_baserunning_replay_review_policy(
) -> CanonicalBaserunningCalibrationPolicy:
    """
    Build the fixed first-pass historical review policy.

    Passing this policy qualifies evidence for additional review only.
    It does not permit simulator mutation or production activation.
    """

    return CanonicalBaserunningCalibrationPolicy(
        minimum_game_count=150,
        maximum_stolen_base_error_per_game=0.25,
        maximum_caught_stealing_error_per_game=0.10,
        maximum_attempt_error_per_game=0.30,
        maximum_success_rate_absolute_error=0.10,
        policy_version=(
            HISTORICAL_BASERUNNING_REPLAY_REVIEW_POLICY_VERSION
        ),
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _validate_digest(
    value: str,
    field_name: str,
) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(
            character not in "0123456789abcdef"
            for character in value
        )
    ):
        raise ValueError(
            f"{field_name} must be a SHA256 digest"
        )


@dataclass(frozen=True)
class CanonicalHistoricalBaserunningReplayEvaluation:
    replay_window_digest: str
    observed_snapshot_digest: str
    games: Tuple[
        CanonicalHistoricalBaserunningGame,
        ...,
    ]
    artifact: CanonicalBaserunningCalibrationArtifact
    digest: str
    evaluation_version: str = (
        CANONICAL_HISTORICAL_BASERUNNING_REPLAY_EVALUATION_VERSION
    )

    def __post_init__(self) -> None:
        for field_name, value in (
            (
                "replay_window_digest",
                self.replay_window_digest,
            ),
            (
                "observed_snapshot_digest",
                self.observed_snapshot_digest,
            ),
            ("digest", self.digest),
        ):
            _validate_digest(value, field_name)

        if not self.games:
            raise ValueError(
                "games must contain evaluated records"
            )

        identities = tuple(
            value.game_pk
            for value in self.games
        )
        if len(identities) != len(set(identities)):
            raise ValueError(
                "evaluated game identifiers must be unique"
            )

        if not isinstance(
            self.artifact,
            CanonicalBaserunningCalibrationArtifact,
        ):
            raise TypeError(
                "artifact must be "
                "CanonicalBaserunningCalibrationArtifact"
            )

        if not self.artifact.ready:
            raise ValueError(
                "evaluation artifact must be ready"
            )

        if self.evaluation_version != (
            CANONICAL_HISTORICAL_BASERUNNING_REPLAY_EVALUATION_VERSION
        ):
            raise ValueError(
                "unsupported historical baserunning "
                "replay evaluation version"
            )

    @property
    def ready(self) -> bool:
        return self.artifact.ready

    @property
    def game_count(self) -> int:
        return len(self.games)

    @property
    def calibration_gate_passed(self) -> bool:
        return self.artifact.calibration_gate_passed

    @property
    def observed_stolen_bases(self) -> int:
        return sum(
            value.observed_stolen_bases
            for value in self.games
        )

    @property
    def observed_caught_stealing(self) -> int:
        return sum(
            value.observed_caught_stealing
            for value in self.games
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        report = self.artifact.report

        return {
            "schema_version": self.evaluation_version,
            "ready": self.ready,
            "game_count": self.game_count,
            "replay_window_digest": (
                self.replay_window_digest
            ),
            "observed_snapshot_digest": (
                self.observed_snapshot_digest
            ),
            "evaluation_digest": self.digest,
            "observed_stolen_bases": (
                self.observed_stolen_bases
            ),
            "observed_caught_stealing": (
                self.observed_caught_stealing
            ),
            "observed_attempts": (
                self.observed_stolen_bases
                + self.observed_caught_stealing
            ),
            "calibration_gate_passed": (
                self.calibration_gate_passed
            ),
            "report": (
                report.to_diagnostics()
                if report is not None
                else None
            ),
            "eligible_for_activation_review": (
                self.calibration_gate_passed
            ),
            "activation_permitted": False,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def evaluate_historical_baserunning_shadow_replays(
    *,
    replays: (
        CanonicalHistoricalBaserunningShadowReplayWindow
    ),
    observed: CanonicalMlbPlayByPlayBaserunningSnapshot,
    policy: CanonicalBaserunningCalibrationPolicy,
) -> CanonicalHistoricalBaserunningReplayEvaluation:
    """
    Compare one complete replay window to observed MLB outcomes.

    Existing canonical validation, materialization, report, and gate
    contracts remain authoritative. This function only assembles them.
    It performs no fetch, persistence, optimization, or activation.
    """

    if not isinstance(
        replays,
        CanonicalHistoricalBaserunningShadowReplayWindow,
    ):
        raise TypeError(
            "replays must be "
            "CanonicalHistoricalBaserunningShadowReplayWindow"
        )

    if not replays.ready:
        raise ValueError(
            "historical replay window must be ready"
        )

    if not isinstance(
        observed,
        CanonicalMlbPlayByPlayBaserunningSnapshot,
    ):
        raise TypeError(
            "observed must be "
            "CanonicalMlbPlayByPlayBaserunningSnapshot"
        )

    if not isinstance(
        policy,
        CanonicalBaserunningCalibrationPolicy,
    ):
        raise TypeError(
            "policy must be "
            "CanonicalBaserunningCalibrationPolicy"
        )

    replay_games = {
        value.game_pk: value
        for value in replays.games
    }
    observed_games = {
        value.game_pk: value
        for value in observed.games
    }

    if set(replay_games) != set(observed_games):
        raise ValueError(
            "replay games must exactly match "
            "observed games"
        )

    execution_games = tuple(
        CanonicalHistoricalBaserunningExecutionGame(
            game_pk=value.game_pk,
            game_date=value.game_date,
            execution=value.execution,
        )
        for value in replays.games
    )

    validations = (
        collect_historical_baserunning_shadow_validations(
            execution_games=execution_games,
            observed=observed,
        )
    )

    games = materialize_play_by_play_baserunning_game_records(
        shadow_games=validations,
        observed=observed,
    )

    payload = (
        assemble_historical_baserunning_calibration_payload(
            window_start=observed.window_start,
            window_end=observed.window_end,
            games=games,
            policy=policy,
        )
    )
    artifact = execute_baserunning_calibration_artifact(
        payload
    )

    if not artifact.ready:
        raise ValueError(
            "historical replay calibration artifact "
            "is unavailable: "
            + (
                artifact.error_message
                or artifact.status
            )
        )

    evaluation_digest = _sha256(
        {
            "schema_version": (
                CANONICAL_HISTORICAL_BASERUNNING_REPLAY_EVALUATION_VERSION
            ),
            "replay_window_digest": replays.digest,
            "observed_snapshot_digest": observed.digest,
            "policy_version": policy.policy_version,
            "games": [
                {
                    "game_pk": value.game_pk,
                    "game_date": value.game_date,
                    "observed_stolen_bases": (
                        value.observed_stolen_bases
                    ),
                    "observed_caught_stealing": (
                        value.observed_caught_stealing
                    ),
                    "validation": (
                        value.validation.to_diagnostics()
                    ),
                }
                for value in games
            ],
            "artifact": artifact.to_diagnostics(),
        }
    )

    return CanonicalHistoricalBaserunningReplayEvaluation(
        replay_window_digest=replays.digest,
        observed_snapshot_digest=observed.digest,
        games=games,
        artifact=artifact,
        digest=evaluation_digest,
    )
