"""Deterministic canonical reliever workload policy."""

from __future__ import annotations

from dataclasses import dataclass

from .pitcher_lifecycle import (
    CanonicalPitcherRole,
    CanonicalPitchingDecision,
    CanonicalPitchingDecisionAction,
    CanonicalPitchingDecisionContext,
)


CANONICAL_RELIEVER_HOOK_POLICY_VERSION = (
    "canonical_reliever_hook_policy_v1"
)


@dataclass(frozen=True)
class CanonicalRelieverHookPolicy:
    """
    Baseline deterministic reliever-removal policy.

    This policy decides whether an active reliever should remain in the
    game. Replacement identity remains owned by the bullpen selector.
    """

    minimum_batters_faced: int = 3
    target_batters_faced: int = 6
    maximum_batters_faced: int = 9
    maximum_runs_during_stint: int = 3
    maximum_walks_allowed: int = 2
    maximum_home_runs_allowed: int = 2
    schema_version: str = (
        CANONICAL_RELIEVER_HOOK_POLICY_VERSION
    )

    def __post_init__(self) -> None:
        thresholds = (
            self.minimum_batters_faced,
            self.target_batters_faced,
            self.maximum_batters_faced,
            self.maximum_runs_during_stint,
            self.maximum_walks_allowed,
            self.maximum_home_runs_allowed,
        )

        if any(value < 0 for value in thresholds):
            raise ValueError(
                "reliever hook thresholds cannot be negative"
            )

        if self.minimum_batters_faced < 3:
            raise ValueError(
                "minimum_batters_faced must be at least three"
            )

        if not (
            self.minimum_batters_faced
            <= self.target_batters_faced
            <= self.maximum_batters_faced
        ):
            raise ValueError(
                "reliever batter thresholds must be ordered"
            )

        if self.schema_version != (
            CANONICAL_RELIEVER_HOOK_POLICY_VERSION
        ):
            raise ValueError(
                "unsupported reliever hook policy schema"
            )

    def decide(
        self,
        context: CanonicalPitchingDecisionContext,
    ) -> CanonicalPitchingDecision:
        lifecycle = context.lifecycle

        if lifecycle.role is not (
            CanonicalPitcherRole.RELIEVER
        ):
            return self._hold(
                lifecycle.pitcher_id,
                "non_reliever_not_evaluated",
            )

        if not lifecycle.active:
            raise ValueError(
                "reliever hook policy requires "
                "an active pitcher"
            )

        if not context.available_reliever_ids:
            return self._hold(
                lifecycle.pitcher_id,
                "no_available_reliever",
            )

        if (
            lifecycle.batters_faced
            < self.minimum_batters_faced
        ):
            return self._hold(
                lifecycle.pitcher_id,
                "minimum_batters_not_reached",
            )

        if (
            lifecycle.batters_faced
            >= self.maximum_batters_faced
        ):
            return self._replace(
                lifecycle.pitcher_id,
                "maximum_batters_reached",
            )

        if (
            lifecycle.runs_scored_during_stint
            >= self.maximum_runs_during_stint
        ):
            return self._replace(
                lifecycle.pitcher_id,
                "runs_threshold_reached",
            )

        if (
            lifecycle.walks_allowed
            >= self.maximum_walks_allowed
        ):
            return self._replace(
                lifecycle.pitcher_id,
                "walks_threshold_reached",
            )

        if (
            lifecycle.home_runs_allowed
            >= self.maximum_home_runs_allowed
        ):
            return self._replace(
                lifecycle.pitcher_id,
                "home_run_threshold_reached",
            )

        if (
            lifecycle.batters_faced
            >= self.target_batters_faced
        ):
            return self._replace(
                lifecycle.pitcher_id,
                "target_workload_reached",
            )

        return self._hold(
            lifecycle.pitcher_id,
            "reliever_within_limits",
        )

    @staticmethod
    def _hold(
        pitcher_id: str,
        reason: str,
    ) -> CanonicalPitchingDecision:
        return CanonicalPitchingDecision(
            action=CanonicalPitchingDecisionAction.HOLD,
            current_pitcher_id=pitcher_id,
            reason=reason,
        )

    @staticmethod
    def _replace(
        pitcher_id: str,
        reason: str,
    ) -> CanonicalPitchingDecision:
        return CanonicalPitchingDecision(
            action=(
                CanonicalPitchingDecisionAction.REPLACE
            ),
            current_pitcher_id=pitcher_id,
            replacement_pitcher_id=(
                "pending_bullpen_selection"
            ),
            reason=reason,
        )


def build_baseline_reliever_hook_policy(
) -> CanonicalRelieverHookPolicy:
    return CanonicalRelieverHookPolicy()
