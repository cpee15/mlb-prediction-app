"""Deterministic baseline canonical starter-hook policy."""

from __future__ import annotations

from dataclasses import dataclass

from .pitcher_lifecycle import (
    CanonicalPitcherRole,
    CanonicalPitchingDecision,
    CanonicalPitchingDecisionAction,
    CanonicalPitchingDecisionContext,
)


CANONICAL_STARTER_HOOK_POLICY_VERSION = (
    "canonical_starter_hook_policy_v1"
)


@dataclass(frozen=True)
class CanonicalStarterHookPolicy:
    """
    Baseline deterministic starter-removal policy.

    This policy is intentionally transparent and parameter-driven.
    It does not choose which reliever enters. A later bullpen selector
    consumes the replace decision and available reliever pool.
    """

    minimum_batters_faced: int = 18
    target_batters_faced: int = 24
    maximum_batters_faced: int = 27
    maximum_runs_during_stint: int = 5
    maximum_walks_allowed: int = 4
    maximum_home_runs_allowed: int = 3
    late_inning_threshold: int = 7
    schema_version: str = (
        CANONICAL_STARTER_HOOK_POLICY_VERSION
    )

    def __post_init__(self) -> None:
        thresholds = (
            self.minimum_batters_faced,
            self.target_batters_faced,
            self.maximum_batters_faced,
            self.maximum_runs_during_stint,
            self.maximum_walks_allowed,
            self.maximum_home_runs_allowed,
            self.late_inning_threshold,
        )

        if any(value < 0 for value in thresholds):
            raise ValueError(
                "starter hook thresholds cannot be negative"
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
                "starter batter thresholds must be ordered"
            )

        if self.late_inning_threshold < 1:
            raise ValueError(
                "late_inning_threshold must be positive"
            )

        if self.schema_version != (
            CANONICAL_STARTER_HOOK_POLICY_VERSION
        ):
            raise ValueError(
                "unsupported starter hook policy schema"
            )

    def decide(
        self,
        context: CanonicalPitchingDecisionContext,
    ) -> CanonicalPitchingDecision:
        """Return a deterministic hold-or-replace decision."""

        lifecycle = context.lifecycle

        if lifecycle.role is not CanonicalPitcherRole.STARTER:
            return self._hold(
                lifecycle.pitcher_id,
                "non_starter_not_evaluated",
            )

        if not lifecycle.active:
            raise ValueError(
                "starter hook policy requires an active pitcher"
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
            and context.inning
            >= self.late_inning_threshold
        ):
            return self._replace(
                lifecycle.pitcher_id,
                "late_game_target_reached",
            )

        if (
            lifecycle.current_lineup_pass >= 3
            and lifecycle.batters_faced
            >= self.target_batters_faced
            and self._is_high_leverage(context)
        ):
            return self._replace(
                lifecycle.pitcher_id,
                "third_time_high_leverage",
            )

        return self._hold(
            lifecycle.pitcher_id,
            "starter_within_limits",
        )

    @staticmethod
    def _is_high_leverage(
        context: CanonicalPitchingDecisionContext,
    ) -> bool:
        score_margin = abs(
            context.fielding_team_score
            - context.batting_team_score
        )

        return (
            score_margin <= 2
            and (
                context.runners_on_base >= 1
                or context.outs <= 1
            )
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
        """
        Signal replacement without selecting a bullpen pitcher.

        The first available reliever is carried only as a temporary
        contract-compatible placeholder until the dedicated bullpen
        selector slice owns replacement identity.
        """

        return CanonicalPitchingDecision(
            action=CanonicalPitchingDecisionAction.REPLACE,
            current_pitcher_id=pitcher_id,
            replacement_pitcher_id="pending_bullpen_selection",
            reason=reason,
        )


def build_baseline_starter_hook_policy(
) -> CanonicalStarterHookPolicy:
    """Return the default deterministic starter-hook policy."""

    return CanonicalStarterHookPolicy()
