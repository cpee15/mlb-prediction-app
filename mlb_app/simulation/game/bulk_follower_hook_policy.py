from dataclasses import dataclass

from .pitcher_lifecycle import (
    CanonicalPitchingDecisionContext,
)
from .reliever_hook_policy import (
    CanonicalRelieverHookPolicy,
)


CANONICAL_BULK_FOLLOWER_HOOK_POLICY_VERSION = (
    "canonical_bulk_follower_hook_policy_v1"
)


@dataclass(frozen=True)
class CanonicalBulkFollowerHookPolicy(
    CanonicalRelieverHookPolicy
):
    """
    Dynamic canonical exit policy for an opener's bulk follower.

    Workload thresholds are decision boundaries, not projected
    innings. Simulated performance can trigger an earlier exit,
    while an efficient appearance can continue deeper.
    """

    minimum_batters_faced: int = 3
    target_batters_faced: int = 24
    maximum_batters_faced: int = 30
    maximum_runs_during_stint: int = 4
    maximum_walks_allowed: int = 3
    maximum_home_runs_allowed: int = 2
    maximum_hits_allowed: int = 8
    maximum_traffic_allowed: int = 10
    schema_version: str = (
        CANONICAL_BULK_FOLLOWER_HOOK_POLICY_VERSION
    )

    def __post_init__(self) -> None:
        thresholds = (
            self.minimum_batters_faced,
            self.target_batters_faced,
            self.maximum_batters_faced,
            self.maximum_runs_during_stint,
            self.maximum_walks_allowed,
            self.maximum_home_runs_allowed,
            self.maximum_hits_allowed,
            self.maximum_traffic_allowed,
        )

        if any(value < 0 for value in thresholds):
            raise ValueError(
                "bulk follower hook thresholds "
                "cannot be negative"
            )

        if self.minimum_batters_faced < 3:
            raise ValueError(
                "minimum_batters_faced must be "
                "at least three"
            )

        if not (
            self.minimum_batters_faced
            <= self.target_batters_faced
            <= self.maximum_batters_faced
        ):
            raise ValueError(
                "bulk follower batter thresholds "
                "must be ordered"
            )

        if self.schema_version != (
            CANONICAL_BULK_FOLLOWER_HOOK_POLICY_VERSION
        ):
            raise ValueError(
                "unsupported bulk follower hook "
                "policy version"
            )

    def decide(
        self,
        context: CanonicalPitchingDecisionContext,
    ):
        lifecycle = context.lifecycle

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
            lifecycle.hits_allowed
            >= self.maximum_hits_allowed
        ):
            return self._replace(
                lifecycle.pitcher_id,
                "hits_threshold_reached",
            )

        traffic = (
            lifecycle.hits_allowed
            + lifecycle.walks_allowed
            + lifecycle.hit_batters
        )

        if traffic >= self.maximum_traffic_allowed:
            return self._replace(
                lifecycle.pitcher_id,
                "traffic_threshold_reached",
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
            "bulk_follower_continues",
        )


def build_baseline_bulk_follower_hook_policy(
) -> CanonicalBulkFollowerHookPolicy:
    return CanonicalBulkFollowerHookPolicy()
