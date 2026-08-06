from dataclasses import replace

from mlb_app.simulation.game.bulk_follower_hook_policy import (
    CanonicalBulkFollowerHookPolicy,
    build_baseline_bulk_follower_hook_policy,
)
from mlb_app.simulation.game.pitcher_lifecycle import (
    CanonicalPitcherLifecycleState,
    CanonicalPitcherRole,
    CanonicalPitchingDecisionAction,
    CanonicalPitchingDecisionContext,
)


def context(**updates):
    lifecycle = CanonicalPitcherLifecycleState(
        team_side="away",
        pitcher_id="bulk",
        role=CanonicalPitcherRole.RELIEVER,
        entered_inning=2,
        entered_half="bottom",
        batters_faced=12,
        outs_recorded=9,
    )

    lifecycle = replace(
        lifecycle,
        **updates,
    )

    return CanonicalPitchingDecisionContext(
        lifecycle=lifecycle,
        inning=5,
        half="bottom",
        outs=0,
        batting_team_score=2,
        fielding_team_score=2,
        runners_on_base=0,
        upcoming_batter_id="batter_1",
        available_reliever_ids=(
            "reliever_1",
            "reliever_2",
        ),
    )


def test_clean_bulk_follower_can_continue():
    policy = (
        build_baseline_bulk_follower_hook_policy()
    )

    decision = policy.decide(context())

    assert decision.action is (
        CanonicalPitchingDecisionAction.HOLD
    )
    assert decision.reason == (
        "bulk_follower_continues"
    )


def test_poor_run_outing_exits_early():
    policy = (
        build_baseline_bulk_follower_hook_policy()
    )

    decision = policy.decide(
        context(
            batters_faced=10,
            outs_recorded=6,
            runs_scored_during_stint=4,
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == (
        "runs_threshold_reached"
    )


def test_walks_can_trigger_early_exit():
    policy = (
        build_baseline_bulk_follower_hook_policy()
    )

    decision = policy.decide(
        context(
            batters_faced=9,
            outs_recorded=5,
            walks_allowed=3,
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == (
        "walks_threshold_reached"
    )


def test_hits_can_trigger_early_exit():
    policy = CanonicalBulkFollowerHookPolicy(
        maximum_hits_allowed=5,
    )

    decision = policy.decide(
        context(
            hits_allowed=5,
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == (
        "hits_threshold_reached"
    )


def test_combined_traffic_can_trigger_exit():
    policy = CanonicalBulkFollowerHookPolicy(
        maximum_hits_allowed=10,
        maximum_walks_allowed=10,
        maximum_traffic_allowed=6,
    )

    decision = policy.decide(
        context(
            hits_allowed=4,
            walks_allowed=1,
            hit_batters=1,
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == (
        "traffic_threshold_reached"
    )


def test_efficient_outing_reaches_workload_target():
    policy = (
        build_baseline_bulk_follower_hook_policy()
    )

    before = policy.decide(
        context(
            batters_faced=23,
            outs_recorded=18,
        )
    )
    at_target = policy.decide(
        context(
            batters_faced=24,
            outs_recorded=19,
        )
    )

    assert before.action is (
        CanonicalPitchingDecisionAction.HOLD
    )
    assert at_target.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert at_target.reason == (
        "target_workload_reached"
    )


def test_three_batter_floor_still_applies():
    policy = (
        build_baseline_bulk_follower_hook_policy()
    )

    decision = policy.decide(
        context(
            batters_faced=2,
            outs_recorded=0,
            runs_scored_during_stint=4,
            hits_allowed=2,
            walks_allowed=2,
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.HOLD
    )
    assert decision.reason == (
        "minimum_batters_not_reached"
    )


def test_no_available_reliever_fails_open():
    policy = (
        build_baseline_bulk_follower_hook_policy()
    )
    base = context(
        runs_scored_during_stint=6,
    )

    decision = policy.decide(
        replace(
            base,
            available_reliever_ids=(),
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.HOLD
    )
    assert decision.reason == (
        "no_available_reliever"
    )
