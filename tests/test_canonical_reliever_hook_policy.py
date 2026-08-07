from dataclasses import replace

import pytest

from mlb_app.simulation.game import (
    CANONICAL_RELIEVER_HOOK_POLICY_VERSION,
    CanonicalPitcherLifecycleState,
    CanonicalPitcherRole,
    CanonicalPitchingDecisionAction,
    CanonicalPitchingDecisionContext,
    CanonicalRelieverHookPolicy,
    build_baseline_reliever_hook_policy,
)


def lifecycle(**changes):
    value = CanonicalPitcherLifecycleState(
        team_side="home",
        pitcher_id="home_reliever",
        role=CanonicalPitcherRole.RELIEVER,
        entered_inning=6,
        entered_half="top",
    )

    return replace(value, **changes)


def context(
    *,
    pitcher=None,
    relievers=("next_reliever",),
):
    return CanonicalPitchingDecisionContext(
        lifecycle=pitcher or lifecycle(),
        inning=7,
        half="top",
        outs=1,
        batting_team_score=3,
        fielding_team_score=4,
        runners_on_base=1,
        upcoming_batter_id="away_batter_0",
        available_reliever_ids=relievers,
    )


def test_default_policy_has_stable_version():
    policy = build_baseline_reliever_hook_policy()

    assert policy.schema_version == (
        CANONICAL_RELIEVER_HOOK_POLICY_VERSION
    )


def test_baseline_reliever_has_dynamic_performance_thresholds():
    policy = build_baseline_reliever_hook_policy()

    assert policy.maximum_hits_allowed == 4
    assert policy.maximum_traffic_allowed == 5


def test_thresholds_must_be_ordered():
    with pytest.raises(
        ValueError,
        match="must be ordered",
    ):
        CanonicalRelieverHookPolicy(
            minimum_batters_faced=6,
            target_batters_faced=3,
        )


def test_minimum_batters_must_be_at_least_three():
    with pytest.raises(
        ValueError,
        match="at least three",
    ):
        CanonicalRelieverHookPolicy(
            minimum_batters_faced=2,
            target_batters_faced=3,
            maximum_batters_faced=6,
        )


def test_holds_before_minimum_batters():
    decision = (
        build_baseline_reliever_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=2,
                    runs_scored_during_stint=5,
                )
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.HOLD
    )
    assert decision.reason == (
        "minimum_batters_not_reached"
    )


def test_holds_when_no_reliever_remains():
    decision = (
        build_baseline_reliever_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=9,
                ),
                relievers=(),
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.HOLD
    )
    assert decision.reason == (
        "no_available_reliever"
    )


def test_replaces_at_maximum_batters():
    decision = (
        build_baseline_reliever_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=9,
                )
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == (
        "maximum_batters_reached"
    )


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        (
            {
                "batters_faced": 3,
                "runs_scored_during_stint": 3,
            },
            "runs_threshold_reached",
        ),
        (
            {
                "batters_faced": 3,
                "walks_allowed": 2,
            },
            "walks_threshold_reached",
        ),
        (
            {
                "batters_faced": 3,
                "home_runs_allowed": 2,
            },
            "home_run_threshold_reached",
        ),
    ],
)
def test_replaces_at_performance_thresholds(
    changes,
    reason,
):
    decision = (
        build_baseline_reliever_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(**changes)
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == reason


def test_hits_can_trigger_early_reliever_exit():
    decision = (
        build_baseline_reliever_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=4,
                    hits_allowed=4,
                )
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == (
        "hits_threshold_reached"
    )


def test_combined_traffic_can_trigger_reliever_exit():
    policy = CanonicalRelieverHookPolicy(
        maximum_hits_allowed=10,
        maximum_traffic_allowed=4,
    )

    decision = policy.decide(
        context(
            pitcher=lifecycle(
                batters_faced=4,
                hits_allowed=3,
                walks_allowed=1,
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == (
        "traffic_threshold_reached"
    )


def test_replaces_at_target_workload():
    decision = (
        build_baseline_reliever_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=6,
                )
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == (
        "target_workload_reached"
    )


def test_starter_is_not_evaluated():
    decision = (
        build_baseline_reliever_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    role=CanonicalPitcherRole.STARTER,
                    batters_faced=20,
                )
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.HOLD
    )
    assert decision.reason == (
        "non_reliever_not_evaluated"
    )


def test_inactive_reliever_is_rejected():
    with pytest.raises(
        ValueError,
        match="active pitcher",
    ):
        (
            build_baseline_reliever_hook_policy()
            .decide(
                context(
                    pitcher=lifecycle(
                        active=False,
                    )
                )
            )
        )
