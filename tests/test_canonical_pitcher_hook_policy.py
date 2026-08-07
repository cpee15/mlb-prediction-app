from dataclasses import replace

import pytest

from mlb_app.simulation.game import (
    CANONICAL_STARTER_HOOK_POLICY_VERSION,
    CanonicalPitcherLifecycleState,
    CanonicalPitcherRole,
    CanonicalPitchingDecisionAction,
    CanonicalPitchingDecisionContext,
    CanonicalStarterHookPolicy,
    build_baseline_starter_hook_policy,
)


def lifecycle(**changes):
    value = CanonicalPitcherLifecycleState(
        team_side="home",
        pitcher_id="home_starter",
        role=CanonicalPitcherRole.STARTER,
        entered_inning=1,
        entered_half="top",
    )

    return replace(value, **changes)


def context(
    *,
    pitcher=None,
    inning=5,
    outs=0,
    batting_score=2,
    fielding_score=2,
    runners=0,
    relievers=("reliever_1", "reliever_2"),
):
    return CanonicalPitchingDecisionContext(
        lifecycle=pitcher or lifecycle(),
        inning=inning,
        half="top",
        outs=outs,
        batting_team_score=batting_score,
        fielding_team_score=fielding_score,
        runners_on_base=runners,
        upcoming_batter_id="away_batter_0",
        available_reliever_ids=relievers,
    )


def test_default_policy_has_stable_version():
    policy = build_baseline_starter_hook_policy()

    assert policy.schema_version == (
        CANONICAL_STARTER_HOOK_POLICY_VERSION
    )


def test_policy_thresholds_must_be_ordered():
    with pytest.raises(
        ValueError,
        match="must be ordered",
    ):
        CanonicalStarterHookPolicy(
            minimum_batters_faced=24,
            target_batters_faced=18,
        )


def test_optional_maximum_must_not_precede_target():
    with pytest.raises(
        ValueError,
        match="must be ordered",
    ):
        CanonicalStarterHookPolicy(
            target_batters_faced=24,
            maximum_batters_faced=20,
        )


def test_holds_before_minimum_batters():
    decision = (
        build_baseline_starter_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=2,
                    runs_scored_during_stint=8,
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


def test_poor_outing_can_exit_before_old_eighteen_batter_floor():
    decision = (
        build_baseline_starter_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=6,
                    runs_scored_during_stint=5,
                )
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == (
        "runs_threshold_reached"
    )


def test_hits_can_trigger_early_starter_exit():
    policy = CanonicalStarterHookPolicy(
        maximum_hits_allowed=6,
    )

    decision = policy.decide(
        context(
            pitcher=lifecycle(
                batters_faced=12,
                hits_allowed=6,
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == (
        "hits_threshold_reached"
    )


def test_combined_traffic_can_trigger_starter_exit():
    policy = CanonicalStarterHookPolicy(
        maximum_hits_allowed=20,
        maximum_walks_allowed=20,
        maximum_traffic_allowed=8,
    )

    decision = policy.decide(
        context(
            pitcher=lifecycle(
                batters_faced=12,
                hits_allowed=5,
                walks_allowed=2,
                hit_batters=1,
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == (
        "traffic_threshold_reached"
    )


def test_efficient_starter_can_continue_before_workload_target():
    decision = (
        build_baseline_starter_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=20,
                    hits_allowed=3,
                    walks_allowed=1,
                    runs_scored_during_stint=1,
                ),
                inning=5,
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.HOLD
    )
    assert decision.reason == (
        "starter_within_limits"
    )

def test_holds_when_no_reliever_is_available():
    decision = (
        build_baseline_starter_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=27,
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


def test_optional_maximum_batters_is_enforced_when_configured():
    policy = CanonicalStarterHookPolicy(
        maximum_batters_faced=27,
    )

    decision = policy.decide(
        context(
            pitcher=lifecycle(
                batters_faced=27,
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == (
        "maximum_batters_reached"
    )


def test_baseline_starter_can_exceed_twenty_seven_batters():
    decision = (
        build_baseline_starter_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=28,
                    hits_allowed=4,
                    walks_allowed=2,
                    runs_scored_during_stint=2,
                ),
                batting_score=0,
                fielding_score=5,
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.HOLD
    )
    assert decision.reason == (
        "starter_within_limits"
    )


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        (
            {
                "batters_faced": 18,
                "runs_scored_during_stint": 5,
            },
            "runs_threshold_reached",
        ),
        (
            {
                "batters_faced": 18,
                "walks_allowed": 4,
            },
            "walks_threshold_reached",
        ),
        (
            {
                "batters_faced": 18,
                "home_runs_allowed": 3,
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
        build_baseline_starter_hook_policy()
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


def test_replaces_after_target_in_late_game():
    decision = (
        build_baseline_starter_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=24,
                ),
                inning=7,
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == (
        "late_game_target_reached"
    )


def test_replaces_third_time_through_in_high_leverage():
    decision = (
        build_baseline_starter_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=24,
                ),
                inning=6,
                outs=1,
                batting_score=3,
                fielding_score=4,
                runners=1,
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == (
        "third_time_high_leverage"
    )


def test_holds_third_time_through_in_low_leverage():
    decision = (
        build_baseline_starter_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=24,
                ),
                inning=6,
                outs=2,
                batting_score=1,
                fielding_score=7,
                runners=0,
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.HOLD
    )
    assert decision.reason == (
        "starter_within_limits"
    )


def test_non_starter_is_not_evaluated():
    decision = (
        build_baseline_starter_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    role=CanonicalPitcherRole.RELIEVER,
                    batters_faced=30,
                )
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.HOLD
    )
    assert decision.reason == (
        "non_starter_not_evaluated"
    )


def test_inactive_starter_is_rejected():
    with pytest.raises(
        ValueError,
        match="active pitcher",
    ):
        (
            build_baseline_starter_hook_policy()
            .decide(
                context(
                    pitcher=lifecycle(
                        active=False,
                    )
                )
            )
        )


def test_baseline_starter_policy_uses_dynamic_decision_floor():
    policy = build_baseline_starter_hook_policy()

    assert policy.minimum_batters_faced == 3
    assert policy.target_batters_faced == 24
    assert policy.maximum_batters_faced is None
    assert policy.maximum_hits_allowed == 9
    assert policy.maximum_traffic_allowed == 12


def test_baseline_opener_policy_has_short_workload():
    from mlb_app.simulation.game.pitcher_hook_policy import (
        build_baseline_opener_hook_policy,
    )

    policy = build_baseline_opener_hook_policy()

    assert policy.minimum_batters_faced == 3
    assert policy.target_batters_faced == 6
    assert policy.maximum_batters_faced == 9
    assert policy.maximum_runs_during_stint == 3
    assert policy.maximum_walks_allowed == 2
    assert policy.maximum_home_runs_allowed == 2
    assert policy.maximum_hits_allowed == 4
    assert policy.maximum_traffic_allowed == 5


def test_baseline_opener_uses_short_role_traffic_threshold():
    from mlb_app.simulation.game.pitcher_hook_policy import (
        build_baseline_opener_hook_policy,
    )

    decision = (
        build_baseline_opener_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=5,
                    hits_allowed=3,
                    walks_allowed=1,
                    hit_batters=1,
                ),
                inning=2,
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == (
        "traffic_threshold_reached"
    )


def test_baseline_opener_replaces_at_nine_batters():
    from mlb_app.simulation.game.pitcher_hook_policy import (
        build_baseline_opener_hook_policy,
    )

    decision = (
        build_baseline_opener_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=9,
                ),
                inning=3,
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.REPLACE
    )
    assert decision.reason == (
        "maximum_batters_reached"
    )


def test_baseline_opener_holds_before_minimum():
    from mlb_app.simulation.game.pitcher_hook_policy import (
        build_baseline_opener_hook_policy,
    )

    decision = (
        build_baseline_opener_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=2,
                    runs_scored_during_stint=5,
                ),
                inning=1,
            )
        )
    )

    assert decision.action is (
        CanonicalPitchingDecisionAction.HOLD
    )
    assert decision.reason == (
        "minimum_batters_not_reached"
    )
