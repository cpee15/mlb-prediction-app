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


def test_holds_before_minimum_batters():
    decision = (
        build_baseline_starter_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=17,
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


def test_replaces_at_maximum_batters():
    decision = (
        build_baseline_starter_hook_policy()
        .decide(
            context(
                pitcher=lifecycle(
                    batters_faced=27,
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


def test_baseline_opener_policy_has_short_workload():
    from mlb_app.simulation.game.pitcher_hook_policy import (
        build_baseline_opener_hook_policy,
    )

    policy = build_baseline_opener_hook_policy()

    assert policy.minimum_batters_faced == 3
    assert policy.target_batters_faced == 6
    assert policy.maximum_batters_faced == 9


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
