from dataclasses import replace

import pytest

from mlb_app.simulation.game import (
    CANONICAL_BULLPEN_SELECTOR_VERSION,
    CanonicalBullpenPitcher,
    CanonicalBullpenRole,
    CanonicalBullpenSelectionContext,
    CanonicalPitcherLifecycleState,
    CanonicalPitcherRole,
    CanonicalPitchingDecision,
    CanonicalPitchingDecisionAction,
    CanonicalPitchingDecisionContext,
    build_canonical_bullpen_selector,
)


def lifecycle():
    return CanonicalPitcherLifecycleState(
        team_side="home",
        pitcher_id="home_starter",
        role=CanonicalPitcherRole.STARTER,
        entered_inning=1,
        entered_half="top",
        batters_faced=24,
    )


def game_context(
    *,
    inning=6,
    outs=1,
    batting_score=2,
    fielding_score=3,
    runners=0,
    available=(
        "long",
        "middle",
        "setup",
        "closer",
    ),
):
    return CanonicalPitchingDecisionContext(
        lifecycle=lifecycle(),
        inning=inning,
        half="top",
        outs=outs,
        batting_team_score=batting_score,
        fielding_team_score=fielding_score,
        runners_on_base=runners,
        upcoming_batter_id="away_batter_0",
        available_reliever_ids=available,
    )


def decision():
    return CanonicalPitchingDecision(
        action=CanonicalPitchingDecisionAction.REPLACE,
        current_pitcher_id="home_starter",
        replacement_pitcher_id=(
            "pending_bullpen_selection"
        ),
        reason="starter_hook",
    )


def bullpen():
    return (
        CanonicalBullpenPitcher(
            pitcher_id="long",
            role=CanonicalBullpenRole.LONG_RELIEF,
            appearance_priority=0,
        ),
        CanonicalBullpenPitcher(
            pitcher_id="middle",
            role=CanonicalBullpenRole.MIDDLE_RELIEF,
            appearance_priority=0,
        ),
        CanonicalBullpenPitcher(
            pitcher_id="setup",
            role=CanonicalBullpenRole.SETUP,
            appearance_priority=0,
            minimum_inning=7,
            maximum_score_margin=3,
        ),
        CanonicalBullpenPitcher(
            pitcher_id="closer",
            role=CanonicalBullpenRole.CLOSER,
            appearance_priority=0,
            minimum_inning=9,
            maximum_score_margin=3,
        ),
    )


def context(
    *,
    game=None,
    options=None,
    used=(),
):
    return CanonicalBullpenSelectionContext(
        pitching_decision=decision(),
        game_context=game or game_context(),
        bullpen=options or bullpen(),
        previously_used_pitcher_ids=used,
    )


def test_selector_has_stable_version():
    selector = build_canonical_bullpen_selector()

    assert selector.version == (
        CANONICAL_BULLPEN_SELECTOR_VERSION
    )


def test_early_exit_prefers_long_reliever():
    selection = (
        build_canonical_bullpen_selector()
        .select(
            context(
                game=game_context(
                    inning=4,
                    batting_score=4,
                    fielding_score=1,
                )
            )
        )
    )

    assert selection.pitcher_id == "long"
    assert selection.role is (
        CanonicalBullpenRole.LONG_RELIEF
    )
    assert selection.reason == (
        "early_exit_long_relief"
    )


def test_middle_inning_prefers_middle_reliever():
    selection = (
        build_canonical_bullpen_selector()
        .select(context())
    )

    assert selection.pitcher_id == "middle"
    assert selection.role is (
        CanonicalBullpenRole.MIDDLE_RELIEF
    )


def test_late_high_leverage_prefers_setup():
    selection = (
        build_canonical_bullpen_selector()
        .select(
            context(
                game=game_context(
                    inning=8,
                    outs=1,
                    batting_score=3,
                    fielding_score=4,
                    runners=2,
                )
            )
        )
    )

    assert selection.pitcher_id == "setup"
    assert selection.role is (
        CanonicalBullpenRole.SETUP
    )
    assert selection.reason == (
        "late_high_leverage_setup"
    )


def test_save_situation_prefers_closer():
    selection = (
        build_canonical_bullpen_selector()
        .select(
            context(
                game=game_context(
                    inning=9,
                    outs=0,
                    batting_score=2,
                    fielding_score=4,
                    runners=0,
                )
            )
        )
    )

    assert selection.pitcher_id == "closer"
    assert selection.role is (
        CanonicalBullpenRole.CLOSER
    )
    assert selection.reason == (
        "save_situation_closer"
    )


def test_used_pitcher_is_excluded():
    selection = (
        build_canonical_bullpen_selector()
        .select(
            context(
                game=game_context(
                    inning=4,
                ),
                used=("long",),
            )
        )
    )

    assert selection.pitcher_id == "middle"
    assert "long" not in (
        selection.candidate_pitcher_ids
    )


def test_unavailable_pitcher_is_excluded():
    options = tuple(
        replace(
            pitcher,
            available=False,
        )
        if pitcher.pitcher_id == "middle"
        else pitcher
        for pitcher in bullpen()
    )

    selection = (
        build_canonical_bullpen_selector()
        .select(
            context(options=options)
        )
    )

    assert selection.pitcher_id == "long"


def test_game_available_pool_is_enforced():
    selection = (
        build_canonical_bullpen_selector()
        .select(
            context(
                game=game_context(
                    available=("setup",),
                    inning=8,
                    runners=1,
                )
            )
        )
    )

    assert selection.pitcher_id == "setup"
    assert selection.candidate_pitcher_ids == (
        "setup",
    )


def test_priority_breaks_same_role_tie():
    options = (
        CanonicalBullpenPitcher(
            pitcher_id="middle_b",
            role=CanonicalBullpenRole.MIDDLE_RELIEF,
            appearance_priority=2,
        ),
        CanonicalBullpenPitcher(
            pitcher_id="middle_a",
            role=CanonicalBullpenRole.MIDDLE_RELIEF,
            appearance_priority=1,
        ),
    )

    game = game_context(
        available=("middle_a", "middle_b"),
    )

    selection = (
        build_canonical_bullpen_selector()
        .select(
            context(
                game=game,
                options=options,
            )
        )
    )

    assert selection.pitcher_id == "middle_a"
    assert selection.candidate_pitcher_ids == (
        "middle_a",
        "middle_b",
    )


def test_identifier_breaks_exact_tie_deterministically():
    options = (
        CanonicalBullpenPitcher(
            pitcher_id="middle_b",
            role=CanonicalBullpenRole.MIDDLE_RELIEF,
        ),
        CanonicalBullpenPitcher(
            pitcher_id="middle_a",
            role=CanonicalBullpenRole.MIDDLE_RELIEF,
        ),
    )

    game = game_context(
        available=("middle_a", "middle_b"),
    )

    selection = (
        build_canonical_bullpen_selector()
        .select(
            context(
                game=game,
                options=options,
            )
        )
    )

    assert selection.pitcher_id == "middle_a"


def test_no_eligible_pitcher_is_rejected():
    with pytest.raises(
        ValueError,
        match="no eligible bullpen pitcher",
    ):
        (
            build_canonical_bullpen_selector()
            .select(
                context(
                    game=game_context(
                        available=(),
                    )
                )
            )
        )


def test_hold_decision_cannot_enter_selector():
    with pytest.raises(
        ValueError,
        match="requires a replace decision",
    ):
        CanonicalBullpenSelectionContext(
            pitching_decision=(
                CanonicalPitchingDecision(
                    action=(
                        CanonicalPitchingDecisionAction.HOLD
                    ),
                    current_pitcher_id="home_starter",
                )
            ),
            game_context=game_context(),
            bullpen=bullpen(),
        )
