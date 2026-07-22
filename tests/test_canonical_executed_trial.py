from dataclasses import replace

import pytest

from mlb_app.simulation.box_score import (
    PitcherBoxScore,
    ReducedBoxScore,
    TeamBoxScore,
)
from mlb_app.simulation.events import (
    Base,
    GameState,
    OutRecord,
    RunnerMovement,
    build_play_event,
)
from mlb_app.simulation.game import (
    CanonicalExecutedTrial,
    CanonicalGameConfig,
    CanonicalGameResult,
    CanonicalLineup,
    CanonicalPitcherRunLine,
    GameCompletionReason,
    HalfInningRecord,
    overlay_reconstructed_pitcher_run_lines,
)


def lineup(side):
    return CanonicalLineup(
        team_side=side,
        player_ids=tuple(
            f"{side}_{index}"
            for index in range(9)
        ),
    )


def completed_game():
    state = GameState(
        inning=1,
        half="top",
    )

    event = build_play_event(
        sequence=0,
        event_type="out",
        batter_id="away_0",
        state_before=state,
        runner_movements=(
            RunnerMovement(
                runner_id="away_0",
                start_base=0,
                end_base=None,
                is_out=True,
            ),
        ),
        outs_recorded=(
            OutRecord(
                runner_id="away_0",
                out_number=1,
                reason="batted_out",
            ),
        ),
    )

    second = replace(
        event,
        sequence=1,
        batter_id="away_1",
        state_before=event.state_after,
        state_after=replace(
            event.state_after,
            outs=2,
            batting_order_index=2,
            plate_appearance_number=2,
        ),
        runner_movements=(
            RunnerMovement(
                runner_id="away_1",
                start_base=0,
                end_base=None,
                is_out=True,
            ),
        ),
        outs_recorded=(
            OutRecord(
                runner_id="away_1",
                out_number=2,
                reason="batted_out",
            ),
        ),
    )

    third = replace(
        second,
        sequence=2,
        batter_id="away_2",
        state_before=second.state_after,
        state_after=replace(
            second.state_after,
            outs=3,
            batting_order_index=3,
            plate_appearance_number=3,
        ),
        runner_movements=(
            RunnerMovement(
                runner_id="away_2",
                start_base=0,
                end_base=None,
                is_out=True,
            ),
        ),
        outs_recorded=(
            OutRecord(
                runner_id="away_2",
                out_number=3,
                reason="batted_out",
            ),
        ),
    )

    half = HalfInningRecord(
        inning=1,
        half="top",
        initial_state=state,
        events=(event, second, third),
        batting_order_start=0,
        batting_order_end=3,
    )

    return CanonicalGameResult(
        config=CanonicalGameConfig(
            regulation_innings=1,
        ),
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        halves=(half,),
        final_state=third.state_after,
        completion_reason=(
            GameCompletionReason.REGULATION
        ),
    )


def box_score():
    return ReducedBoxScore(
        away=TeamBoxScore(
            team_side="away",
        ),
        home=TeamBoxScore(
            team_side="home",
        ),
        pitchers=(
            PitcherBoxScore(
                player_id="starter",
                team_side="home",
                batters_faced=3,
                outs_recorded=3,
                runs_allowed=2,
            ),
            PitcherBoxScore(
                player_id="reliever",
                team_side="home",
                batters_faced=1,
                outs_recorded=0,
                runs_allowed=1,
            ),
        ),
        pitcher_attribution_complete=True,
    )


def test_executed_trial_retains_game_and_run_lines():
    game = completed_game()
    lines = (
        CanonicalPitcherRunLine(
            pitcher_id="starter",
            runs_allowed=1,
            earned_runs=1,
            unearned_runs=0,
        ),
    )

    value = CanonicalExecutedTrial(
        game=game,
        reconstructed_pitcher_run_lines=lines,
        earned_run_reconstruction_complete=True,
    )

    assert value.game is game
    assert value.reconstructed_pitcher_run_lines == (
        lines
    )


def test_overlay_replaces_run_attribution():
    updated = overlay_reconstructed_pitcher_run_lines(
        box_score=box_score(),
        run_lines=(
            CanonicalPitcherRunLine(
                pitcher_id="starter",
                runs_allowed=3,
                earned_runs=2,
                unearned_runs=1,
            ),
        ),
    )

    starter = updated.pitcher("starter")
    reliever = updated.pitcher("reliever")

    assert starter.runs_allowed == 3
    assert starter.earned_runs == 2
    assert starter.earned_run_status == (
        "reconstructed"
    )

    assert reliever.runs_allowed == 0
    assert reliever.earned_runs == 0
    assert reliever.earned_run_status == (
        "reconstructed"
    )

    assert starter.batters_faced == 3
    assert reliever.batters_faced == 1


def test_overlay_rejects_unknown_pitcher():
    with pytest.raises(
        ValueError,
        match="no box-score appearance",
    ):
        overlay_reconstructed_pitcher_run_lines(
            box_score=box_score(),
            run_lines=(
                CanonicalPitcherRunLine(
                    pitcher_id="unknown",
                    runs_allowed=1,
                    earned_runs=1,
                    unearned_runs=0,
                ),
            ),
        )
