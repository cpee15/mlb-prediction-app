import pytest

from mlb_app.simulation.events import (
    Base,
    ErrorType,
    GameState,
    MultiOutPlayResolver,
    SacrificeType,
)


def state(*, outs=0, bases=(None, None, None)):
    return GameState(
        outs=outs,
        bases=bases,
    )


def movements_by_runner(event):
    return {
        movement.runner_id: movement
        for movement in event.runner_movements
    }


def test_ground_ball_double_play_records_two_outs():
    event = MultiOutPlayResolver().resolve(
        state=state(
            outs=0,
            bases=("runner_1", None, None),
        ),
        event_type="ground_ball_double_play",
        batter_id="batter",
        sequence=0,
    )

    assert event.state_after.outs == 2
    assert len(event.outs_recorded) == 2
    assert event.state_after.bases == (
        None,
        None,
        None,
    )


def test_double_play_with_one_out_ends_inning():
    event = MultiOutPlayResolver().resolve(
        state=state(
            outs=1,
            bases=("runner_1", None, None),
        ),
        event_type="ground_ball_double_play",
        batter_id="batter",
        sequence=0,
    )

    assert event.state_after.outs == 3


def test_force_third_out_cancels_run():
    event = MultiOutPlayResolver().resolve(
        state=state(
            outs=1,
            bases=("runner_1", None, "runner_3"),
        ),
        event_type="ground_ball_double_play",
        batter_id="batter",
        sequence=0,
    )

    assert event.runs_scored == ()
    assert event.state_after.away_score == 0


def test_fielders_choice_retires_forced_runner():
    event = MultiOutPlayResolver().resolve(
        state=state(
            bases=("runner_1", None, None),
        ),
        event_type="ground_ball_fielders_choice",
        batter_id="batter",
        sequence=0,
    )

    movements = movements_by_runner(event)

    assert movements["runner_1"].is_out is True
    assert movements["batter"].end_base is Base.FIRST
    assert event.state_after.outs == 1


def test_sacrifice_fly_scores_runner_and_credits_rbi():
    event = MultiOutPlayResolver().resolve(
        state=state(
            outs=1,
            bases=(None, None, "runner_3"),
        ),
        event_type="sacrifice_fly",
        batter_id="batter",
        sequence=0,
    )

    assert event.runs_scored == ("runner_3",)
    assert event.attribution.rbi_count == 1
    assert (
        event.attribution.sacrifice_type
        is SacrificeType.FLY
    )


def test_sacrifice_fly_does_not_score_with_two_outs():
    event = MultiOutPlayResolver().resolve(
        state=state(
            outs=2,
            bases=(None, None, "runner_3"),
        ),
        event_type="sacrifice_fly",
        batter_id="batter",
        sequence=0,
    )

    assert event.runs_scored == ()
    assert event.attribution.rbi_count == 0
    assert event.attribution.sacrifice_type is None


def test_sacrifice_bunt_advances_runners():
    event = MultiOutPlayResolver().resolve(
        state=state(
            bases=("runner_1", "runner_2", None),
        ),
        event_type="sacrifice_bunt",
        batter_id="batter",
        sequence=0,
    )

    assert event.state_after.bases == (
        None,
        "runner_1",
        "runner_2",
    )
    assert (
        event.attribution.sacrifice_type
        is SacrificeType.BUNT
    )


def test_reached_on_error_requires_error_attribution():
    with pytest.raises(
        ValueError,
        match="requires error attribution",
    ):
        MultiOutPlayResolver().resolve(
            state=state(),
            event_type="reached_on_error",
            batter_id="batter",
            sequence=0,
        )


def test_reached_on_error_records_error():
    event = MultiOutPlayResolver().resolve(
        state=state(
            bases=("runner_1", None, None),
        ),
        event_type="reached_on_error",
        batter_id="batter",
        sequence=0,
        error_fielder_id="shortstop",
        error_type=ErrorType.FIELDING,
    )

    assert event.state_after.bases == (
        "batter",
        "runner_1",
        None,
    )
    assert event.attribution.error_fielder_id == "shortstop"
    assert event.attribution.error_type is ErrorType.FIELDING


def test_double_play_requires_runner_on_first():
    with pytest.raises(
        ValueError,
        match="requires a runner on first",
    ):
        MultiOutPlayResolver().resolve(
            state=state(),
            event_type="ground_ball_double_play",
            batter_id="batter",
            sequence=0,
        )


def test_unsupported_multi_out_event_is_rejected():
    with pytest.raises(
        ValueError,
        match="unsupported multi-out event_type",
    ):
        MultiOutPlayResolver().resolve(
            state=state(),
            event_type="triple_play",
            batter_id="batter",
            sequence=0,
        )
