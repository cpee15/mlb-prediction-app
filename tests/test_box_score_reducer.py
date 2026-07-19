from dataclasses import replace

import pytest

from mlb_app.simulation.box_score import (
    BatterDfsScoringRules,
    PitcherDfsScoringRules,
    reduce_box_score,
    score_batter,
    score_pitcher,
    validate_box_score_reconstruction,
)
from mlb_app.simulation.events import (
    DeterministicPlayResolver,
    GameState,
    MultiOutPlayResolver,
)


def with_pitcher(event, pitcher_id="pitcher"):
    return replace(event, pitcher_id=pitcher_id)


def test_home_run_reduces_batter_team_and_pitcher_lines():
    initial = GameState(
        bases=("runner_1", None, None),
    )

    event = with_pitcher(
        DeterministicPlayResolver().resolve(
            state=initial,
            event_type="hr",
            batter_id="batter",
            sequence=0,
        )
    )

    box = reduce_box_score(
        initial_state=initial,
        events=(event,),
    )

    batter = box.batter("batter")
    pitcher = box.pitcher("pitcher")

    assert batter.plate_appearances == 1
    assert batter.at_bats == 1
    assert batter.home_runs == 1
    assert batter.runs == 1
    assert box.batter("runner_1").runs == 1
    assert box.away.runs == 2
    assert box.away.hits == 1
    assert pitcher.batters_faced == 1
    assert pitcher.hits_allowed == 1
    assert pitcher.home_runs_allowed == 1
    assert pitcher.runs_allowed == 2
    assert pitcher.earned_runs is None
    assert pitcher.innings_pitched == "0.0"


def test_walk_is_not_at_bat():
    initial = GameState()

    event = with_pitcher(
        DeterministicPlayResolver().resolve(
            state=initial,
            event_type="bb",
            batter_id="batter",
            sequence=0,
        )
    )

    box = reduce_box_score(
        initial_state=initial,
        events=(event,),
    )

    batter = box.batter("batter")

    assert batter.plate_appearances == 1
    assert batter.at_bats == 0
    assert batter.walks == 1
    assert box.pitcher("pitcher").walks == 1


def test_sacrifice_fly_reduces_rbi_and_sacrifice():
    initial = GameState(
        outs=1,
        bases=(None, None, "runner_3"),
    )

    event = with_pitcher(
        MultiOutPlayResolver().resolve(
            state=initial,
            event_type="sacrifice_fly",
            batter_id="batter",
            sequence=0,
        )
    )

    box = reduce_box_score(
        initial_state=initial,
        events=(event,),
    )

    batter = box.batter("batter")

    assert batter.plate_appearances == 1
    assert batter.at_bats == 0
    assert batter.sacrifice_flies == 1
    assert batter.rbi == 1
    assert box.batter("runner_3").runs == 1
    assert box.pitcher("pitcher").outs_recorded == 1


def test_reached_on_error_charges_defensive_error():
    initial = GameState()

    event = MultiOutPlayResolver().resolve(
        state=initial,
        event_type="reached_on_error",
        batter_id="batter",
        sequence=0,
        error_fielder_id="shortstop",
        error_type=__import__(
            "mlb_app.simulation.events",
            fromlist=["ErrorType"],
        ).ErrorType.FIELDING,
    )

    box = reduce_box_score(
        initial_state=initial,
        events=(event,),
    )

    assert box.batter("batter").reached_on_error == 1
    assert box.home.errors == 1
    assert box.pitcher_attribution_complete is False


def test_dfs_scoring_uses_reduced_lines():
    initial = GameState()

    event = with_pitcher(
        DeterministicPlayResolver().resolve(
            state=initial,
            event_type="hr",
            batter_id="batter",
            sequence=0,
        )
    )

    box = reduce_box_score(
        initial_state=initial,
        events=(event,),
    )

    batter_points = score_batter(
        box.batter("batter"),
        BatterDfsScoringRules(
            home_run=10.0,
            run=2.0,
        ),
    )

    assert batter_points == 12.0


def test_pitcher_earned_run_scoring_requires_reconstruction():
    initial = GameState()

    event = with_pitcher(
        DeterministicPlayResolver().resolve(
            state=initial,
            event_type="hr",
            batter_id="batter",
            sequence=0,
        )
    )

    box = reduce_box_score(
        initial_state=initial,
        events=(event,),
    )

    with pytest.raises(
        ValueError,
        match="earned-run scoring requires",
    ):
        score_pitcher(
            box.pitcher("pitcher"),
            PitcherDfsScoringRules(
                earned_run=-2.0,
            ),
        )


def test_replay_and_reduction_validation_pass():
    initial = GameState()

    event = with_pitcher(
        DeterministicPlayResolver().resolve(
            state=initial,
            event_type="hr",
            batter_id="batter",
            sequence=0,
        )
    )

    events = (event,)

    box = reduce_box_score(
        initial_state=initial,
        events=events,
    )

    validation = validate_box_score_reconstruction(
        initial_state=initial,
        events=events,
        box_score=box,
    )

    assert validation.passed is True
    assert validation.pitcher_attribution_complete is True


def test_reducer_is_deterministic():
    initial = GameState()

    event = with_pitcher(
        DeterministicPlayResolver().resolve(
            state=initial,
            event_type="hbp",
            batter_id="batter",
            sequence=0,
        )
    )

    first = reduce_box_score(
        initial_state=initial,
        events=(event,),
    )
    second = reduce_box_score(
        initial_state=initial,
        events=(event,),
    )

    assert first == second
