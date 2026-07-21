import pytest

from mlb_app.simulation.events import (
    Base,
    GameState,
)
from mlb_app.simulation.game import (
    CanonicalLineup,
    CanonicalMatchupInput,
    CanonicalPitchingPlan,
    CanonicalPlateAppearanceOutcome,
    CanonicalPlateAppearanceQuery,
    CanonicalProbabilityProviderIdentity,
    CanonicalSampledPlateAppearance,
    resolve_canonical_sampled_plate_appearance,
)


def lineup(side):
    return CanonicalLineup(
        team_side=side,
        player_ids=tuple(
            f"{side}_batter_{index}"
            for index in range(9)
        ),
    )


def pitching_plan(side):
    return CanonicalPitchingPlan(
        team_side=side,
        starter_id=f"{side}_starter",
        bullpen_pitcher_ids=(
            f"{side}_reliever",
        ),
    )


def matchup():
    return CanonicalMatchupInput(
        game_pk=123,
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        away_pitching_plan=(
            pitching_plan("away")
        ),
        home_pitching_plan=(
            pitching_plan("home")
        ),
        probability_provider=(
            CanonicalProbabilityProviderIdentity(
                provider_name="resolution-test",
                provider_version="v1",
            )
        ),
    )


def sampled(
    outcome,
    *,
    state=None,
    sequence=0,
):
    state = state or GameState(
        inning=1,
        half="top",
    )

    query = CanonicalPlateAppearanceQuery(
        matchup_input=matchup(),
        state=state,
        batter_id="away_batter_0",
        pitcher_id="home_starter",
        sequence=sequence,
        trial_index=0,
        trial_seed=12345,
    )

    return CanonicalSampledPlateAppearance(
        query=query,
        outcome=outcome,
        draw=0.25,
        sampling_seed=67890,
    )


@pytest.mark.parametrize(
    "outcome,event_type",
    [
        (
            CanonicalPlateAppearanceOutcome.WALK,
            "bb",
        ),
        (
            CanonicalPlateAppearanceOutcome.HIT_BY_PITCH,
            "hbp",
        ),
    ],
)
def test_forced_awards_reuse_deterministic_resolution(
    outcome,
    event_type,
):
    state = GameState(
        inning=1,
        half="top",
        bases=(
            "away_batter_1",
            "away_batter_2",
            "away_batter_3",
        ),
    )

    event = (
        resolve_canonical_sampled_plate_appearance(
            sampled(
                outcome,
                state=state,
            )
        )
    )

    assert event.event_type == event_type
    assert event.pitcher_id == "home_starter"
    assert event.state_after.bases == (
        "away_batter_0",
        "away_batter_1",
        "away_batter_2",
    )
    assert event.runs_scored == (
        "away_batter_3",
    )
    assert event.state_after.away_score == 1


@pytest.mark.parametrize(
    "outcome,event_type,reason",
    [
        (
            CanonicalPlateAppearanceOutcome.STRIKEOUT,
            "k",
            "strikeout",
        ),
    ],
)
def test_batter_out_preserves_existing_runners(
    outcome,
    event_type,
    reason,
):
    state = GameState(
        inning=2,
        half="top",
        outs=1,
        bases=(
            "away_batter_1",
            None,
            "away_batter_3",
        ),
    )

    event = (
        resolve_canonical_sampled_plate_appearance(
            sampled(
                outcome,
                state=state,
                sequence=7,
            )
        )
    )

    assert event.sequence == 7
    assert event.event_type == event_type
    assert event.pitcher_id == "home_starter"
    assert event.state_after.outs == 2
    assert event.state_after.bases == state.bases
    assert event.outs_recorded[0].reason == reason
    assert event.runs_scored == ()


@pytest.mark.parametrize(
    "outcome,destination",
    [
        (
            CanonicalPlateAppearanceOutcome.SINGLE,
            Base.FIRST,
        ),
        (
            CanonicalPlateAppearanceOutcome.DOUBLE,
            Base.SECOND,
        ),
        (
            CanonicalPlateAppearanceOutcome.TRIPLE,
            Base.THIRD,
        ),
    ],
)
def test_empty_base_hits_place_batter_at_fixed_base(
    outcome,
    destination,
):
    event = (
        resolve_canonical_sampled_plate_appearance(
            sampled(outcome)
        )
    )

    assert event.event_type == outcome.value
    assert event.pitcher_id == "home_starter"
    assert event.runner_movements[0].end_base is destination
    assert (
        event.state_after.runner_on(destination)
        == "away_batter_0"
    )
    assert event.state_after.outs == 0
    assert event.runs_scored == ()


def test_occupied_base_triple_advances_runner():
    state = GameState(
        inning=1,
        half="top",
        bases=(
            "away_batter_1",
            None,
            None,
        ),
    )

    event = resolve_canonical_sampled_plate_appearance(
        sampled(
            CanonicalPlateAppearanceOutcome.TRIPLE,
            state=state,
        )
    )

    assert event.event_type == "triple"
    assert event.state_after.first is None
    assert event.state_after.second is None
    assert event.state_after.third == (
        "away_batter_0"
    )
    assert event.state_after.away_score == 1
    assert event.runs_scored == (
        "away_batter_1",
    )


def test_home_run_scores_all_runners_and_batter():
    state = GameState(
        inning=3,
        half="top",
        bases=(
            "away_batter_1",
            "away_batter_2",
            "away_batter_3",
        ),
    )

    event = (
        resolve_canonical_sampled_plate_appearance(
            sampled(
                CanonicalPlateAppearanceOutcome.HOME_RUN,
                state=state,
            )
        )
    )

    assert event.event_type == "hr"
    assert event.pitcher_id == "home_starter"
    assert event.state_after.bases == (
        None,
        None,
        None,
    )
    assert event.runs_scored == (
        "away_batter_1",
        "away_batter_2",
        "away_batter_3",
        "away_batter_0",
    )
    assert event.state_after.away_score == 4


def test_resolution_rejects_non_sample_contract():
    with pytest.raises(
        TypeError,
        match="CanonicalSampledPlateAppearance",
    ):
        resolve_canonical_sampled_plate_appearance(
            object()
        )
