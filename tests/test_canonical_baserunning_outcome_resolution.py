import pytest

from mlb_app.simulation.events import Base, GameState
from mlb_app.simulation.game import (
    CANONICAL_BASERUNNING_RESOLUTION_VERSION,
    CanonicalBaserunningOutcome,
    CanonicalBaserunningProbabilities,
    CanonicalBaserunningSamplingQuery,
    resolve_canonical_sampled_baserunning,
    sample_canonical_baserunning,
)


def sampled(
    *,
    attempt_probability,
    success_probability,
    state=None,
):
    game_state = state or GameState(
        inning=7,
        half="bottom",
        outs=1,
        bases=("runner", None, "stationary-runner"),
        away_score=3,
        home_score=2,
        batting_order_index=5,
        plate_appearance_number=24,
    )
    probabilities = CanonicalBaserunningProbabilities(
        query=CanonicalBaserunningSamplingQuery(
            game_pk=824406,
            trial_index=3,
            trial_seed=918273,
            sequence=17,
            state=game_state,
            batter_id="batter",
            pitcher_id="pitcher",
            runner_id="runner",
            origin_base=Base.FIRST,
            target_base=Base.SECOND,
        ),
        attempt_probability=attempt_probability,
        success_probability=success_probability,
        probability_provenance=(
            "stolen_base_pickoff_evaluator_v1"
        ),
    )
    return sample_canonical_baserunning(
        probabilities
    )


def test_hold_produces_no_canonical_event():
    resolution = resolve_canonical_sampled_baserunning(
        sampled(
            attempt_probability=0.0,
            success_probability=1.0,
        )
    )

    assert (
        resolution.sampled.outcome
        is CanonicalBaserunningOutcome.HOLD
    )
    assert resolution.event is None
    assert (
        resolution.resolution_version
        == CANONICAL_BASERUNNING_RESOLUTION_VERSION
    )


def test_stolen_base_sample_produces_non_pa_event():
    resolution = resolve_canonical_sampled_baserunning(
        sampled(
            attempt_probability=1.0,
            success_probability=1.0,
        )
    )

    event = resolution.event

    assert event is not None
    assert event.event_type == "stolen_base"
    assert event.sequence == 17
    assert event.batter_id == "batter"
    assert event.pitcher_id == "pitcher"
    assert event.is_plate_appearance is False
    assert event.state_after.bases == (
        None,
        "runner",
        "stationary-runner",
    )
    assert event.state_after.outs == 1
    assert event.state_after.batting_order_index == 5
    assert event.state_after.plate_appearance_number == 24


def test_caught_stealing_sample_produces_explicit_out():
    resolution = resolve_canonical_sampled_baserunning(
        sampled(
            attempt_probability=1.0,
            success_probability=0.0,
        )
    )

    event = resolution.event

    assert event is not None
    assert event.event_type == "caught_stealing"
    assert event.state_after.bases == (
        None,
        None,
        "stationary-runner",
    )
    assert event.state_after.outs == 2
    assert len(event.outs_recorded) == 1
    assert event.outs_recorded[0].runner_id == "runner"
    assert event.outs_recorded[0].out_number == 2
    assert event.outs_recorded[0].reason == "caught_stealing"


def test_resolution_preserves_sampling_rng_provenance():
    source = sampled(
        attempt_probability=1.0,
        success_probability=1.0,
    )

    resolution = resolve_canonical_sampled_baserunning(
        source
    )

    assert resolution.sampled is source
    assert resolution.sampled.attempt_seed is not None
    assert resolution.sampled.attempt_draw is not None
    assert resolution.sampled.success_seed is not None
    assert resolution.sampled.success_draw is not None
    assert (
        resolution.sampled.probabilities
        .probability_provenance
        == "stolen_base_pickoff_evaluator_v1"
    )


def test_resolution_rejects_non_sample_contract():
    with pytest.raises(
        TypeError,
        match="sampled must be CanonicalSampledBaserunning",
    ):
        resolve_canonical_sampled_baserunning(
            object()
        )
