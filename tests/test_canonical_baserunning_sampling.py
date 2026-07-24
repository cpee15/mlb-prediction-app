import pytest

from mlb_app.simulation.events import Base, GameState
from mlb_app.simulation.game import (
    CANONICAL_BASERUNNING_SAMPLING_VERSION,
    CanonicalBaserunningOutcome,
    CanonicalBaserunningProbabilities,
    CanonicalBaserunningSamplingQuery,
    sample_canonical_baserunning,
)


def probabilities(
    *,
    attempt_probability=1.0,
    success_probability=1.0,
    trial_seed=12345,
):
    return CanonicalBaserunningProbabilities(
        query=CanonicalBaserunningSamplingQuery(
            game_pk=824406,
            trial_index=4,
            trial_seed=trial_seed,
            sequence=12,
            state=GameState(
                inning=6,
                half="top",
                outs=1,
                bases=("runner", None, None),
                batting_order_index=3,
                plate_appearance_number=21,
            ),
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


def test_fixed_identity_reproduces_baserunning_sample():
    first = sample_canonical_baserunning(
        probabilities()
    )
    second = sample_canonical_baserunning(
        probabilities()
    )

    assert first == second
    assert first.attempt_seed == second.attempt_seed
    assert first.attempt_draw == second.attempt_draw
    assert first.success_seed == second.success_seed
    assert first.success_draw == second.success_draw


def test_zero_attempt_probability_holds_without_success_draw():
    sampled = sample_canonical_baserunning(
        probabilities(
            attempt_probability=0.0,
        )
    )

    assert sampled.outcome is CanonicalBaserunningOutcome.HOLD
    assert sampled.success_seed is None
    assert sampled.success_draw is None


def test_guaranteed_attempt_and_success_selects_stolen_base():
    sampled = sample_canonical_baserunning(
        probabilities(
            attempt_probability=1.0,
            success_probability=1.0,
        )
    )

    assert (
        sampled.outcome
        is CanonicalBaserunningOutcome.STOLEN_BASE
    )
    assert sampled.success_seed is not None
    assert sampled.success_draw is not None
    assert sampled.attempt_seed != sampled.success_seed
    assert (
        sampled.sampling_version
        == CANONICAL_BASERUNNING_SAMPLING_VERSION
    )


def test_guaranteed_attempt_and_failure_selects_caught_stealing():
    sampled = sample_canonical_baserunning(
        probabilities(
            attempt_probability=1.0,
            success_probability=0.0,
        )
    )

    assert (
        sampled.outcome
        is CanonicalBaserunningOutcome.CAUGHT_STEALING
    )
    assert sampled.success_seed is not None
    assert sampled.success_draw is not None


def test_probability_contract_preserves_provenance():
    source = probabilities()

    assert (
        source.probability_provenance
        == "stolen_base_pickoff_evaluator_v1"
    )


@pytest.mark.parametrize(
    "field,value",
    (
        ("attempt_probability", -0.01),
        ("attempt_probability", 1.01),
        ("success_probability", -0.01),
        ("success_probability", 1.01),
    ),
)
def test_probability_contract_rejects_invalid_rates(
    field,
    value,
):
    values = {
        "attempt_probability": 0.2,
        "success_probability": 0.7,
    }
    values[field] = value

    with pytest.raises(
        ValueError,
        match=f"{field} must be between 0 and 1",
    ):
        probabilities(**values)


def test_sampling_query_rejects_occupied_target():
    with pytest.raises(
        ValueError,
        match="target base must be unoccupied",
    ):
        CanonicalBaserunningSamplingQuery(
            game_pk=824406,
            trial_index=0,
            trial_seed=1,
            sequence=0,
            state=GameState(
                bases=("runner", "other-runner", None),
            ),
            batter_id="batter",
            pitcher_id="pitcher",
            runner_id="runner",
            origin_base=Base.FIRST,
            target_base=Base.SECOND,
        )
