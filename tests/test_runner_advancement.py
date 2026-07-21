import random

import pytest

from mlb_app.simulation.events import (
    BASELINE_RUNNER_ADVANCEMENT_MODEL_VERSION,
    Base,
    BaselineRunnerAdvancementModel,
    BaselineRunnerAdvancementSampler,
    BattedBallContext,
    BattedBallDepth,
    BattedBallType,
    ContactQuality,
    GameState,
    SprayDirection,
    enumerate_legal_runner_destinations,
    validate_baseline_advancement_rates,
    validate_runner_movements,
)


def context(
    *,
    batted_ball_type=BattedBallType.LINE_DRIVE,
    depth=BattedBallDepth.MEDIUM,
    contact_quality=ContactQuality.MEDIUM,
):
    return BattedBallContext(
        batted_ball_type=batted_ball_type,
        direction=SprayDirection.CENTER,
        depth=depth,
        contact_quality=contact_quality,
    )


def state(
    *,
    outs=0,
    bases=(None, None, None),
):
    return GameState(
        outs=outs,
        bases=bases,
    )


def movement_by_runner(result):
    return {
        movement.runner_id: movement
        for movement in result.movements
    }


def test_baseline_rates_validate():
    validate_baseline_advancement_rates()


def test_single_places_batter_on_first():
    sampler = BaselineRunnerAdvancementSampler(
        rng=random.Random(1),
    )

    result = sampler.sample(
        state=state(),
        batter_id="batter",
        primary_outcome="single",
        context=context(),
    )

    batter = movement_by_runner(result)["batter"]

    assert batter.start_base is Base.HOME
    assert batter.end_base is Base.FIRST
    assert batter.is_forced is True


def test_double_places_batter_on_second():
    sampler = BaselineRunnerAdvancementSampler(
        rng=random.Random(2),
    )

    result = sampler.sample(
        state=state(),
        batter_id="batter",
        primary_outcome="double",
        context=context(),
    )

    batter = movement_by_runner(result)["batter"]

    assert batter.end_base is Base.SECOND
    assert batter.is_forced is True


def test_single_scores_runner_from_third():
    sampler = BaselineRunnerAdvancementSampler(
        rng=random.Random(3),
    )

    result = sampler.sample(
        state=state(
            bases=(None, None, "runner_3"),
        ),
        batter_id="batter",
        primary_outcome="single",
        context=context(),
    )

    runner = movement_by_runner(result)["runner_3"]

    assert runner.end_base is Base.HOME
    assert runner.scored is True
    assert result.runs_scored == ("runner_3",)


def test_double_scores_runners_from_second_and_third():
    sampler = BaselineRunnerAdvancementSampler(
        rng=random.Random(4),
    )

    result = sampler.sample(
        state=state(
            bases=(None, "runner_2", "runner_3"),
        ),
        batter_id="batter",
        primary_outcome="double",
        context=context(),
    )

    movements = movement_by_runner(result)

    assert movements["runner_2"].scored is True
    assert movements["runner_3"].scored is True
    assert result.runs_scored == (
        "runner_3",
        "runner_2",
    )


def test_forced_zero_rate_keeps_first_runner_at_second():
    model = BaselineRunnerAdvancementModel(
        rates={
            "single_runner_first_to_third": 0.0,
        }
    )
    sampler = BaselineRunnerAdvancementSampler(
        rng=random.Random(5),
        model=model,
    )

    result = sampler.sample(
        state=state(
            bases=("runner_1", None, None),
        ),
        batter_id="batter",
        primary_outcome="single",
        context=context(),
    )

    assert (
        movement_by_runner(result)["runner_1"].end_base
        is Base.SECOND
    )


def test_forced_one_rate_sends_first_runner_to_third():
    model = BaselineRunnerAdvancementModel(
        rates={
            "single_runner_first_to_third": 1.0,
        }
    )
    sampler = BaselineRunnerAdvancementSampler(
        rng=random.Random(6),
        model=model,
    )

    result = sampler.sample(
        state=state(
            bases=("runner_1", None, None),
        ),
        batter_id="batter",
        primary_outcome="single",
        context=context(),
    )

    assert (
        movement_by_runner(result)["runner_1"].end_base
        is Base.THIRD
    )


def test_collision_prevents_two_runners_on_third():
    model = BaselineRunnerAdvancementModel(
        rates={
            "single_runner_second_scores": 0.0,
            "single_runner_first_to_third": 1.0,
        }
    )
    sampler = BaselineRunnerAdvancementSampler(
        rng=random.Random(7),
        model=model,
    )

    result = sampler.sample(
        state=state(
            bases=("runner_1", "runner_2", None),
        ),
        batter_id="batter",
        primary_outcome="single",
        context=context(),
    )

    movements = movement_by_runner(result)

    assert movements["runner_2"].end_base is Base.THIRD
    assert movements["runner_1"].end_base is Base.SECOND
    validate_runner_movements(result.movements)


def test_deep_fly_can_score_runner_from_third():
    model = BaselineRunnerAdvancementModel(
        rates={
            "flyout_runner_third_scores_deep": 1.0,
        }
    )
    sampler = BaselineRunnerAdvancementSampler(
        rng=random.Random(8),
        model=model,
    )

    result = sampler.sample(
        state=state(
            bases=(None, None, "runner_3"),
        ),
        batter_id="batter",
        primary_outcome="out",
        context=context(
            batted_ball_type=BattedBallType.FLY_BALL,
            depth=BattedBallDepth.DEEP,
        ),
    )

    assert (
        movement_by_runner(result)["runner_3"].scored
        is True
    )


def test_runner_does_not_tag_with_two_outs():
    model = BaselineRunnerAdvancementModel(
        rates={
            "flyout_runner_third_scores_deep": 1.0,
        }
    )
    sampler = BaselineRunnerAdvancementSampler(
        rng=random.Random(9),
        model=model,
    )

    result = sampler.sample(
        state=state(
            outs=2,
            bases=(None, None, "runner_3"),
        ),
        batter_id="batter",
        primary_outcome="out",
        context=context(
            batted_ball_type=BattedBallType.FLY_BALL,
            depth=BattedBallDepth.DEEP,
        ),
    )

    runner = movement_by_runner(result)["runner_3"]

    assert runner.end_base is Base.THIRD
    assert runner.scored is False


def test_groundout_can_advance_runner_from_second():
    model = BaselineRunnerAdvancementModel(
        rates={
            "groundout_runner_second_to_third": 1.0,
        }
    )
    sampler = BaselineRunnerAdvancementSampler(
        rng=random.Random(10),
        model=model,
    )

    result = sampler.sample(
        state=state(
            bases=(None, "runner_2", None),
        ),
        batter_id="batter",
        primary_outcome="out",
        context=context(
            batted_ball_type=BattedBallType.GROUND_BALL,
            depth=BattedBallDepth.SHALLOW,
        ),
    )

    assert (
        movement_by_runner(result)["runner_2"].end_base
        is Base.THIRD
    )


def test_fixed_seed_reproduces_advancement_sequence():
    sampler_a = BaselineRunnerAdvancementSampler(
        rng=random.Random(11),
    )
    sampler_b = BaselineRunnerAdvancementSampler(
        rng=random.Random(11),
    )
    game_state = state(
        bases=("runner_1", "runner_2", "runner_3"),
    )
    ball_context = context()

    sequence_a = tuple(
        sampler_a.sample(
            state=game_state,
            batter_id=f"batter_{index}",
            primary_outcome="single",
            context=ball_context,
        )
        for index in range(20)
    )
    sequence_b = tuple(
        sampler_b.sample(
            state=game_state,
            batter_id=f"batter_{index}",
            primary_outcome="single",
            context=ball_context,
        )
        for index in range(20)
    )

    assert sequence_a == sequence_b


def test_result_contains_model_provenance():
    sampler = BaselineRunnerAdvancementSampler(
        rng=random.Random(12),
    )

    result = sampler.sample(
        state=state(),
        batter_id="batter",
        primary_outcome="single",
        context=context(),
    )

    assert (
        result.metadata.model_version
        == BASELINE_RUNNER_ADVANCEMENT_MODEL_VERSION
    )
    assert result.metadata.calibrated is False
    assert result.metadata.production_enabled is False


def test_legal_enumerator_exposes_single_destinations():
    legal = enumerate_legal_runner_destinations(
        state=state(
            bases=("runner_1", "runner_2", None),
        ),
        batter_id="batter",
        primary_outcome="single",
        context=context(),
    )

    by_runner = {
        item.runner_id: item
        for item in legal
    }

    assert by_runner["runner_1"].destinations == (
        Base.SECOND,
        Base.THIRD,
    )
    assert by_runner["runner_2"].destinations == (
        Base.THIRD,
        Base.HOME,
    )
    assert by_runner["batter"].destinations == (
        Base.FIRST,
    )


def test_unsupported_outcome_is_rejected():
    sampler = BaselineRunnerAdvancementSampler(
        rng=random.Random(13),
    )

    with pytest.raises(
        ValueError,
        match="unsupported advancement outcome",
    ):
        sampler.sample(
            state=state(),
            batter_id="batter",
            primary_outcome="home_run",
            context=context(),
        )


def test_unknown_model_rate_is_rejected():
    with pytest.raises(
        ValueError,
        match="unknown advancement rates",
    ):
        BaselineRunnerAdvancementModel(
            rates={"unknown_rate": 0.5}
        )
