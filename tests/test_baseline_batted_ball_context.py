from dataclasses import FrozenInstanceError
import random

import pytest

from mlb_app.simulation.events import (
    BASELINE_BATTED_BALL_MODEL_VERSION,
    BattedBallContext,
    BattedBallDepth,
    BattedBallType,
    BaselineBattedBallContextProvider,
    ContactQuality,
    SprayDirection,
    validate_baseline_batted_ball_distributions,
)


def test_baseline_distributions_validate():
    validate_baseline_batted_ball_distributions()


def test_context_is_immutable():
    context = BattedBallContext(
        batted_ball_type=BattedBallType.GROUND_BALL,
        direction=SprayDirection.PULL,
        depth=BattedBallDepth.SHALLOW,
        contact_quality=ContactQuality.MEDIUM,
    )

    with pytest.raises(FrozenInstanceError):
        context.depth = BattedBallDepth.DEEP


def test_context_exposes_versioned_contract():
    provider = BaselineBattedBallContextProvider(
        rng=random.Random(7),
    )

    context = provider.sample(primary_outcome="single")

    assert context is not None
    assert (
        context.model_version
        == BASELINE_BATTED_BALL_MODEL_VERSION
    )


@pytest.mark.parametrize(
    "primary_outcome",
    ["bb", "hbp", "k", "strikeout"],
)
def test_non_batted_ball_outcomes_return_no_context(
    primary_outcome,
):
    provider = BaselineBattedBallContextProvider(
        rng=random.Random(3),
    )

    assert (
        provider.sample(primary_outcome=primary_outcome)
        is None
    )


def test_groundout_has_ground_ball_context():
    provider = BaselineBattedBallContextProvider(
        rng=random.Random(11),
    )

    context = provider.sample(
        primary_outcome="out",
        outcome_subtype="groundout",
    )

    assert context is not None
    assert (
        context.batted_ball_type
        is BattedBallType.GROUND_BALL
    )


def test_flyout_has_fly_ball_context():
    provider = BaselineBattedBallContextProvider(
        rng=random.Random(13),
    )

    context = provider.sample(
        primary_outcome="out",
        outcome_subtype="flyout",
    )

    assert context is not None
    assert (
        context.batted_ball_type
        is BattedBallType.FLY_BALL
    )


def test_lineout_popout_only_samples_legal_types():
    provider = BaselineBattedBallContextProvider(
        rng=random.Random(17),
    )

    observed = {
        provider.sample(
            primary_outcome="out",
            outcome_subtype="lineout_popout",
        ).batted_ball_type
        for _ in range(100)
    }

    assert observed <= {
        BattedBallType.LINE_DRIVE,
        BattedBallType.POPUP,
    }
    assert observed


def test_home_run_only_samples_line_drive_or_fly_ball():
    provider = BaselineBattedBallContextProvider(
        rng=random.Random(19),
    )

    observed = {
        provider.sample(
            primary_outcome="hr",
        ).batted_ball_type
        for _ in range(100)
    }

    assert observed <= {
        BattedBallType.LINE_DRIVE,
        BattedBallType.FLY_BALL,
    }
    assert observed


def test_fixed_seed_reproduces_context_sequence():
    provider_a = BaselineBattedBallContextProvider(
        rng=random.Random(23),
    )
    provider_b = BaselineBattedBallContextProvider(
        rng=random.Random(23),
    )

    sequence_a = tuple(
        provider_a.sample(primary_outcome="double")
        for _ in range(20)
    )
    sequence_b = tuple(
        provider_b.sample(primary_outcome="double")
        for _ in range(20)
    )

    assert sequence_a == sequence_b


def test_sample_contains_all_required_context_dimensions():
    provider = BaselineBattedBallContextProvider(
        rng=random.Random(29),
    )

    context = provider.sample(
        primary_outcome="single",
    )

    assert context is not None
    assert isinstance(
        context.batted_ball_type,
        BattedBallType,
    )
    assert isinstance(
        context.direction,
        SprayDirection,
    )
    assert isinstance(
        context.depth,
        BattedBallDepth,
    )
    assert isinstance(
        context.contact_quality,
        ContactQuality,
    )


def test_outcome_subtype_requires_primary_out():
    provider = BaselineBattedBallContextProvider(
        rng=random.Random(31),
    )

    with pytest.raises(
        ValueError,
        match="only valid",
    ):
        provider.sample(
            primary_outcome="single",
            outcome_subtype="groundout",
        )


def test_non_batted_ball_outcome_rejects_subtype():
    provider = BaselineBattedBallContextProvider(
        rng=random.Random(37),
    )

    with pytest.raises(
        ValueError,
        match="cannot have",
    ):
        provider.sample(
            primary_outcome="strikeout",
            outcome_subtype="groundout",
        )


def test_unknown_primary_outcome_is_rejected():
    provider = BaselineBattedBallContextProvider(
        rng=random.Random(41),
    )

    with pytest.raises(
        ValueError,
        match="unsupported primary_outcome",
    ):
        provider.sample(
            primary_outcome="catcher_interference",
        )


def test_unknown_outcome_subtype_is_rejected():
    provider = BaselineBattedBallContextProvider(
        rng=random.Random(43),
    )

    with pytest.raises(
        ValueError,
        match="unsupported outcome_subtype",
    ):
        provider.sample(
            primary_outcome="out",
            outcome_subtype="mystery_out",
        )


def test_ground_ball_never_receives_deep_depth():
    provider = BaselineBattedBallContextProvider(
        rng=random.Random(47),
    )

    contexts = tuple(
        provider.sample(
            primary_outcome="out",
            outcome_subtype="groundout",
        )
        for _ in range(200)
    )

    assert all(
        context.depth is not BattedBallDepth.DEEP
        for context in contexts
    )


def test_provider_does_not_mutate_external_game_state():
    provider = BaselineBattedBallContextProvider(
        rng=random.Random(53),
    )
    external_state = {
        "bases": (True, False, True),
        "outs": 1,
        "score": 2,
    }
    original = dict(external_state)

    provider.sample(
        primary_outcome="out",
        outcome_subtype="flyout",
    )

    assert external_state == original
