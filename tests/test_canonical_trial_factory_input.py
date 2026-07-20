import pytest

from mlb_app.simulation.game import (
    CANONICAL_TRIAL_FACTORY_INPUT_VERSION,
    CANONICAL_TRIAL_SEED_VERSION,
    build_canonical_trial_factory_input,
    derive_canonical_base_seed,
    derive_canonical_trial_seed,
)


def test_factory_input_is_deterministic():
    first = build_canonical_trial_factory_input(
        game_pk=123,
        config={
            "simulation_count": 4,
            "environment": {
                "park_factor": 1.05,
            },
        },
    )
    second = build_canonical_trial_factory_input(
        game_pk=123,
        config={
            "environment": {
                "park_factor": 1.05,
            },
            "simulation_count": 4,
        },
    )

    assert first == second
    assert first.schema_version == (
        CANONICAL_TRIAL_FACTORY_INPUT_VERSION
    )
    assert first.seed_version == (
        CANONICAL_TRIAL_SEED_VERSION
    )
    assert first.seed_source == "derived"
    assert len(first.trial_seeds) == 4


def test_game_identity_changes_derived_seeds():
    first = build_canonical_trial_factory_input(
        game_pk=123,
        config={"simulation_count": 3},
    )
    second = build_canonical_trial_factory_input(
        game_pk=124,
        config={"simulation_count": 3},
    )

    assert first.base_seed != second.base_seed
    assert first.trial_seeds != second.trial_seeds


def test_explicit_seed_preserves_trial_prefix():
    short = build_canonical_trial_factory_input(
        game_pk=123,
        config={
            "simulation_count": 2,
            "seed": 98765,
        },
    )
    long = build_canonical_trial_factory_input(
        game_pk=123,
        config={
            "simulation_count": 5,
            "seed": 98765,
        },
    )

    assert short.seed_source == "explicit"
    assert short.base_seed == 98765
    assert long.trial_seeds[:2] == (
        short.trial_seeds
    )


def test_config_round_trip_is_detached():
    factory_input = (
        build_canonical_trial_factory_input(
            game_pk=123,
            config={
                "simulation_count": 2,
                "nested": {
                    "values": [1, 2, 3],
                },
            },
        )
    )

    first = factory_input.config_dict()
    first["nested"]["values"].append(4)

    second = factory_input.config_dict()

    assert second["nested"]["values"] == [
        1,
        2,
        3,
    ]


def test_seed_helpers_are_stable_and_indexed():
    base_seed = derive_canonical_base_seed(
        game_pk=123,
        model_version="canonical-test-v1",
    )

    first = derive_canonical_trial_seed(
        base_seed=base_seed,
        trial_index=0,
    )
    repeated = derive_canonical_trial_seed(
        base_seed=base_seed,
        trial_index=0,
    )
    second = derive_canonical_trial_seed(
        base_seed=base_seed,
        trial_index=1,
    )

    assert first == repeated
    assert first != second


def test_invalid_simulation_count_is_rejected():
    with pytest.raises(
        ValueError,
        match="simulation_count must be positive",
    ):
        build_canonical_trial_factory_input(
            game_pk=123,
            config={
                "simulation_count": 0,
            },
        )
