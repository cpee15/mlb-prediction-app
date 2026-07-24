import math

import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_RUNNER_SPRINT_SPEED_NORMALIZATION_VERSION,
    CANONICAL_RUNNER_SPRINT_SPEED_SOURCE_VERSION,
    CanonicalRunnerSprintSpeedObservation,
    decode_baseball_savant_sprint_speed_rows,
    normalize_runner_sprint_speed,
)


def row(**overrides):
    value = {
        "player_id": 650489,
        "sprint_speed": 28.6,
    }
    value.update(overrides)
    return value


def test_decodes_complete_sprint_speed_observation():
    observations = (
        decode_baseball_savant_sprint_speed_rows(
            (row(),)
        )
    )

    assert len(observations) == 1

    observation = observations[0]

    assert observation.runner_id == "650489"
    assert (
        observation.sprint_speed_ft_per_second
        == 28.6
    )
    assert observation.speed_score == 0.8
    assert observation.source_version == (
        CANONICAL_RUNNER_SPRINT_SPEED_SOURCE_VERSION
    )
    assert observation.normalization_version == (
        CANONICAL_RUNNER_SPRINT_SPEED_NORMALIZATION_VERSION
    )


def test_normalization_clamps_observed_extremes():
    assert normalize_runner_sprint_speed(22.0) == 0.0
    assert normalize_runner_sprint_speed(23.0) == 0.0
    assert normalize_runner_sprint_speed(30.0) == 1.0
    assert normalize_runner_sprint_speed(31.0) == 1.0


def test_missing_identity_is_not_fabricated():
    assert (
        decode_baseball_savant_sprint_speed_rows(
            (row(player_id=None),)
        )
        == ()
    )


def test_missing_speed_is_not_fabricated():
    assert (
        decode_baseball_savant_sprint_speed_rows(
            (row(sprint_speed=None),)
        )
        == ()
    )


def test_invalid_speed_is_not_fabricated():
    assert (
        decode_baseball_savant_sprint_speed_rows(
            (
                row(sprint_speed="unknown"),
                row(
                    player_id=2,
                    sprint_speed=math.nan,
                ),
            )
        )
        == ()
    )


def test_duplicate_runner_identity_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "runner sprint-speed identifiers must "
            "be unique"
        ),
    ):
        decode_baseball_savant_sprint_speed_rows(
            (
                row(),
                row(sprint_speed=28.7),
            )
        )


def test_output_order_is_deterministic():
    observations = (
        decode_baseball_savant_sprint_speed_rows(
            (
                row(
                    player_id=222,
                    sprint_speed=27.0,
                ),
                row(
                    player_id=111,
                    sprint_speed=29.0,
                ),
            )
        )
    )

    assert tuple(
        value.runner_id
        for value in observations
    ) == (
        "111",
        "222",
    )


def test_non_mapping_row_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "each sprint-speed row must be a mapping"
        ),
    ):
        decode_baseball_savant_sprint_speed_rows(
            (object(),)
        )


def test_observation_rejects_invalid_speed():
    with pytest.raises(
        ValueError,
        match=(
            "sprint_speed_ft_per_second must be "
            "positive and finite"
        ),
    ):
        CanonicalRunnerSprintSpeedObservation(
            runner_id="runner",
            sprint_speed_ft_per_second=0.0,
        )


def test_normalization_rejects_invalid_speed():
    with pytest.raises(
        ValueError,
        match=(
            "sprint_speed_ft_per_second must be positive "
            "and finite"
        ),
    ):
        normalize_runner_sprint_speed(
            math.inf
        )
