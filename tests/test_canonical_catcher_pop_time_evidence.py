import math

import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_CATCHER_POP_TIME_NORMALIZATION_VERSION,
    CANONICAL_CATCHER_POP_TIME_SOURCE_VERSION,
    CanonicalCatcherPopTimeObservation,
    decode_baseball_savant_catcher_pop_time_rows,
    normalize_catcher_pop_time,
)


def row(**overrides):
    value = {
        "player_id": 605170,
        "pop_2b_sba": 1.89,
    }
    value.update(overrides)
    return value


def test_decodes_complete_catcher_pop_time():
    observations = (
        decode_baseball_savant_catcher_pop_time_rows(
            (row(),)
        )
    )

    assert len(observations) == 1

    observation = observations[0]

    assert observation.catcher_id == "605170"
    assert observation.pop_time_seconds == 1.89
    assert observation.pop_time_score == 0.7
    assert observation.source_version == (
        CANONICAL_CATCHER_POP_TIME_SOURCE_VERSION
    )
    assert observation.normalization_version == (
        CANONICAL_CATCHER_POP_TIME_NORMALIZATION_VERSION
    )


def test_normalization_rewards_lower_pop_time():
    assert normalize_catcher_pop_time(2.10) == 0.0
    assert normalize_catcher_pop_time(2.00) == 0.333333
    assert normalize_catcher_pop_time(1.90) == 0.666667
    assert normalize_catcher_pop_time(1.80) == 1.0


def test_normalization_clamps_observed_extremes():
    assert normalize_catcher_pop_time(2.20) == 0.0
    assert normalize_catcher_pop_time(1.70) == 1.0


def test_missing_identity_is_not_fabricated():
    assert (
        decode_baseball_savant_catcher_pop_time_rows(
            (row(player_id=None),)
        )
        == ()
    )


def test_missing_pop_time_is_not_fabricated():
    assert (
        decode_baseball_savant_catcher_pop_time_rows(
            (row(pop_2b_sba=None),)
        )
        == ()
    )


def test_invalid_pop_time_is_not_fabricated():
    assert (
        decode_baseball_savant_catcher_pop_time_rows(
            (
                row(pop_2b_sba="unknown"),
                row(
                    player_id=2,
                    pop_2b_sba=math.nan,
                ),
            )
        )
        == ()
    )


def test_duplicate_catcher_identity_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "catcher pop-time identifiers must be unique"
        ),
    ):
        decode_baseball_savant_catcher_pop_time_rows(
            (
                row(),
                row(pop_2b_sba=1.91),
            )
        )


def test_output_order_is_deterministic():
    observations = (
        decode_baseball_savant_catcher_pop_time_rows(
            (
                row(
                    player_id=222,
                    pop_2b_sba=2.01,
                ),
                row(
                    player_id=111,
                    pop_2b_sba=1.88,
                ),
            )
        )
    )

    assert tuple(
        value.catcher_id
        for value in observations
    ) == (
        "111",
        "222",
    )


def test_non_mapping_row_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "each catcher pop-time row must be a mapping"
        ),
    ):
        decode_baseball_savant_catcher_pop_time_rows(
            (object(),)
        )


def test_observation_rejects_invalid_pop_time():
    with pytest.raises(
        ValueError,
        match=(
            "pop_time_seconds must be positive and finite"
        ),
    ):
        CanonicalCatcherPopTimeObservation(
            catcher_id="catcher",
            pop_time_seconds=0.0,
        )


def test_normalization_rejects_invalid_pop_time():
    with pytest.raises(
        ValueError,
        match=(
            "pop_time_seconds must be positive and finite"
        ),
    ):
        normalize_catcher_pop_time(
            math.inf
        )
