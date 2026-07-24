import math

import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_PITCHER_DELIVERY_TIME_NORMALIZATION_VERSION,
    CANONICAL_PITCHER_DELIVERY_TIME_SOURCE_VERSION,
    FAST_DELIVERY_TIME_SECONDS,
    SLOW_DELIVERY_TIME_SECONDS,
    CanonicalPitcherDeliveryTimeObservation,
    decode_pitcher_delivery_time_rows,
    normalize_pitcher_delivery_time,
)


def observation(
    *,
    pitcher_id="pitcher",
    seconds=1.30,
):
    return CanonicalPitcherDeliveryTimeObservation(
        pitcher_id=pitcher_id,
        seconds_to_plate=seconds,
    )


def test_normalizes_explicit_seconds_to_plate():
    assert normalize_pitcher_delivery_time(
        FAST_DELIVERY_TIME_SECONDS
    ) == 1.0
    assert normalize_pitcher_delivery_time(
        SLOW_DELIVERY_TIME_SECONDS
    ) == 0.0
    assert normalize_pitcher_delivery_time(
        1.30
    ) == 0.75
    assert normalize_pitcher_delivery_time(
        1.40
    ) == 0.5


def test_normalization_clamps_outside_bounds():
    assert normalize_pitcher_delivery_time(
        1.00
    ) == 1.0
    assert normalize_pitcher_delivery_time(
        1.80
    ) == 0.0


def test_observation_exposes_raw_and_normalized_values():
    value = observation()

    assert value.pitcher_id == "pitcher"
    assert value.seconds_to_plate == 1.30
    assert value.delivery_time_score == 0.75
    assert value.source_version == (
        CANONICAL_PITCHER_DELIVERY_TIME_SOURCE_VERSION
    )
    assert value.normalization_version == (
        CANONICAL_PITCHER_DELIVERY_TIME_NORMALIZATION_VERSION
    )


def test_decodes_explicit_measurement_rows():
    values = decode_pitcher_delivery_time_rows(
        (
            {
                "pitcher_id": 100,
                "seconds_to_plate": 1.28,
                "source_version": (
                    "measured_delivery_time_feed_v1"
                ),
            },
            {
                "pitcher_id": "200",
                "seconds_to_plate": "1.44",
                "source_version": (
                    "measured_delivery_time_feed_v1"
                ),
            },
        )
    )

    assert tuple(
        value.pitcher_id
        for value in values
    ) == ("100", "200")
    assert values[0].seconds_to_plate == 1.28
    assert values[0].source_version == (
        "measured_delivery_time_feed_v1"
    )


def test_duplicate_pitcher_measurements_are_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "pitcher delivery-time identifiers "
            "must be unique"
        ),
    ):
        decode_pitcher_delivery_time_rows(
            (
                {
                    "pitcher_id": "pitcher",
                    "seconds_to_plate": 1.30,
                },
                {
                    "pitcher_id": "pitcher",
                    "seconds_to_plate": 1.35,
                },
            )
        )


def test_missing_measurement_is_not_fabricated():
    with pytest.raises(
        ValueError,
        match="seconds_to_plate is required",
    ):
        decode_pitcher_delivery_time_rows(
            (
                {
                    "pitcher_id": "pitcher",
                },
            )
        )


def test_non_mapping_row_is_rejected():
    with pytest.raises(
        TypeError,
        match="rows must contain mappings",
    ):
        decode_pitcher_delivery_time_rows(
            (object(),)
        )


def test_non_tuple_rows_are_rejected():
    with pytest.raises(
        TypeError,
        match="rows must be a tuple",
    ):
        decode_pitcher_delivery_time_rows(
            []
        )


def test_invalid_numeric_type_is_rejected():
    with pytest.raises(
        TypeError,
        match="seconds_to_plate must be numeric",
    ):
        normalize_pitcher_delivery_time(
            object()
        )


@pytest.mark.parametrize(
    "value",
    (
        0.0,
        -1.0,
        math.inf,
        math.nan,
    ),
)
def test_invalid_measurement_is_rejected(value):
    with pytest.raises(
        ValueError,
        match=(
            "seconds_to_plate must be positive and finite"
        ),
    ):
        normalize_pitcher_delivery_time(value)


def test_source_version_is_required():
    with pytest.raises(
        ValueError,
        match=(
            "source_version must identify "
            "an available source"
        ),
    ):
        CanonicalPitcherDeliveryTimeObservation(
            pitcher_id="pitcher",
            seconds_to_plate=1.30,
            source_version="unavailable",
        )


def test_observation_is_deterministic():
    assert observation() == observation()
