import math

import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_RUNNER_AVAILABILITY_EVIDENCE_VERSION,
    CanonicalRunnerAvailabilityObservation,
    decode_runner_availability_rows,
)


def observation(
    *,
    runner_id="runner",
    fatigue_index=0.20,
    injury_limit_flag=False,
    source_version="runner_availability_v1",
):
    return CanonicalRunnerAvailabilityObservation(
        runner_id=runner_id,
        fatigue_index=fatigue_index,
        injury_limit_flag=injury_limit_flag,
        source_version=source_version,
    )


def test_preserves_explicit_availability():
    value = observation()

    assert value.runner_id == "runner"
    assert value.fatigue_index == 0.20
    assert value.fatigue_score == 0.20
    assert value.injury_limit_flag is False
    assert value.source_version == (
        "runner_availability_v1"
    )


def test_preserves_explicit_injury_limitation():
    value = observation(
        fatigue_index=0.60,
        injury_limit_flag=True,
    )

    assert value.fatigue_score == 0.60
    assert value.injury_limit_flag is True


def test_fatigue_score_is_rounded_deterministically():
    value = observation(
        fatigue_index=0.12345678,
    )

    assert value.fatigue_score == 0.123457


def test_decoder_preserves_input_order():
    values = decode_runner_availability_rows(
        (
            {
                "runner_id": "second",
                "fatigue_index": 0.30,
                "injury_limit_flag": False,
                "source_version": "source_v1",
            },
            {
                "runner_id": "first",
                "fatigue_index": 0.10,
                "injury_limit_flag": True,
                "source_version": "source_v1",
            },
        )
    )

    assert tuple(
        value.runner_id
        for value in values
    ) == ("second", "first")


def test_empty_input_does_not_impute_availability():
    assert decode_runner_availability_rows(
        ()
    ) == ()


def test_missing_fatigue_is_rejected():
    with pytest.raises(
        ValueError,
        match="fatigue_index is required",
    ):
        decode_runner_availability_rows(
            (
                {
                    "runner_id": "runner",
                    "injury_limit_flag": False,
                    "source_version": "source_v1",
                },
            )
        )


def test_missing_injury_flag_is_rejected():
    with pytest.raises(
        ValueError,
        match="injury_limit_flag is required",
    ):
        decode_runner_availability_rows(
            (
                {
                    "runner_id": "runner",
                    "fatigue_index": 0.20,
                    "source_version": "source_v1",
                },
            )
        )


def test_non_boolean_injury_flag_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "injury_limit_flag must be boolean"
        ),
    ):
        decode_runner_availability_rows(
            (
                {
                    "runner_id": "runner",
                    "fatigue_index": 0.20,
                    "injury_limit_flag": 0,
                    "source_version": "source_v1",
                },
            )
        )


def test_missing_source_provenance_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "source_version must identify "
            "an available source"
        ),
    ):
        decode_runner_availability_rows(
            (
                {
                    "runner_id": "runner",
                    "fatigue_index": 0.20,
                    "injury_limit_flag": False,
                },
            )
        )


def test_duplicate_runner_identity_is_rejected():
    row = {
        "runner_id": "runner",
        "fatigue_index": 0.20,
        "injury_limit_flag": False,
        "source_version": "source_v1",
    }

    with pytest.raises(
        ValueError,
        match=(
            "runner availability identifiers "
            "must be unique"
        ),
    ):
        decode_runner_availability_rows(
            (row, row)
        )


def test_non_tuple_rows_are_rejected():
    with pytest.raises(
        TypeError,
        match="rows must be a tuple",
    ):
        decode_runner_availability_rows([])


def test_non_mapping_row_is_rejected():
    with pytest.raises(
        TypeError,
        match="rows must contain mappings",
    ):
        decode_runner_availability_rows(
            (object(),)
        )


@pytest.mark.parametrize(
    "fatigue_index",
    (
        -0.01,
        1.01,
        math.inf,
        math.nan,
    ),
)
def test_invalid_fatigue_is_rejected(
    fatigue_index,
):
    with pytest.raises(
        ValueError,
        match=(
            "fatigue_index must be finite and "
            "between 0 and 1"
        ),
    ):
        observation(
            fatigue_index=fatigue_index,
        )


def test_non_numeric_fatigue_is_rejected():
    with pytest.raises(
        TypeError,
        match="fatigue_index must be numeric",
    ):
        observation(
            fatigue_index="tired",
        )


def test_unavailable_source_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "source_version must identify "
            "an available source"
        ),
    ):
        observation(
            source_version="unavailable",
        )


def test_evidence_version_is_explicit():
    assert observation().evidence_version == (
        CANONICAL_RUNNER_AVAILABILITY_EVIDENCE_VERSION
    )


def test_observation_is_deterministic():
    assert observation() == observation()
