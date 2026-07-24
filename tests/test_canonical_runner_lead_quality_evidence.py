import math

import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_RUNNER_LEAD_QUALITY_EVIDENCE_VERSION,
    CanonicalRunnerLeadQualityObservation,
    decode_runner_lead_quality_rows,
)


def observation(
    *,
    runner_id="runner",
    lead_quality=0.75,
    source_version="measured_runner_lead_v1",
):
    return CanonicalRunnerLeadQualityObservation(
        runner_id=runner_id,
        lead_quality=lead_quality,
        source_version=source_version,
    )


def test_preserves_explicit_lead_quality():
    value = observation()

    assert value.runner_id == "runner"
    assert value.lead_quality == 0.75
    assert value.lead_quality_score == 0.75
    assert value.source_version == (
        "measured_runner_lead_v1"
    )


def test_score_is_rounded_deterministically():
    value = observation(
        lead_quality=0.71234567,
    )

    assert value.lead_quality_score == 0.712346


def test_decoder_preserves_input_order():
    values = decode_runner_lead_quality_rows(
        (
            {
                "runner_id": "second",
                "lead_quality": 0.60,
                "source_version": "source_v1",
            },
            {
                "runner_id": "first",
                "lead_quality": 0.80,
                "source_version": "source_v1",
            },
        )
    )

    assert tuple(
        value.runner_id
        for value in values
    ) == ("second", "first")


def test_empty_input_does_not_impute_evidence():
    assert decode_runner_lead_quality_rows(
        ()
    ) == ()


def test_missing_lead_quality_is_rejected():
    with pytest.raises(
        ValueError,
        match="lead_quality is required",
    ):
        decode_runner_lead_quality_rows(
            (
                {
                    "runner_id": "runner",
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
        decode_runner_lead_quality_rows(
            (
                {
                    "runner_id": "runner",
                    "lead_quality": 0.70,
                },
            )
        )


def test_duplicate_runner_identity_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "runner lead-quality identifiers "
            "must be unique"
        ),
    ):
        decode_runner_lead_quality_rows(
            (
                {
                    "runner_id": "runner",
                    "lead_quality": 0.70,
                    "source_version": "source_v1",
                },
                {
                    "runner_id": "runner",
                    "lead_quality": 0.80,
                    "source_version": "source_v1",
                },
            )
        )


def test_non_tuple_rows_are_rejected():
    with pytest.raises(
        TypeError,
        match="rows must be a tuple",
    ):
        decode_runner_lead_quality_rows([])


def test_non_mapping_row_is_rejected():
    with pytest.raises(
        TypeError,
        match="rows must contain mappings",
    ):
        decode_runner_lead_quality_rows(
            (object(),)
        )


@pytest.mark.parametrize(
    "lead_quality",
    (
        -0.01,
        1.01,
        math.inf,
        math.nan,
    ),
)
def test_invalid_lead_quality_is_rejected(
    lead_quality,
):
    with pytest.raises(
        ValueError,
        match=(
            "lead_quality must be finite and "
            "between 0 and 1"
        ),
    ):
        observation(
            lead_quality=lead_quality,
        )


def test_non_numeric_lead_quality_is_rejected():
    with pytest.raises(
        TypeError,
        match="lead_quality must be numeric",
    ):
        observation(
            lead_quality="fast",
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
        CANONICAL_RUNNER_LEAD_QUALITY_EVIDENCE_VERSION
    )


def test_observation_is_deterministic():
    assert observation() == observation()
