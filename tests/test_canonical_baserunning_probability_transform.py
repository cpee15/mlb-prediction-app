import pytest

from mlb_app.simulation.game import (
    CANONICAL_BASERUNNING_PROBABILITY_TRANSFORM_VERSION,
    CanonicalBaserunningProbabilityTransform,
)
from mlb_app.simulation.game.baserunning_resolver import (
    CanonicalBaserunningEvidence,
)


def evidence():
    return CanonicalBaserunningEvidence(
        pitcher_id="100",
        attempt_probability=0.40,
        success_probability=0.65,
        probability_provenance="fixture",
    )


def test_selected_candidate_transforms_final_probabilities():
    transform = CanonicalBaserunningProbabilityTransform(
        attempt_probability_multiplier=0.52,
        success_rate_adjustment=0.09,
    )

    result = transform.apply(evidence())

    assert result.attempt_probability == 0.208
    assert result.success_probability == 0.74
    assert transform.is_identity is False
    assert transform.digest in (
        result.probability_provenance
    )


def test_identity_transform_preserves_probabilities():
    transform = CanonicalBaserunningProbabilityTransform()
    result = transform.apply(evidence())

    assert result.attempt_probability == 0.40
    assert result.success_probability == 0.65
    assert transform.is_identity is True


def test_transform_clamps_success_probability():
    transform = CanonicalBaserunningProbabilityTransform(
        success_rate_adjustment=0.50,
    )

    assert (
        transform.apply(evidence()).success_probability
        == 1.0
    )


def test_invalid_transform_is_rejected():
    with pytest.raises(
        ValueError,
        match="attempt_probability_multiplier",
    ):
        CanonicalBaserunningProbabilityTransform(
            attempt_probability_multiplier=1.01,
        )


def test_transform_version_is_explicit():
    assert (
        CANONICAL_BASERUNNING_PROBABILITY_TRANSFORM_VERSION
        == "canonical_baserunning_probability_transform_v1"
    )
