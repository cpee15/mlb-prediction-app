import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_RUNNER_BASERUNNING_EVIDENCE_VERSION,
    CanonicalRunnerBaserunningObservation,
    adapt_observed_runner_baserunning_evidence,
)


def observation(**overrides):
    values = {
        "runner_id": "runner",
        "eligible_opportunities": 20,
        "stolen_bases": 7,
        "caught_stealing": 1,
        "speed_score": 0.82,
        "lead_quality": 0.74,
        "fatigue_index": 0.12,
    }
    values.update(overrides)

    return CanonicalRunnerBaserunningObservation(
        **values
    )


def test_observed_counts_produce_exact_rates():
    source = observation()
    profile = (
        adapt_observed_runner_baserunning_evidence(
            source
        )
    )

    assert source.attempts == 8
    assert source.attempt_rate == 0.4
    assert source.success_rate == 0.875

    assert profile.runner_id == "runner"
    assert profile.attempt_rate == 0.4
    assert profile.success_rate == 0.875
    assert profile.speed_score == 0.82
    assert profile.lead_quality == 0.74
    assert profile.fatigue_index == 0.12


def test_zero_opportunities_produce_zero_rates():
    source = observation(
        eligible_opportunities=0,
        stolen_bases=0,
        caught_stealing=0,
    )

    assert source.attempt_rate == 0.0
    assert source.success_rate == 0.0


def test_attempts_cannot_exceed_eligible_opportunities():
    with pytest.raises(
        ValueError,
        match=(
            "attempts cannot exceed eligible opportunities"
        ),
    ):
        observation(
            eligible_opportunities=1,
            stolen_bases=1,
            caught_stealing=1,
        )


def test_plate_appearance_like_fractional_counts_are_rejected():
    with pytest.raises(
        TypeError,
        match="eligible_opportunities must be an integer",
    ):
        observation(
            eligible_opportunities=20.0,
        )


def test_invalid_rate_evidence_is_rejected():
    with pytest.raises(
        ValueError,
        match="speed_score must be between 0 and 1",
    ):
        observation(
            speed_score=1.01,
        )


def test_observation_digest_is_deterministic():
    first = observation()
    second = observation()

    assert first.digest == second.digest
    assert len(first.digest) == 64

    changed = observation(
        stolen_bases=6,
    )

    assert changed.digest != first.digest


def test_non_observation_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "observation must be a "
            "CanonicalRunnerBaserunningObservation"
        ),
    ):
        adapt_observed_runner_baserunning_evidence(
            object()
        )


def test_source_version_is_explicit():
    assert observation().source_version == (
        CANONICAL_RUNNER_BASERUNNING_EVIDENCE_VERSION
    )
