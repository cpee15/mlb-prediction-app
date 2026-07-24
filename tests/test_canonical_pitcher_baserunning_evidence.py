import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_PITCHER_BASERUNNING_EVIDENCE_VERSION,
    CanonicalPitcherBaserunningObservation,
    adapt_observed_pitcher_baserunning_evidence,
)


def observation(**overrides):
    values = {
        "pitcher_id": "pitcher",
        "eligible_pickoff_opportunities": 40,
        "pickoff_attempts": 8,
        "successful_pickoffs": 2,
        "hold_score": 0.72,
        "delivery_time_score": 0.38,
    }
    values.update(overrides)

    return CanonicalPitcherBaserunningObservation(
        **values
    )


def test_observed_counts_produce_exact_rates():
    source = observation()
    profile = (
        adapt_observed_pitcher_baserunning_evidence(
            source
        )
    )

    assert source.pickoff_attempt_rate == 0.2
    assert source.pickoff_success_rate == 0.25

    assert profile.pitcher_id == "pitcher"
    assert profile.pickoff_attempt_rate == 0.2
    assert profile.pickoff_success_rate == 0.25
    assert profile.hold_score == 0.72
    assert profile.delivery_time_score == 0.38


def test_zero_opportunities_produce_zero_rates():
    source = observation(
        eligible_pickoff_opportunities=0,
        pickoff_attempts=0,
        successful_pickoffs=0,
    )

    assert source.pickoff_attempt_rate == 0.0
    assert source.pickoff_success_rate == 0.0


def test_attempts_cannot_exceed_eligible_opportunities():
    with pytest.raises(
        ValueError,
        match=(
            "pickoff attempts cannot exceed "
            "eligible opportunities"
        ),
    ):
        observation(
            eligible_pickoff_opportunities=1,
            pickoff_attempts=2,
            successful_pickoffs=0,
        )


def test_successes_cannot_exceed_attempts():
    with pytest.raises(
        ValueError,
        match=(
            "successful pickoffs cannot exceed attempts"
        ),
    ):
        observation(
            pickoff_attempts=1,
            successful_pickoffs=2,
        )


def test_fractional_opportunity_counts_are_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "eligible_pickoff_opportunities "
            "must be an integer"
        ),
    ):
        observation(
            eligible_pickoff_opportunities=40.0,
        )


def test_invalid_hold_evidence_is_rejected():
    with pytest.raises(
        ValueError,
        match="hold_score must be between 0 and 1",
    ):
        observation(
            hold_score=-0.01,
        )


def test_observation_digest_is_deterministic():
    first = observation()
    second = observation()

    assert first.digest == second.digest
    assert len(first.digest) == 64

    changed = observation(
        pickoff_attempts=7,
    )

    assert changed.digest != first.digest


def test_non_observation_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "observation must be a "
            "CanonicalPitcherBaserunningObservation"
        ),
    ):
        adapt_observed_pitcher_baserunning_evidence(
            object()
        )


def test_source_version_is_explicit():
    assert observation().source_version == (
        CANONICAL_PITCHER_BASERUNNING_EVIDENCE_VERSION
    )
