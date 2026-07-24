import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_CATCHER_BASERUNNING_EVIDENCE_VERSION,
    CanonicalCatcherBaserunningObservation,
    adapt_observed_catcher_baserunning_evidence,
)


def observation(**overrides):
    values = {
        "catcher_id": "catcher",
        "team_side": "home",
        "steal_attempts_against": 20,
        "caught_stealing": 6,
        "pop_time_score": 0.68,
    }
    values.update(overrides)

    return CanonicalCatcherBaserunningObservation(
        **values
    )


def test_observed_counts_produce_exact_throwing_score():
    source = observation()
    profile = (
        adapt_observed_catcher_baserunning_evidence(
            source
        )
    )

    assert source.throwing_score == 0.3

    assert profile.catcher_id == "catcher"
    assert profile.team_side == "home"
    assert profile.throwing_score == 0.3
    assert profile.pop_time_score == 0.68


def test_zero_attempts_produce_zero_throwing_score():
    source = observation(
        steal_attempts_against=0,
        caught_stealing=0,
    )

    assert source.throwing_score == 0.0


def test_caught_stealing_cannot_exceed_attempts():
    with pytest.raises(
        ValueError,
        match=(
            "caught stealing cannot exceed "
            "steal attempts against"
        ),
    ):
        observation(
            steal_attempts_against=1,
            caught_stealing=2,
        )


def test_fractional_attempt_counts_are_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "steal_attempts_against must be an integer"
        ),
    ):
        observation(
            steal_attempts_against=20.0,
        )


def test_invalid_team_side_is_rejected():
    with pytest.raises(
        ValueError,
        match="team_side must be away or home",
    ):
        observation(
            team_side="neutral",
        )


def test_invalid_pop_time_evidence_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "pop_time_score must be between 0 and 1"
        ),
    ):
        observation(
            pop_time_score=1.01,
        )


def test_observation_digest_is_deterministic():
    first = observation()
    second = observation()

    assert first.digest == second.digest
    assert len(first.digest) == 64

    changed = observation(
        caught_stealing=5,
    )

    assert changed.digest != first.digest


def test_non_observation_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "observation must be a "
            "CanonicalCatcherBaserunningObservation"
        ),
    ):
        adapt_observed_catcher_baserunning_evidence(
            object()
        )


def test_source_version_is_explicit():
    assert observation().source_version == (
        CANONICAL_CATCHER_BASERUNNING_EVIDENCE_VERSION
    )
