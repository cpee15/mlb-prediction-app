import hashlib

import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_BASERUNNING_CALIBRATION_GRID_VERSION,
    build_historical_baserunning_calibration_candidates,
    build_historical_baserunning_replay_review_policy,
    evaluate_historical_baserunning_calibration_candidates,
)
from mlb_app.simulation.shadow.baserunning_calibration_comparison import (
    CanonicalBaserunningCalibrationComparison,
)


DIGEST = hashlib.sha256(b"baseline").hexdigest()


def baseline():
    return CanonicalBaserunningCalibrationComparison(
        status="ready",
        game_count=185,
        projected_stolen_bases=402.28,
        observed_stolen_bases=239,
        stolen_base_absolute_error=163.28,
        projected_caught_stealing=233.80,
        observed_caught_stealing=92,
        caught_stealing_absolute_error=141.80,
        projected_attempts=636.08,
        observed_attempts=331,
        attempt_absolute_error=305.08,
        projected_success_rate=0.632436,
        observed_success_rate=0.722054,
        success_rate_absolute_error=0.089618,
        observed_source_version="fixture",
    )


def evaluate():
    return evaluate_historical_baserunning_calibration_candidates(
        baseline=baseline(),
        baseline_evaluation_digest=DIGEST,
        policy=(
            build_historical_baserunning_replay_review_policy()
        ),
    )


def test_grid_selects_passing_shadow_candidate():
    result = evaluate()
    selected = result.selected_result

    assert result.passing_candidate_count > 0
    assert (
        selected.candidate.candidate_name
        == "attempt_0.520_success_plus_0.090"
    )
    assert selected.calibration_gate_passed is True
    assert selected.projected_attempts == 330.7616
    assert result.to_diagnostics()[
        "candidate_requires_replay_validation"
    ] is True
    assert result.to_diagnostics()[
        "eligible_for_activation_review"
    ] is False


def test_baseline_remains_failed_and_present():
    result = evaluate()
    baseline_result = next(
        value
        for value in result.results
        if (
            value.candidate.candidate_name
            == "baseline_unmodified"
        )
    )

    assert baseline_result.calibration_gate_passed is False
    assert (
        "attempt_error_per_game_exceeded"
        in baseline_result.failures
    )


def test_grid_is_deterministic():
    first = evaluate()
    second = evaluate()

    assert first == second
    assert first.digest == second.digest
    assert (
        first.to_diagnostics()
        == second.to_diagnostics()
    )


def test_candidate_grid_is_explicit():
    candidates = (
        build_historical_baserunning_calibration_candidates()
    )

    assert len(candidates) == 21
    assert len({
        value.candidate_name
        for value in candidates
    }) == 21
    assert (
        CANONICAL_HISTORICAL_BASERUNNING_CALIBRATION_GRID_VERSION
        == "canonical_historical_baserunning_calibration_grid_v1"
    )


def test_invalid_baseline_is_rejected():
    invalid = CanonicalBaserunningCalibrationComparison()

    with pytest.raises(
        ValueError,
        match="must be ready",
    ):
        evaluate_historical_baserunning_calibration_candidates(
            baseline=invalid,
            baseline_evaluation_digest=DIGEST,
            policy=(
                build_historical_baserunning_replay_review_policy()
            ),
        )
