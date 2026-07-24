import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_BASERUNNING_CALIBRATION_GATE_VERSION,
    CanonicalBaserunningCalibrationComparison,
    CanonicalBaserunningCalibrationPolicy,
    evaluate_baserunning_calibration_gate,
)


def comparison(
    *,
    status="ready",
    game_count=100,
    stolen_base_error=10.0,
    caught_stealing_error=5.0,
    attempt_error=12.0,
    success_rate_error=0.03,
    error_message=None,
):
    return CanonicalBaserunningCalibrationComparison(
        status=status,
        game_count=game_count,
        stolen_base_absolute_error=stolen_base_error,
        caught_stealing_absolute_error=(
            caught_stealing_error
        ),
        attempt_absolute_error=attempt_error,
        success_rate_absolute_error=success_rate_error,
        error_message=error_message,
    )


def policy(
    *,
    minimum_game_count=50,
    maximum_stolen_base_error_per_game=0.20,
    maximum_caught_stealing_error_per_game=0.10,
    maximum_attempt_error_per_game=0.20,
    maximum_success_rate_absolute_error=0.05,
):
    return CanonicalBaserunningCalibrationPolicy(
        minimum_game_count=minimum_game_count,
        maximum_stolen_base_error_per_game=(
            maximum_stolen_base_error_per_game
        ),
        maximum_caught_stealing_error_per_game=(
            maximum_caught_stealing_error_per_game
        ),
        maximum_attempt_error_per_game=(
            maximum_attempt_error_per_game
        ),
        maximum_success_rate_absolute_error=(
            maximum_success_rate_absolute_error
        ),
        policy_version="offline_baserunning_policy_v1",
    )


def test_passing_evidence_satisfies_gate():
    result = evaluate_baserunning_calibration_gate(
        comparison(),
        policy(),
    )

    assert result.status == "ready"
    assert result.ready is True
    assert result.eligible is True
    assert result.calibration_gate_passed is True
    assert result.stolen_base_error_per_game == 0.1
    assert result.caught_stealing_error_per_game == 0.05
    assert result.attempt_error_per_game == 0.12
    assert result.success_rate_absolute_error == 0.03
    assert result.failures == ()


def test_minimum_sample_failure_is_explicit():
    result = evaluate_baserunning_calibration_gate(
        comparison(
            game_count=25,
            stolen_base_error=2.5,
            caught_stealing_error=1.25,
            attempt_error=3.0,
        ),
        policy(minimum_game_count=50),
    )

    assert result.status == "ready"
    assert result.eligible is False
    assert result.calibration_gate_passed is False
    assert result.failures == (
        "minimum_game_count_not_met",
    )


def test_each_error_threshold_is_evaluated():
    result = evaluate_baserunning_calibration_gate(
        comparison(
            stolen_base_error=30.0,
            caught_stealing_error=20.0,
            attempt_error=40.0,
            success_rate_error=0.10,
        ),
        policy(),
    )

    assert result.calibration_gate_passed is False
    assert result.failures == (
        "stolen_base_error_per_game_exceeded",
        "caught_stealing_error_per_game_exceeded",
        "attempt_error_per_game_exceeded",
        "success_rate_absolute_error_exceeded",
    )


def test_missing_success_rate_is_not_eligible():
    result = evaluate_baserunning_calibration_gate(
        comparison(success_rate_error=None),
        policy(),
    )

    assert result.eligible is False
    assert result.calibration_gate_passed is False
    assert result.failures == (
        "success_rate_error_unavailable",
    )


def test_unavailable_comparison_fails_open():
    result = evaluate_baserunning_calibration_gate(
        comparison(
            status="unavailable",
            game_count=0,
            stolen_base_error=0.0,
            caught_stealing_error=0.0,
            attempt_error=0.0,
            success_rate_error=None,
            error_message="comparison unavailable",
        ),
        policy(),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.error_message == "comparison unavailable"


def test_invalid_comparison_contract_fails_open():
    result = evaluate_baserunning_calibration_gate(
        object(),
        policy(),
    )

    assert result.status == "error"
    assert result.error_message == (
        "comparison must be "
        "CanonicalBaserunningCalibrationComparison"
    )


def test_invalid_policy_contract_fails_open():
    result = evaluate_baserunning_calibration_gate(
        comparison(),
        object(),
    )

    assert result.status == "error"
    assert result.error_message == (
        "policy must be "
        "CanonicalBaserunningCalibrationPolicy"
    )


def test_policy_requires_positive_sample():
    with pytest.raises(
        ValueError,
        match="minimum_game_count must be positive",
    ):
        policy(minimum_game_count=0)


def test_policy_rejects_invalid_limits():
    with pytest.raises(
        ValueError,
        match=(
            "maximum_attempt_error_per_game "
            "must be nonnegative and finite"
        ),
    ):
        policy(
            maximum_attempt_error_per_game=-0.1
        )

    with pytest.raises(
        ValueError,
        match=(
            "maximum_success_rate_absolute_error "
            "must not exceed one"
        ),
    ):
        policy(
            maximum_success_rate_absolute_error=1.1
        )


def test_diagnostics_never_permit_activation():
    diagnostics = evaluate_baserunning_calibration_gate(
        comparison(),
        policy(),
    ).to_diagnostics()

    assert diagnostics["calibration_gate_passed"] is True
    assert diagnostics["activation_permitted"] is False
    assert diagnostics[
        "production_authority_changed"
    ] is False
    assert diagnostics["authoritative_source"] == "legacy"


def test_gate_is_deterministic():
    first = evaluate_baserunning_calibration_gate(
        comparison(),
        policy(),
    )
    second = evaluate_baserunning_calibration_gate(
        comparison(),
        policy(),
    )

    assert first == second


def test_gate_version_is_explicit():
    result = evaluate_baserunning_calibration_gate(
        comparison(),
        policy(),
    )

    assert result.gate_version == (
        CANONICAL_BASERUNNING_CALIBRATION_GATE_VERSION
    )
