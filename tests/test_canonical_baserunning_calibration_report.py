from mlb_app.simulation.shadow import (
    CANONICAL_BASERUNNING_CALIBRATION_REPORT_VERSION,
    CanonicalBaserunningCalibrationPolicy,
    CanonicalBaserunningOutputValidation,
    CanonicalObservedBaserunningTotals,
    assemble_baserunning_calibration_report,
)


def validation(
    *,
    digest="digest",
    stolen_bases=2.0,
    caught_stealing=0.5,
):
    return CanonicalBaserunningOutputValidation(
        status="ready",
        simulation_count=100,
        catalog_digest=digest,
        runner_projection_count=9,
        stolen_base_mean_total=stolen_bases,
        caught_stealing_mean_total=caught_stealing,
    )


def observed(
    *,
    game_count=2,
    stolen_bases=4,
    caught_stealing=1,
):
    return CanonicalObservedBaserunningTotals(
        game_count=game_count,
        stolen_bases=stolen_bases,
        caught_stealing=caught_stealing,
        source_version="statcast_observed_v1",
    )


def policy(
    *,
    minimum_game_count=2,
    maximum_stolen_base_error_per_game=0.25,
    maximum_caught_stealing_error_per_game=0.25,
    maximum_attempt_error_per_game=0.25,
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


def assemble(**overrides):
    arguments = {
        "validations": (
            validation(digest="digest-a"),
            validation(digest="digest-b"),
        ),
        "observed": observed(),
        "policy": policy(),
    }
    arguments.update(overrides)

    return assemble_baserunning_calibration_report(
        **arguments
    )


def test_assembles_passing_calibration_report():
    report = assemble()

    assert report.status == "ready"
    assert report.ready is True
    assert report.summary is not None
    assert report.summary.ready_count == 2
    assert report.comparison is not None
    assert report.comparison.projected_attempts == 5.0
    assert report.comparison.observed_attempts == 5
    assert report.gate is not None
    assert report.gate.calibration_gate_passed is True
    assert report.calibration_gate_passed is True


def test_failed_gate_remains_ready_diagnostic_evidence():
    report = assemble(
        policy=policy(
            minimum_game_count=50,
        )
    )

    assert report.status == "ready"
    assert report.ready is True
    assert report.gate is not None
    assert report.gate.eligible is False
    assert report.calibration_gate_passed is False
    assert report.gate.failures == (
        "minimum_game_count_not_met",
    )


def test_empty_validations_stop_at_summary():
    report = assemble(
        validations=(),
    )

    assert report.status == "unavailable"
    assert report.ready is False
    assert report.summary is not None
    assert report.comparison is None
    assert report.gate is None
    assert report.error_message == (
        "no baserunning shadow validations were supplied"
    )


def test_invalid_validation_stops_at_summary():
    report = assemble(
        validations=(object(),),
    )

    assert report.status == "error"
    assert report.summary is not None
    assert report.comparison is None
    assert report.gate is None


def test_unaligned_observed_totals_stop_at_comparison():
    report = assemble(
        observed=observed(game_count=3),
    )

    assert report.status == "unavailable"
    assert report.summary is not None
    assert report.comparison is not None
    assert report.gate is None
    assert report.error_message == (
        "observed game_count must match "
        "ready shadow validation count"
    )


def test_invalid_observed_contract_stops_at_comparison():
    report = assemble(
        observed=object(),
    )

    assert report.status == "error"
    assert report.comparison is not None
    assert report.gate is None
    assert report.error_message == (
        "observed must be "
        "CanonicalObservedBaserunningTotals"
    )


def test_invalid_policy_contract_stops_at_gate():
    report = assemble(
        policy=object(),
    )

    assert report.status == "error"
    assert report.summary is not None
    assert report.comparison is not None
    assert report.gate is not None
    assert report.error_message == (
        "policy must be "
        "CanonicalBaserunningCalibrationPolicy"
    )


def test_diagnostics_preserve_all_stages():
    diagnostics = assemble().to_diagnostics()

    assert diagnostics["summary"]["ready"] is True
    assert diagnostics["comparison"]["ready"] is True
    assert diagnostics["gate"]["ready"] is True
    assert diagnostics["calibration_gate_passed"] is True


def test_diagnostics_never_permit_activation():
    diagnostics = assemble().to_diagnostics()

    assert diagnostics["activation_permitted"] is False
    assert diagnostics[
        "production_authority_changed"
    ] is False
    assert diagnostics["authoritative_source"] == "legacy"


def test_report_is_deterministic():
    first = assemble()
    second = assemble()

    assert first == second
    assert first.to_diagnostics() == second.to_diagnostics()


def test_report_version_is_explicit():
    report = assemble()

    assert report.report_version == (
        CANONICAL_BASERUNNING_CALIBRATION_REPORT_VERSION
    )
