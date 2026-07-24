from mlb_app.simulation.shadow import (
    CANONICAL_BASERUNNING_CALIBRATION_ARTIFACT_VERSION,
    CANONICAL_BASERUNNING_CALIBRATION_INPUT_VERSION,
    execute_baserunning_calibration_artifact,
)


def validation(digest):
    return {
        "status": "ready",
        "simulation_count": 100,
        "catalog_digest": digest,
        "runner_projection_count": 9,
        "stolen_base_mean_total": 2.0,
        "caught_stealing_mean_total": 0.5,
        "warnings": [],
        "error_message": None,
    }


def payload(**overrides):
    value = {
        "schema_version": (
            CANONICAL_BASERUNNING_CALIBRATION_INPUT_VERSION
        ),
        "window": {
            "start_date": "2026-04-20",
            "end_date": "2026-05-03",
        },
        "validations": [
            validation("digest-a"),
            validation("digest-b"),
        ],
        "observed": {
            "game_count": 2,
            "stolen_bases": 4,
            "caught_stealing": 1,
            "source_version": "statcast_observed_v1",
        },
        "policy": {
            "minimum_game_count": 2,
            "maximum_stolen_base_error_per_game": 0.25,
            "maximum_caught_stealing_error_per_game": 0.25,
            "maximum_attempt_error_per_game": 0.25,
            "maximum_success_rate_absolute_error": 0.05,
            "policy_version": (
                "offline_baserunning_policy_v1"
            ),
        },
    }
    value.update(overrides)
    return value


def test_executes_complete_artifact():
    result = execute_baserunning_calibration_artifact(
        payload()
    )

    assert result.status == "ready"
    assert result.ready is True
    assert result.window_start == "2026-04-20"
    assert result.window_end == "2026-05-03"
    assert result.report is not None
    assert result.calibration_gate_passed is True


def test_failed_gate_remains_ready_artifact():
    value = payload()
    value["policy"]["minimum_game_count"] = 50

    result = execute_baserunning_calibration_artifact(
        value
    )

    assert result.status == "ready"
    assert result.ready is True
    assert result.calibration_gate_passed is False
    assert result.report is not None
    assert result.report.gate is not None
    assert result.report.gate.failures == (
        "minimum_game_count_not_met",
    )


def test_unaligned_observed_window_is_unavailable():
    value = payload()
    value["observed"]["game_count"] = 3

    result = execute_baserunning_calibration_artifact(
        value
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.report is not None
    assert result.error_message == (
        "observed game_count must match "
        "ready shadow validation count"
    )


def test_unsupported_input_version_fails_open():
    result = execute_baserunning_calibration_artifact(
        payload(schema_version="unsupported")
    )

    assert result.status == "error"
    assert result.report is None
    assert result.error_message == (
        "unsupported baserunning calibration "
        "input version"
    )


def test_invalid_window_fails_open():
    value = payload()
    value["window"] = {
        "start_date": "2026-05-03",
        "end_date": "2026-04-20",
    }

    result = execute_baserunning_calibration_artifact(
        value
    )

    assert result.status == "error"
    assert result.error_message == (
        "window.end_date must not precede "
        "window.start_date"
    )


def test_invalid_date_fails_open():
    value = payload()
    value["window"]["start_date"] = "not-a-date"

    result = execute_baserunning_calibration_artifact(
        value
    )

    assert result.status == "error"
    assert result.report is None


def test_non_mapping_payload_fails_open():
    result = execute_baserunning_calibration_artifact(
        object()
    )

    assert result.status == "error"
    assert result.error_message == (
        "payload must be a mapping"
    )


def test_invalid_validation_fails_open():
    value = payload()
    value["validations"] = [object()]

    result = execute_baserunning_calibration_artifact(
        value
    )

    assert result.status == "error"
    assert result.error_message == (
        "validation must be a mapping"
    )


def test_missing_observed_field_fails_open():
    value = payload()
    del value["observed"]["stolen_bases"]

    result = execute_baserunning_calibration_artifact(
        value
    )

    assert result.status == "error"
    assert result.error_message == "'stolen_bases'"


def test_diagnostics_are_offline_only():
    diagnostics = (
        execute_baserunning_calibration_artifact(
            payload()
        ).to_diagnostics()
    )

    assert diagnostics[
        "external_fetch_performed"
    ] is False
    assert diagnostics["persistence_performed"] is False
    assert diagnostics["activation_permitted"] is False
    assert diagnostics[
        "production_authority_changed"
    ] is False
    assert diagnostics["authoritative_source"] == "legacy"


def test_execution_is_deterministic():
    first = execute_baserunning_calibration_artifact(
        payload()
    )
    second = execute_baserunning_calibration_artifact(
        payload()
    )

    assert first == second
    assert first.to_diagnostics() == second.to_diagnostics()


def test_artifact_version_is_explicit():
    result = execute_baserunning_calibration_artifact(
        payload()
    )

    assert result.artifact_version == (
        CANONICAL_BASERUNNING_CALIBRATION_ARTIFACT_VERSION
    )
