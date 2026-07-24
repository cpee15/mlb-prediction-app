import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_BASERUNNING_CALIBRATION_INPUT_VERSION,
    CANONICAL_BASERUNNING_CALIBRATION_PAYLOAD_VERSION,
    CANONICAL_HISTORICAL_BASERUNNING_GAME_VERSION,
    CanonicalBaserunningCalibrationPolicy,
    CanonicalBaserunningOutputValidation,
    CanonicalHistoricalBaserunningGame,
    assemble_historical_baserunning_calibration_payload,
    execute_baserunning_calibration_artifact,
)


def validation(
    *,
    digest="digest",
    stolen_bases=1.0,
    caught_stealing=0.0,
):
    return CanonicalBaserunningOutputValidation(
        status="ready",
        simulation_count=100,
        catalog_digest=digest,
        runner_projection_count=9,
        stolen_base_mean_total=stolen_bases,
        caught_stealing_mean_total=caught_stealing,
    )


def game(
    *,
    game_pk=1,
    game_date="2026-04-20",
    digest="digest",
    predicted_stolen_bases=1.0,
    predicted_caught_stealing=0.0,
    observed_stolen_bases=1,
    observed_caught_stealing=0,
    observed_source_version="statcast_observed_v1",
):
    return CanonicalHistoricalBaserunningGame(
        game_pk=game_pk,
        game_date=game_date,
        validation=validation(
            digest=digest,
            stolen_bases=predicted_stolen_bases,
            caught_stealing=(
                predicted_caught_stealing
            ),
        ),
        observed_stolen_bases=(
            observed_stolen_bases
        ),
        observed_caught_stealing=(
            observed_caught_stealing
        ),
        observed_source_version=(
            observed_source_version
        ),
    )


def policy():
    return CanonicalBaserunningCalibrationPolicy(
        minimum_game_count=2,
        maximum_stolen_base_error_per_game=0.25,
        maximum_caught_stealing_error_per_game=0.25,
        maximum_attempt_error_per_game=0.25,
        maximum_success_rate_absolute_error=0.05,
        policy_version="offline_baserunning_policy_v1",
    )


def assemble(**overrides):
    arguments = {
        "window_start": "2026-04-20",
        "window_end": "2026-05-03",
        "games": (
            game(
                game_pk=2,
                game_date="2026-04-21",
                digest="digest-b",
                predicted_stolen_bases=2.0,
                predicted_caught_stealing=1.0,
                observed_stolen_bases=2,
                observed_caught_stealing=1,
            ),
            game(
                game_pk=1,
                game_date="2026-04-20",
                digest="digest-a",
            ),
        ),
        "policy": policy(),
    }
    arguments.update(overrides)

    return (
        assemble_historical_baserunning_calibration_payload(
            **arguments
        )
    )


def test_assembles_exact_artifact_input():
    payload = assemble()

    assert payload["schema_version"] == (
        CANONICAL_BASERUNNING_CALIBRATION_INPUT_VERSION
    )
    assert payload["window"] == {
        "start_date": "2026-04-20",
        "end_date": "2026-05-03",
    }
    assert payload["observed"] == {
        "game_count": 2,
        "stolen_bases": 3,
        "caught_stealing": 1,
        "source_version": "statcast_observed_v1",
    }
    assert [
        row["catalog_digest"]
        for row in payload["validations"]
    ] == [
        "digest-a",
        "digest-b",
    ]


def test_payload_executes_complete_artifact():
    artifact = execute_baserunning_calibration_artifact(
        assemble()
    )

    assert artifact.status == "ready"
    assert artifact.ready is True
    assert artifact.calibration_gate_passed is True


def test_duplicate_game_identifiers_are_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "historical game identifiers "
            "must be unique"
        ),
    ):
        assemble(
            games=(
                game(game_pk=1),
                game(
                    game_pk=1,
                    game_date="2026-04-21",
                ),
            )
        )


def test_out_of_window_game_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "historical game date must fall "
            "within calibration window"
        ),
    ):
        assemble(
            games=(
                game(
                    game_pk=1,
                    game_date="2026-04-19",
                ),
            )
        )


def test_unavailable_validation_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "historical game validation "
            "must be ready"
        ),
    ):
        CanonicalHistoricalBaserunningGame(
            game_pk=1,
            game_date="2026-04-20",
            validation=(
                CanonicalBaserunningOutputValidation()
            ),
            observed_stolen_bases=0,
            observed_caught_stealing=0,
            observed_source_version=(
                "statcast_observed_v1"
            ),
        )


def test_mixed_observed_sources_are_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "observed source versions "
            "must be identical"
        ),
    ):
        assemble(
            games=(
                game(game_pk=1),
                game(
                    game_pk=2,
                    game_date="2026-04-21",
                    observed_source_version=(
                        "another_source_v1"
                    ),
                ),
            )
        )


def test_invalid_game_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "games must contain "
            "CanonicalHistoricalBaserunningGame"
        ),
    ):
        assemble(
            games=(object(),)
        )


def test_empty_game_window_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "games must contain historical records"
        ),
    ):
        assemble(games=())


def test_assembly_is_deterministic():
    first = assemble()
    second = assemble()

    assert first == second


def test_versions_are_explicit():
    assert (
        CANONICAL_HISTORICAL_BASERUNNING_GAME_VERSION
        == "canonical_historical_baserunning_game_v1"
    )
    assert (
        CANONICAL_BASERUNNING_CALIBRATION_PAYLOAD_VERSION
        == "canonical_baserunning_calibration_payload_v1"
    )
