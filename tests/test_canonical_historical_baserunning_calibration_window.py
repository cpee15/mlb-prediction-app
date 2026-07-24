from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_BASERUNNING_WINDOW_VERSION,
    CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION,
    CanonicalBaserunningCalibrationPolicy,
    CanonicalBaserunningOutputValidation,
    CanonicalHistoricalBaserunningShadowGame,
    execute_historical_baserunning_calibration_window,
)


def validation(
    *,
    digest,
    stolen_bases,
    caught_stealing,
):
    return CanonicalBaserunningOutputValidation(
        status="ready",
        simulation_count=100,
        catalog_digest=digest,
        runner_projection_count=9,
        stolen_base_mean_total=stolen_bases,
        caught_stealing_mean_total=(
            caught_stealing
        ),
    )


def shadow_game(
    *,
    game_pk,
    game_date,
    digest,
    stolen_bases,
    caught_stealing,
):
    return CanonicalHistoricalBaserunningShadowGame(
        game_pk=game_pk,
        game_date=game_date,
        validation=validation(
            digest=digest,
            stolen_bases=stolen_bases,
            caught_stealing=caught_stealing,
        ),
    )


def row(**overrides):
    value = {
        "game_pk": 1,
        "at_bat_number": 10,
        "pitch_number": 3,
        "pitcher": 100,
        "fielder_2": 200,
        "on_1b": 300,
        "on_2b": None,
        "on_3b": None,
        "des": (
            "Batter strikes out swinging. "
            "Runner steals (1) 2nd base."
        ),
    }
    value.update(overrides)
    return value


def policy(**overrides):
    arguments = {
        "minimum_game_count": 2,
        "maximum_stolen_base_error_per_game": 0.0,
        "maximum_caught_stealing_error_per_game": 0.0,
        "maximum_attempt_error_per_game": 0.0,
        "maximum_success_rate_absolute_error": 0.0,
        "policy_version": (
            "historical_smoke_policy_v1"
        ),
    }
    arguments.update(overrides)

    return CanonicalBaserunningCalibrationPolicy(
        **arguments
    )


def execute(**overrides):
    arguments = {
        "window_start": "2026-04-20",
        "window_end": "2026-05-03",
        "shadow_games": (
            shadow_game(
                game_pk=1,
                game_date="2026-04-20",
                digest="digest-a",
                stolen_bases=1.0,
                caught_stealing=1.0,
            ),
            shadow_game(
                game_pk=2,
                game_date="2026-04-21",
                digest="digest-b",
                stolen_bases=0.0,
                caught_stealing=0.0,
            ),
        ),
        "statcast_rows": (
            row(),
            row(
                at_bat_number=20,
                on_1b=301,
                des=(
                    "Batter strikes out swinging and "
                    "Runner caught stealing 2nd, "
                    "catcher."
                ),
            ),
            row(
                game_pk=2,
                at_bat_number=1,
                pitch_number=1,
                on_1b=None,
                des="Batter grounds out.",
            ),
        ),
        "policy": policy(),
        "observed_source_version": (
            CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION
        ),
    }
    arguments.update(overrides)

    return (
        execute_historical_baserunning_calibration_window(
            **arguments
        )
    )


def test_executes_complete_historical_window():
    result = execute()

    assert result.status == "ready"
    assert result.ready is True
    assert result.game_count == 2
    assert result.statcast_row_count == 3
    assert result.observed_outcome_count == 2
    assert result.artifact is not None
    assert result.calibration_gate_passed is True


def test_failed_gate_remains_ready_execution():
    result = execute(
        policy=policy(
            minimum_game_count=50,
        )
    )

    assert result.status == "ready"
    assert result.ready is True
    assert result.calibration_gate_passed is False
    assert result.artifact is not None
    assert result.artifact.report is not None
    assert result.artifact.report.gate is not None
    assert result.artifact.report.gate.failures == (
        "minimum_game_count_not_met",
    )


def test_zero_activity_game_is_included():
    result = execute()

    assert result.artifact is not None
    assert result.artifact.report is not None
    assert result.artifact.report.comparison is not None
    assert (
        result.artifact.report.comparison.game_count
        == 2
    )


def test_missing_statcast_game_coverage_fails_open():
    result = execute(
        statcast_rows=(
            row(),
            row(
                at_bat_number=20,
                on_1b=301,
                des=(
                    "Runner caught stealing 2nd, "
                    "catcher."
                ),
            ),
        )
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.artifact is None
    assert result.error_message == (
        "every historical shadow game must "
        "have Statcast row coverage"
    )


def test_unmatched_statcast_game_fails_open():
    result = execute(
        statcast_rows=(
            row(game_pk=3),
        )
    )

    assert result.status == "error"
    assert result.error_message == (
        "Statcast row game_pk must match "
        "a historical shadow game"
    )


def test_invalid_statcast_row_fails_open():
    result = execute(
        statcast_rows=(object(),)
    )

    assert result.status == "error"
    assert result.error_message == (
        "statcast_rows must contain mappings"
    )


def test_missing_game_pk_fails_open():
    value = row()
    del value["game_pk"]

    result = execute(
        statcast_rows=(value,)
    )

    assert result.status == "error"
    assert result.error_message == (
        "Statcast row requires game_pk"
    )


def test_duplicate_decoded_outcome_fails_open():
    duplicate = row()

    result = execute(
        shadow_games=(
            shadow_game(
                game_pk=1,
                game_date="2026-04-20",
                digest="digest-a",
                stolen_bases=1.0,
                caught_stealing=0.0,
            ),
        ),
        statcast_rows=(
            duplicate,
            duplicate,
        ),
        policy=policy(
            minimum_game_count=1,
        ),
    )

    assert result.status == "error"
    assert result.error_message == (
        "Statcast outcome identifiers must be unique"
    )


def test_diagnostics_preserve_legacy_authority():
    diagnostics = execute().to_diagnostics()

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
    first = execute()
    second = execute()

    assert first == second
    assert first.to_diagnostics() == second.to_diagnostics()


def test_execution_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_BASERUNNING_WINDOW_VERSION
        == "canonical_historical_baserunning_window_v1"
    )
