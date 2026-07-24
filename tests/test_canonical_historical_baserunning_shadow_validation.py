import pytest

import mlb_app.simulation.shadow.historical_baserunning_shadow_validation as target
from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_BASERUNNING_SHADOW_COLLECTION_VERSION,
    CANONICAL_MLB_PLAY_BY_PLAY_BASERUNNING_SOURCE_VERSION,
    CanonicalBaserunningOutputValidation,
    CanonicalHistoricalBaserunningExecutionGame,
    CanonicalMlbPlayByPlayBaserunningGame,
    CanonicalMlbPlayByPlayBaserunningSnapshot,
    CanonicalProductionShadowExecution,
    collect_historical_baserunning_shadow_validations,
)


def observed():
    return CanonicalMlbPlayByPlayBaserunningSnapshot(
        window_start="2026-04-20",
        window_end="2026-05-03",
        games=(
            CanonicalMlbPlayByPlayBaserunningGame(
                game_pk=1,
                game_date="2026-04-20",
                stolen_bases=1,
                caught_stealing=0,
            ),
            CanonicalMlbPlayByPlayBaserunningGame(
                game_pk=2,
                game_date="2026-04-21",
                stolen_bases=0,
                caught_stealing=1,
            ),
        ),
        event_count=2,
        stolen_bases=1,
        caught_stealing=1,
        duplicate_event_record_count=0,
        digest="a" * 64,
        source_version=(
            CANONICAL_MLB_PLAY_BY_PLAY_BASERUNNING_SOURCE_VERSION
        ),
    )


def execution_game(
    *,
    game_pk,
    game_date,
    status="executed",
):
    return CanonicalHistoricalBaserunningExecutionGame(
        game_pk=game_pk,
        game_date=game_date,
        execution=CanonicalProductionShadowExecution(
            status=status,
            simulation_count=100,
        ),
    )


def execution_games():
    return (
        execution_game(
            game_pk=2,
            game_date="2026-04-21",
        ),
        execution_game(
            game_pk=1,
            game_date="2026-04-20",
        ),
    )


def ready_validation(execution):
    return CanonicalBaserunningOutputValidation(
        status="ready",
        simulation_count=100,
        catalog_digest=(
            f"digest-{execution.status}"
        ),
        runner_projection_count=9,
        stolen_base_mean_total=1.0,
        caught_stealing_mean_total=0.25,
    )


def collect(monkeypatch, **overrides):
    monkeypatch.setattr(
        target,
        "validate_canonical_baserunning_shadow_outputs",
        ready_validation,
    )

    arguments = {
        "execution_games": execution_games(),
        "observed": observed(),
    }
    arguments.update(overrides)

    return (
        collect_historical_baserunning_shadow_validations(
            **arguments
        )
    )


def test_collects_aligned_ready_validations(
    monkeypatch,
):
    records = collect(monkeypatch)

    assert tuple(
        value.game_pk
        for value in records
    ) == (1, 2)
    assert all(
        value.validation.ready
        for value in records
    )


def test_exact_game_coverage_is_required(
    monkeypatch,
):
    with pytest.raises(
        ValueError,
        match=(
            "historical executions must exactly match "
            "observed play-by-play games"
        ),
    ):
        collect(
            monkeypatch,
            execution_games=(
                execution_games()[0],
            ),
        )


def test_duplicate_execution_game_is_rejected(
    monkeypatch,
):
    value = execution_games()[0]

    with pytest.raises(
        ValueError,
        match=(
            "historical execution game identifiers "
            "must be unique"
        ),
    ):
        collect(
            monkeypatch,
            execution_games=(value, value),
        )


def test_official_date_alignment_is_required(
    monkeypatch,
):
    values = list(execution_games())
    values[1] = execution_game(
        game_pk=1,
        game_date="2026-04-22",
    )

    with pytest.raises(
        ValueError,
        match=(
            "historical execution game_date must "
            "match observed official game_date"
        ),
    ):
        collect(
            monkeypatch,
            execution_games=tuple(values),
        )


def test_unavailable_validation_is_rejected(
    monkeypatch,
):
    def unavailable(execution):
        return CanonicalBaserunningOutputValidation(
            status="unavailable",
            error_message="catalog unavailable",
        )

    monkeypatch.setattr(
        target,
        "validate_canonical_baserunning_shadow_outputs",
        unavailable,
    )

    with pytest.raises(
        ValueError,
        match=(
            "historical baserunning shadow validation "
            "unavailable for game_pk 1: "
            "catalog unavailable"
        ),
    ):
        collect_historical_baserunning_shadow_validations(
            execution_games=execution_games(),
            observed=observed(),
        )


def test_default_validator_rejects_unexecuted_material():
    with pytest.raises(
        ValueError,
        match=(
            "historical baserunning shadow validation "
            "unavailable for game_pk 1"
        ),
    ):
        collect_historical_baserunning_shadow_validations(
            execution_games=execution_games(),
            observed=observed(),
        )


def test_invalid_execution_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "execution must be "
            "CanonicalProductionShadowExecution"
        ),
    ):
        CanonicalHistoricalBaserunningExecutionGame(
            game_pk=1,
            game_date="2026-04-20",
            execution=object(),
        )


def test_collection_is_deterministic(
    monkeypatch,
):
    first = collect(monkeypatch)
    second = collect(monkeypatch)

    assert first == second


def test_collection_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_BASERUNNING_SHADOW_COLLECTION_VERSION
        == "canonical_historical_baserunning_shadow_collection_v1"
    )
