from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.database import Base
from mlb_app.simulation.shadow import (
    CANONICAL_BASERUNNING_PRODUCTION_AUTHORITY,
    CANONICAL_BASERUNNING_PRODUCTION_MONITORING_START_DATE,
    CANONICAL_BASERUNNING_PRODUCTION_MONITORING_TARGET,
    CANONICAL_BASERUNNING_PRODUCTION_MONITORING_VERSION,
    CanonicalBaserunningProductionMonitoringRecord,
    evaluate_canonical_production_monitoring_eligibility,
    load_canonical_baserunning_production_observations,
    materialize_canonical_baserunning_production_monitoring,
    store_canonical_baserunning_production_observation,
    summarize_canonical_baserunning_production_monitoring,
)


def session():
    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(
        bind=engine,
        future=True,
    )
    return factory()


def record(
    game_pk=1,
    game_date="2026-07-26",
    observation_digest="a" * 64,
):
    return CanonicalBaserunningProductionMonitoringRecord(
        game_pk=game_pk,
        game_date=game_date,
        canonical_run_id=f"canonical-{game_pk}",
        observation_digest=observation_digest,
        paired_context_digest="b" * 64,
        calibrated_transform_digest="c" * 64,
        simulation_count=250,
        status="ready",
        ready=True,
        production_activation=True,
        authoritative_source=(
            CANONICAL_BASERUNNING_PRODUCTION_AUTHORITY
        ),
        payload={
            "stolen_base_delta": 0.2,
            "caught_stealing_delta": -0.1,
        },
    )


def test_store_is_idempotent_by_observation_digest():
    db = session()
    first, first_created = (
        store_canonical_baserunning_production_observation(
            db,
            record(),
        )
    )
    second, second_created = (
        store_canonical_baserunning_production_observation(
            db,
            record(),
        )
    )

    assert first_created is True
    assert second_created is False
    assert first.id == second.id
    assert len(
        load_canonical_baserunning_production_observations(
            db
        )
    ) == 1


def test_summary_counts_unique_games():
    db = session()

    for game_pk in range(1, 4):
        store_canonical_baserunning_production_observation(
            db,
            record(
                game_pk=game_pk,
                observation_digest=(
                    f"{game_pk:064x}"
                ),
            ),
        )

    rows = (
        load_canonical_baserunning_production_observations(
            db
        )
    )
    summary = (
        summarize_canonical_baserunning_production_monitoring(
            rows
        )
    )

    assert summary["unique_game_count"] == 3
    assert summary["ready_game_count"] == 3
    assert summary["remaining_game_count"] == 97
    assert summary["progress_rate"] == 0.03
    assert summary["monitoring_complete"] is False
    assert summary["transform_frozen"] is True
    assert (
        summary["parameter_reselection_permitted"]
        is False
    )


def test_monitor_completes_at_100_unique_games():
    db = session()

    for game_pk in range(
        1,
        CANONICAL_BASERUNNING_PRODUCTION_MONITORING_TARGET
        + 1,
    ):
        store_canonical_baserunning_production_observation(
            db,
            record(
                game_pk=game_pk,
                observation_digest=(
                    f"{game_pk:064x}"
                ),
            ),
        )

    summary = (
        summarize_canonical_baserunning_production_monitoring(
            load_canonical_baserunning_production_observations(
                db
            )
        )
    )

    assert summary["ready_game_count"] == 100
    assert summary["remaining_game_count"] == 0
    assert summary["progress_rate"] == 1.0
    assert summary["monitoring_complete"] is True


def test_versions_are_explicit():
    assert (
        CANONICAL_BASERUNNING_PRODUCTION_MONITORING_VERSION
        == "canonical_baserunning_production_monitoring_v1"
    )
    assert (
        CANONICAL_BASERUNNING_PRODUCTION_MONITORING_TARGET
        == 100
    )


def eligibility(**overrides):
    values = {
        "game_date": "2026-07-26",
        "game_status": "Scheduled",
        "activation_requested": True,
        "production_activation": True,
        "selected_execution": "calibrated",
        "observation_ready": True,
        "input_parity_verified": True,
        "seed_parity_verified": True,
        "authoritative_source": (
            CANONICAL_BASERUNNING_PRODUCTION_AUTHORITY
        ),
    }
    values.update(overrides)

    return (
        evaluate_canonical_production_monitoring_eligibility(
            **values
        )
    )


def test_ready_pregame_production_run_is_eligible():
    result = eligibility()

    assert result["eligible"] is True
    assert result["failures"] == ()
    assert (
        result["monitoring_start_date"]
        == CANONICAL_BASERUNNING_PRODUCTION_MONITORING_START_DATE
    )


def test_historical_game_is_not_eligible():
    result = eligibility(game_date="2026-07-25")

    assert result["eligible"] is False
    assert (
        "inside_monitoring_window"
        in result["failures"]
    )


def test_final_and_live_games_are_not_eligible():
    for status in (
        "Game Over",
        "Final",
        "In Progress",
        "Delayed",
        "Postponed",
    ):
        result = eligibility(game_status=status)

        assert result["eligible"] is False
        assert "pregame_status" in result["failures"]


def test_fallback_and_parity_failures_are_not_eligible():
    fallback = eligibility(
        production_activation=False,
        selected_execution="legacy_fallback",
        authoritative_source="legacy",
    )
    parity = eligibility(
        input_parity_verified=False,
        seed_parity_verified=False,
    )

    assert fallback["eligible"] is False
    assert "production_activation" in fallback["failures"]
    assert "calibrated_selected" in fallback["failures"]
    assert "canonical_authority" in fallback["failures"]
    assert parity["eligible"] is False
    assert (
        "input_parity_verified"
        in parity["failures"]
    )
    assert "seed_parity_verified" in parity["failures"]


def test_materializer_records_only_eligible_runs():
    db = session()
    eligible = eligibility()
    skipped = eligibility(game_status="Game Over")

    recorded = (
        materialize_canonical_baserunning_production_monitoring(
            db,
            eligibility=eligible,
            record=record(),
        )
    )
    not_recorded = (
        materialize_canonical_baserunning_production_monitoring(
            db,
            eligibility=skipped,
        )
    )

    assert recorded["recorded"] is True
    assert recorded["record_created"] is True
    assert recorded["summary"]["unique_game_count"] == 1
    assert not_recorded["recorded"] is False
    assert not_recorded["record_created"] is False
    assert (
        not_recorded["summary"]["unique_game_count"]
        == 1
    )


def test_materializer_is_idempotent():
    db = session()
    candidate = record()

    first = (
        materialize_canonical_baserunning_production_monitoring(
            db,
            eligibility=eligibility(),
            record=candidate,
        )
    )
    second = (
        materialize_canonical_baserunning_production_monitoring(
            db,
            eligibility=eligibility(),
            record=candidate,
        )
    )

    assert first["record_created"] is True
    assert second["recorded"] is True
    assert second["record_created"] is False
    assert second["summary"]["stored_observation_count"] == 1
