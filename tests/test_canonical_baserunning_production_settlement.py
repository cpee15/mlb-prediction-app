import hashlib
from datetime import date

import pytest

from mlb_app.database import (
    CanonicalBaserunningProductionObservation,
    create_tables,
    get_engine,
    get_session,
)
from mlb_app.simulation.shadow import (
    CANONICAL_BASERUNNING_PRODUCTION_SETTLEMENT_VERSION,
    CanonicalMlbPlayByPlayBaserunningGame,
    build_canonical_baserunning_production_settlement,
    load_canonical_baserunning_production_settlements,
    materialize_canonical_baserunning_production_settlements,
    store_canonical_baserunning_production_settlement,
    summarize_canonical_baserunning_production_settlements,
    source_mlb_play_by_play_baserunning_window,
)


DIGEST = hashlib.sha256(b"monitoring").hexdigest()
SOURCE_DIGEST = hashlib.sha256(b"source").hexdigest()


def observation(
    *,
    game_pk=1,
    game_date="2026-07-26",
):
    return CanonicalBaserunningProductionObservation(
        game_pk=game_pk,
        game_date=date.fromisoformat(game_date),
        canonical_run_id="canonical-run",
        observation_digest=DIGEST,
        paired_context_digest=hashlib.sha256(
            b"context"
        ).hexdigest(),
        calibrated_transform_digest=hashlib.sha256(
            b"transform"
        ).hexdigest(),
        simulation_count=250,
        status="ready",
        ready=True,
        production_activation=True,
        authoritative_source=(
            "canonical_event_driven_calibrated_baserunning"
        ),
        payload_json={
            "schema_version": (
                "canonical_baserunning_"
                "production_monitoring_v1"
            ),
            "payload": {
                "observation": {
                    "calibrated_validation": {
                        "stolen_base_mean_total": 1.4,
                        "caught_stealing_mean_total": 0.3,
                    },
                },
            },
        },
    )


def observed(
    *,
    game_pk=1,
    game_date="2026-07-26",
    stolen_bases=2,
    caught_stealing=1,
):
    return CanonicalMlbPlayByPlayBaserunningGame(
        game_pk=game_pk,
        game_date=game_date,
        stolen_bases=stolen_bases,
        caught_stealing=caught_stealing,
    )


def record(**overrides):
    values = {
        "observation": observation(),
        "observed_game": observed(),
        "observed_source_digest": SOURCE_DIGEST,
        "final_status": "Final",
    }
    values.update(overrides)
    return build_canonical_baserunning_production_settlement(
        **values
    )


def session():
    engine = get_engine("sqlite:///:memory:")
    create_tables(engine)
    return get_session(engine)()


def test_builds_exact_per_game_comparison():
    result = record()

    assert result.projected_attempts == 1.7
    assert result.observed_attempts == 3
    assert result.comparison[
        "stolen_base_error"
    ] == -0.6
    assert result.comparison[
        "caught_stealing_error"
    ] == -0.7
    assert result.comparison[
        "attempt_absolute_error"
    ] == 1.3
    assert result.observed_success_rate == 0.666667
    assert len(result.digest) == 64


def test_zero_activity_game_is_valid_truth():
    result = record(
        observed_game=observed(
            stolen_bases=0,
            caught_stealing=0,
        )
    )

    assert result.observed_attempts == 0
    assert result.observed_success_rate is None


def test_identity_and_final_status_are_required():
    with pytest.raises(
        ValueError,
        match="final game status",
    ):
        record(final_status="Scheduled")

    with pytest.raises(
        ValueError,
        match="game_pk",
    ):
        record(
            observed_game=observed(game_pk=2)
        )

    with pytest.raises(
        ValueError,
        match="game_date",
    ):
        record(
            observed_game=observed(
                game_date="2026-07-27"
            )
        )


def test_store_is_idempotent():
    db = session()
    value = record()

    first, first_created = (
        store_canonical_baserunning_production_settlement(
            db,
            value,
        )
    )
    second, second_created = (
        store_canonical_baserunning_production_settlement(
            db,
            value,
        )
    )

    assert first.id == second.id
    assert first_created is True
    assert second_created is False
    assert len(
        load_canonical_baserunning_production_settlements(
            db
        )
    ) == 1


def test_conflicting_settlement_is_rejected():
    db = session()
    value = record()
    store_canonical_baserunning_production_settlement(
        db,
        value,
    )

    changed = record(
        observed_game=observed(stolen_bases=3)
    )

    with pytest.raises(
        ValueError,
        match="different settlement",
    ):
        store_canonical_baserunning_production_settlement(
            db,
            changed,
        )


def test_summary_tracks_settled_games_and_errors():
    db = session()
    value = record()
    store_canonical_baserunning_production_settlement(
        db,
        value,
    )
    rows = (
        load_canonical_baserunning_production_settlements(
            db
        )
    )
    summary = (
        summarize_canonical_baserunning_production_settlements(
            rows
        )
    )

    assert summary["settled_game_count"] == 1
    assert summary["remaining_game_count"] == 99
    assert summary["progress_rate"] == 0.01
    assert summary["stolen_base_bias"] == -0.6
    assert summary["stolen_base_mae"] == 0.6
    assert (
        summary["parameter_reselection_permitted"]
        is False
    )


def test_version_is_explicit():
    assert (
        CANONICAL_BASERUNNING_PRODUCTION_SETTLEMENT_VERSION
        == "canonical_baserunning_production_settlement_v1"
    )



def completed_snapshot(
    *,
    game_pk=1,
    game_date="2026-07-26",
    stolen_bases=2,
    caught_stealing=1,
):
    runners = []

    for index in range(stolen_bases):
        runners.append(
            {
                "details": {
                    "runner": {
                        "id": 100 + index,
                    },
                    "eventType": "stolen_base_2b",
                }
            }
        )

    for index in range(caught_stealing):
        runners.append(
            {
                "details": {
                    "runner": {
                        "id": 200 + index,
                    },
                    "eventType": "caught_stealing_2b",
                }
            }
        )

    return source_mlb_play_by_play_baserunning_window(
        schedule={
            "dates": [
                {
                    "date": game_date,
                    "games": [
                        {
                            "gamePk": game_pk,
                            "officialDate": game_date,
                            "status": {
                                "abstractGameState": "Final",
                            },
                        },
                    ],
                },
            ],
        },
        game_feeds={
            game_pk: {
                "liveData": {
                    "plays": {
                        "allPlays": [
                            {
                                "about": {
                                    "atBatIndex": 1,
                                },
                                "result": {
                                    "eventType": "",
                                },
                                "runners": runners,
                            },
                        ],
                    },
                },
            },
        },
        window_start=game_date,
        window_end=game_date,
    )


def persist_observation(
    db,
    value=None,
):
    value = value or observation()
    db.add(value)
    db.flush()
    return value


def test_batch_settles_matching_completed_game():
    db = session()
    persist_observation(db)

    result = (
        materialize_canonical_baserunning_production_settlements(
            db,
            observed=completed_snapshot(),
        )
    )

    assert result["created_game_ids"] == (1,)
    assert result["reused_game_ids"] == ()
    assert result["pending_game_ids"] == ()
    assert result["summary"][
        "settled_game_count"
    ] == 1


def test_batch_is_idempotent():
    db = session()
    persist_observation(db)
    snapshot = completed_snapshot()

    first = (
        materialize_canonical_baserunning_production_settlements(
            db,
            observed=snapshot,
        )
    )
    second = (
        materialize_canonical_baserunning_production_settlements(
            db,
            observed=snapshot,
        )
    )

    assert first["created_game_ids"] == (1,)
    assert second["created_game_ids"] == ()
    assert second["reused_game_ids"] == (1,)
    assert second["summary"][
        "settled_game_count"
    ] == 1


def test_batch_leaves_unobserved_monitoring_game_pending():
    db = session()
    pending = observation(
        game_pk=2,
        game_date="2026-07-27",
    )
    pending.observation_digest = hashlib.sha256(
        b"pending"
    ).hexdigest()

    persist_observation(db, observation())
    persist_observation(db, pending)

    result = (
        materialize_canonical_baserunning_production_settlements(
            db,
            observed=completed_snapshot(),
        )
    )

    assert result["created_game_ids"] == (1,)
    assert result["pending_game_ids"] == (2,)


def test_batch_reports_extraneous_completed_game():
    db = session()
    persist_observation(db)

    result = (
        materialize_canonical_baserunning_production_settlements(
            db,
            observed=completed_snapshot(
                game_pk=9,
            ),
        )
    )

    assert result["created_game_ids"] == ()
    assert result[
        "unmatched_observed_game_ids"
    ] == (9,)
    assert result["pending_game_ids"] == (1,)


def test_latest_monitoring_observation_wins_per_game():
    db = session()
    first = observation()
    second = observation()
    second.observation_digest = hashlib.sha256(
        b"second"
    ).hexdigest()
    second.canonical_run_id = "canonical-run-2"

    persist_observation(db, first)
    persist_observation(db, second)

    result = (
        materialize_canonical_baserunning_production_settlements(
            db,
            observed=completed_snapshot(),
        )
    )
    rows = (
        load_canonical_baserunning_production_settlements(
            db
        )
    )

    assert result["created_game_ids"] == (1,)
    assert len(rows) == 1
    assert rows[0].canonical_run_id == (
        "canonical-run-2"
    )


def test_game_can_only_be_settled_once():
    db = session()
    first = observation()
    persist_observation(db, first)

    first_record = record(
        observation=first,
    )
    store_canonical_baserunning_production_settlement(
        db,
        first_record,
    )

    second = observation()
    second.observation_digest = hashlib.sha256(
        b"second-game-observation"
    ).hexdigest()
    second.canonical_run_id = "canonical-run-2"

    conflicting = record(
        observation=second,
    )

    with pytest.raises(
        ValueError,
        match="different monitoring observation",
    ):
        store_canonical_baserunning_production_settlement(
            db,
            conflicting,
        )
