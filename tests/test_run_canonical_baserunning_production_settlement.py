import hashlib
from datetime import date

from mlb_app.database import (
    CanonicalBaserunningProductionObservation,
    create_tables,
    get_engine,
    get_session,
)
from scripts.run_canonical_baserunning_production_settlement import (
    GAME_FEED_URL,
    filter_schedule_to_pending_final_games,
    run_canonical_baserunning_production_settlement,
)


def session():
    engine = get_engine("sqlite:///:memory:")
    create_tables(engine)
    return get_session(engine)()


def observation(
    *,
    game_pk=1,
    game_date="2026-07-26",
):
    digest = hashlib.sha256(
        f"observation-{game_pk}".encode()
    ).hexdigest()

    return CanonicalBaserunningProductionObservation(
        game_pk=game_pk,
        game_date=date.fromisoformat(game_date),
        canonical_run_id=f"run-{game_pk}",
        observation_digest=digest,
        paired_context_digest=hashlib.sha256(
            f"context-{game_pk}".encode()
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
                        "stolen_base_mean_total": 1.2,
                        "caught_stealing_mean_total": 0.2,
                    },
                },
            },
        },
    )


def schedule(
    *,
    game_pk=1,
    state="Final",
):
    return {
        "dates": [
            {
                "date": "2026-07-26",
                "games": [
                    {
                        "gamePk": game_pk,
                        "officialDate": "2026-07-26",
                        "status": {
                            "abstractGameState": state,
                        },
                    },
                ],
            },
        ],
    }


def feed():
    return {
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
                        "runners": [
                            {
                                "details": {
                                    "runner": {
                                        "id": 10,
                                    },
                                    "eventType": (
                                        "stolen_base_2b"
                                    ),
                                },
                            },
                        ],
                    },
                ],
            },
        },
    }


def test_filter_keeps_only_pending_final_games():
    value = {
        "dates": [
            {
                "date": "2026-07-26",
                "games": [
                    schedule(
                        game_pk=1
                    )["dates"][0]["games"][0],
                    schedule(
                        game_pk=2,
                        state="Live",
                    )["dates"][0]["games"][0],
                    schedule(
                        game_pk=3,
                    )["dates"][0]["games"][0],
                ],
            },
        ],
    }

    result = (
        filter_schedule_to_pending_final_games(
            value,
            pending_game_ids={1, 2},
        )
    )

    assert [
        game["gamePk"]
        for game in result["dates"][0]["games"]
    ] == [1]


def test_runner_settles_final_pending_game():
    db = session()
    db.add(observation())
    db.flush()
    urls = []

    def fetcher(url):
        urls.append(url)
        if "schedule" in url:
            return schedule()
        return feed()

    result = (
        run_canonical_baserunning_production_settlement(
            db,
            fetcher=fetcher,
        )
    )

    assert result["status"] == "settled"
    assert result["created_game_ids"] == (1,)
    assert result["pending_game_ids"] == ()
    assert len(urls) == 2
    assert urls[1] == GAME_FEED_URL.format(
        game_pk=1
    )


def test_runner_leaves_nonfinal_game_pending():
    db = session()
    db.add(observation())
    db.flush()

    result = (
        run_canonical_baserunning_production_settlement(
            db,
            fetcher=lambda url: schedule(
                state="Live"
            ),
        )
    )

    assert (
        result["status"]
        == "waiting_for_final_games"
    )
    assert result["pending_game_ids"] == (1,)
    assert result["summary"][
        "settled_game_count"
    ] == 0


def test_runner_without_pending_work_is_noop():
    db = session()
    calls = []

    result = (
        run_canonical_baserunning_production_settlement(
            db,
            fetcher=lambda url: calls.append(url),
        )
    )

    assert result["status"] == "complete"
    assert calls == []
    assert result["pending_observation_count"] == 0


def test_runner_is_idempotent():
    db = session()
    db.add(observation())
    db.flush()

    def fetcher(url):
        if "schedule" in url:
            return schedule()
        return feed()

    first = (
        run_canonical_baserunning_production_settlement(
            db,
            fetcher=fetcher,
        )
    )
    second = (
        run_canonical_baserunning_production_settlement(
            db,
            fetcher=fetcher,
        )
    )

    assert first["created_game_ids"] == (1,)
    assert second["status"] == "complete"
    assert second["summary"][
        "settled_game_count"
    ] == 1
