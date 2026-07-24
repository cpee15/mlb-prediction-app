import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_PLAY_BY_PLAY_BASERUNNING_MATERIALIZATION_VERSION,
    CanonicalBaserunningCalibrationPolicy,
    CanonicalBaserunningOutputValidation,
    CanonicalHistoricalBaserunningShadowGame,
    assemble_historical_baserunning_calibration_payload,
    execute_baserunning_calibration_artifact,
    materialize_play_by_play_baserunning_game_records,
    source_mlb_play_by_play_baserunning_window,
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


def schedule():
    return {
        "dates": [
            {
                "date": "2026-04-20",
                "games": [
                    {
                        "gamePk": 1,
                        "officialDate": "2026-04-20",
                        "status": {
                            "abstractGameState": "Final",
                        },
                    },
                ],
            },
            {
                "date": "2026-04-21",
                "games": [
                    {
                        "gamePk": 2,
                        "officialDate": "2026-04-21",
                        "status": {
                            "abstractGameState": "Final",
                        },
                    },
                ],
            },
        ],
    }


def runner(
    *,
    runner_id,
    event_type,
):
    return {
        "movement": {
            "start": "1B",
            "end": "2B",
            "isOut": (
                "caught_stealing"
                in event_type
            ),
        },
        "details": {
            "eventType": event_type,
            "runner": {
                "id": runner_id,
            },
        },
    }


def feed(*plays):
    return {
        "liveData": {
            "plays": {
                "allPlays": list(plays),
            },
        },
    }


def play(
    *,
    index,
    runners,
):
    return {
        "about": {
            "atBatIndex": index,
        },
        "result": {
            "eventType": "",
        },
        "runners": list(runners),
    }


def observed():
    return source_mlb_play_by_play_baserunning_window(
        schedule=schedule(),
        game_feeds={
            1: feed(
                play(
                    index=1,
                    runners=(
                        runner(
                            runner_id=10,
                            event_type="stolen_base_2b",
                        ),
                        runner(
                            runner_id=11,
                            event_type=(
                                "caught_stealing_2b"
                            ),
                        ),
                    ),
                ),
            ),
            2: feed(),
        },
        window_start="2026-04-20",
        window_end="2026-05-03",
    )


def shadow_games():
    return (
        shadow_game(
            game_pk=2,
            game_date="2026-04-21",
            digest="digest-b",
            stolen_bases=0.0,
            caught_stealing=0.0,
        ),
        shadow_game(
            game_pk=1,
            game_date="2026-04-20",
            digest="digest-a",
            stolen_bases=1.0,
            caught_stealing=1.0,
        ),
    )


def materialize(**overrides):
    arguments = {
        "shadow_games": shadow_games(),
        "observed": observed(),
    }
    arguments.update(overrides)

    return (
        materialize_play_by_play_baserunning_game_records(
            **arguments
        )
    )


def test_materializes_official_per_game_totals():
    records = materialize()

    assert tuple(
        value.game_pk
        for value in records
    ) == (1, 2)
    assert records[0].observed_stolen_bases == 1
    assert records[0].observed_caught_stealing == 1
    assert records[1].observed_stolen_bases == 0
    assert records[1].observed_caught_stealing == 0
    assert records[0].observed_source_version == (
        observed().source_version
    )


def test_materialized_records_execute_artifact():
    records = materialize()
    policy = CanonicalBaserunningCalibrationPolicy(
        minimum_game_count=2,
        maximum_stolen_base_error_per_game=0.0,
        maximum_caught_stealing_error_per_game=0.0,
        maximum_attempt_error_per_game=0.0,
        maximum_success_rate_absolute_error=0.0,
        policy_version="smoke_policy_v1",
    )
    payload = (
        assemble_historical_baserunning_calibration_payload(
            window_start="2026-04-20",
            window_end="2026-05-03",
            games=records,
            policy=policy,
        )
    )
    artifact = execute_baserunning_calibration_artifact(
        payload
    )

    assert artifact.status == "ready"
    assert artifact.calibration_gate_passed is True


def test_missing_shadow_game_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "shadow games must exactly match "
            "observed play-by-play games"
        ),
    ):
        materialize(
            shadow_games=(
                shadow_games()[0],
            )
        )


def test_extra_shadow_game_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "shadow games must exactly match "
            "observed play-by-play games"
        ),
    ):
        materialize(
            shadow_games=(
                *shadow_games(),
                shadow_game(
                    game_pk=3,
                    game_date="2026-04-22",
                    digest="digest-c",
                    stolen_bases=0.0,
                    caught_stealing=0.0,
                ),
            )
        )


def test_mismatched_official_date_is_rejected():
    games = list(shadow_games())
    games[1] = shadow_game(
        game_pk=1,
        game_date="2026-04-22",
        digest="digest-a",
        stolen_bases=1.0,
        caught_stealing=1.0,
    )

    with pytest.raises(
        ValueError,
        match=(
            "shadow game_date must match "
            "observed official game_date"
        ),
    ):
        materialize(
            shadow_games=tuple(games)
        )


def test_duplicate_shadow_game_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "historical shadow game identifiers "
            "must be unique"
        ),
    ):
        materialize(
            shadow_games=(
                shadow_games()[0],
                shadow_games()[0],
            )
        )


def test_invalid_observed_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "observed must be "
            "CanonicalMlbPlayByPlayBaserunningSnapshot"
        ),
    ):
        materialize(observed=object())


def test_materialization_is_deterministic():
    first = materialize()
    second = materialize()

    assert first == second


def test_materialization_version_is_explicit():
    assert (
        CANONICAL_PLAY_BY_PLAY_BASERUNNING_MATERIALIZATION_VERSION
        == "canonical_play_by_play_baserunning_materialization_v1"
    )
