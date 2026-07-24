import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_MLB_PLAY_BY_PLAY_BASERUNNING_SOURCE_VERSION,
    source_mlb_play_by_play_baserunning_window,
)


def schedule():
    return {
        "dates": [
            {
                "date": "2026-04-20",
                "games": [
                    {
                        "gamePk": 1,
                        "status": {
                            "abstractGameState": "Final",
                        },
                    },
                    {
                        "gamePk": 2,
                        "status": {
                            "abstractGameState": "Live",
                        },
                    },
                ],
            },
            {
                "date": "2026-04-21",
                "games": [
                    {
                        "gamePk": 3,
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
            "event": event_type,
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


def source(**overrides):
    arguments = {
        "schedule": schedule(),
        "game_feeds": {
            1: feed(
                play(
                    index=1,
                    runners=(
                        runner(
                            runner_id=10,
                            event_type=(
                                "stolen_base_2b"
                            ),
                        ),
                        runner(
                            runner_id=11,
                            event_type=(
                                "stolen_base_3b"
                            ),
                        ),
                    ),
                ),
                play(
                    index=2,
                    runners=(
                        runner(
                            runner_id=12,
                            event_type=(
                                "caught_stealing_2b"
                            ),
                        ),
                    ),
                ),
            ),
            3: feed(
                play(
                    index=1,
                    runners=(),
                ),
            ),
        },
        "window_start": "2026-04-20",
        "window_end": "2026-05-03",
    }
    arguments.update(overrides)

    return (
        source_mlb_play_by_play_baserunning_window(
            **arguments
        )
    )


def test_sources_completed_games_and_events():
    result = source()

    assert result.game_count == 2
    assert result.event_count == 3
    assert result.stolen_bases == 2
    assert result.caught_stealing == 1
    assert tuple(
        value.game_pk
        for value in result.games
    ) == (1, 3)


def test_zero_activity_completed_game_is_preserved():
    result = source()

    assert result.games[1].stolen_bases == 0
    assert result.games[1].caught_stealing == 0


def test_pickoff_caught_stealing_is_counted():
    value = schedule()
    feeds = {
        1: feed(
            play(
                index=1,
                runners=(
                    runner(
                        runner_id=10,
                        event_type=(
                            "pickoff_caught_stealing_2b"
                        ),
                    ),
                ),
            ),
        ),
        3: feed(),
    }

    result = source(
        schedule=value,
        game_feeds=feeds,
    )

    assert result.stolen_bases == 0
    assert result.caught_stealing == 1


def test_incomplete_feed_coverage_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "game_feeds must exactly cover "
            "completed schedule games"
        ),
    ):
        source(
            game_feeds={
                1: feed(),
            }
        )


def test_duplicate_feed_event_is_deduplicated():
    value = runner(
        runner_id=10,
        event_type="stolen_base_2b",
    )

    result = source(
        game_feeds={
            1: feed(
                play(
                    index=1,
                    runners=(value, value),
                ),
            ),
            3: feed(),
        }
    )

    assert result.event_count == 1
    assert result.stolen_bases == 1
    assert result.caught_stealing == 0
    assert (
        result.duplicate_event_record_count
        == 1
    )
    assert result.to_diagnostics()[
        "duplicate_event_record_count"
    ] == 1


def test_digest_is_deterministic():
    first = source()
    second = source()

    assert first == second
    assert first.digest == second.digest


def test_diagnostics_mark_complete_observed_source():
    diagnostics = source().to_diagnostics()

    assert diagnostics["coverage_complete"] is True
    assert diagnostics[
        "calibration_observed_source_eligible"
    ] is True
    assert diagnostics["production_activation"] is False
    assert diagnostics["authoritative_source"] == "legacy"


def test_source_version_is_explicit():
    assert (
        CANONICAL_MLB_PLAY_BY_PLAY_BASERUNNING_SOURCE_VERSION
        == "canonical_mlb_play_by_play_baserunning_source_v1"
    )


def test_repeated_schedule_game_is_deduplicated():
    value = schedule()
    value["dates"].append(
        {
            "date": "2026-04-22",
            "games": [
                {
                    "gamePk": 1,
                    "officialDate": "2026-04-20",
                    "status": {
                        "abstractGameState": "Final",
                    },
                },
            ],
        }
    )
    value["dates"][0]["games"][0][
        "officialDate"
    ] = "2026-04-20"

    result = source(schedule=value)

    assert result.game_count == 2
    assert tuple(
        game.game_pk
        for game in result.games
    ) == (1, 3)


def test_conflicting_official_dates_are_rejected():
    value = schedule()
    value["dates"].append(
        {
            "date": "2026-04-22",
            "games": [
                {
                    "gamePk": 1,
                    "officialDate": "2026-04-22",
                    "status": {
                        "abstractGameState": "Final",
                    },
                },
            ],
        }
    )
    value["dates"][0]["games"][0][
        "officialDate"
    ] = "2026-04-20"

    with pytest.raises(
        ValueError,
        match=(
            "gamePk must map to one officialDate"
        ),
    ):
        source(schedule=value)
