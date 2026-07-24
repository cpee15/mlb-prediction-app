import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_LINEUP_BULLPEN_SOURCE_VERSION,
    HISTORICAL_BULLPEN_SOURCE,
    HISTORICAL_LINEUP_SOURCE,
    source_historical_lineup_bullpen_snapshots,
)
from mlb_app.simulation.shadow.mlb_play_by_play_baserunning_source import (
    CanonicalMlbPlayByPlayBaserunningGame,
    CanonicalMlbPlayByPlayBaserunningSnapshot,
)


def observed():
    return CanonicalMlbPlayByPlayBaserunningSnapshot(
        window_start="2026-04-20",
        window_end="2026-04-21",
        games=(
            CanonicalMlbPlayByPlayBaserunningGame(
                game_pk=2,
                game_date="2026-04-21",
                stolen_bases=0,
                caught_stealing=1,
            ),
            CanonicalMlbPlayByPlayBaserunningGame(
                game_pk=1,
                game_date="2026-04-20",
                stolen_bases=1,
                caught_stealing=0,
            ),
        ),
        event_count=2,
        stolen_bases=1,
        caught_stealing=1,
        duplicate_event_record_count=0,
        digest="f" * 64,
    )


def team(base):
    players = {}

    for slot in range(1, 10):
        player_id = base + slot
        players[f"ID{player_id}"] = {
            "person": {"id": player_id},
            "battingOrder": str(slot * 100),
        }

    substitute_id = base + 90
    players[f"ID{substitute_id}"] = {
        "person": {"id": substitute_id},
        "battingOrder": "101",
    }

    return {
        "players": players,
        "pitchers": [
            base + 40,
            base + 41,
        ],
        "bullpen": [
            {"id": base + 42},
            {"person": {"id": base + 41}},
            {"id": base + 40},
        ],
    }


def feed(game_date):
    return {
        "gameData": {
            "datetime": {
                "officialDate": game_date,
            },
        },
        "liveData": {
            "boxscore": {
                "teams": {
                    "away": team(1000),
                    "home": team(2000),
                },
            },
        },
    }


def source(feeds=None):
    return source_historical_lineup_bullpen_snapshots(
        observed=observed(),
        game_feeds=(
            feeds
            if feeds is not None
            else {
                2: feed("2026-04-21"),
                1: feed("2026-04-20"),
            }
        ),
    )


def test_complete_archived_feeds_are_ready():
    result = source()

    assert result.ready is True
    assert result.game_count == 2
    assert result.ready_game_count == 2
    assert tuple(
        value.game_pk
        for value in result.games
    ) == (1, 2)

    first = result.games[0]
    assert first.away_lineup_ids == tuple(
        str(1000 + slot)
        for slot in range(1, 10)
    )
    assert first.away_bullpen_ids == (
        "1041",
        "1042",
    )
    assert len(first.lineup_digest) == 64
    assert len(first.bullpen_digest) == 64


def test_substitutes_are_not_starting_lineup_members():
    first = source().games[0]

    assert "1090" not in first.away_lineup_ids
    assert len(first.away_lineup_ids) == 9


def test_probable_starter_is_excluded_from_bullpen():
    first = source().games[0]

    assert "1040" not in first.away_bullpen_ids
    assert first.away_bullpen_ids == (
        "1041",
        "1042",
    )


def test_missing_explicit_bullpen_fails_closed():
    first_feed = feed("2026-04-20")
    del first_feed[
        "liveData"
    ]["boxscore"]["teams"]["away"]["bullpen"]

    result = source(
        {
            1: first_feed,
            2: feed("2026-04-21"),
        }
    )

    first = result.games[0]
    assert first.ready is False
    assert first.lineups_ready is True
    assert first.bullpens_ready is False
    assert first.bullpen_digest is None
    assert result.ready is False


def test_used_pitchers_do_not_replace_missing_bullpen():
    first_feed = feed("2026-04-20")
    first_feed[
        "liveData"
    ]["boxscore"]["teams"]["away"]["bullpen"] = []

    first = source(
        {
            1: first_feed,
            2: feed("2026-04-21"),
        }
    ).games[0]

    assert first.away_bullpen_ids == ()
    assert first.bullpens_ready is False


def test_snapshot_converts_to_audit_evidence():
    first = source().games[0]
    evidence = first.to_replay_input_evidence()

    assert evidence.lineup_source == (
        HISTORICAL_LINEUP_SOURCE
    )
    assert evidence.bullpen_source == (
        HISTORICAL_BULLPEN_SOURCE
    )
    assert evidence.lineups_ready is True
    assert evidence.bullpens_ready is True
    assert (
        evidence.probability_provider_ready
        is False
    )


def test_game_feed_coverage_must_be_exact():
    with pytest.raises(
        ValueError,
        match=(
            "historical game feeds must exactly match "
            "observed play-by-play games"
        ),
    ):
        source(
            {
                1: feed("2026-04-20"),
            }
        )


def test_official_date_must_match_observed_date():
    with pytest.raises(
        ValueError,
        match=(
            "historical game feed officialDate must "
            "match observed game_date"
        ),
    ):
        source(
            {
                1: feed("2026-04-21"),
                2: feed("2026-04-21"),
            }
        )


def test_source_is_deterministic():
    first = source(
        {
            2: feed("2026-04-21"),
            1: feed("2026-04-20"),
        }
    )
    second = source(
        {
            1: feed("2026-04-20"),
            2: feed("2026-04-21"),
        }
    )

    assert first == second
    assert first.digest == second.digest


def test_diagnostics_hide_player_identifiers():
    diagnostics = source().to_diagnostics()
    first = diagnostics["games"][0]

    assert first["player_identifiers_exposed"] is False
    assert first["current_active_roster_used"] is False
    assert "away_lineup_ids" not in first
    assert "away_bullpen_ids" not in first
    assert diagnostics["historical_replay_executed"] is False
    assert diagnostics["production_activation"] is False
    assert (
        diagnostics["production_authority_changed"]
        is False
    )
    assert diagnostics["authoritative_source"] == "legacy"


def test_source_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_LINEUP_BULLPEN_SOURCE_VERSION
        == "canonical_historical_lineup_bullpen_source_v1"
    )
