from __future__ import annotations

from mlb_app.simulation.shadow import (
    build_canonical_shadow_bootstrap_readiness,
    discover_canonical_shadow_lineups,
)


def lineup(start):
    return [
        {
            "batter_id": start + index,
            "lineup_slot": index + 1,
        }
        for index in range(9)
    ]


def test_discovered_lineups_advance_bootstrap_readiness():
    discovery = discover_canonical_shadow_lineups(
        game_pk=123,
        lineup_fetcher=lambda game_pk: {
            "away": lineup(1000),
            "home": lineup(2000),
        },
    )

    matchup = {
        "game_pk": 123,
        "away_pitcher_id": 101,
        "home_pitcher_id": 201,
    }
    matchup.update(
        discovery.readiness_matchup_fields()
    )

    report = build_canonical_shadow_bootstrap_readiness(
        game_pk=123,
        matchup=matchup,
        away_context={},
        home_context={},
        workspace={},
    )

    assert report["requirements"][
        "away_lineup"
    ]["ready"] is True
    assert report["requirements"][
        "home_lineup"
    ]["ready"] is True
    assert report["requirements"][
        "away_lineup"
    ]["player_count"] == 9
    assert report["requirements"][
        "home_lineup"
    ]["player_count"] == 9

    assert report["missing_requirements"] == [
        "away_bullpen",
        "home_bullpen",
        "probability_provider",
        "exact_probability_artifact",
        "fallback_probability_catalog",
    ]


def test_incomplete_discovery_does_not_mark_lineup_ready():
    discovery = discover_canonical_shadow_lineups(
        game_pk=123,
        lineup_fetcher=lambda game_pk: {
            "away": lineup(1000)[:8],
            "home": [],
        },
    )

    matchup = {
        "game_pk": 123,
        "away_pitcher_id": 101,
        "home_pitcher_id": 201,
    }
    matchup.update(
        discovery.readiness_matchup_fields()
    )

    report = build_canonical_shadow_bootstrap_readiness(
        game_pk=123,
        matchup=matchup,
        away_context={},
        home_context={},
        workspace={},
    )

    assert report["requirements"][
        "away_lineup"
    ]["ready"] is False
    assert report["requirements"][
        "home_lineup"
    ]["ready"] is False
