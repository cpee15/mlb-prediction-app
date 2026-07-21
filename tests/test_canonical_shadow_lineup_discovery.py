from __future__ import annotations

from mlb_app.simulation.shadow import (
    CANONICAL_SHADOW_LINEUP_DISCOVERY_VERSION,
    discover_canonical_shadow_lineups,
)


def lineup(prefix):
    return [
        {
            "batter_id": 1000 + index,
            "name": f"{prefix} Player {index}",
            "lineup_slot": index + 1,
        }
        for index in range(9)
    ]


def test_complete_confirmed_lineups_are_ready():
    result = discover_canonical_shadow_lineups(
        game_pk=123,
        lineup_fetcher=lambda game_pk: {
            "away": lineup("Away"),
            "home": lineup("Home"),
        },
    )

    assert result.status == "ready"
    assert result.ready is True
    assert result.away_ready is True
    assert result.home_ready is True

    fields = result.readiness_matchup_fields()

    assert len(fields["away_lineup"]) == 9
    assert len(fields["home_lineup"]) == 9


def test_diagnostics_do_not_expose_player_ids():
    result = discover_canonical_shadow_lineups(
        game_pk=123,
        lineup_fetcher=lambda game_pk: {
            "away": lineup("Away"),
            "home": lineup("Home"),
        },
    )

    diagnostics = result.to_diagnostics()

    assert diagnostics["schema_version"] == (
        CANONICAL_SHADOW_LINEUP_DISCOVERY_VERSION
    )
    assert diagnostics[
        "player_identifiers_exposed"
    ] is False
    assert "away_player_ids" not in diagnostics
    assert "home_player_ids" not in diagnostics
    assert diagnostics["away"][
        "validated_player_count"
    ] == 9


def test_incomplete_side_is_not_forwarded():
    result = discover_canonical_shadow_lineups(
        game_pk=123,
        lineup_fetcher=lambda game_pk: {
            "away": lineup("Away")[:8],
            "home": lineup("Home"),
        },
    )

    assert result.status == "partial"
    assert result.ready is False
    assert result.away_ready is False
    assert result.home_ready is True

    fields = result.readiness_matchup_fields()

    assert "away_lineup" not in fields
    assert len(fields["home_lineup"]) == 9


def test_duplicate_ids_do_not_satisfy_nine_players():
    duplicate_lineup = [
        {
            "batter_id": 999,
            "lineup_slot": index + 1,
        }
        for index in range(9)
    ]

    result = discover_canonical_shadow_lineups(
        game_pk=123,
        lineup_fetcher=lambda game_pk: {
            "away": duplicate_lineup,
            "home": lineup("Home"),
        },
    )

    assert result.away_ready is False
    assert len(result.away_player_ids) == 1


def test_unavailable_lineups_fail_open():
    result = discover_canonical_shadow_lineups(
        game_pk=123,
        lineup_fetcher=lambda game_pk: {
            "away": [],
            "home": [],
        },
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.readiness_matchup_fields() == {}


def test_fetch_failure_returns_error_diagnostics():
    def failing_fetcher(game_pk):
        raise RuntimeError("boxscore unavailable")

    result = discover_canonical_shadow_lineups(
        game_pk=123,
        lineup_fetcher=failing_fetcher,
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.error_type == "RuntimeError"
    assert result.error_message == (
        "boxscore unavailable"
    )
    assert result.readiness_matchup_fields() == {}


def test_missing_game_pk_does_not_call_fetcher():
    calls = []

    def fetcher(game_pk):
        calls.append(game_pk)
        return {}

    result = discover_canonical_shadow_lineups(
        game_pk=None,
        lineup_fetcher=fetcher,
    )

    assert result.status == "blocked"
    assert result.error_type == "missing_game_pk"
    assert calls == []
