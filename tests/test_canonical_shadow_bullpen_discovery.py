from __future__ import annotations

from mlb_app.simulation.shadow import (
    CANONICAL_SHADOW_BULLPEN_DISCOVERY_VERSION,
    discover_canonical_shadow_bullpens,
)


def active_roster(team_id, season, team_name=None):
    starter = 100 if team_id == 10 else 200

    return [
        {
            "mlb_player_id": starter,
            "player_type": "pitcher",
        },
        {
            "mlb_player_id": starter + 1,
            "player_type": "pitcher",
        },
        {
            "mlb_player_id": starter + 2,
            "player_type": "pitcher",
        },
        {
            "mlb_player_id": starter + 3,
            "player_type": "hitter",
        },
    ]


def discovery(**overrides):
    kwargs = {
        "away_team_id": 10,
        "away_team_name": "Away",
        "away_starter_id": 100,
        "home_team_id": 20,
        "home_team_name": "Home",
        "home_starter_id": 200,
        "season": 2026,
        "roster_fetcher": active_roster,
    }
    kwargs.update(overrides)

    return discover_canonical_shadow_bullpens(
        **kwargs
    )


def test_active_roster_pitchers_build_bullpen_candidates():
    result = discovery()

    assert result.status == "ready"
    assert result.ready is True
    assert result.away.bullpen_pitcher_ids == (
        "101",
        "102",
    )
    assert result.home.bullpen_pitcher_ids == (
        "201",
        "202",
    )


def test_scheduled_starters_are_excluded():
    result = discovery()

    assert "100" not in (
        result.away.bullpen_pitcher_ids
    )
    assert "200" not in (
        result.home.bullpen_pitcher_ids
    )


def test_readiness_fields_are_side_specific():
    fields = discovery().readiness_matchup_fields()

    assert len(
        fields["away_bullpen_pitcher_ids"]
    ) == 2
    assert len(
        fields["home_bullpen_pitcher_ids"]
    ) == 2


def test_diagnostics_do_not_expose_pitcher_ids():
    diagnostics = discovery().to_diagnostics()

    assert diagnostics["schema_version"] == (
        CANONICAL_SHADOW_BULLPEN_DISCOVERY_VERSION
    )
    assert diagnostics[
        "pitcher_identifiers_exposed"
    ] is False
    assert diagnostics["away"][
        "validated_pitcher_count"
    ] == 2
    assert "bullpen_pitcher_ids" not in (
        diagnostics["away"]
    )


def test_duplicate_and_non_pitcher_records_are_filtered():
    def roster(team_id, season, team_name=None):
        return [
            {
                "mlb_player_id": 100,
                "player_type": "pitcher",
            },
            {
                "mlb_player_id": 101,
                "player_type": "pitcher",
            },
            {
                "mlb_player_id": 101,
                "player_type": "pitcher",
            },
            {
                "mlb_player_id": 102,
                "player_type": "hitter",
            },
        ]

    result = discovery(
        home_starter_id=100,
        roster_fetcher=roster,
    )

    assert result.away.bullpen_pitcher_ids == (
        "101",
    )
    assert result.home.bullpen_pitcher_ids == (
        "101",
    )


def test_missing_team_id_blocks_only_that_side():
    result = discovery(
        away_team_id=None,
    )

    assert result.status == "partial"
    assert result.away.ready is False
    assert result.away.error_type == (
        "missing_team_id"
    )
    assert result.home.ready is True


def test_missing_starter_id_does_not_treat_roster_as_bullpen():
    result = discovery(
        away_starter_id=None,
    )

    assert result.away.ready is False
    assert result.away.error_type == (
        "missing_starter_id"
    )
    assert (
        "away_bullpen_pitcher_ids"
        not in result.readiness_matchup_fields()
    )


def test_roster_failure_fails_open():
    def failing_roster(
        team_id,
        season,
        team_name=None,
    ):
        raise RuntimeError("roster unavailable")

    result = discovery(
        roster_fetcher=failing_roster,
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.away.error_type == (
        "RuntimeError"
    )
    assert result.readiness_matchup_fields() == {}
