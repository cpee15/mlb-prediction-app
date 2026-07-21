from __future__ import annotations

from mlb_app import model_projections


class DummySession:
    pass


def confirmed_offense_inputs():
    return {
        "source": "confirmed_lineup_player_splits",
        "profile_granularity": "lineup_average",
        "player_count_used": 9,
        "real_player_profile_count": 9,
        "fallback_player_count": 0,
        "lineup": [
            {
                "batter_id": 1000 + index,
                "has_player_split": True,
                "has_batter_aggregate": False,
                "simulation_inputs": {
                    "k_pct": 0.22,
                    "bb_pct": 0.09,
                    "batting_avg": 0.255,
                    "on_base_pct": 0.330,
                    "slugging_pct": 0.430,
                    "iso": 0.175,
                },
            }
            for index in range(1, 10)
        ],
    }


def test_confirmed_player_level_inputs_are_preserved(
    monkeypatch,
):
    confirmed = confirmed_offense_inputs()

    def unexpected_fallback(*args, **kwargs):
        raise AssertionError(
            "team fallback must not replace confirmed inputs"
        )

    monkeypatch.setattr(
        model_projections,
        "_team_split_inputs",
        unexpected_fallback,
    )

    result = (
        model_projections
        ._projection_offense_inputs(
            matchup={
                "away_offense_inputs": confirmed,
            },
            side="away",
            session=DummySession(),
            team_id=1,
            season=2026,
        )
    )

    assert result is confirmed
    assert len(result["lineup"]) == 9
    assert result[
        "profile_granularity"
    ] == "lineup_average"


def test_home_confirmed_inputs_use_home_key(
    monkeypatch,
):
    confirmed = confirmed_offense_inputs()

    monkeypatch.setattr(
        model_projections,
        "_team_split_inputs",
        lambda *args, **kwargs: {
            "source": "unexpected_fallback",
        },
    )

    result = (
        model_projections
        ._projection_offense_inputs(
            matchup={
                "home_offense_inputs": confirmed,
                "away_offense_inputs": {
                    "lineup": [{"batter_id": 999}],
                },
            },
            side="home",
            session=DummySession(),
            team_id=2,
            season=2026,
        )
    )

    assert result is confirmed


def test_missing_confirmed_inputs_use_team_fallback(
    monkeypatch,
):
    fallback = {
        "source": "team_splits",
        "lineup_source": (
            "team_splits_fallback_not_confirmed_lineup"
        ),
    }

    calls = []

    def fake_team_split_inputs(
        session,
        team_id,
        season,
    ):
        calls.append(
            (session, team_id, season)
        )
        return fallback

    monkeypatch.setattr(
        model_projections,
        "_team_split_inputs",
        fake_team_split_inputs,
    )

    session = DummySession()

    result = (
        model_projections
        ._projection_offense_inputs(
            matchup={},
            side="away",
            session=session,
            team_id=17,
            season=2026,
        )
    )

    assert result is fallback
    assert calls == [
        (session, 17, 2026),
    ]


def test_empty_lineup_does_not_claim_confirmed_handoff(
    monkeypatch,
):
    fallback = {
        "source": "team_splits",
    }

    monkeypatch.setattr(
        model_projections,
        "_team_split_inputs",
        lambda *args, **kwargs: fallback,
    )

    result = (
        model_projections
        ._projection_offense_inputs(
            matchup={
                "away_offense_inputs": {
                    "source": (
                        "confirmed_lineup_player_splits"
                    ),
                    "lineup": [],
                },
            },
            side="away",
            session=DummySession(),
            team_id=1,
            season=2026,
        )
    )

    assert result is fallback


def test_handoff_retains_exact_artifact_provenance_fields(
    monkeypatch,
):
    confirmed = confirmed_offense_inputs()

    monkeypatch.setattr(
        model_projections,
        "_team_split_inputs",
        lambda *args, **kwargs: {
            "source": "unexpected_fallback",
        },
    )

    result = (
        model_projections
        ._projection_offense_inputs(
            matchup={
                "away_offense_inputs": confirmed,
            },
            side="away",
            session=DummySession(),
            team_id=1,
            season=2026,
        )
    )

    first = result["lineup"][0]

    assert first["batter_id"] == 1001
    assert first["has_player_split"] is True
    assert (
        first["has_batter_aggregate"]
        is False
    )
    assert first["simulation_inputs"][
        "batting_avg"
    ] == 0.255
