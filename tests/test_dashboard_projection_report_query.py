import pytest

from mlb_app import dashboard_projection_report_query as reports
from mlb_app.dashboard_report_types import describe_report_type


DATE = "2026-07-24"


def projection_payload():
    return {
        "date": DATE,
        "workspace_contract": "model_projection_workspace_v1",
        "probability_contract": "model_projection_probability_v1",
        "artifact": {"cache_key": f"model_projection:{DATE}"},
        "games": [
            {
                "game_pk": 10,
                "game_date": DATE,
                "game_time": "19:05",
                "status": "Preview",
                "venue": {"name": "Wrigley Field"},
                "away_team": {"id": 1, "name": "STL"},
                "home_team": {"id": 2, "name": "CHC"},
                "away_pitcher": {"id": 11, "name": "Away Arm"},
                "home_pitcher": {"id": 12, "name": "Home Arm"},
                "model_projection_probability": {
                    "away_win_probability": 0.42,
                    "home_win_probability": 0.58,
                    "model_version": "projection-v1",
                    "source": "model_projections",
                    "is_fallback": False,
                },
                "lineup_status": "confirmed",
                "data_confidence": "high",
                "sharedSimulation": {
                    "meta": {"simulation_count": 3000},
                    "direct_inputs": {
                        "away_pitcher_profile": {
                            "bat_missing": {"k_rate": 0.24},
                            "command_control": {"bb_rate": 0.08},
                            "contact_management": {"xwoba_allowed": 0.31},
                        },
                        "home_offense_profile": {
                            "contact_skill": {"k_rate": 0.19},
                            "plate_discipline": {"bb_rate": 0.1, "on_base_pct": 0.34},
                            "power": {"iso": 0.18, "slugging_pct": 0.44},
                        },
                        "environment_profile": {
                            "run_environment": {"run_scoring_index": 1.08, "hr_boost_index": 1.1},
                            "weather": {"temperature_f": 82, "wind_speed_mph": 12},
                        },
                    },
                    "derived_outputs": {
                        "bullpen_adjusted_game_simulation": {
                            "away_expected_runs": 4.1,
                            "home_expected_runs": 4.9,
                            "total_expected_runs": 9.0,
                            "tie_after_regulation_probability": 0.09,
                            "calibrated_total_probabilities": {"over_8.5": 0.54, "under_8.5": 0.46},
                        },
                    },
                    "diagnostics": {
                        "canonical_shadow": {
                            "player_projections": {
                                "simulation_count": 1000,
                                "players": [
                                    {
                                        "player_id": "101",
                                        "mlb_player_id": 101,
                                        "full_name": "Alpha Batter",
                                        "player_type": "batter",
                                        "team_side": "home",
                                        "team_id": 2,
                                        "team_name": "CHC",
                                        "primary_position": "OF",
                                        "projected_dfs_points": 12.5,
                                        "dfs_floor": 4.0,
                                        "dfs_median": 11.8,
                                        "dfs_ceiling": 23.0,
                                        "metrics": {
                                            "plate_appearances": {"mean": 4.4},
                                            "home_runs": {"mean": 0.3},
                                            "rbi": {"mean": 0.8},
                                        },
                                    },
                                ],
                            },
                        },
                    },
                },
            },
            {
                "game_pk": 20,
                "game_date": DATE,
                "away_team": {"id": 3, "name": "MIL"},
                "home_team": {"id": 4, "name": "CIN"},
                "model_projection_probability": {
                    "away_win_probability": 0.61,
                    "home_win_probability": 0.39,
                    "model_version": "projection-v1",
                    "source": "model_projections",
                    "is_fallback": True,
                },
                "lineup_status": "projected",
                "data_confidence": "medium",
                "sharedSimulation": {"derived_outputs": {}, "diagnostics": {}},
            },
        ],
    }


def test_projection_game_parent_filters_sorts_and_paginates(monkeypatch):
    monkeypatch.setattr(reports, "get_model_projection_payload", lambda date: projection_payload())
    result = reports.query_projection_report(
        "model_projection_games",
        date=DATE,
        filters={
            "logic": "or",
            "conditions": [
                {"field": "home_team_name", "operator": "eq", "value": "CHC"},
                {"field": "probability_is_fallback", "operator": "eq", "value": True},
            ],
        },
        sort_by="home_win_probability",
        page_size=1,
    )
    assert result["filter_logic"] == "or"
    assert result["totalSize"] == 2
    assert result["records"][0]["game_pk"] == 10
    assert result["records"][0]["projected_total"] == pytest.approx(9.0)
    assert result["records"][0]["away_starter_k_rate"] == pytest.approx(0.24)
    assert result["records"][0]["home_offense_obp"] == pytest.approx(0.34)
    assert result["records"][0]["run_scoring_index"] == pytest.approx(1.08)
    assert result["records"][0]["simulation_count"] == 3000
    assert result["records"][0]["over_8_5_probability"] == pytest.approx(0.54)
    assert result["page_info"]["has_next"] is True
    assert result["object_info"]["relationships"] == ["model_projection_players"]


def test_projection_player_child_uses_same_artifact_and_explicit_fields(monkeypatch):
    monkeypatch.setattr(reports, "get_model_projection_payload", lambda date: projection_payload())
    result = reports.query_projection_report(
        "model_projection_players",
        date=DATE,
        filters={
            "logic": "and",
            "conditions": [
                {"field": "player_type", "operator": "eq", "value": "batter"},
                {"field": "projected_dfs_points", "operator": "gte", "value": 10},
            ],
        },
        sort_by="projected_dfs_points",
    )
    assert result["totalSize"] == 1
    row = result["records"][0]
    assert row["full_name"] == "Alpha Batter"
    assert row["plate_appearances"] == pytest.approx(4.4)
    assert row["home_runs"] == pytest.approx(0.3)
    assert row["rbi"] == pytest.approx(0.8)
    assert result["query_source"] == "model_projection_date_artifact"


def test_projection_reports_reject_weights_and_unknown_fields(monkeypatch):
    monkeypatch.setattr(reports, "get_model_projection_payload", lambda date: projection_payload())
    with pytest.raises(ValueError, match="Weights are not supported"):
        reports.query_projection_report("model_projection_games", date=DATE, weights={"score": 2})
    with pytest.raises(ValueError, match="Unsupported filter field"):
        reports.query_projection_report(
            "model_projection_games",
            date=DATE,
            filters={"conditions": [{"field": "physical_table", "operator": "eq", "value": "games"}]},
        )


def test_projection_catalogs_are_parent_child_and_do_not_expose_nested_payloads():
    game = describe_report_type("model_projection_games")
    player = describe_report_type("model_projection_players")
    assert game["base_object"] == player["base_object"] == "model_projection_date_artifact"
    assert "model_projection_players" in game["relationships"]
    assert "model_projection_games" in player["relationships"]
    assert all(field["data_type"] != "json" for field in game["fields"] + player["fields"])
