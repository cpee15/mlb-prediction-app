from mlb_app.canonical_game_context import build_canonical_game_context


def test_build_canonical_game_context_returns_shared_game_object():
    matchup = {
        "game_pk": 123,
        "game_date": "2026-05-24",
        "away_team_name": "Brewers",
        "home_team_name": "Cubs",
        "home_win_prob": 0.61,
        "away_win_prob": 0.39,
        "lineup_status": "confirmed",
        "data_confidence": "high",
        "missing_inputs": [],
        "probability_components": {
            "bullpen": {
                "source": "build_bullpen_profile",
                "available": True,
                "score": 0.53,
                "diagnostics": {
                    "home_bullpen_quality_score": 0.62,
                    "away_bullpen_quality_score": 0.55,
                },
            },
            "simulation": {
                "diagnostics": {
                    "home_expected_runs": 4.8,
                    "away_expected_runs": 3.9,
                    "total_expected_runs": 8.7,
                }
            },
        },
        "pitcher_overview": {
            "home": {"era": 3.20, "k_minus_bb_pct": 0.19, "xwoba_allowed": 0.298, "hard_hit_rate_allowed": 0.34, "innings_pitched": 72},
            "away": {"era": 4.10, "k_minus_bb_pct": 0.12, "xwoba_allowed": 0.325, "hard_hit_rate_allowed": 0.39, "innings_pitched": 68},
        },
        "home_pitcher_features": {"k_pct": 0.28, "bb_pct": 0.07, "xwoba": 0.301, "hard_hit_pct": 0.33, "avg_velocity": 94.1},
        "away_pitcher_features": {"k_pct": 0.22, "bb_pct": 0.09, "xwoba": 0.327, "hard_hit_pct": 0.40, "avg_velocity": 92.4},
        "home_offense_inputs": {"on_base_pct": 0.336, "slugging_pct": 0.431, "iso": 0.176, "bb_pct": 0.087, "k_pct": 0.221},
        "away_offense_inputs": {"on_base_pct": 0.317, "slugging_pct": 0.401, "iso": 0.154, "bb_pct": 0.073, "k_pct": 0.244},
    }
    game_context = build_canonical_game_context(matchup)
    assert game_context["game_pk"] == 123
    assert game_context["home_win_prob"] == 0.61
    assert game_context["projected_total_runs"] == 8.7
    assert game_context["starting_pitcher_component"]["home"]["pitcher_component_score"] is not None
    assert game_context["team_recent_form_component"]["home"]["team_recent_form_score"] is not None
    assert game_context["bullpen_component"]["bullpen_edge"] == 0.07
