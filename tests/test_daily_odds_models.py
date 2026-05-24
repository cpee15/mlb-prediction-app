from mlb_app.daily_odds_models import build_moneyline_model, build_prop_models


def test_moneyline_model_includes_ev_confidence_tier_and_recommendation_status():
    matchup = {
        "home_win_prob": 0.61,
        "away_win_prob": 0.39,
        "model_version": "canonical_matchup_win_probability_v2",
        "lineup_status": "confirmed",
        "data_confidence": "high",
        "probability_components": {"starter_matchup": {"score": 0.58}},
        "home_team_name": "Cubs",
        "away_team_name": "Brewers",
    }
    market = {
        "market_key": "h2h",
        "selections": [
            {"name": "Cubs", "price": -120},
            {"name": "Brewers", "price": +105},
        ],
    }
    ctx = {"home_team": "Cubs", "away_team": "Brewers", "game_pk": 1}
    output = build_moneyline_model(matchup, market, ctx)

    assert output["model_probability"] == 0.61
    assert output["market_implied_probability"] is not None
    assert output["expected_value"] is not None
    assert output["confidence_tier"] in {"LEAN", "STRONG", "LOCK", "MONITOR", "NO_BET"}
    assert output["recommendation_status"] in {"recommended", "monitor", "no_bet"}
    assert output["data_quality_score"] is not None


def test_batter_prop_over_is_suppressed_by_usage_weighted_no_bet_gate():
    matchup = {
        "home_win_prob": 0.55,
        "away_win_prob": 0.45,
        "lineup_status": "confirmed",
        "data_confidence": "medium",
        "batter_vs_arsenal_summary": {
            "Mookie Betts": {
                "pitcher_arsenal_usage": {"FF": 70, "SL": 25, "CU": 5},
                "hitter_metrics_by_pitch_type": {
                    "FF": {"xwoba": 0.280, "on_base_pct": 0.290, "hard_hit_pct": 0.28, "whiff_pct": 0.34, "k_pct": 0.31},
                    "SL": {"xwoba": 0.295, "on_base_pct": 0.300, "hard_hit_pct": 0.31, "whiff_pct": 0.33, "k_pct": 0.30},
                    "CU": {"xwoba": 0.420, "on_base_pct": 0.410, "hard_hit_pct": 0.60, "whiff_pct": 0.12, "k_pct": 0.10},
                },
            }
        },
    }
    prop_markets = [
        {
            "market_key": "batter_hits",
            "market_name": "batter_hits",
            "selections": [
                {"name": "Over", "description": "Mookie Betts", "line": 1.5, "price": +120},
            ],
        }
    ]

    payload = build_prop_models(matchup, prop_markets, limit=5)
    candidate = payload["top_candidates"][0]

    assert candidate["diagnostics"]["usage_weighted_gate"] is not None
    assert candidate["diagnostics"]["usage_weighted_gate"]["final_pitcher_vs_hitter_recommendation_status"] in {"NO_BET", "MONITOR"}
    assert candidate["recommendation_status"] in {"monitor", "no_bet", "recommended"}
    assert "usage-weighted pitcher-vs-hitter gate" in " ".join(candidate["drivers"])


def test_batter_prop_outputs_expected_value_confidence_tier_and_data_quality():
    matchup = {
        "home_win_prob": 0.52,
        "away_win_prob": 0.48,
        "lineup_status": "projected",
        "data_confidence": "medium",
    }
    prop_markets = [
        {
            "market_key": "batter_total_bases",
            "market_name": "batter_total_bases",
            "selections": [
                {"name": "Over", "description": "Juan Soto", "line": 1.5, "price": +130},
            ],
        }
    ]

    payload = build_prop_models(matchup, prop_markets, limit=5)
    candidate = payload["top_candidates"][0]

    assert candidate["expected_value"] is not None
    assert candidate["confidence_tier"] in {"LEAN", "STRONG", "LOCK", "MONITOR", "NO_BET"}
    assert candidate["data_quality_score"] is not None
    assert candidate["recommendation_status"] in {"recommended", "monitor", "no_bet"}
