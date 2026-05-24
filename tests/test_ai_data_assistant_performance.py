from mlb_app.ai_data_assistant_performance import _daily_odds_answer_prefix


def test_daily_odds_answer_prefix_includes_new_fields():
    context = {
        "available": True,
        "top_game_models": [
            {
                "label": "Brewers @ Cubs",
                "market": "moneyline",
                "pick": "Cubs",
                "edge": 0.0645,
                "expected_value": 0.1183,
                "confidence_tier": "STRONG",
                "recommendation_status": "recommended",
            }
        ],
        "top_prop_candidates": [
            {
                "player_name": "Mookie Betts",
                "market": "batter_hits",
                "pick": "Mookie Betts 1.5",
                "expected_value": -0.07,
                "confidence_tier": "NO_BET",
                "recommendation_status": "no_bet",
                "usage_weighted_gate": {"final_pitcher_vs_hitter_recommendation_status": "NO_BET"},
            }
        ],
    }
    note = _daily_odds_answer_prefix(context)
    assert "Daily Odds note" in note
    assert "moneyline" in note
    assert "EV" in note
    assert "usage gate NO_BET" in note
