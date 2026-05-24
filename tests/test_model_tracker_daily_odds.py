from mlb_app.model_tracker import normalize_ai_data_assistant_rows, normalize_daily_odds_rows


def test_normalize_daily_odds_rows_stores_new_diagnostics_in_json_columns():
    payload = {
        "games": [
            {
                "game_pk": 123,
                "event_id": "evt1",
                "away_team": "Brewers",
                "home_team": "Cubs",
                "home_win_prob": 0.61,
                "away_win_prob": 0.39,
                "model_version": "canonical_matchup_win_probability_v2",
                "lineup_status": "confirmed",
                "data_confidence": "high",
                "models": {
                    "moneyline": {
                        "model": "moneyline_canonical_v2",
                        "market": "moneyline",
                        "pick": "Cubs",
                        "model_probability": 0.61,
                        "market_implied_probability": 0.5455,
                        "edge": 0.0645,
                        "score": 0.61,
                        "confidence": 0.82,
                        "price": -120,
                        "expected_value": 0.1183,
                        "confidence_tier": "STRONG",
                        "data_quality_score": 0.94,
                        "recommendation_status": "recommended",
                        "rejection_reason": None,
                        "drivers": ["canonical probability", "positive edge"],
                        "features_used": [{"name": "home_win_prob", "value": 0.61}],
                        "missing_inputs": [],
                        "diagnostics": {"usage_weighted_gate": None},
                    }
                },
            }
        ],
        "top_prop_model_candidates": [
            {
                "game_pk": 123,
                "event_id": "evt1",
                "away_team": "Brewers",
                "home_team": "Cubs",
                "player_name": "Mookie Betts",
                "market": "batter_hits",
                "pick": "Mookie Betts 1.5",
                "model_probability": 0.44,
                "market_implied_probability": 0.48,
                "edge": -0.04,
                "score": 0.04,
                "confidence": 0.66,
                "price": 110,
                "expected_value": -0.076,
                "confidence_tier": "NO_BET",
                "data_quality_score": 0.71,
                "recommendation_status": "no_bet",
                "rejection_reason": "non_positive_edge",
                "drivers": ["usage-weighted gate suppressed over"],
                "features_used": [],
                "missing_inputs": ["pitch_data_quality_review"],
                "diagnostics": {
                    "usage_weighted_gate": {
                        "final_pitcher_vs_hitter_recommendation_status": "NO_BET",
                        "supported_usage_share": 0.22,
                    }
                },
            }
        ],
    }

    rows = normalize_daily_odds_rows(payload, "2026-05-24")
    assert len(rows) == 2
    moneyline = rows[0]
    prop = rows[1]
    assert "confidence_tier" in (moneyline["reasoning_json"] or "")
    assert "recommendation_status" in (moneyline["reasoning_json"] or "")
    assert "daily_odds_tracker_diagnostics" in (prop["raw_payload_json"] or "")
    assert "NO_BET" in (prop["raw_payload_json"] or "")


def test_normalize_ai_data_assistant_rows_preserves_daily_odds_context():
    payload = {
        "answer": "Here is the current model view.",
        "canonical_probability_context": {"model_version": "canonical_matchup_win_probability_v2"},
        "daily_odds_diagnostics_context": {"available": True, "top_game_models": [{"market": "moneyline"}]},
        "sources_used": ["canonical_matchup_probability_v2", "daily_odds_models"],
        "missing_data": [],
        "sections": [],
    }
    rows = normalize_ai_data_assistant_rows(payload, "2026-05-24", "What does Daily Odds tell us?")
    assert len(rows) == 1
    row = rows[0]
    assert "daily_odds_diagnostics_context" in (row["reasoning_json"] or "")
