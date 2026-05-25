from mlb_app.ai_data_assistant_performance import _compact_daily_odds_game_model
from mlb_app.daily_odds_models import build_game_models


def test_build_game_models_returns_canonical_game_context():
    matchup = {
        "game_pk": 123,
        "away_team_name": "Brewers",
        "home_team_name": "Cubs",
        "home_win_prob": 0.61,
        "away_win_prob": 0.39,
        "lineup_status": "confirmed",
        "data_confidence": "high",
        "missing_inputs": [],
        "probability_components": {
            "bullpen": {"source": "build_bullpen_profile", "available": True, "score": 0.53, "diagnostics": {"home_bullpen_quality_score": 0.62, "away_bullpen_quality_score": 0.55}},
            "simulation": {"diagnostics": {"home_expected_runs": 4.8, "away_expected_runs": 3.9, "total_expected_runs": 8.7}},
        },
        "pitcher_overview": {"home": {}, "away": {}},
        "home_pitcher_features": {},
        "away_pitcher_features": {},
        "home_offense_inputs": {"on_base_pct": 0.336, "slugging_pct": 0.431, "iso": 0.176, "bb_pct": 0.087, "k_pct": 0.221},
        "away_offense_inputs": {"on_base_pct": 0.317, "slugging_pct": 0.401, "iso": 0.154, "bb_pct": 0.073, "k_pct": 0.244},
    }
    event = {
        "event_id": "evt1",
        "markets": [
            {"market_key": "h2h", "selections": [{"name": "Cubs", "price": -120}, {"name": "Brewers", "price": +105}]},
            {"market_key": "spreads", "selections": [{"name": "Cubs", "line": -1.5, "price": +120}, {"name": "Brewers", "line": 1.5, "price": -140}]},
            {"market_key": "totals", "selections": [{"name": "Over", "line": 8.5, "price": -110}, {"name": "Under", "line": 8.5, "price": -110}]},
        ],
    }
    models = build_game_models(matchup, event)
    assert "canonical_game_context" in models
    assert models["canonical_game_context"]["projected_total_runs"] == 8.7


def test_ai_compact_daily_odds_game_model_preserves_canonical_game_context():
    model = {
        "market": "moneyline",
        "pick": "Cubs",
        "model_probability": 0.61,
        "market_implied_probability": 0.5455,
        "edge": 0.0645,
        "expected_value": 0.118,
        "confidence_tier": "STRONG",
        "recommendation_status": "recommended",
        "diagnostics": {"canonical_game_context": {"projected_total_runs": 8.7, "favorite_side": "home"}},
    }
    compact = _compact_daily_odds_game_model(model, "Brewers @ Cubs", 123)
    assert compact["canonical_game_context"]["projected_total_runs"] == 8.7
