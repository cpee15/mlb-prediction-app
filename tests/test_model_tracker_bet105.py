from mlb_app.model_tracker_bet105 import (
    as_model_signals,
    build_bet105_decisions,
    normalize_bet105_markets,
)


def _board():
    return {
        "events": [{
            "event_id": "bet105-1",
            "away_team": "Away",
            "home_team": "Home",
            "markets": [
                {"market_key": "h2h", "market_name": "Moneyline", "selections": [
                    {"id": "away", "name": "Away", "price_american": 120, "active": True},
                    {"id": "home", "name": "Home", "price_american": -140, "active": True},
                ]},
                {"market_key": "totals", "market_name": "Game Total", "line": 8.5, "selections": [
                    {"id": "over", "name": "Over 8.5", "price_american": -110, "active": True},
                    {"id": "under", "name": "Under 8.5", "price_american": -110, "active": True},
                ]},
            ],
        }]
    }


def _projection():
    return {
        "source": "model_projections", "game_pk": 1, "away_team": "Away", "home_team": "Home",
        "home_win_probability": 0.64, "away_win_probability": 0.36,
        "projected_total": 9.2, "confidence": 0.60, "model_name": "test_model",
    }


def test_bet105_normalization_preserves_real_prices_and_implied_probability():
    rows = normalize_bet105_markets(_board(), "2026-07-26")
    home = next(row for row in rows if row["selection"] == "Home")
    assert home["market_type"] == "moneyline"
    assert home["price"] == -140
    assert round(home["implied_probability"], 4) == 0.5833
    assert home["raw"]["selection"]["id"] == "home"


def test_moneyline_decision_requires_positive_edge_and_real_price():
    rows = build_bet105_decisions(_board(), [_projection()], "2026-07-26")
    home = next(row for row in rows if row["market_type"] == "moneyline")
    assert home["source"] == "bet105"
    assert home["pick_type"] == "reportable_decision"
    assert home["edge"] > 0
    assert home["price"] == -140


def test_total_requires_direction_and_conservative_projection_gap():
    rows = build_bet105_decisions(_board(), [_projection()], "2026-07-26")
    total = next(row for row in rows if row["market_type"] == "total")
    assert total["pick_label"] == "Over 8.5"
    assert total["line"] == 8.5


def test_model_and_dashboard_rows_are_explicit_non_reportable_signals():
    rows = as_model_signals([{
        "source": "my_dashboard", "source_component": "hitters", "snapshot_date": "2026-07-26",
        "pick_label": "Player", "market_type": "player_prop", "game_pk": 1,
    }], "2026-07-26")
    assert rows[0]["pick_type"] == "model_signal"
    assert rows[0]["grade"] == "watchlist_only"
    assert rows[0]["price"] is None
    assert rows[0]["missing_inputs_json"] == ["No matching Bet105 market/price available."]
