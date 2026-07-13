from __future__ import annotations

from mlb_app.my_dashboard_observability import summarize_hydration_payload


def test_hydration_summary_remains_secondary_to_lineup_state():
    payload = {
        "results": {
            "hitters": {
                "model_state": "lineup_building",
                "lineup_revision": "abc123",
                "lineup_filter": {
                    "lineup_status": "partial",
                    "confirmed_batter_count": 81,
                    "games_checked": 15,
                    "games_with_lineups": 9,
                    "teams_with_lineups": 18,
                    "warnings": [],
                },
            }
        }
    }
    summary = summarize_hydration_payload(payload)
    assert summary["games_checked"] == 15
    assert summary["games_with_lineups"] == 9
    assert summary["confirmed_batter_count"] == 81
