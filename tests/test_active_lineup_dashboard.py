from mlb_app.active_lineup_solver import _apply_active_lineup_filter, _rerank_items


def test_rerank_items_keeps_every_available_player():
    items = [{"entity_id": str(idx), "entity_name": f"Player {idx}"} for idx in range(1, 16)]

    reranked = _rerank_items(items)

    assert len(reranked) == 15
    assert [row["rank"] for row in reranked] == list(range(1, 16))
    assert reranked[-1]["entity_name"] == "Player 15"


def test_active_lineup_filter_preserves_all_confirmed_hitters_and_metadata():
    response = {
        "date": "2026-07-08",
        "component": "hitters",
        "items": [
            {"entity_id": str(idx), "entity_name": f"Confirmed {idx}", "entity_type": "hitter", "team": "CHC"}
            for idx in range(1, 13)
        ] + [
            {"entity_id": "99", "entity_name": "Bench Bat", "entity_type": "hitter", "team": "CHC"}
        ],
    }
    lineup_index = {
        "confirmed_ids": {str(idx) for idx in range(1, 13)},
        "confirmed_names": set(),
        "metadata": {
            "enabled": True,
            "source": "matchups_boxscore_lineups",
            "lineup_status": "confirmed",
            "confirmed_lineup_date": "2026-07-08",
            "warnings": [],
        },
    }

    filtered = _apply_active_lineup_filter(response, lineup_index, "hitters")

    assert len(filtered["items"]) == 12
    assert filtered["result_count_after_lineup_filter"] == 12
    assert filtered["lineup_filter"]["removed_unconfirmed_count"] == 1
    assert filtered["lineup_filter"]["confirmed_lineup_date"] == "2026-07-08"
    assert filtered["items"][-1]["rank"] == 12
    assert all(row["lineup_verified"] for row in filtered["items"])
    assert all(row["lineup_source"] == "matchups_boxscore_lineups" for row in filtered["items"])
    assert all(row["confirmed_lineup_date"] == "2026-07-08" for row in filtered["items"])
