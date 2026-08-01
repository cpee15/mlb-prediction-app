from __future__ import annotations

from mlb_app.shared_payload_cache import (
    clear_shared_payload_cache,
    get_cache,
    make_cache_key,
    set_cache,
)


def _snapshot_lookup(key: str):
    # Keep this helper name aligned with the FastAPI endpoint function. The
    # explicit refresh boundary is intentionally detected by function name so
    # the normal homepage cache path remains unchanged.
    def snapshot_matchups():
        return get_cache(key, 300)

    return snapshot_matchups()


def setup_function() -> None:
    clear_shared_payload_cache()


def teardown_function() -> None:
    clear_shared_payload_cache()


def test_normal_matchup_lookup_remains_a_cache_hit() -> None:
    key = make_cache_key("matchups", "date", "2026-07-31")
    slate = [{"game_pk": 1, "home_pitcher_id": 101}]
    set_cache(key, slate)

    assert get_cache(key, 300) == slate


def test_snapshot_lookup_bypasses_and_evicts_stale_matchup_slate() -> None:
    key = make_cache_key("matchups", "date", "2026-07-31")
    stale = [{"game_pk": 1, "home_pitcher_id": None}]
    set_cache(key, stale)

    assert _snapshot_lookup(key) is None
    assert get_cache(key, 300) is None

    refreshed = [{"game_pk": 1, "home_pitcher_id": 101}]
    set_cache(key, refreshed)
    assert get_cache(key, 300) == refreshed


def test_snapshot_refresh_does_not_bypass_unrelated_cache_namespaces() -> None:
    key = make_cache_key("model_projection", "date", "2026-07-31")
    payload = {"games": [{"game_pk": 1}]}
    set_cache(key, payload)

    assert _snapshot_lookup(key) == payload
    assert get_cache(key, 300) == payload


def test_doubleheader_games_remain_separate_after_snapshot_replacement() -> None:
    key = make_cache_key("matchups", "date", "2026-07-31")
    stale = [
        {"game_pk": 9001, "home_pitcher_id": 111},
        {"game_pk": 9002, "home_pitcher_id": None},
    ]
    set_cache(key, stale)

    assert _snapshot_lookup(key) is None

    refreshed = [
        {"game_pk": 9001, "home_pitcher_id": 111},
        {"game_pk": 9002, "home_pitcher_id": 222},
    ]
    set_cache(key, refreshed)

    cached = get_cache(key, 300)
    assert [game["game_pk"] for game in cached] == [9001, 9002]
    assert cached[0]["home_pitcher_id"] == 111
    assert cached[1]["home_pitcher_id"] == 222
