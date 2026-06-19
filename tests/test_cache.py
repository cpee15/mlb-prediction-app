from mlb_app.cache import (
    cache_key,
    ttl_cache_clear,
    ttl_cache_delete,
    ttl_cache_get,
    ttl_cache_get_stale,
    ttl_cache_set,
)


def setup_function():
    ttl_cache_clear()


def test_cache_key_skips_empty_parts():
    assert cache_key("matchups", None, "", "2026-06-18") == "matchups:2026-06-18"


def test_ttl_cache_hit_returns_copy():
    payload = {"games": [{"game_pk": 123}]}
    ttl_cache_set("matchups:today", payload, ttl_seconds=60)

    cached = ttl_cache_get("matchups:today")
    assert cached == payload
    assert cached is not payload

    cached["games"][0]["game_pk"] = 999
    assert ttl_cache_get("matchups:today") == payload


def test_ttl_cache_expired_misses_but_stale_can_be_returned():
    ttl_cache_set("matchups:stale", [1, 2, 3], ttl_seconds=0, stale_ttl_seconds=60)

    assert ttl_cache_get("matchups:stale") is None
    assert ttl_cache_get_stale("matchups:stale") == [1, 2, 3]


def test_ttl_cache_stale_window_expires_entry():
    ttl_cache_set("matchups:expired", {"ok": True}, ttl_seconds=0, stale_ttl_seconds=0)

    assert ttl_cache_get("matchups:expired") is None
    assert ttl_cache_get_stale("matchups:expired") is None


def test_ttl_cache_delete():
    ttl_cache_set("matchups:delete", "value", ttl_seconds=60)

    assert ttl_cache_delete("matchups:delete") is True
    assert ttl_cache_get("matchups:delete") is None
    assert ttl_cache_delete("matchups:delete") is False
