from __future__ import annotations

import datetime as dt

from mlb_app.active_lineup_solver import _lineup_cache_ttl, lineup_revision
from mlb_app.shared_payload_cache import _effective_ttl


def test_today_lineup_index_polls_on_baseball_time():
    assert _lineup_cache_ttl(dt.date.today().isoformat()) == 30


def test_historical_lineup_index_can_use_normal_cache_window():
    assert _lineup_cache_ttl("2026-07-01") == 300


def test_active_lineup_solver_payload_cache_is_never_stale_for_five_minutes():
    key = "dashboard_solver:active_lineups_full_result:2026-07-13:hitters:abc"
    assert _effective_ttl(key, 300) == 30


def test_standard_dashboard_payload_keeps_normal_ttl():
    key = "dashboard_solver:component_full_result:2026-07-13:hitters:abc"
    assert _effective_ttl(key, 300) == 300


def test_lineup_revision_changes_when_confirmed_player_identity_changes():
    first = {"confirmed_ids": {"1", "2"}, "confirmed_names": set(), "metadata": {"lineup_status": "partial"}}
    second = {"confirmed_ids": {"1", "2", "3"}, "confirmed_names": set(), "metadata": {"lineup_status": "partial"}}
    assert lineup_revision(first) != lineup_revision(second)
