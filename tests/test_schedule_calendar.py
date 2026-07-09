from __future__ import annotations

import datetime

from mlb_app.schedule_calendar import (
    build_calendar_window_payload,
    build_date_window,
    build_schedule_calendar_snapshot,
    compact_schedule_game,
    get_or_build_schedule_calendar_snapshot,
)
from mlb_app.shared_payload_cache import clear_shared_payload_cache


def _schedule(_date: str):
    return [
        {
            "_game_pk": 123,
            "_game_date": "2026-07-09T19:05:00Z",
            "_venue": "Test Park",
            "_status": "Preview",
            "home": {
                "team": {"id": 1, "name": "Home Club"},
                "probablePitcher": {"id": 11, "fullName": "Home Starter", "pitchHand": {"code": "R"}},
            },
            "away": {
                "team": {"id": 2, "name": "Away Club"},
                "probablePitcher": {"id": 22, "fullName": "Away Starter", "pitchHand": {"code": "L"}},
            },
        }
    ]


def test_compact_schedule_game_omits_heavy_matchup_payload_fields() -> None:
    row = compact_schedule_game(_schedule("2026-07-09")[0])

    assert row["game_pk"] == 123
    assert row["home_team_name"] == "Home Club"
    assert row["away_team_name"] == "Away Club"
    assert row["home_pitcher"]["name"] == "Home Starter"
    assert row["away_pitcher"]["hand"] == "L"
    assert "probability_components" not in row
    assert "home_pitcher_features" not in row
    assert "away_pitch_arsenal" not in row
    assert "sharedSimulation" not in row


def test_build_schedule_calendar_snapshot_uses_schedule_fetcher_only() -> None:
    calls = []

    def fetcher(date: str):
        calls.append(date)
        return _schedule(date)

    payload = build_schedule_calendar_snapshot("2026-07-09", fetcher=fetcher)

    assert calls == ["2026-07-09"]
    assert payload["date"] == "2026-07-09"
    assert payload["count"] == 1
    assert payload["calendar_snapshot_version"] == "schedule_calendar_v1"
    assert payload["heavy_matchup_generation"] is False
    assert payload["probability_source"] == "not_loaded_on_calendar_initial_snapshot"
    assert payload["games"][0]["game_pk"] == 123


def test_get_or_build_schedule_calendar_snapshot_uses_cache() -> None:
    clear_shared_payload_cache("schedule_calendar")
    calls = []

    def fetcher(date: str):
        calls.append(date)
        return _schedule(date)

    first = get_or_build_schedule_calendar_snapshot("2026-07-09", fetcher=fetcher, ttl_seconds=300)
    second = get_or_build_schedule_calendar_snapshot("2026-07-09", fetcher=fetcher, ttl_seconds=300)

    assert calls == ["2026-07-09"]
    assert first["count"] == 1
    assert second["count"] == 1
    assert second["cache_hit"] is True


def test_calendar_window_payload_builds_yesterday_today_tomorrow_without_heavy_matchups() -> None:
    dates = build_date_window(datetime.date(2026, 7, 9))
    payload = build_calendar_window_payload(dates, fetcher=_schedule, ttl_seconds=0)

    assert set(payload) == {"yesterday", "today", "tomorrow"}
    assert payload["yesterday"]["date"] == "2026-07-08"
    assert payload["today"]["date"] == "2026-07-09"
    assert payload["tomorrow"]["date"] == "2026-07-10"
    assert all(day["heavy_matchup_generation"] is False for day in payload.values())
