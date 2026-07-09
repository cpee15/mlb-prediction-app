from __future__ import annotations

import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional

from .etl import fetch_schedule
from .performance import estimate_payload_bytes, record_span, timing_span
from .shared_payload_cache import env_ttl, get_cache, make_cache_key, set_cache

ScheduleFetcher = Callable[[str], Iterable[Dict[str, Any]]]


def build_date_window(today: Optional[datetime.date] = None) -> Dict[str, str]:
    base = today or datetime.date.today()
    return {
        "yesterday": (base - datetime.timedelta(days=1)).isoformat(),
        "today": base.isoformat(),
        "tomorrow": (base + datetime.timedelta(days=1)).isoformat(),
    }


def _team_name(game: Dict[str, Any], side: str) -> Optional[str]:
    team = game.get(side, {}).get("team", {}) if isinstance(game.get(side), dict) else {}
    return team.get("name")


def _team_id(game: Dict[str, Any], side: str) -> Optional[int]:
    team = game.get(side, {}).get("team", {}) if isinstance(game.get(side), dict) else {}
    try:
        return int(team.get("id")) if team.get("id") is not None else None
    except Exception:
        return None


def _pitcher(game: Dict[str, Any], side: str) -> Dict[str, Any]:
    pitcher = game.get(side, {}).get("probablePitcher", {}) if isinstance(game.get(side), dict) else {}
    if not isinstance(pitcher, dict):
        pitcher = {}
    return {
        "id": pitcher.get("id"),
        "name": pitcher.get("fullName"),
        "hand": (pitcher.get("pitchHand") or {}).get("code") if isinstance(pitcher.get("pitchHand"), dict) else None,
    }


def compact_schedule_game(game: Dict[str, Any]) -> Dict[str, Any]:
    """Return only calendar-visible schedule fields, not heavy matchup/model payloads."""
    return {
        "game_pk": game.get("_game_pk") or game.get("gamePk"),
        "game_date": game.get("_game_date") or game.get("gameDate"),
        "game_time": game.get("_game_date") or game.get("gameDate"),
        "venue": game.get("_venue") or ((game.get("venue") or {}).get("name") if isinstance(game.get("venue"), dict) else game.get("venue")),
        "status": game.get("_status") or ((game.get("status") or {}).get("detailedState") if isinstance(game.get("status"), dict) else game.get("status")),
        "home_team_id": _team_id(game, "home"),
        "away_team_id": _team_id(game, "away"),
        "home_team_name": _team_name(game, "home"),
        "away_team_name": _team_name(game, "away"),
        "home_pitcher": _pitcher(game, "home"),
        "away_pitcher": _pitcher(game, "away"),
    }


def build_schedule_calendar_snapshot(
    date_str: str,
    *,
    fetcher: ScheduleFetcher = fetch_schedule,
) -> Dict[str, Any]:
    """Build a lightweight calendar snapshot for one date.

    This deliberately avoids `generate_matchups_for_date`, lineup diagnostics,
    canonical probability, Model Projection generation, and simulations.
    """
    with timing_span("calendar.fetch_schedule", category="schedule", route="/matchups/calendar", date=date_str):
        schedule = list(fetcher(date_str) or [])
    games = [compact_schedule_game(game) for game in schedule]
    payload = {
        "date": date_str,
        "count": len(games),
        "games": games,
        "calendar_snapshot_version": "schedule_calendar_v1",
        "source": "schedule_calendar_snapshot",
        "heavy_matchup_generation": False,
        "probability_source": "not_loaded_on_calendar_initial_snapshot",
        "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    record_span(
        "calendar.snapshot.payload_bytes",
        category="serialization",
        route="/matchups/calendar",
        date=date_str,
        payload_bytes=estimate_payload_bytes(payload),
    )
    return payload


def get_or_build_schedule_calendar_snapshot(
    date_str: str,
    *,
    fetcher: ScheduleFetcher = fetch_schedule,
    ttl_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    ttl = env_ttl("MATCHUPS_CALENDAR_CACHE_TTL_SECONDS") if ttl_seconds is None else ttl_seconds
    cache_key = make_cache_key("schedule_calendar", "date", date_str)
    cached = get_cache(cache_key, ttl)
    if isinstance(cached, dict):
        cached.setdefault("cache_hit", True)
        cached.setdefault("cache_key", cache_key)
        cached.setdefault("ttl_seconds", ttl)
        return cached
    payload = build_schedule_calendar_snapshot(date_str, fetcher=fetcher)
    payload.setdefault("cache_hit", False)
    payload.setdefault("cache_key", cache_key)
    payload.setdefault("ttl_seconds", ttl)
    return set_cache(cache_key, payload)


def build_calendar_window_payload(
    dates: Optional[Dict[str, str]] = None,
    *,
    fetcher: ScheduleFetcher = fetch_schedule,
    ttl_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    resolved_dates = dates or build_date_window()
    payload: Dict[str, Any] = {}
    for key, date_value in resolved_dates.items():
        payload[key] = get_or_build_schedule_calendar_snapshot(
            date_value,
            fetcher=fetcher,
            ttl_seconds=ttl_seconds,
        )
    return payload


def warm_schedule_calendar_window(
    dates: Optional[Dict[str, str]] = None,
    *,
    fetcher: ScheduleFetcher = fetch_schedule,
) -> Dict[str, Any]:
    payload = build_calendar_window_payload(dates, fetcher=fetcher, ttl_seconds=0)
    return {
        "warmed": True,
        "dates": {key: value.get("date") for key, value in payload.items()},
        "counts": {key: value.get("count") for key, value in payload.items()},
        "snapshot_version": "schedule_calendar_v1",
    }


__all__ = [
    "build_calendar_window_payload",
    "build_date_window",
    "build_schedule_calendar_snapshot",
    "compact_schedule_game",
    "get_or_build_schedule_calendar_snapshot",
    "warm_schedule_calendar_window",
]
