from __future__ import annotations

import datetime as dt
import os
import time
from typing import Any, Dict, List, Optional

import requests

APIFY_BASE_URL = "https://api.apify.com/v2"


def _token() -> Optional[str]:
    return os.getenv("APIFY_TOKEN")


def _actor_id() -> Optional[str]:
    return os.getenv("DRAFTKINGS_ODDS_ACTOR_ID")


def _provider_not_configured(scope: str, message: str) -> Dict[str, Any]:
    return {
        "provider": "apify_draftkings",
        "book": "DraftKings",
        "status": "provider_not_configured",
        "scope": scope,
        "events": [],
        "markets": [],
        "raw_count": 0,
        "event_count": 0,
        "market_count": 0,
        "last_updated": None,
        "errors": [],
        "message": message,
    }


def _provider_error(scope: str, exc: Exception, request_input: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "provider": "apify_draftkings",
        "book": "DraftKings",
        "status": "provider_error",
        "scope": scope,
        "events": [],
        "markets": [],
        "raw_count": 0,
        "event_count": 0,
        "market_count": 0,
        "last_updated": int(time.time()),
        "errors": [str(exc)],
        "message": "Apify DraftKings actor failed while fetching odds.",
        "request_input": request_input or {},
    }


def _first(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _safe_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _american_to_decimal(price: Any) -> Optional[float]:
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if p > 0:
        return round(1 + p / 100, 4)
    if p < 0:
        return round(1 + 100 / abs(p), 4)
    return None


def _american_to_implied(price: Any) -> Optional[float]:
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if p > 0:
        return round(100 / (p + 100), 4)
    if p < 0:
        return round(abs(p) / (abs(p) + 100), 4)
    return None


def _extract_price(row: Dict[str, Any]) -> Any:
    return _first(
        row.get("price"),
        row.get("americanOdds"),
        row.get("american_odds"),
        row.get("oddsAmerican"),
        row.get("odds"),
        row.get("oddsPrice"),
    )


def _extract_line(row: Dict[str, Any]) -> Any:
    return _first(row.get("line"), row.get("point"), row.get("points"), row.get("handicap"), row.get("total"))


def _market_key(raw: Any) -> str:
    key = str(raw or "").strip().lower().replace(" ", "_").replace("-", "_")
    aliases = {
        "moneyline": "h2h",
        "money_line": "h2h",
        "run_line": "spreads",
        "spread": "spreads",
        "total_runs": "totals",
        "game_total": "totals",
        "over_under": "totals",
        "batter_hits": "batter_hits",
        "hits": "batter_hits",
        "player_hits": "batter_hits",
        "batter_total_bases": "batter_total_bases",
        "total_bases": "batter_total_bases",
        "player_total_bases": "batter_total_bases",
        "batter_home_runs": "batter_home_runs",
        "home_runs": "batter_home_runs",
        "player_home_runs": "batter_home_runs",
        "batter_rbis": "batter_rbis",
        "rbis": "batter_rbis",
        "batter_runs_scored": "batter_runs_scored",
        "runs_scored": "batter_runs_scored",
        "pitcher_strikeouts": "pitcher_strikeouts",
        "strikeouts": "pitcher_strikeouts",
        "player_strikeouts": "pitcher_strikeouts",
    }
    return aliases.get(key, key or "unknown")


def _selection(row: Dict[str, Any]) -> Dict[str, Any]:
    price = _extract_price(row)
    name = _first(row.get("name"), row.get("selection"), row.get("outcome"), row.get("side"), row.get("label"))
    description = _first(row.get("description"), row.get("playerName"), row.get("player_name"), row.get("participant"), row.get("team"))
    line = _extract_line(row)
    return {
        "selection_id": _first(row.get("id"), row.get("selectionId"), row.get("selection_id")),
        "name": name,
        "description": description,
        "team": _first(row.get("team"), row.get("teamName"), description),
        "side": name,
        "line": line,
        "odds": {
            "american": price,
            "decimal": _american_to_decimal(price),
            "fractional": None,
            "implied_probability": _american_to_implied(price),
        },
        "price": price,
        "is_open": True,
        "raw": row,
    }


def _normalize_market(raw_market: Dict[str, Any]) -> Dict[str, Any]:
    key = _market_key(_first(raw_market.get("market_key"), raw_market.get("marketType"), raw_market.get("market"), raw_market.get("marketName"), raw_market.get("name"), raw_market.get("label")))
    outcomes = _safe_list(raw_market.get("selections")) or _safe_list(raw_market.get("outcomes")) or _safe_list(raw_market.get("odds"))
    if not outcomes and any(k in raw_market for k in ["price", "americanOdds", "oddsAmerican", "line"]):
        outcomes = [raw_market]
    return {
        "market_id": _first(raw_market.get("market_id"), raw_market.get("marketId"), key),
        "market_key": key,
        "market_name": key,
        "market_type": key,
        "line": _extract_line(raw_market),
        "period": _first(raw_market.get("period"), raw_market.get("periodType")),
        "is_open": True,
        "last_update": _first(raw_market.get("last_update"), raw_market.get("lastUpdate"), raw_market.get("updatedAt")),
        "bookmaker_key": "draftkings",
        "bookmaker_title": "DraftKings",
        "selections": [_selection(o) for o in outcomes if isinstance(o, dict)],
        "raw": raw_market,
    }


def _looks_like_selection(row: Dict[str, Any]) -> bool:
    return any(k in row for k in ["price", "americanOdds", "american_odds", "oddsAmerican", "line", "point"])


def _event_name(row: Dict[str, Any]) -> str:
    away = _first(row.get("away_team"), row.get("awayTeam"), row.get("away"), row.get("visitorTeam"))
    home = _first(row.get("home_team"), row.get("homeTeam"), row.get("home"), row.get("homeTeamName"))
    name = _first(row.get("name"), row.get("eventName"), row.get("matchup"), row.get("title"))
    if away and home:
        return f"{away} @ {home}"
    return str(name or "DraftKings Event")


def _event_key(row: Dict[str, Any]) -> str:
    return str(_first(row.get("event_id"), row.get("eventId"), row.get("id"), row.get("gameId"), row.get("game_id"), _event_name(row)))


def _normalize_events(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    events: Dict[str, Dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        event_id = _event_key(item)
        away = _first(item.get("away_team"), item.get("awayTeam"), item.get("away"), item.get("visitorTeam"))
        home = _first(item.get("home_team"), item.get("homeTeam"), item.get("home"), item.get("homeTeamName"))
        start = _first(item.get("start_time"), item.get("startTime"), item.get("commence_time"), item.get("commenceTime"), item.get("date"))
        event = events.setdefault(
            event_id,
            {
                "event_id": event_id,
                "name": _event_name(item),
                "sport": "MLB",
                "league": "baseball_mlb",
                "league_id": "baseball_mlb",
                "home_team": {"name": home},
                "away_team": {"name": away},
                "start_time": start,
                "status": "scheduled",
                "is_live": False,
                "source_url": _first(item.get("url"), item.get("sourceUrl")),
                "scraped_at": int(time.time()),
                "markets": [],
                "raw": item,
            },
        )

        raw_markets = _safe_list(item.get("markets")) or _safe_list(item.get("marketGroups")) or _safe_list(item.get("oddsMarkets"))
        if raw_markets:
            event["markets"].extend([_normalize_market(m) for m in raw_markets if isinstance(m, dict)])
        elif _looks_like_selection(item):
            event["markets"].append(_normalize_market(item))

    normalized = []
    for event in events.values():
        by_key: Dict[str, Dict[str, Any]] = {}
        for market in event["markets"]:
            key = market.get("market_key") or "unknown"
            if key not in by_key:
                by_key[key] = market
            else:
                by_key[key]["selections"].extend(market.get("selections", []))
        event["markets"] = list(by_key.values())
        event["market_count"] = len(event["markets"])
        normalized.append(event)
    return normalized


def _flatten_markets(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    markets: List[Dict[str, Any]] = []
    for event in events:
        for market in event.get("markets", []) or []:
            row = dict(market)
            row.pop("raw", None)
            row["event_id"] = event.get("event_id")
            row["event_name"] = event.get("name")
            row["league"] = event.get("league")
            row["league_id"] = event.get("league_id")
            row["start_time"] = event.get("start_time")
            row["is_live"] = event.get("is_live")
            row["source_url"] = event.get("source_url")
            markets.append(row)
    return markets


def _build_actor_input(date: Optional[str] = None, event_id: Optional[str] = None, props_only: bool = False) -> Dict[str, Any]:
    state = os.getenv("DRAFTKINGS_ODDS_STATE") or os.getenv("ODDS_API_STATE") or "IL"
    payload: Dict[str, Any] = {
        "sport": "MLB",
        "league": "MLB",
        "book": "draftkings",
        "sportsbook": "draftkings",
        "state": state,
        "includeProps": True,
        "propsOnly": props_only,
    }
    if date:
        payload["date"] = date
        payload["targetDate"] = date
    if event_id:
        payload["eventId"] = event_id
    return payload


def _run_actor(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    token = _token()
    actor_id = _actor_id()
    if not token:
        raise RuntimeError("APIFY_TOKEN is not configured")
    if not actor_id:
        raise RuntimeError("DRAFTKINGS_ODDS_ACTOR_ID is not configured")
    url = f"{APIFY_BASE_URL}/acts/{actor_id}/run-sync-get-dataset-items"
    response = requests.post(
        url,
        params={"token": token, "clean": "true", "format": "json", "timeout": "120"},
        json=payload,
        timeout=180,
    )
    response.raise_for_status()
    data = response.json()
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        for key in ["items", "events", "data", "results"]:
            if isinstance(data.get(key), list):
                return [row for row in data[key] if isinstance(row, dict)]
    return []


def fetch_apify_draftkings_events(date: Optional[str] = None, raw: bool = False) -> Dict[str, Any]:
    if not _token() or not _actor_id():
        return _provider_not_configured("events", "APIFY_TOKEN or DRAFTKINGS_ODDS_ACTOR_ID is not configured.")
    payload = _build_actor_input(date=date, props_only=False)
    try:
        items = _run_actor(payload)
        events = _normalize_events(items)
        markets = _flatten_markets(events)
    except Exception as exc:
        return _provider_error("events", exc, request_input=payload)
    out = {
        "provider": "apify_draftkings",
        "book": "DraftKings",
        "status": "ok" if events else "empty",
        "scope": "events",
        "sport": "baseball_mlb",
        "target_date": date,
        "books": ["DraftKings"],
        "events": events,
        "markets": markets,
        "last_updated": int(time.time()),
        "raw_count": len(items),
        "event_count": len(events),
        "market_count": len(markets),
        "errors": [],
        "request_input": {k: v for k, v in payload.items() if k.lower() not in {"token", "apikey", "api_key"}},
        "cache_hit": False,
    }
    if raw:
        out["raw_items_sample"] = items[:5]
    return out


def fetch_apify_draftkings_event_odds(event_id: str, props_only: bool = True, raw: bool = False) -> Dict[str, Any]:
    if not _token() or not _actor_id():
        return _provider_not_configured("event_props", "APIFY_TOKEN or DRAFTKINGS_ODDS_ACTOR_ID is not configured.")
    payload = _build_actor_input(event_id=event_id, props_only=props_only)
    try:
        items = _run_actor(payload)
        events = _normalize_events(items)
        event = events[0] if events else None
        markets = _flatten_markets(events)
    except Exception as exc:
        return _provider_error("event_props", exc, request_input=payload)
    out = {
        "provider": "apify_draftkings",
        "book": "DraftKings",
        "status": "ok" if events else "empty",
        "scope": "event_props",
        "sport": "baseball_mlb",
        "game_pk": event_id,
        "event_id": event_id,
        "books": ["DraftKings"],
        "events": events,
        "event": event,
        "markets": markets,
        "last_updated": int(time.time()),
        "raw_count": len(items),
        "event_count": len(events),
        "market_count": len(markets),
        "errors": [],
        "request_input": {k: v for k, v in payload.items() if k.lower() not in {"token", "apikey", "api_key"}},
        "cache_hit": False,
    }
    if raw:
        out["raw_items_sample"] = items[:5]
    return out
