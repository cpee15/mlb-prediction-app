import datetime as dt
import os
import time
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

try:
    from .apify_draftkings_provider import fetch_apify_draftkings_event_odds, fetch_apify_draftkings_events
except Exception:
    fetch_apify_draftkings_event_odds = None
    fetch_apify_draftkings_events = None

try:
    from .kibl_bet105_provider import fetch_kibl_bet105_event_odds, fetch_kibl_bet105_events, fetch_kibl_bet105_odds
except Exception:
    fetch_kibl_bet105_event_odds = None
    fetch_kibl_bet105_events = None
    fetch_kibl_bet105_odds = None

_CACHE: Dict[str, Dict[str, Any]] = {}
_ODDS_API_BASE = "https://api.the-odds-api.com/v4"
_ODDS_API_SPORT = "baseball_mlb"
_DEFAULT_BOOKMAKER = "draftkings"
_DEFAULT_REGIONS = "us"
_DEFAULT_MARKETS = ["h2h", "spreads", "totals"]
_DEFAULT_PROP_MARKETS = ["batter_home_runs", "batter_hits", "batter_total_bases", "batter_rbis", "batter_runs_scored", "batter_hits_runs_rbis", "pitcher_strikeouts"]
_MARKET_TYPE_MAP = {"moneyline": "h2h", "h2h": "h2h", "spread": "spreads", "spreads": "spreads", "total": "totals", "totals": "totals", "player_props": "player_props", "props": "player_props", "all": "all"}
_MLB_SLATE_TIMEZONE = ZoneInfo(os.getenv("ODDS_API_SLATE_TIMEZONE", "America/New_York"))
_UTC = dt.timezone.utc


def _cache_get(key: str):
    entry = _CACHE.get(key)
    return entry["data"] if entry and time.time() < entry["expires_at"] else None


def _cache_set(key: str, data: Any, ttl: int = 300):
    _CACHE[key] = {"data": data, "expires_at": time.time() + ttl}


def _selected_provider() -> str:
    return (os.getenv("ODDS_PROVIDER") or os.getenv("DRAFTKINGS_ODDS_PROVIDER") or os.getenv("SPORTSBOOK_ODDS_PROVIDER") or "the_odds_api").strip().lower()


def _should_use_kibl() -> bool:
    return _selected_provider() in {"kibl", "kibl_bet105", "bet105"}


def _should_use_apify_first() -> bool:
    return _selected_provider() in {"apify", "draftkings_apify"}


def _has_apify_config() -> bool:
    return bool(os.getenv("APIFY_TOKEN") and os.getenv("DRAFTKINGS_ODDS_ACTOR_ID") and fetch_apify_draftkings_events)


def _provider_not_configured(scope: str, game_pk: Optional[Any] = None, message: str = "ODDS_API_KEY is not configured.") -> Dict[str, Any]:
    return {"provider": "the_odds_api", "book": "DraftKings", "status": "provider_not_configured", "scope": scope, "game_pk": game_pk, "event_id": game_pk, "markets": [], "events": [], "books": ["DraftKings"], "last_updated": None, "raw_count": 0, "event_count": 0, "market_count": 0, "errors": [], "message": message}


def _provider_error(scope: str, game_pk: Optional[Any], exc: Exception, request_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {"provider": "the_odds_api", "book": "DraftKings", "status": "provider_error", "scope": scope, "game_pk": game_pk, "event_id": game_pk, "markets": [], "events": [], "books": ["DraftKings"], "last_updated": int(time.time()), "raw_count": 0, "event_count": 0, "market_count": 0, "errors": [str(exc)], "message": "The Odds API provider failed while fetching DraftKings odds.", "request_params": request_params or {}}


def _parse_markets(market_types: Optional[List[str]], props_only: bool = False) -> List[str]:
    if props_only:
        env_value = os.getenv("ODDS_API_PROP_MARKETS")
        return [m.strip() for m in env_value.split(",") if m.strip()] if env_value else _DEFAULT_PROP_MARKETS
    raw = market_types or ([m.strip() for m in os.getenv("ODDS_API_MARKETS", "").split(",") if m.strip()] or _DEFAULT_MARKETS)
    mapped: List[str] = []
    for market in raw:
        value = _MARKET_TYPE_MAP.get(str(market).strip().lower(), str(market).strip())
        values = _DEFAULT_MARKETS if value == "all" else (_DEFAULT_PROP_MARKETS if value == "player_props" else [value])
        for item in values:
            if item not in mapped:
                mapped.append(item)
    return mapped or _DEFAULT_MARKETS


def _parse_iso_datetime(value: Any) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return (parsed if parsed.tzinfo else parsed.replace(tzinfo=_UTC)).astimezone(_UTC)
    except Exception:
        return None


def _slate_window_utc(target_date: Optional[str]) -> tuple[Optional[dt.datetime], Optional[dt.datetime]]:
    if not target_date:
        return None, None
    try:
        slate_date = dt.date.fromisoformat(str(target_date)[:10])
    except ValueError:
        return None, None
    local_start = dt.datetime.combine(slate_date, dt.time.min, tzinfo=_MLB_SLATE_TIMEZONE)
    return local_start.astimezone(_UTC), (local_start + dt.timedelta(days=1)).astimezone(_UTC)


def _format_api_datetime(value: Optional[dt.datetime]) -> Optional[str]:
    return None if value is None else value.astimezone(_UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _odds_decimal_from_american(price: Optional[float]) -> Optional[float]:
    try:
        price = float(price)
    except (TypeError, ValueError):
        return None
    return round(1 + price / 100, 4) if price > 0 else (round(1 + 100 / abs(price), 4) if price < 0 else None)


def _implied_from_american(price: Optional[float]) -> Optional[float]:
    try:
        price = float(price)
    except (TypeError, ValueError):
        return None
    return round(100 / (price + 100), 4) if price > 0 else (round(abs(price) / (abs(price) + 100), 4) if price < 0 else None)


def _normalize_selection(outcome: Dict[str, Any], market: Dict[str, Any]) -> Dict[str, Any]:
    price = outcome.get("price")
    return {"selection_id": outcome.get("id"), "name": outcome.get("name"), "description": outcome.get("description"), "team": outcome.get("name"), "side": outcome.get("name"), "line": outcome.get("point") if outcome.get("point") is not None else market.get("point"), "odds": {"american": price, "decimal": _odds_decimal_from_american(price), "fractional": None, "implied_probability": _implied_from_american(price)}, "price": price, "is_open": True, "raw": outcome}


def _normalize_event(item: Dict[str, Any], bookmaker_key: str = _DEFAULT_BOOKMAKER) -> Dict[str, Any]:
    target_book = next((b for b in item.get("bookmakers", []) or [] if b.get("key") == bookmaker_key), None) or (item.get("bookmakers") or [None])[0]
    book_markets = target_book.get("markets", []) if isinstance(target_book, dict) else []
    markets = []
    for market in book_markets:
        outcomes = market.get("outcomes") if isinstance(market.get("outcomes"), list) else []
        markets.append({"market_id": market.get("key"), "market_key": market.get("key"), "market_name": market.get("key"), "market_type": market.get("key"), "line": None, "period": None, "is_open": True, "last_update": market.get("last_update"), "bookmaker_key": target_book.get("key") if isinstance(target_book, dict) else None, "bookmaker_title": target_book.get("title") if isinstance(target_book, dict) else None, "selections": [_normalize_selection(outcome, market) for outcome in outcomes if isinstance(outcome, dict)], "raw": market})
    return {"event_id": item.get("id"), "name": f"{item.get('away_team')} @ {item.get('home_team')}", "sport": item.get("sport_title"), "league": item.get("sport_key"), "league_id": item.get("sport_key"), "home_team": {"name": item.get("home_team")}, "away_team": {"name": item.get("away_team")}, "start_time": item.get("commence_time"), "status": "scheduled", "is_live": False, "source_url": None, "scraped_at": int(time.time()), "markets": markets, "market_count": len(markets), "raw": item}


def _flatten_markets(events: List[Dict[str, Any]], game_pk: Optional[Any] = None) -> List[Dict[str, Any]]:
    flat: List[Dict[str, Any]] = []
    for event in events:
        event_id = event.get("event_id")
        if game_pk is not None and event_id is not None and str(event_id) != str(game_pk):
            continue
        for market in event.get("markets", []):
            row = dict(market)
            row.pop("raw", None)
            row.update({"event_id": event_id, "event_name": event.get("name"), "league": event.get("league"), "league_id": event.get("league_id"), "start_time": event.get("start_time"), "is_live": event.get("is_live"), "source_url": event.get("source_url")})
            flat.append(row)
    return flat


def _filter_events(events: List[Dict[str, Any]], game_pk: Optional[Any], target_date: Optional[str]) -> List[Dict[str, Any]]:
    start_utc, end_utc = _slate_window_utc(target_date)
    filtered: List[Dict[str, Any]] = []
    for event in events:
        if game_pk is not None and event.get("event_id") is not None and str(event.get("event_id")) != str(game_pk):
            continue
        commence = _parse_iso_datetime(event.get("start_time")) if start_utc and end_utc else None
        if commence is not None and not (start_utc <= commence < end_utc):
            continue
        filtered.append(event)
    return filtered


def _get_token() -> Optional[str]:
    return os.getenv("ODDS_API_KEY") or os.getenv("THE_ODDS_API_KEY")


def _fetch_odds_api(params: Dict[str, Any]) -> List[Dict[str, Any]]:
    response = requests.get(f"{_ODDS_API_BASE}/sports/{_ODDS_API_SPORT}/odds", params=params, timeout=20)
    response.raise_for_status()
    return response.json()


def _fetch_event_odds_api(event_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
    response = requests.get(f"{_ODDS_API_BASE}/sports/{_ODDS_API_SPORT}/events/{event_id}/odds", params=params, timeout=20)
    response.raise_for_status()
    return response.json()


def build_draftkings_run_input(scope: str = "pregame", props_only: bool = False, date: Optional[str] = None, league: Optional[str] = None, market_types: Optional[List[str]] = None, live_only: Optional[bool] = None, state: Optional[str] = None) -> Dict[str, Any]:
    request = {"apiKey": "***", "regions": os.getenv("ODDS_API_REGIONS", _DEFAULT_REGIONS), "markets": ",".join(_parse_markets(market_types, props_only=props_only)), "oddsFormat": os.getenv("ODDS_API_ODDS_FORMAT", "american"), "dateFormat": os.getenv("ODDS_API_DATE_FORMAT", "iso"), "bookmakers": os.getenv("ODDS_API_BOOKMAKERS", _DEFAULT_BOOKMAKER)}
    start_utc, end_utc = _slate_window_utc(date)
    if start_utc and end_utc:
        request["commenceTimeFrom"] = _format_api_datetime(start_utc)
        request["commenceTimeTo"] = _format_api_datetime(end_utc)
    return request


def _attach_fallback(primary: Dict[str, Any], fallback: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if fallback:
        primary["fallback_provider"] = {"provider": fallback.get("provider"), "status": fallback.get("status"), "raw_count": fallback.get("raw_count"), "event_count": fallback.get("event_count"), "market_count": fallback.get("market_count"), "errors": fallback.get("errors", []), "message": fallback.get("message")}
    return primary


def _use_fallback_if_needed(primary: Dict[str, Any], scope: str, date: Optional[str] = None, event_id: Optional[str] = None, props_only: bool = False, raw: bool = False) -> Dict[str, Any]:
    if not _has_apify_config() or (primary.get("status") == "ok" and primary.get("event_count", 0) > 0):
        return primary
    try:
        fallback = fetch_apify_draftkings_event_odds(str(event_id), props_only=props_only, raw=raw) if event_id and fetch_apify_draftkings_event_odds else fetch_apify_draftkings_events(date=date, raw=raw)
    except Exception as exc:
        primary.setdefault("fallback_errors", []).append(str(exc))
        return primary
    if fallback and fallback.get("status") == "ok" and fallback.get("event_count", 0) > 0:
        fallback["primary_provider"] = {"provider": primary.get("provider"), "status": primary.get("status"), "raw_count": primary.get("raw_count"), "event_count": primary.get("event_count"), "market_count": primary.get("market_count"), "errors": primary.get("errors", []), "message": primary.get("message")}
        return fallback
    return _attach_fallback(primary, fallback)


def _fetch_kibl_or_error(scope: str, game_pk: Optional[Any] = None, props_only: bool = False, date: Optional[str] = None, raw: bool = False, league: Optional[str] = None, market_types: Optional[List[str]] = None, live_only: Optional[bool] = None, state: Optional[str] = None) -> Dict[str, Any]:
    if fetch_kibl_bet105_odds is None:
        return {"provider": "kibl_bet105", "book": "Bet105", "status": "provider_error", "scope": scope, "game_pk": game_pk, "event_id": game_pk, "markets": [], "events": [], "books": ["Bet105"], "last_updated": int(time.time()), "raw_count": 0, "event_count": 0, "market_count": 0, "errors": ["kibl_bet105_provider could not be imported"], "message": "KIBL Bet105 provider module is unavailable."}
    return fetch_kibl_bet105_odds(scope=scope, game_pk=game_pk, props_only=props_only, date=date, raw=raw, league=league, market_types=market_types, live_only=live_only, state=state)


def fetch_draftkings_odds(scope: str = "pregame", game_pk: Optional[Any] = None, props_only: bool = False, date: Optional[str] = None, raw: bool = False, league: Optional[str] = None, market_types: Optional[List[str]] = None, live_only: Optional[bool] = None, state: Optional[str] = None) -> Dict[str, Any]:
    if _should_use_kibl():
        return _fetch_kibl_or_error(scope, game_pk, props_only, date, raw, league, market_types, live_only, state)
    if _should_use_apify_first() and _has_apify_config() and game_pk is None:
        return fetch_apify_draftkings_events(date=date, raw=raw)
    token = _get_token()
    if not token:
        return _use_fallback_if_needed(_provider_not_configured(scope, game_pk=game_pk), scope=scope, date=date, event_id=game_pk, props_only=props_only, raw=raw)
    request_params = build_draftkings_run_input(scope, props_only, date, league, market_types, live_only, state)
    actual_params = dict(request_params, apiKey=token)
    cache_key = f"oddsapi:{scope}:{game_pk or 'all'}:{props_only}:{date or 'any'}:{request_params}:{raw}"
    cached = _cache_get(cache_key)
    if cached:
        return dict(cached, cache_hit=True)
    try:
        items = _fetch_odds_api(actual_params)
        events = [_normalize_event(item, bookmaker_key=os.getenv("ODDS_API_BOOKMAKERS", _DEFAULT_BOOKMAKER)) for item in items if isinstance(item, dict)]
        events = _filter_events(events, game_pk=game_pk, target_date=date)
        markets = _flatten_markets(events, game_pk=game_pk)
    except Exception as exc:
        return _use_fallback_if_needed(_provider_error(scope, game_pk, exc, request_params=request_params), scope=scope, date=date, event_id=game_pk, props_only=props_only, raw=raw)
    normalized = {"provider": "the_odds_api", "book": "DraftKings", "status": "ok" if events else "empty", "scope": scope, "sport": _ODDS_API_SPORT, "game_pk": game_pk, "event_id": game_pk, "target_date": date, "books": ["DraftKings"], "events": events, "markets": markets, "last_updated": int(time.time()), "raw_count": len(items), "event_count": len(events), "market_count": len(markets), "errors": [], "request_params": request_params, "cache_hit": False}
    if raw or scope == "debug":
        normalized["raw_items_sample"] = items[:10]
    normalized = _use_fallback_if_needed(normalized, scope=scope, date=date, event_id=game_pk, props_only=props_only, raw=raw)
    _cache_set(cache_key, normalized, int(os.getenv("ODDS_API_CACHE_TTL_SECONDS", os.getenv("DRAFTKINGS_ODDS_CACHE_TTL_SECONDS", "300"))))
    return normalized


def fetch_draftkings_event_odds(event_id: str, props_only: bool = False, raw: bool = False, market_types: Optional[List[str]] = None) -> Dict[str, Any]:
    if _should_use_kibl():
        return fetch_kibl_bet105_event_odds(event_id, props_only=props_only, raw=raw, market_types=market_types) if fetch_kibl_bet105_event_odds else _fetch_kibl_or_error("event_props" if props_only else "event", game_pk=event_id, props_only=props_only, raw=raw, market_types=market_types)
    if _should_use_apify_first() and _has_apify_config():
        return fetch_apify_draftkings_event_odds(event_id, props_only=props_only, raw=raw)
    token = _get_token()
    if not token:
        return _use_fallback_if_needed(_provider_not_configured("event", game_pk=event_id), scope="event", event_id=event_id, props_only=props_only, raw=raw)
    request_params = build_draftkings_run_input(scope="event_props" if props_only else "event", props_only=props_only, market_types=market_types)
    actual_params = dict(request_params, apiKey=token)
    cache_key = f"oddsapi:event:{event_id}:{props_only}:{request_params}:{raw}"
    cached = _cache_get(cache_key)
    if cached:
        return dict(cached, cache_hit=True)
    try:
        item = _fetch_event_odds_api(event_id, actual_params)
        event = _normalize_event(item, bookmaker_key=os.getenv("ODDS_API_BOOKMAKERS", _DEFAULT_BOOKMAKER)) if isinstance(item, dict) else None
        events = [event] if event else []
        markets = _flatten_markets(events, game_pk=event_id)
    except Exception as exc:
        return _use_fallback_if_needed(_provider_error("event_props" if props_only else "event", event_id, exc, request_params=request_params), scope="event_props" if props_only else "event", event_id=event_id, props_only=props_only, raw=raw)
    normalized = {"provider": "the_odds_api", "book": "DraftKings", "status": "ok" if events else "empty", "scope": "event_props" if props_only else "event", "sport": _ODDS_API_SPORT, "game_pk": event_id, "event_id": event_id, "books": ["DraftKings"], "events": events, "event": event, "markets": markets, "last_updated": int(time.time()), "raw_count": 1 if event else 0, "event_count": len(events), "market_count": len(markets), "errors": [], "request_params": request_params, "cache_hit": False}
    if raw:
        normalized["raw_item"] = item
    normalized = _use_fallback_if_needed(normalized, scope="event_props" if props_only else "event", event_id=event_id, props_only=props_only, raw=raw)
    _cache_set(cache_key, normalized, int(os.getenv("ODDS_API_CACHE_TTL_SECONDS", os.getenv("DRAFTKINGS_ODDS_CACHE_TTL_SECONDS", "300"))))
    return normalized


def fetch_draftkings_events(date: Optional[str] = None, raw: bool = False) -> Dict[str, Any]:
    if _should_use_kibl() and fetch_kibl_bet105_events is not None:
        return fetch_kibl_bet105_events(date=date, raw=raw)
    return fetch_draftkings_odds(scope="events", date=date, raw=raw)
