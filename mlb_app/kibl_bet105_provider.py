from __future__ import annotations

import datetime as dt
import os
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

try:
    import boto3
except Exception:  # pragma: no cover - dependency may be absent in local/dev installs
    boto3 = None

_PROVIDER = "kibl_bet105"
_BOOK = "Bet105"
_DEFAULT_REGION = "us-west-2"
_DEFAULT_CLIENT_ID = "3udv7qsqgju8c4riqvk72bqcl"
_DEFAULT_BASE_URL = "https://api.kibl.io/sports/get"
_DEFAULT_FEED_SOURCE_ID = "171"
_DEFAULT_PREMATCH_BETTING_TYPE_ID = "1"
_DEFAULT_LIVE_BETTING_TYPE_ID = "3"
_DEFAULT_TIMEOUT_SECONDS = 20
_DEFAULT_CACHE_TTL_SECONDS = 120
_TOKEN_SKEW_SECONDS = 60

_TOKEN_CACHE: Dict[str, Any] = {}
_RESPONSE_CACHE: Dict[str, Dict[str, Any]] = {}
_ET = ZoneInfo(os.getenv("KIBL_TIMEZONE", "America/New_York"))
_UTC = dt.timezone.utc

_MARKET_ALIASES = {
    "h2h": "h2h",
    "moneyline": "h2h",
    "ml": "h2h",
    "spreads": "spreads",
    "spread": "spreads",
    "run_line": "spreads",
    "runline": "spreads",
    "totals": "totals",
    "total": "totals",
    "over_under": "totals",
    "ou": "totals",
}

_SECRET_KEYS = {"password", "token", "authorization", "access_token", "accesstoken", "id_token", "refresh_token"}


def _now() -> int:
    return int(time.time())


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    entry = _RESPONSE_CACHE.get(key)
    if entry and time.time() < entry.get("expires_at", 0):
        data = dict(entry.get("data") or {})
        data["cache_hit"] = True
        return data
    return None


def _cache_set(key: str, data: Dict[str, Any], ttl: Optional[int] = None) -> None:
    ttl_seconds = ttl if ttl is not None else int(os.getenv("KIBL_CACHE_TTL_SECONDS", str(_DEFAULT_CACHE_TTL_SECONDS)))
    _RESPONSE_CACHE[key] = {"data": data, "expires_at": time.time() + ttl_seconds}


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        redacted: Dict[str, Any] = {}
        for key, item in value.items():
            lower = str(key).lower().replace("-", "_")
            redacted[key] = "***" if lower in _SECRET_KEYS or any(secret in lower for secret in _SECRET_KEYS) else _redact(item)
        return redacted
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


def _safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    number = _safe_float(value)
    if number is None:
        return None
    return int(round(number))


def _decimal_from_american(price: Optional[float]) -> Optional[float]:
    if price is None:
        return None
    try:
        price = float(price)
    except (TypeError, ValueError):
        return None
    if price > 0:
        return round(1 + price / 100, 4)
    if price < 0:
        return round(1 + 100 / abs(price), 4)
    return None


def _implied_from_american(price: Optional[float]) -> Optional[float]:
    if price is None:
        return None
    try:
        price = float(price)
    except (TypeError, ValueError):
        return None
    if price > 0:
        return round(100 / (price + 100), 4)
    if price < 0:
        return round(abs(price) / (abs(price) + 100), 4)
    return None


def _american_from_decimal(decimal_price: Optional[float]) -> Optional[int]:
    if decimal_price is None or decimal_price <= 1:
        return None
    if decimal_price >= 2:
        return int(round((decimal_price - 1) * 100))
    return int(round(-100 / (decimal_price - 1)))


def _extract_first(item: Dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in item and item.get(key) not in (None, ""):
            return item.get(key)
    return None


def _walk_dicts(value: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


def _find_list_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("events", "games", "data", "items", "results", "fixtures", "matches"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    lists: List[List[Dict[str, Any]]] = []
    for value in payload.values():
        if isinstance(value, list) and value and all(isinstance(item, dict) for item in value):
            lists.append(value)
    return max(lists, key=len) if lists else []


def _parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _format_kibl_time(value: dt.datetime) -> str:
    return value.astimezone(_ET).replace(tzinfo=None, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


def _date_window_params(date: Optional[str]) -> Dict[str, str]:
    slate = _parse_date(date)
    if not slate:
        return {}
    start = dt.datetime.combine(slate, dt.time.min, tzinfo=_ET)
    end = start + dt.timedelta(days=1)
    return {
        "start_date": _format_kibl_time(start),
        "end_date": _format_kibl_time(end),
        "from": _format_kibl_time(start),
        "to": _format_kibl_time(end),
    }


def _parse_datetime(value: Any) -> Optional[dt.datetime]:
    if not value:
        return None
    raw = str(value).strip()
    formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"]
    for fmt in formats:
        try:
            parsed = dt.datetime.strptime(raw[:19] + ("Z" if fmt.endswith("Z") and raw.endswith("Z") else ""), fmt)
            tz = _UTC if raw.endswith("Z") else _ET
            return parsed.replace(tzinfo=tz).astimezone(_UTC)
        except ValueError:
            pass
    try:
        parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_ET)
        return parsed.astimezone(_UTC)
    except ValueError:
        return None


def _iso(value: Any) -> Optional[str]:
    parsed = _parse_datetime(value)
    if not parsed:
        return None
    return parsed.astimezone(_UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _configured() -> bool:
    return bool(os.getenv("KIBL_USERNAME") and os.getenv("KIBL_PASSWORD"))


def _not_configured(scope: str, game_pk: Optional[Any] = None) -> Dict[str, Any]:
    return {
        "provider": _PROVIDER,
        "book": _BOOK,
        "status": "provider_not_configured",
        "scope": scope,
        "game_pk": game_pk,
        "event_id": game_pk,
        "markets": [],
        "events": [],
        "books": [_BOOK],
        "last_updated": None,
        "raw_count": 0,
        "event_count": 0,
        "market_count": 0,
        "errors": [],
        "message": "KIBL_USERNAME and KIBL_PASSWORD must be configured in the deployment environment.",
        "cache_hit": False,
    }


def _provider_error(scope: str, game_pk: Optional[Any], exc: Exception, request_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "provider": _PROVIDER,
        "book": _BOOK,
        "status": "provider_error",
        "scope": scope,
        "game_pk": game_pk,
        "event_id": game_pk,
        "markets": [],
        "events": [],
        "books": [_BOOK],
        "last_updated": _now(),
        "raw_count": 0,
        "event_count": 0,
        "market_count": 0,
        "errors": [str(exc)],
        "message": "The KIBL Bet105 provider failed while fetching odds.",
        "request_params": _redact(request_params or {}),
        "cache_hit": False,
    }


def _get_access_token() -> str:
    if not _configured():
        raise RuntimeError("KIBL credentials are not configured")
    cached_token = _TOKEN_CACHE.get("access_token")
    expires_at = int(_TOKEN_CACHE.get("expires_at") or 0)
    if cached_token and time.time() < expires_at - _TOKEN_SKEW_SECONDS:
        return str(cached_token)
    if boto3 is None:
        raise RuntimeError("boto3 is required for KIBL Cognito authentication")

    client = boto3.client("cognito-idp", region_name=os.getenv("KIBL_COGNITO_REGION", _DEFAULT_REGION))
    response = client.initiate_auth(
        ClientId=os.getenv("KIBL_COGNITO_CLIENT_ID", _DEFAULT_CLIENT_ID),
        AuthFlow=os.getenv("KIBL_COGNITO_AUTH_FLOW", "USER_PASSWORD_AUTH"),
        AuthParameters={
            "USERNAME": os.environ["KIBL_USERNAME"],
            "PASSWORD": os.environ["KIBL_PASSWORD"],
        },
    )
    auth = response.get("AuthenticationResult") or {}
    access_token = auth.get("AccessToken")
    if not access_token:
        raise RuntimeError("KIBL Cognito authentication did not return an access token")
    expires_in = int(auth.get("ExpiresIn") or 3600)
    _TOKEN_CACHE["access_token"] = access_token
    _TOKEN_CACHE["expires_at"] = int(time.time()) + expires_in
    return str(access_token)


def _market_filter(market_types: Optional[List[str]], props_only: bool = False) -> List[str]:
    if props_only:
        env_value = os.getenv("KIBL_PROP_MARKETS", "player_props,pitcher_props,batter_props")
    elif market_types:
        env_value = ",".join(market_types)
    else:
        env_value = os.getenv("KIBL_MARKETS", "h2h,spreads,totals")
    values: List[str] = []
    for piece in env_value.split(","):
        raw = piece.strip()
        if not raw:
            continue
        values.append(_MARKET_ALIASES.get(raw.lower(), raw))
    return values


def build_kibl_bet105_request_params(
    scope: str = "events",
    date: Optional[str] = None,
    props_only: bool = False,
    market_types: Optional[List[str]] = None,
    live_only: Optional[bool] = None,
    event_id: Optional[str] = None,
) -> Dict[str, Any]:
    is_live = bool(live_only or str(scope).lower() == "live")
    params: Dict[str, Any] = {
        "feed_source_id": os.getenv("KIBL_FEED_SOURCE_ID", _DEFAULT_FEED_SOURCE_ID),
        "betting_type_id": os.getenv(
            "KIBL_LIVE_BETTING_TYPE_ID" if is_live else "KIBL_PREMATCH_BETTING_TYPE_ID",
            _DEFAULT_LIVE_BETTING_TYPE_ID if is_live else _DEFAULT_PREMATCH_BETTING_TYPE_ID,
        ),
        "markets": ",".join(_market_filter(market_types, props_only=props_only)),
    }
    params.update(_date_window_params(date))
    if event_id:
        params["event_id"] = event_id
    return params


def _candidate_paths(scope: str, event_id: Optional[str] = None) -> List[str]:
    configured_path = os.getenv("KIBL_ODDS_PATH")
    if configured_path:
        paths = [configured_path]
    elif event_id:
        paths = ["odds", "events", "lines"]
    else:
        paths = ["odds", "events", "lines"]
    clean: List[str] = []
    for path in paths:
        value = str(path).strip("/")
        if value and value not in clean:
            clean.append(value)
    return clean


def _fetch_kibl_payload(scope: str, params: Dict[str, Any], event_id: Optional[str] = None) -> Tuple[Any, str]:
    token = _get_access_token()
    base_url = os.getenv("KIBL_BASE_URL", _DEFAULT_BASE_URL).rstrip("/")
    timeout = int(os.getenv("KIBL_TIMEOUT_SECONDS", str(_DEFAULT_TIMEOUT_SECONDS)))
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    errors: List[str] = []
    for path in _candidate_paths(scope, event_id=event_id):
        url = f"{base_url}/{path}/"
        try:
            response = requests.get(url, params=params, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response.json(), path
        except Exception as exc:
            errors.append(f"{path}: {exc}")
            if os.getenv("KIBL_ODDS_PATH"):
                break
    raise RuntimeError("; ".join(errors) or "KIBL request failed")


def _event_id(item: Dict[str, Any], fallback_index: int) -> str:
    value = _extract_first(item, ("event_id", "eventId", "game_id", "gameId", "id", "fixture_id", "match_id"))
    return str(value if value is not None else f"kibl_event_{fallback_index}")


def _team_names(item: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    home = _extract_first(item, ("home_team", "homeTeam", "home", "home_name", "homeName", "team_home"))
    away = _extract_first(item, ("away_team", "awayTeam", "away", "away_name", "awayName", "team_away"))
    if isinstance(home, dict):
        home = _extract_first(home, ("name", "team_name", "display_name"))
    if isinstance(away, dict):
        away = _extract_first(away, ("name", "team_name", "display_name"))
    competitors = item.get("competitors") or item.get("participants") or item.get("teams")
    if (not home or not away) and isinstance(competitors, list):
        for participant in competitors:
            if not isinstance(participant, dict):
                continue
            name = _extract_first(participant, ("name", "team_name", "display_name"))
            role = str(_extract_first(participant, ("home_away", "side", "type", "qualifier")) or "").lower()
            if "home" in role and not home:
                home = name
            elif "away" in role and not away:
                away = name
        if len(competitors) >= 2:
            away = away or _extract_first(competitors[0], ("name", "team_name", "display_name"))
            home = home or _extract_first(competitors[1], ("name", "team_name", "display_name"))
    return str(away) if away else None, str(home) if home else None


def _market_key(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    value = re.sub(r"[^a-z0-9_]+", "_", value).strip("_")
    return _MARKET_ALIASES.get(value, value or "unknown_market")


def _price_from_selection(selection: Dict[str, Any]) -> Optional[int]:
    american = _extract_first(selection, ("american", "american_odds", "odds_american", "price", "line_price", "moneyline"))
    parsed = _safe_int(american)
    if parsed is not None and abs(parsed) >= 100:
        return parsed
    decimal_price = _safe_float(_extract_first(selection, ("decimal", "decimal_odds", "odds_decimal")))
    return _american_from_decimal(decimal_price)


def _line_from_selection(selection: Dict[str, Any], market: Dict[str, Any]) -> Optional[float]:
    return _safe_float(_extract_first(selection, ("line", "point", "points", "handicap", "total", "value")) or _extract_first(market, ("line", "point", "points", "handicap", "total", "value")))


def _selection_name(selection: Dict[str, Any], market: Dict[str, Any]) -> Optional[str]:
    value = _extract_first(selection, ("name", "selection", "outcome", "label", "team", "participant", "player_name", "description"))
    if isinstance(value, dict):
        value = _extract_first(value, ("name", "display_name", "team_name"))
    if value is None:
        side = _extract_first(selection, ("side", "designation"))
        if side:
            value = side
    return str(value) if value is not None else None


def _normalize_selection(selection: Dict[str, Any], market: Dict[str, Any], index: int) -> Dict[str, Any]:
    price = _price_from_selection(selection)
    name = _selection_name(selection, market)
    line = _line_from_selection(selection, market)
    return {
        "selection_id": _extract_first(selection, ("id", "selection_id", "outcome_id")) or f"selection_{index}",
        "name": name,
        "description": _extract_first(selection, ("description", "label", "market_description")),
        "team": _extract_first(selection, ("team", "team_name")) or name,
        "side": _extract_first(selection, ("side", "designation")) or name,
        "line": line,
        "odds": {
            "american": price,
            "decimal": _decimal_from_american(price),
            "fractional": None,
            "implied_probability": _implied_from_american(price),
        },
        "price": price,
        "is_open": bool(_extract_first(selection, ("is_open", "open", "active", "is_active")) if _extract_first(selection, ("is_open", "open", "active", "is_active")) is not None else True),
        "raw": selection,
    }


def _selection_lists(market: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("selections", "outcomes", "runners", "prices", "odds", "lines"):
        value = market.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _market_lists(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("markets", "bookmakers", "odds", "lines", "prices"):
        value = item.get(key)
        if isinstance(value, list) and any(isinstance(child, dict) for child in value):
            return [child for child in value if isinstance(child, dict)]
    discovered: List[Dict[str, Any]] = []
    for child in _walk_dicts(item):
        if child is item:
            continue
        if _selection_lists(child):
            discovered.append(child)
    return discovered


def _normalize_market(market: Dict[str, Any], index: int) -> Dict[str, Any]:
    raw_key = _extract_first(market, ("market_key", "market_type", "market", "type", "name", "description", "key"))
    market_key = _market_key(raw_key)
    selections = [_normalize_selection(selection, market, idx) for idx, selection in enumerate(_selection_lists(market))]
    return {
        "market_id": _extract_first(market, ("id", "market_id", "key")) or f"market_{index}",
        "market_key": market_key,
        "market_name": raw_key or market_key,
        "market_type": market_key,
        "line": _safe_float(_extract_first(market, ("line", "point", "points", "handicap", "total"))),
        "period": _extract_first(market, ("period", "period_name")),
        "is_open": bool(_extract_first(market, ("is_open", "open", "active", "is_active")) if _extract_first(market, ("is_open", "open", "active", "is_active")) is not None else True),
        "last_update": _extract_first(market, ("last_update", "updated_at", "timestamp")),
        "bookmaker_key": "bet105",
        "bookmaker_title": _BOOK,
        "selections": selections,
        "raw": market,
    }


def _normalize_event(item: Dict[str, Any], index: int, is_live: bool = False) -> Dict[str, Any]:
    event_id = _event_id(item, index)
    away, home = _team_names(item)
    start_time = _iso(_extract_first(item, ("start_time", "commence_time", "game_time", "scheduled", "event_time", "startDate", "eventDate")))
    markets = [_normalize_market(market, idx) for idx, market in enumerate(_market_lists(item))]
    return {
        "event_id": event_id,
        "name": f"{away} @ {home}" if away or home else str(_extract_first(item, ("name", "event_name", "description")) or event_id),
        "sport": _extract_first(item, ("sport", "sport_title", "sport_name")) or "Baseball",
        "league": _extract_first(item, ("league", "league_name", "competition", "sport_key")) or "MLB",
        "league_id": _extract_first(item, ("league_id", "competition_id")) or "mlb",
        "home_team": {"name": home},
        "away_team": {"name": away},
        "start_time": start_time,
        "status": _extract_first(item, ("status", "event_status")) or ("live" if is_live else "scheduled"),
        "is_live": bool(is_live or _extract_first(item, ("is_live", "live", "in_play")) is True),
        "source_url": None,
        "scraped_at": _now(),
        "markets": markets,
        "market_count": len(markets),
        "raw": item,
    }


def _flatten_markets(events: List[Dict[str, Any]], game_pk: Optional[Any] = None) -> List[Dict[str, Any]]:
    flat: List[Dict[str, Any]] = []
    for event in events:
        event_id = event.get("event_id")
        if game_pk is not None and event_id is not None and str(event_id) != str(game_pk):
            continue
        for market in event.get("markets", []) or []:
            row = dict(market)
            row.pop("raw", None)
            row["event_id"] = event_id
            row["event_name"] = event.get("name")
            row["league"] = event.get("league")
            row["league_id"] = event.get("league_id")
            row["start_time"] = event.get("start_time")
            row["is_live"] = event.get("is_live")
            row["source_url"] = event.get("source_url")
            flat.append(row)
    return flat


def _without_raw_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    for event in events:
        event_copy = dict(event)
        event_copy.pop("raw", None)
        event_copy["markets"] = []
        for market in event.get("markets", []) or []:
            market_copy = dict(market)
            market_copy.pop("raw", None)
            market_copy["selections"] = []
            for selection in market.get("selections", []) or []:
                selection_copy = dict(selection)
                selection_copy.pop("raw", None)
                market_copy["selections"].append(selection_copy)
            event_copy["markets"].append(market_copy)
        cleaned.append(event_copy)
    return cleaned


def fetch_kibl_bet105_odds(
    scope: str = "events",
    game_pk: Optional[Any] = None,
    props_only: bool = False,
    date: Optional[str] = None,
    raw: bool = False,
    league: Optional[str] = None,
    market_types: Optional[List[str]] = None,
    live_only: Optional[bool] = None,
    state: Optional[str] = None,
) -> Dict[str, Any]:
    if not _configured():
        return _not_configured(scope, game_pk=game_pk)
    params = build_kibl_bet105_request_params(scope, date=date, props_only=props_only, market_types=market_types, live_only=live_only, event_id=str(game_pk) if game_pk is not None else None)
    cache_key = f"kibl:{scope}:{game_pk or 'all'}:{props_only}:{date or 'any'}:{params}:{raw}:{live_only}"
    cached = _cache_get(cache_key)
    if cached:
        return cached
    try:
        payload, path = _fetch_kibl_payload(scope, params, event_id=str(game_pk) if game_pk is not None else None)
        items = _find_list_payload(payload)
        is_live = bool(live_only or str(scope).lower() == "live")
        events = [_normalize_event(item, idx, is_live=is_live) for idx, item in enumerate(items)]
        if game_pk is not None:
            events = [event for event in events if str(event.get("event_id")) == str(game_pk)]
        markets = _flatten_markets(events, game_pk=game_pk)
    except Exception as exc:
        return _provider_error(scope, game_pk, exc, request_params=params)
    normalized: Dict[str, Any] = {
        "provider": _PROVIDER,
        "book": _BOOK,
        "status": "ok" if events else "empty",
        "scope": scope,
        "sport": league or "baseball_mlb",
        "game_pk": game_pk,
        "event_id": game_pk,
        "target_date": date,
        "books": [_BOOK],
        "events": events if raw else _without_raw_events(events),
        "markets": markets,
        "last_updated": _now(),
        "raw_count": len(items),
        "event_count": len(events),
        "market_count": len(markets),
        "errors": [],
        "request_params": _redact({**params, "path": path}),
        "cache_hit": False,
    }
    if raw or scope == "debug":
        normalized["raw_items_sample"] = items[:10]
    _cache_set(cache_key, normalized)
    return normalized


def fetch_kibl_bet105_event_odds(event_id: str, props_only: bool = False, raw: bool = False, market_types: Optional[List[str]] = None) -> Dict[str, Any]:
    payload = fetch_kibl_bet105_odds(
        scope="event_props" if props_only else "event",
        game_pk=event_id,
        props_only=props_only,
        raw=raw,
        market_types=market_types,
    )
    events = payload.get("events") or []
    payload["event"] = events[0] if events else None
    return payload


def fetch_kibl_bet105_events(date: Optional[str] = None, raw: bool = False, live_only: Optional[bool] = None) -> Dict[str, Any]:
    return fetch_kibl_bet105_odds(scope="live" if live_only else "events", date=date, raw=raw, live_only=live_only)
