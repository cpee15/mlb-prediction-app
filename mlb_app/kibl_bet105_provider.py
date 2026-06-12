from __future__ import annotations

import datetime as dt
import os
import re
import time
from collections import defaultdict
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
_DEFAULT_LEAGUE_ID = "20,643"
_DEFAULT_PREMATCH_BETTING_TYPE_ID = "1"
_DEFAULT_LIVE_BETTING_TYPE_ID = "3"
_DEFAULT_TIMEOUT_SECONDS = 20
_DEFAULT_CACHE_TTL_SECONDS = 120
_TOKEN_SKEW_SECONDS = 60

_TOKEN_CACHE: Dict[str, Any] = {}
_RESPONSE_CACHE: Dict[str, Dict[str, Any]] = {}
_ET = ZoneInfo(os.getenv("KIBL_TIMEZONE", "America/New_York"))
_UTC = dt.timezone.utc

_SECRET_KEYS = {"password", "token", "authorization", "access_token", "accesstoken", "id_token", "refresh_token"}

# KIBL's Bet105 feed uses POSTs to /sports/get/info/fixtures/ and /sports/get/info/markets/.
# Keep legacy path names as a last-resort fallback for environment overrides or older feed variants.
_FIXTURE_PATHS = ("info/fixtures", "fixtures", "events")
_MARKET_PATHS = ("info/markets", "markets", "odds", "lines")
_DEBUG_PATHS = ("info/markets", "info/fixtures", "markets", "fixtures", "odds", "events", "lines")

_LIST_KEYS = (
    "events", "games", "data", "items", "results", "fixtures", "matches",
    "odds", "lines", "rows", "records", "markets", "prices", "tickets",
)
_EVENT_ID_KEYS = (
    "event_id", "eventId", "eventID", "fixture_id", "fixtureId", "fixtureID",
    "game_id", "gameId", "gameID", "match_id", "matchId", "id", "event",
    "event_key", "kibl_event_id", "sports_event_id",
)
_HOME_KEYS = (
    "home_team", "homeTeam", "home_team_name", "homeTeamName", "home", "home_name",
    "homeName", "team_home", "home_participant", "homeParticipant", "home_competitor",
)
_AWAY_KEYS = (
    "away_team", "awayTeam", "away_team_name", "awayTeamName", "away", "away_name",
    "awayName", "team_away", "away_participant", "awayParticipant", "away_competitor",
)
_START_KEYS = (
    "start_time", "startTime", "commence_time", "commenceTime", "game_time", "gameTime",
    "scheduled", "scheduled_time", "event_time", "eventTime", "startDate", "start_date",
    "eventDate", "event_date", "date", "match_date", "fixture_date",
)
_MARKET_KEYS = (
    "market_key", "marketKey", "market_type", "marketType", "market_name", "marketName",
    "market", "bet_type", "betType", "wager_type", "wagerType", "type", "name",
    "description", "key", "label", "market_type_id",
)
_SELECTION_KEYS = (
    "selection", "selection_name", "selectionName", "outcome", "outcome_name", "outcomeName",
    "runner", "runner_name", "label", "team", "team_name", "participant", "participant_name",
    "player_name", "playerName", "name", "side", "designation", "description",
)
_PRICE_KEYS = (
    "american", "american_odds", "americanOdds", "odds_american", "price", "line_price",
    "moneyline", "odds", "current_price", "currentPrice", "value", "odds_value", "price_american",
)
_DECIMAL_KEYS = ("decimal", "decimal_odds", "decimalOdds", "odds_decimal", "price_decimal")
_LINE_KEYS = ("line", "point", "points", "handicap", "spread", "total", "threshold")

_MARKET_ALIASES = {
    "h2h": "h2h",
    "head_to_head": "h2h",
    "moneyline": "h2h",
    "money_line": "h2h",
    "money_line_3_way": "h2h",
    "ml": "h2h",
    "1": "h2h",
    "spreads": "spreads",
    "spread": "spreads",
    "point_spread": "spreads",
    "run_line": "spreads",
    "runline": "spreads",
    "2": "spreads",
    "totals": "totals",
    "total": "totals",
    "total_runs": "totals",
    "over_under": "totals",
    "ou": "totals",
    "3": "totals",
}

_SIDE_ID_LABELS = {1: "away", 2: "home", 3: "over", 4: "under", 5: "draw"}


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
    return int(round(number)) if number is not None else None


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


def _nested_name(value: Any) -> Optional[str]:
    if isinstance(value, dict):
        direct = _extract_first(value, ("name", "display_name", "displayName", "team_name", "teamName", "fullName", "title"))
        if direct not in (None, ""):
            return str(direct)
        for nested_key in ("team", "participant", "competitor", "competitorTeam", "runner", "selection"):
            nested = _nested_name(value.get(nested_key))
            if nested:
                return nested
        return None
    if isinstance(value, list):
        for child in value:
            nested = _nested_name(child)
            if nested:
                return nested
        return None
    if value in (None, ""):
        return None
    return str(value)


def _walk_dicts(value: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


def _walk_lists(value: Any) -> Iterable[List[Dict[str, Any]]]:
    if isinstance(value, dict):
        for key in _LIST_KEYS:
            child = value.get(key)
            if isinstance(child, list) and any(isinstance(item, dict) for item in child):
                yield [item for item in child if isinstance(item, dict)]
        for child in value.values():
            yield from _walk_lists(child)
    elif isinstance(value, list):
        if any(isinstance(item, dict) for item in value):
            yield [item for item in value if isinstance(item, dict)]
        for child in value:
            yield from _walk_lists(child)


def _row_signal(row: Dict[str, Any]) -> int:
    keys = set(row.keys())
    score = 0
    if keys.intersection(_EVENT_ID_KEYS):
        score += 3
    if keys.intersection(_HOME_KEYS) or keys.intersection(_AWAY_KEYS):
        score += 4
    if keys.intersection(_MARKET_KEYS):
        score += 2
    if keys.intersection(_SELECTION_KEYS):
        score += 1
    if keys.intersection(_PRICE_KEYS) or keys.intersection(_DECIMAL_KEYS):
        score += 3
    if keys.intersection(_START_KEYS):
        score += 1
    return score


def _find_list_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    candidates = list(_walk_lists(payload))
    if not candidates:
        return []
    return max(candidates, key=lambda rows: (sum(_row_signal(row) for row in rows[:50]), len(rows)))


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
        AuthParameters={"USERNAME": os.environ["KIBL_USERNAME"], "PASSWORD": os.environ["KIBL_PASSWORD"]},
    )
    auth = response.get("AuthenticationResult") or {}
    access_token = auth.get("AccessToken")
    if not access_token:
        raise RuntimeError("KIBL Cognito authentication did not return an access token")
    expires_in = int(auth.get("ExpiresIn") or 3600)
    _TOKEN_CACHE["access_token"] = access_token
    _TOKEN_CACHE["expires_at"] = int(time.time()) + expires_in
    return str(access_token)


def _post_kibl_json(url: str, params: Dict[str, Any], timeout: int) -> Any:
    def _headers(token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Accept": "application/json", "Content-Type": "application/json"}

    token = _get_access_token()
    response = requests.post(url, json=params, headers=_headers(token), timeout=timeout)
    if response.status_code == 401:
        _TOKEN_CACHE.clear()
        token = _get_access_token()
        response = requests.post(url, json=params, headers=_headers(token), timeout=timeout)
    response.raise_for_status()
    return response.json()


def _market_filter(market_types: Optional[List[str]], props_only: bool = False) -> List[str]:
    if props_only:
        env_value = os.getenv("KIBL_PROP_MARKETS", "")
    elif market_types:
        env_value = ",".join(market_types)
    else:
        env_value = os.getenv("KIBL_MARKETS", "")
    values: List[str] = []
    for piece in env_value.split(","):
        raw = piece.strip()
        if raw:
            values.append(_MARKET_ALIASES.get(raw.lower(), raw))
    return values


def build_kibl_bet105_request_params(scope: str = "events", date: Optional[str] = None, props_only: bool = False, market_types: Optional[List[str]] = None, live_only: Optional[bool] = None, event_id: Optional[str] = None, include_markets: bool = True) -> Dict[str, Any]:
    is_live = bool(live_only or str(scope).lower() == "live")
    params: Dict[str, Any] = {
        "feed_source_id": int(os.getenv("KIBL_FEED_SOURCE_ID", _DEFAULT_FEED_SOURCE_ID)),
        "betting_type_id": int(os.getenv("KIBL_LIVE_BETTING_TYPE_ID" if is_live else "KIBL_PREMATCH_BETTING_TYPE_ID", _DEFAULT_LIVE_BETTING_TYPE_ID if is_live else _DEFAULT_PREMATCH_BETTING_TYPE_ID)),
        "league_id": os.getenv("KIBL_LEAGUE_ID", _DEFAULT_LEAGUE_ID),
        "from_cache": False,
    }
    markets = _market_filter(market_types, props_only=props_only) if include_markets else []
    if markets:
        params["markets"] = ",".join(markets)
    params.update(_date_window_params(date))
    if event_id:
        params["event_id"] = event_id
    return params


def _candidate_paths(scope: str, event_id: Optional[str] = None, kind: str = "markets") -> List[str]:
    configured_path = os.getenv("KIBL_ODDS_PATH")
    if configured_path:
        paths = [configured_path]
    elif kind == "fixtures":
        paths = list(_FIXTURE_PATHS)
    elif kind == "debug":
        paths = list(_DEBUG_PATHS)
    else:
        paths = list(_MARKET_PATHS)
    clean: List[str] = []
    for path in paths:
        value = str(path).strip("/")
        if value and value not in clean:
            clean.append(value)
    return clean


def _payload_item_count(payload: Any) -> int:
    return len(_find_list_payload(payload))


def _fetch_kibl_payload(scope: str, params: Dict[str, Any], event_id: Optional[str] = None, kind: str = "markets") -> Tuple[Any, str]:
    base_url = os.getenv("KIBL_BASE_URL", _DEFAULT_BASE_URL).rstrip("/")
    timeout = int(os.getenv("KIBL_TIMEOUT_SECONDS", str(_DEFAULT_TIMEOUT_SECONDS)))
    errors: List[str] = []
    first_success: Optional[Tuple[Any, str]] = None
    for path in _candidate_paths(scope, event_id=event_id, kind=kind):
        url = f"{base_url}/{path}/"
        try:
            payload = _post_kibl_json(url, params, timeout)
            if first_success is None:
                first_success = (payload, path)
            if _payload_item_count(payload) > 0:
                return payload, path
        except Exception as exc:
            errors.append(f"{path}: {exc}")
            if os.getenv("KIBL_ODDS_PATH"):
                break
    if first_success is not None:
        return first_success
    raise RuntimeError("; ".join(errors) or "KIBL request failed")


def _market_key(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    value = re.sub(r"[^a-z0-9_]+", "_", value).strip("_")
    return _MARKET_ALIASES.get(value, value or "unknown_market")


def _price_from_selection(selection: Dict[str, Any]) -> Optional[int]:
    american = _extract_first(selection, _PRICE_KEYS)
    if isinstance(american, dict):
        nested = _extract_first(american, ("american", "american_odds", "americanOdds", "price", "price_american"))
        parsed = _safe_int(nested)
        if parsed is not None:
            return parsed
        return _american_from_decimal(_safe_float(_extract_first(american, _DECIMAL_KEYS)))
    parsed = _safe_int(american)
    if parsed is not None:
        return parsed
    return _american_from_decimal(_safe_float(_extract_first(selection, _DECIMAL_KEYS)))


def _line_from_selection(selection: Dict[str, Any], market: Optional[Dict[str, Any]] = None) -> Optional[float]:
    return _safe_float(_extract_first(selection, _LINE_KEYS) or _extract_first(market or {}, _LINE_KEYS))


def _team_name_for_side(selection: Dict[str, Any], market: Optional[Dict[str, Any]] = None) -> Optional[str]:
    market = market or {}
    side_id = _safe_int(_extract_first(selection, ("participant_side_id", "side_id", "sideId", "participantSideId")))
    if side_id == 1:
        return _nested_name(_extract_first(market, ("away_team", "awayTeam", "away", "away_name", "awayName"))) or "Away"
    if side_id == 2:
        return _nested_name(_extract_first(market, ("home_team", "homeTeam", "home", "home_name", "homeName"))) or "Home"
    return None


def _selection_name(selection: Dict[str, Any], market: Optional[Dict[str, Any]] = None) -> Optional[str]:
    value = _extract_first(selection, _SELECTION_KEYS)
    if isinstance(value, dict):
        value = _nested_name(value)
    if value is None:
        value = _team_name_for_side(selection, market)
    if value is None:
        side = _safe_int(_extract_first(selection, ("participant_side_id", "side_id", "sideId", "participantSideId")))
        if side in _SIDE_ID_LABELS:
            value = _SIDE_ID_LABELS[side].title()
    return str(value) if value is not None else None


def _normalize_selection(selection: Dict[str, Any], market: Optional[Dict[str, Any]], index: int) -> Dict[str, Any]:
    price = _price_from_selection(selection)
    name = _selection_name(selection, market) or "Selection"
    line = _line_from_selection(selection, market)
    return {
        "selection_id": _extract_first(selection, ("id", "selection_id", "selectionId", "outcome_id", "outcomeId", "fixture_participant_id", "participant_id")) or f"selection_{index}",
        "name": name,
        "description": _extract_first(selection, ("description", "label", "market_description", "marketDescription")) or name,
        "team": _nested_name(_extract_first(selection, ("team", "team_name", "participant"))) or _team_name_for_side(selection, market) or name,
        "side": _extract_first(selection, ("side", "designation")) or _SIDE_ID_LABELS.get(_safe_int(_extract_first(selection, ("participant_side_id", "side_id", "sideId", "participantSideId"))), name),
        "line": line,
        "odds": {"american": price, "decimal": _safe_float(_extract_first(selection, _DECIMAL_KEYS)) or _decimal_from_american(price), "fractional": _extract_first(selection, ("price_fraction", "fractional", "fractional_odds")), "implied_probability": _implied_from_american(price)},
        "price": price,
        "is_open": bool(_extract_first(selection, ("is_current", "is_open", "open", "active", "is_active")) if _extract_first(selection, ("is_current", "is_open", "open", "active", "is_active")) is not None else True),
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
    raw_key = _extract_first(market, _MARKET_KEYS)
    market_key = _market_key(raw_key)
    selections = [_normalize_selection(selection, market, idx) for idx, selection in enumerate(_selection_lists(market))]
    return {
        "market_id": _extract_first(market, ("id", "market_id", "marketId", "key", "market_type_id")) or f"market_{index}",
        "market_key": market_key,
        "market_name": raw_key or market_key,
        "market_type": market_key,
        "line": _safe_float(_extract_first(market, _LINE_KEYS)),
        "period": _extract_first(market, ("period", "period_name", "periodName", "segment_id")),
        "is_open": bool(_extract_first(market, ("is_current", "is_open", "open", "active", "is_active")) if _extract_first(market, ("is_current", "is_open", "open", "active", "is_active")) is not None else True),
        "last_update": _extract_first(market, ("last_update", "lastUpdate", "updated_at", "updatedAt", "timestamp", "inserted_on")),
        "bookmaker_key": "bet105",
        "bookmaker_title": _BOOK,
        "selections": selections,
        "raw": market,
    }


def _event_id(item: Dict[str, Any], fallback_index: int) -> str:
    value = _extract_first(item, _EVENT_ID_KEYS)
    if isinstance(value, dict):
        value = _extract_first(value, ("id", "event_id", "eventId"))
    if value is not None:
        return str(value)
    away, home = _team_names(item)
    start = _extract_first(item, _START_KEYS)
    if away or home or start:
        return re.sub(r"[^a-zA-Z0-9]+", "_", f"{away}_{home}_{start}").strip("_")
    return f"kibl_event_{fallback_index}"


def _team_names(item: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    home = _nested_name(_extract_first(item, _HOME_KEYS))
    away = _nested_name(_extract_first(item, _AWAY_KEYS))
    competitors = item.get("competitors") or item.get("participants") or item.get("teams")
    if (not home or not away) and isinstance(competitors, list):
        for participant in competitors:
            if not isinstance(participant, dict):
                continue
            name = _nested_name(participant)
            role = str(_extract_first(participant, ("home_away", "homeAway", "side", "type", "qualifier")) or "").lower()
            if "home" in role and not home:
                home = name
            elif "away" in role and not away:
                away = name
        if len(competitors) >= 2:
            away = away or _nested_name(competitors[0])
            home = home or _nested_name(competitors[1])
    return str(away) if away else None, str(home) if home else None


def _normalize_event(item: Dict[str, Any], index: int, is_live: bool = False) -> Dict[str, Any]:
    event_id = _event_id(item, index)
    away, home = _team_names(item)
    start_time = _iso(_extract_first(item, _START_KEYS))
    markets = [_normalize_market(market, idx) for idx, market in enumerate(_market_lists(item))]
    return {
        "event_id": event_id,
        "name": f"{away} @ {home}" if away or home else str(_extract_first(item, ("name", "event_name", "eventName", "description")) or event_id),
        "sport": _extract_first(item, ("sport", "sport_title", "sport_name", "sportName")) or "Baseball",
        "league": _extract_first(item, ("league", "league_name", "leagueName", "competition", "sport_key")) or "MLB",
        "league_id": _extract_first(item, ("league_id", "leagueId", "competition_id")) or "mlb",
        "home_team": {"name": home},
        "away_team": {"name": away},
        "start_time": start_time,
        "status": _extract_first(item, ("status", "event_status", "eventStatus")) or ("live" if is_live else "scheduled"),
        "is_live": bool(is_live or _extract_first(item, ("is_live", "isLive", "live", "in_play", "inPlay")) is True),
        "source_url": None,
        "scraped_at": _now(),
        "markets": markets,
        "market_count": len(markets),
        "raw": item,
    }


def _looks_like_flat_odds_row(row: Dict[str, Any]) -> bool:
    has_price = _extract_first(row, _PRICE_KEYS) is not None or _extract_first(row, _DECIMAL_KEYS) is not None
    has_market_or_side = _extract_first(row, _MARKET_KEYS) is not None or _extract_first(row, _SELECTION_KEYS) is not None or _extract_first(row, ("participant_side_id", "side_id", "sideId")) is not None
    return bool(has_price and has_market_or_side)


def _event_name_from_row(row: Dict[str, Any], event_id: str, away: Optional[str], home: Optional[str]) -> str:
    explicit = _extract_first(row, ("event_name", "eventName", "matchup", "name", "description"))
    if explicit:
        return str(explicit)
    if away or home:
        return f"{away} @ {home}"
    return event_id


def _events_from_flat_rows(rows: List[Dict[str, Any]], is_live: bool = False) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for idx, row in enumerate(rows):
        if not isinstance(row, dict) or not _looks_like_flat_odds_row(row):
            continue
        groups[_event_id(row, idx)].append(row)

    events: List[Dict[str, Any]] = []
    for event_index, (event_id, event_rows) in enumerate(groups.items()):
        seed = event_rows[0]
        away, home = _team_names(seed)
        start_time = _iso(_extract_first(seed, _START_KEYS))
        market_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in event_rows:
            raw_market = _extract_first(row, _MARKET_KEYS) or _extract_first(seed, _MARKET_KEYS) or "market"
            line = _line_from_selection(row)
            key = f"{_market_key(raw_market)}:{line if line is not None else 'none'}"
            market_groups[key].append(row)

        markets: List[Dict[str, Any]] = []
        for market_index, (market_group_key, market_rows) in enumerate(market_groups.items()):
            first = dict(market_rows[0])
            first.setdefault("away_team", {"name": away} if away else None)
            first.setdefault("home_team", {"name": home} if home else None)
            raw_key = _extract_first(first, _MARKET_KEYS) or market_group_key.split(":", 1)[0]
            market_key = _market_key(raw_key)
            line = _line_from_selection(first)
            markets.append({
                "market_id": _extract_first(first, ("market_id", "marketId", "id", "key", "market_type_id")) or f"{event_id}_market_{market_index}",
                "market_key": market_key,
                "market_name": raw_key or market_key,
                "market_type": market_key,
                "line": line,
                "period": _extract_first(first, ("period", "period_name", "periodName", "segment_id")),
                "is_open": bool(_extract_first(first, ("is_current", "is_open", "open", "active", "is_active")) if _extract_first(first, ("is_current", "is_open", "open", "active", "is_active")) is not None else True),
                "last_update": _extract_first(first, ("last_update", "lastUpdate", "updated_at", "updatedAt", "timestamp", "inserted_on")),
                "bookmaker_key": "bet105",
                "bookmaker_title": _BOOK,
                "selections": [_normalize_selection({**row, "away_team": first.get("away_team"), "home_team": first.get("home_team")}, first, idx) for idx, row in enumerate(market_rows)],
                "raw": {"rows": market_rows},
            })

        events.append({
            "event_id": event_id,
            "name": _event_name_from_row(seed, event_id, away, home),
            "sport": _extract_first(seed, ("sport", "sport_title", "sport_name", "sportName")) or "Baseball",
            "league": _extract_first(seed, ("league", "league_name", "leagueName", "competition", "sport_key")) or "MLB",
            "league_id": _extract_first(seed, ("league_id", "leagueId", "competition_id")) or "mlb",
            "home_team": {"name": home},
            "away_team": {"name": away},
            "start_time": start_time,
            "status": _extract_first(seed, ("status", "event_status", "eventStatus")) or ("live" if is_live else "scheduled"),
            "is_live": bool(is_live or _extract_first(seed, ("is_live", "isLive", "live", "in_play", "inPlay")) is True),
            "source_url": None,
            "scraped_at": _now(),
            "markets": markets,
            "market_count": len(markets),
            "raw": {"rows": event_rows},
        })
    return events


def _normalize_payload_items(items: List[Dict[str, Any]], is_live: bool = False) -> List[Dict[str, Any]]:
    nested_events = [_normalize_event(item, idx, is_live=is_live) for idx, item in enumerate(items)]
    nested_events = [event for event in nested_events if event.get("markets")]
    flat_events = _events_from_flat_rows(items, is_live=is_live)
    if flat_events and (not nested_events or sum(event.get("market_count", 0) for event in flat_events) >= sum(event.get("market_count", 0) for event in nested_events)):
        return flat_events
    if nested_events:
        return nested_events
    return [_normalize_event(item, idx, is_live=is_live) for idx, item in enumerate(items) if _row_signal(item) > 0]


def _merge_fixture_metadata(market_events: List[Dict[str, Any]], fixture_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not fixture_events:
        return market_events
    fixture_index: Dict[str, Dict[str, Any]] = {str(fixture.get("event_id")): fixture for fixture in fixture_events}
    merged: List[Dict[str, Any]] = []
    for event in market_events:
        fixture = fixture_index.get(str(event.get("event_id")))
        if fixture:
            for key in ("name", "sport", "league", "league_id", "home_team", "away_team", "start_time", "status", "is_live"):
                if not event.get(key) or event.get(key) in ({"name": None}, {"name": ""}):
                    event[key] = fixture.get(key)
        merged.append(event)
    return merged


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


def _fetch_items(scope: str, params: Dict[str, Any], game_pk: Optional[Any], is_live: bool, kind: str) -> Tuple[Any, str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    payload, path = _fetch_kibl_payload(scope, params, event_id=str(game_pk) if game_pk is not None else None, kind=kind)
    items = _find_list_payload(payload)
    events = _normalize_payload_items(items, is_live=is_live)
    if game_pk is not None:
        events = [event for event in events if str(event.get("event_id")) == str(game_pk)]
    return payload, path, items, events


def fetch_kibl_bet105_odds(scope: str = "events", game_pk: Optional[Any] = None, props_only: bool = False, date: Optional[str] = None, raw: bool = False, league: Optional[str] = None, market_types: Optional[List[str]] = None, live_only: Optional[bool] = None, state: Optional[str] = None) -> Dict[str, Any]:
    if not _configured():
        return _not_configured(scope, game_pk=game_pk)
    is_live = bool(live_only or str(scope).lower() == "live")
    params = build_kibl_bet105_request_params(scope, date=date, props_only=props_only, market_types=market_types, live_only=live_only, event_id=str(game_pk) if game_pk is not None else None)
    cache_key = f"kibl:{scope}:{game_pk or 'all'}:{props_only}:{date or 'any'}:{params}:{raw}:{live_only}:post-info-v2"
    cached = _cache_get(cache_key)
    if cached:
        return cached
    retry_notes: List[str] = []
    raw_items: List[Dict[str, Any]] = []
    request_path = None
    try:
        _, market_path, market_items, market_events = _fetch_items(scope, params, game_pk, is_live, kind="markets")
        raw_items = market_items
        request_path = market_path
        fixture_items: List[Dict[str, Any]] = []
        fixture_events: List[Dict[str, Any]] = []
        try:
            _, fixture_path, fixture_items, fixture_events = _fetch_items(scope, params, game_pk, is_live, kind="fixtures")
            retry_notes.append(f"fixtures_path:{fixture_path}")
        except Exception as exc:
            retry_notes.append(f"fixtures_fetch_skipped:{exc}")
        events = _merge_fixture_metadata(market_events, fixture_events) if market_events else fixture_events
        if fixture_items and len(fixture_items) > len(raw_items):
            raw_items = fixture_items if not market_items else market_items + fixture_items
        if not events and params.get("markets"):
            retry_params = build_kibl_bet105_request_params(scope, date=date, props_only=props_only, market_types=None, live_only=live_only, event_id=str(game_pk) if game_pk is not None else None, include_markets=False)
            _, retry_path, retry_items, retry_events = _fetch_items(scope, retry_params, game_pk, is_live, kind="markets")
            retry_notes.append("retried_without_markets_filter")
            if retry_events or len(retry_items) > len(raw_items):
                params, raw_items, events, request_path = retry_params, retry_items, retry_events, retry_path
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
        "raw_count": len(raw_items),
        "event_count": len(events),
        "market_count": len(markets),
        "errors": [],
        "request_params": _redact({**params, "path": request_path}),
        "cache_hit": False,
        "normalization_notes": retry_notes,
    }
    if raw or scope == "debug":
        normalized["raw_items_sample"] = _redact(raw_items[:10])
    _cache_set(cache_key, normalized)
    return normalized


def fetch_kibl_bet105_event_odds(event_id: str, props_only: bool = False, raw: bool = False, market_types: Optional[List[str]] = None) -> Dict[str, Any]:
    payload = fetch_kibl_bet105_odds(scope="event_props" if props_only else "event", game_pk=event_id, props_only=props_only, raw=raw, market_types=market_types)
    events = payload.get("events") or []
    payload["event"] = events[0] if events else None
    return payload


def fetch_kibl_bet105_events(date: Optional[str] = None, raw: bool = False, live_only: Optional[bool] = None) -> Dict[str, Any]:
    return fetch_kibl_bet105_odds(scope="live" if live_only else "events", date=date, raw=raw, live_only=live_only)
