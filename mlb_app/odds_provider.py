import json
import os
import time
from typing import Any, Dict, List, Optional

try:
    from apify_client import ApifyClient
except ImportError:
    ApifyClient = None

_CACHE: Dict[str, Dict[str, Any]] = {}
_ALLOWED_STATES = {
    "Arizona", "Colorado", "Illinois", "Indiana", "Iowa", "Louisiana",
    "Maryland", "Michigan", "New Jersey", "New York", "Ohio",
    "Pennsylvania", "Tennessee", "Virginia", "West Virginia", "Wyoming",
}
_STATE_ABBREVIATIONS = {
    "AZ": "Arizona", "CO": "Colorado", "IL": "Illinois", "IN": "Indiana",
    "IA": "Iowa", "LA": "Louisiana", "MD": "Maryland", "MI": "Michigan",
    "NJ": "New Jersey", "NY": "New York", "OH": "Ohio", "PA": "Pennsylvania",
    "TN": "Tennessee", "VA": "Virginia", "WV": "West Virginia", "WY": "Wyoming",
}
_DEFAULT_MLB_LEAGUE_ID = "84240"
_DEFAULT_MARKET_TYPES = ["all"]


def _cache_get(key: str):
    entry = _CACHE.get(key)
    if entry and time.time() < entry["expires_at"]:
        return entry["data"]
    return None


def _cache_set(key: str, data: Any, ttl: int = 300):
    _CACHE[key] = {"data": data, "expires_at": time.time() + ttl}


def _normalize_state(value: Optional[str]) -> str:
    raw = (value or "Illinois").strip()
    if not raw:
        return "Illinois"
    upper = raw.upper()
    if upper in _STATE_ABBREVIATIONS:
        return _STATE_ABBREVIATIONS[upper]
    title = raw.title()
    if title in _ALLOWED_STATES:
        return title
    return "Illinois"


def _parse_csv(value: Optional[str], fallback: List[str]) -> List[str]:
    if not value:
        return fallback
    parsed = [v.strip() for v in value.split(",") if v.strip()]
    return parsed or fallback


def _json_env(name: str) -> Optional[Dict[str, Any]]:
    raw = os.getenv(name)
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _provider_not_configured(scope: str, game_pk: Optional[int] = None, message: str = "APIFY_TOKEN is not configured.") -> Dict[str, Any]:
    return {
        "provider": "draftkings",
        "status": "provider_not_configured",
        "scope": scope,
        "game_pk": game_pk,
        "markets": [],
        "events": [],
        "books": ["DraftKings"],
        "last_updated": None,
        "raw_count": 0,
        "event_count": 0,
        "market_count": 0,
        "errors": [],
        "message": message,
    }


def _provider_error(scope: str, game_pk: Optional[int], exc: Exception, run_input: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "provider": "draftkings",
        "status": "provider_error",
        "scope": scope,
        "game_pk": game_pk,
        "markets": [],
        "events": [],
        "books": ["DraftKings"],
        "last_updated": int(time.time()),
        "raw_count": 0,
        "event_count": 0,
        "market_count": 0,
        "errors": [str(exc)],
        "message": "DraftKings odds provider failed while fetching Apify data.",
        "run_input": run_input or {},
    }


def _first_present(row: Dict[str, Any], keys: List[str]) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _odds_dict(selection: Dict[str, Any]) -> Dict[str, Any]:
    odds = selection.get("odds") if isinstance(selection.get("odds"), dict) else {}
    american = _first_present(odds, ["american", "americanOdds", "oddsAmerican"])
    if american is None:
        american = _first_present(selection, ["americanOdds", "american_odds", "oddsAmerican", "price"])
    decimal = _first_present(odds, ["decimal", "decimalOdds"])
    fractional = _first_present(odds, ["fractional", "fractionalOdds"])
    implied = _first_present(odds, ["impliedProbability", "implied_probability"])
    return {
        "american": american,
        "decimal": decimal,
        "fractional": fractional,
        "implied_probability": implied,
    }


def _normalize_selection(selection: Dict[str, Any], market: Dict[str, Any]) -> Dict[str, Any]:
    odds = _odds_dict(selection)
    return {
        "selection_id": _first_present(selection, ["id", "selectionId", "outcomeId"]),
        "name": _first_present(selection, ["name", "label", "outcome", "participant", "playerName", "teamName"]),
        "team": _first_present(selection, ["team", "teamAbbreviation", "teamName"]),
        "side": _first_present(selection, ["side", "homeAway", "designation"]),
        "line": _first_present(selection, ["line", "points", "handicap", "total", "spread"]) or _first_present(market, ["line", "points", "total", "spread"]),
        "odds": odds,
        "price": odds.get("american"),
        "is_open": selection.get("isOpen", selection.get("is_open", market.get("isOpen"))),
        "raw": selection,
    }


def _normalize_event(item: Dict[str, Any]) -> Dict[str, Any]:
    event_markets = item.get("markets") if isinstance(item.get("markets"), list) else []
    markets: List[Dict[str, Any]] = []
    for market in event_markets:
        if not isinstance(market, dict):
            continue
        selections = market.get("selections") if isinstance(market.get("selections"), list) else []
        markets.append({
            "market_id": _first_present(market, ["id", "marketId"]),
            "market_key": _first_present(market, ["type", "marketType", "market_key"]),
            "market_name": _first_present(market, ["name", "marketName", "label"]),
            "market_type": _first_present(market, ["type", "marketType"]),
            "line": _first_present(market, ["line", "points", "total", "spread"]),
            "period": _first_present(market, ["period", "periodName"]),
            "is_open": market.get("isOpen", market.get("is_open")),
            "selections": [_normalize_selection(sel, market) for sel in selections if isinstance(sel, dict)],
            "raw": market,
        })
    return {
        "event_id": _first_present(item, ["eventId", "event_id", "id"]),
        "name": _first_present(item, ["name", "eventName"]),
        "sport": item.get("sport"),
        "league": item.get("league"),
        "league_id": _first_present(item, ["leagueId", "league_id"]),
        "home_team": item.get("homeTeam"),
        "away_team": item.get("awayTeam"),
        "start_time": _first_present(item, ["startTime", "start_time", "commence_time"]),
        "status": item.get("status"),
        "is_live": bool(item.get("isLive", item.get("is_live", False))),
        "source_url": _first_present(item, ["sourceUrl", "source_url"]),
        "scraped_at": _first_present(item, ["scrapedAt", "scraped_at"]),
        "markets": markets,
        "market_count": len(markets),
        "raw": item,
    }


def _flatten_markets(events: List[Dict[str, Any]], game_pk: Optional[int] = None) -> List[Dict[str, Any]]:
    flat: List[Dict[str, Any]] = []
    for event in events:
        event_id = event.get("event_id")
        if game_pk is not None and event_id is not None and str(event_id) != str(game_pk):
            continue
        for market in event.get("markets", []):
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


def _filter_events(events: List[Dict[str, Any]], game_pk: Optional[int], target_date: Optional[str]) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for event in events:
        if game_pk is not None and event.get("event_id") is not None and str(event.get("event_id")) != str(game_pk):
            continue
        if target_date:
            start_time = event.get("start_time") or ""
            if start_time and not str(start_time).startswith(target_date):
                continue
        filtered.append(event)
    return filtered


def build_draftkings_run_input(
    scope: str = "pregame",
    props_only: bool = False,
    date: Optional[str] = None,
    league: Optional[str] = None,
    market_types: Optional[List[str]] = None,
    live_only: Optional[bool] = None,
    state: Optional[str] = None,
) -> Dict[str, Any]:
    override = _json_env("DRAFTKINGS_ODDS_RUN_INPUT_JSON")
    if override is not None:
        return override
    if props_only:
        resolved_market_types = ["player_props"]
    else:
        resolved_market_types = market_types or _parse_csv(os.getenv("DRAFTKINGS_ODDS_MARKET_TYPES"), _DEFAULT_MARKET_TYPES)
    resolved_league = league or os.getenv("DRAFTKINGS_ODDS_LEAGUE", _DEFAULT_MLB_LEAGUE_ID)
    return {
        "leagues": [resolved_league],
        "marketTypes": resolved_market_types,
        "maxEvents": int(os.getenv("DRAFTKINGS_ODDS_MAX_EVENTS", "500")),
        "liveOnly": scope == "live" if live_only is None else live_only,
        "oddsFormat": os.getenv("DRAFTKINGS_ODDS_FORMAT", "all"),
        "usState": _normalize_state(state or os.getenv("DRAFTKINGS_ODDS_STATE", "Illinois")),
        "requestDelay": int(os.getenv("DRAFTKINGS_ODDS_REQUEST_DELAY_MS", "500")),
    }


def _run_actor(token: str, run_input: Dict[str, Any]) -> List[Dict[str, Any]]:
    client = ApifyClient(token)
    run = client.actor(os.getenv("DRAFTKINGS_ODDS_ACTOR_ID", "mherzog/draftkings-sportsbook-odds")).call(run_input=run_input)
    dataset_id = run.get("defaultDatasetId") or run.get("default_dataset_id")
    if not dataset_id:
        return []
    return list(client.dataset(dataset_id).iterate_items())


def fetch_draftkings_odds(
    scope: str = "pregame",
    game_pk: Optional[int] = None,
    props_only: bool = False,
    date: Optional[str] = None,
    raw: bool = False,
    league: Optional[str] = None,
    market_types: Optional[List[str]] = None,
    live_only: Optional[bool] = None,
    state: Optional[str] = None,
) -> Dict[str, Any]:
    if ApifyClient is None:
        return _provider_not_configured(scope, game_pk=game_pk, message="apify-client is not installed in the running image.")
    token = os.getenv("APIFY_TOKEN")
    if not token:
        return _provider_not_configured(scope, game_pk=game_pk)

    run_input = build_draftkings_run_input(scope, props_only, date, league, market_types, live_only, state)
    cache_key = f"dk:{scope}:{game_pk or 'all'}:{props_only}:{date or 'any'}:{json.dumps(run_input, sort_keys=True)}:{raw}"
    cached = _cache_get(cache_key)
    if cached:
        cached_copy = dict(cached)
        cached_copy["cache_hit"] = True
        return cached_copy

    try:
        items = _run_actor(token, run_input)
        events = [_normalize_event(item) for item in items if isinstance(item, dict)]
        events = _filter_events(events, game_pk=game_pk, target_date=date)
        markets = _flatten_markets(events, game_pk=game_pk)
    except Exception as exc:
        return _provider_error(scope, game_pk, exc, run_input=run_input)

    normalized = {
        "provider": "draftkings",
        "status": "ok" if items else "empty",
        "scope": scope,
        "sport": "baseball_mlb",
        "game_pk": game_pk,
        "target_date": date,
        "books": ["DraftKings"],
        "events": events,
        "markets": markets,
        "last_updated": int(time.time()),
        "raw_count": len(items),
        "event_count": len(events),
        "market_count": len(markets),
        "errors": [],
        "run_input": run_input,
        "cache_hit": False,
    }
    if raw or scope == "debug":
        normalized["raw_items_sample"] = items[:10]
    ttl = int(os.getenv("DRAFTKINGS_ODDS_CACHE_TTL_SECONDS", "300"))
    _cache_set(cache_key, normalized, ttl)
    return normalized
