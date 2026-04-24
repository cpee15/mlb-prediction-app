import os
import time
from typing import Any, Dict, List, Optional, Tuple

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
_DEFAULT_MARKET_TYPES = ["moneyline", "spread", "total", "player_props"]


def _cache_get(key: str):
    entry = _CACHE.get(key)
    if entry and time.time() < entry["expires_at"]:
        return entry["data"]
    return None


def _cache_set(key: str, data: Any, ttl: int = 60):
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


def _provider_not_configured(scope: str, game_pk: Optional[int] = None, message: str = "APIFY_TOKEN is not configured.") -> Dict[str, Any]:
    return {
        "provider": "draftkings",
        "status": "provider_not_configured",
        "scope": scope,
        "game_pk": game_pk,
        "markets": [],
        "books": ["DraftKings"],
        "last_updated": None,
        "raw_count": 0,
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
        "books": ["DraftKings"],
        "last_updated": int(time.time()),
        "raw_count": 0,
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


def _decimal_price(price: Any) -> Optional[float]:
    n = _to_float(price)
    if n is None:
        return None
    if n > 0:
        return round((n / 100) + 1, 4)
    if n < 0:
        return round((100 / abs(n)) + 1, 4)
    return None


def _implied_probability(price: Any) -> Optional[float]:
    n = _to_float(price)
    if n is None:
        return None
    if n > 0:
        return round(100 / (n + 100), 4)
    if n < 0:
        return round(abs(n) / (abs(n) + 100), 4)
    return None


def _price_value(value: Any) -> Any:
    if isinstance(value, dict):
        return _first_present(value, ["american", "americanOdds", "oddsAmerican", "price"])
    return value


def _normalize_selection(row: Dict[str, Any]) -> Dict[str, Any]:
    price = _price_value(_first_present(row, ["price", "odds", "americanOdds", "american_odds", "oddsAmerican", "displayOdds"]))
    return {
        "name": _first_present(row, ["name", "selection", "outcome", "label", "participant", "playerName", "teamName"]),
        "team": _first_present(row, ["team", "teamAbbreviation", "team_abbreviation", "teamName"]),
        "side": _first_present(row, ["side", "homeAway", "designation"]),
        "line": _first_present(row, ["line", "points", "handicap", "total", "spread"]),
        "price": price,
        "decimal_price": _decimal_price(price),
        "implied_probability": _implied_probability(price),
        "raw": row,
    }


def _normalize_items(items: List[Dict[str, Any]], game_pk: Optional[int] = None) -> List[Dict[str, Any]]:
    markets: List[Dict[str, Any]] = []
    for idx, item in enumerate(items):
        if game_pk is not None:
            raw_game_pk = _first_present(item, ["game_pk", "gamePk", "mlbGamePk", "eventId", "event_id"])
            if raw_game_pk is not None and str(raw_game_pk) != str(game_pk):
                continue

        market_name = _first_present(item, ["marketName", "market_name", "market", "name", "categoryName"])
        market_key = _first_present(item, ["marketKey", "market_key", "marketType", "market_type", "category"])
        selections = _first_present(item, ["selections", "outcomes", "runners", "offers", "participants"])

        if isinstance(selections, list):
            normalized_selections = [_normalize_selection(sel) for sel in selections if isinstance(sel, dict)]
        else:
            normalized_selections = [_normalize_selection(item)]

        markets.append({
            "market_key": str(market_key or market_name or f"market_{idx}"),
            "market_name": str(market_name or market_key or "DraftKings Market"),
            "market_type": _first_present(item, ["marketType", "market_type", "type"]),
            "period": _first_present(item, ["period", "periodName", "period_name"]),
            "game_pk": _first_present(item, ["game_pk", "gamePk", "mlbGamePk"]),
            "event_id": _first_present(item, ["eventId", "event_id", "id"]),
            "start_time": _first_present(item, ["startTime", "start_time", "commence_time"]),
            "is_live": bool(_first_present(item, ["isLive", "is_live", "live"])),
            "selections": normalized_selections,
            "raw": item,
        })
    return markets


def build_draftkings_run_input(
    scope: str = "pregame",
    props_only: bool = False,
    date: Optional[str] = None,
    league: Optional[str] = None,
    market_types: Optional[List[str]] = None,
    live_only: Optional[bool] = None,
    state: Optional[str] = None,
) -> Dict[str, Any]:
    resolved_market_types = market_types or (["player_props"] if props_only else _parse_csv(os.getenv("DRAFTKINGS_ODDS_MARKET_TYPES"), _DEFAULT_MARKET_TYPES))
    run_input: Dict[str, Any] = {
        "leagues": [league or os.getenv("DRAFTKINGS_ODDS_LEAGUE", "MLB")],
        "marketTypes": resolved_market_types,
        "maxEvents": int(os.getenv("DRAFTKINGS_ODDS_MAX_EVENTS", "500")),
        "liveOnly": scope == "live" if live_only is None else live_only,
        "usState": _normalize_state(state or os.getenv("DRAFTKINGS_ODDS_STATE", "Illinois")),
    }
    if date:
        run_input["date"] = date
    return run_input


def build_draftkings_run_input_candidates(
    scope: str = "pregame",
    props_only: bool = False,
    date: Optional[str] = None,
    league: Optional[str] = None,
    market_types: Optional[List[str]] = None,
    live_only: Optional[bool] = None,
    state: Optional[str] = None,
) -> List[Dict[str, Any]]:
    full = build_draftkings_run_input(scope, props_only, date, league, market_types, live_only, state)
    resolved_live = scope == "live" if live_only is None else live_only
    resolved_state = full["usState"]
    resolved_league = league or os.getenv("DRAFTKINGS_ODDS_LEAGUE", "MLB")
    resolved_markets = full.get("marketTypes") or _DEFAULT_MARKET_TYPES
    candidates = [
        full,
        {"usState": resolved_state, "liveOnly": resolved_live, "maxEvents": full["maxEvents"]},
        {"usState": resolved_state, "liveOnly": resolved_live},
        {"usState": resolved_state},
        {"state": resolved_state, "liveOnly": resolved_live, "league": resolved_league},
        {"usState": resolved_state, "sports": [resolved_league], "liveOnly": resolved_live},
        {"usState": resolved_state, "leagues": [resolved_league], "liveOnly": resolved_live},
        {"usState": resolved_state, "marketTypes": resolved_markets, "liveOnly": resolved_live},
    ]
    unique: List[Dict[str, Any]] = []
    seen = set()
    for candidate in candidates:
        key = str(sorted(candidate.items()))
        if key not in seen:
            seen.add(key)
            unique.append(candidate)
    return unique


def _run_actor(token: str, run_input: Dict[str, Any]) -> List[Dict[str, Any]]:
    client = ApifyClient(token)
    run = client.actor(os.getenv("DRAFTKINGS_ODDS_ACTOR_ID", "mherzog/draftkings-sportsbook-odds")).call(run_input=run_input)
    dataset_id = run.get("defaultDatasetId") or run.get("default_dataset_id")
    if not dataset_id:
        return []
    return list(client.dataset(dataset_id).iterate_items())


def _run_actor_with_fallbacks(token: str, candidates: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]]]:
    attempts: List[Dict[str, Any]] = []
    last_items: List[Dict[str, Any]] = []
    last_input = candidates[0] if candidates else {}
    for run_input in candidates:
        try:
            items = _run_actor(token, run_input)
            attempts.append({"run_input": run_input, "raw_count": len(items), "error": None})
            last_items = items
            last_input = run_input
            if items:
                return items, run_input, attempts
        except Exception as exc:
            attempts.append({"run_input": run_input, "raw_count": 0, "error": str(exc)})
            last_input = run_input
    return last_items, last_input, attempts


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

    candidates = build_draftkings_run_input_candidates(
        scope=scope,
        props_only=props_only,
        date=date,
        league=league,
        market_types=market_types,
        live_only=live_only,
        state=state,
    )

    cache_key = f"dk:{scope}:{game_pk or 'all'}:{props_only}:{date or 'any'}:{str(candidates)}:{raw}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    try:
        items, used_input, attempts = _run_actor_with_fallbacks(token, candidates)
        markets = _normalize_items(items, game_pk=game_pk)
    except Exception as exc:
        return _provider_error(scope, game_pk, exc, run_input=candidates[0] if candidates else {})

    normalized = {
        "provider": "draftkings",
        "status": "ok" if items else "empty",
        "scope": scope,
        "sport": "baseball_mlb",
        "game_pk": game_pk,
        "target_date": date,
        "books": ["DraftKings"],
        "markets": markets,
        "last_updated": int(time.time()),
        "raw_count": len(items),
        "market_count": len(markets),
        "errors": [],
        "run_input": used_input,
        "attempts": attempts,
    }
    if raw or scope == "debug":
        normalized["raw_items_sample"] = items[:10]

    ttl = int(os.getenv("DRAFTKINGS_ODDS_CACHE_TTL_SECONDS", "60"))
    _cache_set(cache_key, normalized, ttl)
    return normalized
