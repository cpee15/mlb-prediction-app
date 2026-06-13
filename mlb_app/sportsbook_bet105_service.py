from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import kibl_bet105_provider as base
from . import kibl_bet105_sportsbook_enrichment as enrichment
from . import kibl_bet105_sportsbook_provider as legacy


def _walk_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


def _is_fixture_candidate(item: Dict[str, Any]) -> bool:
    if base._extract_first(item, ("fixture_id", "fixtureId", "fixtureID", "event_id", "eventId")) is None:
        return False
    competitors = item.get("participants") or item.get("competitors") or item.get("teams")
    if isinstance(competitors, list) and any(isinstance(child, dict) for child in competitors):
        return True
    if base._extract_first(item, base._HOME_KEYS) is not None or base._extract_first(item, base._AWAY_KEYS) is not None:
        return True
    return False


def _fixture_items_from_payload(payload: Any) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in _walk_dicts(payload):
        if not _is_fixture_candidate(item):
            continue
        fixture_id = base._extract_first(item, ("fixture_id", "fixtureId", "fixtureID", "event_id", "eventId", "id"))
        fingerprint = str(fixture_id) if fixture_id is not None else str(id(item))
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        items.append(item)
    return items


def _incomplete_count(events: List[Dict[str, Any]]) -> Dict[str, int]:
    placeholder_event_names = 0
    placeholder_market_names = 0
    placeholder_selection_names = 0
    missing_start_times = 0
    for event in events:
        if enrichment.placeholder_event_name(event.get("name")):
            placeholder_event_names += 1
        if not event.get("start_time"):
            missing_start_times += 1
        for market in event.get("markets") or []:
            market_key = str(market.get("market_key") or market.get("market_type") or "")
            if enrichment.placeholder_market_name(market.get("market_name"), market_key):
                placeholder_market_names += 1
            for selection in market.get("selections") or []:
                if enrichment.placeholder_selection_text(selection.get("name")) or enrichment.placeholder_selection_text(selection.get("description")):
                    placeholder_selection_names += 1
    return {
        "placeholder_event_names": placeholder_event_names,
        "placeholder_market_names": placeholder_market_names,
        "placeholder_selection_names": placeholder_selection_names,
        "missing_start_times": missing_start_times,
    }


def fetch_board(
    date: Optional[str] = None,
    raw: bool = False,
    live_only: Optional[bool] = None,
    game_pk: Optional[Any] = None,
    props_only: bool = False,
    market_types: Optional[List[str]] = None,
) -> Dict[str, Any]:
    scope = "event_props" if props_only and game_pk is not None else ("event" if game_pk is not None else ("live" if live_only else "events"))
    if not base._configured():
        return base._not_configured(scope, game_pk=game_pk)

    is_live = bool(live_only or str(scope).lower() == "live")
    params = base.build_kibl_bet105_request_params(
        scope,
        date=date,
        props_only=props_only,
        market_types=market_types,
        live_only=live_only,
        event_id=str(game_pk) if game_pk is not None else None,
    )
    cache_key = f"kibl-sportsbook:{scope}:{game_pk or 'all'}:{props_only}:{date or 'any'}:{params}:{raw}:{live_only}:fixture-first-v7"
    cached = base._cache_get(cache_key)
    if cached:
        return cached

    notes: List[str] = []
    fixture_items: List[Dict[str, Any]] = []
    fixture_events: List[Dict[str, Any]] = []
    market_items: List[Dict[str, Any]] = []
    market_events: List[Dict[str, Any]] = []
    request_path: Optional[str] = None
    request_params: Dict[str, Any] = dict(params)

    try:
        try:
            fixture_payload, fixture_path = base._fetch_kibl_payload(scope, params, event_id=str(game_pk) if game_pk is not None else None, kind="fixtures")
            fixture_items = _fixture_items_from_payload(fixture_payload)
            fixture_events = [
                {
                    "event_id": meta.get("event_id"),
                    "name": meta.get("name"),
                    "sport": meta.get("sport") or "Baseball",
                    "league": meta.get("league") or "MLB",
                    "league_id": meta.get("league_id") or "mlb",
                    "home_team": meta.get("home_team"),
                    "away_team": meta.get("away_team"),
                    "start_time": meta.get("start_time"),
                    "status": meta.get("status") or ("live" if is_live else "scheduled"),
                    "is_live": bool(is_live if meta.get("is_live") is None else meta.get("is_live")),
                    "source_url": None,
                    "scraped_at": base._now(),
                    "markets": [],
                    "market_count": 0,
                    "raw": item,
                }
                for item, meta in ((item, enrichment.fixture_metadata_from_item(item, idx)) for idx, item in enumerate(fixture_items))
                if meta.get("event_id")
            ]
            notes.append(f"fixtures_direct:{fixture_path}:{len(fixture_items)}:{len(fixture_events)}")
        except Exception as exc:
            notes.append(f"fixtures_direct_error:{exc}")

        ids = legacy._fixture_ids(fixture_items, fixture_events)
        best_flattened_market_count = 0
        for label, body_base, bodies in legacy._market_body_sets(params, ids):
            for body in bodies:
                try:
                    _, market_path, items, events = base._fetch_items(scope, body, game_pk, is_live, kind="markets")
                    flattened_market_count = legacy._flattened_market_count(events, game_pk=game_pk)
                    notes.append(f"markets_{label}:{market_path}:{len(items)}:{len(events)}:{flattened_market_count}:{','.join(sorted(set(body) - set(body_base))) or 'base'}")
                    if legacy._prefer_market_candidate(events, market_events, game_pk=game_pk):
                        market_items = items
                        market_events = events
                        request_path = market_path
                        request_params = body
                        best_flattened_market_count = flattened_market_count
                    # Keep scanning every request body. KIBL can return partial market boards
                    # for early request shapes, so stopping after the first non-empty response
                    # can lock production into one-event/one-market incomplete_normalization.
                except Exception as exc:
                    notes.append(f"markets_{label}_error:{exc}")

        if params.get("markets"):
            retry_params = base.build_kibl_bet105_request_params(
                scope,
                date=None,
                props_only=props_only,
                market_types=None,
                live_only=live_only,
                event_id=str(game_pk) if game_pk is not None else None,
                include_markets=False,
            )
            for body in legacy._market_request_bodies(retry_params, ids):
                try:
                    _, market_path, items, events = base._fetch_items(scope, body, game_pk, is_live, kind="markets")
                    flattened_market_count = legacy._flattened_market_count(events, game_pk=game_pk)
                    notes.append(f"markets_no_filter_core:{market_path}:{len(items)}:{len(events)}:{flattened_market_count}:{','.join(sorted(set(body) - set(retry_params))) or 'base'}")
                    if legacy._prefer_market_candidate(events, market_events, game_pk=game_pk):
                        market_items = items
                        market_events = events
                        request_path = market_path
                        request_params = body
                        best_flattened_market_count = flattened_market_count
                    # Do not break here either; the unfiltered response may also be partial.
                except Exception as exc:
                    notes.append(f"markets_no_filter_core_error:{exc}")

        events = enrichment.enrich_market_events_with_fixture_metadata(market_events, fixture_items, fixture_events) if market_events else fixture_events
        markets = base._flatten_markets(events, game_pk=game_pk)
        diagnostics = _incomplete_count(events)
        status = "ok" if markets else ("fixtures_only" if events else "empty")
        if status == "ok" and any(diagnostics.values()):
            status = "incomplete_normalization"
    except Exception as exc:
        return base._provider_error(scope, game_pk, exc, request_params=params)

    raw_debug_items = (market_items or []) + (fixture_items or [])
    normalized: Dict[str, Any] = {
        "provider": base._PROVIDER,
        "book": base._BOOK,
        "status": status,
        "scope": scope,
        "sport": "baseball_mlb",
        "game_pk": game_pk,
        "event_id": game_pk,
        "target_date": date,
        "books": [base._BOOK],
        "events": events if raw else base._without_raw_events(events),
        "markets": markets,
        "last_updated": base._now(),
        "raw_count": len(market_items or fixture_items),
        "event_count": len(events),
        "market_count": len(markets),
        "errors": [],
        "request_params": base._redact({**request_params, "path": request_path or "info/markets"}),
        "cache_hit": False,
        "normalization_notes": notes,
    }
    if raw or scope == "debug":
        normalized["raw_items_sample"] = base._redact(raw_debug_items[:10])
        normalized["diagnostics"] = diagnostics
        normalized["fixtures"] = {
            "count": len(fixture_items),
            "fixture_ids": [str(base._extract_first(item, ("fixture_id", "fixtureId", "fixtureID", "event_id", "eventId", "id"))) for item in fixture_items[:20]],
        }
        normalized["markets_meta"] = {
            "row_count": len(market_items),
            "market_type_ids": [base._extract_first(item, ("market_type_id", "marketTypeId")) for item in market_items[:20]],
            "best_flattened_market_count": best_flattened_market_count,
        }
    base._cache_set(cache_key, normalized)
    return normalized


def fetch_event_board(event_id: str, props_only: bool = False, raw: bool = False, market_types: Optional[List[str]] = None) -> Dict[str, Any]:
    payload = fetch_board(game_pk=event_id, props_only=props_only, raw=raw, market_types=market_types)
    events = payload.get("events") or []
    payload["event"] = events[0] if events else None
    return payload
