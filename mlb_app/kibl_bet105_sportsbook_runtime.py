from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import kibl_bet105_provider as base
from . import kibl_bet105_sportsbook_enrichment as enrichment
from . import kibl_bet105_sportsbook_provider as legacy


def fetch_kibl_bet105_events(date: Optional[str] = None, raw: bool = False, live_only: Optional[bool] = None) -> Dict[str, Any]:
    scope = "live" if live_only else "events"
    return fetch_kibl_bet105_odds(scope=scope, date=date, raw=raw, live_only=live_only)


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
    cache_key = f"kibl-sportsbook:{scope}:{game_pk or 'all'}:{props_only}:{date or 'any'}:{params}:{raw}:{live_only}:fixture-first-v5"
    cached = base._cache_get(cache_key)
    if cached:
        return cached

    notes: List[str] = []
    raw_items: List[Dict[str, Any]] = []
    fixture_items: List[Dict[str, Any]] = []
    fixture_events: List[Dict[str, Any]] = []
    market_items: List[Dict[str, Any]] = []
    market_events: List[Dict[str, Any]] = []
    best_flattened_market_count = 0
    request_path: Optional[str] = None
    request_params: Dict[str, Any] = dict(params)

    try:
        try:
            _, fixture_path, fixture_items, fixture_events = base._fetch_items(scope, params, game_pk, is_live, kind="fixtures")
            notes.append(f"fixtures:{fixture_path}:{len(fixture_items)}:{len(fixture_events)}")
        except Exception as exc:
            notes.append(f"fixtures_error:{exc}")

        ids = legacy._fixture_ids(fixture_items, fixture_events)
        for label, body_base, bodies in legacy._market_body_sets(params, ids):
            for body in bodies:
                try:
                    _, market_path, items, events = base._fetch_items(scope, body, game_pk, is_live, kind="markets")
                    flattened_market_count = legacy._flattened_market_count(events, game_pk=game_pk)
                    notes.append(
                        f"markets_{label}:{market_path}:{len(items)}:{len(events)}:{flattened_market_count}:{','.join(sorted(set(body) - set(body_base))) or 'base'}"
                    )
                    if legacy._prefer_market_candidate(events, market_events, game_pk=game_pk):
                        market_items = items
                        market_events = events
                        request_path = market_path
                        request_params = body
                        best_flattened_market_count = legacy._flattened_market_count(market_events, game_pk=game_pk)
                    if flattened_market_count > 0:
                        break
                except Exception as exc:
                    notes.append(f"markets_{label}_error:{exc}")
            if best_flattened_market_count > 0:
                break

        if best_flattened_market_count == 0 and params.get("markets"):
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
                    notes.append(
                        f"markets_no_filter_core:{market_path}:{len(items)}:{len(events)}:{flattened_market_count}:{','.join(sorted(set(body) - set(retry_params))) or 'base'}"
                    )
                    if legacy._prefer_market_candidate(events, market_events, game_pk=game_pk):
                        market_items = items
                        market_events = events
                        request_path = market_path
                        request_params = body
                        best_flattened_market_count = legacy._flattened_market_count(market_events, game_pk=game_pk)
                    if flattened_market_count > 0:
                        break
                except Exception as exc:
                    notes.append(f"markets_no_filter_core_error:{exc}")

        if market_events:
            market_events = base._merge_fixture_metadata(market_events, fixture_events)
            events = enrichment.enrich_market_events_with_fixture_metadata(market_events, fixture_items, fixture_events)
        else:
            events = fixture_events
        markets = base._flatten_markets(events, game_pk=game_pk)
        raw_items = market_items or fixture_items
    except Exception as exc:
        return base._provider_error(scope, game_pk, exc, request_params=params)

    status = "ok" if markets else ("fixtures_only" if events else "empty")
    normalized: Dict[str, Any] = {
        "provider": base._PROVIDER,
        "book": base._BOOK,
        "status": status,
        "scope": scope,
        "sport": league or "baseball_mlb",
        "game_pk": game_pk,
        "event_id": game_pk,
        "target_date": date,
        "books": [base._BOOK],
        "events": events if raw else base._without_raw_events(events),
        "markets": markets,
        "last_updated": base._now(),
        "raw_count": len(raw_items),
        "event_count": len(events),
        "market_count": len(markets),
        "errors": [],
        "request_params": base._redact({**request_params, "path": request_path or "info/markets"}),
        "cache_hit": False,
        "normalization_notes": notes,
    }
    if raw or scope == "debug":
        normalized["raw_items_sample"] = base._redact(raw_items[:10])
    base._cache_set(cache_key, normalized)
    return normalized
