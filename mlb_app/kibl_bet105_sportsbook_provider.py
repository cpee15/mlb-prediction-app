from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import kibl_bet105_provider as base


def _fixture_ids(fixture_items: List[Dict[str, Any]], fixture_events: List[Dict[str, Any]]) -> List[str]:
    ids: List[str] = []
    for idx, row in enumerate(fixture_items):
        try:
            value = base._event_id(row, idx)
        except Exception:
            value = None
        if value and value not in ids:
            ids.append(str(value))
    for event in fixture_events:
        value = event.get("event_id")
        if value and str(value) not in ids:
            ids.append(str(value))
    return ids


def _market_request_bodies(params: Dict[str, Any], fixture_ids: List[str]) -> List[Dict[str, Any]]:
    bodies: List[Dict[str, Any]] = [dict(params)]
    if not fixture_ids:
        return bodies

    for key in ("fixture_ids", "event_ids"):
        bodies.append({**params, key: fixture_ids})
        bodies.append({**params, key: ",".join(fixture_ids)})

    for key in ("fixture_id", "event_id", "id"):
        for value in fixture_ids:
            bodies.append({**params, key: value})

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for body in bodies:
        fingerprint = tuple(sorted((key, str(value)) for key, value in body.items()))
        if fingerprint not in seen:
            seen.add(fingerprint)
            deduped.append(body)
    return deduped


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
    cache_key = f"kibl-sportsbook:{scope}:{game_pk or 'all'}:{props_only}:{date or 'any'}:{params}:{raw}:{live_only}:fixture-first-v1"
    cached = base._cache_get(cache_key)
    if cached:
        return cached

    notes: List[str] = []
    raw_items: List[Dict[str, Any]] = []
    fixture_items: List[Dict[str, Any]] = []
    fixture_events: List[Dict[str, Any]] = []
    market_items: List[Dict[str, Any]] = []
    market_events: List[Dict[str, Any]] = []
    request_path: Optional[str] = None
    request_params: Dict[str, Any] = dict(params)

    try:
        try:
            _, fixture_path, fixture_items, fixture_events = base._fetch_items(scope, params, game_pk, is_live, kind="fixtures")
            notes.append(f"fixtures:{fixture_path}:{len(fixture_items)}:{len(fixture_events)}")
        except Exception as exc:
            notes.append(f"fixtures_error:{exc}")

        ids = _fixture_ids(fixture_items, fixture_events)
        for body in _market_request_bodies(params, ids):
            try:
                _, market_path, items, events = base._fetch_items(scope, body, game_pk, is_live, kind="markets")
                notes.append(f"markets:{market_path}:{len(items)}:{len(events)}:{','.join(sorted(set(body) - set(params))) or 'base'}")
                if len(events) > len(market_events) or sum(event.get("market_count", 0) for event in events) > sum(event.get("market_count", 0) for event in market_events):
                    market_items = items
                    market_events = events
                    request_path = market_path
                    request_params = body
                if base._flatten_markets(events, game_pk=game_pk):
                    break
            except Exception as exc:
                notes.append(f"markets_error:{exc}")

        if not market_events and params.get("markets"):
            retry_params = base.build_kibl_bet105_request_params(
                scope,
                date=date,
                props_only=props_only,
                market_types=None,
                live_only=live_only,
                event_id=str(game_pk) if game_pk is not None else None,
                include_markets=False,
            )
            for body in _market_request_bodies(retry_params, ids):
                try:
                    _, market_path, items, events = base._fetch_items(scope, body, game_pk, is_live, kind="markets")
                    notes.append(f"markets_no_filter:{market_path}:{len(items)}:{len(events)}:{','.join(sorted(set(body) - set(retry_params))) or 'base'}")
                    if events:
                        market_items = items
                        market_events = events
                        request_path = market_path
                        request_params = body
                        break
                except Exception as exc:
                    notes.append(f"markets_no_filter_error:{exc}")

        events = base._merge_fixture_metadata(market_events, fixture_events) if market_events else fixture_events
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
