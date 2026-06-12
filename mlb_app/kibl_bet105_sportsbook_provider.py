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


def _core_market_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """Keep KIBL feed filters, but drop fixture date-window filters for markets."""
    date_keys = {"start_date", "end_date", "from", "to"}
    return {key: value for key, value in params.items() if key not in date_keys}


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


def _market_body_sets(params: Dict[str, Any], fixture_ids: List[str]) -> List[tuple[str, Dict[str, Any], List[Dict[str, Any]]]]:
    core_params = _core_market_params(params)
    return [
        ("dated", params, _market_request_bodies(params, fixture_ids)),
        ("core", core_params, _market_request_bodies(core_params, fixture_ids)),
    ]


def _flattened_market_count(events: List[Dict[str, Any]], game_pk: Optional[Any] = None) -> int:
    return len(base._flatten_markets(events, game_pk=game_pk))


def _event_market_count(events: List[Dict[str, Any]]) -> int:
    total = 0
    for event in events:
        try:
            total += int(event.get("market_count") or 0)
        except (TypeError, ValueError):
            continue
    return total


def _prefer_market_candidate(candidate_events: List[Dict[str, Any]], current_events: List[Dict[str, Any]], game_pk: Optional[Any] = None) -> bool:
    candidate_flat = _flattened_market_count(candidate_events, game_pk=game_pk)
    current_flat = _flattened_market_count(current_events, game_pk=game_pk)
    if candidate_flat != current_flat:
        return candidate_flat > current_flat

    candidate_markets = _event_market_count(candidate_events)
    current_markets = _event_market_count(current_events)
    if candidate_markets != current_markets:
        return candidate_markets > current_markets

    return len(candidate_events) > len(current_events)


def _extract_fixture_id_from_event(event: Dict[str, Any]) -> Optional[str]:
    raw = event.get("raw")
    if isinstance(raw, dict):
        if raw.get("fixture_id") is not None:
            return str(raw.get("fixture_id"))
        rows = raw.get("rows")
        if isinstance(rows, list) and rows:
            first = rows[0]
            if isinstance(first, dict) and first.get("fixture_id") is not None:
                return str(first.get("fixture_id"))
    return None


def _placeholder_team_name(value: Any) -> bool:
    name = None
    if isinstance(value, dict):
        name = value.get("name")
    elif value is not None:
        name = str(value)
    if name is None:
        return True
    return str(name).strip() in {"", "Away", "Home", "Unknown"}


def _placeholder_event_name(value: Any) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    return text in {"", "Away @ Home", "Home @ Away", "Unknown @ Unknown"}


def _fixture_metadata_from_item(item: Dict[str, Any], fallback_index: int = 0) -> Dict[str, Any]:
    away, home = base._team_names(item)
    start_time = base._iso(base._extract_first(item, base._START_KEYS))
    event_id = str(base._event_id(item, fallback_index))
    fixture_id = item.get("fixture_id")
    if fixture_id is not None:
        fixture_id = str(fixture_id)
    return {
        "event_id": event_id,
        "fixture_id": fixture_id,
        "name": f"{away} @ {home}" if away or home else None,
        "sport": base._extract_first(item, ("sport", "sport_title", "sport_name", "sportName")) or None,
        "league": base._extract_first(item, ("league", "league_name", "leagueName", "competition", "sport_key")) or None,
        "league_id": base._extract_first(item, ("league_id", "leagueId", "competition_id")) or None,
        "home_team": {"name": home} if home else None,
        "away_team": {"name": away} if away else None,
        "start_time": start_time,
        "status": base._extract_first(item, ("status", "event_status", "eventStatus")) or None,
        "is_live": item.get("is_live"),
    }


def _build_fixture_indexes(fixture_items: List[Dict[str, Any]], fixture_events: List[Dict[str, Any]]) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    by_event_id: Dict[str, Dict[str, Any]] = {}
    by_fixture_id: Dict[str, Dict[str, Any]] = {}

    for idx, item in enumerate(fixture_items):
        metadata = _fixture_metadata_from_item(item, idx)
        if metadata.get("event_id"):
            by_event_id[str(metadata["event_id"])] = metadata
        if metadata.get("fixture_id"):
            by_fixture_id[str(metadata["fixture_id"])] = metadata

    for event in fixture_events:
        metadata = {
            "event_id": str(event.get("event_id")) if event.get("event_id") is not None else None,
            "fixture_id": _extract_fixture_id_from_event(event),
            "name": event.get("name"),
            "sport": event.get("sport"),
            "league": event.get("league"),
            "league_id": event.get("league_id"),
            "home_team": event.get("home_team"),
            "away_team": event.get("away_team"),
            "start_time": event.get("start_time"),
            "status": event.get("status"),
            "is_live": event.get("is_live"),
        }
        if metadata.get("event_id"):
            existing = by_event_id.get(str(metadata["event_id"]), {})
            by_event_id[str(metadata["event_id"])] = {**existing, **{k: v for k, v in metadata.items() if v not in (None, "", {"name": None})}}
        if metadata.get("fixture_id"):
            existing = by_fixture_id.get(str(metadata["fixture_id"]), {})
            by_fixture_id[str(metadata["fixture_id"])] = {**existing, **{k: v for k, v in metadata.items() if v not in (None, "", {"name": None})}}

    return by_event_id, by_fixture_id


def _enrich_market_events_with_fixture_metadata(
    market_events: List[Dict[str, Any]],
    fixture_items: List[Dict[str, Any]],
    fixture_events: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    by_event_id, by_fixture_id = _build_fixture_indexes(fixture_items, fixture_events)
    enriched: List[Dict[str, Any]] = []

    for event in market_events:
        fixture_id = _extract_fixture_id_from_event(event)
        metadata = None
        if fixture_id:
            metadata = by_fixture_id.get(str(fixture_id))
        if metadata is None and event.get("event_id") is not None:
            metadata = by_event_id.get(str(event.get("event_id")))
        if metadata is None:
            enriched.append(event)
            continue

        event_copy = dict(event)
        if _placeholder_team_name(event_copy.get("away_team")) and metadata.get("away_team"):
            event_copy["away_team"] = metadata["away_team"]
        if _placeholder_team_name(event_copy.get("home_team")) and metadata.get("home_team"):
            event_copy["home_team"] = metadata["home_team"]
        if not event_copy.get("start_time") and metadata.get("start_time"):
            event_copy["start_time"] = metadata["start_time"]
        if _placeholder_event_name(event_copy.get("name")):
            away_name = None
            home_name = None
            if isinstance(event_copy.get("away_team"), dict):
                away_name = event_copy["away_team"].get("name")
            if isinstance(event_copy.get("home_team"), dict):
                home_name = event_copy["home_team"].get("name")
            if away_name or home_name:
                event_copy["name"] = f"{away_name or 'Away'} @ {home_name or 'Home'}"
            elif metadata.get("name"):
                event_copy["name"] = metadata["name"]
        for key in ("sport", "league", "league_id", "status", "is_live"):
            if event_copy.get(key) in (None, "") and metadata.get(key) not in (None, ""):
                event_copy[key] = metadata[key]
        enriched.append(event_copy)

    return enriched


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
    cache_key = f"kibl-sportsbook:{scope}:{game_pk or 'all'}:{props_only}:{date or 'any'}:{params}:{raw}:{live_only}:fixture-first-v4"
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

        ids = _fixture_ids(fixture_items, fixture_events)
        for label, body_base, bodies in _market_body_sets(params, ids):
            for body in bodies:
                try:
                    _, market_path, items, events = base._fetch_items(scope, body, game_pk, is_live, kind="markets")
                    flattened_market_count = _flattened_market_count(events, game_pk=game_pk)
                    notes.append(
                        f"markets_{label}:{market_path}:{len(items)}:{len(events)}:{flattened_market_count}:{','.join(sorted(set(body) - set(body_base))) or 'base'}"
                    )
                    if _prefer_market_candidate(events, market_events, game_pk=game_pk):
                        market_items = items
                        market_events = events
                        request_path = market_path
                        request_params = body
                        best_flattened_market_count = _flattened_market_count(market_events, game_pk=game_pk)
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
            for body in _market_request_bodies(retry_params, ids):
                try:
                    _, market_path, items, events = base._fetch_items(scope, body, game_pk, is_live, kind="markets")
                    flattened_market_count = _flattened_market_count(events, game_pk=game_pk)
                    notes.append(
                        f"markets_no_filter_core:{market_path}:{len(items)}:{len(events)}:{flattened_market_count}:{','.join(sorted(set(body) - set(retry_params))) or 'base'}"
                    )
                    if _prefer_market_candidate(events, market_events, game_pk=game_pk):
                        market_items = items
                        market_events = events
                        request_path = market_path
                        request_params = body
                        best_flattened_market_count = _flattened_market_count(market_events, game_pk=game_pk)
                    if flattened_market_count > 0:
                        break
                except Exception as exc:
                    notes.append(f"markets_no_filter_core_error:{exc}")

        if market_events:
            market_events = base._merge_fixture_metadata(market_events, fixture_events)
            events = _enrich_market_events_with_fixture_metadata(market_events, fixture_items, fixture_events)
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
