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


def _raw_dict(item: Dict[str, Any]) -> Dict[str, Any]:
    raw = item.get("raw")
    return raw if isinstance(raw, dict) else item


def _raw_market_rows(market: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = _raw_dict(market)
    rows = raw.get("rows")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return [raw] if raw else []


def _extract_market_type_id(market: Dict[str, Any]) -> Any:
    if market.get("market_type_id") is not None:
        return market.get("market_type_id")
    for row in _raw_market_rows(market):
        value = base._extract_first(row, ("market_type_id", "marketTypeId", "marketTypeID"))
        if value is not None:
            return value
    return None


def _canonical_market_key(market: Dict[str, Any]) -> str:
    market_type_id = _extract_market_type_id(market)
    try:
        market_type_int = int(market_type_id) if market_type_id is not None else None
    except (TypeError, ValueError):
        market_type_int = None
    if market_type_int in enrichment._KIBL_MARKET_TYPE_MAP:
        return enrichment._KIBL_MARKET_TYPE_MAP[market_type_int]
    return str(market.get("market_key") or market.get("market_type") or base._market_key(market_type_id) or "unknown_market")


def _market_group_line(market: Dict[str, Any]) -> Any:
    key = _canonical_market_key(market)
    line = base._safe_float(market.get("line"))
    if line is None:
        for row in _raw_market_rows(market):
            line = base._safe_float(base._extract_first(row, base._LINE_KEYS))
            if line is not None:
                break
    if key == "h2h":
        return "none"
    if key == "spreads" and line is not None:
        return abs(line)
    return line if line is not None else "none"


def _selection_fingerprint(selection: Dict[str, Any]) -> tuple[Any, ...]:
    raw = _raw_dict(selection)
    return (
        base._extract_first(selection, ("selection_id", "selectionId", "id", "outcome_id", "outcomeId", "fixture_participant_id"))
        or base._extract_first(raw, ("selection_id", "selectionId", "id", "outcome_id", "outcomeId", "fixture_participant_id")),
        base._extract_first(selection, ("participant_id", "participantId"))
        or base._extract_first(raw, ("participant_id", "participantId")),
        _selection_side_id(selection),
        base._extract_first(raw, ("market_id", "marketId")),
        base._extract_first(selection, ("price", "price_american")) or base._price_from_selection(raw),
        selection.get("line") if selection.get("line") is not None else base._extract_first(raw, base._LINE_KEYS),
    )


def _merge_market_group(group: List[Dict[str, Any]]) -> Dict[str, Any]:
    merged = dict(group[0])
    selections: List[Dict[str, Any]] = []
    seen_selections: set[tuple[Any, ...]] = set()
    raw_rows: List[Dict[str, Any]] = []
    seen_rows: set[str] = set()
    for market in group:
        for selection in market.get("selections", []) or []:
            selection_copy = dict(selection)
            fingerprint = _selection_fingerprint(selection_copy)
            if fingerprint in seen_selections:
                continue
            seen_selections.add(fingerprint)
            selections.append(selection_copy)
        for row in _raw_market_rows(market):
            row_key = repr(sorted(row.items()))
            if row_key in seen_rows:
                continue
            seen_rows.add(row_key)
            raw_rows.append(row)
    merged["selections"] = selections
    if raw_rows:
        merged["raw"] = {"rows": raw_rows}
    return merged


def _selection_side_id(selection: Dict[str, Any]) -> Optional[int]:
    raw = _raw_dict(selection)
    return base._safe_int(base._extract_first(raw, ("participant_side_id", "side_id", "sideId", "participantSideId")) or base._extract_first(selection, ("participant_side_id", "side_id", "sideId", "participantSideId")))


def _event_merge_key(event: Dict[str, Any], fallback_index: int) -> str:
    fixture_id = enrichment.extract_fixture_id_from_event(event)
    if fixture_id:
        return f"fixture:{fixture_id}"
    event_id = event.get("event_id")
    if event_id not in (None, ""):
        return f"event:{event_id}"
    return f"fallback:{fallback_index}"


def _merge_market_event_candidates(candidate_sets: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Union market candidates returned by different KIBL request bodies.

    KIBL can return a partial board for a specific request body, especially when
    fixture-specific filters are used. The board endpoint should keep scanning
    every viable request and combine compatible event buckets instead of keeping
    only the single largest response.
    """

    merged_by_key: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for candidate_set in candidate_sets:
        for event in candidate_set:
            key = _event_merge_key(event, len(order))
            if key not in merged_by_key:
                event_copy = dict(event)
                event_copy["markets"] = [dict(market) for market in event.get("markets", []) or []]
                merged_by_key[key] = event_copy
                order.append(key)
                continue

            target = merged_by_key[key]
            for meta_key in ("name", "sport", "league", "league_id", "home_team", "away_team", "start_time", "commence_time", "status", "is_live", "source_url"):
                current = target.get(meta_key)
                incoming = event.get(meta_key)
                if current in (None, "", {"name": None}, {"name": ""}) and incoming not in (None, "", {"name": None}, {"name": ""}):
                    target[meta_key] = incoming
            target.setdefault("markets", [])
            target["markets"].extend([dict(market) for market in event.get("markets", []) or []])
            target["market_count"] = len(target["markets"])
            if target.get("raw") is None and event.get("raw") is not None:
                target["raw"] = event.get("raw")
    return [merged_by_key[key] for key in order]


def _dedupe_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        key = repr(sorted(item.items()))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _finalize_selection(selection: Dict[str, Any]) -> Dict[str, Any]:
    selection_copy = dict(selection)
    raw = _raw_dict(selection_copy)
    side_id = _selection_side_id(selection_copy)
    participant_id = base._extract_first(raw, ("participant_id", "participantId", "participantID")) or selection_copy.get("participant_id")
    price_american = selection_copy.get("price")
    if price_american is None:
        price_american = base._price_from_selection(raw)
    price_decimal = None
    odds = selection_copy.get("odds") if isinstance(selection_copy.get("odds"), dict) else {}
    if odds:
        price_decimal = odds.get("decimal")
    if price_decimal is None:
        price_decimal = base._safe_float(base._extract_first(raw, base._DECIMAL_KEYS)) or base._decimal_from_american(price_american)
    price_fraction = None
    if odds:
        price_fraction = odds.get("fractional")
    if price_fraction is None:
        price_fraction = base._extract_first(raw, ("price_fraction", "fractional", "fractional_odds", "fractionalOdds"))
    implied_probability = None
    if odds:
        implied_probability = odds.get("implied_probability")
    if implied_probability is None:
        implied_probability = base._implied_from_american(price_american)
    is_current = base._extract_first(raw, ("is_current", "isCurrent", "active", "is_active", "isActive"))
    if is_current is None:
        is_current = selection_copy.get("is_open", True)
    selection_copy.update(
        {
            "side_id": side_id,
            "participant_id": participant_id,
            "price": price_american,
            "price_american": price_american,
            "price_decimal": price_decimal,
            "price_fraction": price_fraction,
            "implied_probability": implied_probability,
            "is_current": bool(is_current),
            "active": bool(is_current),
        }
    )
    selection_copy["odds"] = {
        "american": price_american,
        "decimal": price_decimal,
        "fractional": price_fraction,
        "implied_probability": implied_probability,
    }
    return selection_copy


def _finalize_market(market: Dict[str, Any]) -> Dict[str, Any]:
    market_copy = dict(market)
    market_type_id = _extract_market_type_id(market_copy)
    market_key = _canonical_market_key(market_copy)
    market_copy["market_type_id"] = market_type_id
    market_copy["market_key"] = market_key
    market_copy["market_type"] = market_key
    if enrichment.placeholder_market_name(market_copy.get("market_name"), market_key):
        market_copy["market_name"] = enrichment.market_display_name(market_copy)
    if not market_copy.get("period"):
        for row in _raw_market_rows(market_copy):
            period = base._extract_first(row, ("period", "period_name", "periodName", "segment_id", "segmentId"))
            if period is not None:
                market_copy["period"] = period
                break
    market_copy["selections"] = [_finalize_selection(selection) for selection in market_copy.get("selections", []) or []]
    return market_copy


def _finalize_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    finalized: List[Dict[str, Any]] = []
    for event in events:
        event_copy = dict(event)
        if event_copy.get("start_time") and not event_copy.get("commence_time"):
            event_copy["commence_time"] = event_copy["start_time"]
        market_groups: Dict[tuple[Any, Any, Any], List[Dict[str, Any]]] = {}
        for market in event_copy.get("markets", []) or []:
            key = (_canonical_market_key(market), market.get("period"), _market_group_line(market))
            market_groups.setdefault(key, []).append(market)
        merged_markets = [_finalize_market(_merge_market_group(group)) for group in market_groups.values()]
        event_copy["markets"] = merged_markets
        event_copy["market_count"] = len(merged_markets)
        finalized.append(event_copy)
    return finalized


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
    cache_key = f"kibl-sportsbook:{scope}:{game_pk or 'all'}:{props_only}:{date or 'any'}:{params}:{raw}:{live_only}:fixture-first-v9"
    cached = base._cache_get(cache_key)
    if cached:
        return cached

    notes: List[str] = []
    fixture_items: List[Dict[str, Any]] = []
    fixture_events: List[Dict[str, Any]] = []
    market_items: List[Dict[str, Any]] = []
    market_events: List[Dict[str, Any]] = []
    candidate_event_sets: List[List[Dict[str, Any]]] = []
    candidate_market_items: List[Dict[str, Any]] = []
    request_path: Optional[str] = None
    request_params: Dict[str, Any] = dict(params)

    try:
        try:
            fixture_payload, fixture_path = base._fetch_kibl_payload(scope, params, event_id=str(game_pk) if game_pk is not None else None, kind="fixtures")
            fixture_items = _fixture_items_from_payload(fixture_payload)
            fixture_events = [
                {
                    "event_id": meta.get("event_id"),
                    "fixture_id": meta.get("fixture_id"),
                    "name": meta.get("name"),
                    "sport": meta.get("sport") or "Baseball",
                    "league": meta.get("league") or "MLB",
                    "league_id": meta.get("league_id") or "mlb",
                    "home_team": meta.get("home_team"),
                    "away_team": meta.get("away_team"),
                    "start_time": meta.get("start_time"),
                    "commence_time": meta.get("start_time"),
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
                    if events:
                        candidate_event_sets.append(events)
                    if items:
                        candidate_market_items.extend(items)
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
                    if events:
                        candidate_event_sets.append(events)
                    if items:
                        candidate_market_items.extend(items)
                    if legacy._prefer_market_candidate(events, market_events, game_pk=game_pk):
                        market_items = items
                        market_events = events
                        request_path = market_path
                        request_params = body
                        best_flattened_market_count = flattened_market_count
                    # Do not break here either; the unfiltered response may also be partial.
                except Exception as exc:
                    notes.append(f"markets_no_filter_core_error:{exc}")

        union_market_events = _merge_market_event_candidates(candidate_event_sets)
        union_flattened_market_count = legacy._flattened_market_count(union_market_events, game_pk=game_pk)
        if union_market_events and union_flattened_market_count > best_flattened_market_count:
            notes.append(f"markets_union_selected:{len(candidate_event_sets)}:{len(union_market_events)}:{union_flattened_market_count}")
            market_events = union_market_events
            market_items = _dedupe_items(candidate_market_items)
            best_flattened_market_count = union_flattened_market_count
            request_path = request_path or "info/markets"
            request_params = {**params, "combined_market_candidates": True}
        elif candidate_event_sets:
            notes.append(f"markets_union_not_selected:{len(candidate_event_sets)}:{len(union_market_events)}:{union_flattened_market_count}")

        events = enrichment.enrich_market_events_with_fixture_metadata(market_events, fixture_items, fixture_events) if market_events else fixture_events
        events = _finalize_events(events)
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
        "raw_count": len(raw_debug_items),
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
            "candidate_event_set_count": len(candidate_event_sets),
        }
    base._cache_set(cache_key, normalized)
    return normalized


def fetch_event_board(event_id: str, props_only: bool = False, raw: bool = False, market_types: Optional[List[str]] = None) -> Dict[str, Any]:
    payload = fetch_board(game_pk=event_id, props_only=props_only, raw=raw, market_types=market_types)
    events = payload.get("events") or []
    payload["event"] = events[0] if events else None
    return payload
