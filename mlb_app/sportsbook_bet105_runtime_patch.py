from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import kibl_bet105_provider as base
from . import kibl_bet105_sportsbook_enrichment as enrichment
from . import sportsbook_bet105_service as service

_ID_KEYS = ("fixture_id", "fixtureId", "fixtureID", "event_id", "eventId", "id")


def _add(out: List[str], value: Any) -> None:
    if value in (None, ""):
        return
    text = str(value)
    if text not in out:
        out.append(text)


def _fixture_ids(payload: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for item in payload.get("raw_items_sample") or []:
        if isinstance(item, dict):
            _add(out, enrichment.deep_extract_first(item, _ID_KEYS))
    for event in payload.get("events") or []:
        if not isinstance(event, dict):
            continue
        _add(out, enrichment.extract_fixture_id_from_event(event) or event.get("event_id"))
        raw = event.get("raw")
        rows = raw.get("rows") if isinstance(raw, dict) else []
        for row in rows or []:
            if isinstance(row, dict):
                _add(out, enrichment.deep_extract_first(row, _ID_KEYS))
        for market in event.get("markets") or []:
            for row in service._raw_market_rows(market):
                _add(out, enrichment.deep_extract_first(row, _ID_KEYS))
            for selection in market.get("selections") or []:
                raw_sel = selection.get("raw") if isinstance(selection, dict) else None
                if isinstance(raw_sel, dict):
                    _add(out, enrichment.deep_extract_first(raw_sel, _ID_KEYS))
    return out


def _fixture_events(items: List[Dict[str, Any]], live: bool) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for idx, item in enumerate(items):
        meta = enrichment.fixture_metadata_from_item(item, idx)
        if not meta.get("event_id"):
            continue
        events.append({
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
            "status": meta.get("status") or ("live" if live else "scheduled"),
            "is_live": live if meta.get("is_live") is None else bool(meta.get("is_live")),
            "source_url": None,
            "scraped_at": base._now(),
            "markets": [],
            "market_count": 0,
            "raw": item,
        })
    return events


def _retry_fixture_items(payload: Dict[str, Any], params: Dict[str, Any], scope: str) -> List[Dict[str, Any]]:
    notes = payload.setdefault("normalization_notes", [])
    items: List[Dict[str, Any]] = []
    tried: set[str] = set()
    for fixture_id in _fixture_ids(payload)[:20]:
        for key in ("fixture_id", "event_id", "id"):
            body = {**params, key: fixture_id}
            fingerprint = repr(sorted(body.items()))
            if fingerprint in tried:
                continue
            tried.add(fingerprint)
            try:
                raw, path = base._fetch_kibl_payload(scope, body, event_id=fixture_id, kind="fixtures")
                found = service._fixture_items_from_payload(raw)
                notes.append(f"fixtures_market_id_retry:{key}:{path}:{len(found)}")
                items.extend(found)
            except Exception as exc:
                notes.append(f"fixtures_market_id_retry_error:{key}:{exc}")
    return service._dedupe_items(items)


def _side_label(selection: Dict[str, Any]) -> Optional[str]:
    raw = selection.get("raw") if isinstance(selection.get("raw"), dict) else selection
    info = raw.get("info") if isinstance(raw, dict) else None
    side = info.get("side") if isinstance(info, dict) else raw.get("side") if isinstance(raw, dict) else None
    text = str(side or "").strip().lower()
    if text in {"yes", "y"}:
        return "Yes"
    if text in {"no", "n"}:
        return "No"
    return None


def _label_yes_no(events: List[Dict[str, Any]]) -> None:
    for event in events:
        for market in event.get("markets") or []:
            for selection in market.get("selections") or []:
                label = _side_label(selection)
                if not label:
                    continue
                if enrichment.placeholder_selection_text(selection.get("name")):
                    selection["name"] = label
                if enrichment.placeholder_selection_text(selection.get("description")):
                    selection["description"] = label
                selection["side"] = label.lower()


def _refresh(payload: Dict[str, Any], game_pk: Optional[Any], raw: bool) -> Dict[str, Any]:
    _label_yes_no(payload.get("events") or [])
    payload["events"] = service._finalize_events(payload.get("events") or [])
    payload["markets"] = base._flatten_markets(payload["events"], game_pk=game_pk)
    diagnostics = service._incomplete_count(payload["events"])
    payload["diagnostics"] = diagnostics
    payload["event_count"] = len(payload["events"])
    payload["market_count"] = len(payload["markets"])
    if payload["markets"] and not any(diagnostics.values()):
        payload["status"] = "ok"
    if not raw:
        payload["events"] = base._without_raw_events(payload["events"])
    return payload


def fetch_board(date: Optional[str] = None, raw: bool = False, live_only: Optional[bool] = None, game_pk: Optional[Any] = None, props_only: bool = False, market_types: Optional[List[str]] = None) -> Dict[str, Any]:
    payload = service.fetch_board(date=date, raw=True, live_only=live_only, game_pk=game_pk, props_only=props_only, market_types=market_types)
    fixtures = payload.get("fixtures") if isinstance(payload.get("fixtures"), dict) else {}
    if fixtures.get("count"):
        return _refresh(payload, game_pk, raw)
    params = payload.get("request_params") if isinstance(payload.get("request_params"), dict) else {}
    scope = str(payload.get("scope") or ("live" if live_only else "events"))
    items = _retry_fixture_items(payload, params, scope)
    if items:
        live = bool(live_only or scope == "live")
        fixture_events = _fixture_events(items, live)
        payload["events"] = enrichment.enrich_market_events_with_fixture_metadata(payload.get("events") or [], items, fixture_events)
        payload.setdefault("fixtures", {})["count"] = len(items)
        payload["fixtures"]["fixture_ids"] = [str(enrichment.deep_extract_first(item, _ID_KEYS)) for item in items[:20]]
        payload["normalization_notes"].append(f"fixtures_market_id_retry_selected:{len(items)}:{len(fixture_events)}")
    payload.setdefault("fixtures", {})["market_fixture_ids"] = _fixture_ids(payload)[:20]
    return _refresh(payload, game_pk, raw)


def fetch_event_board(event_id: str, props_only: bool = False, raw: bool = False, market_types: Optional[List[str]] = None) -> Dict[str, Any]:
    payload = fetch_board(game_pk=event_id, props_only=props_only, raw=raw, market_types=market_types)
    events = payload.get("events") or []
    payload["event"] = events[0] if events else None
    return payload
