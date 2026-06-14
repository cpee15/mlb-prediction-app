from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import kibl_bet105_provider as base
from . import kibl_bet105_sportsbook_enrichment as enrichment
from . import sportsbook_bet105_fixture_discovery as discovery
from . import sportsbook_bet105_service as service
from . import sportsbook_bet105_service_v11 as v11

_ID_KEYS = ("fixture_id", "fixtureId", "fixtureID", "event_id", "eventId", "id")
_DROP = {"path", "from_cache", "combined_market_candidates"}
_DATE_KEYS = {"start_date", "end_date", "from", "to"}


def _add(out: List[str], value: Any) -> None:
    if value not in (None, "") and str(value) not in out:
        out.append(str(value))


def _rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    rows.extend([x for x in payload.get("raw_items_sample") or [] if isinstance(x, dict)])
    for event in payload.get("events") or []:
        if not isinstance(event, dict):
            continue
        raw = event.get("raw")
        if isinstance(raw, dict):
            rows.append(raw)
            if isinstance(raw.get("rows"), list):
                rows.extend([x for x in raw["rows"] if isinstance(x, dict)])
        for market in event.get("markets") or []:
            rows.extend(service._raw_market_rows(market))
            for selection in market.get("selections") or []:
                raw_sel = selection.get("raw") if isinstance(selection, dict) else None
                if isinstance(raw_sel, dict):
                    rows.append(raw_sel)
    return rows


def _fixture_ids(payload: Dict[str, Any]) -> List[str]:
    ids: List[str] = []
    for row in _rows(payload):
        _add(ids, enrichment.deep_extract_first(row, _ID_KEYS))
    return ids


def _detail_ids(payload: Dict[str, Any]) -> Dict[str, List[str]]:
    found = {"fixture_participant_id": [], "participant_id": [], "market_id": [], "contestant_id": [], "line_id": []}
    for row in _rows(payload):
        info = row.get("info") if isinstance(row.get("info"), dict) else {}
        for key in found:
            _add(found[key], row.get(key))
            _add(found[key], info.get(key))
    return found


def _clean(params: Dict[str, Any], compact: bool = False) -> Dict[str, Any]:
    banned = _DROP | (_DATE_KEYS if compact else set())
    return {k: v for k, v in params.items() if k not in banned and v not in (None, "")}


def _fixture_events(items: List[Dict[str, Any]], live: bool) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, item in enumerate(items):
        meta = enrichment.fixture_metadata_from_item(item, idx)
        if not meta.get("event_id"):
            continue
        out.append({
            "event_id": meta.get("event_id"), "fixture_id": meta.get("fixture_id"), "name": meta.get("name"),
            "sport": meta.get("sport") or "Baseball", "league": meta.get("league") or "MLB", "league_id": meta.get("league_id") or "mlb",
            "home_team": meta.get("home_team"), "away_team": meta.get("away_team"),
            "start_time": meta.get("start_time"), "commence_time": meta.get("start_time"),
            "status": meta.get("status") or ("live" if live else "scheduled"), "is_live": live if meta.get("is_live") is None else bool(meta.get("is_live")),
            "source_url": None, "scraped_at": base._now(), "markets": [], "market_count": 0, "raw": item,
        })
    return out


def _retry_bodies(payload: Dict[str, Any], params: Dict[str, Any]) -> List[tuple[str, Dict[str, Any]]]:
    bodies: List[tuple[str, Dict[str, Any]]] = []
    ids = _fixture_ids(payload)[:50]
    for root in (_clean(params), _clean(params, compact=True)):
        for key in ("ids", "fixture_ids", "event_ids"):
            if ids:
                bodies.append((key, {**root, key: ids}))
        for value in ids[:20]:
            bodies.append(("ids_single_list", {**root, "ids": [value]}))
            bodies.append(("fixture_ids_single_list", {**root, "fixture_ids": [value]}))
    details = _detail_ids(payload)
    for key, values in details.items():
        for root in (_clean(params), _clean(params, compact=True)):
            if values:
                bodies.append((f"{key}_list", {**root, key: values[:50]}))
    seen: set[str] = set()
    out: List[tuple[str, Dict[str, Any]]] = []
    for label, body in bodies:
        fp = repr(sorted((k, str(v)) for k, v in body.items()))
        if fp not in seen:
            seen.add(fp); out.append((label, body))
    return out


def _retry_fixture_items(payload: Dict[str, Any], params: Dict[str, Any], scope: str) -> List[Dict[str, Any]]:
    notes = payload.setdefault("normalization_notes", [])
    items: List[Dict[str, Any]] = []
    labels: List[str] = []
    for label, body in _retry_bodies(payload, params)[:60]:
        labels.append(label)
        try:
            raw, path = base._fetch_kibl_payload(scope, body, kind="fixtures")
            found = service._fixture_items_from_payload(raw)
            notes.append(f"fixtures_id_list_retry:{label}:{path}:{len(found)}")
            items.extend(found)
        except Exception as exc:
            notes.append(f"fixtures_id_list_retry_error:{label}:{exc}")
    payload.setdefault("fixtures", {})["fixture_retry_labels"] = labels[:40]
    payload["fixtures"]["market_detail_ids"] = _detail_ids(payload)
    if not items:
        notes.append("fixtures_id_list_retry_empty:starting_path_discovery")
        items = discovery.discover_fixture_items(payload, params)
    return service._dedupe_items(items)


def _side_label(selection: Dict[str, Any]) -> Optional[str]:
    raw = selection.get("raw") if isinstance(selection.get("raw"), dict) else selection
    info = raw.get("info") if isinstance(raw, dict) else None
    side = info.get("side") if isinstance(info, dict) else raw.get("side") if isinstance(raw, dict) else None
    text = str(side or "").strip().lower()
    return "Yes" if text in {"yes", "y"} else "No" if text in {"no", "n"} else None


def _label_binary(events: List[Dict[str, Any]]) -> None:
    for event in events:
        for market in event.get("markets") or []:
            for selection in market.get("selections") or []:
                label = _side_label(selection)
                if label and enrichment.placeholder_selection_text(selection.get("name")):
                    selection["name"] = label
                if label and enrichment.placeholder_selection_text(selection.get("description")):
                    selection["description"] = label
                if label:
                    selection["side"] = label.lower()


def _refresh(payload: Dict[str, Any], game_pk: Optional[Any], raw: bool) -> Dict[str, Any]:
    _label_binary(payload.get("events") or [])
    payload["events"] = service._finalize_events(payload.get("events") or [])
    payload["markets"] = base._flatten_markets(payload["events"], game_pk=game_pk)
    diagnostics = service._incomplete_count(payload["events"])
    payload.update({"diagnostics": diagnostics, "event_count": len(payload["events"]), "market_count": len(payload["markets"])})
    if payload["markets"] and not any(diagnostics.values()):
        payload["status"] = "ok"
    if not raw:
        payload["events"] = base._without_raw_events(payload["events"])
    return payload


def fetch_board(date: Optional[str] = None, raw: bool = False, live_only: Optional[bool] = None, game_pk: Optional[Any] = None, props_only: bool = False, market_types: Optional[List[str]] = None) -> Dict[str, Any]:
    return v11.fetch_board(date=date, raw=raw, live_only=live_only, game_pk=game_pk, props_only=props_only, market_types=market_types)


def fetch_event_board(event_id: str, props_only: bool = False, raw: bool = False, market_types: Optional[List[str]] = None) -> Dict[str, Any]:
    return v11.fetch_event_board(event_id=event_id, props_only=props_only, raw=raw, market_types=market_types)
