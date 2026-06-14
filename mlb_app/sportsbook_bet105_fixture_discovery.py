from __future__ import annotations

import os
from typing import Any, Dict, List, Tuple

from . import kibl_bet105_provider as base
from . import kibl_bet105_sportsbook_enrichment as enrichment
from . import sportsbook_bet105_service as service

ID_KEYS = ("fixture_id", "fixtureId", "fixtureID", "event_id", "eventId", "id")
DETAIL_KEYS = ("fixture_participant_id", "participant_id", "market_id", "contestant_id", "line_id")
DROP_KEYS = {"path", "from_cache", "combined_market_candidates"}
DATE_KEYS = {"start_date", "end_date", "from", "to"}
PATHS = (
    "info/fixtures",
    "fixtures",
    "events",
    "info/events",
    "info/games",
    "info/matches",
    "info/participants",
    "info/fixture_participants",
    "info/fixture-participants",
    "info/contestants",
    "info/competitors",
    "info/teams",
)


def _add(out: List[str], value: Any) -> None:
    if value not in (None, "") and str(value) not in out:
        out.append(str(value))


def _rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    rows.extend([row for row in payload.get("raw_items_sample") or [] if isinstance(row, dict)])
    for event in payload.get("events") or []:
        if not isinstance(event, dict):
            continue
        raw = event.get("raw")
        if isinstance(raw, dict):
            rows.append(raw)
            if isinstance(raw.get("rows"), list):
                rows.extend([row for row in raw["rows"] if isinstance(row, dict)])
        for market in event.get("markets") or []:
            rows.extend(service._raw_market_rows(market))
            for selection in market.get("selections") or []:
                raw_selection = selection.get("raw") if isinstance(selection, dict) else None
                if isinstance(raw_selection, dict):
                    rows.append(raw_selection)
    return rows


def fixture_ids(payload: Dict[str, Any]) -> List[str]:
    ids: List[str] = []
    for row in _rows(payload):
        _add(ids, enrichment.deep_extract_first(row, ID_KEYS))
    return ids


def detail_ids(payload: Dict[str, Any]) -> Dict[str, List[str]]:
    found = {key: [] for key in DETAIL_KEYS}
    for row in _rows(payload):
        info = row.get("info") if isinstance(row.get("info"), dict) else {}
        for key in DETAIL_KEYS:
            _add(found[key], row.get(key))
            _add(found[key], info.get(key))
    return found


def _clean(params: Dict[str, Any], compact: bool = False) -> Dict[str, Any]:
    banned = DROP_KEYS | (DATE_KEYS if compact else set())
    return {key: value for key, value in params.items() if key not in banned and value not in (None, "")}


def _bodies(payload: Dict[str, Any], params: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    fixture_values = fixture_ids(payload)[:50]
    details = detail_ids(payload)
    bodies: List[Tuple[str, Dict[str, Any]]] = []
    for root in (_clean(params), _clean(params, compact=True)):
        for paging in ({}, {"offset": 0, "limit": 1000}, {"offset": 0, "limit": 5000}):
            base_body = {**root, **paging}
            if fixture_values:
                for key in ("ids", "fixture_ids", "event_ids"):
                    bodies.append((key, {**base_body, key: fixture_values}))
                for value in fixture_values[:20]:
                    bodies.append(("ids_single", {**base_body, "ids": [value]}))
                    bodies.append(("fixture_ids_single", {**base_body, "fixture_ids": [value]}))
            for key, values in details.items():
                if values:
                    bodies.append((f"{key}_list", {**base_body, key: values[:50]}))
    seen: set[str] = set()
    deduped: List[Tuple[str, Dict[str, Any]]] = []
    for label, body in bodies:
        fingerprint = repr(sorted((key, str(value)) for key, value in body.items()))
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        deduped.append((label, body))
    return deduped


def _post_path(path: str, body: Dict[str, Any]) -> Any:
    base_url = os.getenv("KIBL_BASE_URL", base._DEFAULT_BASE_URL).rstrip("/")
    timeout = int(os.getenv("KIBL_TIMEOUT_SECONDS", str(base._DEFAULT_TIMEOUT_SECONDS)))
    return base._post_kibl_json(f"{base_url}/{path.strip('/')}/", body, timeout)


def discover_fixture_items(payload: Dict[str, Any], params: Dict[str, Any]) -> List[Dict[str, Any]]:
    fixtures = payload.setdefault("fixtures", {})
    notes = payload.setdefault("normalization_notes", [])
    probes: List[Dict[str, Any]] = []
    items: List[Dict[str, Any]] = []
    for path in PATHS:
        for label, body in _bodies(payload, params)[:80]:
            try:
                raw = _post_path(path, body)
                count = len(base._find_list_payload(raw))
                found = service._fixture_items_from_payload(raw)
                probes.append({"path": path, "label": label, "count": count, "fixture_count": len(found)})
                notes.append(f"fixtures_path_probe:{path}:{label}:{count}:{len(found)}")
                if found:
                    items.extend(found)
                    fixtures["fixture_discovery_path"] = path
                    fixtures["fixture_discovery_label"] = label
                    return service._dedupe_items(items)
            except Exception as exc:
                probes.append({"path": path, "label": label, "error": str(exc)[:180]})
                notes.append(f"fixtures_path_probe_error:{path}:{label}:{str(exc)[:120]}")
    fixtures["path_probe_sample"] = probes[:40]
    fixtures["market_detail_ids"] = detail_ids(payload)
    fixtures["market_fixture_ids"] = fixture_ids(payload)[:20]
    return service._dedupe_items(items)
