from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import kibl_bet105_provider as base

# Display names for known market keys. These map canonical Bet105/KIBL market identifiers
# to human-friendly labels.
_MARKET_DISPLAY_NAMES = {
    "h2h": "Moneyline",
    "spreads": "Spread",
    "totals": "Total",
}

# Mapping of KIBL numeric market_type_id values to canonical market keys. If a
# market_type_id is present on a KIBL market row, we use this map to derive the
# internal market_key and market_type. Unknown IDs are surfaced as their raw
# numeric identifier for debugging purposes.
_KIBL_MARKET_TYPE_MAP = {
    1: "h2h",
    2: "spreads",
    3: "totals",
}

_SELECTION_PLACEHOLDERS = {"", "away", "home", "over", "under", "draw", "unknown", "selection"}
_FIXTURE_ID_KEYS = ("fixture_id", "fixtureId", "fixtureID", "event_id", "eventId", "eventID", "id")
_SIDE_KEYS = ("participant_side_id", "side_id", "sideId", "participantSideId", "home_away", "homeAway", "side", "type", "qualifier")
_PARTICIPANT_NAME_KEYS = (
    "participant_name",
    "participantName",
    "team_name",
    "teamName",
    "team",
    "participant",
    "competitor",
    "competitor_name",
    "competitorName",
    "runner",
    "runner_name",
    "name",
    "display_name",
    "displayName",
    "fullName",
    "full_name",
)


def placeholder_team_name(value: Any) -> bool:
    name = None
    if isinstance(value, dict):
        name = value.get("name")
    elif value is not None:
        name = str(value)
    if name is None:
        return True
    return str(name).strip() in {"", "Away", "Home", "Unknown", "Team metadata missing"}


def placeholder_event_name(value: Any) -> bool:
    if value is None:
        return True
    return str(value).strip() in {"", "Away @ Home", "Home @ Away", "Unknown @ Unknown", "Team metadata missing"}


def placeholder_selection_text(value: Any) -> bool:
    if value is None:
        return True
    return str(value).strip().lower() in _SELECTION_PLACEHOLDERS or str(value).strip() == "Selection metadata missing"


def placeholder_market_name(value: Any, market_key: str) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    if not text:
        return True
    normalized = text.lower().replace(" ", "_")
    return normalized in {market_key, "1", "2", "3", "market", "unknown_market", "market_metadata_missing"}


def deep_extract_first(item: Dict[str, Any], keys: tuple[str, ...]) -> Any:
    direct = base._extract_first(item, keys)
    if direct not in (None, ""):
        return direct
    for child in base._walk_dicts(item):
        if child is item:
            continue
        value = base._extract_first(child, keys)
        if value not in (None, ""):
            return value
    return None


def participant_side_id(item: Dict[str, Any]) -> Optional[int]:
    value = deep_extract_first(item, _SIDE_KEYS)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if "away" in normalized:
            return 1
        if "home" in normalized:
            return 2
        if "over" in normalized:
            return 3
        if "under" in normalized:
            return 4
    return base._safe_int(value)


def _participant_name(item: Dict[str, Any]) -> Optional[str]:
    value = deep_extract_first(item, _PARTICIPANT_NAME_KEYS)
    if isinstance(value, dict) or isinstance(value, list):
        return base._nested_name(value)
    if value in (None, ""):
        return None
    text = str(value).strip()
    if text.lower() in {"away", "home", "over", "under", "draw", "selection", "market"}:
        return None
    return text


def _side_from_role(participant: Dict[str, Any]) -> Optional[int]:
    side_id = participant_side_id(participant)
    if side_id in {1, 2, 3, 4, 5}:
        return side_id
    role = str(deep_extract_first(participant, _SIDE_KEYS) or "").lower()
    if "away" in role:
        return 1
    if "home" in role:
        return 2
    if "over" in role:
        return 3
    if "under" in role:
        return 4
    return None


def fixture_team_names(item: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    away, home = base._team_names(item)
    if away and home:
        return away, home

    competitors = item.get("competitors") or item.get("participants") or item.get("teams")
    if not isinstance(competitors, list):
        competitors = [child for child in base._walk_dicts(item) if isinstance(child.get("participants"), list) for child in child.get("participants")]

    if isinstance(competitors, list):
        for participant in competitors:
            if not isinstance(participant, dict):
                continue
            name = _participant_name(participant) or base._nested_name(participant)
            side_id = _side_from_role(participant)
            if side_id == 1 and not away:
                away = name
            elif side_id == 2 and not home:
                home = name
            if away and home:
                break
        real_participants = [p for p in competitors if isinstance(p, dict)]
        if len(real_participants) >= 2:
            away = away or _participant_name(real_participants[0]) or base._nested_name(real_participants[0])
            home = home or _participant_name(real_participants[1]) or base._nested_name(real_participants[1])

    return away, home


def extract_fixture_id_from_event(event: Dict[str, Any]) -> Optional[str]:
    direct = base._extract_first(event, _FIXTURE_ID_KEYS)
    if direct not in (None, ""):
        return str(direct)
    raw = event.get("raw")
    if isinstance(raw, dict):
        value = deep_extract_first(raw, _FIXTURE_ID_KEYS)
        if value not in (None, ""):
            return str(value)
        rows = raw.get("rows")
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict):
                    value = deep_extract_first(row, _FIXTURE_ID_KEYS)
                    if value not in (None, ""):
                        return str(value)
    return None


def fixture_metadata_from_item(item: Dict[str, Any], fallback_index: int = 0) -> Dict[str, Any]:
    away, home = fixture_team_names(item)
    start_time = base._iso(deep_extract_first(item, base._START_KEYS))
    fixture_id = deep_extract_first(item, _FIXTURE_ID_KEYS)
    event_id = str(fixture_id) if fixture_id not in (None, "") else str(base._event_id(item, fallback_index))
    if fixture_id is not None:
        fixture_id = str(fixture_id)
    return {
        "event_id": event_id,
        "fixture_id": fixture_id,
        "name": f"{away} @ {home}" if away and home else None,
        "sport": deep_extract_first(item, ("sport", "sport_title", "sport_name", "sportName")),
        "league": deep_extract_first(item, ("league", "league_name", "leagueName", "competition", "sport_key")),
        "league_id": deep_extract_first(item, ("league_id", "leagueId", "competition_id")),
        "home_team": {"name": home} if home else None,
        "away_team": {"name": away} if away else None,
        "start_time": start_time,
        "status": deep_extract_first(item, ("status", "event_status", "eventStatus")),
        "is_live": deep_extract_first(item, ("is_live", "isLive", "live", "in_play", "inPlay")),
    }


def _metadata_from_market_event(event: Dict[str, Any]) -> Dict[str, Any]:
    away = event_team_name(event, "away")
    home = event_team_name(event, "home")
    rows: List[Dict[str, Any]] = []
    raw = event.get("raw")
    if isinstance(raw, dict):
        if isinstance(raw.get("rows"), list):
            rows.extend([row for row in raw["rows"] if isinstance(row, dict)])
        else:
            rows.append(raw)
    for market in event.get("markets") or []:
        market_raw = market.get("raw") if isinstance(market, dict) else None
        if isinstance(market_raw, dict):
            if isinstance(market_raw.get("rows"), list):
                rows.extend([row for row in market_raw["rows"] if isinstance(row, dict)])
            else:
                rows.append(market_raw)
        for selection in market.get("selections", []) if isinstance(market, dict) else []:
            selection_raw = selection.get("raw") if isinstance(selection, dict) else None
            if isinstance(selection_raw, dict):
                rows.append(selection_raw)

    for row in rows:
        name = _participant_name(row)
        side_id = _side_from_role(row)
        if side_id == 1 and name and not away:
            away = name
        elif side_id == 2 and name and not home:
            home = name
        if away and home:
            break

    fixture_id = extract_fixture_id_from_event(event)
    start_time = event.get("start_time")
    if not start_time:
        for row in rows:
            start_time = base._iso(deep_extract_first(row, base._START_KEYS))
            if start_time:
                break
    return {
        "event_id": str(event.get("event_id")) if event.get("event_id") not in (None, "") else fixture_id,
        "fixture_id": fixture_id,
        "name": f"{away} @ {home}" if away and home else event.get("name"),
        "home_team": {"name": home} if home else event.get("home_team"),
        "away_team": {"name": away} if away else event.get("away_team"),
        "start_time": start_time,
    }


def _merge_metadata(existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(existing)
    for key, value in incoming.items():
        if value in (None, "", {"name": None}, {"name": ""}):
            continue
        current = merged.get(key)
        if current in (None, "", {"name": None}, {"name": ""}) or key in {"away_team", "home_team", "name", "start_time"}:
            merged[key] = value
    return merged


def build_fixture_indexes(
    fixture_items: List[Dict[str, Any]],
    fixture_events: List[Dict[str, Any]],
) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    by_event_id: Dict[str, Dict[str, Any]] = {}
    by_fixture_id: Dict[str, Dict[str, Any]] = {}
    for idx, item in enumerate(fixture_items):
        metadata = fixture_metadata_from_item(item, idx)
        if metadata.get("event_id"):
            by_event_id[str(metadata["event_id"])] = _merge_metadata(by_event_id.get(str(metadata["event_id"]), {}), metadata)
        if metadata.get("fixture_id"):
            by_fixture_id[str(metadata["fixture_id"])] = _merge_metadata(by_fixture_id.get(str(metadata["fixture_id"]), {}), metadata)
    for event in fixture_events:
        metadata = {
            "event_id": str(event.get("event_id")) if event.get("event_id") is not None else None,
            "fixture_id": extract_fixture_id_from_event(event),
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
            by_event_id[str(metadata["event_id"])] = _merge_metadata(by_event_id.get(str(metadata["event_id"]), {}), metadata)
        if metadata.get("fixture_id"):
            by_fixture_id[str(metadata["fixture_id"])] = _merge_metadata(by_fixture_id.get(str(metadata["fixture_id"]), {}), metadata)
    return by_event_id, by_fixture_id


def event_team_name(event: Dict[str, Any], side: str) -> Optional[str]:
    value = event.get(f"{side}_team")
    if isinstance(value, dict):
        return base._nested_name(value)
    return str(value) if value not in (None, "") else None


def selection_display_name(selection: Dict[str, Any], event: Dict[str, Any]) -> Optional[str]:
    raw = selection.get("raw") if isinstance(selection.get("raw"), dict) else selection
    explicit = _participant_name(raw) or _participant_name(selection)
    if explicit:
        return explicit
    side_id = participant_side_id(raw) or participant_side_id(selection)
    if side_id == 1:
        return event_team_name(event, "away")
    if side_id == 2:
        return event_team_name(event, "home")
    if side_id == 3:
        return "Over"
    if side_id == 4:
        return "Under"
    if side_id == 5:
        return "Draw"
    return None


def market_display_name(market: Dict[str, Any]) -> str:
    """
    Resolve the market display name. Prefer canonical market keys mapped from the
    KIBL market_type_id when available. Unknown market_type_id values are surfaced
    explicitly to aid diagnostics rather than silently falling back to a generic label.
    """
    market_key = market.get("market_key") or market.get("market_type") or ""
    market_type_id = None
    raw = market.get("raw") if isinstance(market.get("raw"), dict) else None
    if market.get("market_type_id") is not None:
        market_type_id = market.get("market_type_id")
    elif raw and deep_extract_first(raw, ("market_type_id", "marketTypeId", "marketTypeID")) is not None:
        market_type_id = deep_extract_first(raw, ("market_type_id", "marketTypeId", "marketTypeID"))
    try:
        mt_int = int(market_type_id) if market_type_id is not None else None
    except (TypeError, ValueError):
        mt_int = None
    if mt_int is not None:
        canonical_key = _KIBL_MARKET_TYPE_MAP.get(mt_int)
        if canonical_key:
            market_key = canonical_key
        else:
            return f"Unknown Market Type {mt_int}"
    key_str = str(market_key)
    return _MARKET_DISPLAY_NAMES.get(key_str, str(market.get("market_name") or key_str or "Market metadata missing"))


def enrich_market(event: Dict[str, Any], market: Dict[str, Any]) -> Dict[str, Any]:
    market_copy = dict(market)
    market_type_id = None
    raw = market_copy.get("raw") if isinstance(market_copy.get("raw"), dict) else None
    if market_copy.get("market_type_id") is not None:
        market_type_id = market_copy.get("market_type_id")
    elif raw and deep_extract_first(raw, ("market_type_id", "marketTypeId", "marketTypeID")) is not None:
        market_type_id = deep_extract_first(raw, ("market_type_id", "marketTypeId", "marketTypeID"))
    try:
        mt_int = int(market_type_id) if market_type_id is not None else None
    except (TypeError, ValueError):
        mt_int = None
    if mt_int is not None:
        canonical_key = _KIBL_MARKET_TYPE_MAP.get(mt_int)
        if canonical_key:
            market_copy["market_key"] = canonical_key
            market_copy["market_type"] = canonical_key
        else:
            market_copy.setdefault("market_key", str(market_type_id))
            market_copy.setdefault("market_type", str(market_type_id))
    if not market_copy.get("market_type"):
        market_copy["market_type"] = market_copy.get("market_key")
    if placeholder_market_name(
        market_copy.get("market_name"), str(market_copy.get("market_key") or market_copy.get("market_type") or "")
    ):
        market_copy["market_name"] = market_display_name(market_copy)
    selections: List[Dict[str, Any]] = []
    for selection in market.get("selections", []) or []:
        selection_copy = dict(selection)
        display_name = selection_display_name(selection_copy, event)
        if display_name:
            if placeholder_selection_text(selection_copy.get("name")):
                selection_copy["name"] = display_name
            if placeholder_selection_text(selection_copy.get("description")):
                selection_copy["description"] = selection_copy.get("name") or display_name
            if placeholder_team_name(selection_copy.get("team")):
                selection_copy["team"] = (
                    display_name if display_name not in {"Over", "Under", "Draw"} else selection_copy.get("team")
                )
        if not selection_copy.get("description") and selection_copy.get("name"):
            selection_copy["description"] = selection_copy["name"]
        selections.append(selection_copy)
    market_copy["selections"] = selections
    return market_copy


def enrich_market_events_with_fixture_metadata(
    market_events: List[Dict[str, Any]],
    fixture_items: List[Dict[str, Any]],
    fixture_events: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    by_event_id, by_fixture_id = build_fixture_indexes(fixture_items, fixture_events)
    enriched: List[Dict[str, Any]] = []
    for event in market_events:
        fixture_id = extract_fixture_id_from_event(event)
        metadata = by_fixture_id.get(str(fixture_id)) if fixture_id else None
        if metadata is None and event.get("event_id") is not None:
            metadata = by_event_id.get(str(event.get("event_id")))
        market_metadata = _metadata_from_market_event(event)
        metadata = _merge_metadata(metadata or {}, market_metadata)
        event_copy = dict(event)
        if metadata:
            if placeholder_team_name(event_copy.get("away_team")) and metadata.get("away_team"):
                event_copy["away_team"] = metadata["away_team"]
            if placeholder_team_name(event_copy.get("home_team")) and metadata.get("home_team"):
                event_copy["home_team"] = metadata["home_team"]
            if not event_copy.get("start_time") and metadata.get("start_time"):
                event_copy["start_time"] = metadata["start_time"]
            if placeholder_event_name(event_copy.get("name")):
                away_name = event_team_name(event_copy, "away")
                home_name = event_team_name(event_copy, "home")
                if away_name and home_name:
                    event_copy["name"] = f"{away_name} @ {home_name}"
                elif metadata.get("name") and not placeholder_event_name(metadata.get("name")):
                    event_copy["name"] = metadata["name"]
            for key in ("sport", "league", "league_id", "status", "is_live"):
                if event_copy.get(key) in (None, "") and metadata.get(key) not in (None, ""):
                    event_copy[key] = metadata[key]
        event_copy["markets"] = [enrich_market(event_copy, m) for m in event.get("markets", []) or []]
        event_copy["market_count"] = len(event_copy["markets"])
        enriched.append(event_copy)
    return enriched
