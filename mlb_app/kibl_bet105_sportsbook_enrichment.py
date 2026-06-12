from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import kibl_bet105_provider as base

_MARKET_DISPLAY_NAMES = {
    "h2h": "Moneyline",
    "spreads": "Spread",
    "totals": "Total",
}
_SELECTION_PLACEHOLDERS = {"", "away", "home", "over", "under", "draw", "unknown", "selection"}


def placeholder_team_name(value: Any) -> bool:
    name = None
    if isinstance(value, dict):
        name = value.get("name")
    elif value is not None:
        name = str(value)
    if name is None:
        return True
    return str(name).strip() in {"", "Away", "Home", "Unknown"}


def placeholder_event_name(value: Any) -> bool:
    if value is None:
        return True
    return str(value).strip() in {"", "Away @ Home", "Home @ Away", "Unknown @ Unknown"}


def placeholder_selection_text(value: Any) -> bool:
    if value is None:
        return True
    return str(value).strip().lower() in _SELECTION_PLACEHOLDERS


def placeholder_market_name(value: Any, market_key: str) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    if not text:
        return True
    normalized = text.lower().replace(" ", "_")
    return normalized in {market_key, "1", "2", "3", "market", "unknown_market"}


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
    return base._safe_int(base._extract_first(item, ("participant_side_id", "side_id", "sideId", "participantSideId")))


def fixture_team_names(item: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    away, home = base._team_names(item)
    if away and home:
        return away, home

    competitors = item.get("competitors") or item.get("participants") or item.get("teams")
    if isinstance(competitors, list):
        for participant in competitors:
            if not isinstance(participant, dict):
                continue
            name = base._nested_name(participant)
            side_id = participant_side_id(participant)
            if side_id == 1 and not away:
                away = name
            elif side_id == 2 and not home:
                home = name
            if away and home:
                break
    return away, home


def extract_fixture_id_from_event(event: Dict[str, Any]) -> Optional[str]:
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


def fixture_metadata_from_item(item: Dict[str, Any], fallback_index: int = 0) -> Dict[str, Any]:
    away, home = fixture_team_names(item)
    start_time = base._iso(deep_extract_first(item, base._START_KEYS))
    event_id = str(base._event_id(item, fallback_index))
    fixture_id = base._extract_first(item, ("fixture_id", "fixtureId", "fixtureID", "id"))
    if fixture_id is not None:
        fixture_id = str(fixture_id)
    return {
        "event_id": event_id,
        "fixture_id": fixture_id,
        "name": f"{away} @ {home}" if away or home else None,
        "sport": deep_extract_first(item, ("sport", "sport_title", "sport_name", "sportName")),
        "league": deep_extract_first(item, ("league", "league_name", "leagueName", "competition", "sport_key")),
        "league_id": deep_extract_first(item, ("league_id", "leagueId", "competition_id")),
        "home_team": {"name": home} if home else None,
        "away_team": {"name": away} if away else None,
        "start_time": start_time,
        "status": deep_extract_first(item, ("status", "event_status", "eventStatus")),
        "is_live": deep_extract_first(item, ("is_live", "isLive", "live", "in_play", "inPlay")),
    }


def build_fixture_indexes(
    fixture_items: List[Dict[str, Any]],
    fixture_events: List[Dict[str, Any]],
) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    by_event_id: Dict[str, Dict[str, Any]] = {}
    by_fixture_id: Dict[str, Dict[str, Any]] = {}

    for idx, item in enumerate(fixture_items):
        metadata = fixture_metadata_from_item(item, idx)
        if metadata.get("event_id"):
            by_event_id[str(metadata["event_id"])] = metadata
        if metadata.get("fixture_id"):
            by_fixture_id[str(metadata["fixture_id"])] = metadata

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
            existing = by_event_id.get(str(metadata["event_id"]), {})
            by_event_id[str(metadata["event_id"])] = {
                **existing,
                **{k: v for k, v in metadata.items() if v not in (None, "", {"name": None})},
            }
        if metadata.get("fixture_id"):
            existing = by_fixture_id.get(str(metadata["fixture_id"]), {})
            by_fixture_id[str(metadata["fixture_id"])] = {
                **existing,
                **{k: v for k, v in metadata.items() if v not in (None, "", {"name": None})},
            }

    return by_event_id, by_fixture_id


def event_team_name(event: Dict[str, Any], side: str) -> Optional[str]:
    value = event.get(f"{side}_team")
    if isinstance(value, dict):
        return base._nested_name(value)
    return str(value) if value not in (None, "") else None


def selection_display_name(selection: Dict[str, Any], event: Dict[str, Any]) -> Optional[str]:
    raw = selection.get("raw") if isinstance(selection.get("raw"), dict) else selection
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
    market_key = str(market.get("market_key") or market.get("market_type") or "")
    return _MARKET_DISPLAY_NAMES.get(market_key, str(market.get("market_name") or market_key or "Market"))


def enrich_market(event: Dict[str, Any], market: Dict[str, Any]) -> Dict[str, Any]:
    market_copy = dict(market)
    if not market_copy.get("market_type"):
        market_copy["market_type"] = market_copy.get("market_key")
    if placeholder_market_name(market_copy.get("market_name"), str(market_copy.get("market_key") or market_copy.get("market_type") or "")):
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
                if away_name or home_name:
                    event_copy["name"] = f"{away_name or 'Away'} @ {home_name or 'Home'}"
                elif metadata.get("name"):
                    event_copy["name"] = metadata["name"]
            for key in ("sport", "league", "league_id", "status", "is_live"):
                if event_copy.get(key) in (None, "") and metadata.get(key) not in (None, ""):
                    event_copy[key] = metadata[key]

        event_copy["markets"] = [enrich_market(event_copy, market) for market in event.get("markets", []) or []]
        event_copy["market_count"] = len(event_copy["markets"])
        enriched.append(event_copy)

    return enriched
