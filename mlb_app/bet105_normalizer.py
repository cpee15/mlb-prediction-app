from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional

from . import kibl_bet105_provider as legacy
from . import kibl_bet105_sportsbook_enrichment as enrichment
from .kibl_bet105_types import Bet105RawBoard


def _value(row: Dict[str, Any], keys: tuple[str, ...]) -> Any:
    return enrichment.deep_extract_first(row, keys)


def _text(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    return str(value)


def _id(value: Any) -> Optional[str]:
    text = _text(value)
    return text.strip() if text else None


def _name(row: Dict[str, Any]) -> Optional[str]:
    value = _value(row, ("name", "display_name", "displayName", "team_name", "teamName", "fullName", "title"))
    if isinstance(value, dict):
        value = _value(value, ("name", "display_name", "displayName", "team_name", "teamName", "fullName", "title"))
    return _text(value)


def _binary_side(row: Dict[str, Any]) -> Optional[str]:
    info = row.get("info") if isinstance(row.get("info"), dict) else {}
    value = info.get("side") or row.get("side")
    text = str(value or "").strip().lower()
    if text in {"yes", "y"}:
        return "Yes"
    if text in {"no", "n"}:
        return "No"
    return None


def _price(row: Dict[str, Any]) -> Optional[int]:
    parsed = legacy._safe_int(_value(row, ("price_american", "american", "price", "line_price")))
    if parsed is not None:
        return parsed
    return legacy._american_from_decimal(legacy._safe_float(_value(row, ("price_decimal", "decimal", "decimal_odds"))))


def _fixture_meta(row: Dict[str, Any], index: int = 0) -> Dict[str, Any]:
    return enrichment.fixture_metadata_from_item(row, index)


def _fixture_indexes(board: Bet105RawBoard) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    by_fixture: Dict[str, Dict[str, Any]] = {}
    by_event: Dict[str, Dict[str, Any]] = {}
    for idx, row in enumerate(board.fixture_rows):
        meta = _fixture_meta(row, idx)
        if meta.get("fixture_id"):
            by_fixture[str(meta["fixture_id"])] = meta
        if meta.get("event_id"):
            by_event[str(meta["event_id"])] = meta
    return by_fixture, by_event


def _fixture_summary_ids(board: Bet105RawBoard) -> List[str]:
    ids: List[str] = []
    for row in board.fixture_rows:
        for key in ("fixture_id", "event_id", "id"):
            value = _id(row.get(key))
            if value and value not in ids:
                ids.append(value)
    return ids


def _participant_index(board: Bet105RawBoard) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for row in board.participant_rows:
        for key in ("fixture_participant_id", "participant_id", "contestant_id", "line_id", "id"):
            value = _id(_value(row, (key,)))
            if value:
                index[value] = row
    return index


def _selection(row: Dict[str, Any], participant_rows: Dict[str, Dict[str, Any]], event: Dict[str, Any], index: int) -> Dict[str, Any]:
    label = _binary_side(row)
    participant = None
    info = row.get("info") if isinstance(row.get("info"), dict) else {}
    for key in ("fixture_participant_id", "participant_id"):
        participant = participant_rows.get(_id(row.get(key)) or "")
        if participant:
            break
    if participant is None:
        for key in ("contestant_id", "line_id"):
            participant = participant_rows.get(_id(info.get(key)) or "")
            if participant:
                break
    if not label and participant:
        label = _name(participant)
    if not label:
        side_id = legacy._safe_int(row.get("participant_side_id") or row.get("side_id"))
        if side_id == 1:
            label = enrichment.event_team_name(event, "away")
        elif side_id == 2:
            label = enrichment.event_team_name(event, "home")
        elif side_id == 3:
            label = "Over"
        elif side_id == 4:
            label = "Under"
    label = label or "Selection metadata missing"
    price = _price(row)
    return {
        "selection_id": _id(row.get("fixture_participant_id") or row.get("participant_id") or row.get("market_id")) or f"selection_{index}",
        "name": label,
        "description": label,
        "team": label,
        "side": str(label).lower(),
        "line": legacy._safe_float(row.get("point")),
        "price": price,
        "odds": {
            "american": price,
            "decimal": legacy._safe_float(row.get("price_decimal")) or legacy._decimal_from_american(price),
            "fractional": row.get("price_fraction"),
            "implied_probability": legacy._implied_from_american(price),
        },
        "is_open": bool(row.get("is_current", True)),
        "raw": row,
    }


def _market_name(row: Dict[str, Any]) -> tuple[str, str]:
    market_type_id = _id(row.get("market_type_id"))
    if market_type_id == "1":
        return "h2h", "Moneyline"
    if market_type_id == "2":
        return "spreads", "Run Line"
    if market_type_id == "3":
        return "totals", "Total Runs"
    if market_type_id == "0":
        return "binary_yes_no", "Binary Yes/No Market"
    return f"market_{market_type_id or 'unknown'}", f"Unknown Market Type {market_type_id or 'unknown'}"


def _status(markets: List[Dict[str, Any]], diagnostics: Dict[str, int], board: Bet105RawBoard) -> str:
    if not markets:
        return "fixtures_only" if board.fixture_rows else "empty"
    if board.fixture_rows and len(board.fixture_rows) > 1 and len(markets) <= 1:
        return "incomplete_market_discovery"
    if any(diagnostics.values()):
        return "incomplete_normalization"
    return "ok"


def normalize_board(board: Bet105RawBoard, live_only: Optional[bool] = None, game_pk: Optional[Any] = None, raw: bool = False) -> Dict[str, Any]:
    by_fixture, by_event = _fixture_indexes(board)
    participants = _participant_index(board)
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in board.market_rows:
        fixture_id = _id(row.get("fixture_id")) or _id(row.get("event_id")) or "unknown_fixture"
        groups[fixture_id].append(row)

    events: List[Dict[str, Any]] = []
    for idx, (fixture_id, rows) in enumerate(groups.items()):
        meta = by_fixture.get(fixture_id) or by_event.get(fixture_id) or {}
        away = meta.get("away_team")
        home = meta.get("home_team")
        away_name = away.get("name") if isinstance(away, dict) else None
        home_name = home.get("name") if isinstance(home, dict) else None
        event_name = f"{away_name} @ {home_name}" if away_name and home_name else fixture_id
        event = {
            "event_id": fixture_id,
            "fixture_id": fixture_id,
            "name": event_name,
            "sport": meta.get("sport") or "Baseball",
            "league": meta.get("league") or "MLB",
            "league_id": meta.get("league_id") or "mlb",
            "home_team": home,
            "away_team": away,
            "start_time": meta.get("start_time"),
            "commence_time": meta.get("start_time"),
            "status": meta.get("status") or ("live" if live_only else "scheduled"),
            "is_live": bool(live_only),
            "source_url": None,
            "scraped_at": legacy._now(),
            "markets": [],
            "raw": {"rows": rows, "fixture_meta": meta},
        }
        market_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            market_key, market_name = _market_name(row)
            market_groups[market_key].append(row)
        for market_key, market_rows in market_groups.items():
            _, market_name = _market_name(market_rows[0])
            selections = [_selection(row, participants, event, sel_idx) for sel_idx, row in enumerate(market_rows)]
            event["markets"].append({
                "market_key": market_key,
                "market_type": market_key,
                "market_name": market_name,
                "selections": selections,
                "selection_count": len(selections),
                "raw": {"rows": market_rows},
            })
        event["market_count"] = len(event["markets"])
        events.append(event)

    events = sorted(events, key=lambda row: (row.get("start_time") or "", row.get("event_id") or ""))
    markets = legacy._flatten_markets(events, game_pk=game_pk)
    diagnostics = {
        "placeholder_event_names": sum(1 for event in events if str(event.get("name") or "") in {"", "Away @ Home", "Home @ Away"}),
        "placeholder_market_names": 0,
        "placeholder_selection_names": sum(1 for event in events for market in event.get("markets") or [] for selection in market.get("selections") or [] if "metadata missing" in str(selection.get("name") or "").lower()),
        "missing_start_times": sum(1 for event in events if not event.get("start_time")),
    }
    fixture_summary_ids = _fixture_summary_ids(board)
    payload = {
        "provider": "kibl_bet105",
        "book": "bet105",
        "status": _status(markets, diagnostics, board),
        "scope": "live" if live_only else "events",
        "events": events,
        "markets": markets,
        "books": ["Bet105"],
        "last_updated": legacy._now(),
        "raw_count": len(board.market_rows),
        "event_count": len(events),
        "market_count": len(markets),
        "diagnostics": diagnostics,
        "fixtures": {
            "count": len(board.fixture_rows),
            "fixture_ids": fixture_summary_ids or board.ids.get("fixture_id") or [],
            "market_detail_ids": board.ids,
            "fixture_row_count": len(board.fixture_rows),
            "participant_row_count": len(board.participant_rows),
        },
        "request_params": board.filters,
        "normalization_notes": board.notes,
        "raw_items_sample": board.market_rows[:5],
        "cache_hit": False,
    }
    if not raw:
        payload["events"] = legacy._without_raw_events(payload["events"])
    return payload
