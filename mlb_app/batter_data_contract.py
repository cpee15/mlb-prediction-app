from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from .database import StatcastEvent
from .db_utils import (
    TERMINAL_EVENTS,
    _calculate_batter_stats,
    _dedupe_terminal_pas,
    _is_true_ab_event,
    _ordered_batter_terminal_query,
    get_batter_data_quality,
)

STATCAST_ROLLING_SOURCE = "postgres_statcast_events_deduped_terminal_pa"
MLB_SEASON_SOURCE = "mlb_stats_api_people_stats_season"
MLB_SPLITS_SOURCE = "mlb_stats_api_people_stats_stat_splits"
MLB_YEAR_BY_YEAR_SOURCE = "mlb_stats_api_people_stats_year_by_year"
LOCAL_SPLITS_SOURCE = "postgres_player_splits"


def parse_window_list(raw: Optional[str], defaults: Iterable[int], *, minimum: int = 1, maximum: int = 5000) -> List[int]:
    """Parse, bound, and de-duplicate requested rolling windows in request order."""
    values: List[int] = []
    for token in str(raw or "").split(","):
        token = token.strip()
        if not token.isdigit():
            continue
        value = int(token)
        if minimum <= value <= maximum and value not in values:
            values.append(value)
    if values:
        return values
    out: List[int] = []
    for value in defaults:
        if minimum <= int(value) <= maximum and int(value) not in out:
            out.append(int(value))
    return out


def _quality_with_source(session: Session, batter_id: int, removed: int = 0) -> Dict[str, Any]:
    quality = dict(get_batter_data_quality(session, batter_id) or {})
    quality["source"] = STATCAST_ROLLING_SOURCE
    quality["dedupe_policy"] = "terminal PA identity: game_pk + at_bat_number + pitcher_id + batter_id"
    quality["duplicate_terminal_pa_rows_removed_in_window"] = removed
    warnings = list(quality.get("warnings") or [])
    if removed:
        warnings.append(f"Removed {removed} duplicate terminal PA row(s) before calculating this rolling window.")
    quality["warnings"] = warnings
    return quality


def _clean_terminal_events(session: Session, batter_id: int, requested: int, *, multiplier: int = 4) -> Tuple[List[StatcastEvent], int]:
    raw_limit = max(requested * multiplier, requested + 50)
    raw_events = _ordered_batter_terminal_query(session, batter_id).limit(raw_limit).all()
    deduped = _dedupe_terminal_pas(raw_events)
    removed = max(len(raw_events) - len(deduped), 0)
    return deduped, removed


def clean_rolling_by_pa(session: Session, batter_id: int, n_pa: int) -> Optional[Dict[str, Any]]:
    events, removed = _clean_terminal_events(session, batter_id, n_pa)
    window_events = events[:n_pa]
    if not window_events:
        return None
    stats = _calculate_batter_stats(window_events, raw_event_count=len(window_events) + removed)
    stats.update({
        "actual_pa": len(window_events),
        "requested_pa": n_pa,
        "window_type": "PA",
        "label": f"Last {n_pa} PA",
        "source": STATCAST_ROLLING_SOURCE,
        "deduped": True,
        "duplicate_rows_removed": removed,
        "data_quality": _quality_with_source(session, batter_id, removed),
    })
    return stats


def clean_rolling_by_ab(session: Session, batter_id: int, n_ab: int) -> Optional[Dict[str, Any]]:
    events, removed = _clean_terminal_events(session, batter_id, n_ab, multiplier=7)
    ab_events = [event for event in events if _is_true_ab_event(event.events)][:n_ab]
    if not ab_events:
        return None
    stats = _calculate_batter_stats(ab_events, raw_event_count=len(events) + removed)
    stats.update({
        "actual_ab": len(ab_events),
        "requested_ab": n_ab,
        "window_type": "AB",
        "label": f"Last {n_ab} AB",
        "source": STATCAST_ROLLING_SOURCE,
        "deduped": True,
        "duplicate_rows_removed": removed,
        "data_quality": _quality_with_source(session, batter_id, removed),
    })
    return stats


def clean_rolling_by_games(session: Session, batter_id: int, n_games: int) -> Optional[Dict[str, Any]]:
    game_rows = (
        session.query(StatcastEvent.game_pk, func.max(StatcastEvent.game_date).label("game_date"))
        .filter(
            StatcastEvent.batter_id == batter_id,
            StatcastEvent.game_pk.isnot(None),
            StatcastEvent.events.in_(TERMINAL_EVENTS),
        )
        .group_by(StatcastEvent.game_pk)
        .order_by(func.max(StatcastEvent.game_date).desc(), StatcastEvent.game_pk.desc())
        .limit(n_games)
        .all()
    )
    if not game_rows:
        return None
    game_pks = [row.game_pk for row in game_rows]
    raw_events = (
        session.query(StatcastEvent)
        .filter(
            StatcastEvent.batter_id == batter_id,
            StatcastEvent.game_pk.in_(game_pks),
            StatcastEvent.events.in_(TERMINAL_EVENTS),
        )
        .all()
    )
    events = _dedupe_terminal_pas(raw_events)
    removed = max(len(raw_events) - len(events), 0)
    if not events:
        return None
    stats = _calculate_batter_stats(events, raw_event_count=len(raw_events))
    stats.update({
        "actual_games": len(game_pks),
        "requested_games": n_games,
        "window_type": "games",
        "source": STATCAST_ROLLING_SOURCE,
        "deduped": True,
        "duplicate_rows_removed": removed,
        "data_quality": _quality_with_source(session, batter_id, removed),
    })
    return stats


def clean_rolling_splits(session: Session, batter_id: int, n_pa: int = 100) -> Dict[str, Any]:
    events, removed = _clean_terminal_events(session, batter_id, n_pa)
    window_events = events[:n_pa]
    grouped = {"vsL": [], "vsR": [], "unknown": []}
    for event in window_events:
        key = "vsL" if event.p_throws == "L" else "vsR" if event.p_throws == "R" else "unknown"
        grouped[key].append(event)
    return {
        "window_type": "PA",
        "requested_pa": n_pa,
        "actual_pa": len(window_events),
        "source": STATCAST_ROLLING_SOURCE,
        "deduped": True,
        "duplicate_rows_removed": removed,
        "splits": {key: ({**_calculate_batter_stats(value), "actual_pa": len(value), "source": STATCAST_ROLLING_SOURCE} if value else None) for key, value in grouped.items()},
        "data_quality": _quality_with_source(session, batter_id, removed),
    }


def clean_rolling_pitch_types(session: Session, batter_id: int, n_pa: int = 100) -> Dict[str, Any]:
    events, removed = _clean_terminal_events(session, batter_id, n_pa)
    window_events = events[:n_pa]
    grouped: Dict[str, List[StatcastEvent]] = {}
    for event in window_events:
        key = event.pitch_type or "unknown"
        grouped.setdefault(key, []).append(event)
    return {
        "window_type": "PA",
        "requested_pa": n_pa,
        "actual_pa": len(window_events),
        "source": STATCAST_ROLLING_SOURCE,
        "deduped": True,
        "duplicate_rows_removed": removed,
        "pitch_types": {key: {**_calculate_batter_stats(value), "actual_pa": len(value), "source": STATCAST_ROLLING_SOURCE} for key, value in sorted(grouped.items(), key=lambda item: len(item[1]), reverse=True)},
        "data_quality": _quality_with_source(session, batter_id, removed),
    }


def dedupe_rows(rows: Iterable[Dict[str, Any]], keys: Iterable[str]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        key = tuple(row.get(k) for k in keys)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out
