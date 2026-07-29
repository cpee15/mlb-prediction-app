"""Cutoff-safe actual hitter components for shadow profile evaluation."""

from __future__ import annotations

import datetime as dt
from collections import defaultdict
from typing import Any, Iterable, Mapping, Optional

from mlb_app.db_utils import (
    HIT_EVENTS,
    NON_AB_EVENTS,
    STRIKEOUT_EVENTS,
    TERMINAL_EVENTS,
    TOTAL_BASES,
    WALK_EVENTS,
)

SCHEMA_VERSION = "shadow_hitter_actual_components_v1"
WINDOW_PRIORS = {
    "current_season": 0.50,
    "prior_season": 0.30,
    "career_pre_prior": 0.20,
}
MIN_CURRENT_PA = 25
RELIABILITY_PA = 200
MAX_SOURCE_AGE_DAYS = 14


def _value(row: Any, name: str, default: Any = None) -> Any:
    if isinstance(row, Mapping):
        return row.get(name, default)
    return getattr(row, name, default)


def _date(value: Any) -> Optional[dt.date]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _split_throw_hand(split: str) -> str:
    normalized = str(split or "").strip()
    if normalized not in {"vsR", "vsL"}:
        raise ValueError("split must be 'vsR' or 'vsL'")
    return normalized[-1]


def _terminal_rows(
    rows: Iterable[Any],
    *,
    split: str,
    as_of_date: dt.date,
) -> list[Any]:
    """Return the final canonical terminal row for each eligible plate appearance."""

    throw_hand = _split_throw_hand(split)
    selected: dict[tuple[Any, Any], Any] = {}
    for row in rows:
        game_date = _date(_value(row, "game_date"))
        event = str(_value(row, "events", "") or "").strip().lower()
        game_pk = _value(row, "game_pk")
        at_bat_number = _value(row, "at_bat_number")
        if (
            game_date is None
            or game_date > as_of_date
            or event not in TERMINAL_EVENTS
            or game_pk is None
            or at_bat_number is None
            or str(_value(row, "p_throws", "") or "").upper() != throw_hand
        ):
            continue
        key = (game_pk, at_bat_number)
        rank = (
            int(_value(row, "pitch_number", 0) or 0),
            int(_value(row, "id", 0) or 0),
        )
        current = selected.get(key)
        current_rank = (
            int(_value(current, "pitch_number", 0) or 0),
            int(_value(current, "id", 0) or 0),
        ) if current is not None else (-1, -1)
        if rank >= current_rank:
            selected[key] = row
    return sorted(
        selected.values(),
        key=lambda row: (
            _date(_value(row, "game_date")) or dt.date.min,
            int(_value(row, "game_pk", 0) or 0),
            int(_value(row, "at_bat_number", 0) or 0),
        ),
    )


def _aggregate(rows: Iterable[Any]) -> dict[str, Any]:
    outcomes = [
        str(_value(row, "events", "") or "").strip().lower()
        for row in rows
    ]
    pa = len(outcomes)
    ab = sum(event not in NON_AB_EVENTS for event in outcomes)
    hits = sum(event in HIT_EVENTS for event in outcomes)
    walks = sum(event in WALK_EVENTS for event in outcomes)
    strikeouts = sum(event in STRIKEOUT_EVENTS for event in outcomes)
    total_bases = sum(TOTAL_BASES.get(event, 0) for event in outcomes)
    batting_avg = hits / ab if ab else None
    slugging_pct = total_bases / ab if ab else None
    return {
        "pa": pa,
        "ab": ab,
        "hits": hits,
        "walks": walks,
        "strikeouts": strikeouts,
        "total_bases": total_bases,
        "k_pct": strikeouts / pa if pa else None,
        "bb_pct": walks / pa if pa else None,
        "batting_avg": batting_avg,
        "slugging_pct": slugging_pct,
        "iso": (
            slugging_pct - batting_avg
            if slugging_pct is not None and batting_avg is not None
            else None
        ),
    }


def _window_name(row_season: int, season: int) -> Optional[str]:
    if row_season == season:
        return "current_season"
    if row_season == season - 1:
        return "prior_season"
    if row_season <= season - 2:
        return "career_pre_prior"
    return None


def build_shadow_hitter_actual_components(
    *,
    player_id: int,
    season: int,
    split: str,
    statcast_events: Iterable[Any],
    as_of_date: Any,
    source_latest_date: Any = None,
    career_start_season: Optional[int] = None,
) -> dict[str, Any]:
    """Build disjoint, cutoff-safe actual-stat windows for one hitter split."""

    cutoff = _date(as_of_date)
    if cutoff is None:
        raise ValueError("as_of_date must be date-like")
    _split_throw_hand(split)
    career_start = int(career_start_season or season - 3)
    rows = _terminal_rows(statcast_events, split=split, as_of_date=cutoff)
    rows = [
        row for row in rows
        if career_start <= (_date(_value(row, "game_date")) or cutoff).year <= season
    ]

    grouped: dict[str, list[Any]] = defaultdict(list)
    for row in rows:
        game_date = _date(_value(row, "game_date"))
        name = _window_name(game_date.year, season) if game_date else None
        if name:
            grouped[name].append(row)

    windows: dict[str, dict[str, Any]] = {}
    for name in ("current_season", "prior_season", "career_pre_prior"):
        if not grouped.get(name):
            continue
        aggregate = _aggregate(grouped[name])
        aggregate["seasons"] = sorted({
            _date(_value(row, "game_date")).year for row in grouped[name]
        })
        aggregate["start_date"] = min(
            _date(_value(row, "game_date")) for row in grouped[name]
        ).isoformat()
        aggregate["end_date"] = max(
            _date(_value(row, "game_date")) for row in grouped[name]
        ).isoformat()
        aggregate["prior_weight"] = WINDOW_PRIORS[name]
        aggregate["reliability"] = min(1.0, aggregate["pa"] / RELIABILITY_PA)
        aggregate["raw_weight"] = (
            aggregate["prior_weight"] * aggregate["reliability"]
        )
        windows[name] = aggregate

    weight_total = sum(window["raw_weight"] for window in windows.values())
    for window in windows.values():
        window["normalized_weight"] = (
            window["raw_weight"] / weight_total if weight_total else 0.0
        )

    metrics = ("k_pct", "bb_pct", "batting_avg", "slugging_pct", "iso")
    blended = {}
    for metric in metrics:
        available = [
            window for window in windows.values()
            if window.get(metric) is not None
        ]
        denominator = sum(window["normalized_weight"] for window in available)
        blended[metric] = (
            sum(
                window[metric] * window["normalized_weight"]
                for window in available
            ) / denominator
            if denominator
            else None
        )

    blockers: list[str] = []
    current_pa = windows.get("current_season", {}).get("pa", 0)
    if not current_pa:
        blockers.append("missing_current_season_actuals")
    elif current_pa < MIN_CURRENT_PA:
        blockers.append("insufficient_current_season_pa")
    if len(windows) < 2:
        blockers.append("insufficient_disjoint_windows")

    source_latest = _date(source_latest_date)
    source_age = (
        (cutoff - source_latest).days if source_latest is not None else None
    )
    if source_latest is None:
        blockers.append("statcast_source_freshness_unverifiable")
    elif source_age > MAX_SOURCE_AGE_DAYS:
        blockers.append("stale_statcast_source")

    player_latest = max(
        (_date(_value(row, "game_date")) for row in rows),
        default=None,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "blocked" if blockers else "ready",
        "shadow_only": True,
        "production_authority_changed": False,
        "player_id": int(player_id),
        "season": int(season),
        "split": split,
        "as_of_date": cutoff.isoformat(),
        "career_start_season": career_start,
        "source_latest_date": (
            source_latest.isoformat() if source_latest else None
        ),
        "source_age_days": source_age,
        "player_latest_date": (
            player_latest.isoformat() if player_latest else None
        ),
        "windows": windows,
        "blended_actual_metrics": blended,
        "blockers": blockers,
        "warnings": [],
        "cutoff_policy": {
            "date_rule": "game_date <= as_of_date",
            "window_policy": "disjoint_current_prior_career_pre_prior",
            "pa_dedupe_key": ["game_pk", "at_bat_number"],
            "terminal_event_source": "mlb_app.db_utils.TERMINAL_EVENTS",
            "split_field": "p_throws",
        },
    }


def load_shadow_hitter_actual_components(
    session: Any,
    *,
    player_id: int,
    season: int,
    split: str,
    as_of_date: Any,
    career_start_season: Optional[int] = None,
) -> dict[str, Any]:
    """Load cutoff-safe Statcast evidence for one player and pitcher-hand split."""

    from sqlalchemy import func

    from mlb_app.database import StatcastEvent

    cutoff = _date(as_of_date)
    if cutoff is None:
        raise ValueError("as_of_date must be date-like")
    career_start = int(career_start_season or season - 3)
    start_date = dt.date(career_start, 1, 1)
    rows = (
        session.query(StatcastEvent)
        .filter(
            StatcastEvent.batter_id == int(player_id),
            StatcastEvent.game_date >= start_date,
            StatcastEvent.game_date <= cutoff,
            StatcastEvent.events.isnot(None),
            StatcastEvent.p_throws == _split_throw_hand(split),
        )
        .order_by(
            StatcastEvent.game_date,
            StatcastEvent.game_pk,
            StatcastEvent.at_bat_number,
            StatcastEvent.pitch_number,
            StatcastEvent.id,
        )
        .all()
    )
    source_latest = (
        session.query(func.max(StatcastEvent.game_date))
        .filter(StatcastEvent.game_date <= cutoff)
        .scalar()
    )
    result = build_shadow_hitter_actual_components(
        player_id=player_id,
        season=season,
        split=split,
        statcast_events=rows,
        as_of_date=cutoff,
        source_latest_date=source_latest,
        career_start_season=career_start,
    )
    result["storage_evidence"] = {
        "raw_row_count": len(rows),
        "terminal_pa_count": sum(
            window["pa"] for window in result["windows"].values()
        ),
        "career_start_date": start_date.isoformat(),
    }
    return result
