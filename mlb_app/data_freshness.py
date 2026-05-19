from __future__ import annotations

import datetime as dt
import os
from typing import Any, Dict, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from .database import BatterAggregate, BatterPitchTypeMatchup, PitchArsenal, PitcherAggregate, StatcastEvent
from .db_utils import TERMINAL_EVENTS


def _iso(value: Optional[Any]) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _days_stale(value: Optional[dt.date], as_of: dt.date) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        value = value.date()
    return (as_of - value).days


def _db_type(database_url: Optional[str] = None) -> str:
    raw = database_url or os.getenv("DATABASE_URL") or "sqlite:///mlb.db"
    lowered = raw.lower()
    if lowered.startswith(("postgresql", "postgres")):
        return "postgresql"
    if lowered.startswith("sqlite"):
        return "sqlite"
    return "other" if raw else "missing"


def build_data_freshness_payload(session: Session, database_url: Optional[str] = None, as_of: Optional[dt.date] = None) -> Dict[str, Any]:
    as_of = as_of or dt.date.today()
    season = as_of.year
    season_start = dt.date(season, 1, 1)
    season_end = dt.date(season, 12, 31)
    recent_cutoff = as_of - dt.timedelta(days=7)

    latest_statcast = session.query(func.max(StatcastEvent.game_date)).scalar()
    latest_pitcher_event = (
        session.query(func.max(StatcastEvent.game_date))
        .filter(StatcastEvent.pitcher_id.isnot(None), StatcastEvent.pitcher_id != 0)
        .scalar()
    )
    latest_batter_event = (
        session.query(func.max(StatcastEvent.game_date))
        .filter(StatcastEvent.batter_id.isnot(None), StatcastEvent.batter_id != 0)
        .scalar()
    )
    latest_pitcher_agg = session.query(func.max(PitcherAggregate.end_date)).scalar()
    latest_batter_agg = session.query(func.max(BatterAggregate.end_date)).scalar()
    latest_stored_365_refresh = session.query(func.max(BatterPitchTypeMatchup.refreshed_at)).scalar()
    latest_batter_board = (
        session.query(func.max(StatcastEvent.game_date))
        .filter(
            StatcastEvent.game_date >= season_start,
            StatcastEvent.game_date <= season_end,
            StatcastEvent.batter_id.isnot(None),
            StatcastEvent.events.in_(list(TERMINAL_EVENTS)),
        )
        .scalar()
    )

    pitch_arsenal_count = (
        session.query(func.count(PitchArsenal.id)).filter(PitchArsenal.season == season).scalar() or 0
    )
    batter_terminal_rows = (
        session.query(func.count(StatcastEvent.id))
        .filter(
            StatcastEvent.game_date >= season_start,
            StatcastEvent.game_date <= season_end,
            StatcastEvent.batter_id.isnot(None),
            StatcastEvent.events.in_(list(TERMINAL_EVENTS)),
        )
        .scalar()
        or 0
    )
    statcast_rows = (
        session.query(func.count(StatcastEvent.id))
        .filter(StatcastEvent.game_date >= season_start, StatcastEvent.game_date <= season_end)
        .scalar()
        or 0
    )
    recent_distinct_pitcher_count_7d = (
        session.query(func.count(func.distinct(StatcastEvent.pitcher_id)))
        .filter(
            StatcastEvent.game_date >= recent_cutoff,
            StatcastEvent.pitcher_id.isnot(None),
            StatcastEvent.pitcher_id != 0,
        )
        .scalar()
        or 0
    )
    recent_distinct_batter_count_7d = (
        session.query(func.count(func.distinct(StatcastEvent.batter_id)))
        .filter(
            StatcastEvent.game_date >= recent_cutoff,
            StatcastEvent.batter_id.isnot(None),
            StatcastEvent.batter_id != 0,
        )
        .scalar()
        or 0
    )

    statcast_days = _days_stale(latest_statcast, as_of)
    pitcher_event_days = _days_stale(latest_pitcher_event, as_of)
    batter_event_days = _days_stale(latest_batter_event, as_of)
    pitcher_days = _days_stale(latest_pitcher_agg, as_of)
    batter_board_days = _days_stale(latest_batter_board, as_of)
    batter_agg_days = _days_stale(latest_batter_agg, as_of)

    warnings = []
    if statcast_days is None:
        warnings.append("No StatcastEvent rows found.")
    elif statcast_days > 2:
        warnings.append(f"StatcastEvent data is stale by {statcast_days} days.")
    if pitcher_event_days is None:
        warnings.append("No pitcher StatcastEvent rows found.")
    elif pitcher_event_days > 2:
        warnings.append(f"Pitcher StatcastEvent rows are stale by {pitcher_event_days} days.")
    if batter_event_days is None:
        warnings.append("No batter StatcastEvent rows found.")
    elif batter_event_days > 2:
        warnings.append(f"Batter StatcastEvent rows are stale by {batter_event_days} days.")
    if pitcher_days is None:
        warnings.append("No PitcherAggregate rows found.")
    elif pitcher_days > 2:
        warnings.append(f"PitcherAggregate data is stale by {pitcher_days} days.")
    if batter_board_days is None:
        warnings.append("No current-season terminal batter Statcast rows found for leaderboards.")
    elif batter_board_days > 2:
        warnings.append(f"Batter leaderboard source rows are stale by {batter_board_days} days.")
    if pitch_arsenal_count == 0:
        warnings.append("No current-season PitchArsenal rows found.")

    if latest_statcast is None and latest_pitcher_agg is None:
        status = "empty"
    elif any(
        days is not None and days > 2
        for days in (statcast_days, pitcher_event_days, batter_event_days, pitcher_days, batter_board_days)
    ):
        status = "stale"
    else:
        status = "fresh"

    return {
        "status": status,
        "as_of_date": as_of.isoformat(),
        "season": season,
        "database_url_type": _db_type(database_url),
        "latest_statcast_event_date": _iso(latest_statcast),
        "statcast_days_stale": statcast_days,
        "latest_pitcher_event_date": _iso(latest_pitcher_event),
        "pitcher_event_days_stale": pitcher_event_days,
        "latest_batter_event_date": _iso(latest_batter_event),
        "batter_event_days_stale": batter_event_days,
        "pitcher_aggregate_latest_end_date": _iso(latest_pitcher_agg),
        "pitcher_aggregate_days_stale": pitcher_days,
        "batter_aggregate_latest_end_date": _iso(latest_batter_agg),
        "batter_aggregate_days_stale": batter_agg_days,
        "batter_leaderboard_latest_event_date": _iso(latest_batter_board),
        "batter_leaderboard_days_stale": batter_board_days,
        "pitch_arsenal_current_season_count": int(pitch_arsenal_count),
        "current_season_batter_terminal_rows": int(batter_terminal_rows),
        "current_season_statcast_rows": int(statcast_rows),
        "recent_distinct_pitcher_count_7d": int(recent_distinct_pitcher_count_7d),
        "recent_distinct_batter_count_7d": int(recent_distinct_batter_count_7d),
        "latest_batter_pitch_type_matchup_refresh": _iso(latest_stored_365_refresh),
        "warnings": warnings,
    }


__all__ = ["build_data_freshness_payload"]
