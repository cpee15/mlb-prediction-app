#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if load_dotenv is not None:
    load_dotenv(ROOT / ".env")

from sqlalchemy import func

from mlb_app.data_freshness import build_data_freshness_payload
from mlb_app.database import (
    BatterAggregate,
    BatterPitchTypeMatchup,
    PitchArsenal,
    PitcherAggregate,
    StatcastEvent,
    create_tables,
    get_engine,
    get_session,
)
from mlb_app.db_utils import TERMINAL_EVENTS
from mlb_app.etl import run_etl_for_date

MLB_TZ = ZoneInfo("America/New_York")


def _today_mlb() -> dt.date:
    return dt.datetime.now(MLB_TZ).date()


def resolve_target_dates(date_arg: Optional[str], days: int, include_today: bool) -> List[str]:
    if date_arg:
        dt.date.fromisoformat(date_arg)
        return [date_arg]

    today = _today_mlb()
    dates: List[dt.date] = []

    if days <= 0:
        dates.append(today - dt.timedelta(days=1))
    else:
        end_offset = 0 if include_today else 1
        for offset in range(days - 1 + end_offset, end_offset - 1, -1):
            dates.append(today - dt.timedelta(days=offset))

    if include_today and today not in dates:
        dates.append(today)

    deduped = []
    for value in dates:
        iso = value.isoformat()
        if iso not in deduped:
            deduped.append(iso)
    return deduped


def _database_url(allow_sqlite_local: bool) -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    if allow_sqlite_local:
        return "sqlite:///mlb.db"
    raise RuntimeError("DATABASE_URL is required for nightly Statcast refresh. Use --allow-sqlite-local only for local validation.")


def _rows_for_date(session, date_str: str) -> int:
    target = dt.date.fromisoformat(date_str)
    return session.query(func.count(StatcastEvent.id)).filter(StatcastEvent.game_date == target).scalar() or 0


def _validation_summary(database_url: str, dates: List[str]) -> Dict[str, Any]:
    engine = get_engine(database_url)
    create_tables(engine)
    Session = get_session(engine)
    as_of = _today_mlb()
    season = as_of.year
    recent_cutoff = as_of - dt.timedelta(days=7)

    with Session() as session:
        freshness = build_data_freshness_payload(session, database_url=database_url, as_of=as_of)
        statcast_rows_by_date = {date_str: _rows_for_date(session, date_str) for date_str in dates}
        recent_pitcher_aggregate_count = (
            session.query(func.count(PitcherAggregate.id))
            .filter(PitcherAggregate.end_date >= recent_cutoff)
            .scalar()
            or 0
        )
        recent_batter_aggregate_count = (
            session.query(func.count(BatterAggregate.id))
            .filter(BatterAggregate.end_date >= recent_cutoff)
            .scalar()
            or 0
        )
        current_season_pitch_arsenal_count = (
            session.query(func.count(PitchArsenal.id)).filter(PitchArsenal.season == season).scalar() or 0
        )
        current_season_batter_terminal_rows = (
            session.query(func.count(StatcastEvent.id))
            .filter(
                StatcastEvent.game_date >= dt.date(season, 1, 1),
                StatcastEvent.game_date <= dt.date(season, 12, 31),
                StatcastEvent.batter_id.isnot(None),
                StatcastEvent.events.in_(list(TERMINAL_EVENTS)),
            )
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
        latest_stored_365_refresh = session.query(func.max(BatterPitchTypeMatchup.refreshed_at)).scalar()

    return {
        "freshness": freshness,
        "latest_statcast_event_date": freshness.get("latest_statcast_event_date"),
        "statcast_rows_by_date": statcast_rows_by_date,
        "recent_pitcher_aggregate_count": int(recent_pitcher_aggregate_count),
        "recent_batter_aggregate_count": int(recent_batter_aggregate_count),
        "current_season_pitch_arsenal_count": int(current_season_pitch_arsenal_count),
        "current_season_batter_terminal_rows": int(current_season_batter_terminal_rows),
        "recent_distinct_pitcher_count_7d": int(recent_distinct_pitcher_count_7d),
        "recent_distinct_batter_count_7d": int(recent_distinct_batter_count_7d),
        "latest_batter_pitch_type_matchup_refresh": latest_stored_365_refresh.isoformat() if latest_stored_365_refresh else None,
    }


def run_refresh(
    date_arg: Optional[str],
    days: int,
    include_today: bool,
    allow_sqlite_local: bool,
    validate_only: bool = False,
) -> Dict[str, Any]:
    database_url = _database_url(allow_sqlite_local)
    os.environ["DATABASE_URL"] = database_url
    dates = resolve_target_dates(date_arg, days, include_today)

    errors: List[Dict[str, str]] = []
    refreshed: List[str] = []
    refresh_results: List[Dict[str, Any]] = []

    if not validate_only:
        for date_str in dates:
            try:
                result = run_etl_for_date(date_str, datewide_statcast=True)
                refresh_results.append(result if isinstance(result, dict) else {"date": date_str, "status": "ok"})
                refreshed.append(date_str)
            except Exception as exc:
                errors.append({"date": date_str, "type": exc.__class__.__name__, "message": str(exc)})

    validation = _validation_summary(database_url, dates)
    warnings = list((validation.get("freshness") or {}).get("warnings") or [])
    if validate_only:
        warnings.append("validate_only=true; no ETL refresh was executed.")

    status = "ok"
    if errors and not refreshed and not validate_only:
        status = "error"
    elif errors:
        status = "partial"
    elif (validation.get("freshness") or {}).get("status") in {"stale", "empty"}:
        status = "warning"

    return {
        "status": status,
        "validate_only": validate_only,
        "dates_requested": dates,
        "dates_refreshed": refreshed,
        "refresh_results": refresh_results,
        "latest_statcast_event_date": validation.get("latest_statcast_event_date"),
        "statcast_rows_by_date": validation.get("statcast_rows_by_date"),
        "recent_pitcher_aggregate_count": validation.get("recent_pitcher_aggregate_count"),
        "recent_batter_aggregate_count": validation.get("recent_batter_aggregate_count"),
        "current_season_pitch_arsenal_count": validation.get("current_season_pitch_arsenal_count"),
        "current_season_batter_terminal_rows": validation.get("current_season_batter_terminal_rows"),
        "recent_distinct_pitcher_count_7d": validation.get("recent_distinct_pitcher_count_7d"),
        "recent_distinct_batter_count_7d": validation.get("recent_distinct_batter_count_7d"),
        "latest_batter_pitch_type_matchup_refresh": validation.get("latest_batter_pitch_type_matchup_refresh"),
        "freshness": validation.get("freshness"),
        "warnings": warnings,
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh Statcast-backed MLB data for production UI surfaces.")
    parser.add_argument("--date", help="Refresh one date in YYYY-MM-DD format")
    parser.add_argument(
        "--days",
        type=int,
        default=10,
        help="Number of recent MLB dates to refresh when --date is omitted. Default is 10 for overlap and late Statcast availability.",
    )
    parser.add_argument("--include-today", action="store_true", help="Include the current MLB Eastern date")
    parser.add_argument("--allow-sqlite-local", action="store_true", help="Allow sqlite:///mlb.db fallback for local validation")
    parser.add_argument("--validate-only", action="store_true", help="Only inspect DB freshness without running ETL")
    args = parser.parse_args()

    try:
        result = run_refresh(
            date_arg=args.date,
            days=args.days,
            include_today=args.include_today,
            allow_sqlite_local=args.allow_sqlite_local,
            validate_only=args.validate_only,
        )
    except Exception as exc:
        result = {
            "status": "error",
            "errors": [{"type": exc.__class__.__name__, "message": str(exc)}],
            "warnings": [],
        }
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return 1

    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 1 if result.get("status") == "error" else 0


if __name__ == "__main__":
    raise SystemExit(main())
