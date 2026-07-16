#!/usr/bin/env python3
"""Inspect or refresh the canonical My Dashboard player projection."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mlb_app.dashboard_canonical_status import canonical_dashboard_status
from mlb_app.dashboard_projection_operator import run_canonical_projection_refresh, run_projection_backfill
from mlb_app.database import create_tables, get_engine, get_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=dt.date.today().isoformat(), help="Target MLB date (YYYY-MM-DD)")
    parser.add_argument("--refresh", action="store_true", help="Gather verified sources and refresh the canonical current projection")
    parser.add_argument("--backfill-days", type=int, default=0, help="Create projection snapshots for this many days ending on --date")
    parser.add_argument("--transition-missing-players", action="store_true", help="Deactivate players absent from the verified complete source set")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    target_date = dt.date.fromisoformat(args.date)
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    factory = get_session(engine)

    with factory() as session:
        before = canonical_dashboard_status(session)
        output = {"before": before, "operation": "status_only"}
        if args.refresh:
            output["operation"] = "canonical_refresh"
            output["refresh"] = run_canonical_projection_refresh(
                session,
                target_date=target_date,
                transition_missing_players=args.transition_missing_players,
            )
        if args.backfill_days:
            if args.backfill_days < 1:
                raise ValueError("--backfill-days must be positive")
            dates = [target_date - dt.timedelta(days=offset) for offset in reversed(range(args.backfill_days))]
            output["operation"] = "refresh_and_backfill" if args.refresh else "projection_backfill"
            output["backfill"] = run_projection_backfill(session, dates=dates)
        output["after"] = canonical_dashboard_status(session)
        print(json.dumps(output, indent=2, sort_keys=True, default=str))
        return 0 if output["after"]["status"] == "ready" or output["operation"] == "status_only" else 2


if __name__ == "__main__":
    raise SystemExit(main())
