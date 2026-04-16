#!/usr/bin/env python3
"""
Bootstrap the database with the last N days of data.

Usage:
    python seed_db.py              # last 30 days
    python seed_db.py --days 7     # last 7 days
    python seed_db.py --date 2026-04-15  # single date
"""

import argparse
import os
import sys

from dotenv import load_dotenv

load_dotenv()

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(__file__))

from mlb_app.database import get_engine, create_tables, get_session
from mlb_app.etl import run_etl_for_date, run_backfill


def main():
    parser = argparse.ArgumentParser(description="Seed MLB database")
    parser.add_argument("--days", type=int, default=30, help="Days to backfill (default 30)")
    parser.add_argument("--date", help="Load a single date YYYY-MM-DD instead of backfill")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(db_url)
    create_tables(engine)
    print(f"Database initialized at: {db_url}")

    if args.date:
        print(f"Loading data for {args.date}...")
        run_etl_for_date(args.date)
    else:
        print(f"Backfilling last {args.days} days...")
        run_backfill(args.days)

    print("Seed complete.")


if __name__ == "__main__":
    main()
