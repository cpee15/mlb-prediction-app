#!/usr/bin/env python3
"""Backfill historical hitter Statcast for sampled lineup hitters.

This is a production-safe helper for the player true-talent profile work.

Default behavior is audit/dry-run only:
    DRY_RUN=true
    APPLY=false

It targets hitters from the historical backtest lineup sample, not today's slate.
It reuses the existing hitter Statcast upsert/checkpoint logic from:
    scripts/backfill_hitter_statcast.py

Typical dry run:
    BACKTEST_START=2026-04-20 BACKTEST_END=2026-05-03 \
    python scripts/backfill_historical_hitter_statcast_sample.py

Apply one season after reviewing dry-run:
    APPLY=true DRY_RUN=false SEASONS=2025 \
    BACKTEST_START=2026-04-20 BACKTEST_END=2026-05-03 \
    python scripts/backfill_historical_hitter_statcast_sample.py
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mlb_app.database import StatcastEvent, create_tables, get_engine, get_session
from mlb_app.lineup_profile import fetch_boxscore_lineup
from mlb_app.matchup_generator import generate_matchups_for_date
from mlb_app.statcast_utils import fetch_statcast_batter_data

# Reuse proven fetch/upsert/checkpoint helpers from the existing script.
from scripts.backfill_hitter_statcast import (  # noqa: E402
    _ensure_checkpoint_table,
    _insert_or_update_batter_statcast,
)


DEFAULT_SEASONS = [2023, 2024, 2025]


def _log(message: str) -> None:
    timestamp = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    print(f"[{timestamp}] {message}", flush=True)


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or isinstance(value, bool):
            return None
        return int(value)
    except Exception:
        return None


def _date_range(start: str, end: str) -> Iterable[str]:
    current = dt.date.fromisoformat(start)
    stop = dt.date.fromisoformat(end)
    while current <= stop:
        yield current.isoformat()
        current += dt.timedelta(days=1)


def _parse_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_seasons() -> List[int]:
    raw = os.getenv("SEASONS")
    if not raw:
        return list(DEFAULT_SEASONS)

    seasons: List[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        seasons.append(int(part))

    return seasons


def _season_window(season: int) -> Tuple[str, str]:
    return f"{season}-03-01", f"{season}-11-30"


def _collect_sample_hitters(session, start: str, end: str, max_hitters: Optional[int]) -> List[Dict[str, Any]]:
    hitters_by_id: Dict[int, Dict[str, Any]] = {}

    for date_str in _date_range(start, end):
        matchups = generate_matchups_for_date(session, date_str)

        for matchup in matchups:
            game_pk = _safe_int(matchup.get("game_pk"))
            if not game_pk:
                continue

            try:
                lineups = fetch_boxscore_lineup(game_pk)
            except Exception as exc:
                _log(f"Lineup fetch failed game_pk={game_pk} date={date_str}: {exc}")
                continue

            for side in ("away", "home"):
                team_id = matchup.get(f"{side}_team_id")
                team_name = matchup.get(f"{side}_team_name")

                for hitter in lineups.get(side) or []:
                    player_id = _safe_int(hitter.get("batter_id"))
                    if player_id is None:
                        continue

                    existing = hitters_by_id.get(player_id)
                    if existing is None:
                        hitters_by_id[player_id] = {
                            "player_id": player_id,
                            "player_name": hitter.get("name"),
                            "first_seen_date": date_str,
                            "last_seen_date": date_str,
                            "sample_lineup_appearances": 1,
                            "sample_team_ids": {str(team_id)} if team_id is not None else set(),
                            "sample_team_names": {str(team_name)} if team_name else set(),
                        }
                    else:
                        existing["last_seen_date"] = date_str
                        existing["sample_lineup_appearances"] += 1
                        if team_id is not None:
                            existing["sample_team_ids"].add(str(team_id))
                        if team_name:
                            existing["sample_team_names"].add(str(team_name))

    hitters: List[Dict[str, Any]] = []
    for row in hitters_by_id.values():
        row["sample_team_ids"] = ",".join(sorted(row["sample_team_ids"]))
        row["sample_team_names"] = ",".join(sorted(row["sample_team_names"]))
        hitters.append(row)

    hitters.sort(key=lambda row: (row.get("player_name") or "", row["player_id"]))

    if max_hitters is not None and max_hitters > 0:
        hitters = hitters[:max_hitters]

    return hitters


def _existing_event_count(session, batter_id: int, start_date: str, end_date: str) -> int:
    start = dt.date.fromisoformat(start_date)
    end = dt.date.fromisoformat(end_date)
    return int(
        session.query(StatcastEvent.id)
        .filter(
            StatcastEvent.batter_id == batter_id,
            StatcastEvent.game_date >= start,
            StatcastEvent.game_date <= end,
        )
        .count()
        or 0
    )


def _dry_run_fetch_estimate(batter: Dict[str, Any], start_date: str, end_date: str) -> Dict[str, Any]:
    batter_id = int(batter["player_id"])
    try:
        df = fetch_statcast_batter_data(batter_id, start_date, end_date)
    except Exception as exc:
        return {
            **batter,
            "start_date": start_date,
            "end_date": end_date,
            "fetched_rows": 0,
            "error": str(exc),
        }

    fetched_rows = 0 if df is None else int(len(df))
    return {
        **batter,
        "start_date": start_date,
        "end_date": end_date,
        "fetched_rows": fetched_rows,
    }


def main() -> int:
    start = os.getenv("BACKTEST_START", "2026-04-20")
    end = os.getenv("BACKTEST_END", "2026-05-03")
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    seasons = _parse_seasons()

    dry_run = _parse_bool_env("DRY_RUN", True)
    apply = _parse_bool_env("APPLY", False)
    should_write = bool(apply and not dry_run)

    max_hitters_raw = os.getenv("MAX_HITTERS")
    max_hitters = int(max_hitters_raw) if max_hitters_raw else None

    fetch_in_dry_run = _parse_bool_env("FETCH_IN_DRY_RUN", False)

    engine = get_engine(database_url)
    create_tables(engine)
    _ensure_checkpoint_table(engine)
    SessionLocal = get_session(engine)

    _log("=== HISTORICAL HITTER STATCAST SAMPLE BACKFILL ===")
    _log(f"BACKTEST_START={start}")
    _log(f"BACKTEST_END={end}")
    _log(f"SEASONS={','.join(str(s) for s in seasons)}")
    _log(f"DATABASE_URL={database_url}")
    _log(f"DRY_RUN={dry_run}")
    _log(f"APPLY={apply}")
    _log(f"should_write={should_write}")
    _log(f"FETCH_IN_DRY_RUN={fetch_in_dry_run}")
    _log(f"MAX_HITTERS={max_hitters}")

    rows: List[Dict[str, Any]] = []

    with SessionLocal() as session:
        hitters = _collect_sample_hitters(session, start, end, max_hitters=max_hitters)
        _log(f"Collected sampled hitters: {len(hitters)}")

        for season in seasons:
            season_start, season_end = _season_window(season)
            _log(f"Processing season={season} window={season_start} to {season_end}")

            for index, hitter in enumerate(hitters, start=1):
                batter_id = int(hitter["player_id"])
                existing_rows = _existing_event_count(session, batter_id, season_start, season_end)

                base_row = {
                    **hitter,
                    "season": season,
                    "start_date": season_start,
                    "end_date": season_end,
                    "existing_rows_before": existing_rows,
                    "dry_run": dry_run,
                    "apply": apply,
                    "should_write": should_write,
                }

                if dry_run and not fetch_in_dry_run:
                    rows.append(
                        {
                            **base_row,
                            "action": "dry_run_no_fetch",
                            "fetched_rows": None,
                            "inserted_rows": 0,
                            "updated_rows": 0,
                        }
                    )
                    continue

                if should_write:
                    result = _insert_or_update_batter_statcast(
                        session=session,
                        batter=hitter,
                        start_date=season_start,
                        end_date=season_end,
                    )
                    rows.append(
                        {
                            **base_row,
                            **result,
                            "action": "applied",
                            "existing_rows_after": _existing_event_count(
                                session, batter_id, season_start, season_end
                            ),
                        }
                    )
                else:
                    result = _dry_run_fetch_estimate(hitter, season_start, season_end)
                    rows.append(
                        {
                            **base_row,
                            **result,
                            "action": "dry_run_fetch_only",
                            "inserted_rows": 0,
                            "updated_rows": 0,
                        }
                    )

                if index % 25 == 0:
                    _log(f"season={season} progress={index}/{len(hitters)}")

    summary: Dict[str, Any] = {
        "backtest_start": start,
        "backtest_end": end,
        "database_url": database_url,
        "seasons": seasons,
        "dry_run": dry_run,
        "apply": apply,
        "should_write": should_write,
        "fetch_in_dry_run": fetch_in_dry_run,
        "rows": len(rows),
        "targets": len({row["player_id"] for row in rows}) if rows else 0,
        "existing_rows_before": sum(int(row.get("existing_rows_before") or 0) for row in rows),
        "fetched_rows": sum(int(row.get("fetched_rows") or 0) for row in rows),
        "inserted_rows": sum(int(row.get("inserted_rows") or 0) for row in rows),
        "updated_rows": sum(int(row.get("updated_rows") or 0) for row in rows),
        "errors": sum(1 for row in rows if row.get("error")),
        "actions": {},
    }

    action_counts: Dict[str, int] = {}
    for row in rows:
        action = row.get("action") or "unknown"
        action_counts[action] = action_counts.get(action, 0) + 1
    summary["actions"] = action_counts

    output_dir = Path("tmp")
    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"historical_hitter_statcast_sample_backfill_{start}_to_{end}"
    output_path = output_dir / f"{prefix}.json"

    output_path.write_text(
        json.dumps(
            {
                "summary": summary,
                "rows": rows,
            },
            indent=2,
            sort_keys=True,
            default=str,
        )
    )

    _log("=== SUMMARY ===")
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    _log(f"Wrote {output_path}")

    if not should_write:
        _log("No database writes were performed. Set APPLY=true DRY_RUN=false to apply.")
    else:
        _log("Database writes were applied.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
