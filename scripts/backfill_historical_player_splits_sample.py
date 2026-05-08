#!/usr/bin/env python3
"""Backfill historical PlayerSplit rows for sampled lineup hitters.

This is the second step after loading historical StatcastEvent rows.

Default behavior is dry-run only:
    DRY_RUN=true
    APPLY=false

It targets the same hitters from the historical backtest lineup sample and
builds PlayerSplit rows from local StatcastEvent data by season and pitcher-hand
split.

Typical dry run:
    SEASONS=2025 MAX_HITTERS=3 \
    BACKTEST_START=2026-04-20 BACKTEST_END=2026-05-03 \
    python scripts/backfill_historical_player_splits_sample.py

Apply:
    APPLY=true DRY_RUN=false SEASONS=2025 MAX_HITTERS=3 \
    BACKTEST_START=2026-04-20 BACKTEST_END=2026-05-03 \
    python scripts/backfill_historical_player_splits_sample.py
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mlb_app.database import PlayerSplit, StatcastEvent, create_tables, get_engine, get_session
from mlb_app.lineup_profile import fetch_boxscore_lineup
from mlb_app.matchup_generator import generate_matchups_for_date

from scripts.backfill_lineup_player_splits import (  # noqa: E402
    calculate_split_metrics,
    split_for_pitcher_hand,
    upsert_player_split,
)


DEFAULT_SEASONS = [2023, 2024, 2025]


def _log(message: str) -> None:
    timestamp = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
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


def _season_window(season: int) -> Tuple[dt.date, dt.date]:
    return dt.date(season, 3, 1), dt.date(season, 11, 30)


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


def _existing_player_split_pa(session, player_id: int, season: int, split: str) -> int:
    row = (
        session.query(PlayerSplit)
        .filter(
            PlayerSplit.player_id == player_id,
            PlayerSplit.season == season,
            PlayerSplit.split == split,
        )
        .one_or_none()
    )
    return int(row.pa or 0) if row else 0


def _load_events(session, player_id: int, start_date: dt.date, end_date: dt.date) -> List[StatcastEvent]:
    return (
        session.query(StatcastEvent)
        .filter(
            StatcastEvent.batter_id == player_id,
            StatcastEvent.game_date >= start_date,
            StatcastEvent.game_date <= end_date,
        )
        .all()
    )


def _process_hitter_season(
    session,
    hitter: Dict[str, Any],
    season: int,
    start_date: dt.date,
    end_date: dt.date,
    min_split_pa: int,
    should_write: bool,
) -> Dict[str, Any]:
    player_id = int(hitter["player_id"])
    events = _load_events(session, player_id, start_date, end_date)

    by_split: Dict[str, List[StatcastEvent]] = {"vsL": [], "vsR": []}
    ignored_unknown_hand = 0

    for event in events:
        split = split_for_pitcher_hand(event.p_throws)
        if split is None:
            ignored_unknown_hand += 1
            continue
        by_split[split].append(event)

    split_rows: List[Dict[str, Any]] = []
    created = 0
    updated = 0
    skipped = 0

    for split, split_events in by_split.items():
        existing_pa = _existing_player_split_pa(session, player_id, season, split)
        metrics = calculate_split_metrics(split_events)

        row = {
            "split": split,
            "raw_event_count": len(split_events),
            "existing_player_split_pa": existing_pa,
            "metrics": metrics,
            "action": None,
            "skipped": False,
            "skipped_reason": None,
        }

        if metrics is None:
            row["skipped"] = True
            row["skipped_reason"] = "no_terminal_pa_events"
            skipped += 1
            split_rows.append(row)
            continue

        if int(metrics.get("pa") or 0) < min_split_pa:
            row["skipped"] = True
            row["skipped_reason"] = f"insufficient_pa:{metrics.get('pa')}"
            skipped += 1
            split_rows.append(row)
            continue

        if should_write:
            action = upsert_player_split(
                session=session,
                player_id=player_id,
                season=season,
                split=split,
                metrics=metrics,
            )
        else:
            action = "would_update" if existing_pa > 0 else "would_create"

        row["action"] = action
        if action in {"created", "would_create"}:
            created += 1
        elif action in {"updated", "would_update"}:
            updated += 1

        split_rows.append(row)

    return {
        **hitter,
        "season": season,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "raw_event_count": len(events),
        "ignored_unknown_hand_events": ignored_unknown_hand,
        "splits": split_rows,
        "splits_created_or_would_create": created,
        "splits_updated_or_would_update": updated,
        "splits_skipped": skipped,
    }


def main() -> int:
    start = os.getenv("BACKTEST_START", "2026-04-20")
    end = os.getenv("BACKTEST_END", "2026-05-03")
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    seasons = _parse_seasons()

    dry_run = _parse_bool_env("DRY_RUN", True)
    apply = _parse_bool_env("APPLY", False)
    should_write = bool(apply and not dry_run)

    min_split_pa = int(os.getenv("MIN_SPLIT_PA", "25"))

    max_hitters_raw = os.getenv("MAX_HITTERS")
    max_hitters = int(max_hitters_raw) if max_hitters_raw else None

    engine = get_engine(database_url)
    create_tables(engine)
    SessionLocal = get_session(engine)

    _log("=== HISTORICAL PLAYER SPLIT SAMPLE BACKFILL ===")
    _log(f"BACKTEST_START={start}")
    _log(f"BACKTEST_END={end}")
    _log(f"SEASONS={','.join(str(s) for s in seasons)}")
    _log(f"DATABASE_URL={database_url}")
    _log(f"MIN_SPLIT_PA={min_split_pa}")
    _log(f"DRY_RUN={dry_run}")
    _log(f"APPLY={apply}")
    _log(f"should_write={should_write}")
    _log(f"MAX_HITTERS={max_hitters}")

    rows: List[Dict[str, Any]] = []

    with SessionLocal() as session:
        hitters = _collect_sample_hitters(session, start, end, max_hitters=max_hitters)
        _log(f"Collected sampled hitters: {len(hitters)}")

        for season in seasons:
            season_start, season_end = _season_window(season)
            _log(f"Processing season={season} window={season_start.isoformat()} to {season_end.isoformat()}")

            for hitter in hitters:
                rows.append(
                    _process_hitter_season(
                        session=session,
                        hitter=hitter,
                        season=season,
                        start_date=season_start,
                        end_date=season_end,
                        min_split_pa=min_split_pa,
                        should_write=should_write,
                    )
                )

        if should_write:
            session.commit()
        else:
            session.rollback()

    summary: Dict[str, Any] = {
        "backtest_start": start,
        "backtest_end": end,
        "database_url": database_url,
        "seasons": seasons,
        "dry_run": dry_run,
        "apply": apply,
        "should_write": should_write,
        "min_split_pa": min_split_pa,
        "rows": len(rows),
        "targets": len({row["player_id"] for row in rows}) if rows else 0,
        "raw_event_count": sum(int(row.get("raw_event_count") or 0) for row in rows),
        "splits_created_or_would_create": sum(int(row.get("splits_created_or_would_create") or 0) for row in rows),
        "splits_updated_or_would_update": sum(int(row.get("splits_updated_or_would_update") or 0) for row in rows),
        "splits_skipped": sum(int(row.get("splits_skipped") or 0) for row in rows),
        "split_actions": {},
        "skipped_reasons": {},
    }

    split_actions: Dict[str, int] = {}
    skipped_reasons: Dict[str, int] = {}
    for row in rows:
        for split_row in row.get("splits") or []:
            action = split_row.get("action")
            if action:
                split_actions[action] = split_actions.get(action, 0) + 1
            if split_row.get("skipped"):
                reason = split_row.get("skipped_reason") or "unknown"
                skipped_reasons[reason] = skipped_reasons.get(reason, 0) + 1

    summary["split_actions"] = split_actions
    summary["skipped_reasons"] = skipped_reasons

    output_dir = Path("tmp")
    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"historical_player_splits_sample_backfill_{start}_to_{end}"
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
