from __future__ import annotations

import csv
import datetime as dt
import json
import os
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, Iterable, List, Optional, Set

from sqlalchemy import func

from mlb_app.database import PlayerSplit, StatcastEvent, create_tables, get_engine, get_session
from mlb_app.lineup_profile import fetch_boxscore_lineup
from mlb_app.matchup_generator import generate_matchups_for_date


SEASONS = [2023, 2024, 2025, 2026]
SPLITS = ["vsL", "vsR"]


def _date_range(start: str, end: str) -> Iterable[str]:
    current = dt.date.fromisoformat(start)
    stop = dt.date.fromisoformat(end)
    while current <= stop:
        yield current.isoformat()
        current += dt.timedelta(days=1)


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or isinstance(value, bool):
            return None
        return int(value)
    except Exception:
        return None


def _distribution(values: List[Any]) -> Dict[str, Optional[float]]:
    clean = sorted(float(v) for v in values if v is not None)
    if not clean:
        return {"min": None, "median": None, "avg": None, "max": None}
    return {
        "min": round(clean[0], 4),
        "median": round(float(median(clean)), 4),
        "avg": round(float(mean(clean)), 4),
        "max": round(clean[-1], 4),
    }


def _season_bounds(season: int, audit_end: dt.date) -> tuple[dt.date, dt.date]:
    start = dt.date(season, 3, 1)
    end = dt.date(season, 12, 31)
    if season == audit_end.year:
        end = audit_end
    return start, end


def _collect_sample_hitters(session, start: str, end: str) -> List[Dict[str, Any]]:
    hitters_by_id: Dict[int, Dict[str, Any]] = {}

    for date_str in _date_range(start, end):
        matchups = generate_matchups_for_date(session, date_str)

        for matchup in matchups:
            game_pk = _safe_int(matchup.get("game_pk"))
            if not game_pk:
                continue

            try:
                lineups = fetch_boxscore_lineup(game_pk)
            except Exception:
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
                            "sample_teams": {str(team_id): team_name},
                        }
                    else:
                        existing["last_seen_date"] = date_str
                        existing["sample_lineup_appearances"] += 1
                        existing["sample_teams"][str(team_id)] = team_name

    rows = []
    for hitter in hitters_by_id.values():
        teams = hitter.pop("sample_teams", {})
        hitter["sample_team_ids"] = ",".join(sorted(teams.keys()))
        hitter["sample_team_names"] = ",".join(sorted(str(v) for v in teams.values() if v))
        rows.append(hitter)

    return sorted(rows, key=lambda row: (row.get("player_name") or "", row["player_id"]))


def _statcast_pa_count(session, player_id: int, start_date: dt.date, end_date: dt.date, split: Optional[str]) -> int:
    query = (
        session.query(StatcastEvent.game_pk, StatcastEvent.at_bat_number)
        .filter(
            StatcastEvent.batter_id == player_id,
            StatcastEvent.game_date >= start_date,
            StatcastEvent.game_date <= end_date,
            StatcastEvent.at_bat_number.isnot(None),
        )
    )

    if split == "vsR":
        query = query.filter(StatcastEvent.p_throws == "R")
    elif split == "vsL":
        query = query.filter(StatcastEvent.p_throws == "L")

    return len({(row[0], row[1]) for row in query.all()})


def _statcast_event_count(session, player_id: int, start_date: dt.date, end_date: dt.date) -> int:
    return int(
        session.query(func.count(StatcastEvent.id))
        .filter(
            StatcastEvent.batter_id == player_id,
            StatcastEvent.game_date >= start_date,
            StatcastEvent.game_date <= end_date,
        )
        .scalar()
        or 0
    )


def _player_split_pa(session, player_id: int, season: int, split: str) -> int:
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


def _audit_hitter(session, hitter: Dict[str, Any], audit_end: dt.date) -> Dict[str, Any]:
    player_id = int(hitter["player_id"])
    row = dict(hitter)

    for season in SEASONS:
        start_date, end_date = _season_bounds(season, audit_end)

        event_count = _statcast_event_count(session, player_id, start_date, end_date)
        row[f"{season}_statcast_event_rows"] = event_count

        total_statcast_pa = 0
        total_player_split_pa = 0

        for split in SPLITS:
            statcast_pa = _statcast_pa_count(session, player_id, start_date, end_date, split)
            player_split_pa = _player_split_pa(session, player_id, season, split)

            row[f"{season}_{split}_statcast_pa"] = statcast_pa
            row[f"{season}_{split}_player_split_pa"] = player_split_pa
            row[f"{season}_{split}_has_statcast"] = statcast_pa > 0
            row[f"{season}_{split}_has_player_split"] = player_split_pa > 0

            total_statcast_pa += statcast_pa
            total_player_split_pa += player_split_pa

        row[f"{season}_statcast_pa"] = total_statcast_pa
        row[f"{season}_player_split_pa"] = total_player_split_pa
        row[f"{season}_has_statcast"] = total_statcast_pa > 0
        row[f"{season}_has_player_split"] = total_player_split_pa > 0

        if total_statcast_pa > 0 and total_player_split_pa == 0:
            gap = "aggregate_missing"
        elif total_statcast_pa == 0 and total_player_split_pa == 0:
            gap = "source_missing"
        elif total_statcast_pa == 0 and total_player_split_pa > 0:
            gap = "split_exists_without_source_window"
        else:
            gap = "available"

        row[f"{season}_gap_type"] = gap

    return row


def _summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "hitters": len(rows),
        "seasons": {},
        "recommendation": None,
    }

    for season in SEASONS:
        season_rows = rows
        summary["seasons"][str(season)] = {
            "hitters_with_statcast": sum(1 for row in season_rows if row.get(f"{season}_has_statcast")),
            "hitters_with_player_split": sum(1 for row in season_rows if row.get(f"{season}_has_player_split")),
            "statcast_pa_distribution": _distribution([row.get(f"{season}_statcast_pa") for row in season_rows]),
            "player_split_pa_distribution": _distribution([row.get(f"{season}_player_split_pa") for row in season_rows]),
            "gap_types": {},
        }

        gap_counts: Dict[str, int] = {}
        for row in season_rows:
            gap = row.get(f"{season}_gap_type") or "unknown"
            gap_counts[gap] = gap_counts.get(gap, 0) + 1
        summary["seasons"][str(season)]["gap_types"] = gap_counts

    historical_statcast = sum(
        summary["seasons"][str(season)]["hitters_with_statcast"]
        for season in [2023, 2024, 2025]
    )
    historical_splits = sum(
        summary["seasons"][str(season)]["hitters_with_player_split"]
        for season in [2023, 2024, 2025]
    )

    if historical_statcast > 0 and historical_splits == 0:
        summary["recommendation"] = (
            "Historical Statcast exists but PlayerSplit aggregates are missing. "
            "Next step: build a safe dry-run historical PlayerSplit backfill."
        )
    elif historical_statcast == 0:
        summary["recommendation"] = (
            "Historical Statcast is missing for the sampled hitters. "
            "Next step: load historical Statcast first, then generate PlayerSplit aggregates."
        )
    else:
        summary["recommendation"] = (
            "Some historical splits are available. Next step: inspect coverage by season/split "
            "and rerun the true-talent audit using historical components."
        )

    return summary


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return

    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    start = os.getenv("BACKTEST_START", "2026-04-20")
    end = os.getenv("BACKTEST_END", "2026-05-03")
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")

    audit_end = dt.date.fromisoformat(end)

    engine = get_engine(database_url)
    create_tables(engine)
    SessionLocal = get_session(engine)

    with SessionLocal() as session:
        hitters = _collect_sample_hitters(session, start, end)
        rows = [_audit_hitter(session, hitter, audit_end) for hitter in hitters]

    summary = _summarize(rows)

    output_dir = Path("tmp")
    output_dir.mkdir(parents=True, exist_ok=True)

    prefix = f"historical_hitter_data_availability_{start}_to_{end}"
    json_path = output_dir / f"{prefix}.json"
    csv_path = output_dir / f"{prefix}.csv"

    payload = {
        "start": start,
        "end": end,
        "database_url": database_url,
        "summary": summary,
        "rows": rows,
    }

    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str))
    _write_csv(csv_path, rows)

    print(json.dumps(summary, indent=2, sort_keys=True))
    print(f"Wrote {json_path}")
    print(f"Wrote {csv_path}")


if __name__ == "__main__":
    main()
