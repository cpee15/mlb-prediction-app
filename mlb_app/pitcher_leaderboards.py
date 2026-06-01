from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from mlb_app.database import PitchArsenal, PitcherAggregate


def _serialize_aggregate_row(row: PitcherAggregate, value: float, rank: int) -> Dict[str, Any]:
    return {
        "rank": rank,
        "player_id": row.pitcher_id,
        "player_name": f"Pitcher #{row.pitcher_id}",
        "team": None,
        "value": value,
        "sample": row.window,
        "window": row.window,
        "end_date": row.end_date.isoformat() if row.end_date else None,
    }


def _aggregate_board(rows: List[PitcherAggregate], attr: str, *, limit: int, reverse: bool) -> List[Dict[str, Any]]:
    usable = []
    for row in rows:
        value = getattr(row, attr, None)
        if value is None:
            continue
        usable.append((row, float(value)))
    usable.sort(key=lambda item: item[1], reverse=reverse)
    return [_serialize_aggregate_row(row, value, idx + 1) for idx, (row, value) in enumerate(usable[:limit])]


def _pitch_board(rows: List[PitchArsenal], attr: str, *, limit: int, reverse: bool) -> List[Dict[str, Any]]:
    usable = []
    for row in rows:
        value = getattr(row, attr, None)
        if value is None:
            continue
        usable.append((row, float(value)))
    usable.sort(key=lambda item: item[1], reverse=reverse)
    board = []
    for idx, (row, value) in enumerate(usable[:limit]):
        pitch_label = row.pitch_name or row.pitch_type or "Pitch"
        board.append({
            "rank": idx + 1,
            "player_id": row.pitcher_id,
            "player_name": f"Pitcher #{row.pitcher_id}",
            "team": None,
            "value": value,
            "sample": f"{pitch_label} · {row.pitch_count or 0} pitches",
            "window": str(row.season),
            "pitch_type": row.pitch_type,
            "pitch_name": row.pitch_name,
            "pitch_count": row.pitch_count,
            "end_date": None,
        })
    return board


def build_pitcher_leaderboards(session: Session, season: Optional[int] = None, limit: int = 10) -> Dict[str, Any]:
    if season is None:
        season = dt.date.today().year

    aggregate_rows = (
        session.query(PitcherAggregate)
        .filter(PitcherAggregate.end_date >= dt.date(int(season), 1, 1))
        .order_by(PitcherAggregate.end_date.desc())
        .all()
    )

    latest_by_pitcher: Dict[int, PitcherAggregate] = {}
    for row in aggregate_rows:
        latest_by_pitcher.setdefault(row.pitcher_id, row)
    latest = list(latest_by_pitcher.values())

    arsenal_rows = session.query(PitchArsenal).filter(PitchArsenal.season == int(season)).all()

    leaderboards = {
        "avg_velocity": _aggregate_board(latest, "avg_velocity", limit=limit, reverse=True),
        "avg_spin_rate": _aggregate_board(latest, "avg_spin_rate", limit=limit, reverse=True),
        "k_pct": _aggregate_board(latest, "k_pct", limit=limit, reverse=True),
        "bb_pct_lowest": _aggregate_board(latest, "bb_pct", limit=limit, reverse=False),
        "hard_hit_pct_lowest": _aggregate_board(latest, "hard_hit_pct", limit=limit, reverse=False),
        "xwoba_lowest": _aggregate_board(latest, "xwoba", limit=limit, reverse=False),
        "xba_lowest": _aggregate_board(latest, "xba", limit=limit, reverse=False),
        "extension": _aggregate_board(latest, "avg_release_extension", limit=limit, reverse=True),
        "release_height": _aggregate_board(latest, "avg_release_pos_z", limit=limit, reverse=True),
        "arsenal_whiff": _pitch_board(arsenal_rows, "whiff_pct", limit=limit, reverse=True),
        "arsenal_xwoba_lowest": _pitch_board(arsenal_rows, "xwoba", limit=limit, reverse=False),
    }

    unavailable = [key for key, rows in leaderboards.items() if not rows]
    return {
        "season": int(season),
        "limit": int(limit),
        "leaderboards": leaderboards,
        "notes": [
            "Leaderboard rows use PitcherAggregate and PitchArsenal only; this avoids expensive per-pitcher intelligence calls on the landing page.",
            "Release leaderboards use PitcherAggregate release fields. Plate-location visuals use plate_x/plate_z in the pitcher intelligence endpoint.",
        ],
        "unavailable_metrics": unavailable,
    }
