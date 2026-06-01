from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional, Set

import requests
from sqlalchemy.orm import Session

from mlb_app.database import PitchArsenal, PitcherAggregate

MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"


def _fetch_pitcher_names(player_ids: Set[int]) -> Dict[int, Dict[str, Optional[str]]]:
    """Best-effort player identity hydration for leaderboard display.

    The aggregate tables are metric stores, not identity tables. Use MLB Stats
    people hydrate to avoid ugly `Pitcher #ID` rows on the landing dashboard.
    If this external lookup fails, callers still receive stable fallback rows.
    """
    ids = sorted(pid for pid in player_ids if pid)
    if not ids:
        return {}
    out: Dict[int, Dict[str, Optional[str]]] = {}
    for start in range(0, len(ids), 75):
        chunk = ids[start:start + 75]
        try:
            resp = requests.get(
                f"{MLB_STATS_BASE}/people",
                params={"personIds": ",".join(str(pid) for pid in chunk), "hydrate": "currentTeam"},
                timeout=8,
            )
            if not resp.ok:
                continue
            for person in resp.json().get("people", []) or []:
                pid = person.get("id")
                if not pid:
                    continue
                team = person.get("currentTeam") or {}
                out[int(pid)] = {
                    "player_name": person.get("fullName") or f"Pitcher #{pid}",
                    "team": team.get("abbreviation") or team.get("name"),
                }
        except Exception:
            continue
    return out


def _display_name(pid: int, identities: Dict[int, Dict[str, Optional[str]]]) -> str:
    return (identities.get(pid) or {}).get("player_name") or f"Pitcher #{pid}"


def _team(pid: int, identities: Dict[int, Dict[str, Optional[str]]]) -> Optional[str]:
    return (identities.get(pid) or {}).get("team")


def _serialize_aggregate_row(row: PitcherAggregate, value: float, rank: int, identities: Dict[int, Dict[str, Optional[str]]]) -> Dict[str, Any]:
    return {
        "rank": rank,
        "player_id": row.pitcher_id,
        "player_name": _display_name(row.pitcher_id, identities),
        "team": _team(row.pitcher_id, identities),
        "value": value,
        "sample": row.window,
        "window": row.window,
        "end_date": row.end_date.isoformat() if row.end_date else None,
    }


def _aggregate_board(rows: List[PitcherAggregate], attr: str, *, limit: int, reverse: bool, identities: Dict[int, Dict[str, Optional[str]]]) -> List[Dict[str, Any]]:
    usable = []
    for row in rows:
        value = getattr(row, attr, None)
        if value is None:
            continue
        usable.append((row, float(value)))
    usable.sort(key=lambda item: item[1], reverse=reverse)
    return [_serialize_aggregate_row(row, value, idx + 1, identities) for idx, (row, value) in enumerate(usable[:limit])]


def _pitch_board(rows: List[PitchArsenal], attr: str, *, limit: int, reverse: bool, identities: Dict[int, Dict[str, Optional[str]]]) -> List[Dict[str, Any]]:
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
            "player_name": _display_name(row.pitcher_id, identities),
            "team": _team(row.pitcher_id, identities),
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
    identity_ids = {row.pitcher_id for row in latest} | {row.pitcher_id for row in arsenal_rows}
    identities = _fetch_pitcher_names(identity_ids)

    leaderboards = {
        "avg_velocity": _aggregate_board(latest, "avg_velocity", limit=limit, reverse=True, identities=identities),
        "avg_spin_rate": _aggregate_board(latest, "avg_spin_rate", limit=limit, reverse=True, identities=identities),
        "k_pct": _aggregate_board(latest, "k_pct", limit=limit, reverse=True, identities=identities),
        "bb_pct_lowest": _aggregate_board(latest, "bb_pct", limit=limit, reverse=False, identities=identities),
        "hard_hit_pct_lowest": _aggregate_board(latest, "hard_hit_pct", limit=limit, reverse=False, identities=identities),
        "xwoba_lowest": _aggregate_board(latest, "xwoba", limit=limit, reverse=False, identities=identities),
        "xba_lowest": _aggregate_board(latest, "xba", limit=limit, reverse=False, identities=identities),
        "extension": _aggregate_board(latest, "avg_release_extension", limit=limit, reverse=True, identities=identities),
        "release_height": _aggregate_board(latest, "avg_release_pos_z", limit=limit, reverse=True, identities=identities),
        "arsenal_whiff": _pitch_board(arsenal_rows, "whiff_pct", limit=limit, reverse=True, identities=identities),
        "arsenal_xwoba_lowest": _pitch_board(arsenal_rows, "xwoba", limit=limit, reverse=False, identities=identities),
    }

    unavailable = [key for key, rows in leaderboards.items() if not rows]
    return {
        "season": int(season),
        "limit": int(limit),
        "leaderboards": leaderboards,
        "identity_source": "mlb_stats_api_people" if identities else "fallback_player_id",
        "notes": [
            "Pitcher leaderboard rows use PitcherAggregate and PitchArsenal, with MLB Stats API identity hydration for display names.",
            "Release leaderboards use PitcherAggregate release fields. Plate-location visuals use plate_x/plate_z in the pitcher intelligence endpoint.",
        ],
        "unavailable_metrics": unavailable,
    }
