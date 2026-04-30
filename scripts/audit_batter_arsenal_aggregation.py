#!/usr/bin/env python3
"""Audit Batter vs Arsenal DB-backed aggregation coverage.

This script does not call external Statcast APIs and does not start the app.
It verifies the local/production database path used by Batter vs Arsenal:

    statcast_events -> batter_pitch_type_matchups -> competitive endpoint payload

It checks whether raw batter Statcast rows exist for current matchup hitters and
whether matching aggregated rows exist in batter_pitch_type_matchups.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from sqlalchemy import func

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mlb_app.database import BatterPitchTypeMatchup, StatcastEvent, get_engine, get_session
from mlb_app.db_utils import get_pitch_arsenal_with_fallback


MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("BATTER_ARSENAL_AUDIT_TIMEOUT_SECONDS", "30"))


def _request_json(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def _fetch_schedule(date_str: str) -> List[Dict[str, Any]]:
    data = _request_json(
        f"{MLB_STATS_BASE}/schedule",
        params={"sportId": 1, "date": date_str, "hydrate": "probablePitcher,team,lineups"},
    )
    games: List[Dict[str, Any]] = []
    for day in data.get("dates", []) or []:
        games.extend(day.get("games", []) or [])
    return games


def _lineup_hitters(game: Dict[str, Any], side: str) -> List[Dict[str, Any]]:
    teams = game.get("teams") or {}
    team = ((teams.get(side) or {}).get("team") or {})
    lineups = game.get("lineups") or {}
    lineup_key = "homePlayers" if side == "home" else "awayPlayers"
    players = lineups.get(lineup_key) or []
    hitters: List[Dict[str, Any]] = []
    for order, player in enumerate(players, start=1):
        player_id = player.get("id")
        if not player_id:
            continue
        hitters.append(
            {
                "batter_id": int(player_id),
                "batter_name": player.get("fullName"),
                "team_id": team.get("id"),
                "team_name": team.get("name"),
                "batting_order": order,
            }
        )
    return hitters


def _pitch_types_for_pitcher(session, pitcher_id: int, season: int) -> List[str]:
    arsenal, _ = get_pitch_arsenal_with_fallback(session, pitcher_id, season)
    pitch_types: List[str] = []
    for row in arsenal or []:
        pitch_type = getattr(row, "pitch_type", None)
        if pitch_type and pitch_type not in pitch_types:
            pitch_types.append(pitch_type)
    return pitch_types


def _raw_statcast_count(session, batter_id: int, pitch_type: str, start_date: dt.date) -> int:
    return int(
        session.query(func.count(StatcastEvent.id))
        .filter(
            StatcastEvent.batter_id == batter_id,
            StatcastEvent.pitch_type == pitch_type,
            StatcastEvent.game_date >= start_date,
        )
        .scalar()
        or 0
    )


def _aggregate_row(session, batter_id: int, pitcher_id: int, pitch_type: str, target_date: dt.date) -> Optional[BatterPitchTypeMatchup]:
    exact = (
        session.query(BatterPitchTypeMatchup)
        .filter(
            BatterPitchTypeMatchup.batter_id == batter_id,
            BatterPitchTypeMatchup.opposing_pitcher_id == pitcher_id,
            BatterPitchTypeMatchup.pitch_type == pitch_type,
            BatterPitchTypeMatchup.target_date == target_date,
        )
        .order_by(BatterPitchTypeMatchup.refreshed_at.desc().nullslast(), BatterPitchTypeMatchup.id.desc())
        .first()
    )
    if exact:
        return exact
    return (
        session.query(BatterPitchTypeMatchup)
        .filter(
            BatterPitchTypeMatchup.batter_id == batter_id,
            BatterPitchTypeMatchup.pitch_type == pitch_type,
        )
        .order_by(
            BatterPitchTypeMatchup.target_date.desc().nullslast(),
            BatterPitchTypeMatchup.refreshed_at.desc().nullslast(),
            BatterPitchTypeMatchup.id.desc(),
        )
        .first()
    )


def audit(date_str: str, days_back: int, game_pk_filter: Optional[int] = None) -> Dict[str, Any]:
    target_date = dt.date.fromisoformat(date_str)
    start_date = target_date - dt.timedelta(days=days_back)
    season = int(date_str[:4])

    engine = get_engine(DATABASE_URL)
    Session = get_session(engine)
    games = _fetch_schedule(date_str)
    if game_pk_filter is not None:
        games = [game for game in games if int(game.get("gamePk") or 0) == game_pk_filter]

    output: Dict[str, Any] = {
        "date": date_str,
        "days_back": days_back,
        "game_count": len(games),
        "checks": [],
        "summary": {
            "total_checks": 0,
            "raw_statcast_present": 0,
            "aggregate_present": 0,
            "raw_present_but_aggregate_missing": 0,
        },
    }

    with Session() as session:
        for game in games:
            teams = game.get("teams") or {}
            home = teams.get("home") or {}
            away = teams.get("away") or {}
            home_team = ((home.get("team") or {}).get("name"))
            away_team = ((away.get("team") or {}).get("name"))
            home_pitcher_id = ((home.get("probablePitcher") or {}).get("id"))
            away_pitcher_id = ((away.get("probablePitcher") or {}).get("id"))
            game_pk = game.get("gamePk")

            sides = [
                ("away", _lineup_hitters(game, "away"), home_pitcher_id, home_team),
                ("home", _lineup_hitters(game, "home"), away_pitcher_id, away_team),
            ]
            for side, hitters, opposing_pitcher_id, opposing_team_name in sides:
                if not opposing_pitcher_id:
                    continue
                pitch_types = _pitch_types_for_pitcher(session, int(opposing_pitcher_id), season)
                for hitter in hitters:
                    for pitch_type in pitch_types:
                        raw_rows = _raw_statcast_count(session, hitter["batter_id"], pitch_type, start_date)
                        aggregate = _aggregate_row(
                            session,
                            hitter["batter_id"],
                            int(opposing_pitcher_id),
                            pitch_type,
                            target_date,
                        )
                        aggregate_present = aggregate is not None
                        output["checks"].append(
                            {
                                "game_pk": game_pk,
                                "matchup": f"{away_team} @ {home_team}",
                                "side": side,
                                "batter_id": hitter["batter_id"],
                                "batter_name": hitter.get("batter_name"),
                                "opposing_pitcher_id": int(opposing_pitcher_id),
                                "opposing_team": opposing_team_name,
                                "pitch_type": pitch_type,
                                "raw_statcast_rows": raw_rows,
                                "aggregate_present": aggregate_present,
                                "aggregate_id": aggregate.id if aggregate else None,
                                "aggregate_target_date": aggregate.target_date.isoformat() if aggregate and aggregate.target_date else None,
                                "aggregate_pitches_seen": aggregate.pitches_seen if aggregate else None,
                                "aggregate_swings": aggregate.swings if aggregate else None,
                                "aggregate_whiffs": aggregate.whiffs if aggregate else None,
                                "aggregate_xwoba": aggregate.xwoba if aggregate else None,
                                "status": (
                                    "ok"
                                    if raw_rows > 0 and aggregate_present
                                    else "raw_present_aggregate_missing"
                                    if raw_rows > 0 and not aggregate_present
                                    else "no_raw_statcast_rows"
                                ),
                            }
                        )

    summary = output["summary"]
    summary["total_checks"] = len(output["checks"])
    summary["raw_statcast_present"] = sum(1 for row in output["checks"] if row["raw_statcast_rows"] > 0)
    summary["aggregate_present"] = sum(1 for row in output["checks"] if row["aggregate_present"])
    summary["raw_present_but_aggregate_missing"] = sum(
        1 for row in output["checks"] if row["raw_statcast_rows"] > 0 and not row["aggregate_present"]
    )
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit Batter vs Arsenal DB aggregation coverage")
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--days-back", type=int, default=int(os.getenv("HITTING_MATCHUPS_DAYS_BACK", "365")))
    parser.add_argument("--game-pk", type=int, default=None)
    parser.add_argument("--output", default="batter_arsenal_aggregation_audit.json")
    args = parser.parse_args()

    result = audit(args.date, args.days_back, args.game_pk)
    output_path = Path(args.output)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result["summary"], indent=2, sort_keys=True))
    print(f"Wrote audit artifact to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
