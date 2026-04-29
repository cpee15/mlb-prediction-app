#!/usr/bin/env python3
"""Targeted runner for rebuilding hitter-centered hittingMatchups metrics.

This script is intentionally additive and safe:
- it does not start the API server
- it does not modify app.py or frontend code
- it reads today's and tomorrow's games from MLB Stats API
- it finds probable pitchers and likely hitters from lineups or active rosters
- it builds hitter-vs-pitch-type summaries using mlb_app.hitting_matchups
- it writes a JSON artifact for inspection and downstream wiring

Environment controls:
    HITTING_MATCHUPS_DAYS_BACK=365
    HITTING_MATCHUPS_MAX_BATTERS=40
    HITTING_MATCHUPS_OUTPUT_PATH=/tmp/hitting_matchups_refresh.json
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mlb_app.database import create_tables, get_engine, get_session
from mlb_app.db_utils import get_pitch_arsenal_with_fallback
from mlb_app.hitting_matchups import build_batter_pitch_type_summary


MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
DAYS_BACK = int(os.getenv("HITTING_MATCHUPS_DAYS_BACK", "365"))
MAX_BATTERS = int(os.getenv("HITTING_MATCHUPS_MAX_BATTERS", "40"))
OUTPUT_PATH = os.getenv("HITTING_MATCHUPS_OUTPUT_PATH", "hitting_matchups_refresh.json")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("HITTING_MATCHUPS_TIMEOUT_SECONDS", "30"))


def _log(message: str) -> None:
    timestamp = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    print(f"[{timestamp}] {message}", flush=True)


def _request_json(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def _target_dates() -> List[str]:
    today = dt.date.today()
    return [today.isoformat(), (today + dt.timedelta(days=1)).isoformat()]


def _fetch_schedule(date_str: str) -> List[Dict[str, Any]]:
    data = _request_json(
        f"{MLB_STATS_BASE}/schedule",
        params={
            "sportId": 1,
            "date": date_str,
            "hydrate": "probablePitcher,team,lineups",
        },
    )

    games: List[Dict[str, Any]] = []
    for day in data.get("dates", []) or []:
        games.extend(day.get("games", []) or [])
    return games


def _fetch_active_roster_hitters(team_id: int, season: int) -> List[Dict[str, Any]]:
    try:
        data = _request_json(
            f"{MLB_STATS_BASE}/teams/{team_id}/roster",
            params={"rosterType": "active", "season": season},
        )
    except Exception as exc:
        _log(f"Roster fetch failed team={team_id}: {exc}")
        return []

    hitters: List[Dict[str, Any]] = []
    for row in data.get("roster", []) or []:
        person = row.get("person") or {}
        position = row.get("position") or {}
        if (position.get("type") or "").lower() == "pitcher":
            continue
        player_id = person.get("id")
        if not player_id:
            continue
        hitters.append(
            {
                "id": int(player_id),
                "name": person.get("fullName"),
                "team_id": team_id,
                "source": "active_roster",
            }
        )
    return hitters


def _lineup_hitters_from_game(game: Dict[str, Any], side: str) -> List[Dict[str, Any]]:
    teams = game.get("teams") or {}
    team = ((teams.get(side) or {}).get("team") or {})
    team_id = team.get("id")
    lineups = game.get("lineups") or {}
    lineup_key = "homePlayers" if side == "home" else "awayPlayers"
    players = lineups.get(lineup_key) or []

    hitters: List[Dict[str, Any]] = []
    for player in players:
        player_id = player.get("id")
        if not player_id:
            continue
        hitters.append(
            {
                "id": int(player_id),
                "name": player.get("fullName"),
                "team_id": team_id,
                "source": "official_lineup",
            }
        )
    return hitters


def _collect_targets() -> List[Dict[str, Any]]:
    targets: List[Dict[str, Any]] = []
    seen_pairs: Set[Tuple[int, int]] = set()

    for date_str in _target_dates():
        games = _fetch_schedule(date_str)
        season = int(date_str[:4])
        _log(f"Loaded {len(games)} games for {date_str}")

        for game in games:
            teams = game.get("teams") or {}
            game_pk = game.get("gamePk")

            home = teams.get("home") or {}
            away = teams.get("away") or {}
            home_team_id = ((home.get("team") or {}).get("id"))
            away_team_id = ((away.get("team") or {}).get("id"))
            home_pitcher_id = ((home.get("probablePitcher") or {}).get("id"))
            away_pitcher_id = ((away.get("probablePitcher") or {}).get("id"))

            away_hitters = _lineup_hitters_from_game(game, "away")
            home_hitters = _lineup_hitters_from_game(game, "home")

            if not away_hitters and away_team_id:
                away_hitters = _fetch_active_roster_hitters(int(away_team_id), season)
            if not home_hitters and home_team_id:
                home_hitters = _fetch_active_roster_hitters(int(home_team_id), season)

            for hitter in away_hitters:
                if not home_pitcher_id:
                    continue
                key = (int(hitter["id"]), int(home_pitcher_id))
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)
                targets.append(
                    {
                        "date": date_str,
                        "game_pk": game_pk,
                        "batter_id": int(hitter["id"]),
                        "batter_name": hitter.get("name"),
                        "batter_team_id": hitter.get("team_id"),
                        "opposing_pitcher_id": int(home_pitcher_id),
                        "source": hitter.get("source"),
                    }
                )

            for hitter in home_hitters:
                if not away_pitcher_id:
                    continue
                key = (int(hitter["id"]), int(away_pitcher_id))
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)
                targets.append(
                    {
                        "date": date_str,
                        "game_pk": game_pk,
                        "batter_id": int(hitter["id"]),
                        "batter_name": hitter.get("name"),
                        "batter_team_id": hitter.get("team_id"),
                        "opposing_pitcher_id": int(away_pitcher_id),
                        "source": hitter.get("source"),
                    }
                )

    return targets[:MAX_BATTERS]


def _pitch_types_for_pitcher(session, pitcher_id: int, season: int) -> List[str]:
    arsenal, _ = get_pitch_arsenal_with_fallback(session, pitcher_id, season)
    pitch_types = []
    for row in arsenal or []:
        pitch_type = getattr(row, "pitch_type", None)
        if pitch_type and pitch_type not in pitch_types:
            pitch_types.append(pitch_type)
    return pitch_types


def run() -> Dict[str, Any]:
    engine = get_engine(DATABASE_URL)
    create_tables(engine)
    Session = get_session(engine)

    targets = _collect_targets()
    _log(f"Collected {len(targets)} batter/pitcher targets, max={MAX_BATTERS}")

    output: Dict[str, Any] = {
        "generated_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "days_back": DAYS_BACK,
        "max_batters": MAX_BATTERS,
        "target_count": len(targets),
        "rows": [],
    }

    with Session() as session:
        for target in targets:
            season = int(str(target["date"])[:4])
            pitch_types = _pitch_types_for_pitcher(session, target["opposing_pitcher_id"], season)
            if not pitch_types:
                _log(
                    f"No arsenal found pitcher={target['opposing_pitcher_id']} "
                    f"batter={target['batter_id']}"
                )
                continue

            for pitch_type in pitch_types:
                summary = build_batter_pitch_type_summary(
                    session=session,
                    batter_id=target["batter_id"],
                    pitch_type=pitch_type,
                    days_back=DAYS_BACK,
                )
                output["rows"].append({**target, **summary})

    output_path = Path(OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True) if output_path.parent != Path(".") else None
    output_path.write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")
    _log(f"Wrote {len(output['rows'])} hittingMatchups rows to {output_path}")

    return output


def main() -> int:
    try:
        run()
        return 0
    except Exception as exc:
        _log(f"hittingMatchups refresh failed: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
