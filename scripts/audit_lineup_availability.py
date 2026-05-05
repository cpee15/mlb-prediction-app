from __future__ import annotations

import datetime as dt
import json
import os
from typing import Any, Dict, List, Optional

import requests

from mlb_app.etl import MLB_STATS_BASE, fetch_schedule


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def fetch_boxscore(game_pk: int) -> Dict[str, Any]:
    url = f"{MLB_STATS_BASE}/game/{game_pk}/boxscore"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def player_sort_key(player: Dict[str, Any]) -> tuple:
    order = player.get("batting_order")
    if order is None:
        return (999999, player.get("name") or "")
    return (order, player.get("name") or "")


def extract_lineup(boxscore: Dict[str, Any], side: str) -> Dict[str, Any]:
    team = ((boxscore.get("teams") or {}).get(side) or {})
    players = team.get("players") or {}

    hitters: List[Dict[str, Any]] = []
    players_with_batting_order = 0

    for raw_key, player_obj in players.items():
        person = player_obj.get("person") or {}
        player_id = safe_int(person.get("id"))

        batting_order_raw = player_obj.get("battingOrder")
        batting_order = safe_int(batting_order_raw)

        if batting_order is not None:
            players_with_batting_order += 1

        position = player_obj.get("position") or {}

        # Keep only players with an actual batting-order slot for the primary lineup.
        if batting_order is None:
            continue

        hitters.append({
            "raw_key": raw_key,
            "player_id": player_id,
            "name": person.get("fullName"),
            "batting_order_raw": batting_order_raw,
            "batting_order": batting_order,
            "normalized_slot": int(batting_order / 100) if batting_order and batting_order >= 100 else batting_order,
            "position_code": position.get("code"),
            "position_name": position.get("name"),
            "position_abbrev": position.get("abbreviation"),
            "status": player_obj.get("status", {}).get("description") if isinstance(player_obj.get("status"), dict) else player_obj.get("status"),
        })

    hitters = sorted(hitters, key=player_sort_key)

    return {
        "side": side,
        "team_id": safe_int((team.get("team") or {}).get("id")),
        "team_name": (team.get("team") or {}).get("name"),
        "player_object_count": len(players),
        "players_with_batting_order": players_with_batting_order,
        "batter_count": len(hitters),
        "has_batting_order": len(hitters) > 0,
        "has_full_lineup": len(hitters) >= 9,
        "lineup": hitters,
    }


def summarize_game(game: Dict[str, Any]) -> Dict[str, Any]:
    game_pk = safe_int(game.get("_game_pk"))
    away_team = (game.get("away") or {}).get("team") or {}
    home_team = (game.get("home") or {}).get("team") or {}

    base = {
        "game_pk": game_pk,
        "game_time": game.get("_game_date"),
        "status": game.get("_status"),
        "matchup": f"{away_team.get('name')} @ {home_team.get('name')}",
        "away_team_id": safe_int(away_team.get("id")),
        "home_team_id": safe_int(home_team.get("id")),
        "away_team_name": away_team.get("name"),
        "home_team_name": home_team.get("name"),
        "boxscore_error": None,
        "away": None,
        "home": None,
    }

    if game_pk is None:
        base["boxscore_error"] = "missing_game_pk"
        return base

    try:
        boxscore = fetch_boxscore(game_pk)
        base["away"] = extract_lineup(boxscore, "away")
        base["home"] = extract_lineup(boxscore, "home")
    except Exception as exc:
        base["boxscore_error"] = str(exc)

    return base


def print_game_summary(row: Dict[str, Any]) -> None:
    print("\n" + "=" * 92)
    print(f"{row.get('game_pk')} | {row.get('matchup')} | {row.get('status')}")

    if row.get("boxscore_error"):
        print(f"BOX SCORE ERROR: {row['boxscore_error']}")
        return

    for side in ["away", "home"]:
        info = row.get(side) or {}
        print(
            f"\n{side.upper()} {info.get('team_name')}: "
            f"batters={info.get('batter_count')}, "
            f"with_battingOrder={info.get('players_with_batting_order')}, "
            f"full_lineup={info.get('has_full_lineup')}"
        )

        for hitter in info.get("lineup") or []:
            print(
                f"  {hitter.get('normalized_slot')}: "
                f"{hitter.get('name')} "
                f"(id={hitter.get('player_id')}, "
                f"battingOrder={hitter.get('batting_order_raw')}, "
                f"pos={hitter.get('position_abbrev')})"
            )


def main() -> None:
    target_date = os.getenv("AUDIT_DATE") or dt.date.today().isoformat()

    print("\n=== LINEUP AVAILABILITY AUDIT ===")
    print(f"date: {target_date}")

    games = fetch_schedule(target_date)
    print(f"schedule_games: {len(games)}")

    rows = [summarize_game(game) for game in games]

    usable_games = 0
    partial_games = 0
    missing_games = 0

    for row in rows:
        print_game_summary(row)

        if row.get("boxscore_error"):
            missing_games += 1
            continue

        away = row.get("away") or {}
        home = row.get("home") or {}

        if away.get("has_full_lineup") and home.get("has_full_lineup"):
            usable_games += 1
        elif away.get("has_batting_order") or home.get("has_batting_order"):
            partial_games += 1
        else:
            missing_games += 1

    summary = {
        "date": target_date,
        "schedule_games": len(games),
        "usable_full_lineup_games": usable_games,
        "partial_lineup_games": partial_games,
        "missing_lineup_games": missing_games,
        "full_lineup_rate": round(usable_games / len(games), 4) if games else None,
    }

    print("\n" + "=" * 92)
    print("=== SUMMARY ===")
    print(json.dumps(summary, indent=2))

    os.makedirs("tmp", exist_ok=True)
    out_path = f"tmp/lineup_availability_{target_date}.json"
    with open(out_path, "w") as f:
        json.dump({"summary": summary, "games": rows}, f, indent=2, default=str)

    print(f"\nWrote full JSON audit to {out_path}")


if __name__ == "__main__":
    main()
