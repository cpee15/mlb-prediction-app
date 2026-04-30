#!/usr/bin/env python3
"""Backfill pitch-level Statcast rows for daily lineup hitters.

This script is intentionally capped and lineup-first. It fills the exact
`statcast_events` rows needed by Batter vs Arsenal cards, which aggregate by:

    batter_id + pitch_type

The standard ETL is pitcher-driven, so it can miss large chunks of hitter pitch
history. This runner pulls hitter Statcast directly for today's and tomorrow's
lineup hitters, upserts pitch rows by MLB pitch identity, and preserves the
new swing/xwOBA fields required by the UI.

Default controls:
    HITTER_STATCAST_START_DATE=2023-03-01
    HITTER_STATCAST_MAX_PLAYERS=150
    HITTER_STATCAST_OUTPUT_PATH=hitter_statcast_backfill.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mlb_app.database import StatcastEvent, create_tables, get_engine, get_session
from mlb_app.statcast_utils import fetch_statcast_batter_data


MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
DEFAULT_START_DATE = os.getenv("HITTER_STATCAST_START_DATE", "2023-03-01")
DEFAULT_MAX_PLAYERS = int(os.getenv("HITTER_STATCAST_MAX_PLAYERS", "150"))
OUTPUT_PATH = os.getenv("HITTER_STATCAST_OUTPUT_PATH", "hitter_statcast_backfill.json")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("HITTER_STATCAST_TIMEOUT_SECONDS", "30"))


IDENTITY_COLUMNS = (
    "game_pk",
    "at_bat_number",
    "pitch_number",
    "pitcher_id",
    "batter_id",
    "pitch_type",
)


def _log(message: str) -> None:
    timestamp = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    print(f"[{timestamp}] {message}", flush=True)


def _request_json(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or pd.isna(value):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or pd.isna(value):
            return None
        value_float = float(value)
        if pd.isna(value_float):
            return None
        return value_float
    except (TypeError, ValueError):
        return None


def _safe_str(value: Any, max_len: int) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    text = str(value).strip()
    return text[:max_len] if text else None


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


def _game_date_candidates(game_date_iso: str) -> List[str]:
    candidates: List[str] = []
    if game_date_iso:
        try:
            utc_dt = dt.datetime.fromisoformat(game_date_iso.replace("Z", "+00:00"))
            for offset_hours in (0, -4, -5, -6, -7, -8):
                candidate = (utc_dt + dt.timedelta(hours=offset_hours)).date().isoformat()
                if candidate not in candidates:
                    candidates.append(candidate)
        except Exception:
            pass

    today = dt.date.today().isoformat()
    if today not in candidates:
        candidates.append(today)
    return candidates


def _fetch_previous_completed_game_lineup(team_id: int, game_date_iso: str) -> List[Dict[str, Any]]:
    """Use previous completed-game lineup as safe projected lineup fallback."""
    for candidate_date in _game_date_candidates(game_date_iso):
        try:
            start_date = (dt.date.fromisoformat(candidate_date) - dt.timedelta(days=7)).isoformat()
            data = _request_json(
                f"{MLB_STATS_BASE}/schedule",
                params={
                    "startDate": start_date,
                    "endDate": candidate_date,
                    "teamId": team_id,
                    "hydrate": "lineups",
                    "sportId": 1,
                },
            )

            completed_games: List[Dict[str, Any]] = []
            for date_row in data.get("dates", []) or []:
                for game in date_row.get("games", []) or []:
                    status = (game.get("status") or {}).get("codedGameState")
                    if status == "F":
                        completed_games.append(game)

            completed_games.sort(key=lambda game: game.get("gameDate") or "", reverse=True)

            for game in completed_games:
                teams = game.get("teams") or {}
                for side in ("home", "away"):
                    team = ((teams.get(side) or {}).get("team") or {})
                    if team.get("id") != team_id:
                        continue
                    lineup_key = "homePlayers" if side == "home" else "awayPlayers"
                    players = (game.get("lineups") or {}).get(lineup_key) or []
                    if players:
                        return players[:9]
        except Exception as exc:
            _log(f"Previous lineup fetch failed team={team_id} candidate={candidate_date}: {exc}")
            continue
    return []


def _lineup_players_for_game(game: Dict[str, Any], side: str) -> Tuple[List[Dict[str, Any]], str]:
    teams = game.get("teams") or {}
    team = ((teams.get(side) or {}).get("team") or {})
    team_id = team.get("id")
    lineups = game.get("lineups") or {}
    lineup_key = "homePlayers" if side == "home" else "awayPlayers"
    players = (lineups.get(lineup_key) or [])[:9]

    if players:
        return players, "official_lineup"

    if team_id:
        previous = _fetch_previous_completed_game_lineup(int(team_id), game.get("gameDate") or "")
        if previous:
            return previous[:9], "projected_previous_completed_game"

    return [], "missing_lineup"


def collect_daily_lineup_hitters(max_players: int) -> List[Dict[str, Any]]:
    """Collect up to max_players lineup hitters from today and tomorrow.

    With 15 games, this targets 30 teams x 9 hitters = 270 possible hitters.
    The default cap of 150 covers a normal full slate without pulling every
    projected bench/roster player.
    """
    players: List[Dict[str, Any]] = []
    seen_ids: Set[int] = set()

    for date_str in _target_dates():
        games = _fetch_schedule(date_str)
        _log(f"Loaded {len(games)} games for hitter Statcast date={date_str}")

        for game in games:
            game_pk = game.get("gamePk")
            teams = game.get("teams") or {}
            for side in ("away", "home"):
                team = (((teams.get(side) or {}).get("team")) or {})
                team_id = team.get("id")
                team_name = team.get("name")
                lineup, source = _lineup_players_for_game(game, side)

                for order, player in enumerate(lineup[:9], start=1):
                    player_id = player.get("id")
                    if not player_id:
                        continue
                    player_id = int(player_id)
                    if player_id in seen_ids:
                        continue
                    seen_ids.add(player_id)
                    players.append(
                        {
                            "player_id": player_id,
                            "player_name": player.get("fullName"),
                            "team_id": team_id,
                            "team_name": team_name,
                            "game_pk": game_pk,
                            "target_date": date_str,
                            "side": side,
                            "batting_order": order,
                            "lineup_source": source,
                        }
                    )
                    if len(players) >= max_players:
                        return players

    return players


def _pitch_identity_from_values(
    game_pk: Optional[int],
    at_bat_number: Optional[int],
    pitch_number: Optional[int],
    pitcher_id: Optional[int],
    batter_id: Optional[int],
    pitch_type: Optional[str],
) -> Optional[Tuple[Any, ...]]:
    if (
        game_pk is None
        or at_bat_number is None
        or pitch_number is None
        or pitcher_id is None
        or batter_id is None
        or not pitch_type
    ):
        return None
    return (game_pk, at_bat_number, pitch_number, pitcher_id, batter_id, pitch_type)


def _pitch_identity_from_event(event: StatcastEvent) -> Optional[Tuple[Any, ...]]:
    return _pitch_identity_from_values(
        event.game_pk,
        event.at_bat_number,
        event.pitch_number,
        event.pitcher_id,
        event.batter_id,
        event.pitch_type,
    )


def _pitch_identity_from_row(row: pd.Series) -> Optional[Tuple[Any, ...]]:
    return _pitch_identity_from_values(
        _safe_int(row.get("game_pk")),
        _safe_int(row.get("at_bat_number")),
        _safe_int(row.get("pitch_number")),
        _safe_int(row.get("pitcher")),
        _safe_int(row.get("batter")),
        _safe_str(row.get("pitch_type"), 5),
    )


def _existing_event_map(session, batter_id: int, start_date: dt.date, end_date: dt.date) -> Dict[Tuple[Any, ...], StatcastEvent]:
    rows = (
        session.query(StatcastEvent)
        .filter(
            StatcastEvent.batter_id == batter_id,
            StatcastEvent.game_date >= start_date,
            StatcastEvent.game_date <= end_date,
        )
        .all()
    )
    existing: Dict[Tuple[Any, ...], StatcastEvent] = {}
    for row in rows:
        key = _pitch_identity_from_event(row)
        if key is not None and key not in existing:
            existing[key] = row
    return existing


def _assign_event_fields(event: StatcastEvent, row: pd.Series, batter_id: int) -> None:
    event.game_date = pd.to_datetime(row.get("game_date")).date() if row.get("game_date") else event.game_date
    event.game_pk = _safe_int(row.get("game_pk"))
    event.at_bat_number = _safe_int(row.get("at_bat_number"))
    event.pitch_number = _safe_int(row.get("pitch_number"))
    event.inning = _safe_int(row.get("inning"))
    event.inning_topbot = _safe_str(row.get("inning_topbot"), 10)
    event.outs_when_up = _safe_int(row.get("outs_when_up"))
    event.home_team = _safe_str(row.get("home_team"), 10)
    event.away_team = _safe_str(row.get("away_team"), 10)
    event.pitcher_id = _safe_int(row.get("pitcher")) or 0
    event.batter_id = _safe_int(row.get("batter")) or batter_id
    event.pitch_type = _safe_str(row.get("pitch_type"), 5)
    event.release_speed = _safe_float(row.get("release_speed"))
    event.release_spin_rate = _safe_float(row.get("release_spin_rate"))
    event.pfx_x = _safe_float(row.get("pfx_x"))
    event.pfx_z = _safe_float(row.get("pfx_z"))
    event.plate_x = _safe_float(row.get("plate_x"))
    event.plate_z = _safe_float(row.get("plate_z"))
    event.balls = _safe_int(row.get("balls"))
    event.strikes = _safe_int(row.get("strikes"))
    event.events = _safe_str(row.get("events"), 50)
    event.description = _safe_str(row.get("description"), 60)
    event.launch_speed = _safe_float(row.get("launch_speed"))
    event.launch_angle = _safe_float(row.get("launch_angle"))
    event.estimated_woba_using_speedangle = _safe_float(row.get("estimated_woba_using_speedangle"))
    event.estimated_ba_using_speedangle = _safe_float(row.get("estimated_ba_using_speedangle"))
    event.stand = _safe_str(row.get("stand"), 1)
    event.p_throws = _safe_str(row.get("p_throws"), 1)


def _insert_or_update_batter_statcast(session, batter: Dict[str, Any], start_date: str, end_date: str) -> Dict[str, Any]:
    batter_id = int(batter["player_id"])
    start = dt.date.fromisoformat(start_date)
    end = dt.date.fromisoformat(end_date)

    try:
        df = fetch_statcast_batter_data(batter_id, start_date, end_date)
    except Exception as exc:
        _log(f"Batter Statcast fetch failed batter={batter_id} name={batter.get('player_name')}: {exc}")
        return {"player_id": batter_id, "fetched_rows": 0, "inserted_rows": 0, "updated_rows": 0, "error": str(exc)}

    if df is None or df.empty:
        _log(f"No hitter Statcast rows batter={batter_id} name={batter.get('player_name')}")
        return {"player_id": batter_id, "fetched_rows": 0, "inserted_rows": 0, "updated_rows": 0}

    existing = _existing_event_map(session, batter_id, start, end)
    inserted = 0
    updated = 0
    skipped_no_identity = 0
    seen_in_frame: Set[Tuple[Any, ...]] = set()

    for _, row in df.iterrows():
        key = _pitch_identity_from_row(row)
        if key is None:
            skipped_no_identity += 1
            continue
        if key in seen_in_frame:
            continue
        seen_in_frame.add(key)

        event = existing.get(key)
        if event is None:
            event = StatcastEvent(
                game_date=pd.to_datetime(row.get("game_date")).date(),
                pitcher_id=_safe_int(row.get("pitcher")) or 0,
                batter_id=batter_id,
            )
            _assign_event_fields(event, row, batter_id)
            session.add(event)
            existing[key] = event
            inserted += 1
        else:
            before_description = event.description
            before_xwoba = event.estimated_woba_using_speedangle
            before_xba = event.estimated_ba_using_speedangle
            _assign_event_fields(event, row, batter_id)
            if (
                before_description != event.description
                or before_xwoba != event.estimated_woba_using_speedangle
                or before_xba != event.estimated_ba_using_speedangle
            ):
                updated += 1

    session.commit()

    _log(
        f"Backfilled hitter Statcast batter={batter_id} name={batter.get('player_name')} "
        f"fetched={len(df)} inserted={inserted} updated={updated} skipped_no_identity={skipped_no_identity}"
    )

    return {
        **batter,
        "fetched_rows": int(len(df)),
        "inserted_rows": inserted,
        "updated_rows": updated,
        "skipped_no_identity": skipped_no_identity,
    }


def run(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    max_players: Optional[int] = None,
) -> Dict[str, Any]:
    start_date = start_date or DEFAULT_START_DATE
    end_date = end_date or dt.date.today().isoformat()
    max_players = max_players or DEFAULT_MAX_PLAYERS

    engine = get_engine(DATABASE_URL)
    create_tables(engine)
    Session = get_session(engine)

    hitters = collect_daily_lineup_hitters(max_players=max_players)
    _log(
        f"Collected {len(hitters)} lineup hitters for Statcast backfill, "
        f"max_players={max_players}, start_date={start_date}, end_date={end_date}"
    )

    output: Dict[str, Any] = {
        "generated_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "start_date": start_date,
        "end_date": end_date,
        "max_players": max_players,
        "target_count": len(hitters),
        "fetched_rows": 0,
        "inserted_rows": 0,
        "updated_rows": 0,
        "players": [],
    }

    with Session() as session:
        for batter in hitters:
            result = _insert_or_update_batter_statcast(session, batter, start_date, end_date)
            output["players"].append(result)
            output["fetched_rows"] += int(result.get("fetched_rows") or 0)
            output["inserted_rows"] += int(result.get("inserted_rows") or 0)
            output["updated_rows"] += int(result.get("updated_rows") or 0)

    output_path = Path(OUTPUT_PATH)
    if output_path.parent != Path("."):
        output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")

    _log(
        f"Hitter Statcast backfill completed: targets={output['target_count']}, "
        f"fetched_rows={output['fetched_rows']}, inserted_rows={output['inserted_rows']}, "
        f"updated_rows={output['updated_rows']}; artifact={output_path}"
    )

    return output


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill hitter Statcast rows for Batter vs Arsenal cards")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=dt.date.today().isoformat())
    parser.add_argument("--max-players", type=int, default=DEFAULT_MAX_PLAYERS)
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        run(start_date=args.start_date, end_date=args.end_date, max_players=args.max_players)
        return 0
    except Exception as exc:
        _log(f"Hitter Statcast backfill failed: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
