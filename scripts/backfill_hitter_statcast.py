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
    HITTER_STATCAST_MAX_PLAYERS=1000
    HITTER_STATCAST_OUTPUT_PATH=hitter_statcast_backfill.json
    HITTER_STATCAST_SKIP_COMPLETED=1
    HITTER_STATCAST_FORCE_REFRESH=0
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
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mlb_app.database import StatcastEvent, create_tables, get_engine, get_session
from mlb_app.statcast_utils import fetch_statcast_batter_data


MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
DEFAULT_START_DATE = os.getenv("HITTER_STATCAST_START_DATE", "2023-03-01")
DEFAULT_MAX_PLAYERS = int(os.getenv("HITTER_STATCAST_MAX_PLAYERS", "1000"))
OUTPUT_PATH = os.getenv("HITTER_STATCAST_OUTPUT_PATH", "hitter_statcast_backfill.json")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("HITTER_STATCAST_TIMEOUT_SECONDS", "30"))
SKIP_COMPLETED = os.getenv("HITTER_STATCAST_SKIP_COMPLETED", "1") == "1"
FORCE_REFRESH = os.getenv("HITTER_STATCAST_FORCE_REFRESH", "0") == "1"
CHECKPOINT_TABLE = "hitter_statcast_backfill_checkpoints"


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
        params={"sportId": 1, "date": date_str, "hydrate": "probablePitcher,team,lineups"},
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
                    if (game.get("status") or {}).get("codedGameState") == "F":
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
    """Collect up to max_players lineup hitters from today and tomorrow."""
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
                            "team_id": team.get("id"),
                            "team_name": team.get("name"),
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


def _ensure_checkpoint_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
                    batter_id INTEGER NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    player_name VARCHAR(120),
                    fetched_rows INTEGER DEFAULT 0,
                    inserted_rows INTEGER DEFAULT 0,
                    updated_rows INTEGER DEFAULT 0,
                    completed_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (batter_id, start_date, end_date)
                )
                """
            )
        )


def _is_completed(session, batter_id: int, start_date: str, end_date: str) -> bool:
    if FORCE_REFRESH or not SKIP_COMPLETED:
        return False
    row = session.execute(
        text(
            f"""
            SELECT batter_id
            FROM {CHECKPOINT_TABLE}
            WHERE batter_id = :batter_id
              AND start_date = :start_date
              AND end_date = :end_date
            LIMIT 1
            """
        ),
        {"batter_id": batter_id, "start_date": start_date, "end_date": end_date},
    ).first()
    return row is not None


def _mark_completed(
    session,
    batter_id: int,
    player_name: Optional[str],
    start_date: str,
    end_date: str,
    fetched_rows: int,
    inserted_rows: int,
    updated_rows: int,
) -> None:
    completed_at = dt.datetime.utcnow()
    if session.bind and session.bind.dialect.name == "postgresql":
        stmt = text(
            f"""
            INSERT INTO {CHECKPOINT_TABLE}
                (batter_id, start_date, end_date, player_name, fetched_rows, inserted_rows, updated_rows, completed_at)
            VALUES
                (:batter_id, :start_date, :end_date, :player_name, :fetched_rows, :inserted_rows, :updated_rows, :completed_at)
            ON CONFLICT (batter_id, start_date, end_date)
            DO UPDATE SET
                player_name = EXCLUDED.player_name,
                fetched_rows = EXCLUDED.fetched_rows,
                inserted_rows = EXCLUDED.inserted_rows,
                updated_rows = EXCLUDED.updated_rows,
                completed_at = EXCLUDED.completed_at
            """
        )
    else:
        stmt = text(
            f"""
            INSERT OR REPLACE INTO {CHECKPOINT_TABLE}
                (batter_id, start_date, end_date, player_name, fetched_rows, inserted_rows, updated_rows, completed_at)
            VALUES
                (:batter_id, :start_date, :end_date, :player_name, :fetched_rows, :inserted_rows, :updated_rows, :completed_at)
            """
        )
    session.execute(
        stmt,
        {
            "batter_id": batter_id,
            "start_date": start_date,
            "end_date": end_date,
            "player_name": player_name,
            "fetched_rows": fetched_rows,
            "inserted_rows": inserted_rows,
            "updated_rows": updated_rows,
            "completed_at": completed_at,
        },
    )


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

    if _is_completed(session, batter_id, start_date, end_date):
        _log(
            f"Skipping completed hitter Statcast batter={batter_id} "
            f"name={batter.get('player_name')} start_date={start_date} end_date={end_date}"
        )
        return {**batter, "fetched_rows": 0, "inserted_rows": 0, "updated_rows": 0, "skipped_completed": True}

    try:
        df = fetch_statcast_batter_data(batter_id, start_date, end_date)
    except Exception as exc:
        _log(f"Batter Statcast fetch failed batter={batter_id} name={batter.get('player_name')}: {exc}")
        return {**batter, "fetched_rows": 0, "inserted_rows": 0, "updated_rows": 0, "error": str(exc)}

    if df is None or df.empty:
        _log(f"No hitter Statcast rows batter={batter_id} name={batter.get('player_name')}")
        _mark_completed(session, batter_id, batter.get("player_name"), start_date, end_date, 0, 0, 0)
        session.commit()
        return {**batter, "fetched_rows": 0, "inserted_rows": 0, "updated_rows": 0, "skipped_completed": False}

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
            if before_description != event.description or before_xwoba != event.estimated_woba_using_speedangle or before_xba != event.estimated_ba_using_speedangle:
                updated += 1

    _mark_completed(
        session,
        batter_id=batter_id,
        player_name=batter.get("player_name"),
        start_date=start_date,
        end_date=end_date,
        fetched_rows=int(len(df)),
        inserted_rows=inserted,
        updated_rows=updated,
    )
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
        "skipped_completed": False,
    }


def run(start_date: Optional[str] = None, end_date: Optional[str] = None, max_players: Optional[int] = None) -> Dict[str, Any]:
    start_date = start_date or DEFAULT_START_DATE
    end_date = end_date or dt.date.today().isoformat()
    max_players = max_players or DEFAULT_MAX_PLAYERS

    engine = get_engine(DATABASE_URL)
    create_tables(engine)
    _ensure_checkpoint_table(engine)
    Session = get_session(engine)

    hitters = collect_daily_lineup_hitters(max_players=max_players)
    _log(
        f"Collected {len(hitters)} lineup hitters for Statcast backfill, "
        f"max_players={max_players}, start_date={start_date}, end_date={end_date}, "
        f"skip_completed={int(SKIP_COMPLETED)}, force_refresh={int(FORCE_REFRESH)}"
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
        "skipped_completed_players": 0,
        "players": [],
    }

    with Session() as session:
        for batter in hitters:
            result = _insert_or_update_batter_statcast(session, batter, start_date, end_date)
            output["players"].append(result)
            output["fetched_rows"] += int(result.get("fetched_rows") or 0)
            output["inserted_rows"] += int(result.get("inserted_rows") or 0)
            output["updated_rows"] += int(result.get("updated_rows") or 0)
            if result.get("skipped_completed"):
                output["skipped_completed_players"] += 1

    output_path = Path(OUTPUT_PATH)
    if output_path.parent != Path("."):
        output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")
    _log(
        f"Hitter Statcast backfill completed: targets={output['target_count']}, "
        f"skipped_completed_players={output['skipped_completed_players']}, "
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
