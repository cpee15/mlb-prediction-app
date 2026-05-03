#!/usr/bin/env python3
"""Targeted runner for rebuilding hitter-centered hittingMatchups metrics.

This script is intentionally additive and safe:
- it does not start the API server
- it does not modify app.py or frontend code
- it does not fetch Statcast from pybaseball or external Statcast endpoints
- it reads existing raw statcast_events rows from DATABASE_URL
- it reads today's/tomorrow's games, or one targeted game, from MLB Stats API
- it finds probable pitchers and likely hitters from official lineups first
- it falls back to each team's previous completed-game lineup before active roster
- it builds hitter-vs-pitch-type summaries from existing raw statcast_events rows
- it upserts rows into batter_pitch_type_matchups for Batter vs Arsenal cards
- it writes a JSON artifact for inspection

Targeted recovery mode:
    HITTING_MATCHUPS_TARGET_DATE=YYYY-MM-DD
    HITTING_MATCHUPS_TARGET_GAME_PK=824929
    HITTING_MATCHUPS_MAX_BATTERS=18
    python scripts/run_hitting_matchups_refresh.py

Important:
    DATABASE_URL must point at the same Postgres database used by the backend API.
    If targeted mode is used with sqlite:///mlb.db, this script fails fast instead
    of writing rows into the wrong container-local SQLite file.
"""

from __future__ import annotations

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

from mlb_app.database import BatterPitchTypeMatchup, StatcastEvent, create_tables, get_engine, get_session
from mlb_app.db_utils import get_pitch_arsenal_with_fallback
from mlb_app.hitting_matchups import build_batter_pitch_type_summary


MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
DAYS_BACK = int(os.getenv("HITTING_MATCHUPS_DAYS_BACK", "365"))
MAX_BATTERS = int(os.getenv("HITTING_MATCHUPS_MAX_BATTERS", "240"))
OUTPUT_PATH = os.getenv("HITTING_MATCHUPS_OUTPUT_PATH", "hitting_matchups_refresh.json")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("HITTING_MATCHUPS_TIMEOUT_SECONDS", "30"))
TARGET_DATE = os.getenv("HITTING_MATCHUPS_TARGET_DATE", "").strip()
TARGET_GAME_PK = os.getenv("HITTING_MATCHUPS_TARGET_GAME_PK", "").strip()
TARGETED_MODE = bool(TARGET_DATE or TARGET_GAME_PK)


def _log(message: str) -> None:
    timestamp = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    print(f"[{timestamp}] {message}", flush=True)


def _database_label() -> str:
    if DATABASE_URL.startswith("postgresql"):
        return "postgresql"
    if DATABASE_URL.startswith("sqlite"):
        return DATABASE_URL
    return DATABASE_URL.split(":", 1)[0] if ":" in DATABASE_URL else "unknown"


def _validate_runtime() -> None:
    _log(
        "Starting hittingMatchups refresh: "
        f"targeted_mode={int(TARGETED_MODE)}, "
        f"target_date={TARGET_DATE or 'auto'}, "
        f"target_game_pk={TARGET_GAME_PK or 'all'}, "
        f"max_batters={MAX_BATTERS}, "
        f"days_back={DAYS_BACK}, "
        f"database={_database_label()}"
    )

    if TARGETED_MODE and DATABASE_URL.startswith("sqlite"):
        raise RuntimeError(
            "Targeted Batter vs Arsenal refresh was started with a SQLite DATABASE_URL. "
            "Set the cron/job service DATABASE_URL to the same Postgres DATABASE_URL used by the backend API, "
            "then rerun this command. This guard prevents writing aggregates to the wrong container-local mlb.db."
        )

    if TARGET_GAME_PK and not TARGET_DATE:
        raise RuntimeError(
            "HITTING_MATCHUPS_TARGET_GAME_PK requires HITTING_MATCHUPS_TARGET_DATE. "
            "Set both values for a one-game targeted refresh."
        )


def _request_json(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def _target_dates() -> List[str]:
    if TARGET_DATE:
        return [TARGET_DATE]
    today = dt.date.today()
    return [today.isoformat(), (today + dt.timedelta(days=1)).isoformat()]


def _parse_iso_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


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

    if TARGET_GAME_PK:
        target_game_pk = int(TARGET_GAME_PK)
        games = [game for game in games if int(game.get("gamePk") or 0) == target_game_pk]

    return games


def _game_date_candidates(game_date_iso: str, target_date: str) -> List[str]:
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
    if target_date and target_date not in candidates:
        candidates.append(target_date)
    today = dt.date.today().isoformat()
    if today not in candidates:
        candidates.append(today)
    return candidates


def _fetch_previous_completed_game_lineup(team_id: int, game_date_iso: str, target_date: str) -> List[Dict[str, Any]]:
    """Use the previous completed-game lineup as the projected lineup fallback."""
    for candidate_date in _game_date_candidates(game_date_iso, target_date):
        try:
            end_date = dt.date.fromisoformat(candidate_date)
            start_date = (end_date - dt.timedelta(days=10)).isoformat()
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
            _log(f"Previous completed-game lineup fetch failed team={team_id} date={candidate_date}: {exc}")
    return []


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
                "source": "active_roster_limited",
            }
        )
    return hitters[:9]


def _lineup_hitters_from_game(game: Dict[str, Any], side: str, date_str: str, season: int) -> List[Dict[str, Any]]:
    teams = game.get("teams") or {}
    team = ((teams.get(side) or {}).get("team") or {})
    team_id = team.get("id")
    lineups = game.get("lineups") or {}
    lineup_key = "homePlayers" if side == "home" else "awayPlayers"
    players = (lineups.get(lineup_key) or [])[:9]
    source = "official_lineup"

    if not players and team_id:
        players = _fetch_previous_completed_game_lineup(int(team_id), game.get("gameDate") or "", date_str)
        source = "projected_previous_completed_game"

    if not players and team_id:
        return _fetch_active_roster_hitters(int(team_id), season)

    hitters: List[Dict[str, Any]] = []
    for player in players[:9]:
        player_id = player.get("id")
        if not player_id:
            continue
        hitters.append(
            {
                "id": int(player_id),
                "name": player.get("fullName"),
                "team_id": team_id,
                "source": source,
            }
        )
    return hitters


def _collect_targets() -> List[Dict[str, Any]]:
    targets: List[Dict[str, Any]] = []
    seen_pairs: Set[Tuple[int, int, str]] = set()

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

            away_hitters = _lineup_hitters_from_game(game, "away", date_str, season)
            home_hitters = _lineup_hitters_from_game(game, "home", date_str, season)

            _log(
                f"Targets game_pk={game_pk} date={date_str}: "
                f"away_team={away_team_id} away_hitters={len(away_hitters)} home_pitcher={home_pitcher_id}; "
                f"home_team={home_team_id} home_hitters={len(home_hitters)} away_pitcher={away_pitcher_id}"
            )

            for hitter in away_hitters:
                if not home_pitcher_id:
                    continue
                key = (int(hitter["id"]), int(home_pitcher_id), date_str)
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
                key = (int(hitter["id"]), int(away_pitcher_id), date_str)
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

    if len(targets) > MAX_BATTERS:
        _log(f"Capping targets from {len(targets)} to MAX_BATTERS={MAX_BATTERS}")
    return targets[:MAX_BATTERS]


def _pitch_types_from_raw_statcast(session, pitcher_id: int, days_back: int) -> List[str]:
    start_date = dt.date.today() - dt.timedelta(days=days_back)
    rows = (
        session.query(StatcastEvent.pitch_type, func.count(StatcastEvent.id).label("pitch_count"))
        .filter(
            StatcastEvent.pitcher_id == pitcher_id,
            StatcastEvent.pitch_type.isnot(None),
            StatcastEvent.game_date >= start_date,
        )
        .group_by(StatcastEvent.pitch_type)
        .order_by(func.count(StatcastEvent.id).desc())
        .all()
    )
    return [row.pitch_type for row in rows if row.pitch_type]


def _pitch_types_for_pitcher(session, pitcher_id: int, season: int) -> List[str]:
    arsenal, _ = get_pitch_arsenal_with_fallback(session, pitcher_id, season)
    pitch_types: List[str] = []
    for row in arsenal or []:
        pitch_type = getattr(row, "pitch_type", None)
        if pitch_type and pitch_type not in pitch_types:
            pitch_types.append(pitch_type)

    if pitch_types:
        return pitch_types

    raw_pitch_types = _pitch_types_from_raw_statcast(session, pitcher_id, DAYS_BACK)
    if raw_pitch_types:
        _log(f"Using raw statcast_events pitch types for pitcher={pitcher_id}: {raw_pitch_types}")
    return raw_pitch_types


def _set_if_present(record: BatterPitchTypeMatchup, field: str, source: Dict[str, Any]) -> None:
    if field in source:
        setattr(record, field, source.get(field))


def _upsert_matchup_row(session, target: Dict[str, Any], summary: Dict[str, Any]) -> BatterPitchTypeMatchup:
    target_date = _parse_iso_date(target.get("date"))
    pitch_type = summary["pitch_type"]

    record = (
        session.query(BatterPitchTypeMatchup)
        .filter(
            BatterPitchTypeMatchup.batter_id == target["batter_id"],
            BatterPitchTypeMatchup.opposing_pitcher_id == target["opposing_pitcher_id"],
            BatterPitchTypeMatchup.pitch_type == pitch_type,
            BatterPitchTypeMatchup.target_date == target_date,
        )
        .first()
    )

    if record is None:
        record = BatterPitchTypeMatchup(
            batter_id=target["batter_id"],
            opposing_pitcher_id=target["opposing_pitcher_id"],
            pitch_type=pitch_type,
            target_date=target_date,
        )
        session.add(record)

    record.batter_name = target.get("batter_name")
    record.batter_team_id = target.get("batter_team_id")
    record.game_pk = target.get("game_pk")
    record.source = target.get("source")
    record.days_back = DAYS_BACK
    record.date_start = _parse_iso_date(summary.get("date_start"))
    record.date_end = _parse_iso_date(summary.get("date_end"))
    record.refreshed_at = dt.datetime.utcnow()

    fields = [
        "raw_rows",
        "deduped_rows",
        "duplicate_rows_removed",
        "pitches_seen",
        "swings",
        "whiffs",
        "strikeouts",
        "putaway_swings",
        "two_strike_pitches",
        "pa",
        "pa_ended",
        "ab",
        "hits",
        "batting_avg",
        "xwoba",
        "xba",
        "avg_ev",
        "avg_exit_velocity",
        "avg_la",
        "avg_launch_angle",
        "batted_ball_count",
        "hard_hit_count",
        "whiff_pct",
        "k_pct",
        "putaway_pct",
        "hardhit_pct",
        "hard_hit_pct",
    ]

    for field in fields:
        _set_if_present(record, field, summary)

    return record


def _stored_row_needs_gap_fill(session, target: Dict[str, Any], pitch_type: str) -> bool:
    target_date = _parse_iso_date(target.get("date"))

    record = (
        session.query(BatterPitchTypeMatchup)
        .filter(
            BatterPitchTypeMatchup.batter_id == target["batter_id"],
            BatterPitchTypeMatchup.opposing_pitcher_id == target["opposing_pitcher_id"],
            BatterPitchTypeMatchup.pitch_type == pitch_type,
            BatterPitchTypeMatchup.target_date == target_date,
            BatterPitchTypeMatchup.days_back == DAYS_BACK,
        )
        .first()
    )

    if record is None:
        return True

    if not record.refreshed_at:
        return True

    if record.refreshed_at.date() < dt.datetime.utcnow().date():
        return True

    if int(record.raw_rows or 0) <= 0 and int(record.pitches_seen or 0) <= 0:
        return True

    return False


def _gap_fill_missing_stored_365_rows(session, targets: List[Dict[str, Any]], output: Dict[str, Any]) -> None:
    checked = 0
    gapfilled = 0
    still_missing = 0

    for target in targets:
        season = int(str(target["date"])[:4])
        pitch_types = _pitch_types_for_pitcher(session, target["opposing_pitcher_id"], season)

        if not pitch_types:
            continue

        for pitch_type in pitch_types:
            checked += 1

            if not _stored_row_needs_gap_fill(session, target, pitch_type):
                continue

            summary = build_batter_pitch_type_summary(
                session=session,
                batter_id=target["batter_id"],
                pitch_type=pitch_type,
                days_back=DAYS_BACK,
            )

            record = _upsert_matchup_row(session, target, summary)
            gapfilled += 1

            if int(summary.get("raw_rows") or 0) <= 0 and int(summary.get("pitches_seen") or 0) <= 0:
                still_missing += 1

            output["rows"].append({
                **target,
                **summary,
                "aggregate_id": getattr(record, "id", None),
                "gap_fill": True,
            })

    output["gap_fill_checked_rows"] = checked
    output["gap_fill_upserted_rows"] = gapfilled
    output["gap_fill_still_missing_rows"] = still_missing

    _log(
        f"Stored 365 gap-fill complete: "
        f"checked={checked}; "
        f"gapfilled={gapfilled}; "
        f"still_missing={still_missing}"
    )


def run() -> Dict[str, Any]:
    _validate_runtime()
    engine = get_engine(DATABASE_URL)
    create_tables(engine)
    Session = get_session(engine)

    targets = _collect_targets()
    _log(f"Collected {len(targets)} batter/pitcher targets, max={MAX_BATTERS}")

    output: Dict[str, Any] = {
        "generated_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "database": _database_label(),
        "targeted_mode": TARGETED_MODE,
        "target_date": TARGET_DATE or None,
        "target_game_pk": TARGET_GAME_PK or None,
        "days_back": DAYS_BACK,
        "max_batters": MAX_BATTERS,
        "target_count": len(targets),
        "rows": [],
        "upserted_rows": 0,
        "skipped_no_arsenal": 0,
        "rows_with_raw_statcast": 0,
        "rows_without_raw_statcast": 0,
        "gap_fill_checked_rows": 0,
        "gap_fill_upserted_rows": 0,
        "gap_fill_still_missing_rows": 0,
    }

    with Session() as session:
        for target in targets:
            season = int(str(target["date"])[:4])
            pitch_types = _pitch_types_for_pitcher(session, target["opposing_pitcher_id"], season)
            if not pitch_types:
                output["skipped_no_arsenal"] += 1
                _log(
                    f"No arsenal or raw pitcher pitch types found pitcher={target['opposing_pitcher_id']} "
                    f"batter={target['batter_id']} game_pk={target.get('game_pk')}"
                )
                continue

            for pitch_type in pitch_types:
                summary = build_batter_pitch_type_summary(
                    session=session,
                    batter_id=target["batter_id"],
                    pitch_type=pitch_type,
                    days_back=DAYS_BACK,
                )
                record = _upsert_matchup_row(session, target, summary)
                output["upserted_rows"] += 1
                if int(summary.get("raw_rows") or 0) > 0:
                    output["rows_with_raw_statcast"] += 1
                else:
                    output["rows_without_raw_statcast"] += 1
                output["rows"].append({**target, **summary, "aggregate_id": getattr(record, "id", None)})

        _gap_fill_missing_stored_365_rows(session, targets, output)

        session.commit()

    output_path = Path(OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True) if output_path.parent != Path(".") else None
    output_path.write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")
    _log(
        f"Upserted {output['upserted_rows']} hittingMatchups rows; "
        f"rows_with_raw_statcast={output['rows_with_raw_statcast']}; "
        f"rows_without_raw_statcast={output['rows_without_raw_statcast']}; "
        f"gap_fill_checked_rows={output['gap_fill_checked_rows']}; "
        f"gap_fill_upserted_rows={output['gap_fill_upserted_rows']}; "
        f"gap_fill_still_missing_rows={output['gap_fill_still_missing_rows']}; "
        f"skipped_no_arsenal={output['skipped_no_arsenal']}; "
        f"wrote JSON artifact to {output_path}"
    )

    if TARGETED_MODE and output["upserted_rows"] == 0:
        raise RuntimeError(
            "Targeted hittingMatchups refresh completed with zero upserted rows. "
            "Check target date, game_pk, probable pitchers, and pitcher pitch types."
        )

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
