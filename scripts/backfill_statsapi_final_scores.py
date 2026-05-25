from __future__ import annotations

import csv
import datetime as dt
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional


BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
BACKTEST_END = os.getenv("BACKTEST_END", "2026-05-03")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mlb.db")

FETCH = os.getenv("FETCH", "false").lower() == "true"
APPLY = os.getenv("APPLY", "false").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

STATSAPI_BASE = "https://statsapi.mlb.com/api/v1/schedule"


def sqlite_path_from_url(database_url: str) -> str:
    if not database_url.startswith("sqlite:///"):
        raise ValueError(
            "This audit script currently supports sqlite:/// DATABASE_URL only."
        )
    return database_url.replace("sqlite:///", "")


def daterange(start: str, end: str) -> List[str]:
    current = dt.date.fromisoformat(start)
    stop = dt.date.fromisoformat(end)

    dates = []
    while current <= stop:
        dates.append(current.isoformat())
        current += dt.timedelta(days=1)
    return dates


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return

    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return [row[1] for row in rows]
    except Exception:
        return []


def collect_local_games(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    local: Dict[int, Dict[str, Any]] = {}

    # Primary source: statcast_events has game_pk coverage for the backtest window.
    statcast_cols = table_columns(conn, "statcast_events")
    if {"game_pk", "game_date"}.issubset(set(statcast_cols)):
        rows = conn.execute(
            """
            SELECT
                game_pk,
                MIN(game_date) AS game_date,
                MAX(home_team) AS home_team,
                MAX(away_team) AS away_team,
                COUNT(*) AS statcast_rows
            FROM statcast_events
            WHERE game_date >= ?
              AND game_date <= ?
              AND game_pk IS NOT NULL
            GROUP BY game_pk
            ORDER BY game_date, game_pk
            """,
            (BACKTEST_START, BACKTEST_END),
        ).fetchall()

        for row in rows:
            game_pk = int(row["game_pk"])
            local[game_pk] = {
                "game_pk": game_pk,
                "game_date": row["game_date"],
                "home_team": row["home_team"],
                "away_team": row["away_team"],
                "source": "statcast_events",
                "statcast_rows": row["statcast_rows"],
            }

    # Secondary source: matchups if it ever has game_pk in a future schema.
    matchup_cols = table_columns(conn, "matchups")
    if {"game_pk"}.issubset(set(matchup_cols)):
        select_cols = ["game_pk"]
        for col in [
            "game_date",
            "home_team_id",
            "away_team_id",
            "home_team_name",
            "away_team_name",
            "status",
        ]:
            if col in matchup_cols:
                select_cols.append(col)

        rows = conn.execute(
            f"""
            SELECT {", ".join(select_cols)}
            FROM matchups
            """
        ).fetchall()

        for row in rows:
            game_pk = int(row["game_pk"])
            existing = local.get(game_pk, {"game_pk": game_pk})
            for key in row.keys():
                existing[key] = row[key]
            existing["source"] = (
                existing.get("source", "") + "+matchups"
                if existing.get("source")
                else "matchups"
            )
            local[game_pk] = existing

    return sorted(local.values(), key=lambda row: (row.get("game_date") or "", row["game_pk"]))


def fetch_statsapi_schedule(start: str, end: str) -> Dict[str, Any]:
    params = {
        "sportId": "1",
        "startDate": start,
        "endDate": end,
        "hydrate": "team",
    }
    url = STATSAPI_BASE + "?" + urllib.parse.urlencode(params)

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "mlb-prediction-app-audit/1.0",
            "Accept": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return {"ok": True, "url": url, "payload": payload, "error": None}
    except urllib.error.URLError as exc:
        return {"ok": False, "url": url, "payload": None, "error": str(exc)}
    except Exception as exc:
        return {"ok": False, "url": url, "payload": None, "error": str(exc)}


def parse_schedule_games(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    games: List[Dict[str, Any]] = []

    for date_block in payload.get("dates", []) or []:
        game_date = date_block.get("date")

        for game in date_block.get("games", []) or []:
            teams = game.get("teams", {}) or {}
            home = teams.get("home", {}) or {}
            away = teams.get("away", {}) or {}

            home_team = home.get("team", {}) or {}
            away_team = away.get("team", {}) or {}

            status = game.get("status", {}) or {}
            detailed_state = status.get("detailedState") or status.get("abstractGameState")

            home_score = home.get("score")
            away_score = away.get("score")

            games.append(
                {
                    "game_pk": game.get("gamePk"),
                    "game_date": game_date,
                    "official_date": game.get("officialDate"),
                    "status": status.get("abstractGameState"),
                    "detailed_state": detailed_state,
                    "coded_game_state": status.get("codedGameState"),
                    "home_team_id": home_team.get("id"),
                    "home_team_name": home_team.get("name"),
                    "away_team_id": away_team.get("id"),
                    "away_team_name": away_team.get("name"),
                    "home_score": home_score,
                    "away_score": away_score,
                    "is_final": str(detailed_state).lower() in {
                        "final",
                        "game over",
                        "completed early",
                    }
                    or str(status.get("abstractGameState")).lower() == "final",
                    "source": "statsapi_schedule",
                }
            )

    return games


def create_results_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS actual_game_results (
            game_pk INTEGER PRIMARY KEY,
            game_date TEXT,
            official_date TEXT,
            home_team_id INTEGER,
            away_team_id INTEGER,
            home_team_name TEXT,
            away_team_name TEXT,
            home_score INTEGER,
            away_score INTEGER,
            status TEXT,
            detailed_state TEXT,
            source TEXT,
            created_at TEXT
        )
        """
    )


def apply_results(conn: sqlite3.Connection, rows: List[Dict[str, Any]]) -> int:
    create_results_table(conn)

    applied = 0
    now = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"

    for row in rows:
        if not row.get("is_final"):
            continue
        if row.get("home_score") is None or row.get("away_score") is None:
            continue

        conn.execute(
            """
            INSERT INTO actual_game_results (
                game_pk,
                game_date,
                official_date,
                home_team_id,
                away_team_id,
                home_team_name,
                away_team_name,
                home_score,
                away_score,
                status,
                detailed_state,
                source,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(game_pk) DO UPDATE SET
                game_date=excluded.game_date,
                official_date=excluded.official_date,
                home_team_id=excluded.home_team_id,
                away_team_id=excluded.away_team_id,
                home_team_name=excluded.home_team_name,
                away_team_name=excluded.away_team_name,
                home_score=excluded.home_score,
                away_score=excluded.away_score,
                status=excluded.status,
                detailed_state=excluded.detailed_state,
                source=excluded.source,
                created_at=excluded.created_at
            """,
            (
                row.get("game_pk"),
                row.get("game_date"),
                row.get("official_date"),
                row.get("home_team_id"),
                row.get("away_team_id"),
                row.get("home_team_name"),
                row.get("away_team_name"),
                row.get("home_score"),
                row.get("away_score"),
                row.get("status"),
                row.get("detailed_state"),
                row.get("source"),
                now,
            ),
        )
        applied += 1

    conn.commit()
    return applied


def main() -> None:
    out_dir = Path("tmp")
    out_dir.mkdir(parents=True, exist_ok=True)

    prefix = f"statsapi_final_scores_{BACKTEST_START}_to_{BACKTEST_END}"

    json_path = out_dir / f"{prefix}.json"
    local_csv = out_dir / f"{prefix}_games.csv"
    fetched_csv = out_dir / f"{prefix}_fetch.csv"
    matched_csv = out_dir / f"{prefix}_matched.csv"
    missing_csv = out_dir / f"{prefix}_missing.csv"

    sqlite_path = sqlite_path_from_url(DATABASE_URL)
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row

    local_games = collect_local_games(conn)
    local_game_pks = {int(row["game_pk"]) for row in local_games}

    fetched_games: List[Dict[str, Any]] = []
    fetch_error: Optional[str] = None
    fetch_url: Optional[str] = None

    if FETCH:
        fetch_result = fetch_statsapi_schedule(BACKTEST_START, BACKTEST_END)
        fetch_url = fetch_result.get("url")
        if fetch_result["ok"]:
            fetched_games = parse_schedule_games(fetch_result["payload"])
        else:
            fetch_error = fetch_result["error"]

    fetched_by_pk = {
        int(row["game_pk"]): row
        for row in fetched_games
        if row.get("game_pk") is not None
    }

    matched_rows = []
    missing_rows = []

    for local in local_games:
        game_pk = int(local["game_pk"])
        fetched = fetched_by_pk.get(game_pk)

        if fetched:
            matched_rows.append(
                {
                    **{f"local_{k}": v for k, v in local.items()},
                    **{f"fetched_{k}": v for k, v in fetched.items()},
                    "game_pk": game_pk,
                    "has_final_score": bool(
                        fetched.get("is_final")
                        and fetched.get("home_score") is not None
                        and fetched.get("away_score") is not None
                    ),
                }
            )
        else:
            missing_rows.append(local)

    final_rows = [
        row
        for row in fetched_games
        if int(row.get("game_pk") or -1) in local_game_pks
        and row.get("is_final")
        and row.get("home_score") is not None
        and row.get("away_score") is not None
    ]

    applied_rows = 0
    apply_error = None

    if APPLY and not DRY_RUN:
        try:
            applied_rows = apply_results(conn, final_rows)
        except Exception as exc:
            apply_error = str(exc)

    conn.close()

    if not FETCH:
        diagnosis = "fetch_disabled"
        recommended = "Run with FETCH=true APPLY=false DRY_RUN=true to fetch final scores without DB writes."
    elif fetch_error:
        diagnosis = "statsapi_fetch_failed"
        recommended = "Inspect fetch_error and retry when network/API access is available."
    elif len(final_rows) == len(local_games) and local_games and APPLY and not DRY_RUN and not apply_error:
        diagnosis = "final_scores_backfilled_to_audit_table"
        recommended = "Run Layer 4F true calibration against actual_game_results."
    elif len(final_rows) == len(local_games) and local_games:
        diagnosis = "final_scores_backfill_ready"
        recommended = "Run with FETCH=true APPLY=true DRY_RUN=false to persist audit-only actual_game_results."
    elif final_rows:
        diagnosis = "final_scores_missing_for_local_games"
        recommended = "Inspect missing local games and date/team mismatches before applying."
    else:
        diagnosis = "final_scores_missing_for_local_games"
        recommended = "StatsAPI returned no matching final-score rows for local games."

    if apply_error:
        diagnosis = "actual_result_backfill_schema_error"
        recommended = "Inspect apply_error and actual_game_results schema."

    payload = {
        "backtest_start": BACKTEST_START,
        "backtest_end": BACKTEST_END,
        "fetch_enabled": FETCH,
        "apply_enabled": APPLY,
        "dry_run": DRY_RUN,
        "fetch_url": fetch_url,
        "fetch_error": fetch_error,
        "apply_error": apply_error,
        "local_games_expected": len(local_games),
        "fetched_games": len(fetched_games),
        "matched_games": len(matched_rows),
        "missing_games": len(missing_rows),
        "games_with_final_scores": len(final_rows),
        "games_not_final": len(
            [
                row
                for row in fetched_games
                if int(row.get("game_pk") or -1) in local_game_pks
                and not row.get("is_final")
            ]
        ),
        "applied_rows": applied_rows,
        "diagnosis": diagnosis,
        "recommended_next_step": recommended,
    }

    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    write_csv(local_csv, local_games)
    write_csv(fetched_csv, fetched_games)
    write_csv(matched_csv, matched_rows)
    write_csv(missing_csv, missing_rows)

    print(json.dumps(payload, indent=2, sort_keys=True))
    print(f"Wrote {json_path}")
    print(f"Wrote {local_csv}")
    print(f"Wrote {fetched_csv}")
    print(f"Wrote {matched_csv}")
    print(f"Wrote {missing_csv}")


if __name__ == "__main__":
    main()
