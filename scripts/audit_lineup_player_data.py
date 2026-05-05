from __future__ import annotations

import datetime as dt
import json
import os
from typing import Any, Dict, List, Optional

from sqlalchemy import func

from mlb_app.database import (
    BatterAggregate,
    PlayerSplit,
    StatcastEvent,
    create_tables,
    get_engine,
    get_session,
)
from mlb_app.etl import fetch_schedule
from mlb_app.lineup_profile import fetch_boxscore_lineup


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def pitcher_hand_to_split(hand: Optional[str]) -> str:
    return "vsL" if hand == "L" else "vsR"


def opposite_split(split: str) -> str:
    return "vsL" if split == "vsR" else "vsR"


def has_player_split(session, player_id: int, season: int, split: str) -> bool:
    return (
        session.query(PlayerSplit.id)
        .filter(
            PlayerSplit.player_id == player_id,
            PlayerSplit.season == season,
            PlayerSplit.split == split,
        )
        .first()
        is not None
    )


def get_player_split_summary(session, player_id: int, season: int, split: str) -> Optional[Dict[str, Any]]:
    row = (
        session.query(PlayerSplit)
        .filter(
            PlayerSplit.player_id == player_id,
            PlayerSplit.season == season,
            PlayerSplit.split == split,
        )
        .first()
    )
    if not row:
        return None

    return {
        "season": row.season,
        "split": row.split,
        "pa": row.pa,
        "batting_avg": row.batting_avg,
        "on_base_pct": row.on_base_pct,
        "slugging_pct": row.slugging_pct,
        "iso": row.iso,
        "k_pct": row.k_pct,
        "bb_pct": row.bb_pct,
        "home_runs": row.home_runs,
    }


def get_batter_aggregate_summary(session, player_id: int, window: str) -> Optional[Dict[str, Any]]:
    row = (
        session.query(BatterAggregate)
        .filter(
            BatterAggregate.batter_id == player_id,
            BatterAggregate.window == window,
        )
        .order_by(BatterAggregate.end_date.desc())
        .first()
    )
    if not row:
        return None

    return {
        "window": row.window,
        "end_date": row.end_date.isoformat() if row.end_date else None,
        "batting_avg": row.batting_avg,
        "k_pct": row.k_pct,
        "bb_pct": row.bb_pct,
        "avg_exit_velocity": row.avg_exit_velocity,
        "avg_launch_angle": row.avg_launch_angle,
        "hard_hit_pct": row.hard_hit_pct,
        "barrel_pct": row.barrel_pct,
    }


def get_statcast_summary(session, player_id: int) -> Dict[str, Any]:
    count = (
        session.query(func.count(StatcastEvent.id))
        .filter(StatcastEvent.batter_id == player_id)
        .scalar()
        or 0
    )
    latest = (
        session.query(func.max(StatcastEvent.game_date))
        .filter(StatcastEvent.batter_id == player_id)
        .scalar()
    )
    terminal_count = (
        session.query(func.count(StatcastEvent.id))
        .filter(
            StatcastEvent.batter_id == player_id,
            StatcastEvent.events.isnot(None),
        )
        .scalar()
        or 0
    )

    return {
        "has_statcast_event": count > 0,
        "statcast_event_count": count,
        "terminal_or_event_rows": terminal_count,
        "latest_statcast_event_date": latest.isoformat() if latest else None,
    }


def table_counts(session) -> Dict[str, Any]:
    def count_model(model) -> int:
        try:
            return session.query(func.count(model.id)).scalar() or 0
        except Exception:
            return 0

    return {
        "player_splits": count_model(PlayerSplit),
        "batter_aggregates": count_model(BatterAggregate),
        "statcast_events": count_model(StatcastEvent),
    }


def game_pitcher_split(game: Dict[str, Any], side: str) -> str:
    """
    For a team's hitters, split is based on opposing probable pitcher hand.
    """
    if side == "away":
        opposing = game.get("home", {}).get("probablePitcher", {})
    else:
        opposing = game.get("away", {}).get("probablePitcher", {})

    hand = opposing.get("pitchHand", {}).get("code")
    return pitcher_hand_to_split(hand)


def team_name(game: Dict[str, Any], side: str) -> Optional[str]:
    return game.get(side, {}).get("team", {}).get("name")


def audit_hitter(session, game: Dict[str, Any], side: str, hitter: Dict[str, Any], season: int) -> Dict[str, Any]:
    player_id = safe_int(hitter.get("batter_id"))
    selected_split = game_pitcher_split(game, side)
    opp_split = opposite_split(selected_split)

    row: Dict[str, Any] = {
        "game_pk": game.get("_game_pk"),
        "game_status": game.get("_status"),
        "side": side,
        "team": team_name(game, side),
        "player_id": player_id,
        "player_name": hitter.get("name"),
        "batting_order": hitter.get("batting_order"),
        "lineup_slot": hitter.get("lineup_slot"),
        "position": hitter.get("position"),
        "season": season,
        "selected_split": selected_split,
        "opposite_split": opp_split,
        "has_selected_player_split": False,
        "has_opposite_player_split": False,
        "has_batter_aggregate_90d": False,
        "has_batter_aggregate_current_season": False,
        "has_any_statcast_event": False,
        "selected_player_split": None,
        "opposite_player_split": None,
        "batter_aggregate_90d": None,
        "batter_aggregate_current_season": None,
        "statcast": None,
        "has_any_player_level_data": False,
    }

    if player_id is None:
        return row

    selected_summary = get_player_split_summary(session, player_id, season, selected_split)
    opposite_summary = get_player_split_summary(session, player_id, season, opp_split)
    agg_90d = get_batter_aggregate_summary(session, player_id, "90d")
    agg_season = get_batter_aggregate_summary(session, player_id, str(season))
    statcast = get_statcast_summary(session, player_id)

    row.update({
        "has_selected_player_split": selected_summary is not None,
        "has_opposite_player_split": opposite_summary is not None,
        "has_batter_aggregate_90d": agg_90d is not None,
        "has_batter_aggregate_current_season": agg_season is not None,
        "has_any_statcast_event": bool(statcast.get("has_statcast_event")),
        "selected_player_split": selected_summary,
        "opposite_player_split": opposite_summary,
        "batter_aggregate_90d": agg_90d,
        "batter_aggregate_current_season": agg_season,
        "statcast": statcast,
    })

    row["has_any_player_level_data"] = bool(
        row["has_selected_player_split"]
        or row["has_opposite_player_split"]
        or row["has_batter_aggregate_90d"]
        or row["has_batter_aggregate_current_season"]
        or row["has_any_statcast_event"]
    )

    return row


def summarize(rows: List[Dict[str, Any]], counts: Dict[str, Any]) -> Dict[str, Any]:
    total = len(rows)

    def c(key: str) -> int:
        return sum(1 for row in rows if row.get(key))

    by_team: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        team = row.get("team") or "unknown"
        if team not in by_team:
            by_team[team] = {
                "starting_hitters": 0,
                "selected_player_split": 0,
                "opposite_player_split": 0,
                "batter_aggregate_90d": 0,
                "batter_aggregate_current_season": 0,
                "any_statcast_event": 0,
                "any_player_level_data": 0,
            }

        by_team[team]["starting_hitters"] += 1
        if row.get("has_selected_player_split"):
            by_team[team]["selected_player_split"] += 1
        if row.get("has_opposite_player_split"):
            by_team[team]["opposite_player_split"] += 1
        if row.get("has_batter_aggregate_90d"):
            by_team[team]["batter_aggregate_90d"] += 1
        if row.get("has_batter_aggregate_current_season"):
            by_team[team]["batter_aggregate_current_season"] += 1
        if row.get("has_any_statcast_event"):
            by_team[team]["any_statcast_event"] += 1
        if row.get("has_any_player_level_data"):
            by_team[team]["any_player_level_data"] += 1

    return {
        "table_counts": counts,
        "total_starting_hitters": total,
        "with_selected_player_split": c("has_selected_player_split"),
        "with_opposite_player_split": c("has_opposite_player_split"),
        "with_batter_aggregate_90d": c("has_batter_aggregate_90d"),
        "with_batter_aggregate_current_season": c("has_batter_aggregate_current_season"),
        "with_any_statcast_event": c("has_any_statcast_event"),
        "with_any_player_level_data": c("has_any_player_level_data"),
        "with_no_player_level_data": sum(1 for row in rows if not row.get("has_any_player_level_data")),
        "coverage_rates": {
            "selected_player_split": round(c("has_selected_player_split") / total, 4) if total else None,
            "opposite_player_split": round(c("has_opposite_player_split") / total, 4) if total else None,
            "batter_aggregate_90d": round(c("has_batter_aggregate_90d") / total, 4) if total else None,
            "batter_aggregate_current_season": round(c("has_batter_aggregate_current_season") / total, 4) if total else None,
            "any_statcast_event": round(c("has_any_statcast_event") / total, 4) if total else None,
            "any_player_level_data": round(c("has_any_player_level_data") / total, 4) if total else None,
        },
        "by_team": by_team,
    }


def print_summary(summary: Dict[str, Any]) -> None:
    print("\n=== LINEUP PLAYER DATA COVERAGE SUMMARY ===")
    print(json.dumps(summary, indent=2, default=str))

    print("\n=== TEAM COVERAGE ===")
    for team, row in sorted(summary.get("by_team", {}).items()):
        print(
            f"{team}: "
            f"hitters={row['starting_hitters']}, "
            f"selected_split={row['selected_player_split']}, "
            f"opposite_split={row['opposite_player_split']}, "
            f"agg90d={row['batter_aggregate_90d']}, "
            f"aggSeason={row['batter_aggregate_current_season']}, "
            f"statcast={row['any_statcast_event']}, "
            f"any={row['any_player_level_data']}"
        )


def print_missing_examples(rows: List[Dict[str, Any]], limit: int = 20) -> None:
    missing = [row for row in rows if not row.get("has_any_player_level_data")]
    if not missing:
        return

    print("\n=== MISSING PLAYER-LEVEL DATA EXAMPLES ===")
    for row in missing[:limit]:
        print(
            f"{row.get('team')} {row.get('lineup_slot')}. {row.get('player_name')} "
            f"(id={row.get('player_id')}, split={row.get('selected_split')})"
        )


def main() -> None:
    target_date = os.getenv("AUDIT_DATE") or dt.date.today().isoformat()
    season = int(target_date[:4])
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")

    engine = get_engine(database_url)
    create_tables(engine)
    Session = get_session(engine)

    print("\n=== LINEUP PLAYER DATA COVERAGE AUDIT ===")
    print(f"date: {target_date}")
    print(f"database_url: {database_url}")

    games = fetch_schedule(target_date)
    print(f"schedule_games: {len(games)}")

    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    with Session() as session:
        counts = table_counts(session)

        for game in games:
            game_pk = safe_int(game.get("_game_pk"))
            matchup = f"{team_name(game, 'away')} @ {team_name(game, 'home')}"
            print(f"\n{game_pk} | {matchup} | {game.get('_status')}")

            if game_pk is None:
                errors.append({"game_pk": None, "matchup": matchup, "error": "missing_game_pk"})
                continue

            try:
                lineups = fetch_boxscore_lineup(game_pk)
            except Exception as exc:
                errors.append({"game_pk": game_pk, "matchup": matchup, "error": str(exc)})
                print(f"  lineup_fetch_error: {exc}")
                continue

            for side in ["away", "home"]:
                lineup = lineups.get(side) or []
                print(f"  {side}: starting_hitters={len(lineup)}")

                for hitter in lineup:
                    rows.append(audit_hitter(session, game, side, hitter, season))

        summary = summarize(rows, counts)

    print_summary(summary)
    print_missing_examples(rows)

    os.makedirs("tmp", exist_ok=True)
    out_path = f"tmp/lineup_player_data_coverage_{target_date}.json"

    with open(out_path, "w") as f:
        json.dump(
            {
                "date": target_date,
                "summary": summary,
                "rows": rows,
                "errors": errors,
            },
            f,
            indent=2,
            default=str,
        )

    print(f"\nWrote full JSON audit to {out_path}")


if __name__ == "__main__":
    main()
