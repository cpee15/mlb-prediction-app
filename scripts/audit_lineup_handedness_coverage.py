from __future__ import annotations

import datetime as dt
import json
import os
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple

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


def normalize_stand(value: Any) -> Optional[str]:
    if value is None:
        return None

    text = str(value).strip().upper()
    if text in {"L", "R"}:
        return text

    # Statcast usually records switch hitters by actual PA side, not "S".
    # If a hitter has both L and R rows in Statcast, we infer switch.
    if text in {"S", "B"}:
        return "S"

    return None


def get_nested(row: Dict[str, Any], *keys: str) -> Any:
    cur: Any = row
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def team_name(game: Dict[str, Any], side: str) -> Optional[str]:
    return get_nested(game, side, "team", "name")


def lineup_slot_from_batting_order(value: Any) -> Optional[int]:
    order = safe_int(value)
    if order is None:
        return None

    # MLB boxscore battingOrder can contain substitution values like 301, 601, 901.
    # The lineup slot is the hundreds digit.
    if order >= 100:
        return order // 100

    return order


def collect_confirmed_lineup_hitters(target_date: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    games = fetch_schedule(target_date)
    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for game in games:
        game_pk = safe_int(game.get("_game_pk") or game.get("gamePk"))
        matchup = f"{team_name(game, 'away')} @ {team_name(game, 'home')}"
        status = game.get("_status") or get_nested(game, "status", "detailedState")

        if game_pk is None:
            errors.append({"game_pk": None, "matchup": matchup, "error": "missing_game_pk"})
            continue

        try:
            lineups = fetch_boxscore_lineup(game_pk)
        except Exception as exc:
            errors.append({"game_pk": game_pk, "matchup": matchup, "error": str(exc)})
            continue

        for side in ["away", "home"]:
            team = team_name(game, side)
            for hitter in lineups.get(side) or []:
                player_id = safe_int(
                    hitter.get("batter_id")
                    or hitter.get("player_id")
                    or hitter.get("id")
                )
                if player_id is None:
                    continue

                batting_order = hitter.get("batting_order")
                rows.append(
                    {
                        "game_pk": game_pk,
                        "matchup": matchup,
                        "game_status": status,
                        "side": side,
                        "team": team,
                        "hitter_id": player_id,
                        "name": hitter.get("name") or hitter.get("player_name"),
                        "batting_order": batting_order,
                        "lineup_slot": hitter.get("lineup_slot") or lineup_slot_from_batting_order(batting_order),
                        "position": hitter.get("position"),
                    }
                )

    # Keep confirmed lineup appearances, but also provide unique hitter counts later.
    rows.sort(
        key=lambda r: (
            r.get("game_pk") or 0,
            r.get("side") or "",
            r.get("lineup_slot") or 99,
            r.get("name") or "",
        )
    )
    return rows, errors


def statcast_stand_counts(session, hitter_id: int, season_start: dt.date, target_date: dt.date) -> Dict[str, int]:
    rows = (
        session.query(StatcastEvent.stand, func.count(StatcastEvent.id))
        .filter(
            StatcastEvent.batter_id == hitter_id,
            StatcastEvent.game_date >= season_start,
            StatcastEvent.game_date <= target_date,
            StatcastEvent.stand.isnot(None),
        )
        .group_by(StatcastEvent.stand)
        .all()
    )

    counts: Dict[str, int] = {"L": 0, "R": 0, "S": 0, "unknown": 0}
    for raw_stand, count in rows:
        stand = normalize_stand(raw_stand)
        if stand in {"L", "R", "S"}:
            counts[stand] += int(count or 0)
        else:
            counts["unknown"] += int(count or 0)

    return counts


def infer_handedness_from_counts(counts: Dict[str, int]) -> Dict[str, Any]:
    l_count = counts.get("L", 0)
    r_count = counts.get("R", 0)
    s_count = counts.get("S", 0)
    known = l_count + r_count + s_count

    if known <= 0:
        return {
            "inferred_stand": None,
            "source": "missing_statcast_stand",
            "confidence": "missing",
            "switch_hitter_handling": None,
            "known_statcast_stand_rows": 0,
        }

    # If explicit S exists, trust it, but keep counts.
    if s_count > 0:
        return {
            "inferred_stand": "S",
            "source": "statcast_stand_explicit_switch",
            "confidence": "high" if known >= 20 else "medium",
            "switch_hitter_handling": "explicit_switch_from_statcast",
            "known_statcast_stand_rows": known,
        }

    # Statcast stand usually records the batter side for the PA.
    # A hitter with meaningful rows from both sides is likely a switch hitter.
    min_side = min(l_count, r_count)
    max_side = max(l_count, r_count)

    if l_count > 0 and r_count > 0:
        if min_side >= 5:
            return {
                "inferred_stand": "S",
                "source": "statcast_stand_both_sides",
                "confidence": "high" if known >= 30 else "medium",
                "switch_hitter_handling": "treat_as_switch_for_lineup_mix",
                "known_statcast_stand_rows": known,
            }

        # Tiny opposite-side samples can be noise/data quirks.
        dominant = "L" if l_count >= r_count else "R"
        share = max_side / known if known else 0.0
        return {
            "inferred_stand": dominant,
            "source": "statcast_stand_dominant_side_with_tiny_opposite_sample",
            "confidence": "medium" if share >= 0.9 and known >= 20 else "low",
            "switch_hitter_handling": "opposite_sample_too_small_to_call_switch",
            "known_statcast_stand_rows": known,
        }

    if l_count > 0:
        return {
            "inferred_stand": "L",
            "source": "statcast_stand_single_side",
            "confidence": "high" if l_count >= 20 else "medium" if l_count >= 5 else "low",
            "switch_hitter_handling": None,
            "known_statcast_stand_rows": known,
        }

    if r_count > 0:
        return {
            "inferred_stand": "R",
            "source": "statcast_stand_single_side",
            "confidence": "high" if r_count >= 20 else "medium" if r_count >= 5 else "low",
            "switch_hitter_handling": None,
            "known_statcast_stand_rows": known,
        }

    return {
        "inferred_stand": None,
        "source": "missing_statcast_stand",
        "confidence": "missing",
        "switch_hitter_handling": None,
        "known_statcast_stand_rows": known,
    }


def player_split_count(session, hitter_id: int, season: int) -> int:
    return (
        session.query(PlayerSplit)
        .filter(PlayerSplit.player_id == hitter_id, PlayerSplit.season == season)
        .count()
    )


def batter_aggregate_count(session, hitter_id: int, target_date: dt.date) -> int:
    return (
        session.query(BatterAggregate)
        .filter(
            BatterAggregate.batter_id == hitter_id,
            BatterAggregate.end_date <= target_date,
        )
        .count()
    )


def statcast_event_count(session, hitter_id: int, season_start: dt.date, target_date: dt.date) -> int:
    return (
        session.query(StatcastEvent)
        .filter(
            StatcastEvent.batter_id == hitter_id,
            StatcastEvent.game_date >= season_start,
            StatcastEvent.game_date <= target_date,
        )
        .count()
    )


def main() -> None:
    target_date_str = os.getenv("AUDIT_DATE") or dt.date.today().isoformat()
    target_date = dt.date.fromisoformat(target_date_str)
    season = int(target_date_str[:4])
    season_start = dt.date(season, 1, 1)

    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    Session = get_session(engine)

    print("\n=== LINEUP HANDEDNESS COVERAGE AUDIT ===")
    print(f"date: {target_date_str}")
    print(f"season window: {season_start.isoformat()} to {target_date.isoformat()}")
    print(f"database_url: {database_url}")

    lineup_rows, lineup_errors = collect_confirmed_lineup_hitters(target_date_str)

    results: List[Dict[str, Any]] = []

    with Session() as session:
        for row in lineup_rows:
            hitter_id = int(row["hitter_id"])
            counts = statcast_stand_counts(session, hitter_id, season_start, target_date)
            inferred = infer_handedness_from_counts(counts)

            result = {
                **row,
                "statcast_stand_counts": counts,
                **inferred,
                "statcast_event_count": statcast_event_count(session, hitter_id, season_start, target_date),
                "player_split_count": player_split_count(session, hitter_id, season),
                "batter_aggregate_count": batter_aggregate_count(session, hitter_id, target_date),
            }
            results.append(result)

    unique_hitters: Dict[int, Dict[str, Any]] = {}
    for row in results:
        unique_hitters.setdefault(int(row["hitter_id"]), row)

    team_mix: Dict[str, Dict[str, Any]] = {}
    for row in results:
        key = f"{row.get('game_pk')}|{row.get('side')}|{row.get('team')}"
        if key not in team_mix:
            team_mix[key] = {
                "game_pk": row.get("game_pk"),
                "matchup": row.get("matchup"),
                "side": row.get("side"),
                "team": row.get("team"),
                "counts": {"L": 0, "R": 0, "S": 0, "unknown": 0},
                "hitter_ids": [],
            }

        stand = row.get("inferred_stand") or "unknown"
        if stand not in {"L", "R", "S"}:
            stand = "unknown"

        team_mix[key]["counts"][stand] += 1
        team_mix[key]["hitter_ids"].append(row.get("hitter_id"))

    source_counts = Counter(row.get("source") for row in results)
    confidence_counts = Counter(row.get("confidence") for row in results)
    stand_counts = Counter(row.get("inferred_stand") or "unknown" for row in results)

    total_lineup_rows = len(results)
    total_unique_hitters = len(unique_hitters)
    lineup_rows_with_stand = sum(1 for row in results if row.get("inferred_stand") in {"L", "R", "S"})
    unique_hitters_with_stand = sum(
        1 for row in unique_hitters.values()
        if row.get("inferred_stand") in {"L", "R", "S"}
    )

    missing_rows = [
        row for row in results
        if row.get("inferred_stand") not in {"L", "R", "S"}
    ]

    low_confidence_rows = [
        row for row in results
        if row.get("confidence") in {"missing", "low"}
    ]

    summary = {
        "date": target_date_str,
        "season": season,
        "total_lineup_rows": total_lineup_rows,
        "total_unique_hitters": total_unique_hitters,
        "lineup_rows_with_handedness": lineup_rows_with_stand,
        "unique_hitters_with_handedness": unique_hitters_with_stand,
        "lineup_handedness_coverage_rate": round(lineup_rows_with_stand / total_lineup_rows, 4) if total_lineup_rows else None,
        "unique_handedness_coverage_rate": round(unique_hitters_with_stand / total_unique_hitters, 4) if total_unique_hitters else None,
        "missing_lineup_rows": len(missing_rows),
        "low_confidence_rows": len(low_confidence_rows),
        "inferred_stand_counts": dict(stand_counts),
        "source_counts": dict(source_counts),
        "confidence_counts": dict(confidence_counts),
        "lineup_fetch_errors": len(lineup_errors),
        "team_lineup_mixes": list(team_mix.values()),
        "missing_hitters": [
            {
                "hitter_id": row.get("hitter_id"),
                "name": row.get("name"),
                "team": row.get("team"),
                "game_pk": row.get("game_pk"),
                "batting_order": row.get("batting_order"),
                "statcast_event_count": row.get("statcast_event_count"),
                "statcast_stand_counts": row.get("statcast_stand_counts"),
                "source": row.get("source"),
                "confidence": row.get("confidence"),
            }
            for row in missing_rows
        ],
        "low_confidence_examples": [
            {
                "hitter_id": row.get("hitter_id"),
                "name": row.get("name"),
                "team": row.get("team"),
                "game_pk": row.get("game_pk"),
                "batting_order": row.get("batting_order"),
                "inferred_stand": row.get("inferred_stand"),
                "statcast_stand_counts": row.get("statcast_stand_counts"),
                "source": row.get("source"),
                "confidence": row.get("confidence"),
            }
            for row in low_confidence_rows[:20]
        ],
    }

    print("\n=== HANDEDNESS COVERAGE SUMMARY ===")
    print(json.dumps(summary, indent=2, default=str))

    print("\n=== TEAM LINEUP HANDEDNESS MIXES ===")
    for mix in summary["team_lineup_mixes"]:
        print(
            f"{mix['game_pk']} | {mix['team']} ({mix['side']}): "
            f"L={mix['counts']['L']} R={mix['counts']['R']} "
            f"S={mix['counts']['S']} unknown={mix['counts']['unknown']}"
        )

    if missing_rows:
        print("\n=== MISSING HANDEDNESS HITTERS ===")
        for row in missing_rows[:30]:
            print(
                f"{row.get('game_pk')} | {row.get('team')} | "
                f"{row.get('batting_order')} {row.get('name')} "
                f"id={row.get('hitter_id')} events={row.get('statcast_event_count')} "
                f"stand_counts={row.get('statcast_stand_counts')}"
            )

    os.makedirs("tmp", exist_ok=True)
    out_path = f"tmp/lineup_handedness_coverage_{target_date_str}.json"
    with open(out_path, "w") as f:
        json.dump(
            {
                "summary": summary,
                "rows": results,
                "lineup_errors": lineup_errors,
            },
            f,
            indent=2,
            default=str,
        )

    print(f"\nWrote full JSON audit to {out_path}")


if __name__ == "__main__":
    main()
