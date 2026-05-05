from __future__ import annotations

import datetime as dt
import json
import os
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_

from mlb_app.database import (
    BatterAggregate,
    StatcastEvent,
    create_tables,
    get_engine,
    get_session,
)
from mlb_app.etl import fetch_schedule
from mlb_app.lineup_profile import fetch_boxscore_lineup


HIT_EVENTS = {"single", "double", "triple", "home_run"}
WALK_EVENTS = {"walk", "intent_walk"}
STRIKEOUT_EVENTS = {"strikeout", "strikeout_double_play"}
TERMINAL_EVENTS = {
    "single",
    "double",
    "triple",
    "home_run",
    "strikeout",
    "strikeout_double_play",
    "walk",
    "intent_walk",
    "hit_by_pitch",
    "field_out",
    "force_out",
    "double_play",
    "grounded_into_double_play",
    "fielders_choice",
    "fielders_choice_out",
    "sac_fly",
    "sac_bunt",
    "catcher_interf",
    "catcher_interference",
}
NON_AB_EVENTS = {
    "walk",
    "intent_walk",
    "hit_by_pitch",
    "sac_bunt",
    "sac_fly",
    "catcher_interf",
    "catcher_interference",
}


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def clean_event(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"", "none", "nan", "null", "na", "n/a"}:
        return None
    return text


def is_terminal(event_name: Any) -> bool:
    return clean_event(event_name) in TERMINAL_EVENTS


def is_true_ab(event_name: Any) -> bool:
    event = clean_event(event_name)
    return bool(event and event in TERMINAL_EVENTS and event not in NON_AB_EVENTS)


def collect_lineup_hitters(target_date: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    games = fetch_schedule(target_date)
    hitters_by_id: Dict[int, Dict[str, Any]] = {}
    errors: List[Dict[str, Any]] = []

    for game in games:
        game_pk = safe_int(game.get("_game_pk"))
        if game_pk is None:
            errors.append({"game_pk": None, "error": "missing_game_pk"})
            continue

        matchup = f"{game.get('away', {}).get('team', {}).get('name')} @ {game.get('home', {}).get('team', {}).get('name')}"

        try:
            lineups = fetch_boxscore_lineup(game_pk)
        except Exception as exc:
            errors.append({"game_pk": game_pk, "matchup": matchup, "error": str(exc)})
            continue

        for side in ["away", "home"]:
            team_name = game.get(side, {}).get("team", {}).get("name")
            for hitter in lineups.get(side) or []:
                player_id = safe_int(hitter.get("batter_id"))
                if player_id is None:
                    continue

                if player_id not in hitters_by_id:
                    hitters_by_id[player_id] = {
                        "batter_id": player_id,
                        "name": hitter.get("name"),
                        "team": team_name,
                        "side": side,
                        "game_pk": game_pk,
                        "batting_order": hitter.get("batting_order"),
                        "lineup_slot": hitter.get("lineup_slot"),
                        "position": hitter.get("position"),
                    }

    hitters = sorted(
        hitters_by_id.values(),
        key=lambda row: (row.get("team") or "", row.get("lineup_slot") or 99, row.get("name") or ""),
    )
    return hitters, errors


def dedupe_terminal_pas(events: List[StatcastEvent]) -> List[StatcastEvent]:
    seen = set()
    out: List[StatcastEvent] = []

    for event in events:
        if not is_terminal(event.events):
            continue

        if event.game_pk is not None and event.at_bat_number is not None:
            key = (event.game_pk, event.at_bat_number, event.pitcher_id, event.batter_id)
        else:
            key = (
                event.game_date,
                event.pitcher_id,
                event.batter_id,
                clean_event(event.events),
                event.inning,
                event.inning_topbot,
                event.outs_when_up,
            )

        if key in seen:
            continue

        seen.add(key)
        out.append(event)

    return out


def calculate_batter_aggregate(events: List[StatcastEvent]) -> Optional[Dict[str, Any]]:
    terminal = dedupe_terminal_pas(events)
    if not terminal:
        return None

    pa = len(terminal)
    ab_events = [event for event in terminal if is_true_ab(event.events)]
    ab = len(ab_events)

    outcomes = [clean_event(event.events) for event in terminal]
    hits = sum(1 for event in outcomes if event in HIT_EVENTS)
    walks = sum(1 for event in outcomes if event in WALK_EVENTS)
    strikeouts = sum(1 for event in outcomes if event in STRIKEOUT_EVENTS)

    batted_balls = [event for event in terminal if event.launch_speed is not None]
    launch_angles = [event.launch_angle for event in terminal if event.launch_angle is not None]
    hard_hits = [
        event
        for event in batted_balls
        if event.launch_speed is not None and event.launch_speed >= 95
    ]

    barrels = [
        event
        for event in batted_balls
        if (
            event.launch_speed is not None
            and event.launch_angle is not None
            and event.launch_speed >= 98
            and 8 <= event.launch_angle <= 50
        )
    ]

    return {
        "actual_pa": pa,
        "actual_ab": ab,
        "hits": hits,
        "walks": walks,
        "strikeouts": strikeouts,
        "batted_ball_count": len(batted_balls),
        "batting_avg": round(hits / ab, 3) if ab else None,
        "k_pct": round(strikeouts / pa, 4) if pa else None,
        "bb_pct": round(walks / pa, 4) if pa else None,
        "avg_exit_velocity": round(
            sum(event.launch_speed for event in batted_balls if event.launch_speed is not None) / len(batted_balls),
            2,
        ) if batted_balls else None,
        "avg_launch_angle": round(sum(launch_angles) / len(launch_angles), 2) if launch_angles else None,
        "hard_hit_pct": round(len(hard_hits) / len(batted_balls), 4) if batted_balls else None,
        "barrel_pct": round(len(barrels) / len(batted_balls), 4) if batted_balls else None,
    }


def upsert_batter_aggregate(session, batter_id: int, end_date: dt.date, metrics: Dict[str, Any]) -> str:
    existing = (
        session.query(BatterAggregate)
        .filter(
            BatterAggregate.batter_id == batter_id,
            BatterAggregate.window == "90d",
            BatterAggregate.end_date == end_date,
        )
        .first()
    )

    if existing:
        target = existing
        action = "updated"
    else:
        target = BatterAggregate(
            batter_id=batter_id,
            window="90d",
            end_date=end_date,
        )
        session.add(target)
        action = "created"

    target.avg_exit_velocity = metrics.get("avg_exit_velocity")
    target.avg_launch_angle = metrics.get("avg_launch_angle")
    target.hard_hit_pct = metrics.get("hard_hit_pct")
    target.barrel_pct = metrics.get("barrel_pct")
    target.k_pct = metrics.get("k_pct")
    target.bb_pct = metrics.get("bb_pct")
    target.batting_avg = metrics.get("batting_avg")

    return action


def process_hitter(session, hitter: Dict[str, Any], start_date: dt.date, end_date: dt.date) -> Dict[str, Any]:
    batter_id = int(hitter["batter_id"])

    events = (
        session.query(StatcastEvent)
        .filter(
            StatcastEvent.batter_id == batter_id,
            StatcastEvent.game_date >= start_date,
            StatcastEvent.game_date <= end_date,
        )
        .all()
    )

    row = {
        **hitter,
        "raw_event_count": len(events),
        "terminal_pa_count": 0,
        "action": None,
        "skipped": False,
        "skipped_reason": None,
        "metrics": None,
    }

    if not events:
        row["skipped"] = True
        row["skipped_reason"] = "no_statcast_events_in_90d_window"
        return row

    metrics = calculate_batter_aggregate(events)

    if not metrics:
        row["skipped"] = True
        row["skipped_reason"] = "no_terminal_pa_events_in_90d_window"
        return row

    if metrics.get("actual_pa", 0) < 5:
        row["skipped"] = True
        row["skipped_reason"] = f"insufficient_terminal_pa:{metrics.get('actual_pa')}"
        row["metrics"] = metrics
        return row

    action = upsert_batter_aggregate(session, batter_id, end_date, metrics)

    row["terminal_pa_count"] = metrics.get("actual_pa")
    row["action"] = action
    row["metrics"] = metrics

    return row


def main() -> None:
    target_date_str = os.getenv("AUDIT_DATE") or dt.date.today().isoformat()
    target_date = dt.date.fromisoformat(target_date_str)
    start_date = target_date - dt.timedelta(days=90)

    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    Session = get_session(engine)

    print("\n=== LINEUP BATTER AGGREGATE BACKFILL ===")
    print(f"date: {target_date_str}")
    print(f"window: {start_date.isoformat()} to {target_date.isoformat()}")
    print(f"database_url: {database_url}")

    hitters, lineup_errors = collect_lineup_hitters(target_date_str)

    print(f"hitters targeted: {len(hitters)}")

    rows: List[Dict[str, Any]] = []

    with Session() as session:
        for hitter in hitters:
            row = process_hitter(session, hitter, start_date, target_date)
            rows.append(row)

        session.commit()

    created = sum(1 for row in rows if row.get("action") == "created")
    updated = sum(1 for row in rows if row.get("action") == "updated")
    skipped = sum(1 for row in rows if row.get("skipped"))

    skipped_reasons: Dict[str, int] = {}
    for row in rows:
        reason = row.get("skipped_reason")
        if reason:
            skipped_reasons[reason] = skipped_reasons.get(reason, 0) + 1

    summary = {
        "date": target_date_str,
        "window_start": start_date.isoformat(),
        "window_end": target_date.isoformat(),
        "hitters_targeted": len(hitters),
        "aggregates_created": created,
        "aggregates_updated": updated,
        "hitters_skipped": skipped,
        "skipped_reasons": skipped_reasons,
        "lineup_fetch_errors": len(lineup_errors),
    }

    print("\n=== BACKFILL SUMMARY ===")
    print(json.dumps(summary, indent=2, default=str))

    if skipped:
        print("\n=== SKIPPED EXAMPLES ===")
        for row in [r for r in rows if r.get("skipped")][:20]:
            print(
                f"{row.get('team')} {row.get('lineup_slot')}. {row.get('name')} "
                f"(id={row.get('batter_id')}): {row.get('skipped_reason')}"
            )

    os.makedirs("tmp", exist_ok=True)
    out_path = f"tmp/lineup_batter_aggregate_backfill_{target_date_str}.json"
    with open(out_path, "w") as f:
        json.dump(
            {
                "summary": summary,
                "rows": rows,
                "lineup_errors": lineup_errors,
            },
            f,
            indent=2,
            default=str,
        )

    print(f"\nWrote full JSON report to {out_path}")


if __name__ == "__main__":
    main()
