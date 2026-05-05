from __future__ import annotations

import datetime as dt
import json
import os
from collections import defaultdict
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


HIT_EVENTS = {"single", "double", "triple", "home_run"}
DOUBLE_EVENTS = {"double"}
TRIPLE_EVENTS = {"triple"}
HOME_RUN_EVENTS = {"home_run"}
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


def pitcher_hand_to_split(hand: Optional[str]) -> str:
    return "vsL" if hand == "L" else "vsR"


def opposite_split(split: str) -> str:
    return "vsL" if split == "vsR" else "vsR"


def game_pitcher_split(game: Dict[str, Any], side: str) -> str:
    if side == "away":
        opposing = game.get("home", {}).get("probablePitcher", {})
    else:
        opposing = game.get("away", {}).get("probablePitcher", {})

    hand = opposing.get("pitchHand", {}).get("code")
    return pitcher_hand_to_split(hand)


def team_name(game: Dict[str, Any], side: str) -> Optional[str]:
    return game.get(side, {}).get("team", {}).get("name")


def is_terminal(event_name: Any) -> bool:
    return clean_event(event_name) in TERMINAL_EVENTS


def is_true_ab(event_name: Any) -> bool:
    event = clean_event(event_name)
    return bool(event and event in TERMINAL_EVENTS and event not in NON_AB_EVENTS)


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


def calculate_pa_metrics(events: List[StatcastEvent]) -> Dict[str, Any]:
    terminal = dedupe_terminal_pas(events)

    pa = len(terminal)
    ab_events = [event for event in terminal if is_true_ab(event.events)]
    ab = len(ab_events)

    outcomes = [clean_event(event.events) for event in terminal]

    hits = sum(1 for event in outcomes if event in HIT_EVENTS)
    doubles = sum(1 for event in outcomes if event in DOUBLE_EVENTS)
    triples = sum(1 for event in outcomes if event in TRIPLE_EVENTS)
    home_runs = sum(1 for event in outcomes if event in HOME_RUN_EVENTS)
    walks = sum(1 for event in outcomes if event in WALK_EVENTS)
    strikeouts = sum(1 for event in outcomes if event in STRIKEOUT_EVENTS)
    hit_by_pitch = sum(1 for event in outcomes if event == "hit_by_pitch")

    total_bases = hits + doubles + (2 * triples) + (3 * home_runs)

    obp_denominator = ab + walks + hit_by_pitch
    batting_avg = round(hits / ab, 3) if ab else None
    on_base_pct = round((hits + walks + hit_by_pitch) / obp_denominator, 3) if obp_denominator else None
    slugging_pct = round(total_bases / ab, 3) if ab else None
    iso = round(slugging_pct - batting_avg, 3) if slugging_pct is not None and batting_avg is not None else None
    k_pct = round(strikeouts / pa, 4) if pa else None
    bb_pct = round(walks / pa, 4) if pa else None

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
        "statcast_pa_count": pa,
        "statcast_ab_count": ab,
        "hits": hits,
        "doubles": doubles,
        "triples": triples,
        "home_runs": home_runs,
        "walks": walks,
        "strikeouts": strikeouts,
        "hit_by_pitch": hit_by_pitch,
        "batting_avg": batting_avg,
        "on_base_pct": on_base_pct,
        "slugging_pct": slugging_pct,
        "iso": iso,
        "k_pct": k_pct,
        "bb_pct": bb_pct,
        "batted_ball_count": len(batted_balls),
        "avg_exit_velocity": round(
            sum(event.launch_speed for event in batted_balls if event.launch_speed is not None) / len(batted_balls),
            2,
        ) if batted_balls else None,
        "avg_launch_angle": round(sum(launch_angles) / len(launch_angles), 2) if launch_angles else None,
        "hard_hit_pct": round(len(hard_hits) / len(batted_balls), 4) if batted_balls else None,
        "barrel_pct": round(len(barrels) / len(batted_balls), 4) if batted_balls else None,
    }


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
        "hits": row.hits,
        "doubles": row.doubles,
        "triples": row.triples,
        "home_runs": row.home_runs,
        "walks": row.walks,
        "strikeouts": row.strikeouts,
        "batting_avg": row.batting_avg,
        "on_base_pct": row.on_base_pct,
        "slugging_pct": row.slugging_pct,
        "iso": row.iso,
        "k_pct": row.k_pct,
        "bb_pct": row.bb_pct,
    }


def get_batter_aggregate_summary(session, player_id: int) -> Optional[Dict[str, Any]]:
    row = (
        session.query(BatterAggregate)
        .filter(
            BatterAggregate.batter_id == player_id,
            BatterAggregate.window == "90d",
        )
        .order_by(BatterAggregate.end_date.desc())
        .first()
    )
    if not row:
        return None

    # Current BatterAggregate schema does not include OBP/SLG/ISO/XBH fields.
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
        "contains_power_fields": False,
        "power_fields_missing": [
            "on_base_pct",
            "slugging_pct",
            "iso",
            "doubles",
            "triples",
            "home_runs",
            "hits",
            "walks",
            "strikeouts",
        ],
    }


def fetch_hitter_events(session, player_id: int, start_date: dt.date, end_date: dt.date) -> List[StatcastEvent]:
    return (
        session.query(StatcastEvent)
        .filter(
            StatcastEvent.batter_id == player_id,
            StatcastEvent.game_date >= start_date,
            StatcastEvent.game_date <= end_date,
        )
        .all()
    )


def split_events_by_pitcher_hand(events: List[StatcastEvent]) -> Dict[str, List[StatcastEvent]]:
    grouped: Dict[str, List[StatcastEvent]] = {"vsL": [], "vsR": [], "unknown": []}
    for event in events:
        if event.p_throws == "L":
            grouped["vsL"].append(event)
        elif event.p_throws == "R":
            grouped["vsR"].append(event)
        else:
            grouped["unknown"].append(event)
    return grouped


def table_counts(session) -> Dict[str, int]:
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


def audit_hitter(
    session,
    game: Dict[str, Any],
    side: str,
    hitter: Dict[str, Any],
    season: int,
    start_date: dt.date,
    end_date: dt.date,
) -> Dict[str, Any]:
    player_id = safe_int(hitter.get("batter_id"))
    selected_split = game_pitcher_split(game, side)
    opp_split = opposite_split(selected_split)

    row: Dict[str, Any] = {
        "game_pk": game.get("_game_pk"),
        "game_status": game.get("_status"),
        "side": side,
        "team": team_name(game, side),
        "batter_id": player_id,
        "name": hitter.get("name"),
        "batting_order": hitter.get("batting_order"),
        "lineup_slot": hitter.get("lineup_slot"),
        "position": hitter.get("position"),
        "season": season,
        "selected_split": selected_split,
        "opposite_split": opp_split,
        "selected_player_split_exists": False,
        "opposite_player_split_exists": False,
        "batter_aggregate_exists": False,
        "batter_aggregate_contains_power_fields": False,
        "batter_aggregate_power_shape": "missing",
        "selected_player_split": None,
        "opposite_player_split": None,
        "batter_aggregate": None,
        "overall_statcast_metrics": None,
        "selected_split_statcast_metrics": None,
        "opposite_split_statcast_metrics": None,
        "vsL_statcast_metrics": None,
        "vsR_statcast_metrics": None,
        "has_enough_pa_for_any_split_profile": False,
        "has_enough_pa_for_selected_split_profile": False,
        "likely_power_fallback": True,
    }

    if player_id is None:
        return row

    selected_ps = get_player_split_summary(session, player_id, season, selected_split)
    opposite_ps = get_player_split_summary(session, player_id, season, opp_split)
    agg = get_batter_aggregate_summary(session, player_id)

    events = fetch_hitter_events(session, player_id, start_date, end_date)
    grouped = split_events_by_pitcher_hand(events)

    overall_metrics = calculate_pa_metrics(events)
    vs_l_metrics = calculate_pa_metrics(grouped["vsL"])
    vs_r_metrics = calculate_pa_metrics(grouped["vsR"])
    selected_metrics = vs_l_metrics if selected_split == "vsL" else vs_r_metrics
    opposite_metrics = vs_r_metrics if selected_split == "vsL" else vs_l_metrics

    row.update({
        "selected_player_split_exists": selected_ps is not None,
        "opposite_player_split_exists": opposite_ps is not None,
        "batter_aggregate_exists": agg is not None,
        "batter_aggregate_contains_power_fields": bool(agg and agg.get("contains_power_fields")),
        "batter_aggregate_power_shape": "contact_only" if agg else "missing",
        "selected_player_split": selected_ps,
        "opposite_player_split": opposite_ps,
        "batter_aggregate": agg,
        "overall_statcast_metrics": overall_metrics,
        "selected_split_statcast_metrics": selected_metrics,
        "opposite_split_statcast_metrics": opposite_metrics,
        "vsL_statcast_metrics": vs_l_metrics,
        "vsR_statcast_metrics": vs_r_metrics,
    })

    min_pa = 5
    row["has_enough_pa_for_any_split_profile"] = (
        (vs_l_metrics.get("statcast_pa_count") or 0) >= min_pa
        or (vs_r_metrics.get("statcast_pa_count") or 0) >= min_pa
    )
    row["has_enough_pa_for_selected_split_profile"] = (
        (selected_metrics.get("statcast_pa_count") or 0) >= min_pa
    )

    # Power fallback is likely if no selected PlayerSplit exists, because BatterAggregate
    # currently lacks OBP/SLG/ISO/XBH fields.
    row["likely_power_fallback"] = selected_ps is None

    return row


def summarize(rows: List[Dict[str, Any]], counts: Dict[str, int]) -> Dict[str, Any]:
    total = len(rows)

    def c(predicate) -> int:
        return sum(1 for row in rows if predicate(row))

    with_selected_ps = c(lambda r: r.get("selected_player_split_exists"))
    with_opp_ps = c(lambda r: r.get("opposite_player_split_exists"))
    with_agg = c(lambda r: r.get("batter_aggregate_exists"))
    with_agg_power = c(lambda r: r.get("batter_aggregate_contains_power_fields"))
    enough_any_split = c(lambda r: r.get("has_enough_pa_for_any_split_profile"))
    enough_selected_split = c(lambda r: r.get("has_enough_pa_for_selected_split_profile"))
    likely_fallback = c(lambda r: r.get("likely_power_fallback"))

    vs_l_sample = c(lambda r: ((r.get("vsL_statcast_metrics") or {}).get("statcast_pa_count") or 0) >= 5)
    vs_r_sample = c(lambda r: ((r.get("vsR_statcast_metrics") or {}).get("statcast_pa_count") or 0) >= 5)

    by_team: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "hitters": 0,
        "selected_player_split": 0,
        "opposite_player_split": 0,
        "batter_aggregate": 0,
        "batter_aggregate_power_fields": 0,
        "enough_selected_split_pa": 0,
        "likely_power_fallback": 0,
    })

    for row in rows:
        team = row.get("team") or "unknown"
        by_team[team]["hitters"] += 1
        if row.get("selected_player_split_exists"):
            by_team[team]["selected_player_split"] += 1
        if row.get("opposite_player_split_exists"):
            by_team[team]["opposite_player_split"] += 1
        if row.get("batter_aggregate_exists"):
            by_team[team]["batter_aggregate"] += 1
        if row.get("batter_aggregate_contains_power_fields"):
            by_team[team]["batter_aggregate_power_fields"] += 1
        if row.get("has_enough_pa_for_selected_split_profile"):
            by_team[team]["enough_selected_split_pa"] += 1
        if row.get("likely_power_fallback"):
            by_team[team]["likely_power_fallback"] += 1

    return {
        "table_counts": counts,
        "total_lineup_hitters": total,
        "with_selected_player_split": with_selected_ps,
        "with_opposite_player_split": with_opp_ps,
        "with_batter_aggregate_90d": with_agg,
        "with_batter_aggregate_power_fields": with_agg_power,
        "hitters_with_enough_pa_for_any_split_profile": enough_any_split,
        "hitters_with_enough_pa_for_selected_split_profile": enough_selected_split,
        "hitters_with_vsL_sample": vs_l_sample,
        "hitters_with_vsR_sample": vs_r_sample,
        "hitters_missing_power_fields": total - with_selected_ps,
        "likely_power_fallback_count": likely_fallback,
        "coverage_rates": {
            "selected_player_split": round(with_selected_ps / total, 4) if total else None,
            "opposite_player_split": round(with_opp_ps / total, 4) if total else None,
            "batter_aggregate_90d": round(with_agg / total, 4) if total else None,
            "batter_aggregate_power_fields": round(with_agg_power / total, 4) if total else None,
            "enough_any_split_profile": round(enough_any_split / total, 4) if total else None,
            "enough_selected_split_profile": round(enough_selected_split / total, 4) if total else None,
            "likely_power_fallback": round(likely_fallback / total, 4) if total else None,
        },
        "by_team": dict(sorted(by_team.items())),
    }


def print_summary(summary: Dict[str, Any]) -> None:
    print("\n=== LINEUP POWER / SPLIT COVERAGE SUMMARY ===")
    print(json.dumps(summary, indent=2, default=str))

    print("\n=== TEAM POWER/SPLIT COVERAGE ===")
    for team, row in summary.get("by_team", {}).items():
        print(
            f"{team}: "
            f"hitters={row['hitters']}, "
            f"selectedPS={row['selected_player_split']}, "
            f"oppPS={row['opposite_player_split']}, "
            f"agg={row['batter_aggregate']}, "
            f"aggPower={row['batter_aggregate_power_fields']}, "
            f"enoughSelectedPA={row['enough_selected_split_pa']}, "
            f"likelyFallback={row['likely_power_fallback']}"
        )


def print_examples(rows: List[Dict[str, Any]]) -> None:
    print("\n=== SAMPLE HITTER POWER/SPLIT METRICS ===")
    for row in rows[:15]:
        selected = row.get("selected_split_statcast_metrics") or {}
        print(
            f"{row.get('team')} {row.get('lineup_slot')}. {row.get('name')} "
            f"id={row.get('batter_id')} split={row.get('selected_split')} "
            f"PA={selected.get('statcast_pa_count')} "
            f"AVG={selected.get('batting_avg')} "
            f"OBP={selected.get('on_base_pct')} "
            f"SLG={selected.get('slugging_pct')} "
            f"ISO={selected.get('iso')} "
            f"HR={selected.get('home_runs')} "
            f"selectedPS={row.get('selected_player_split_exists')}"
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

    print("\n=== LINEUP POWER / SPLIT COVERAGE AUDIT ===")
    print(f"date: {target_date_str}")
    print(f"season window: {season_start.isoformat()} to {target_date.isoformat()}")
    print(f"database_url: {database_url}")

    games = fetch_schedule(target_date_str)
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
                    rows.append(
                        audit_hitter(
                            session=session,
                            game=game,
                            side=side,
                            hitter=hitter,
                            season=season,
                            start_date=season_start,
                            end_date=target_date,
                        )
                    )

        summary = summarize(rows, counts)

    print_summary(summary)
    print_examples(rows)

    os.makedirs("tmp", exist_ok=True)
    out_path = f"tmp/lineup_power_split_coverage_{target_date_str}.json"
    with open(out_path, "w") as f:
        json.dump(
            {
                "date": target_date_str,
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
