from __future__ import annotations

import datetime as dt
import json
import os
from typing import Any, Dict, List, Optional, Tuple

from mlb_app.database import (
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


def is_terminal(event_name: Any) -> bool:
    return clean_event(event_name) in TERMINAL_EVENTS


def is_true_ab(event_name: Any) -> bool:
    event = clean_event(event_name)
    return bool(event and event in TERMINAL_EVENTS and event not in NON_AB_EVENTS)


def split_for_pitcher_hand(p_throws: Optional[str]) -> Optional[str]:
    if p_throws == "L":
        return "vsL"
    if p_throws == "R":
        return "vsR"
    return None


def collect_lineup_hitters(target_date: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    games = fetch_schedule(target_date)
    hitters_by_id: Dict[int, Dict[str, Any]] = {}
    errors: List[Dict[str, Any]] = []

    for game in games:
        game_pk = safe_int(game.get("_game_pk"))
        matchup = f"{game.get('away', {}).get('team', {}).get('name')} @ {game.get('home', {}).get('team', {}).get('name')}"

        if game_pk is None:
            errors.append({"game_pk": None, "matchup": matchup, "error": "missing_game_pk"})
            continue

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
                        "player_id": player_id,
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


def calculate_split_metrics(events: List[StatcastEvent]) -> Optional[Dict[str, Any]]:
    terminal = dedupe_terminal_pas(events)
    if not terminal:
        return None

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
    iso = round(max(slugging_pct - batting_avg, 0.0), 3) if slugging_pct is not None and batting_avg is not None else None
    k_pct = round(strikeouts / pa, 4) if pa else None
    bb_pct = round(walks / pa, 4) if pa else None

    return {
        "pa": pa,
        "ab": ab,
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
    }


def upsert_player_split(
    session,
    player_id: int,
    season: int,
    split: str,
    metrics: Dict[str, Any],
) -> str:
    existing = (
        session.query(PlayerSplit)
        .filter(
            PlayerSplit.player_id == player_id,
            PlayerSplit.season == season,
            PlayerSplit.split == split,
        )
        .first()
    )

    if existing:
        target = existing
        action = "updated"
    else:
        target = PlayerSplit(player_id=player_id, season=season, split=split)
        session.add(target)
        action = "created"

    target.pa = metrics.get("pa")
    target.hits = metrics.get("hits")
    target.doubles = metrics.get("doubles")
    target.triples = metrics.get("triples")
    target.home_runs = metrics.get("home_runs")
    target.walks = metrics.get("walks")
    target.strikeouts = metrics.get("strikeouts")
    target.batting_avg = metrics.get("batting_avg")
    target.on_base_pct = metrics.get("on_base_pct")
    target.slugging_pct = metrics.get("slugging_pct")
    target.iso = metrics.get("iso")
    target.k_pct = metrics.get("k_pct")
    target.bb_pct = metrics.get("bb_pct")

    return action


def process_hitter(
    session,
    hitter: Dict[str, Any],
    season: int,
    start_date: dt.date,
    end_date: dt.date,
    min_pa: int,
) -> Dict[str, Any]:
    player_id = int(hitter["player_id"])

    events = (
        session.query(StatcastEvent)
        .filter(
            StatcastEvent.batter_id == player_id,
            StatcastEvent.game_date >= start_date,
            StatcastEvent.game_date <= end_date,
        )
        .all()
    )

    by_split: Dict[str, List[StatcastEvent]] = {"vsL": [], "vsR": []}
    ignored_unknown_hand = 0

    for event in events:
        split = split_for_pitcher_hand(event.p_throws)
        if split is None:
            ignored_unknown_hand += 1
            continue
        by_split[split].append(event)

    split_rows: List[Dict[str, Any]] = []

    for split, split_events in by_split.items():
        metrics = calculate_split_metrics(split_events)

        row = {
            "split": split,
            "raw_event_count": len(split_events),
            "action": None,
            "skipped": False,
            "skipped_reason": None,
            "metrics": metrics,
        }

        if metrics is None:
            row["skipped"] = True
            row["skipped_reason"] = "no_terminal_pa_events"
            split_rows.append(row)
            continue

        if metrics.get("pa", 0) < min_pa:
            row["skipped"] = True
            row["skipped_reason"] = f"insufficient_pa:{metrics.get('pa')}"
            split_rows.append(row)
            continue

        action = upsert_player_split(
            session=session,
            player_id=player_id,
            season=season,
            split=split,
            metrics=metrics,
        )

        row["action"] = action
        split_rows.append(row)

    return {
        **hitter,
        "season": season,
        "raw_event_count": len(events),
        "ignored_unknown_hand_events": ignored_unknown_hand,
        "splits": split_rows,
    }


def main() -> None:
    target_date_str = os.getenv("AUDIT_DATE") or dt.date.today().isoformat()
    target_date = dt.date.fromisoformat(target_date_str)
    season = int(target_date_str[:4])
    season_start = dt.date(season, 1, 1)
    min_pa = int(os.getenv("MIN_SPLIT_PA", "5"))

    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    Session = get_session(engine)

    print("\n=== LINEUP PLAYER SPLIT BACKFILL ===")
    print(f"date: {target_date_str}")
    print(f"season window: {season_start.isoformat()} to {target_date.isoformat()}")
    print(f"minimum split PA: {min_pa}")
    print(f"database_url: {database_url}")

    hitters, lineup_errors = collect_lineup_hitters(target_date_str)

    print(f"hitters targeted: {len(hitters)}")

    rows: List[Dict[str, Any]] = []

    with Session() as session:
        for hitter in hitters:
            rows.append(
                process_hitter(
                    session=session,
                    hitter=hitter,
                    season=season,
                    start_date=season_start,
                    end_date=target_date,
                    min_pa=min_pa,
                )
            )

        session.commit()

    splits_created = 0
    splits_updated = 0
    splits_skipped = 0
    skipped_reasons: Dict[str, int] = {}

    for row in rows:
        for split_row in row.get("splits") or []:
            action = split_row.get("action")
            if action == "created":
                splits_created += 1
            elif action == "updated":
                splits_updated += 1

            if split_row.get("skipped"):
                splits_skipped += 1
                reason = split_row.get("skipped_reason") or "unknown"
                skipped_reasons[reason] = skipped_reasons.get(reason, 0) + 1

    summary = {
        "date": target_date_str,
        "season": season,
        "season_start": season_start.isoformat(),
        "season_end": target_date.isoformat(),
        "min_split_pa": min_pa,
        "hitters_targeted": len(hitters),
        "splits_created": splits_created,
        "splits_updated": splits_updated,
        "splits_skipped": splits_skipped,
        "skipped_reasons": skipped_reasons,
        "lineup_fetch_errors": len(lineup_errors),
    }

    print("\n=== PLAYER SPLIT BACKFILL SUMMARY ===")
    print(json.dumps(summary, indent=2, default=str))

    if splits_skipped:
        print("\n=== SKIPPED SPLIT EXAMPLES ===")
        shown = 0
        for row in rows:
            for split_row in row.get("splits") or []:
                if not split_row.get("skipped"):
                    continue
                print(
                    f"{row.get('team')} {row.get('lineup_slot')}. {row.get('name')} "
                    f"(id={row.get('player_id')}) {split_row.get('split')}: "
                    f"{split_row.get('skipped_reason')}"
                )
                shown += 1
                if shown >= 20:
                    break
            if shown >= 20:
                break

    os.makedirs("tmp", exist_ok=True)
    out_path = f"tmp/lineup_player_split_backfill_{target_date_str}.json"
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
