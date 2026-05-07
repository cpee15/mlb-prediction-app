from __future__ import annotations

import datetime as dt
import json
import os
from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, Iterable, List, Optional, Tuple

from mlb_app.database import (
    BatterAggregate,
    PlayerSplit,
    StatcastEvent,
    create_tables,
    get_engine,
    get_session,
)


HIT_EVENTS = {"single", "double", "triple", "home_run"}
TOTAL_BASES = {"single": 1, "double": 2, "triple": 3, "home_run": 4}
WALK_EVENTS = {"walk", "intent_walk", "intentional_walk"}
STRIKEOUT_EVENTS = {"strikeout", "strikeout_double_play"}
HBP_EVENTS = {"hit_by_pitch"}
SAC_FLY_EVENTS = {"sac_fly"}
SAC_BUNT_EVENTS = {"sac_bunt"}
CATCHER_INTERFERENCE_EVENTS = {"catcher_interf", "catcher_interference"}

TERMINAL_EVENTS = {
    "single",
    "double",
    "triple",
    "home_run",
    "walk",
    "intent_walk",
    "intentional_walk",
    "strikeout",
    "strikeout_double_play",
    "field_out",
    "force_out",
    "grounded_into_double_play",
    "fielders_choice",
    "fielders_choice_out",
    "hit_by_pitch",
    "sac_fly",
    "sac_bunt",
    "double_play",
    "triple_play",
    "catcher_interf",
    "catcher_interference",
    "field_error",
}


def _parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _safe_div(numerator: float, denominator: float) -> Optional[float]:
    if denominator is None or denominator == 0:
        return None
    return round(float(numerator) / float(denominator), 4)


def _avg(values: List[float]) -> Optional[float]:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return round(sum(clean) / len(clean), 4)


def _distribution(values: List[int]) -> Dict[str, Optional[float]]:
    clean = sorted(int(value) for value in values if value is not None)
    if not clean:
        return {"min": None, "median": None, "avg": None, "max": None}
    return {
        "min": clean[0],
        "median": round(float(median(clean)), 2),
        "avg": round(float(mean(clean)), 2),
        "max": clean[-1],
    }


def _event_key(event: StatcastEvent) -> Tuple[Any, Any, Any]:
    game_pk = event.game_pk if event.game_pk is not None else f"date:{event.game_date}"
    at_bat = event.at_bat_number if event.at_bat_number is not None else f"row:{event.id}"
    return game_pk, event.batter_id, at_bat


def _terminal_pa_events(events: Iterable[StatcastEvent]) -> List[StatcastEvent]:
    by_pa: Dict[Tuple[Any, Any, Any], List[StatcastEvent]] = defaultdict(list)

    for event in events:
        if not event.events:
            continue
        event_name = str(event.events).strip()
        if event_name not in TERMINAL_EVENTS:
            continue
        by_pa[_event_key(event)].append(event)

    terminal: List[StatcastEvent] = []
    for group in by_pa.values():
        group_sorted = sorted(
            group,
            key=lambda row: (
                row.pitch_number if row.pitch_number is not None else -1,
                row.id if row.id is not None else -1,
            ),
        )
        terminal.append(group_sorted[-1])

    return terminal


def _compute_batting_metrics(events: List[StatcastEvent]) -> Dict[str, Any]:
    terminal = _terminal_pa_events(events)
    pa = len(terminal)

    hits = 0
    doubles = 0
    triples = 0
    home_runs = 0
    walks = 0
    strikeouts = 0
    hbp = 0
    sac_flies = 0
    sac_bunts = 0
    catcher_interference = 0
    total_bases = 0

    for event in terminal:
        event_name = str(event.events or "").strip()

        if event_name in HIT_EVENTS:
            hits += 1
            total_bases += TOTAL_BASES.get(event_name, 0)
        if event_name == "double":
            doubles += 1
        elif event_name == "triple":
            triples += 1
        elif event_name == "home_run":
            home_runs += 1

        if event_name in WALK_EVENTS:
            walks += 1
        if event_name in STRIKEOUT_EVENTS:
            strikeouts += 1
        if event_name in HBP_EVENTS:
            hbp += 1
        if event_name in SAC_FLY_EVENTS:
            sac_flies += 1
        if event_name in SAC_BUNT_EVENTS:
            sac_bunts += 1
        if event_name in CATCHER_INTERFERENCE_EVENTS:
            catcher_interference += 1

    ab = pa - walks - hbp - sac_flies - sac_bunts - catcher_interference
    batting_avg = _safe_div(hits, ab)
    on_base_pct = _safe_div(hits + walks + hbp, ab + walks + hbp + sac_flies)
    slugging_pct = _safe_div(total_bases, ab)
    iso = None
    if slugging_pct is not None and batting_avg is not None:
        iso = round(max(slugging_pct - batting_avg, 0.0), 4)

    launch_speeds = [float(event.launch_speed) for event in events if event.launch_speed is not None]
    launch_angles = [float(event.launch_angle) for event in events if event.launch_angle is not None]
    hard_hit_count = sum(1 for value in launch_speeds if value >= 95.0)

    return {
        "pa": pa,
        "ab": ab,
        "hits": hits,
        "doubles": doubles,
        "triples": triples,
        "home_runs": home_runs,
        "walks": walks,
        "strikeouts": strikeouts,
        "hbp": hbp,
        "sac_flies": sac_flies,
        "sac_bunts": sac_bunts,
        "catcher_interference": catcher_interference,
        "total_bases": total_bases,
        "batting_avg": batting_avg,
        "on_base_pct": on_base_pct,
        "slugging_pct": slugging_pct,
        "iso": iso,
        "k_pct": _safe_div(strikeouts, pa),
        "bb_pct": _safe_div(walks, pa),
        "avg_exit_velocity": _avg(launch_speeds),
        "avg_launch_angle": _avg(launch_angles),
        "hard_hit_pct": _safe_div(hard_hit_count, len(launch_speeds)),
        "barrel_pct": None,
        "terminal_pa_count": pa,
        "batted_ball_count": len(launch_speeds),
    }


def _query_events(session, start_date: dt.date, end_date: dt.date) -> List[StatcastEvent]:
    return (
        session.query(StatcastEvent)
        .filter(
            StatcastEvent.game_date >= start_date,
            StatcastEvent.game_date <= end_date,
            StatcastEvent.batter_id.isnot(None),
        )
        .all()
    )


def _query_batter_events(session, batter_id: int, start_date: dt.date, end_date: dt.date) -> List[StatcastEvent]:
    return (
        session.query(StatcastEvent)
        .filter(
            StatcastEvent.batter_id == batter_id,
            StatcastEvent.game_date >= start_date,
            StatcastEvent.game_date <= end_date,
        )
        .all()
    )


def _existing_batter_aggregate(session, batter_id: int, window: str, end_date: dt.date) -> Optional[BatterAggregate]:
    return (
        session.query(BatterAggregate)
        .filter(
            BatterAggregate.batter_id == batter_id,
            BatterAggregate.window == window,
            BatterAggregate.end_date == end_date,
        )
        .one_or_none()
    )


def _existing_player_split(session, player_id: int, season: int, split: str) -> Optional[PlayerSplit]:
    return (
        session.query(PlayerSplit)
        .filter(
            PlayerSplit.player_id == player_id,
            PlayerSplit.season == season,
            PlayerSplit.split == split,
        )
        .one_or_none()
    )


def _split_for_pitcher_hand(p_throws: Optional[str]) -> Optional[str]:
    if not p_throws:
        return None
    value = str(p_throws).strip().upper()
    if value == "R":
        return "vsR"
    if value == "L":
        return "vsL"
    return None


def _apply_batter_aggregate(row: BatterAggregate, metrics: Dict[str, Any]) -> None:
    row.avg_exit_velocity = metrics.get("avg_exit_velocity")
    row.avg_launch_angle = metrics.get("avg_launch_angle")
    row.hard_hit_pct = metrics.get("hard_hit_pct")
    row.barrel_pct = metrics.get("barrel_pct")
    row.k_pct = metrics.get("k_pct")
    row.bb_pct = metrics.get("bb_pct")
    row.batting_avg = metrics.get("batting_avg")


def _apply_player_split(row: PlayerSplit, metrics: Dict[str, Any]) -> None:
    row.pa = metrics.get("pa")
    row.hits = metrics.get("hits")
    row.doubles = metrics.get("doubles")
    row.triples = metrics.get("triples")
    row.home_runs = metrics.get("home_runs")
    row.walks = metrics.get("walks")
    row.strikeouts = metrics.get("strikeouts")
    row.batting_avg = metrics.get("batting_avg")
    row.on_base_pct = metrics.get("on_base_pct")
    row.slugging_pct = metrics.get("slugging_pct")
    row.iso = metrics.get("iso")
    row.k_pct = metrics.get("k_pct")
    row.bb_pct = metrics.get("bb_pct")


def _record_batter_aggregate_action(
    *,
    session,
    batter_id: int,
    window: str,
    end_date: dt.date,
    metrics: Dict[str, Any],
    only_missing: bool,
    should_write: bool,
    min_pa: int,
    report: Dict[str, Any],
) -> str:
    report["batter_aggregate_candidates"] += 1
    report["batter_aggregate_pa_samples"].append(metrics.get("pa") or 0)

    if (metrics.get("pa") or 0) < min_pa:
        report["batter_aggregates_skipped_low_pa"] += 1
        return "skipped_low_pa"

    existing = _existing_batter_aggregate(session, batter_id, window, end_date)
    if existing and only_missing:
        report["batter_aggregates_skipped_existing"] += 1
        return "skipped_existing"

    if existing:
        report["batter_aggregates_would_update"] += 1
        if should_write:
            _apply_batter_aggregate(existing, metrics)
            report["batter_aggregates_updated"] += 1
        return "would_update" if not should_write else "updated"

    report["batter_aggregates_would_insert"] += 1
    if should_write:
        row = BatterAggregate(batter_id=batter_id, window=window, end_date=end_date)
        _apply_batter_aggregate(row, metrics)
        session.add(row)
        report["batter_aggregates_inserted"] += 1
        return "inserted"

    return "would_insert"


def _record_player_split_action(
    *,
    session,
    player_id: int,
    season: int,
    split: str,
    metrics: Dict[str, Any],
    only_missing: bool,
    should_write: bool,
    min_pa: int,
    report: Dict[str, Any],
) -> str:
    report["player_split_candidates"] += 1
    report["player_split_pa_samples"].append(metrics.get("pa") or 0)

    if (metrics.get("pa") or 0) < min_pa:
        report["player_splits_skipped_low_pa"] += 1
        return "skipped_low_pa"

    existing = _existing_player_split(session, player_id, season, split)
    if existing and only_missing:
        report["player_splits_skipped_existing"] += 1
        return "skipped_existing"

    if existing:
        report["player_splits_would_update"] += 1
        if should_write:
            _apply_player_split(existing, metrics)
            report["player_splits_updated"] += 1
        return "would_update" if not should_write else "updated"

    report["player_splits_would_insert"] += 1
    if should_write:
        row = PlayerSplit(player_id=player_id, season=season, split=split)
        _apply_player_split(row, metrics)
        session.add(row)
        report["player_splits_inserted"] += 1
        return "inserted"

    return "would_insert"


def main() -> None:
    backfill_start = _parse_date(os.getenv("BACKFILL_START") or os.getenv("BACKTEST_START") or "2026-04-20")
    backfill_end = _parse_date(os.getenv("BACKFILL_END") or os.getenv("BACKTEST_END") or "2026-05-03")
    season = backfill_end.year

    player_split_start = _parse_date(os.getenv("PLAYER_SPLIT_START") or f"{season}-03-01")
    player_split_end = _parse_date(os.getenv("PLAYER_SPLIT_END") or backfill_end.isoformat())

    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    only_missing = _env_bool("ONLY_MISSING", True)
    dry_run = _env_bool("DRY_RUN", True)
    apply = _env_bool("APPLY", False)
    should_write = apply and not dry_run

    batter_window = os.getenv("BATTER_AGGREGATE_WINDOW") or os.getenv("WINDOW") or "90d"
    window_days = 90
    if batter_window.endswith("d"):
        try:
            window_days = int(batter_window[:-1])
        except ValueError:
            window_days = 90

    aggregate_start = backfill_end - dt.timedelta(days=window_days)
    min_player_split_pa = int(os.getenv("MIN_PLAYER_SPLIT_PA", "25"))
    min_batter_aggregate_pa = int(os.getenv("MIN_BATTER_AGGREGATE_PA", "10"))

    warnings: List[str] = []
    if player_split_start > dt.date(season, 4, 1):
        warnings.append(
            f"PLAYER_SPLIT_START={player_split_start} is later than April 1. "
            "PlayerSplit is season-level; use season-to-date unless intentionally testing."
        )
    if not should_write:
        warnings.append("DRY RUN ONLY: no database rows will be inserted or updated.")
    if apply and dry_run:
        warnings.append("APPLY=true was set, but DRY_RUN=true prevents writes.")
    if not apply and not dry_run:
        warnings.append("DRY_RUN=false was set, but APPLY=true is required to write.")

    engine = get_engine(database_url)
    create_tables(engine)
    SessionLocal = get_session(engine)
    session = SessionLocal()

    report: Dict[str, Any] = {
        "database_url": database_url,
        "backfill_start": backfill_start.isoformat(),
        "backfill_end": backfill_end.isoformat(),
        "player_split_start": player_split_start.isoformat(),
        "player_split_end": player_split_end.isoformat(),
        "batter_aggregate_start": aggregate_start.isoformat(),
        "batter_aggregate_end": backfill_end.isoformat(),
        "batter_aggregate_window": batter_window,
        "only_missing": only_missing,
        "dry_run": dry_run,
        "apply": apply,
        "should_write": should_write,
        "min_player_split_pa": min_player_split_pa,
        "min_batter_aggregate_pa": min_batter_aggregate_pa,
        "warnings": warnings,
        "distinct_batters_seen": 0,
        "batter_aggregate_candidates": 0,
        "batter_aggregates_would_insert": 0,
        "batter_aggregates_would_update": 0,
        "batter_aggregates_inserted": 0,
        "batter_aggregates_updated": 0,
        "batter_aggregates_skipped_existing": 0,
        "batter_aggregates_skipped_low_pa": 0,
        "player_split_candidates": 0,
        "player_splits_would_insert": 0,
        "player_splits_would_update": 0,
        "player_splits_inserted": 0,
        "player_splits_updated": 0,
        "player_splits_skipped_existing": 0,
        "player_splits_skipped_low_pa": 0,
        "batters_with_no_terminal_pa": 0,
        "splits_with_no_terminal_pa": 0,
        "batter_aggregate_pa_samples": [],
        "player_split_pa_samples": [],
        "batters": [],
    }

    try:
        range_events = _query_events(session, backfill_start, backfill_end)
        batter_ids = sorted({int(event.batter_id) for event in range_events if event.batter_id is not None})
        report["distinct_batters_seen"] = len(batter_ids)

        for batter_id in batter_ids:
            batter_report: Dict[str, Any] = {
                "batter_id": batter_id,
                "batter_aggregate_action": None,
                "batter_aggregate_pa": None,
                "splits": {},
            }

            aggregate_events = _query_batter_events(session, batter_id, aggregate_start, backfill_end)
            aggregate_metrics = _compute_batting_metrics(aggregate_events)
            batter_report["batter_aggregate_pa"] = aggregate_metrics.get("pa")

            if aggregate_metrics["terminal_pa_count"] <= 0:
                report["batters_with_no_terminal_pa"] += 1

            agg_action = _record_batter_aggregate_action(
                session=session,
                batter_id=batter_id,
                window=batter_window,
                end_date=backfill_end,
                metrics=aggregate_metrics,
                only_missing=only_missing,
                should_write=should_write,
                min_pa=min_batter_aggregate_pa,
                report=report,
            )
            batter_report["batter_aggregate_action"] = agg_action

            split_source_events = _query_batter_events(session, batter_id, player_split_start, player_split_end)
            split_events: Dict[str, List[StatcastEvent]] = defaultdict(list)
            for event in split_source_events:
                split = _split_for_pitcher_hand(event.p_throws)
                if split:
                    split_events[split].append(event)

            for split in ("vsR", "vsL"):
                events = split_events.get(split, [])
                metrics = _compute_batting_metrics(events)
                if metrics["terminal_pa_count"] <= 0:
                    report["splits_with_no_terminal_pa"] += 1

                action = _record_player_split_action(
                    session=session,
                    player_id=batter_id,
                    season=season,
                    split=split,
                    metrics=metrics,
                    only_missing=only_missing,
                    should_write=should_write,
                    min_pa=min_player_split_pa,
                    report=report,
                )

                batter_report["splits"][split] = {
                    "event_rows": len(events),
                    "terminal_pa_count": metrics["terminal_pa_count"],
                    "action": action,
                }

            report["batters"].append(batter_report)

        if should_write:
            session.commit()
        else:
            session.rollback()

    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    report["sample_size_distribution"] = {
        "player_split_pa": _distribution(report.pop("player_split_pa_samples")),
        "batter_aggregate_pa": _distribution(report.pop("batter_aggregate_pa_samples")),
    }

    Path("tmp").mkdir(exist_ok=True)
    output_path = Path("tmp") / f"hitter_profile_backfill_{backfill_start}_to_{backfill_end}.json"
    output_path.write_text(json.dumps(report, indent=2, default=str))

    print("=== HITTER PROFILE BACKFILL FROM STATCAST ===")
    print(json.dumps({k: v for k, v in report.items() if k != "batters"}, indent=2, default=str))
    print()
    print(f"Wrote JSON report to {output_path}")


if __name__ == "__main__":
    main()
