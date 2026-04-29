"""Hitter-centered Statcast aggregation for the restored hittingMatchups layer.

This module rebuilds the core hitter-vs-pitch-type metrics from the old
hittingMatchups workbook layer. It is intentionally read-only for now: it
summarizes existing StatcastEvent rows without changing API routes or UI code.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

from mlb_app.database import StatcastEvent


HIT_EVENTS = {"single", "double", "triple", "home_run"}

TERMINAL_EVENTS = {
    "single",
    "double",
    "triple",
    "home_run",
    "strikeout",
    "strikeout_double_play",
    "field_out",
    "force_out",
    "sac_fly",
    "sac_bunt",
    "walk",
    "intent_walk",
    "hit_by_pitch",
    "double_play",
    "grounded_into_double_play",
    "fielders_choice",
    "fielders_choice_out",
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

SWING_DESCRIPTIONS = {
    "swinging_strike",
    "swinging_strike_blocked",
    "foul",
    "foul_tip",
    "foul_bunt",
    "missed_bunt",
    "hit_into_play",
    "hit_into_play_no_out",
    "hit_into_play_score",
}

WHIFF_DESCRIPTIONS = {
    "swinging_strike",
    "swinging_strike_blocked",
    "foul_tip",
    "missed_bunt",
}

IN_PLAY_DESCRIPTIONS = {
    "hit_into_play",
    "hit_into_play_no_out",
    "hit_into_play_score",
}


def _rate(numerator: int, denominator: int, digits: int = 4) -> Optional[float]:
    if not denominator:
        return None
    return round(numerator / denominator, digits)


def _avg(values: Iterable[Optional[float]], digits: int = 3) -> Optional[float]:
    nums = [value for value in values if value is not None]
    if not nums:
        return None
    return round(sum(nums) / len(nums), digits)


def _event_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return None


def _dedupe_events(events: List[StatcastEvent]) -> List[StatcastEvent]:
    """Collapse duplicate Statcast rows before aggregation."""
    seen = set()
    deduped: List[StatcastEvent] = []

    for event in events:
        if (
            getattr(event, "game_pk", None) is not None
            and getattr(event, "at_bat_number", None) is not None
            and getattr(event, "pitch_number", None) is not None
        ):
            key = (
                event.game_pk,
                event.at_bat_number,
                event.pitch_number,
                event.pitcher_id,
                event.batter_id,
                event.pitch_type,
            )
        else:
            key = (
                event.game_date,
                event.pitcher_id,
                event.batter_id,
                event.pitch_type,
                event.events,
                event.release_speed,
                event.release_spin_rate,
                event.launch_speed,
                event.launch_angle,
                event.balls,
                event.strikes,
            )

        if key in seen:
            continue
        seen.add(key)
        deduped.append(event)

    return deduped


def build_batter_pitch_type_summary(
    session: Session,
    batter_id: int,
    pitch_type: str,
    days_back: int = 365,
) -> Dict[str, Any]:
    """Build one hittingMatchups-style row for a batter and pitch type.

    Mirrors the old workbook concepts:
    F-O: pitch_type, Swings, Whiffs, Strikeouts, PutAwaySwings,
         TwoStrikePitches, xwOBA, xBA, Avg EV, Avg LA
    AA-AD: Whiff%, K%, PutAway%, HardHit%
    """
    start_date = (datetime.utcnow() - timedelta(days=days_back)).date()

    raw_events: List[StatcastEvent] = (
        session.query(StatcastEvent)
        .filter(
            StatcastEvent.batter_id == batter_id,
            StatcastEvent.pitch_type == pitch_type,
            StatcastEvent.game_date >= start_date,
        )
        .all()
    )

    events = _dedupe_events(raw_events)

    swings = 0
    whiffs = 0
    strikeouts = 0
    putaway_swings = 0
    two_strike_pitches = 0
    terminal_pa = 0
    official_ab = 0
    hits = 0
    batted_balls = 0
    hard_hits = 0

    ev_values: List[float] = []
    la_values: List[float] = []
    xwoba_values: List[float] = []
    xba_values: List[float] = []

    for event in events:
        description = getattr(event, "description", None)
        event_name = getattr(event, "events", None)
        strikes = getattr(event, "strikes", None)

        if description in SWING_DESCRIPTIONS:
            swings += 1

        if description in WHIFF_DESCRIPTIONS:
            whiffs += 1

        if strikes == 2:
            two_strike_pitches += 1

        if event_name in TERMINAL_EVENTS:
            terminal_pa += 1
            if event_name not in NON_AB_EVENTS:
                official_ab += 1

        if event_name in HIT_EVENTS:
            hits += 1

        if event_name in {"strikeout", "strikeout_double_play"}:
            strikeouts += 1
            if strikes == 2:
                putaway_swings += 1

        launch_speed = getattr(event, "launch_speed", None)
        launch_angle = getattr(event, "launch_angle", None)

        if launch_speed is not None:
            batted_balls += 1
            ev_values.append(float(launch_speed))
            if launch_speed >= 95:
                hard_hits += 1

        if launch_angle is not None:
            la_values.append(float(launch_angle))

        xwoba = getattr(event, "estimated_woba_using_speedangle", None)
        if xwoba is None:
            xwoba = getattr(event, "estimated_woba_using_speed_angle", None)
        if xwoba is not None:
            xwoba_values.append(float(xwoba))

        xba = getattr(event, "estimated_ba_using_speedangle", None)
        if xba is None:
            xba = getattr(event, "estimated_ba_using_speed_angle", None)
        if xba is not None:
            xba_values.append(float(xba))

    dates = [_event_date(getattr(event, "game_date", None)) for event in events]
    dates = [d for d in dates if d is not None]

    return {
        "batter_id": batter_id,
        "pitch_type": pitch_type,
        "date_start": start_date.isoformat(),
        "date_end": max(dates).isoformat() if dates else None,
        "raw_rows": len(raw_events),
        "deduped_rows": len(events),
        "duplicate_rows_removed": max(len(raw_events) - len(events), 0),
        "pitches_seen": len(events),
        "swings": swings,
        "whiffs": whiffs,
        "strikeouts": strikeouts,
        "putaway_swings": putaway_swings,
        "two_strike_pitches": two_strike_pitches,
        "pa": terminal_pa,
        "pa_ended": terminal_pa,
        "ab": official_ab,
        "hits": hits,
        "batting_avg": _rate(hits, official_ab, 3),
        "xwoba": _avg(xwoba_values, 3),
        "xba": _avg(xba_values, 3),
        "avg_ev": _avg(ev_values, 1),
        "avg_exit_velocity": _avg(ev_values, 1),
        "avg_la": _avg(la_values, 1),
        "avg_launch_angle": _avg(la_values, 1),
        "batted_ball_count": batted_balls,
        "hard_hit_count": hard_hits,
        "whiff_pct": _rate(whiffs, swings),
        "k_pct": _rate(strikeouts, terminal_pa),
        "putaway_pct": _rate(putaway_swings, two_strike_pitches),
        "hardhit_pct": _rate(hard_hits, batted_balls),
        "hard_hit_pct": _rate(hard_hits, batted_balls),
    }


def build_batter_pitch_type_summaries(
    session: Session,
    batter_id: int,
    pitch_types: Iterable[str],
    days_back: int = 365,
) -> List[Dict[str, Any]]:
    return [
        build_batter_pitch_type_summary(session, batter_id, pitch_type, days_back)
        for pitch_type in pitch_types
        if pitch_type
    ]
