from datetime import datetime, timedelta
from typing import Dict, List

from sqlalchemy.orm import Session

from mlb_app.models import StatcastEvent


TERMINAL_EVENTS = {
    "single",
    "double",
    "triple",
    "home_run",
    "strikeout",
    "field_out",
    "force_out",
    "sac_fly",
    "walk",
}

SWING_DESCRIPTIONS = {
    "swinging_strike",
    "swinging_strike_blocked",
    "foul",
    "foul_tip",
    "hit_into_play",
}


def build_batter_pitch_type_summary(
    session: Session,
    batter_id: int,
    pitch_type: str,
    days_back: int = 365,
) -> Dict:
    start_date = datetime.utcnow() - timedelta(days=days_back)

    events: List[StatcastEvent] = (
        session.query(StatcastEvent)
        .filter(
            StatcastEvent.batter_id == batter_id,
            StatcastEvent.pitch_type == pitch_type,
            StatcastEvent.game_date >= start_date,
        )
        .all()
    )

    swings = 0
    whiffs = 0
    strikeouts = 0
    batted_balls = 0
    hard_hits = 0

    ev_values = []
    la_values = []

    terminal_pa = 0

    for e in events:
        desc = getattr(e, "description", None)
        event = getattr(e, "events", None)

        if desc in SWING_DESCRIPTIONS:
            swings += 1

        if desc in {"swinging_strike", "swinging_strike_blocked"}:
            whiffs += 1

        if event == "strikeout":
            strikeouts += 1

        if event in TERMINAL_EVENTS:
            terminal_pa += 1

        launch_speed = getattr(e, "launch_speed", None)
        launch_angle = getattr(e, "launch_angle", None)

        if launch_speed is not None:
            batted_balls += 1
            ev_values.append(launch_speed)

            if launch_speed >= 95:
                hard_hits += 1

        if launch_angle is not None:
            la_values.append(launch_angle)

    avg_ev = sum(ev_values) / len(ev_values) if ev_values else None
    avg_la = sum(la_values) / len(la_values) if la_values else None

    whiff_pct = (whiffs / swings) if swings else None
    k_pct = (strikeouts / terminal_pa) if terminal_pa else None
    hardhit_pct = (hard_hits / batted_balls) if batted_balls else None

    return {
        "batter_id": batter_id,
        "pitch_type": pitch_type,
        "swings": swings,
        "whiffs": whiffs,
        "strikeouts": strikeouts,
        "pa": terminal_pa,
        "avg_ev": avg_ev,
        "avg_la": avg_la,
        "whiff_pct": whiff_pct,
        "k_pct": k_pct,
        "hardhit_pct": hardhit_pct,
    }
