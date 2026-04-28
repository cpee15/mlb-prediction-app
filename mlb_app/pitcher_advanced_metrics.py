"""
Derived pitcher metric helpers from stored StatcastEvent rows.

These helpers intentionally use only fields currently persisted in the
StatcastEvent model. Metrics that require unavailable raw fields, such as
CSW rate from pitch description, remain None until those fields are stored.
"""

from __future__ import annotations

import math
from typing import Any, Dict, Iterable, Optional


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _average(values: Iterable[Optional[float]]) -> Optional[float]:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def _get_field(event: Any, key: str) -> Any:
    if isinstance(event, dict):
        return event.get(key)
    return getattr(event, key, None)


def _is_in_approx_zone(event: Any) -> bool:
    plate_x = _safe_float(_get_field(event, "plate_x"))
    plate_z = _safe_float(_get_field(event, "plate_z"))
    if plate_x is None or plate_z is None:
        return False

    # Approximate rulebook zone. This is intentionally conservative until
    # batter-specific sz_top/sz_bot fields are available.
    return abs(plate_x) <= 0.83 and 1.5 <= plate_z <= 3.5


def _is_first_pitch(event: Any) -> bool:
    return _get_field(event, "balls") == 0 and _get_field(event, "strikes") == 0


def _is_first_pitch_strike(event: Any) -> bool:
    if not _is_first_pitch(event):
        return False

    # Without Statcast description, use zone location as the safest persisted
    # proxy for first-pitch strike. This excludes chase swings for now.
    return _is_in_approx_zone(event)


def _is_barrel_approx(event: Any) -> bool:
    launch_speed = _safe_float(_get_field(event, "launch_speed"))
    launch_angle = _safe_float(_get_field(event, "launch_angle"))
    if launch_speed is None or launch_angle is None:
        return False

    # Lightweight barrel approximation. True Savant barrels use a variable
    # launch-angle band by EV; this captures the high-value core region.
    return launch_speed >= 98 and 26 <= launch_angle <= 30


def _description(event: Any) -> Optional[str]:
    value = _get_field(event, "description")
    if value is None:
        return None
    value = str(value).strip().lower()
    return value or None


def _is_csw(event: Any) -> bool:
    return _description(event) in {
        "called_strike",
        "swinging_strike",
        "swinging_strike_blocked",
    }


def _event_name(event: Any) -> Optional[str]:
    value = _get_field(event, "events")
    if value is None:
        return None
    value = str(value).strip().lower()
    return value or None


TERMINAL_PA_EVENTS = {
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
}


def _batter_stand(event: Any) -> Optional[str]:
    value = _get_field(event, "stand")
    if value is None:
        return None
    value = str(value).strip().upper()
    return value if value in {"L", "R"} else None


def _platoon_summary(rows: Iterable[Any], stand: str) -> Dict[str, Optional[float]]:
    stand_rows = [row for row in rows if _batter_stand(row) == stand]
    terminal_rows = [
        row for row in stand_rows
        if _event_name(row) in TERMINAL_PA_EVENTS
    ]
    xwoba_rows = [
        row for row in stand_rows
        if _safe_float(_get_field(row, "estimated_woba_using_speedangle")) is not None
    ]

    pa = len(terminal_rows)
    k_rate = (
        sum(1 for row in terminal_rows if _event_name(row) in {"strikeout", "strikeout_double_play"}) / pa
        if pa else None
    )
    bb_rate = (
        sum(1 for row in terminal_rows if _event_name(row) == "walk") / pa
        if pa else None
    )
    xwoba_allowed = _average(
        _safe_float(_get_field(row, "estimated_woba_using_speedangle")) for row in xwoba_rows
    )

    return {
        "rows": len(stand_rows),
        "pa": pa,
        "xwoba_allowed": xwoba_allowed,
        "k_rate": k_rate,
        "bb_rate": bb_rate,
    }


def derive_pitcher_advanced_metrics(events: Iterable[Any]) -> Dict[str, Optional[float]]:
    """
    Compute pitcher advanced metrics from stored StatcastEvent rows.

    Returns rates as decimals, matching the existing profile schema.
    """
    rows = list(events or [])
    if not rows:
        return {
            "csw_rate": None,
            "zone_rate": None,
            "first_pitch_strike_rate": None,
            "barrel_rate_allowed": None,
            "avg_exit_velocity_allowed": None,
            "avg_launch_angle_allowed": None,
            "vs_lhb_woba_allowed": None,
            "vs_rhb_woba_allowed": None,
            "vs_lhb_k_rate": None,
            "vs_rhb_k_rate": None,
            "vs_lhb_bb_rate": None,
            "vs_rhb_bb_rate": None,
            "_debug": {
                "advanced_event_rows_used": 0,
                "advanced_zone_rows_used": 0,
                "advanced_first_pitch_rows_used": 0,
                "advanced_batted_ball_rows_used": 0,
                "advanced_metrics_available": [],
            },
        }

    description_rows = [row for row in rows if _description(row) is not None]
    plate_appearance_rows = [
        row for row in rows
        if _event_name(row) in TERMINAL_PA_EVENTS
    ]
    xba_rows = [
        row for row in rows
        if _safe_float(_get_field(row, "estimated_ba_using_speedangle")) is not None
    ]
    zone_known = [
        row for row in rows
        if _safe_float(_get_field(row, "plate_x")) is not None
        and _safe_float(_get_field(row, "plate_z")) is not None
    ]
    first_pitch_rows = [row for row in rows if _is_first_pitch(row)]
    batted_ball_rows = [
        row for row in rows
        if _safe_float(_get_field(row, "launch_speed")) is not None
        or _safe_float(_get_field(row, "launch_angle")) is not None
    ]

    csw_rate = (
        sum(1 for row in description_rows if _is_csw(row)) / len(description_rows)
        if description_rows else None
    )
    bb_rate = (
        sum(1 for row in plate_appearance_rows if _event_name(row) == "walk") / len(plate_appearance_rows)
        if plate_appearance_rows else None
    )
    xba_allowed = _average(
        _safe_float(_get_field(row, "estimated_ba_using_speedangle")) for row in xba_rows
    )
    zone_rate = (
        sum(1 for row in zone_known if _is_in_approx_zone(row)) / len(zone_known)
        if zone_known else None
    )
    first_pitch_strike_rate = (
        sum(1 for row in first_pitch_rows if _is_first_pitch_strike(row)) / len(first_pitch_rows)
        if first_pitch_rows else None
    )
    barrel_rate_allowed = (
        sum(1 for row in batted_ball_rows if _is_barrel_approx(row)) / len(batted_ball_rows)
        if batted_ball_rows else None
    )

    vs_lhb = _platoon_summary(rows, "L")
    vs_rhb = _platoon_summary(rows, "R")

    metrics = {
        "csw_rate": csw_rate,
        "bb_rate": bb_rate,
        "zone_rate": zone_rate,
        "first_pitch_strike_rate": first_pitch_strike_rate,
        "barrel_rate_allowed": barrel_rate_allowed,
        "avg_exit_velocity_allowed": _average(
            _safe_float(_get_field(row, "launch_speed")) for row in batted_ball_rows
        ),
        "avg_launch_angle_allowed": _average(
            _safe_float(_get_field(row, "launch_angle")) for row in batted_ball_rows
        ),
        "xba_allowed": xba_allowed,
        "vs_lhb_woba_allowed": vs_lhb["xwoba_allowed"],
        "vs_rhb_woba_allowed": vs_rhb["xwoba_allowed"],
        "vs_lhb_k_rate": vs_lhb["k_rate"],
        "vs_rhb_k_rate": vs_rhb["k_rate"],
        "vs_lhb_bb_rate": vs_lhb["bb_rate"],
        "vs_rhb_bb_rate": vs_rhb["bb_rate"],
    }

    metrics["_debug"] = {
        "advanced_event_rows_used": len(rows),
        "advanced_description_rows_used": len(description_rows),
        "advanced_zone_rows_used": len(zone_known),
        "advanced_first_pitch_rows_used": len(first_pitch_rows),
        "advanced_batted_ball_rows_used": len(batted_ball_rows),
        "advanced_lhb_rows_used": vs_lhb["rows"],
        "advanced_rhb_rows_used": vs_rhb["rows"],
        "advanced_lhb_pa_used": vs_lhb["pa"],
        "advanced_rhb_pa_used": vs_rhb["pa"],
        "advanced_metrics_available": sorted(
            [key for key, value in metrics.items() if not key.startswith("_") and value is not None]
        ),
    }

    return metrics
