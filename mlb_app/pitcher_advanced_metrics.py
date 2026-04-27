"""
Derived pitcher metric helpers from stored StatcastEvent rows.

These helpers intentionally use only fields currently persisted in the
StatcastEvent model. Metrics that require unavailable raw fields, such as
CSW rate from pitch description, remain None until those fields are stored.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _average(values: Iterable[Optional[float]]) -> Optional[float]:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def _is_in_approx_zone(event: Any) -> bool:
    plate_x = _safe_float(getattr(event, "plate_x", None))
    plate_z = _safe_float(getattr(event, "plate_z", None))
    if plate_x is None or plate_z is None:
        return False

    # Approximate rulebook zone. This is intentionally conservative until
    # batter-specific sz_top/sz_bot fields are available.
    return abs(plate_x) <= 0.83 and 1.5 <= plate_z <= 3.5


def _is_first_pitch(event: Any) -> bool:
    return getattr(event, "balls", None) == 0 and getattr(event, "strikes", None) == 0


def _is_first_pitch_strike(event: Any) -> bool:
    if not _is_first_pitch(event):
        return False

    # Without Statcast description, use zone location as the safest persisted
    # proxy for first-pitch strike. This excludes chase swings for now.
    return _is_in_approx_zone(event)


def _is_barrel_approx(event: Any) -> bool:
    launch_speed = _safe_float(getattr(event, "launch_speed", None))
    launch_angle = _safe_float(getattr(event, "launch_angle", None))
    if launch_speed is None or launch_angle is None:
        return False

    # Lightweight barrel approximation. True Savant barrels use a variable
    # launch-angle band by EV; this captures the high-value core region.
    return launch_speed >= 98 and 26 <= launch_angle <= 30


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
            "_debug": {
                "advanced_event_rows_used": 0,
                "advanced_zone_rows_used": 0,
                "advanced_first_pitch_rows_used": 0,
                "advanced_batted_ball_rows_used": 0,
                "advanced_metrics_available": [],
            },
        }

    zone_known = [
        row for row in rows
        if _safe_float(getattr(row, "plate_x", None)) is not None
        and _safe_float(getattr(row, "plate_z", None)) is not None
    ]
    first_pitch_rows = [row for row in rows if _is_first_pitch(row)]
    batted_ball_rows = [
        row for row in rows
        if _safe_float(getattr(row, "launch_speed", None)) is not None
        or _safe_float(getattr(row, "launch_angle", None)) is not None
    ]

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

    metrics = {
        # Requires Statcast description; not persisted yet.
        "csw_rate": None,
        "zone_rate": zone_rate,
        "first_pitch_strike_rate": first_pitch_strike_rate,
        "barrel_rate_allowed": barrel_rate_allowed,
        "avg_exit_velocity_allowed": _average(
            _safe_float(getattr(row, "launch_speed", None)) for row in batted_ball_rows
        ),
        "avg_launch_angle_allowed": _average(
            _safe_float(getattr(row, "launch_angle", None)) for row in batted_ball_rows
        ),
    }

    metrics["_debug"] = {
        "advanced_event_rows_used": len(rows),
        "advanced_zone_rows_used": len(zone_known),
        "advanced_first_pitch_rows_used": len(first_pitch_rows),
        "advanced_batted_ball_rows_used": len(batted_ball_rows),
        "advanced_metrics_available": sorted(
            [key for key, value in metrics.items() if not key.startswith("_") and value is not None]
        ),
    }

    return metrics
