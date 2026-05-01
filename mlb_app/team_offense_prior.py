"""
Team offense prior profile.

Used when official/projected lineups are unavailable or do not produce enough
usable hitter data. V1 provides a stable conservative profile so PA/game
simulation can still run for future games before lineups are posted.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


TEAM_OFFENSE_PRIOR_V1 = {
    "contact_skill": {
        "k_rate": 0.225,
        "whiff_rate": 0.235,
        "contact_rate": 0.765,
    },
    "plate_discipline": {
        "bb_rate": 0.085,
        "chase_rate": 0.300,
        "swing_rate": 0.470,
    },
    "power": {
        "iso": 0.165,
        "barrel_rate": 0.075,
        "hard_hit_rate": 0.395,
    },
    "batted_ball_quality": {
        "avg_exit_velocity": 88.5,
        "avg_launch_angle": 12.5,
    },
    "platoon_profile": {
        "vs_lhp_woba": 0.315,
        "vs_rhp_woba": 0.315,
        "vs_lhp_iso": 0.160,
        "vs_rhp_iso": 0.160,
    },
}


# Conservative team offense quality layer.
# Positive score = stronger than prior offense.
# Negative score = weaker than prior offense.
# This should later be replaced by live/team aggregate batting data.
TEAM_OFFENSE_QUALITY_V1 = {
    119: {"quality_score": 0.08, "label": "strong_offense"},       # Dodgers
    147: {"quality_score": 0.07, "label": "strong_offense"},       # Yankees
    141: {"quality_score": 0.06, "label": "above_average_offense"},# Blue Jays
    140: {"quality_score": 0.05, "label": "above_average_offense"},# Rangers
    108: {"quality_score": 0.05, "label": "above_average_offense"},# Angels
    116: {"quality_score": 0.04, "label": "above_average_offense"},# Tigers

    115: {"quality_score": -0.04, "label": "below_average_offense"},# Rockies
    146: {"quality_score": -0.04, "label": "below_average_offense"},# Marlins
    120: {"quality_score": -0.04, "label": "below_average_offense"},# Nationals
    133: {"quality_score": -0.03, "label": "slightly_below_average_offense"},# Athletics
}


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _deepcopy_profile(profile: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {
        section: dict(values)
        for section, values in profile.items()
    }


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _apply_team_quality(profile: Dict[str, Dict[str, Any]], quality_score: float) -> None:
    q = _clamp(quality_score, -0.12, 0.12)

    profile["contact_skill"]["k_rate"] = round(_clamp(profile["contact_skill"]["k_rate"] - (q * 0.16), 0.17, 0.30), 3)
    profile["contact_skill"]["whiff_rate"] = round(_clamp(profile["contact_skill"]["whiff_rate"] - (q * 0.14), 0.18, 0.30), 3)
    profile["contact_skill"]["contact_rate"] = round(_clamp(profile["contact_skill"]["contact_rate"] + (q * 0.14), 0.70, 0.82), 3)

    profile["plate_discipline"]["bb_rate"] = round(_clamp(profile["plate_discipline"]["bb_rate"] + (q * 0.08), 0.055, 0.125), 3)
    profile["plate_discipline"]["chase_rate"] = round(_clamp(profile["plate_discipline"]["chase_rate"] - (q * 0.10), 0.24, 0.36), 3)
    profile["plate_discipline"]["swing_rate"] = round(_clamp(profile["plate_discipline"]["swing_rate"] + (q * 0.04), 0.42, 0.52), 3)

    profile["power"]["iso"] = round(_clamp(profile["power"]["iso"] + (q * 0.22), 0.115, 0.230), 3)
    profile["power"]["barrel_rate"] = round(_clamp(profile["power"]["barrel_rate"] + (q * 0.08), 0.045, 0.115), 3)
    profile["power"]["hard_hit_rate"] = round(_clamp(profile["power"]["hard_hit_rate"] + (q * 0.16), 0.32, 0.48), 3)

    profile["batted_ball_quality"]["avg_exit_velocity"] = round(_clamp(profile["batted_ball_quality"]["avg_exit_velocity"] + (q * 5.0), 86.0, 91.5), 1)
    profile["batted_ball_quality"]["avg_launch_angle"] = round(_clamp(profile["batted_ball_quality"]["avg_launch_angle"] + (q * 3.0), 8.0, 17.0), 1)

    profile["platoon_profile"]["vs_lhp_woba"] = round(_clamp(profile["platoon_profile"]["vs_lhp_woba"] + (q * 0.14), 0.275, 0.365), 3)
    profile["platoon_profile"]["vs_rhp_woba"] = round(_clamp(profile["platoon_profile"]["vs_rhp_woba"] + (q * 0.14), 0.275, 0.365), 3)
    profile["platoon_profile"]["vs_lhp_iso"] = round(_clamp(profile["platoon_profile"]["vs_lhp_iso"] + (q * 0.12), 0.115, 0.220), 3)
    profile["platoon_profile"]["vs_rhp_iso"] = round(_clamp(profile["platoon_profile"]["vs_rhp_iso"] + (q * 0.12), 0.115, 0.220), 3)


def build_team_offense_prior(
    team_id: Optional[int] = None,
    team_name: Optional[str] = None,
    raw_context: Optional[dict] = None,
) -> Dict[str, Any]:
    raw_context = raw_context or {}
    profile = _deepcopy_profile(TEAM_OFFENSE_PRIOR_V1)

    team_quality = TEAM_OFFENSE_QUALITY_V1.get(team_id, {})
    quality_score = _safe_float(raw_context.get("quality_score", team_quality.get("quality_score")))
    quality_label = raw_context.get("quality_label", team_quality.get("label", "league_average_offense"))

    if quality_score is not None:
        _apply_team_quality(profile, quality_score)
    else:
        quality_score = 0.0

    profile["metadata"] = {
        "source_type": raw_context.get("source_type", "team_offense_prior_v1"),
        "team_id": team_id,
        "team_name": team_name,
        "data_confidence": raw_context.get("data_confidence", "low"),
        "generated_from": "build_team_offense_prior",
        "profile_granularity": "team_offense_prior",
        "sample_window": raw_context.get("sample_window", "prior"),
        "sample_size": raw_context.get("sample_size"),
        "team_offense_prior_version": "team_offense_prior_v1",
        "team_offense_quality_version": "team_offense_quality_v1",
        "team_offense_quality_score": quality_score,
        "team_offense_quality_label": quality_label,
        "team_quality_adjustment_applied": quality_score != 0.0,
        "notes": [
            "V1 uses conservative team offense priors when lineup data is unavailable.",
            "Future versions should aggregate projected/active hitters and recent team offense data.",
            "Use this profile only as a low-confidence fallback for pre-lineup games.",
        ],
    }

    return profile
