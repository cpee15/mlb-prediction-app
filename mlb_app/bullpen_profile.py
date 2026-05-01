"""
Team bullpen profile foundation.

V1 creates a stable bullpen profile contract that mirrors starter pitcher
profile sections. Later branches can enrich these priors with actual reliever
aggregates, recent workload, handedness mix, and leverage availability.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


BULLPEN_PRIOR_V1 = {
    "bat_missing": {
        "k_rate": 0.235,
        "whiff_rate": 0.255,
        "csw_rate": 0.285,
    },
    "command_control": {
        "bb_rate": 0.090,
        "zone_rate": 0.485,
        "first_pitch_strike_rate": 0.600,
    },
    "contact_management": {
        "hard_hit_rate_allowed": 0.390,
        "barrel_rate_allowed": 0.075,
        "avg_exit_velocity_allowed": 88.5,
        "avg_launch_angle_allowed": 12.0,
        "xwoba_allowed": 0.320,
        "xba_allowed": 0.245,
    },
    "platoon_profile": {
        "vs_lhb_woba_allowed": 0.320,
        "vs_rhb_woba_allowed": 0.320,
        "vs_lhb_iso_allowed": 0.155,
        "vs_rhb_iso_allowed": 0.155,
    },
    "arsenal": {
        "pitch_mix": "Bullpen prior mix",
        "avg_velocity": 94.0,
        "avg_spin_rate": 2350.0,
    },
}

# V1 team-quality layer.
#
# These are conservative profile adjustments, not final measured bullpen stats.
# Positive quality_score means better-than-prior bullpen run prevention.
# Negative quality_score means weaker-than-prior bullpen run prevention.
#
# Future versions should replace this with active-reliever aggregation.
TEAM_BULLPEN_QUALITY_V1 = {
    # Conservative v1 bullpen priors for all MLB teams.
    # Positive quality_score means better-than-prior bullpen run prevention.
    # Negative quality_score means weaker-than-prior bullpen run prevention.
    #
    # These are intentionally modest and should be replaced later with
    # active-reliever aggregation, recent workload, and role availability.

    # Strong / above-average bullpen priors
    119: {"quality_score": 0.08, "label": "strong_bullpen"},          # Los Angeles Dodgers
    147: {"quality_score": 0.07, "label": "strong_bullpen"},          # New York Yankees
    139: {"quality_score": 0.06, "label": "above_average_bullpen"},   # Tampa Bay Rays
    114: {"quality_score": 0.05, "label": "above_average_bullpen"},   # Cleveland Guardians
    117: {"quality_score": 0.04, "label": "above_average_bullpen"},   # Houston Astros
    121: {"quality_score": 0.04, "label": "above_average_bullpen"},   # New York Mets
    138: {"quality_score": 0.04, "label": "above_average_bullpen"},   # St. Louis Cardinals
    158: {"quality_score": 0.03, "label": "slightly_above_average_bullpen"}, # Milwaukee Brewers
    135: {"quality_score": 0.03, "label": "slightly_above_average_bullpen"}, # San Diego Padres
    142: {"quality_score": 0.03, "label": "slightly_above_average_bullpen"}, # Minnesota Twins
    145: {"quality_score": 0.02, "label": "slightly_above_average_bullpen"}, # Chicago White Sox
    143: {"quality_score": 0.02, "label": "slightly_above_average_bullpen"}, # Philadelphia Phillies
    110: {"quality_score": 0.02, "label": "slightly_above_average_bullpen"}, # Baltimore Orioles

    # Near-average bullpen priors
    140: {"quality_score": 0.01, "label": "near_average_bullpen"},    # Texas Rangers
    133: {"quality_score": 0.01, "label": "near_average_bullpen"},    # Athletics
    116: {"quality_score": 0.00, "label": "league_average_bullpen"},  # Detroit Tigers
    141: {"quality_score": 0.00, "label": "league_average_bullpen"},  # Toronto Blue Jays
    108: {"quality_score": 0.00, "label": "league_average_bullpen"},  # Los Angeles Angels
    144: {"quality_score": 0.00, "label": "league_average_bullpen"},  # Atlanta Braves
    137: {"quality_score": -0.01, "label": "near_average_bullpen"},   # San Francisco Giants
    112: {"quality_score": -0.01, "label": "near_average_bullpen"},   # Chicago Cubs

    # Below-average bullpen priors
    109: {"quality_score": -0.02, "label": "slightly_below_average_bullpen"}, # Arizona Diamondbacks
    136: {"quality_score": -0.02, "label": "slightly_below_average_bullpen"}, # Seattle Mariners
    118: {"quality_score": -0.03, "label": "slightly_below_average_bullpen"}, # Kansas City Royals
    113: {"quality_score": -0.03, "label": "slightly_below_average_bullpen"}, # Cincinnati Reds
    134: {"quality_score": -0.03, "label": "slightly_below_average_bullpen"}, # Pittsburgh Pirates
    111: {"quality_score": -0.04, "label": "below_average_bullpen"},  # Boston Red Sox
    120: {"quality_score": -0.04, "label": "below_average_bullpen"},  # Washington Nationals
    146: {"quality_score": -0.04, "label": "below_average_bullpen"},  # Miami Marlins
    115: {"quality_score": -0.05, "label": "below_average_bullpen"},  # Colorado Rockies
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
    """
    Apply conservative team bullpen quality adjustments in-place.

    Better bullpens:
    - increase K/whiff/CSW
    - reduce BB
    - improve zone/FPS
    - reduce hard contact/xwOBA/xBA
    """
    q = _clamp(quality_score, -0.12, 0.12)

    profile["bat_missing"]["k_rate"] = round(_clamp(profile["bat_missing"]["k_rate"] + (q * 0.20), 0.16, 0.32), 3)
    profile["bat_missing"]["whiff_rate"] = round(_clamp(profile["bat_missing"]["whiff_rate"] + (q * 0.18), 0.18, 0.34), 3)
    profile["bat_missing"]["csw_rate"] = round(_clamp(profile["bat_missing"]["csw_rate"] + (q * 0.12), 0.23, 0.34), 3)

    profile["command_control"]["bb_rate"] = round(_clamp(profile["command_control"]["bb_rate"] - (q * 0.10), 0.055, 0.13), 3)
    profile["command_control"]["zone_rate"] = round(_clamp(profile["command_control"]["zone_rate"] + (q * 0.08), 0.43, 0.53), 3)
    profile["command_control"]["first_pitch_strike_rate"] = round(_clamp(profile["command_control"]["first_pitch_strike_rate"] + (q * 0.10), 0.54, 0.66), 3)

    profile["contact_management"]["hard_hit_rate_allowed"] = round(_clamp(profile["contact_management"]["hard_hit_rate_allowed"] - (q * 0.18), 0.31, 0.47), 3)
    profile["contact_management"]["barrel_rate_allowed"] = round(_clamp(profile["contact_management"]["barrel_rate_allowed"] - (q * 0.05), 0.045, 0.105), 3)
    profile["contact_management"]["xwoba_allowed"] = round(_clamp(profile["contact_management"]["xwoba_allowed"] - (q * 0.15), 0.275, 0.365), 3)
    profile["contact_management"]["xba_allowed"] = round(_clamp(profile["contact_management"]["xba_allowed"] - (q * 0.10), 0.215, 0.285), 3)

    profile["platoon_profile"]["vs_lhb_woba_allowed"] = round(_clamp(profile["platoon_profile"]["vs_lhb_woba_allowed"] - (q * 0.13), 0.28, 0.36), 3)
    profile["platoon_profile"]["vs_rhb_woba_allowed"] = round(_clamp(profile["platoon_profile"]["vs_rhb_woba_allowed"] - (q * 0.13), 0.28, 0.36), 3)
    profile["platoon_profile"]["vs_lhb_iso_allowed"] = round(_clamp(profile["platoon_profile"]["vs_lhb_iso_allowed"] - (q * 0.08), 0.12, 0.20), 3)
    profile["platoon_profile"]["vs_rhb_iso_allowed"] = round(_clamp(profile["platoon_profile"]["vs_rhb_iso_allowed"] - (q * 0.08), 0.12, 0.20), 3)

    profile["arsenal"]["avg_velocity"] = round(_clamp(profile["arsenal"]["avg_velocity"] + (q * 6.0), 91.0, 97.5), 1)
    profile["arsenal"]["avg_spin_rate"] = round(_clamp(profile["arsenal"]["avg_spin_rate"] + (q * 250.0), 2150.0, 2550.0), 0)


def build_bullpen_profile(
    team_id: Optional[int] = None,
    team_name: Optional[str] = None,
    raw_context: Optional[dict] = None,
) -> Dict[str, Any]:
    """
    Build a team bullpen profile.

    V1 intentionally returns conservative priors with metadata so downstream
    simulation can rely on a stable shape before real reliever aggregation is
    added.
    """
    raw_context = raw_context or {}
    profile = _deepcopy_profile(BULLPEN_PRIOR_V1)

    team_quality = TEAM_BULLPEN_QUALITY_V1.get(team_id, {})
    quality_score = _safe_float(raw_context.get("quality_score", team_quality.get("quality_score")))
    quality_label = raw_context.get("quality_label", team_quality.get("label", "league_average_bullpen"))

    if quality_score is not None:
        _apply_team_quality(profile, quality_score)
    else:
        quality_score = 0.0

    # Allow explicit overrides when future callers have already-computed team
    # bullpen values. This keeps v1 extensible without changing the response
    # contract later.
    overrides = raw_context.get("bullpen_profile_overrides") or {}
    for section, values in overrides.items():
        if section not in profile or not isinstance(values, dict):
            continue
        for key, value in values.items():
            if key in profile[section] and value is not None:
                profile[section][key] = value

    profile["metadata"] = {
        "source_type": raw_context.get("source_type", "bullpen_prior_v1"),
        "team_id": team_id,
        "team_name": team_name,
        "data_confidence": raw_context.get("data_confidence", "low"),
        "generated_from": "build_bullpen_profile",
        "profile_granularity": "team_bullpen",
        "sample_window": raw_context.get("sample_window", "prior"),
        "sample_size": raw_context.get("sample_size"),
        "bullpen_profile_version": "bullpen_profile_v1",
        "bullpen_quality_version": "team_bullpen_quality_v1",
        "bullpen_quality_score": quality_score,
        "bullpen_quality_label": quality_label,
        "team_quality_adjustment_applied": quality_score != 0.0,
        "notes": [
            "V1 uses conservative league-average bullpen priors.",
            "Future versions should aggregate active relievers by team and role.",
            "Use this profile as a stable contract before bullpen simulation integration.",
        ],
    }

    return profile
