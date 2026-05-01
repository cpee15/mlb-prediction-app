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
        "notes": [
            "V1 uses conservative league-average bullpen priors.",
            "Future versions should aggregate active relievers by team and role.",
            "Use this profile as a stable contract before bullpen simulation integration.",
        ],
    }

    return profile
