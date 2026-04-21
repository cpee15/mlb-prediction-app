"""
Utilities for building pitcher profile summaries for matchup previews.

This module defines a player-level pitcher profile structure that can later
be populated with real calculations from split and Statcast inputs.
"""


def compute_pitcher_profile(raw_stats: dict) -> dict:
    """
    Build a structured pitcher profile from raw pitcher inputs.

    Parameters
    ----------
    raw_stats : dict
        Dictionary of pitcher stats from upstream ingestion or transformed sources.

    Returns
    -------
    dict
        A structured player-level pitcher profile using raw metrics grouped
        by trait. Missing fields are returned as None.
    """
    return {
        "arsenal": {
            "pitch_mix": raw_stats.get("pitch_mix"),
            "avg_velocity": raw_stats.get("avg_velocity"),
            "avg_spin_rate": raw_stats.get("avg_spin_rate"),
        },
        "bat_missing": {
            "k_rate": raw_stats.get("k_rate"),
            "whiff_rate": raw_stats.get("whiff_rate"),
            "csw_rate": raw_stats.get("csw_rate"),
        },
        "command_control": {
            "bb_rate": raw_stats.get("bb_rate"),
            "zone_rate": raw_stats.get("zone_rate"),
            "first_pitch_strike_rate": raw_stats.get("first_pitch_strike_rate"),
        },
        "contact_management": {
            "hard_hit_rate_allowed": raw_stats.get("hard_hit_rate_allowed"),
            "barrel_rate_allowed": raw_stats.get("barrel_rate_allowed"),
            "avg_exit_velocity_allowed": raw_stats.get("avg_exit_velocity_allowed"),
            "avg_launch_angle_allowed": raw_stats.get("avg_launch_angle_allowed"),
        },
        "platoon_profile": {
            "vs_lhb_woba_allowed": raw_stats.get("vs_lhb_woba_allowed"),
            "vs_rhb_woba_allowed": raw_stats.get("vs_rhb_woba_allowed"),
            "vs_lhb_k_rate": raw_stats.get("vs_lhb_k_rate"),
            "vs_rhb_k_rate": raw_stats.get("vs_rhb_k_rate"),
            "vs_lhb_bb_rate": raw_stats.get("vs_lhb_bb_rate"),
            "vs_rhb_bb_rate": raw_stats.get("vs_rhb_bb_rate"),
        },
    }