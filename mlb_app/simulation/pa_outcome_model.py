"""
Plate appearance outcome probability model.

This module converts hitter skill, pitcher skill, and environment context into
a normalized plate appearance outcome distribution.

The model is intentionally conservative for v1:
- starts from MLB-like baseline outcome rates
- blends batter and pitcher skill signals
- applies environment adjustments by outcome type
- normalizes probabilities so they sum to 1
"""

from __future__ import annotations

from typing import Any, Dict, Optional


BASE_PA_OUTCOMES = {
    "k": 0.225,
    "bb": 0.085,
    "hbp": 0.011,
    "single": 0.145,
    "double": 0.045,
    "triple": 0.004,
    "hr": 0.030,
    "reached_on_error": 0.007,
    "out": 0.459,
}

MIN_OUTCOME = {
    "k": 0.08,
    "bb": 0.03,
    "hbp": 0.002,
    "single": 0.06,
    "double": 0.015,
    "triple": 0.0005,
    "hr": 0.005,
    "reached_on_error": 0.001,
    "out": 0.25,
}

MAX_OUTCOME = {
    "k": 0.42,
    "bb": 0.18,
    "hbp": 0.025,
    "single": 0.24,
    "double": 0.09,
    "triple": 0.018,
    "hr": 0.085,
    "reached_on_error": 0.018,
    "out": 0.68,
}


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _nested(profile: Optional[dict], section: str, key: str) -> Optional[float]:
    if not profile:
        return None
    return _safe_float((profile.get(section) or {}).get(key))


def _blend(values, fallback: float) -> float:
    cleaned = [_safe_float(v) for v in values]
    cleaned = [v for v in cleaned if v is not None]
    if not cleaned:
        return fallback
    return sum(cleaned) / len(cleaned)


def _normalize(probs: Dict[str, float]) -> Dict[str, float]:
    bounded = {
        key: _clamp(float(value), MIN_OUTCOME[key], MAX_OUTCOME[key])
        for key, value in probs.items()
    }
    total = sum(bounded.values())
    if total <= 0:
        return dict(BASE_PA_OUTCOMES)
    return {key: round(value / total, 4) for key, value in bounded.items()}


def _env_index(environment_profile: Optional[dict], key: str, fallback: float = 1.0) -> float:
    if not environment_profile:
        return fallback
    value = _safe_float((environment_profile.get("run_environment") or {}).get(key))
    return value if value is not None else fallback


def build_pa_outcome_probabilities(
    batter_profile: Optional[dict],
    pitcher_profile: Optional[dict],
    environment_profile: Optional[dict],
) -> Dict[str, Any]:
    """
    Build a normalized plate appearance outcome distribution.

    Returns a dict containing:
    - probabilities: normalized outcome probabilities
    - inputs_used: key model inputs for debugging
    - model_version: semantic version for future calibration/backtesting
    """
    batter_k = _nested(batter_profile, "contact_skill", "k_rate")
    batter_bb = _nested(batter_profile, "plate_discipline", "bb_rate")
    batter_iso = _nested(batter_profile, "power", "iso")
    batter_barrel = _nested(batter_profile, "power", "barrel_rate")
    batter_hard_hit = _nested(batter_profile, "power", "hard_hit_rate")
    batter_contact = _nested(batter_profile, "contact_skill", "contact_rate")

    pitcher_k = _nested(pitcher_profile, "bat_missing", "k_rate")
    pitcher_bb = _nested(pitcher_profile, "command_control", "bb_rate")
    pitcher_barrel_allowed = _nested(pitcher_profile, "contact_management", "barrel_rate_allowed")
    pitcher_hard_hit_allowed = _nested(pitcher_profile, "contact_management", "hard_hit_rate_allowed")
    pitcher_xba_allowed = _nested(pitcher_profile, "contact_management", "xba_allowed")

    hr_index = _env_index(environment_profile, "hr_boost_index")
    hit_index = _env_index(environment_profile, "hit_boost_index")
    run_index = _env_index(environment_profile, "run_scoring_index")

    k_rate = _blend([batter_k, pitcher_k], BASE_PA_OUTCOMES["k"])
    bb_rate = _blend([batter_bb, pitcher_bb], BASE_PA_OUTCOMES["bb"])

    # HBP and reached-on-error are included as conservative baseline events in v1.
    # They can be upgraded later with player/team/defense-specific features.
    hbp_rate = BASE_PA_OUTCOMES["hbp"]
    reached_on_error_rate = BASE_PA_OUTCOMES["reached_on_error"]

    contact_quality = _blend(
        [
            batter_hard_hit,
            pitcher_hard_hit_allowed,
            batter_contact,
        ],
        0.35,
    )

    power_signal = _blend(
        [
            batter_iso,
            batter_barrel,
            pitcher_barrel_allowed,
        ],
        0.12,
    )

    hit_skill = _blend(
        [
            batter_contact,
            pitcher_xba_allowed,
            contact_quality,
        ],
        0.30,
    )

    # Keep v1 conservative: environment indices are already calibrated, so apply
    # only partial elasticity to avoid over-adjusting PA outcomes.
    env_hr_multiplier = 1.0 + ((hr_index - 1.0) * 0.85)
    env_hit_multiplier = 1.0 + ((hit_index - 1.0) * 0.60)
    env_run_multiplier = 1.0 + ((run_index - 1.0) * 0.35)

    hr_rate = BASE_PA_OUTCOMES["hr"] * (1.0 + (power_signal - 0.12) * 1.8) * env_hr_multiplier
    double_rate = BASE_PA_OUTCOMES["double"] * (1.0 + (power_signal - 0.12) * 0.9) * env_hit_multiplier
    triple_rate = BASE_PA_OUTCOMES["triple"] * env_hit_multiplier
    single_rate = BASE_PA_OUTCOMES["single"] * (1.0 + (hit_skill - 0.30) * 0.75) * env_hit_multiplier

    # Apply broad scoring environment lightly to non-HR hit outcomes.
    single_rate *= env_run_multiplier
    double_rate *= env_run_multiplier
    triple_rate *= env_run_multiplier

    out_rate = 1.0 - (
        k_rate
        + bb_rate
        + hbp_rate
        + single_rate
        + double_rate
        + triple_rate
        + hr_rate
        + reached_on_error_rate
    )

    probabilities = _normalize({
        "k": k_rate,
        "bb": bb_rate,
        "hbp": hbp_rate,
        "single": single_rate,
        "double": double_rate,
        "triple": triple_rate,
        "hr": hr_rate,
        "reached_on_error": reached_on_error_rate,
        "out": out_rate,
    })

    summary = {
        "hit_probability": round(
            probabilities["single"] + probabilities["double"] + probabilities["triple"] + probabilities["hr"],
            4,
        ),
        "on_base_probability": round(
            probabilities["bb"]
            + probabilities["hbp"]
            + probabilities["single"]
            + probabilities["double"]
            + probabilities["triple"]
            + probabilities["hr"]
            + probabilities["reached_on_error"],
            4,
        ),
        "extra_base_hit_probability": round(
            probabilities["double"] + probabilities["triple"] + probabilities["hr"],
            4,
        ),
        "total_out_probability": round(
            probabilities["k"] + probabilities["out"],
            4,
        ),
        "non_hit_on_base_probability": round(
            probabilities["bb"] + probabilities["hbp"] + probabilities["reached_on_error"],
            4,
        ),
        "contact_out_probability": probabilities["out"],
    }

    return {
        "model_version": "pa_outcome_v1",
        "probabilities": probabilities,
        "summary": summary,
        "inputs_used": {
            "batter_k_rate": batter_k,
            "batter_bb_rate": batter_bb,
            "batter_iso": batter_iso,
            "batter_barrel_rate": batter_barrel,
            "batter_hard_hit_rate": batter_hard_hit,
            "batter_contact_rate": batter_contact,
            "pitcher_k_rate": pitcher_k,
            "pitcher_bb_rate": pitcher_bb,
            "pitcher_barrel_rate_allowed": pitcher_barrel_allowed,
            "pitcher_hard_hit_rate_allowed": pitcher_hard_hit_allowed,
            "pitcher_xba_allowed": pitcher_xba_allowed,
            "hr_boost_index": hr_index,
            "hit_boost_index": hit_index,
            "run_scoring_index": run_index,
            "blended_k_rate": k_rate,
            "blended_bb_rate": bb_rate,
            "contact_quality_signal": contact_quality,
            "power_signal": power_signal,
            "hit_skill_signal": hit_skill,
        },
    }
