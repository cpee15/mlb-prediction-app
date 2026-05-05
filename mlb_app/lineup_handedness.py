from __future__ import annotations

import datetime as dt
from collections import Counter
from typing import Any, Dict, Iterable, Optional

from sqlalchemy import func

from .database import StatcastEvent


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _normalize_stand(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if text in {"L", "R", "S"}:
        return text
    return None


def _safe_factor(value: Any, default: float = 1.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def infer_hitter_stand_from_statcast(
    session,
    hitter_id: int,
    season_start: dt.date,
    target_date: dt.date,
) -> Dict[str, Any]:
    """Infer hitter batting side from local StatcastEvent.stand.

    Statcast stand is PA-side. If a hitter has meaningful L and R samples,
    we treat the hitter as switch for lineup mix diagnostics.
    """
    hitter_id = int(hitter_id)

    rows = (
        session.query(StatcastEvent.stand, func.count(StatcastEvent.id))
        .filter(
            StatcastEvent.batter_id == hitter_id,
            StatcastEvent.game_date >= season_start,
            StatcastEvent.game_date <= target_date,
            StatcastEvent.stand.isnot(None),
        )
        .group_by(StatcastEvent.stand)
        .all()
    )

    counts = {"L": 0, "R": 0, "S": 0, "unknown": 0}
    for raw_stand, count in rows:
        stand = _normalize_stand(raw_stand)
        if stand in {"L", "R", "S"}:
            counts[stand] += int(count or 0)
        else:
            counts["unknown"] += int(count or 0)

    l_count = counts["L"]
    r_count = counts["R"]
    s_count = counts["S"]
    known = l_count + r_count + s_count

    if known <= 0:
        return {
            "hitter_id": hitter_id,
            "inferred_stand": None,
            "source": "missing_statcast_stand",
            "confidence": "missing",
            "stand_counts": counts,
            "known_statcast_stand_rows": 0,
            "switch_hitter_handling": None,
        }

    if s_count > 0:
        return {
            "hitter_id": hitter_id,
            "inferred_stand": "S",
            "source": "statcast_stand_explicit_switch",
            "confidence": "high" if known >= 20 else "medium",
            "stand_counts": counts,
            "known_statcast_stand_rows": known,
            "switch_hitter_handling": "explicit_switch_from_statcast",
        }

    if l_count > 0 and r_count > 0:
        min_side = min(l_count, r_count)
        max_side = max(l_count, r_count)
        if min_side >= 5:
            return {
                "hitter_id": hitter_id,
                "inferred_stand": "S",
                "source": "statcast_stand_both_sides",
                "confidence": "high" if known >= 30 else "medium",
                "stand_counts": counts,
                "known_statcast_stand_rows": known,
                "switch_hitter_handling": "treat_as_switch_for_lineup_mix",
            }

        dominant = "L" if l_count >= r_count else "R"
        share = max_side / known if known else 0.0
        return {
            "hitter_id": hitter_id,
            "inferred_stand": dominant,
            "source": "statcast_stand_dominant_side_with_tiny_opposite_sample",
            "confidence": "medium" if share >= 0.9 and known >= 20 else "low",
            "stand_counts": counts,
            "known_statcast_stand_rows": known,
            "switch_hitter_handling": "opposite_sample_too_small_to_call_switch",
        }

    if l_count > 0:
        return {
            "hitter_id": hitter_id,
            "inferred_stand": "L",
            "source": "statcast_stand_single_side",
            "confidence": "high" if l_count >= 20 else "medium" if l_count >= 5 else "low",
            "stand_counts": counts,
            "known_statcast_stand_rows": known,
            "switch_hitter_handling": None,
        }

    if r_count > 0:
        return {
            "hitter_id": hitter_id,
            "inferred_stand": "R",
            "source": "statcast_stand_single_side",
            "confidence": "high" if r_count >= 20 else "medium" if r_count >= 5 else "low",
            "stand_counts": counts,
            "known_statcast_stand_rows": known,
            "switch_hitter_handling": None,
        }

    return {
        "hitter_id": hitter_id,
        "inferred_stand": None,
        "source": "missing_statcast_stand",
        "confidence": "missing",
        "stand_counts": counts,
        "known_statcast_stand_rows": known,
        "switch_hitter_handling": None,
    }


def build_lineup_handedness_mix(
    session,
    hitter_ids: Iterable[int],
    season_start: dt.date,
    target_date: dt.date,
) -> Dict[str, Any]:
    """Build L/R/S/unknown lineup mix diagnostics from hitter ids."""
    clean_ids = []
    for hitter_id in hitter_ids or []:
        parsed = _safe_int(hitter_id)
        if parsed is not None:
            clean_ids.append(parsed)

    counts = {"L": 0, "R": 0, "S": 0, "unknown": 0}
    player_diagnostics = []

    for hitter_id in clean_ids:
        inferred = infer_hitter_stand_from_statcast(session, hitter_id, season_start, target_date)
        stand = inferred.get("inferred_stand")
        if stand not in {"L", "R", "S"}:
            stand = "unknown"
        counts[stand] += 1
        player_diagnostics.append(inferred)

    total = sum(counts.values())
    known = counts["L"] + counts["R"] + counts["S"]
    weights = {
        key: round((value / total), 4) if total else 0.0
        for key, value in counts.items()
    }

    return {
        "source": "statcast_stand",
        "hitter_count": total,
        "known_hitter_count": known,
        "coverage_rate": round((known / total), 4) if total else None,
        "counts": counts,
        "weights": weights,
        "switch_hitter_strategy": "generic_hr_factor_first_pass",
        "unknown_hitter_strategy": "generic_hr_factor",
        "player_diagnostics": player_diagnostics,
    }


def estimate_handedness_weighted_hr_factor(
    park_profile: Dict[str, Any],
    handedness_mix: Optional[Dict[str, Any]],
    shrinkage: float = 0.50,
) -> Dict[str, Any]:
    """Diagnostic-only weighted HR park factor.

    This should not be wired into hr_boost_index in this PR.
    """
    generic = _safe_factor(park_profile.get("home_run_factor"), 1.0)
    lhb = park_profile.get("home_run_factor_lhb")
    rhb = park_profile.get("home_run_factor_rhb")

    if not handedness_mix:
        return {
            "generic_home_run_factor": generic,
            "home_run_factor_lhb": lhb,
            "home_run_factor_rhb": rhb,
            "weighted_home_run_factor_raw": generic,
            "weighted_home_run_factor_shrunk": generic,
            "weighted_vs_generic_delta": 0.0,
            "shrinkage": shrinkage,
            "fallback_used": True,
            "fallback_reason": "missing_lineup_handedness_mix",
            "active_model_input_changed": False,
        }

    counts = handedness_mix.get("counts") or {}
    l_count = int(counts.get("L") or 0)
    r_count = int(counts.get("R") or 0)
    s_count = int(counts.get("S") or 0)
    unknown_count = int(counts.get("unknown") or 0)
    total = l_count + r_count + s_count + unknown_count

    if total <= 0:
        return {
            "generic_home_run_factor": generic,
            "home_run_factor_lhb": lhb,
            "home_run_factor_rhb": rhb,
            "weighted_home_run_factor_raw": generic,
            "weighted_home_run_factor_shrunk": generic,
            "weighted_vs_generic_delta": 0.0,
            "shrinkage": shrinkage,
            "fallback_used": True,
            "fallback_reason": "empty_lineup_handedness_mix",
            "active_model_input_changed": False,
        }

    l_factor = _safe_factor(lhb, generic)
    r_factor = _safe_factor(rhb, generic)
    fallback_reasons = []
    if lhb is None or rhb is None:
        fallback_reasons.append("missing_lhr_rhr_park_factors")
    if s_count:
        fallback_reasons.append("switch_hitters_use_generic_hr_factor")
    if unknown_count:
        fallback_reasons.append("unknown_hitters_use_generic_hr_factor")

    raw = (
        (l_count * l_factor)
        + (r_count * r_factor)
        + (s_count * generic)
        + (unknown_count * generic)
    ) / total

    shrunk = generic + (float(shrinkage) * (raw - generic))

    return {
        "generic_home_run_factor": round(generic, 4),
        "home_run_factor_lhb": lhb,
        "home_run_factor_rhb": rhb,
        "weighted_home_run_factor_raw": round(raw, 4),
        "weighted_home_run_factor_shrunk": round(shrunk, 4),
        "weighted_vs_generic_delta": round(raw - generic, 4),
        "shrunk_vs_generic_delta": round(shrunk - generic, 4),
        "shrinkage": shrinkage,
        "fallback_used": bool(fallback_reasons),
        "fallback_reason": ",".join(fallback_reasons) if fallback_reasons else None,
        "active_model_input_changed": False,
    }


__all__ = [
    "infer_hitter_stand_from_statcast",
    "build_lineup_handedness_mix",
    "estimate_handedness_weighted_hr_factor",
]
