from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple


CONFIDENCE_TIERS = ("NO_BET", "MONITOR", "LEAN", "STRONG", "LOCK")


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_rate(value: Any) -> Optional[float]:
    numeric = safe_float(value)
    if numeric is None:
        return None
    return numeric / 100.0 if numeric > 1 else numeric


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def american_to_implied_probability(price: Any) -> Optional[float]:
    odds = safe_float(price)
    if odds is None or odds == 0:
        return None
    if odds > 0:
        return round(100.0 / (odds + 100.0), 4)
    return round(abs(odds) / (abs(odds) + 100.0), 4)


def calculate_expected_value(model_probability: Any, american_odds: Any) -> Optional[float]:
    probability = safe_float(model_probability)
    odds = safe_float(american_odds)
    if probability is None or odds is None:
        return None
    if probability < 0 or probability > 1 or odds == 0:
        return None
    profit_on_one_unit = odds / 100.0 if odds > 0 else 100.0 / abs(odds)
    loss_probability = 1.0 - probability
    return round((probability * profit_on_one_unit) - loss_probability, 4)


def assign_confidence_tier(
    data_quality_score: Any,
    confidence_score: Any,
    probability_edge: Any,
    expected_value: Any,
    missing_inputs: Optional[Iterable[str]] = None,
) -> str:
    data_quality = safe_float(data_quality_score)
    confidence = safe_float(confidence_score)
    edge = safe_float(probability_edge)
    ev = safe_float(expected_value)
    missing_count = len(list(missing_inputs or []))

    if None in (data_quality, confidence, edge, ev):
        return "MONITOR"

    if missing_count >= 3:
        return "NO_BET"
    if data_quality < 0.45 or confidence < 0.45 or edge <= 0 or ev <= 0:
        return "NO_BET"
    if data_quality < 0.60 or confidence < 0.58:
        return "MONITOR"
    if edge < 0.025 or ev < 0.015:
        return "LEAN"
    if data_quality >= 0.88 and confidence >= 0.84 and edge >= 0.060 and ev >= 0.050 and missing_count == 0:
        return "LOCK"
    if data_quality >= 0.72 and confidence >= 0.68 and edge >= 0.040 and ev >= 0.030:
        return "STRONG"
    return "LEAN"


def _weighted_average(parts: List[Tuple[Optional[float], float]]) -> Optional[float]:
    numerator = 0.0
    denominator = 0.0
    for value, weight in parts:
        if value is None:
            continue
        numerator += value * weight
        denominator += weight
    if denominator == 0:
        return None
    return numerator / denominator


def _pitch_type_support_metrics(metrics: Dict[str, Any]) -> Dict[str, Optional[float]]:
    xwoba = safe_float(metrics.get("xwoba"))
    on_base = safe_float(metrics.get("on_base_pct"))
    hard_hit = normalize_rate(metrics.get("hard_hit_pct"))
    barrel = normalize_rate(metrics.get("barrel_pct"))
    whiff = normalize_rate(metrics.get("whiff_pct"))
    strikeout = normalize_rate(metrics.get("k_pct"))

    positive_contact = _weighted_average([
        (((xwoba - 0.320) * 3.0) if xwoba is not None else None, 1.2),
        (((on_base - 0.320) * 2.5) if on_base is not None else None, 1.0),
        (((hard_hit - 0.38) * 2.5) if hard_hit is not None else None, 1.1),
        (((barrel - 0.08) * 3.0) if barrel is not None else None, 0.9),
    ])
    whiff_risk = _weighted_average([
        (((whiff - 0.28) * 3.0) if whiff is not None else None, 1.2),
        (((strikeout - 0.24) * 2.5) if strikeout is not None else None, 1.0),
    ])
    net_score = None
    if positive_contact is not None or whiff_risk is not None:
        net_score = (positive_contact or 0.0) - (whiff_risk or 0.0)

    return {
        "xwoba": xwoba,
        "on_base_pct": on_base,
        "hard_hit_pct": hard_hit,
        "barrel_pct": barrel,
        "whiff_pct": whiff,
        "k_pct": strikeout,
        "positive_contact_score": round(positive_contact, 4) if positive_contact is not None else None,
        "whiff_strikeout_risk": round(whiff_risk, 4) if whiff_risk is not None else None,
        "net_pitch_type_score": round(net_score, 4) if net_score is not None else None,
    }


def evaluate_usage_weighted_pitcher_vs_hitter(
    pitcher_arsenal_usage: Dict[str, Any],
    hitter_metrics_by_pitch_type: Dict[str, Dict[str, Any]],
    *,
    min_majority_usage: float = 0.50,
    min_supported_usage_for_positive_note: float = 0.10,
) -> Dict[str, Any]:
    normalized_usage: Dict[str, float] = {}
    for pitch_type, raw_usage in (pitcher_arsenal_usage or {}).items():
        usage = normalize_rate(raw_usage)
        if usage is not None and usage > 0:
            normalized_usage[str(pitch_type)] = usage

    total_usage = sum(normalized_usage.values())
    if total_usage <= 0:
        return {
            "status": "NO_BET",
            "reason": "missing_pitcher_arsenal_usage",
            "pitcher_arsenal_usage": {},
            "expected_pitch_type_exposure": {},
            "hitter_metrics_by_pitch_type": hitter_metrics_by_pitch_type or {},
            "usage_weighted_positive_contact_score": None,
            "usage_weighted_whiff_strikeout_risk": None,
            "usage_weighted_xwoba_or_on_base_score": None,
            "usage_weighted_hard_hit_score": None,
            "pitch_types_supporting_edge": [],
            "pitch_types_hurting_edge": [],
            "low_usage_pitch_warnings": ["No valid arsenal usage available."],
            "pitch_data_quality_flags": ["missing_pitch_usage_data"],
            "majority_usage_supported": False,
            "supported_usage_share": 0.0,
            "usage_weighted_pitcher_vs_hitter_score": None,
            "final_pitcher_vs_hitter_recommendation_status": "NO_BET",
        }

    exposure = {pitch_type: round(usage / total_usage, 4) for pitch_type, usage in normalized_usage.items()}
    supporting: List[Dict[str, Any]] = []
    hurting: List[Dict[str, Any]] = []
    low_usage_warnings: List[str] = []
    pitch_data_quality_flags: List[str] = []
    weighted_positive_contact = 0.0
    weighted_whiff_risk = 0.0
    weighted_on_base = 0.0
    weighted_hard_hit = 0.0
    weighted_net = 0.0
    supported_usage_share = 0.0

    for pitch_type, usage_share in sorted(exposure.items(), key=lambda item: item[1], reverse=True):
        metrics = dict((hitter_metrics_by_pitch_type or {}).get(pitch_type) or {})
        support_metrics = _pitch_type_support_metrics(metrics)
        quality_flag = metrics.get("data_quality_flag")
        sample_size = safe_float(metrics.get("sample_size"))
        if quality_flag:
            pitch_data_quality_flags.append(f"{pitch_type}:{quality_flag}")
        if sample_size is not None and sample_size < 5:
            pitch_data_quality_flags.append(f"{pitch_type}:low_sample_size")

        pos = support_metrics.get("positive_contact_score") or 0.0
        risk = support_metrics.get("whiff_strikeout_risk") or 0.0
        net = support_metrics.get("net_pitch_type_score")
        xwoba = support_metrics.get("xwoba")
        on_base = support_metrics.get("on_base_pct")
        hard_hit = support_metrics.get("hard_hit_pct")

        weighted_positive_contact += usage_share * pos
        weighted_whiff_risk += usage_share * risk
        if xwoba is not None or on_base is not None:
            weighted_on_base += usage_share * (((xwoba - 0.320) if xwoba is not None else 0.0) + ((on_base - 0.320) if on_base is not None else 0.0))
        if hard_hit is not None:
            weighted_hard_hit += usage_share * (hard_hit - 0.38)
        if net is not None:
            weighted_net += usage_share * net

        record = {
            "pitch_type": pitch_type,
            "usage_share": round(usage_share, 4),
            **support_metrics,
        }

        pitch_positive = (
            net is not None
            and net > 0
            and (support_metrics.get("whiff_strikeout_risk") or 0.0) <= (support_metrics.get("positive_contact_score") or 0.0)
        )
        if pitch_positive:
            supporting.append(record)
            supported_usage_share += usage_share
        else:
            hurting.append(record)

        if usage_share < min_supported_usage_for_positive_note and pitch_positive:
            low_usage_warnings.append(
                f"{pitch_type} supports the hitter, but only accounts for {round(usage_share * 100, 1)}% of expected usage."
            )

    majority_usage_supported = supported_usage_share >= min_majority_usage
    weighted_positive_contact = round(weighted_positive_contact, 4)
    weighted_whiff_risk = round(weighted_whiff_risk, 4)
    weighted_on_base = round(weighted_on_base, 4)
    weighted_hard_hit = round(weighted_hard_hit, 4)
    weighted_net = round(weighted_net, 4)

    status = "NO_BET"
    if pitch_data_quality_flags:
        status = "MONITOR"
    if (
        majority_usage_supported
        and weighted_net > 0
        and weighted_positive_contact > 0
        and weighted_whiff_risk <= weighted_positive_contact
        and weighted_on_base > 0
    ):
        status = "STRONG" if weighted_net >= 0.08 and weighted_hard_hit >= 0 else "LEAN"
    elif weighted_net > 0 and not majority_usage_supported:
        status = "MONITOR"
        low_usage_warnings.append("Positive signals do not cover a majority of projected pitch exposure.")
    elif weighted_whiff_risk > weighted_positive_contact:
        low_usage_warnings.append("Usage-weighted whiff/strikeout risk overwhelms positive contact indicators.")

    return {
        "status": status,
        "reason": "usage_weighted_pitcher_vs_hitter_evaluation",
        "pitcher_arsenal_usage": {key: round(value, 4) for key, value in normalized_usage.items()},
        "expected_pitch_type_exposure": exposure,
        "hitter_metrics_by_pitch_type": hitter_metrics_by_pitch_type or {},
        "usage_weighted_positive_contact_score": weighted_positive_contact,
        "usage_weighted_whiff_strikeout_risk": weighted_whiff_risk,
        "usage_weighted_xwoba_or_on_base_score": weighted_on_base,
        "usage_weighted_hard_hit_score": weighted_hard_hit,
        "pitch_types_supporting_edge": supporting,
        "pitch_types_hurting_edge": hurting,
        "low_usage_pitch_warnings": low_usage_warnings,
        "pitch_data_quality_flags": sorted(set(pitch_data_quality_flags)),
        "majority_usage_supported": majority_usage_supported,
        "supported_usage_share": round(supported_usage_share, 4),
        "usage_weighted_pitcher_vs_hitter_score": weighted_net,
        "final_pitcher_vs_hitter_recommendation_status": status,
    }


def build_team_recent_form_component(
    season_score: Any,
    l30_score: Any,
    l15_score: Any,
    l7_score: Any = None,
    *,
    home_away_context: Any = None,
    vs_handedness_context: Any = None,
) -> Dict[str, Any]:
    season = safe_float(season_score)
    l30 = safe_float(l30_score)
    l15 = safe_float(l15_score)
    l7 = safe_float(l7_score)
    home_away = safe_float(home_away_context)
    handedness = safe_float(vs_handedness_context)

    weighted_score = _weighted_average([
        (season, 0.40),
        (l30, 0.25),
        (l15, 0.20),
        (l7, 0.10),
        (home_away, 0.03),
        (handedness, 0.02),
    ])
    current = _weighted_average([
        (l30, 0.45),
        (l15, 0.35),
        (l7, 0.20),
    ])
    recent_form_delta = None
    if current is not None and season is not None:
        recent_form_delta = round(current - season, 4)
    trend = "stable"
    if recent_form_delta is not None:
        if recent_form_delta >= 0.03:
            trend = "improving"
        elif recent_form_delta <= -0.03:
            trend = "declining"
    missing_inputs = [
        name for name, value in {
            "season_score": season,
            "l30_score": l30,
            "l15_score": l15,
            "l7_score": l7,
            "home_away_context": home_away,
            "vs_handedness_context": handedness,
        }.items() if value is None
    ]
    data_quality = round(clamp(1.0 - (len(missing_inputs) * 0.12), 0.25, 1.0), 3)
    return {
        "team_offense_season_score": round(season, 4) if season is not None else None,
        "team_offense_l30_score": round(l30, 4) if l30 is not None else None,
        "team_offense_l15_score": round(l15, 4) if l15 is not None else None,
        "team_offense_l7_score": round(l7, 4) if l7 is not None else None,
        "team_home_away_context": round(home_away, 4) if home_away is not None else None,
        "team_vs_handedness_context": round(handedness, 4) if handedness is not None else None,
        "team_recent_form_score": round(weighted_score, 4) if weighted_score is not None else None,
        "team_recent_form_delta": recent_form_delta,
        "team_recent_form_trend": trend,
        "team_recent_form_data_quality": data_quality,
        "missing_inputs": missing_inputs,
    }


def build_starting_pitcher_component(
    season_baseline_score: Any,
    recent_form_score: Any,
    k_bb_score: Any,
    contact_quality_allowed_score: Any,
    arsenal_quality_score: Any,
    platoon_risk_score: Any,
    expected_workload_score: Any,
    *,
    velocity_or_stuff_trend: Any = None,
    command_trend: Any = None,
) -> Dict[str, Any]:
    season = safe_float(season_baseline_score)
    recent = safe_float(recent_form_score)
    kbb = safe_float(k_bb_score)
    contact = safe_float(contact_quality_allowed_score)
    arsenal = safe_float(arsenal_quality_score)
    platoon = safe_float(platoon_risk_score)
    workload = safe_float(expected_workload_score)
    velocity = safe_float(velocity_or_stuff_trend)
    command = safe_float(command_trend)

    weighted_score = _weighted_average([
        (season, 0.24),
        (recent, 0.20),
        (kbb, 0.16),
        (contact, 0.16),
        (arsenal, 0.10),
        (-platoon if platoon is not None else None, 0.06),
        (workload, 0.04),
        (velocity, 0.02),
        (command, 0.02),
    ])
    recent_delta = None
    if recent is not None and season is not None:
        recent_delta = round(recent - season, 4)
    trend = "stable"
    if recent_delta is not None:
        if recent_delta >= 0.03 or (velocity is not None and velocity >= 0.03) or (command is not None and command >= 0.03):
            trend = "improving"
        elif recent_delta <= -0.03 or (velocity is not None and velocity <= -0.03) or (command is not None and command <= -0.03):
            trend = "declining"
    missing_inputs = [
        name for name, value in {
            "season_baseline_score": season,
            "recent_form_score": recent,
            "k_bb_score": kbb,
            "contact_quality_allowed_score": contact,
            "arsenal_quality_score": arsenal,
            "platoon_risk_score": platoon,
            "expected_workload_score": workload,
            "velocity_or_stuff_trend": velocity,
            "command_trend": command,
        }.items() if value is None
    ]
    data_quality = round(clamp(1.0 - (len(missing_inputs) * 0.10), 0.25, 1.0), 3)
    return {
        "pitcher_season_baseline_score": round(season, 4) if season is not None else None,
        "pitcher_recent_form_score": round(recent, 4) if recent is not None else None,
        "pitcher_k_bb_score": round(kbb, 4) if kbb is not None else None,
        "pitcher_contact_quality_allowed_score": round(contact, 4) if contact is not None else None,
        "pitcher_arsenal_quality_score": round(arsenal, 4) if arsenal is not None else None,
        "pitcher_platoon_risk_score": round(platoon, 4) if platoon is not None else None,
        "pitcher_expected_workload_score": round(workload, 4) if workload is not None else None,
        "pitcher_velocity_or_stuff_trend": round(velocity, 4) if velocity is not None else None,
        "pitcher_command_trend": round(command, 4) if command is not None else None,
        "pitcher_component_score": round(weighted_score, 4) if weighted_score is not None else None,
        "pitcher_recent_form_delta": recent_delta,
        "pitcher_trend": trend,
        "pitcher_data_quality_flags": missing_inputs,
        "pitcher_data_quality_score": data_quality,
    }
