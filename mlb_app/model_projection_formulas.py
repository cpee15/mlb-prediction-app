from __future__ import annotations

from typing import Any, Dict, List, Optional


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


def weighted_average(parts: List[tuple[Optional[float], float]]) -> Optional[float]:
    numerator = 0.0
    denominator = 0.0
    for value, weight in parts:
        if value is None:
            continue
        numerator += value * weight
        denominator += weight
    return round(numerator / denominator, 2) if denominator else None


def confidence_from_inputs(required: List[str], inputs: Dict[str, Any], sample_size: Optional[int] = None) -> str:
    present = sum(1 for key in required if inputs.get(key) is not None)
    ratio = present / max(len(required), 1)
    if sample_size is not None and sample_size >= 5:
        ratio = min(1.0, ratio + 0.1)
    if ratio >= 0.85:
        return "high"
    if ratio >= 0.55:
        return "medium"
    if ratio > 0:
        return "low"
    return "unavailable"


def model_object(model_name: str, formula: str, inputs: Dict[str, Any], steps: List[str], missing: List[str], notes: List[str], score: Optional[float], confidence: Optional[str] = None) -> Dict[str, Any]:
    return {
        "model_name": model_name,
        "status": "calculated" if score is not None and not missing else "partial" if score is not None else "missing_inputs",
        "score": round(score, 2) if score is not None else None,
        "formula": formula,
        "inputs": inputs,
        "calculation_steps": steps,
        "missing_inputs": missing,
        "data_confidence": confidence or confidence_from_inputs(list(inputs.keys()), inputs),
        "source_notes": notes,
    }


def pitching_volatility_score(features: Dict[str, Any], arsenal: Dict[str, Any]) -> Dict[str, Any]:
    k_pct = normalize_rate(features.get("k_pct"))
    bb_pct = normalize_rate(features.get("bb_pct"))
    xwoba = safe_float(features.get("xwoba"))
    xba = safe_float(features.get("xba"))
    hard_hit_pct = normalize_rate(features.get("hard_hit_pct"))
    velocity = safe_float(features.get("avg_velocity"))
    spin = safe_float(features.get("avg_spin_rate"))
    arsenal_whiff = None
    if arsenal:
        vals = []
        for row in arsenal.values():
            if isinstance(row, dict):
                whiff = normalize_rate(row.get("whiff_pct"))
                usage = normalize_rate(row.get("usage_pct")) or 0
                if whiff is not None and usage > 0:
                    vals.append((whiff, usage))
        total = sum(w for _, w in vals)
        arsenal_whiff = sum(v * w for v, w in vals) / total if total else None
    inputs = {"k_pct": k_pct, "bb_pct": bb_pct, "xwoba": xwoba, "xba": xba, "hard_hit_pct": hard_hit_pct, "avg_velocity": velocity, "avg_spin_rate": spin, "usage_weighted_arsenal_whiff_pct": arsenal_whiff}
    score = weighted_average([
        (abs(k_pct - bb_pct) * 100 if k_pct is not None and bb_pct is not None else None, 1.3),
        (abs(xwoba - 0.320) * 250 if xwoba is not None else None, 1.2),
        (abs(xba - 0.245) * 250 if xba is not None else None, 0.8),
        (hard_hit_pct * 100 if hard_hit_pct is not None else None, 1.0),
        (((velocity - 88) / 10) * 100 if velocity is not None else None, 0.5),
        ((spin / 2800) * 100 if spin is not None else None, 0.3),
        (arsenal_whiff * 100 if arsenal_whiff is not None else None, 0.8),
    ])
    return model_object("Model 1: Pitching Volatility Score", "weighted_avg(|K%-BB%|, |xwOBA-.320|, |xBA-.245|, HardHit%, velo, spin, arsenal whiff)", inputs, ["Normalize rates.", "Calculate volatility/contact components.", "Blend available pitcher and arsenal inputs."], [k for k, v in inputs.items() if v is None], ["Uses main pitcher aggregate and pitch arsenal data."], score, confidence_from_inputs(list(inputs), inputs, len(arsenal or {})))


def offensive_firepower_score(inputs_raw: Dict[str, Any]) -> Dict[str, Any]:
    barrel_pct = normalize_rate(inputs_raw.get("barrel_pct"))
    hard_hit_pct = normalize_rate(inputs_raw.get("hard_hit_pct"))
    launch_angle = safe_float(inputs_raw.get("avg_launch_angle"))
    bb_pct = normalize_rate(inputs_raw.get("bb_pct"))
    k_pct = normalize_rate(inputs_raw.get("k_pct"))
    xwoba = safe_float(inputs_raw.get("xwoba"))
    iso = safe_float(inputs_raw.get("iso"))
    slg = safe_float(inputs_raw.get("slugging_pct"))
    obp = safe_float(inputs_raw.get("on_base_pct"))
    bb_k = bb_pct / k_pct if bb_pct is not None and k_pct not in (None, 0) else None
    launch_score = max(0, min(100, 100 - abs(launch_angle - 14) * 5)) if launch_angle is not None else None
    inputs = {"barrel_pct": barrel_pct, "hard_hit_pct": hard_hit_pct, "avg_launch_angle": launch_angle, "bb_pct": bb_pct, "k_pct": k_pct, "bb_k_ratio": bb_k, "xwoba": xwoba, "iso": iso, "slugging_pct": slg, "on_base_pct": obp, "pa": inputs_raw.get("pa"), "lineup_source": inputs_raw.get("lineup_source"), "player_count_used": inputs_raw.get("player_count_used"), "sample_blend": inputs_raw.get("sample_blend")}
    score = weighted_average([(barrel_pct * 100 if barrel_pct is not None else None, 1.3), (hard_hit_pct * 100 if hard_hit_pct is not None else None, 1.1), (launch_score, 0.7), (bb_k * 100 if bb_k is not None else None, 0.8), ((xwoba - 0.300) * 250 if xwoba is not None else None, 1.2), (iso * 250 if iso is not None else None, 0.8), ((slg - 0.350) * 160 if slg is not None else None, 0.6), ((obp - 0.290) * 180 if obp is not None else None, 0.6)])
    return model_object("Model 2: Offensive Firepower Score", "weighted_avg(Barrel%, HardHit%, launch-angle fit, BB/K, xwOBA, ISO, SLG, OBP)", inputs, ["Read projected lineup or team split inputs.", "Normalize rates and calculate BB/K.", "Blend power, discipline, and contact-quality signals."], [k for k, v in inputs.items() if v is None], ["Projected-lineup data is preferred; team split fallback is clearly labeled."], score)


def bullpen_collapse_index(raw: Dict[str, Any]) -> Dict[str, Any]:
    era = safe_float(raw.get("era"))
    bb9 = safe_float(raw.get("bb_per_9") or raw.get("bb9"))
    whip = safe_float(raw.get("whip"))
    inputs = {"era": era, "bb_per_9": bb9, "whip": whip, "source_table": raw.get("source_table")}
    missing = [k for k in ["era", "bb_per_9", "whip"] if inputs.get(k) is None]
    score = round(era * bb9 + whip, 3) if not missing else None
    return model_object("Model 3: Bullpen Collapse Index", "BCI = ERA * BB/9 + WHIP", inputs, ["Read ERA, BB/9, and WHIP from main data if a bullpen table exists.", "Calculate only when all required inputs exist."], missing, ["No bullpen score is fabricated when required fields are missing."], score)


def pitch_identity_disruption_score(arsenal: Dict[str, Any], hitter_pitch_rows: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    pitch_mix = []
    for pitch_type, row in (arsenal or {}).items():
        if not isinstance(row, dict):
            continue
        usage = normalize_rate(row.get("usage_pct"))
        whiff = normalize_rate(row.get("whiff_pct"))
        xwoba = safe_float(row.get("xwoba"))
        hard_hit = normalize_rate(row.get("hard_hit_pct"))
        pitch_score = weighted_average([(usage * 100 if usage is not None else None, 1.0), (whiff * 100 if whiff is not None else None, 1.2), ((0.340 - xwoba) * 250 if xwoba is not None else None, 1.0), ((0.42 - hard_hit) * 100 if hard_hit is not None else None, 0.6)])
        pitch_mix.append({"pitch_type": pitch_type, "usage_pct": usage, "whiff_pct": whiff, "xwoba": xwoba, "hard_hit_pct": hard_hit, "pitch_score": pitch_score})
    scored = [p for p in pitch_mix if p.get("pitch_score") is not None]
    score = round(sum(p["pitch_score"] for p in scored) / len(scored), 2) if scored else None
    inputs = {"pitch_mix": pitch_mix, "hitter_vs_pitch_type_rows": hitter_pitch_rows or [], "biggest_edge": max(scored, key=lambda p: p["pitch_score"], default=None), "biggest_weakness": min(scored, key=lambda p: p["pitch_score"], default=None), "pitch_count_used": len(scored)}
    missing = [] if pitch_mix else ["pitch_arsenal"]
    if not hitter_pitch_rows:
        missing.append("hitter_vs_pitch_type_data")
    return model_object("Model 4: Pitch Identity Disruption Score", "pitch-mix disruption = usage + whiff% + xwOBA suppression + hard-contact suppression, enriched by hitter-vs-pitch data when present", inputs, ["Score each pitch in the arsenal.", "Identify biggest pitch edge and weakness.", "Attach hitter-vs-pitch rows only when production data exists."], missing, ["Pitcher arsenal is real when available; hitter-vs-pitch data is never faked."], score, confidence_from_inputs(["pitch_mix"], {"pitch_mix": pitch_mix if pitch_mix else None}, len(scored)))
