"""
Canonical matchup win probability v2.

Additive backend layer that keeps the existing scoring engine as legacy v1 while
producing one canonical probability payload for /matchups and downstream consumers.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy.orm import Session

from .bullpen_profile import build_bullpen_profile
from .environment_profile import compute_environment_profile
from .scoring import compute_win_probability
from .simulation.game_simulator import simulate_game_with_bullpen, starter_quality_score
from .simulation.pa_outcome_model import build_pa_outcome_probabilities

log = logging.getLogger(__name__)

CANONICAL_MODEL_VERSION = "canonical_matchup_win_probability_v2"
LEGACY_MODEL_VERSION = "legacy_matchup_win_probability_v1"
BATTER_VS_ARSENAL_SCHEMA_VERSION = "batter_vs_arsenal_v2"

COMPONENT_WEIGHTS = {
    "simulation": 0.35,
    "starter_matchup": 0.25,
    "batter_vs_arsenal": 0.20,
    "bullpen": 0.10,
    "environment": 0.05,
    "market_context": 0.05,
}


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clamp(value: float, lower: float = 0.001, upper: float = 0.999) -> float:
    return max(lower, min(upper, value))


def _round_prob(value: float) -> float:
    return round(_clamp(float(value)), 4)


def _normalize_pair(home_probability: float) -> Tuple[float, float]:
    home_probability = _round_prob(home_probability)
    return home_probability, round(1.0 - home_probability, 4)


def _component(score: Optional[float], weight: float, source: str, confidence: str = "low", missing_inputs: Optional[List[str]] = None, diagnostics: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    advantage = None
    if score is not None:
        advantage = "home" if score > 0.515 else "away" if score < 0.485 else "neutral"
    return {
        "score": _round_prob(score) if score is not None else None,
        "weight": weight,
        "adjusted_weight": 0.0,
        "advantage": advantage,
        "confidence": confidence,
        "source": source,
        "available": score is not None,
        "missing_inputs": missing_inputs or [],
        "diagnostics": diagnostics or {},
    }


def _reweight_components(components: Dict[str, Dict[str, Any]]) -> None:
    available_weight = sum(float(c.get("weight") or 0.0) for c in components.values() if c.get("available") and c.get("score") is not None)
    if available_weight <= 0:
        return
    for c in components.values():
        c["adjusted_weight"] = round(float(c.get("weight") or 0.0) / available_weight, 4) if c.get("available") and c.get("score") is not None else 0.0


def _weighted_home_probability(components: Dict[str, Dict[str, Any]], fallback: float) -> float:
    _reweight_components(components)
    numerator = 0.0
    denominator = 0.0
    for c in components.values():
        score = c.get("score")
        weight = _safe_float(c.get("adjusted_weight"))
        if score is None or weight is None or weight <= 0:
            continue
        numerator += float(score) * weight
        denominator += weight
    return fallback if denominator <= 0 else numerator / denominator


def _missing_from_components(components: Dict[str, Dict[str, Any]]) -> List[str]:
    missing: List[str] = []
    for name, c in components.items():
        if not c.get("available"):
            missing.append(name)
        for item in c.get("missing_inputs") or []:
            missing.append(f"{name}.{item}")
    return sorted(set(missing))


def _lineup_status(home_inputs: Optional[Dict[str, Any]], away_inputs: Optional[Dict[str, Any]]) -> str:
    joined = " ".join([
        str((home_inputs or {}).get("lineup_source") or (home_inputs or {}).get("source") or "").lower(),
        str((away_inputs or {}).get("lineup_source") or (away_inputs or {}).get("source") or "").lower(),
    ])
    if "confirmed" in joined and "fallback" not in joined:
        return "confirmed"
    if "previous" in joined:
        return "previous_lineup_fallback"
    if "roster" in joined:
        return "roster_fallback"
    if "team_splits" in joined or "team_split" in joined:
        return "team_split_fallback"
    if "projected" in joined:
        return "projected"
    return "unknown"


def _data_confidence(lineup_status: str, components: Dict[str, Dict[str, Any]], missing_inputs: Iterable[str]) -> str:
    available = sum(1 for c in components.values() if c.get("available"))
    missing_count = len(list(missing_inputs))
    if lineup_status == "confirmed" and available >= 4 and missing_count <= 2:
        return "high"
    if available >= 3 and missing_count <= 6:
        return "medium"
    return "low"


def _pitcher_profile(pitcher_id: Optional[int], pitcher_name: Optional[str], features: Optional[Dict[str, Any]], arsenal: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    features = features or {}
    return {
        "metadata": {
            "source_type": "matchup_generator_pitcher_features",
            "generated_from": "canonical_matchup_probability._pitcher_profile",
            "data_confidence": "medium" if any(v is not None for v in features.values()) else "low",
            "pitcher_id": pitcher_id,
            "pitcher_name": pitcher_name,
            "missing_inputs": [k for k, v in features.items() if v is None],
        },
        "bat_missing": {"k_rate": _safe_float(features.get("k_pct")), "whiff_rate": _safe_float(features.get("whiff_rate")), "csw_rate": _safe_float(features.get("csw_rate"))},
        "command_control": {"bb_rate": _safe_float(features.get("bb_pct")), "zone_rate": None, "first_pitch_strike_rate": None},
        "contact_management": {
            "hard_hit_rate_allowed": _safe_float(features.get("hard_hit_pct")),
            "barrel_rate_allowed": _safe_float(features.get("barrel_pct")),
            "avg_exit_velocity_allowed": _safe_float(features.get("avg_exit_velocity")),
            "avg_launch_angle_allowed": _safe_float(features.get("avg_launch_angle")),
            "xwoba_allowed": _safe_float(features.get("xwoba")),
            "xba_allowed": _safe_float(features.get("xba")),
        },
        "arsenal": {"pitch_mix": arsenal or {}, "avg_velocity": _safe_float(features.get("avg_velocity")), "avg_spin_rate": _safe_float(features.get("avg_spin_rate"))},
        "release_profile": {
            "release_pos_x": _safe_float(features.get("avg_release_pos_x")),
            "release_pos_z": _safe_float(features.get("avg_release_pos_z")),
            "release_extension": _safe_float(features.get("avg_release_extension")),
            "avg_horizontal_break": _safe_float(features.get("avg_horiz_break")),
            "avg_vertical_break": _safe_float(features.get("avg_vert_break")),
            "source": "statcast_aggregate_fields_when_available",
        },
    }


def _offense_profile(team_id: Optional[int], team_name: Optional[str], inputs: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    inputs = inputs or {}
    return {
        "metadata": {"source_type": inputs.get("source") or "offense_inputs", "team_id": team_id, "team_name": team_name, "lineup_source": inputs.get("lineup_source"), "sample_blend": inputs.get("sample_blend")},
        "contact_skill": {"k_rate": _safe_float(inputs.get("k_pct")), "batting_avg": _safe_float(inputs.get("batting_avg")), "contact_rate": None},
        "plate_discipline": {"bb_rate": _safe_float(inputs.get("bb_pct")), "on_base_pct": _safe_float(inputs.get("on_base_pct"))},
        "power": {"iso": _safe_float(inputs.get("iso")), "slugging_pct": _safe_float(inputs.get("slugging_pct")), "home_runs": _safe_float(inputs.get("home_runs")), "barrel_rate": _safe_float(inputs.get("barrel_pct")), "hard_hit_rate": _safe_float(inputs.get("hard_hit_pct"))},
        "run_creation": {"pa": _safe_float(inputs.get("pa")), "hits": _safe_float(inputs.get("hits")), "walks": _safe_float(inputs.get("walks")), "strikeouts": _safe_float(inputs.get("strikeouts"))},
    }


def _starter_overview(profile: Dict[str, Any]) -> Dict[str, Any]:
    metadata = profile.get("metadata") or {}
    bat = profile.get("bat_missing") or {}
    cmd = profile.get("command_control") or {}
    contact = profile.get("contact_management") or {}
    arsenal = profile.get("arsenal") or {}
    missing: List[str] = []
    overview = {
        "pitcher_id": metadata.get("pitcher_id"),
        "pitcher_name": metadata.get("pitcher_name"),
        "k_pct": bat.get("k_rate"),
        "bb_pct": cmd.get("bb_rate"),
        "k_minus_bb_pct": None,
        "xwoba_allowed": contact.get("xwoba_allowed"),
        "xba_allowed": contact.get("xba_allowed"),
        "hard_hit_rate_allowed": contact.get("hard_hit_rate_allowed"),
        "barrel_rate_allowed": contact.get("barrel_rate_allowed"),
        "average_exit_velocity_allowed": contact.get("avg_exit_velocity_allowed"),
        "average_launch_angle_allowed": contact.get("avg_launch_angle_allowed"),
        "whiff_rate": bat.get("whiff_rate"),
        "csw_rate": bat.get("csw_rate"),
        "avg_velocity": arsenal.get("avg_velocity"),
        "avg_spin_rate": arsenal.get("avg_spin_rate"),
        "release_profile": profile.get("release_profile") or {},
        "metric_sources": {},
        "missing_inputs": missing,
        "data_window_used": "90d_pitcher_aggregate_with_existing_fallbacks",
    }
    if overview["k_pct"] is not None and overview["bb_pct"] is not None:
        overview["k_minus_bb_pct"] = round(float(overview["k_pct"]) - float(overview["bb_pct"]), 4)
    for key, value in list(overview.items()):
        if key in {"release_profile", "metric_sources", "missing_inputs"}:
            continue
        overview["metric_sources"][key] = "db_or_statcast_aggregate" if value is not None else "missing"
        if value is None:
            missing.append(key)
    for key in ("era", "fip", "xfip", "siera", "xsiera", "hr_per_9", "gb_pct", "fb_pct", "hr_fb", "babip", "lob_pct", "pitch_count_sample_size"):
        overview[key] = None
        overview["metric_sources"][key] = "missing"
        missing.append(key)
    return overview


def _simulation_component(context: Dict[str, Any], home_pitcher: Dict[str, Any], away_pitcher: Dict[str, Any], home_bp: Dict[str, Any], away_bp: Dict[str, Any], env: Dict[str, Any]) -> Dict[str, Any]:
    try:
        home_off = _offense_profile(context.get("home_team_id"), context.get("home_team_name"), context.get("home_offense_inputs"))
        away_off = _offense_profile(context.get("away_team_id"), context.get("away_team_name"), context.get("away_offense_inputs"))
        away_starter_pa = build_pa_outcome_probabilities(away_off, home_pitcher, env)
        home_starter_pa = build_pa_outcome_probabilities(home_off, away_pitcher, env)
        away_bullpen_pa = build_pa_outcome_probabilities(away_off, home_bp, env)
        home_bullpen_pa = build_pa_outcome_probabilities(home_off, away_bp, env)
        game_pk = context.get("game_pk")
        seed = int(game_pk) if str(game_pk or "").isdigit() else None
        sim = simulate_game_with_bullpen(
            away_starter_probabilities=away_starter_pa["probabilities"],
            home_starter_probabilities=home_starter_pa["probabilities"],
            away_bullpen_probabilities=away_bullpen_pa["probabilities"],
            home_bullpen_probabilities=home_bullpen_pa["probabilities"],
            simulations=1500,
            seed=seed,
            away_starter_quality=starter_quality_score(away_pitcher),
            home_starter_quality=starter_quality_score(home_pitcher),
        )
        return _component(sim.get("home_win_probability"), COMPONENT_WEIGHTS["simulation"], "simulate_game_with_bullpen", "medium", diagnostics={
            "simulation_model_version": sim.get("model_version"),
            "home_expected_runs": sim.get("home_expected_runs"),
            "away_expected_runs": sim.get("away_expected_runs"),
            "total_expected_runs": sim.get("total_expected_runs"),
            "home_starter_quality_score": sim.get("home_starter_quality_score"),
            "away_starter_quality_score": sim.get("away_starter_quality_score"),
            "metadata": sim.get("metadata"),
        })
    except Exception as exc:
        log.exception("Canonical simulation component failed")
        return _component(None, COMPONENT_WEIGHTS["simulation"], "simulate_game_with_bullpen", "low", ["simulation_error"], {"error": str(exc), "error_type": exc.__class__.__name__})


def _offense_component(home_inputs: Optional[Dict[str, Any]], away_inputs: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    def signal(inputs: Optional[Dict[str, Any]]) -> Optional[float]:
        inputs = inputs or {}
        values: List[float] = []
        obp, slg, iso, bb, k = (_safe_float(inputs.get(k)) for k in ("on_base_pct", "slugging_pct", "iso", "bb_pct", "k_pct"))
        if obp is not None: values.append((obp - 0.320) * 1.8)
        if slg is not None: values.append((slg - 0.410) * 1.1)
        if iso is not None: values.append((iso - 0.160) * 1.4)
        if bb is not None and k is not None: values.append((bb - k + 0.140) * 0.7)
        return None if not values else sum(values) / len(values)
    home_signal, away_signal = signal(home_inputs), signal(away_inputs)
    missing = []
    if home_signal is None: missing.append("home_offense_inputs")
    if away_signal is None: missing.append("away_offense_inputs")
    if home_signal is None or away_signal is None:
        return _component(None, COMPONENT_WEIGHTS["batter_vs_arsenal"], "offense_inputs_proxy_until_batter_vs_arsenal_v2", missing_inputs=missing)
    score = 0.5 + max(-0.08, min(0.08, home_signal - away_signal))
    return _component(score, COMPONENT_WEIGHTS["batter_vs_arsenal"], "offense_inputs_proxy_until_batter_vs_arsenal_v2", "medium", diagnostics={
        "home_offense_signal": round(home_signal, 4),
        "away_offense_signal": round(away_signal, 4),
        "bat_tracking_adjustment": None,
        "bat_tracking_data_confidence": "missing",
        "note": "Sprint 1 uses lineup/team offense inputs as a nullable proxy. Sprint 4 will enrich this with Batter vs Arsenal v2 bat-tracking interactions.",
    })


def _bullpen_component(home_bp: Dict[str, Any], away_bp: Dict[str, Any]) -> Dict[str, Any]:
    home_q = _safe_float((home_bp.get("metadata") or {}).get("bullpen_quality_score"))
    away_q = _safe_float((away_bp.get("metadata") or {}).get("bullpen_quality_score"))
    if home_q is None or away_q is None:
        return _component(None, COMPONENT_WEIGHTS["bullpen"], "build_bullpen_profile", missing_inputs=["bullpen_quality_score"])
    return _component(0.5 + max(-0.04, min(0.04, (home_q - away_q) * 0.8)), COMPONENT_WEIGHTS["bullpen"], "build_bullpen_profile", "low", diagnostics={
        "home_bullpen_quality_score": home_q,
        "away_bullpen_quality_score": away_q,
        "home_bullpen_quality_label": (home_bp.get("metadata") or {}).get("bullpen_quality_label"),
        "away_bullpen_quality_label": (away_bp.get("metadata") or {}).get("bullpen_quality_label"),
    })


def _environment_component(env: Dict[str, Any]) -> Dict[str, Any]:
    run_env = env.get("run_environment") or {}
    scoring_index = _safe_float(run_env.get("run_scoring_index"))
    if scoring_index is None:
        return _component(None, COMPONENT_WEIGHTS["environment"], "compute_environment_profile", missing_inputs=["run_scoring_index"])
    return _component(0.5, COMPONENT_WEIGHTS["environment"], "compute_environment_profile", "medium", diagnostics={
        "run_scoring_index": scoring_index,
        "park_label": run_env.get("park_label"),
        "scoring_label": run_env.get("scoring_label"),
        "note": "Environment informs run distribution more than side probability; side score remains neutral to avoid fake directional bias.",
    })


def compute_canonical_matchup_probability(session: Session, home_pitcher_id: int, away_pitcher_id: int, home_team_id: int, away_team_id: int, season: int, home_pitcher_throws: str = "R", away_pitcher_throws: str = "R", context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    context = dict(context or {})
    legacy_home, legacy_away = compute_win_probability(session, home_pitcher_id, away_pitcher_id, home_team_id, away_team_id, season, home_pitcher_throws, away_pitcher_throws)

    home_pitcher = _pitcher_profile(home_pitcher_id, context.get("home_pitcher_name"), context.get("home_pitcher_features"), context.get("home_pitch_arsenal"))
    away_pitcher = _pitcher_profile(away_pitcher_id, context.get("away_pitcher_name"), context.get("away_pitcher_features"), context.get("away_pitch_arsenal"))
    env = compute_environment_profile({"venue": context.get("venue"), "venue_name": context.get("venue"), "weather": context.get("weather")})
    home_bp = build_bullpen_profile(team_id=home_team_id, team_name=context.get("home_team_name"))
    away_bp = build_bullpen_profile(team_id=away_team_id, team_name=context.get("away_team_name"))

    components = {
        "simulation": _simulation_component(context, home_pitcher, away_pitcher, home_bp, away_bp, env),
        "starter_matchup": _component(legacy_home, COMPONENT_WEIGHTS["starter_matchup"], LEGACY_MODEL_VERSION, "medium", diagnostics={"legacy_home_win_prob": legacy_home, "legacy_away_win_prob": legacy_away, "anti_double_counting_note": "Legacy starter matchup is treated as a directional adjustment, not a second full-strength projection layer."}),
        "batter_vs_arsenal": _offense_component(context.get("home_offense_inputs"), context.get("away_offense_inputs")),
        "bullpen": _bullpen_component(home_bp, away_bp),
        "environment": _environment_component(env),
        "market_context": _component(None, COMPONENT_WEIGHTS["market_context"], "market_sanity_context_optional_not_model_driver", "low", ["market_context_not_supplied"], {"note": "Odds are intentionally not used as a model driver in canonical v2."}),
    }

    home_win_prob, away_win_prob = _normalize_pair(_weighted_home_probability(components, fallback=legacy_home))
    missing_inputs = _missing_from_components(components)
    status = _lineup_status(context.get("home_offense_inputs"), context.get("away_offense_inputs"))
    confidence = _data_confidence(status, components, missing_inputs)

    return {
        "model_version": CANONICAL_MODEL_VERSION,
        "legacy_model_version": LEGACY_MODEL_VERSION,
        "home_win_prob": home_win_prob,
        "away_win_prob": away_win_prob,
        "legacy_home_win_prob": _round_prob(legacy_home),
        "legacy_away_win_prob": _round_prob(legacy_away),
        "lineup_status": status,
        "data_confidence": confidence,
        "probability_components": components,
        "pitcher_overview": {"home": _starter_overview(home_pitcher), "away": _starter_overview(away_pitcher)},
        "batter_vs_arsenal_schema_version": BATTER_VS_ARSENAL_SCHEMA_VERSION,
        "batter_vs_arsenal_summary": {
            "schema_version": BATTER_VS_ARSENAL_SCHEMA_VERSION,
            "bat_tracking_adjustment": (components.get("batter_vs_arsenal") or {}).get("diagnostics", {}).get("bat_tracking_adjustment"),
            "bat_tracking_data_confidence": (components.get("batter_vs_arsenal") or {}).get("diagnostics", {}).get("bat_tracking_data_confidence", "missing"),
            "missing_inputs": (components.get("batter_vs_arsenal") or {}).get("missing_inputs", []),
        },
        "missing_inputs": missing_inputs,
    }


__all__ = ["BATTER_VS_ARSENAL_SCHEMA_VERSION", "CANONICAL_MODEL_VERSION", "LEGACY_MODEL_VERSION", "compute_canonical_matchup_probability"]
