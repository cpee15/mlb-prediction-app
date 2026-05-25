from __future__ import annotations

from typing import Any, Dict, List, Optional

from .canonical_model_engine import build_starting_pitcher_component, build_team_recent_form_component, clamp, safe_float

GAME_CONTEXT_VERSION = "canonical_game_context_v1"


def _offense_signal(inputs: Optional[Dict[str, Any]]) -> Optional[float]:
    inputs = inputs or {}
    obp = safe_float(inputs.get("on_base_pct"))
    slg = safe_float(inputs.get("slugging_pct"))
    iso = safe_float(inputs.get("iso") or inputs.get("stored_iso"))
    bb = safe_float(inputs.get("bb_pct"))
    k = safe_float(inputs.get("k_pct"))
    values: List[float] = []
    if obp is not None:
        values.append(0.5 + ((obp - 0.320) * 2.5))
    if slg is not None:
        values.append(0.5 + ((slg - 0.410) * 1.6))
    if iso is not None:
        values.append(0.5 + ((iso - 0.160) * 1.8))
    if bb is not None and k is not None:
        values.append(0.5 + ((bb - k + 0.140) * 1.0))
    return round(clamp(sum(values) / len(values), 0.0, 1.0), 4) if values else None


def _pitcher_scores(overview: Optional[Dict[str, Any]], features: Optional[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    overview = overview or {}
    features = features or {}
    era = safe_float(overview.get("era"))
    kbb = safe_float(overview.get("k_minus_bb_pct"))
    xwoba = safe_float(overview.get("xwoba_allowed"))
    hard_hit = safe_float(overview.get("hard_hit_rate_allowed"))
    season_parts: List[float] = []
    if era is not None:
        season_parts.append(0.5 + ((4.00 - era) * 0.06))
    if kbb is not None:
        season_parts.append(0.5 + ((kbb - 0.14) * 1.8))
    if xwoba is not None:
        season_parts.append(0.5 + ((0.320 - xwoba) * 2.6))
    if hard_hit is not None:
        season_parts.append(0.5 + ((0.38 - hard_hit) * 1.6))
    season_score = round(clamp(sum(season_parts) / len(season_parts), 0.0, 1.0), 4) if season_parts else None
    feat_k = safe_float(features.get("k_pct"))
    feat_bb = safe_float(features.get("bb_pct"))
    feat_xwoba = safe_float(features.get("xwoba"))
    feat_hard_hit = safe_float(features.get("hard_hit_pct"))
    feat_velocity = safe_float(features.get("avg_velocity"))
    recent_parts: List[float] = []
    if feat_k is not None and feat_bb is not None:
        recent_parts.append(0.5 + (((feat_k - feat_bb) - 0.14) * 1.5))
    if feat_xwoba is not None:
        recent_parts.append(0.5 + ((0.320 - feat_xwoba) * 2.4))
    if feat_hard_hit is not None:
        recent_parts.append(0.5 + ((0.38 - feat_hard_hit) * 1.5))
    if feat_velocity is not None:
        recent_parts.append(0.5 + ((feat_velocity - 93.0) * 0.025))
    recent_score = round(clamp(sum(recent_parts) / len(recent_parts), 0.0, 1.0), 4) if recent_parts else season_score
    return {
        "season_baseline_score": season_score,
        "recent_form_score": recent_score,
        "k_bb_score": round(clamp(0.5 + (((feat_k or 0.20) - (feat_bb or 0.08) - 0.12) * 1.8), 0.0, 1.0), 4) if feat_k is not None or feat_bb is not None else None,
        "contact_quality_allowed_score": round(clamp(0.5 + ((0.320 - (feat_xwoba if feat_xwoba is not None else xwoba if xwoba is not None else 0.320)) * 2.6), 0.0, 1.0), 4) if (feat_xwoba is not None or xwoba is not None) else None,
        "arsenal_quality_score": round(clamp(0.5 + ((feat_velocity - 93.0) * 0.025), 0.0, 1.0), 4) if feat_velocity is not None else None,
        "platoon_risk_score": round(clamp((xwoba - 0.300) * 2.0, 0.0, 1.0), 4) if xwoba is not None else None,
        "expected_workload_score": round(clamp((safe_float(overview.get("innings_pitched")) or 0.0) / 180.0, 0.0, 1.0), 4) if safe_float(overview.get("innings_pitched")) is not None else None,
        "velocity_or_stuff_trend": round((feat_velocity - 93.0) / 10.0, 4) if feat_velocity is not None else None,
        "command_trend": round((0.08 - feat_bb) * 2.0, 4) if feat_bb is not None else None,
    }


def build_canonical_game_context(matchup_like: Dict[str, Any], projection_sim: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    home_team = matchup_like.get("home_team_name") or matchup_like.get("home_team") or ((matchup_like.get("home_team") or {}).get("name") if isinstance(matchup_like.get("home_team"), dict) else None)
    away_team = matchup_like.get("away_team_name") or matchup_like.get("away_team") or ((matchup_like.get("away_team") or {}).get("name") if isinstance(matchup_like.get("away_team"), dict) else None)
    home_prob = safe_float(matchup_like.get("home_win_prob") or matchup_like.get("home_win_probability"))
    away_prob = safe_float(matchup_like.get("away_win_prob") or matchup_like.get("away_win_probability"))
    probability_components = matchup_like.get("probability_components") or ((matchup_like.get("main_matchup_probabilities") or {}).get("probability_components")) or {}
    pitcher_overview = matchup_like.get("pitcher_overview") or ((matchup_like.get("main_matchup_probabilities") or {}).get("pitcher_overview")) or {}
    home_overview = pitcher_overview.get("home") if isinstance(pitcher_overview, dict) else {}
    away_overview = pitcher_overview.get("away") if isinstance(pitcher_overview, dict) else {}
    home_features = matchup_like.get("home_pitcher_features") or ((matchup_like.get("teams") or {}).get("home") or {}).get("pitcher_features") or {}
    away_features = matchup_like.get("away_pitcher_features") or ((matchup_like.get("teams") or {}).get("away") or {}).get("pitcher_features") or {}
    home_offense_inputs = matchup_like.get("home_offense_inputs") or ((matchup_like.get("teams") or {}).get("home") or {}).get("offense_inputs") or {}
    away_offense_inputs = matchup_like.get("away_offense_inputs") or ((matchup_like.get("teams") or {}).get("away") or {}).get("offense_inputs") or {}
    home_pitcher_component = build_starting_pitcher_component(**_pitcher_scores(home_overview, home_features))
    away_pitcher_component = build_starting_pitcher_component(**_pitcher_scores(away_overview, away_features))
    home_signal = _offense_signal(home_offense_inputs)
    away_signal = _offense_signal(away_offense_inputs)
    home_team_component = build_team_recent_form_component(home_signal, home_signal, home_signal, home_signal)
    away_team_component = build_team_recent_form_component(away_signal, away_signal, away_signal, away_signal)
    sim_diag = ((probability_components or {}).get("simulation") or {}).get("diagnostics") or {}
    projection_sim = projection_sim or {}
    home_runs = safe_float(projection_sim.get("home_expected_runs") or sim_diag.get("home_expected_runs"))
    away_runs = safe_float(projection_sim.get("away_expected_runs") or sim_diag.get("away_expected_runs"))
    total_runs = safe_float(projection_sim.get("total_expected_runs") or sim_diag.get("total_expected_runs"))
    probability_gap = round(abs((home_prob or 0.5) - (away_prob or 0.5)), 4) if home_prob is not None and away_prob is not None else None
    favorite_side = "home" if home_prob is not None and away_prob is not None and home_prob >= away_prob else "away" if home_prob is not None and away_prob is not None else None
    return {
        "object_version": GAME_CONTEXT_VERSION,
        "game_pk": matchup_like.get("game_pk"),
        "game_date": matchup_like.get("game_date"),
        "away_team": away_team,
        "home_team": home_team,
        "home_win_prob": round(home_prob, 4) if home_prob is not None else None,
        "away_win_prob": round(away_prob, 4) if away_prob is not None else None,
        "projected_home_runs": round(home_runs, 3) if home_runs is not None else None,
        "projected_away_runs": round(away_runs, 3) if away_runs is not None else None,
        "projected_total_runs": round(total_runs, 3) if total_runs is not None else None,
        "starting_pitcher_component": {"home": home_pitcher_component, "away": away_pitcher_component},
        "team_recent_form_component": {"home": home_team_component, "away": away_team_component},
        "bullpen_component": ((probability_components or {}).get("bullpen") or {}),
        "lineup_status": matchup_like.get("lineup_status"),
        "data_confidence": matchup_like.get("data_confidence"),
        "probability_gap": probability_gap,
        "favorite_side": favorite_side,
        "missing_inputs": matchup_like.get("missing_inputs") or [],
    }
