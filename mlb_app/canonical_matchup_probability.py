"""
Canonical matchup win probability v2.

Additive backend layer that keeps the existing scoring engine as legacy v1 while
producing one canonical probability payload for /matchups and downstream consumers.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
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
MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"

COMPONENT_WEIGHTS = {
    "simulation": 0.35,
    "starter_matchup": 0.25,
    "batter_vs_arsenal": 0.20,
    "bullpen": 0.10,
    "environment": 0.05,
    "market_context": 0.05,
}

STARTER_OVERVIEW_FIELDS = [
    ("era", "ERA"),
    ("fip", "FIP"),
    ("xfip", "xFIP"),
    ("siera", "SIERA"),
    ("xsiera", "xSIERA"),
    ("k_pct", "K%"),
    ("bb_pct", "BB%"),
    ("k_minus_bb_pct", "K-BB%"),
    ("hr_per_9", "HR/9"),
    ("gb_pct", "GB%"),
    ("fb_pct", "FB%"),
    ("hr_fb", "HR/FB"),
    ("babip", "BABIP"),
    ("lob_pct", "LOB%"),
    ("xwoba_allowed", "xwOBA Allowed"),
    ("xba_allowed", "xBA Allowed"),
    ("hard_hit_rate_allowed", "Hard Hit% Allowed"),
    ("barrel_rate_allowed", "Barrel% Allowed"),
    ("average_exit_velocity_allowed", "Avg EV Allowed"),
    ("average_launch_angle_allowed", "Avg LA Allowed"),
    ("whiff_rate", "Whiff%"),
    ("csw_rate", "CSW%"),
    ("pitch_count_sample_size", "Pitch Count / Sample"),
    ("innings_pitched", "IP"),
    ("batters_faced", "Batters Faced"),
    ("pitches_thrown", "Pitches"),
]


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, str) and value.strip().lower() in {"", "none", "null", "nan", "n/a", "na", "-.--"}:
            return None
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


def _pct(value: Any) -> Optional[float]:
    number = _safe_float(value)
    if number is None:
        return None
    return round(number / 100.0, 4) if number > 1.5 else round(number, 4)


def _round_stat(value: Any, digits: int = 3) -> Optional[float]:
    number = _safe_float(value)
    return round(number, digits) if number is not None else None


def _innings_to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if "." in text:
            whole, frac = text.split(".", 1)
            outs = int(frac[:1] or 0)
            if outs in {0, 1, 2}:
                return round(float(int(whole)) + outs / 3.0, 3)
        return float(text)
    except (TypeError, ValueError):
        return None


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


@lru_cache(maxsize=512)
def _fetch_mlb_pitching_season_stats(pitcher_id: int, season: int) -> Dict[str, Any]:
    """Official MLB Stats API direct-on-load fallback for starter overview fields."""
    if not pitcher_id or not season:
        return {"available": False, "source": "missing_pitcher_or_season", "stat": {}}
    try:
        response = requests.get(
            f"{MLB_STATS_BASE}/people/{int(pitcher_id)}",
            params={"hydrate": f"stats(group=[pitching],type=[season],season={int(season)})"},
            timeout=8,
        )
        response.raise_for_status()
        person = (response.json().get("people") or [{}])[0]
        splits = (((person.get("stats") or [{}])[0]).get("splits") or [])
        stat = (splits[0] or {}).get("stat") if splits else {}
        return {"available": bool(stat), "source": "mlb_stats_api_people_pitching_season", "person": person, "stat": stat or {}}
    except Exception as exc:
        log.warning("MLB pitching overview fallback failed pitcher_id=%s season=%s: %s", pitcher_id, season, exc)
        return {"available": False, "source": "mlb_stats_api_people_pitching_season", "error": str(exc), "stat": {}}


def _first_stat(stats: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in stats and stats.get(key) not in {None, ""}:
            return stats.get(key)
    return None


def _put_metric(overview: Dict[str, Any], key: str, value: Any, source: str, missing_reason: str, digits: int = 3) -> None:
    overview[key] = _round_stat(value, digits=digits)
    overview.setdefault("metric_sources", {})[key] = source if overview[key] is not None else "missing"
    if overview[key] is None:
        overview.setdefault("missing_inputs", []).append(f"{key}.{missing_reason}")


def _derive_standard_pitching_metrics(stats: Dict[str, Any]) -> Dict[str, Any]:
    ip = _innings_to_float(_first_stat(stats, "inningsPitched", "ip"))
    bf = _safe_float(_first_stat(stats, "battersFaced", "totalBattersFaced"))
    so = _safe_float(_first_stat(stats, "strikeOuts", "strikeouts", "so"))
    bb = _safe_float(_first_stat(stats, "baseOnBalls", "walks", "bb"))
    hbp = _safe_float(_first_stat(stats, "hitBatsmen", "hitByPitch", "hbp")) or 0.0
    hr = _safe_float(_first_stat(stats, "homeRuns", "homeRunsAllowed", "hr"))
    hits = _safe_float(_first_stat(stats, "hits", "h"))
    runs = _safe_float(_first_stat(stats, "runs", "r"))
    at_bats = _safe_float(_first_stat(stats, "atBats", "ab"))
    sac_flies = _safe_float(_first_stat(stats, "sacFlies", "sf")) or 0.0
    ground_outs = _safe_float(_first_stat(stats, "groundOuts", "groundouts"))
    air_outs = _safe_float(_first_stat(stats, "airOuts", "flyOuts", "airouts"))
    pitches = _safe_float(_first_stat(stats, "numberOfPitches", "pitchesThrown", "pitches"))
    derived: Dict[str, Any] = {"innings_pitched": ip, "batters_faced": bf, "pitches_thrown": pitches}
    if ip and hr is not None:
        derived["hr_per_9"] = (hr * 9.0) / ip
    if bf and so is not None:
        derived["k_pct"] = so / bf
    if bf and bb is not None:
        derived["bb_pct"] = bb / bf
    if ground_outs is not None and air_outs is not None and (ground_outs + air_outs) > 0:
        derived["gb_pct"] = ground_outs / (ground_outs + air_outs)
        derived["fb_pct"] = air_outs / (ground_outs + air_outs)
    if hr is not None and air_outs is not None and air_outs > 0:
        derived["hr_fb"] = hr / air_outs
    if hits is not None and hr is not None and at_bats is not None and so is not None:
        denominator = at_bats - so - hr + sac_flies
        if denominator > 0:
            derived["babip"] = (hits - hr) / denominator
    if hits is not None and bb is not None and runs is not None and hr is not None:
        denominator = hits + bb + hbp - hr
        if denominator > 0:
            derived["lob_pct"] = (hits + bb + hbp - runs) / denominator
    return derived


def _arm_slot_label(release_pos_z: Optional[float]) -> str:
    if release_pos_z is None:
        return "unknown"
    if release_pos_z >= 6.1:
        return "high three-quarter"
    if release_pos_z >= 5.5:
        return "three-quarter"
    if release_pos_z >= 4.7:
        return "low three-quarter"
    return "sidearm"


def _pitcher_profile(pitcher_id: Optional[int], pitcher_name: Optional[str], features: Optional[Dict[str, Any]], arsenal: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    features = features or {}
    release_z = _safe_float(features.get("avg_release_pos_z"))
    return {
        "metadata": {
            "source_type": "matchup_generator_pitcher_features",
            "generated_from": "canonical_matchup_probability._pitcher_profile",
            "data_confidence": "medium" if any(v is not None for v in features.values()) else "low",
            "pitcher_id": pitcher_id,
            "pitcher_name": pitcher_name,
            "missing_inputs": [k for k, v in features.items() if v is None],
        },
        "bat_missing": {"k_rate": _pct(features.get("k_pct")), "whiff_rate": _pct(features.get("whiff_rate")), "csw_rate": _pct(features.get("csw_rate"))},
        "command_control": {"bb_rate": _pct(features.get("bb_pct")), "zone_rate": _pct(features.get("zone_rate")), "first_pitch_strike_rate": _pct(features.get("first_pitch_strike_rate"))},
        "contact_management": {
            "hard_hit_rate_allowed": _pct(features.get("hard_hit_pct")),
            "barrel_rate_allowed": _pct(features.get("barrel_pct")),
            "avg_exit_velocity_allowed": _safe_float(features.get("avg_exit_velocity")),
            "avg_launch_angle_allowed": _safe_float(features.get("avg_launch_angle")),
            "xwoba_allowed": _safe_float(features.get("xwoba")),
            "xba_allowed": _safe_float(features.get("xba")),
        },
        "arsenal": {"pitch_mix": arsenal or {}, "avg_velocity": _safe_float(features.get("avg_velocity")), "avg_spin_rate": _safe_float(features.get("avg_spin_rate"))},
        "release_profile": {
            "arm_slot_label": _arm_slot_label(release_z),
            "arm_slot_source": "derived_from_release_pos_z" if release_z is not None else "missing",
            "arm_angle": _safe_float(features.get("arm_angle")),
            "release_pos_x": _safe_float(features.get("avg_release_pos_x")),
            "release_pos_z": release_z,
            "release_extension": _safe_float(features.get("avg_release_extension")),
            "avg_horizontal_break": _safe_float(features.get("avg_horiz_break")),
            "avg_vertical_break": _safe_float(features.get("avg_vert_break")),
            "avg_velocity": _safe_float(features.get("avg_velocity")),
            "avg_spin_rate": _safe_float(features.get("avg_spin_rate")),
            "pitch_mix": arsenal or {},
            "release_consistency": None,
            "extension_advantage": None,
            "vertical_approach_angle": None,
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


def _starter_overview(profile: Dict[str, Any], season: Optional[int] = None) -> Dict[str, Any]:
    metadata = profile.get("metadata") or {}
    bat = profile.get("bat_missing") or {}
    cmd = profile.get("command_control") or {}
    contact = profile.get("contact_management") or {}
    arsenal = profile.get("arsenal") or {}
    pitcher_id = metadata.get("pitcher_id")
    fallback = _fetch_mlb_pitching_season_stats(int(pitcher_id), int(season)) if pitcher_id and season else {"available": False, "stat": {}, "source": "missing_pitcher_or_season"}
    stats = fallback.get("stat") or {}
    derived = _derive_standard_pitching_metrics(stats)
    missing: List[str] = []
    overview: Dict[str, Any] = {
        "pitcher_id": pitcher_id,
        "pitcher_name": metadata.get("pitcher_name"),
        "metric_sources": {},
        "missing_inputs": missing,
        "data_window_used": "DB 90d pitcher aggregate first; MLB Stats API season fallback for standard overview stats",
        "overview_source_priority": ["db_pitcher_aggregates_or_current_matchup_features", "mlb_stats_api_people_pitching_season", "formula_derived_from_complete_standard_stats", "missing"],
        "mlb_stats_api_available": bool(fallback.get("available")),
    }
    for key, aliases in {
        "era": ("era",),
        "whip": ("whip",),
        "wins": ("wins",),
        "losses": ("losses",),
        "games_pitched": ("gamesPitched", "games"),
        "games_started": ("gamesStarted",),
        "strikeouts": ("strikeOuts", "strikeouts"),
        "walks": ("baseOnBalls", "walks"),
        "home_runs_allowed": ("homeRuns", "homeRunsAllowed"),
        "earned_runs": ("earnedRuns",),
        "runs_allowed": ("runs",),
        "hits_allowed": ("hits",),
    }.items():
        raw = _first_stat(stats, *aliases)
        _put_metric(overview, key, raw, fallback.get("source") if raw is not None else "missing", "mlb_standard_stat_missing")
    _put_metric(overview, "k_pct", bat.get("k_rate") if bat.get("k_rate") is not None else derived.get("k_pct"), "db_or_statcast_aggregate" if bat.get("k_rate") is not None else "formula_derived_from_mlb_standard_stats", "k_pct_missing")
    _put_metric(overview, "bb_pct", cmd.get("bb_rate") if cmd.get("bb_rate") is not None else derived.get("bb_pct"), "db_or_statcast_aggregate" if cmd.get("bb_rate") is not None else "formula_derived_from_mlb_standard_stats", "bb_pct_missing")
    overview["k_minus_bb_pct"] = round(float(overview["k_pct"]) - float(overview["bb_pct"]), 4) if overview.get("k_pct") is not None and overview.get("bb_pct") is not None else None
    overview["metric_sources"]["k_minus_bb_pct"] = "formula_derived_from_k_pct_and_bb_pct" if overview.get("k_minus_bb_pct") is not None else "missing"
    if overview.get("k_minus_bb_pct") is None:
        missing.append("k_minus_bb_pct.requires_k_pct_and_bb_pct")
    hr9_api = _first_stat(stats, "homeRunsPer9")
    _put_metric(overview, "hr_per_9", hr9_api if hr9_api is not None else derived.get("hr_per_9"), fallback.get("source") if hr9_api is not None else "formula_derived_from_mlb_standard_stats", "hr_per_9_missing")
    _put_metric(overview, "gb_pct", derived.get("gb_pct"), "estimated_from_mlb_standard_groundouts_airouts", "groundouts_airouts_missing")
    _put_metric(overview, "fb_pct", derived.get("fb_pct"), "estimated_from_mlb_standard_groundouts_airouts", "groundouts_airouts_missing")
    _put_metric(overview, "hr_fb", derived.get("hr_fb"), "estimated_from_hr_and_airouts", "home_runs_or_airouts_missing")
    babip_api = _first_stat(stats, "babip")
    _put_metric(overview, "babip", babip_api if babip_api is not None else derived.get("babip"), fallback.get("source") if babip_api is not None else "formula_derived_from_mlb_standard_stats", "babip_inputs_missing")
    _put_metric(overview, "lob_pct", derived.get("lob_pct"), "estimated_from_standard_hits_walks_hbp_runs_hr", "lob_inputs_missing")
    _put_metric(overview, "xwoba_allowed", contact.get("xwoba_allowed"), "db_or_statcast_aggregate", "xwoba_missing")
    _put_metric(overview, "xba_allowed", contact.get("xba_allowed"), "db_or_statcast_aggregate", "xba_missing")
    _put_metric(overview, "hard_hit_rate_allowed", contact.get("hard_hit_rate_allowed"), "db_or_statcast_aggregate", "hard_hit_missing")
    _put_metric(overview, "barrel_rate_allowed", contact.get("barrel_rate_allowed"), "db_or_statcast_aggregate", "barrel_missing")
    _put_metric(overview, "average_exit_velocity_allowed", contact.get("avg_exit_velocity_allowed"), "db_or_statcast_aggregate", "avg_ev_missing")
    _put_metric(overview, "average_launch_angle_allowed", contact.get("avg_launch_angle_allowed"), "db_or_statcast_aggregate", "avg_la_missing")
    _put_metric(overview, "whiff_rate", bat.get("whiff_rate"), "db_or_statcast_aggregate", "whiff_rate_missing")
    _put_metric(overview, "csw_rate", bat.get("csw_rate"), "db_or_statcast_aggregate", "csw_rate_missing")
    _put_metric(overview, "avg_velocity", arsenal.get("avg_velocity"), "db_or_statcast_aggregate", "avg_velocity_missing")
    _put_metric(overview, "avg_spin_rate", arsenal.get("avg_spin_rate"), "db_or_statcast_aggregate", "avg_spin_rate_missing")
    _put_metric(overview, "innings_pitched", derived.get("innings_pitched"), fallback.get("source") if derived.get("innings_pitched") is not None else "missing", "innings_pitched_missing")
    _put_metric(overview, "batters_faced", derived.get("batters_faced"), fallback.get("source") if derived.get("batters_faced") is not None else "missing", "batters_faced_missing")
    _put_metric(overview, "pitches_thrown", derived.get("pitches_thrown"), fallback.get("source") if derived.get("pitches_thrown") is not None else "missing", "pitches_thrown_missing")
    _put_metric(overview, "pitch_count_sample_size", derived.get("pitches_thrown") or derived.get("batters_faced"), fallback.get("source") if derived.get("pitches_thrown") or derived.get("batters_faced") else "missing", "pitch_count_or_batters_faced_missing")
    for key, required in {
        "fip": "requires_hr_bb_hbp_k_ip_and_verified_season_fip_constant",
        "xfip": "requires_fly_balls_league_hr_fb_bb_hbp_k_ip_and_verified_season_constant",
        "siera": "requires_documented_formula_and_all_input_rates",
        "xsiera": "requires_existing_internal_or_trusted_external_source",
    }.items():
        overview[key] = None
        overview["metric_sources"][key] = "missing"
        missing.append(f"{key}.{required}")
    overview["release_profile"] = profile.get("release_profile") or {}
    overview["display_metrics"] = [
        {"key": key, "label": label, "value": overview.get(key), "source": overview.get("metric_sources", {}).get(key), "available": overview.get(key) is not None}
        for key, label in STARTER_OVERVIEW_FIELDS
    ]
    overview["available_metric_count"] = sum(1 for row in overview["display_metrics"] if row.get("available"))
    overview["requested_metric_count"] = len(overview["display_metrics"])
    overview["missing_inputs"] = sorted(set(missing))
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
        if obp is not None:
            values.append((obp - 0.320) * 1.8)
        if slg is not None:
            values.append((slg - 0.410) * 1.1)
        if iso is not None:
            values.append((iso - 0.160) * 1.4)
        if bb is not None and k is not None:
            values.append((bb - k + 0.140) * 0.7)
        return None if not values else sum(values) / len(values)
    home_signal, away_signal = signal(home_inputs), signal(away_inputs)
    missing = []
    if home_signal is None:
        missing.append("home_offense_inputs")
    if away_signal is None:
        missing.append("away_offense_inputs")
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
        "pitcher_overview": {"home": _starter_overview(home_pitcher, season=season), "away": _starter_overview(away_pitcher, season=season)},
        "batter_vs_arsenal_schema_version": BATTER_VS_ARSENAL_SCHEMA_VERSION,
        "batter_vs_arsenal_summary": {
            "schema_version": BATTER_VS_ARSENAL_SCHEMA_VERSION,
            "bat_tracking_adjustment": (components.get("batter_vs_arsenal") or {}).get("diagnostics", {}).get("bat_tracking_adjustment"),
            "bat_tracking_data_confidence": (components.get("batter_vs_arsenal") or {}).get("diagnostics", {}).get("bat_tracking_data_confidence", "missing"),
            "missing_inputs": (components.get("batter_vs_arsenal") or {}).get("missing_inputs", []),
        },
        "missing_inputs": missing_inputs,
    }


__all__ = ["BATTER_VS_ARSENAL_SCHEMA_VERSION", "CANONICAL_MODEL_VERSION", "LEGACY_MODEL_VERSION", "STARTER_OVERVIEW_FIELDS", "compute_canonical_matchup_probability"]
