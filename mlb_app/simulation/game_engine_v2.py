from __future__ import annotations

import copy
from mlb_app.simulation.formula_map import build_formula_map

import datetime
import inspect
import os
from typing import Any, Dict, Mapping, Optional

from mlb_app.database import create_tables, get_engine, get_session
from mlb_app.matchup_generator import generate_matchups_for_date
from mlb_app.model_projections import (
    _offense_workspace_profile,
    _pitcher_workspace_profile,
)
from mlb_app.environment_profile import compute_environment_profile
from mlb_app.bullpen_profile import build_bullpen_profile
from mlb_app.simulation.inning_simulator import simulate_half_innings
from mlb_app.simulation.game_simulator import simulate_game, simulate_game_with_bullpen


ENGINE_VERSION = "game-engine-v2"


def _session_factory():
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    return get_session(engine)


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _pick_game(matchups: list[Dict[str, Any]], game_pk: int) -> Optional[Dict[str, Any]]:
    for game in matchups or []:
        if str(game.get("game_pk") or game.get("gamePk")) == str(game_pk):
            return game
    return None


def _game_date_from_matchup(matchup: Dict[str, Any]) -> str:
    value = (
        matchup.get("game_date")
        or matchup.get("gameDate")
        or matchup.get("date")
        or datetime.date.today().isoformat()
    )
    return str(value)[:10]


def _starter_quality_score_or_default(value):
    if isinstance(value, dict):
        score = value.get("score")
    else:
        score = value
    try:
        return float(score)
    except (TypeError, ValueError):
        return 0.0


def _build_pa_model(
    *,
    offense_profile: Dict[str, Any],
    opposing_pitcher_profile: Dict[str, Any],
    environment_profile: Dict[str, Any],
    side: str,
    pitcher_role: str = "starter",
) -> Dict[str, Any]:
    """
    Transparent PA probability model.

    Converts offense, opposing pitcher, and environment inputs into normalized
    plate appearance outcome probabilities used by the game simulator.
    """

    offense_profile = offense_profile or {}
    opposing_pitcher_profile = opposing_pitcher_profile or {}
    environment_profile = environment_profile or {}

    def pick(source: Dict[str, Any], keys, default):
        def values_from_nested(obj):
            if not isinstance(obj, dict):
                return
            for value in obj.values():
                if isinstance(value, dict):
                    yield from values_from_nested(value)
                else:
                    yield value

        for key in keys:
            value = source.get(key)
            if value is not None:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return default

        for section in source.values():
            if not isinstance(section, dict):
                continue
            for key in keys:
                value = section.get(key)
                if value is not None:
                    try:
                        return float(value)
                    except (TypeError, ValueError):
                        return default

        return default

    def clamp(value: float, low: float, high: float) -> float:
        return max(low, min(high, value))

    # -------------------------
    # Direct model inputs
    # -------------------------
    off_k = pick(offense_profile, ["k_rate", "k_pct", "strikeout_rate", "strikeout_pct"], 0.22)
    off_bb = pick(offense_profile, ["bb_rate", "bb_pct", "walk_rate", "walk_pct"], 0.08)
    off_avg = pick(offense_profile, ["batting_avg", "avg", "hit_rate"], 0.250)
    off_iso = pick(offense_profile, ["iso", "isolated_power"], 0.170)
    off_slg = pick(offense_profile, ["slugging_pct", "slg"], 0.410)

    pit_k = pick(opposing_pitcher_profile, ["k_rate", "k_pct", "strikeout_rate", "strikeout_pct"], 0.22)
    pit_bb = pick(opposing_pitcher_profile, ["bb_rate", "bb_pct", "walk_rate", "walk_pct"], 0.08)
    pit_xba = pick(opposing_pitcher_profile, ["xba_allowed", "xba", "batting_avg_allowed"], 0.250)
    pit_xwoba = pick(opposing_pitcher_profile, ["xwoba_allowed", "xwoba"], 0.320)
    pit_hard_hit = pick(opposing_pitcher_profile, ["hard_hit_rate_allowed", "hard_hit_pct", "hard_hit_rate"], 0.38)
    pit_hr = pick(opposing_pitcher_profile, ["hr_rate", "home_run_rate", "hr_per_pa"], 0.030)

    run_env = pick(environment_profile, ["run_scoring_index"], 1.0)
    hr_env = pick(environment_profile, ["hr_boost_index"], 1.0)
    hit_env = pick(environment_profile, ["hit_boost_index"], 1.0)

    role = (pitcher_role or "starter").lower()

    if role == "bullpen":
        # Bullpen profile is a staff aggregate, so shrink slightly more toward offense/league priors.
        k_off_w, k_pitch_w = 0.55, 0.45
        bb_off_w, bb_pitch_w = 0.55, 0.45
        hit_off_w, hit_pitch_w, xwoba_w = 0.58, 0.32, 0.10
        power_iso_w, power_slg_w, power_hard_hit_w, power_xwoba_w = 0.46, 0.22, 0.18, 0.14
    else:
        # Starter profile is more pitcher-specific, especially for K/contact management.
        k_off_w, k_pitch_w = 0.45, 0.55
        bb_off_w, bb_pitch_w = 0.50, 0.50
        hit_off_w, hit_pitch_w, xwoba_w = 0.50, 0.40, 0.10
        power_iso_w, power_slg_w, power_hard_hit_w, power_xwoba_w = 0.40, 0.20, 0.25, 0.15

    # -------------------------
    # Probability blends
    # -------------------------
    k_prob = clamp((k_off_w * off_k) + (k_pitch_w * pit_k), 0.12, 0.36)
    bb_prob = clamp((bb_off_w * off_bb) + (bb_pitch_w * pit_bb), 0.04, 0.15)

    # Center pitcher xwOBA around league-average run quality before using it
    # as a hit-probability adjustment. The previous formulation subtracted
    # 0.070, which made xwOBA behave like a second batting-average input and
    # inflated hit rates / suppressed contact outs.
    xwoba_hit_adjustment = xwoba_w * (pit_xwoba - 0.320)
    base_hit_prob = (hit_off_w * off_avg) + (hit_pitch_w * pit_xba) + xwoba_hit_adjustment
    hit_prob = clamp(base_hit_prob * hit_env * (0.98 + 0.02 * run_env), 0.17, 0.32)

    power_index = (
        (power_iso_w * off_iso)
        + (power_slg_w * max(off_slg - 0.300, 0.0))
        + (power_hard_hit_w * pit_hard_hit)
        + (power_xwoba_w * pit_xwoba)
    )

    hr_prob = clamp(((0.70 * pit_hr) + (0.30 * power_index * 0.12)) * hr_env, 0.010, 0.075)
    hr_prob = min(hr_prob, hit_prob * 0.35)

    non_hr_hit_prob = max(hit_prob - hr_prob, 0.0)

    single_prob = non_hr_hit_prob * 0.72
    double_prob = non_hr_hit_prob * 0.24
    triple_prob = non_hr_hit_prob * 0.04

    hbp_prob = 0.010
    roe_prob = 0.007

    used = (
        k_prob
        + bb_prob
        + hbp_prob
        + single_prob
        + double_prob
        + triple_prob
        + hr_prob
        + roe_prob
    )

    contact_out_prob = max(0.0, 1.0 - used)

    total = (
        k_prob
        + bb_prob
        + hbp_prob
        + single_prob
        + double_prob
        + triple_prob
        + hr_prob
        + roe_prob
        + contact_out_prob
    )

    def norm(value: float) -> float:
        return value / total if total > 0 else 0.0

    probabilities = {
        "strikeout": norm(k_prob),
        "walk": norm(bb_prob),
        "hit_by_pitch": norm(hbp_prob),
        "single": norm(single_prob),
        "double": norm(double_prob),
        "triple": norm(triple_prob),
        "home_run": norm(hr_prob),
        "reached_on_error": norm(roe_prob),
        "out": norm(contact_out_prob),
    }

    summary = {
        "k_rate": probabilities["strikeout"],
        "bb_rate": probabilities["walk"],
        "hit_rate": (
            probabilities["single"]
            + probabilities["double"]
            + probabilities["triple"]
            + probabilities["home_run"]
        ),
        "hr_rate": probabilities["home_run"],
        "xbh_rate": (
            probabilities["double"]
            + probabilities["triple"]
            + probabilities["home_run"]
        ),
        "out_rate": probabilities["out"],
    }

    return {
        "model_version": "transparent-pa-model-v1",
        "side": side,
        "lineup_average_probabilities": probabilities,
        "lineup_average_summary": summary,
        "direct_inputs": {
            "offense": {
                "k_rate": off_k,
                "bb_rate": off_bb,
                "batting_avg": off_avg,
                "iso": off_iso,
                "slugging_pct": off_slg,
            },
            "pitcher": {
                "k_rate": pit_k,
                "bb_rate": pit_bb,
                "xba_allowed": pit_xba,
                "xwoba_allowed": pit_xwoba,
                "hard_hit_rate_allowed": pit_hard_hit,
                "hr_rate": pit_hr,
            },
            "environment": {
                "run_scoring_index": run_env,
                "hr_boost_index": hr_env,
                "hit_boost_index": hit_env,
            },
            "role_weighting": {
                "pitcher_role": role,
                "k_weights": {"offense": k_off_w, "pitcher": k_pitch_w},
                "bb_weights": {"offense": bb_off_w, "pitcher": bb_pitch_w},
                "hit_weights": {"offense": hit_off_w, "pitcher_xba": hit_pitch_w, "pitcher_xwoba": xwoba_w},
                "power_weights": {
                    "offense_iso": power_iso_w,
                    "offense_slg": power_slg_w,
                    "pitcher_hard_hit": power_hard_hit_w,
                    "pitcher_xwoba": power_xwoba_w,
                },
            },
        },
    }


def _build_half_inning(pa_model: Dict[str, Any], side: str, simulations: int, seed: int) -> Dict[str, Any]:
    try:
        return simulate_half_innings(
            probabilities=pa_model.get("lineup_average_probabilities") or {},
            simulations=simulations,
            seed=seed,
        )
    except TypeError:
        try:
            return simulate_half_innings(
                pa_model.get("lineup_average_probabilities") or {},
                simulations=simulations,
                seed=seed,
            )
        except TypeError:
            payload = {
                "status": "skipped",
                "side": side,
                "reason": "simulate_half_innings signature mismatch",
            }


def _build_game_sim(
    away_pa_model: Dict[str, Any],
    home_pa_model: Dict[str, Any],
    simulations: int,
    seed: int,
) -> Dict[str, Any]:
    return simulate_game(
        away_probabilities=away_pa_model.get("lineup_average_probabilities") or {},
        home_probabilities=home_pa_model.get("lineup_average_probabilities") or {},
        simulations=simulations,
        seed=seed,
        innings=9,
    )


def _build_bullpen_pa_model(
    *,
    offense_profile: Dict[str, Any],
    opposing_bullpen_profile: Dict[str, Any],
    environment_profile: Dict[str, Any],
    side: str,
) -> Dict[str, Any]:
    payload = _build_pa_model(
        offense_profile=offense_profile,
        opposing_pitcher_profile=opposing_bullpen_profile,
        environment_profile=environment_profile,
        side=side,
        pitcher_role="bullpen",
    )

    payload["model_version"] = "transparent-bullpen-pa-model-v1"
    return payload



def _starter_quality_score(pitcher_profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a starter profile into an explainable quality score.
    Higher score means the starter is expected to cover more innings.
    """
    pitcher_profile = pitcher_profile or {}

    def pick(keys, default):
        for key in keys:
            value = pitcher_profile.get(key)
            if value is not None:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return default
        return default

    def clamp(value, low=0.0, high=1.0):
        return max(low, min(high, value))

    k_rate = pick(["k_rate", "k_pct", "strikeout_rate", "strikeout_pct"], 0.22)
    bb_rate = pick(["bb_rate", "bb_pct", "walk_rate", "walk_pct"], 0.08)
    xwoba = pick(["xwoba_allowed", "xwoba"], 0.320)
    hard_hit = pick(["hard_hit_rate_allowed", "hard_hit_pct", "hard_hit_rate"], 0.38)
    xba = pick(["xba_allowed", "xba", "batting_avg_allowed"], 0.250)

    components = {
        "k_component": clamp((k_rate - 0.18) / (0.32 - 0.18)),
        "bb_component": clamp((0.12 - bb_rate) / (0.12 - 0.04)),
        "xwoba_component": clamp((0.380 - xwoba) / (0.380 - 0.260)),
        "hard_hit_component": clamp((0.460 - hard_hit) / (0.460 - 0.300)),
        "xba_component": clamp((0.300 - xba) / (0.300 - 0.210)),
    }

    score = (
        0.30 * components["k_component"]
        + 0.18 * components["bb_component"]
        + 0.27 * components["xwoba_component"]
        + 0.15 * components["hard_hit_component"]
        + 0.10 * components["xba_component"]
    )
    score = clamp(score)

    if score >= 0.78:
        label = "elite"
    elif score >= 0.62:
        label = "above_average"
    elif score >= 0.45:
        label = "average"
    elif score >= 0.30:
        label = "below_average"
    else:
        label = "low_quality"

    return {
        "score": round(score, 4),
        "label": label,
        "components": components,
        "inputs": {
            "k_rate": k_rate,
            "bb_rate": bb_rate,
            "xwoba_allowed": xwoba,
            "hard_hit_rate_allowed": hard_hit,
            "xba_allowed": xba,
        },
    }


def _expected_starter_innings(quality: Dict[str, Any]) -> float:
    """
    Map starter quality score to expected innings.
    First version uses a conservative 4.3 to 6.4 inning range.
    """
    score = float((quality or {}).get("score") or 0.45)
    innings = 4.3 + (score * 2.1)
    innings = max(3.8, min(6.7, innings))
    return round(innings, 2)


def _build_bullpen_adjusted_game_sim(
    away_starter_pa_model,
    home_starter_pa_model,
    away_bullpen_pa_model,
    home_bullpen_pa_model,
    simulations,
    seed,
    starter_innings,
    away_starter_quality=None,
    home_starter_quality=None,
    dynamic_starter_exit=True,
):
    # ✅ normalize inside function (NOT in signature)
    away_starter_pa_model = away_starter_pa_model or {}
    home_starter_pa_model = home_starter_pa_model or {}
    away_bullpen_pa_model = away_bullpen_pa_model or {}
    home_bullpen_pa_model = home_bullpen_pa_model or {}

    return simulate_game_with_bullpen(
        away_starter_probabilities=away_starter_pa_model.get("lineup_average_probabilities") or {},
        home_starter_probabilities=home_starter_pa_model.get("lineup_average_probabilities") or {},
        away_bullpen_probabilities=away_bullpen_pa_model.get("lineup_average_probabilities") or {},
        home_bullpen_probabilities=home_bullpen_pa_model.get("lineup_average_probabilities") or {},
        simulations=simulations,
        seed=seed,
        innings=9,
        starter_innings=starter_innings,
        away_starter_quality=away_starter_quality,
        home_starter_quality=home_starter_quality,
        dynamic_starter_exit=dynamic_starter_exit,
    )

def _source_summary(matchup: Dict[str, Any], environment_profile: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
        "offense_source": "model_projections._offense_workspace_profile",
        "pitcher_source": "model_projections._pitcher_workspace_profile",
        "bullpen_source": "bullpen_profile.build_bullpen_profile",
        "environment_source": (
            environment_profile.get("metadata", {}).get("source")
            if isinstance(environment_profile, dict)
            else None
        ) or "environment_profile.compute_environment_profile",
        "matchup_source": "matchup_generator.generate_matchups_for_date",
        "game_pk": matchup.get("game_pk") or matchup.get("gamePk"),
    }
    return payload



def _compute_environment_profile_compatible(game_context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compatibility wrapper around compute_environment_profile while the app's
    environment API stabilizes.
    """
    try:
        signature = inspect.signature(compute_environment_profile)
        params = signature.parameters

        if "game_context" in params:
            return compute_environment_profile(game_context=game_context)

        kwargs = {}
        if "game" in params:
            kwargs["game"] = game_context
        if "game_info" in params:
            kwargs["game_info"] = game_context
        if "matchup" in params:
            kwargs["matchup"] = game_context
        if "weather" in params:
            kwargs["weather"] = game_context.get("weather") or {}
        if "venue" in params:
            kwargs["venue"] = game_context.get("venue")
        if "game_date" in params:
            kwargs["game_date"] = game_context.get("game_date")
        if "game_pk" in params:
            kwargs["game_pk"] = game_context.get("game_pk")

        if kwargs:
            return compute_environment_profile(**kwargs)

        # Positional fallback.
        return compute_environment_profile(game_context)

    except Exception as exc:
        payload = {
            "metadata": {
                "source": "environment_profile.compute_environment_profile",
                "status": "error",
                "error": str(exc),
            },
            "run_scoring_index": 1.0,
            "hr_boost_index": 1.0,
            "hit_boost_index": 1.0,
            "raw_context": game_context,
        }



def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _extract_handedness_mix_from_offense(offense_profile: Mapping[str, Any]) -> dict[str, Any] | None:
    if not isinstance(offense_profile, Mapping):
        return None

    for key in ("lineup_handedness_mix", "handedness_mix", "lineup_handedness"):
        value = offense_profile.get(key)
        if isinstance(value, Mapping):
            return dict(value)

    diagnostics = offense_profile.get("diagnostics")
    if isinstance(diagnostics, Mapping):
        for key in ("lineup_handedness_mix", "handedness_mix"):
            value = diagnostics.get(key)
            if isinstance(value, Mapping):
                return dict(value)

    return None


def _apply_handedness_weighted_hr_adjustment(
    environment_profile: Mapping[str, Any],
    offense_profile: Mapping[str, Any],
) -> dict[str, Any]:
    """Apply conservative side-specific HR boost adjustment if handedness mix exists.

    No DB/session access. If handedness mix is unavailable, returns unchanged copy.
    """
    adjusted = copy.deepcopy(dict(environment_profile))

    run_environment = dict(adjusted.get("run_environment") or {})
    components = adjusted.get("environment_components") or {}
    park_component = components.get("park_component") or {}

    try:
        base_hr_boost = float(run_environment.get("hr_boost_index"))
    except Exception:
        base_hr_boost = 1.0

    try:
        generic = float(park_component.get("home_run_factor"))
    except Exception:
        generic = 1.0

    lhb = park_component.get("home_run_factor_lhb")
    rhb = park_component.get("home_run_factor_rhb")
    mix = _extract_handedness_mix_from_offense(offense_profile)

    diagnostics = {
        "active_model_input_changed": False,
        "generic_home_run_factor": round(generic, 4),
        "home_run_factor_lhb": lhb,
        "home_run_factor_rhb": rhb,
        "base_hr_boost_index": round(base_hr_boost, 4),
        "adjusted_hr_boost_index": round(base_hr_boost, 4),
        "weighted_home_run_factor_raw": round(generic, 4),
        "handedness_adjustment_raw": 1.0,
        "handedness_adjustment_final": 1.0,
        "fallback_used": True,
        "fallback_reason": None,
    }

    if not mix:
        diagnostics["fallback_reason"] = "missing_lineup_handedness_mix"
        adjusted["handedness_weighted_hr_diagnostics"] = diagnostics
        return adjusted

    counts = mix.get("counts") or {}
    l_count = int(counts.get("L") or 0)
    r_count = int(counts.get("R") or 0)
    s_count = int(counts.get("S") or 0)
    unknown_count = int(counts.get("unknown") or 0)
    total = l_count + r_count + s_count + unknown_count

    diagnostics["handedness_counts"] = {
        "L": l_count,
        "R": r_count,
        "S": s_count,
        "unknown": unknown_count,
    }
    diagnostics["handedness_coverage_rate"] = mix.get("coverage_rate")

    if total <= 0:
        diagnostics["fallback_reason"] = "empty_lineup_handedness_mix"
        adjusted["handedness_weighted_hr_diagnostics"] = diagnostics
        return adjusted

    if lhb is None or rhb is None:
        diagnostics["fallback_reason"] = "missing_lhr_rhr_park_factors"
        adjusted["handedness_weighted_hr_diagnostics"] = diagnostics
        return adjusted

    try:
        lhb = float(lhb)
        rhb = float(rhb)
    except Exception:
        diagnostics["fallback_reason"] = "invalid_lhr_rhr_park_factors"
        adjusted["handedness_weighted_hr_diagnostics"] = diagnostics
        return adjusted

    weighted_raw = (
        (l_count * lhb)
        + (r_count * rhb)
        + (s_count * generic)
        + (unknown_count * generic)
    ) / total

    raw_adjustment = weighted_raw / generic if generic else 1.0
    final_adjustment = _clamp(1.0 + (0.50 * (raw_adjustment - 1.0)), 0.97, 1.03)
    adjusted_hr_boost = base_hr_boost * final_adjustment

    run_environment["hr_boost_index"] = round(adjusted_hr_boost, 4)
    adjusted["run_environment"] = run_environment

    diagnostics.update({
        "active_model_input_changed": True,
        "weighted_home_run_factor_raw": round(weighted_raw, 4),
        "handedness_adjustment_raw": round(raw_adjustment, 4),
        "handedness_adjustment_final": round(final_adjustment, 4),
        "adjusted_hr_boost_index": round(adjusted_hr_boost, 4),
        "fallback_used": False,
        "fallback_reason": None,
    })

    if s_count:
        diagnostics["switch_hitter_strategy"] = "generic_home_run_factor"
    if unknown_count:
        diagnostics["unknown_hitter_strategy"] = "generic_home_run_factor"

    adjusted["handedness_weighted_hr_diagnostics"] = diagnostics
    return adjusted

def run_full_game_simulation(game_pk: int, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    config = config or {}

    simulations = int(config.get("simulation_count") or 5000)
    seed = int(config.get("seed") or 42)
    starter_innings_override = config.get("starter_innings")

    requested_date = config.get("date")
    target_date = str(requested_date or datetime.date.today().isoformat())[:10]

    # Prefer an already-built matchup object from the caller. This keeps the
    # engine from trying to rediscover a game that the route already has.
    matchup = None
    game_date = None

    matchup_config = config.get("matchup") or {}
    if isinstance(matchup_config, dict):
        matchup = matchup_config.get("raw")
        game_date = matchup_config.get("game_date")

    # Fallback only when caller did not provide matchup context.
    if matchup is None:
        session_factory = _session_factory()

        with session_factory() as session:
            matchups = generate_matchups_for_date(session, target_date)
            matchup = _pick_game(matchups, int(game_pk))

            if matchup is None:
                today = datetime.date.fromisoformat(target_date)

                for offset in range(-7, 8):
                    candidate_date = (today + datetime.timedelta(days=offset)).isoformat()
                    matchups = generate_matchups_for_date(session, candidate_date)
                    matchup = _pick_game(matchups, int(game_pk))

                    if matchup is not None:
                        target_date = candidate_date
                        break

    if matchup is None:
        payload = {
            "status": "missing_game",
            "error": f"Matchup not found for game_pk={game_pk}",
            "game_pk": game_pk,
            "meta": {
                "engine": ENGINE_VERSION,
                "source_builder": "mlb_app.simulation.game_simulation_builder",
            },
            "derived_outputs": {},
            "direct_inputs": {},
        }

        payload["formulaMap"] = build_formula_map(payload)
        return payload

    if not game_date:
        game_date = _game_date_from_matchup(matchup)

    away_team = {
        "team_id": matchup.get("away_team_id") or matchup.get("awayTeamId"),
        "team_name": matchup.get("away_team_name") or matchup.get("awayTeamName") or matchup.get("away_team"),
        "pitcher_id": matchup.get("away_pitcher_id") or matchup.get("awayPitcherId"),
        "pitcher_name": matchup.get("away_pitcher_name") or matchup.get("awayPitcherName"),
        "pitcher_features": matchup.get("away_pitcher_features") or {},
        "pitch_arsenal": matchup.get("away_pitch_arsenal") or matchup.get("away_pitcher_arsenal") or {},
        "pitch_arsenal_source": matchup.get("away_pitch_arsenal_source"),
        "offense_inputs": matchup.get("away_offense_inputs") or matchup.get("away_team_offense") or {},
    }

    home_team = {
        "team_id": matchup.get("home_team_id") or matchup.get("homeTeamId"),
        "team_name": matchup.get("home_team_name") or matchup.get("homeTeamName") or matchup.get("home_team"),
        "pitcher_id": matchup.get("home_pitcher_id") or matchup.get("homePitcherId"),
        "pitcher_name": matchup.get("home_pitcher_name") or matchup.get("homePitcherName"),
        "pitcher_features": matchup.get("home_pitcher_features") or {},
        "pitch_arsenal": matchup.get("home_pitch_arsenal") or matchup.get("home_pitcher_arsenal") or {},
        "pitch_arsenal_source": matchup.get("home_pitch_arsenal_source"),
        "offense_inputs": matchup.get("home_offense_inputs") or matchup.get("home_team_offense") or {},
    }

    venue_name = matchup.get("venue_name") or matchup.get("venue")

    environment_profile = _compute_environment_profile_compatible({
        "game_pk": game_pk,
        "game_date": game_date,
        "venue_name": venue_name,
        "venue": venue_name,
        "weather": matchup.get("weather") or {},
        "matchup": matchup,
    })

    # Side-specific environment profiles are intentionally identical copies for now.
    # This creates a safe structure for future lineup-handedness HR adjustments without
    # changing current model outputs.
    away_offense_environment_profile = copy.deepcopy(environment_profile)
    home_offense_environment_profile = copy.deepcopy(environment_profile)
    side_specific_environment_diagnostics = {
        "side_specific_environment_enabled": True,
        "side_specific_environment_adjustment_source": "copied_from_game_environment",
        "active_model_input_changed": False,
    }

    away_offense_profile = _offense_workspace_profile(away_team) or {}
    home_offense_profile = _offense_workspace_profile(home_team) or {}

    away_offense_environment_profile = _apply_handedness_weighted_hr_adjustment(
        away_offense_environment_profile,
        away_offense_profile,
    )
    home_offense_environment_profile = _apply_handedness_weighted_hr_adjustment(
        home_offense_environment_profile,
        home_offense_profile,
    )
    side_specific_environment_diagnostics = {
        "side_specific_environment_enabled": True,
        "side_specific_environment_adjustment_source": "handedness_weighted_hr_if_available",
        "active_model_input_changed": bool(
            (away_offense_environment_profile.get("handedness_weighted_hr_diagnostics") or {}).get("active_model_input_changed")
            or (home_offense_environment_profile.get("handedness_weighted_hr_diagnostics") or {}).get("active_model_input_changed")
        ),
        "away_hr_diagnostics": away_offense_environment_profile.get("handedness_weighted_hr_diagnostics"),
        "home_hr_diagnostics": home_offense_environment_profile.get("handedness_weighted_hr_diagnostics"),
    }

    away_pitcher_profile = _pitcher_workspace_profile(away_team) or {}
    home_pitcher_profile = _pitcher_workspace_profile(home_team) or {}

    away_starter_quality = _starter_quality_score(away_pitcher_profile)
    home_starter_quality = _starter_quality_score(home_pitcher_profile)

    away_expected_starter_innings = (
        float(starter_innings_override)
        if starter_innings_override is not None
        else _expected_starter_innings(away_starter_quality)
    )
    home_expected_starter_innings = (
        float(starter_innings_override)
        if starter_innings_override is not None
        else _expected_starter_innings(home_starter_quality)
    )

    # Current simulator accepts one shared starter_innings value.
    # Use average until side-specific starter innings are supported.
    starter_innings = round(
        (away_expected_starter_innings + home_expected_starter_innings) / 2,
        2,
    )

    # Away offense faces home pitcher.
    away_starter_pa = _build_pa_model(
        offense_profile=away_offense_profile,
        opposing_pitcher_profile=home_pitcher_profile,
        environment_profile=away_offense_environment_profile,
        side="away_offense_vs_home_starter",
    )

    # Home offense faces away pitcher.
    home_starter_pa = _build_pa_model(
        offense_profile=home_offense_profile,
        opposing_pitcher_profile=away_pitcher_profile,
        environment_profile=home_offense_environment_profile,
        side="home_offense_vs_away_starter",
    )

    away_half_inning = _build_half_inning(
        away_starter_pa,
        side="away",
        simulations=simulations,
        seed=seed,
    )

    home_half_inning = _build_half_inning(
        home_starter_pa,
        side="home",
        simulations=simulations,
        seed=seed + 1,
    )

    game_simulation = _build_game_sim(
        away_starter_pa,
        home_starter_pa,
        simulations=simulations,
        seed=seed,
    )

    home_bullpen_profile = build_bullpen_profile(
        team_id=home_team.get("team_id") or {},
        team_name=home_team.get("team_name") or {},
    )

    away_bullpen_profile = build_bullpen_profile(
        team_id=away_team.get("team_id") or {},
        team_name=away_team.get("team_name") or {},
    )

    away_vs_home_bullpen_pa = _build_bullpen_pa_model(
        offense_profile=away_offense_profile,
        opposing_bullpen_profile=home_bullpen_profile,
        environment_profile=away_offense_environment_profile,
        side="away_offense_vs_home_bullpen",
    )

    home_vs_away_bullpen_pa = _build_bullpen_pa_model(
        offense_profile=home_offense_profile,
        opposing_bullpen_profile=away_bullpen_profile,
        environment_profile=home_offense_environment_profile,
        side="home_offense_vs_away_bullpen",
    )

    bullpen_adjusted_game_simulation = _build_bullpen_adjusted_game_sim(
        away_starter_pa,
        home_starter_pa,
        away_vs_home_bullpen_pa,
        home_vs_away_bullpen_pa,
        simulations=simulations,
        seed=seed,
        starter_innings=starter_innings,
        away_starter_quality=_starter_quality_score_or_default(away_starter_quality),
        home_starter_quality=_starter_quality_score_or_default(home_starter_quality),
        dynamic_starter_exit=True,
    )

    sources = _source_summary(matchup, environment_profile) or {}

    payload = {
        "status": "ok",
        "game_pk": game_pk,
        "game_date": game_date,
        "simulation_count": simulations,
        "seed": seed,
        "engine_version": ENGINE_VERSION,
        "teams": {
            "away": {
                "team_id": away_team.get("team_id"),
                "team_name": away_team.get("team_name"),
                "pitcher_id": away_team.get("pitcher_id"),
                "pitcher_name": away_team.get("pitcher_name"),
            },
            "home": {
                "team_id": home_team.get("team_id"),
                "team_name": home_team.get("team_name"),
                "pitcher_id": home_team.get("pitcher_id"),
                "pitcher_name": home_team.get("pitcher_name"),
            },
        },
        "direct_inputs": {
            "away_offense_profile": away_offense_profile,
            "home_offense_profile": home_offense_profile,
            "away_pitcher_profile": away_pitcher_profile,
            "home_pitcher_profile": home_pitcher_profile,
            "away_bullpen_profile": away_bullpen_profile,
            "home_bullpen_profile": home_bullpen_profile,
            "environment_profile": environment_profile,
            "away_offense_environment_profile": away_offense_environment_profile,
            "home_offense_environment_profile": home_offense_environment_profile,
            "side_specific_environment_diagnostics": side_specific_environment_diagnostics,
        },
        "pa_models": {
            "away_vs_home_starter": away_starter_pa,
            "home_vs_away_starter": home_starter_pa,
            "away_vs_home_bullpen": away_vs_home_bullpen_pa,
            "home_vs_away_bullpen": home_vs_away_bullpen_pa,
        },
        "derived_outputs": {
            "away_half_inning_simulation": away_half_inning,
            "home_half_inning_simulation": home_half_inning,
            "game_simulation": game_simulation,
            "bullpen_adjusted_game_simulation": bullpen_adjusted_game_simulation,
        },
        "diagnostics": {
            "sources": sources,
            "missing_inputs": [],
            "config": config,
            "starter_exit_model": {
                "away_starter_quality": away_starter_quality,
                "home_starter_quality": home_starter_quality,
                "away_expected_starter_innings": away_expected_starter_innings,
                "home_expected_starter_innings": home_expected_starter_innings,
                "simulator_starter_innings": starter_innings,
                "note": "simulate_game_with_bullpen now receives starter quality scores and samples starter exit innings dynamically per simulation.",
            },
        },
        "meta": {
            "engine": ENGINE_VERSION,
            "version": "v1",
            "model_version": "shared-simulation-v1",
            "source_builder": "mlb_app.simulation.game_simulation_builder",
            "simulation_count": simulations,
            "seed": seed,
            "starter_exit_enabled": True,
            "starter_quality_score": None,
            "starter_quality_label": None,
            "calibration_version": "calibration-v1",
            **sources,
        },
    }

    payload["formulaMap"] = build_formula_map(payload)
    return payload
