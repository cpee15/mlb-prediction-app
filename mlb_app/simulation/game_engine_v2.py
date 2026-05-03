from __future__ import annotations
from mlb_app.simulation.formula_map import build_formula_map

import datetime
import inspect
import os
from typing import Any, Dict, Optional

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
        for key in keys:
            value = source.get(key)
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

    base_hit_prob = (hit_off_w * off_avg) + (hit_pitch_w * pit_xba) + (xwoba_w * (pit_xwoba - 0.070))
    hit_prob = clamp(base_hit_prob * hit_env * (0.98 + 0.02 * run_env), 0.17, 0.36)

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


def _build_bullpen_adjusted_game_sim(
    away_starter_pa_model,
    home_starter_pa_model,
    away_bullpen_pa_model,
    home_bullpen_pa_model,
    simulations,
    seed,
    starter_innings,
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


def run_full_game_simulation(game_pk: int, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    config = config or {}

    simulations = int(config.get("simulation_count") or 5000)
    seed = int(config.get("seed") or 42)
    starter_innings = int(config.get("starter_innings") or 5)

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

    environment_profile = _compute_environment_profile_compatible({
        "game_pk": game_pk,
        "game_date": game_date,
        "venue": matchup.get("venue_name") or matchup.get("venue"),
        "weather": matchup.get("weather") or {},
        "matchup": matchup,
    })

    away_offense_profile = _offense_workspace_profile(away_team) or {}
    home_offense_profile = _offense_workspace_profile(home_team) or {}

    away_pitcher_profile = _pitcher_workspace_profile(away_team) or {}
    home_pitcher_profile = _pitcher_workspace_profile(home_team) or {}

    # Away offense faces home pitcher.
    away_starter_pa = _build_pa_model(
        offense_profile=away_offense_profile,
        opposing_pitcher_profile=home_pitcher_profile,
        environment_profile=environment_profile,
        side="away_offense_vs_home_starter",
    )

    # Home offense faces away pitcher.
    home_starter_pa = _build_pa_model(
        offense_profile=home_offense_profile,
        opposing_pitcher_profile=away_pitcher_profile,
        environment_profile=environment_profile,
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
        environment_profile=environment_profile,
        side="away_offense_vs_home_bullpen",
    )

    home_vs_away_bullpen_pa = _build_bullpen_pa_model(
        offense_profile=home_offense_profile,
        opposing_bullpen_profile=away_bullpen_profile,
        environment_profile=environment_profile,
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
