from __future__ import annotations

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
from mlb_app.simulation.pa_outcome_model import build_pa_outcome_probabilities
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
) -> Dict[str, Any]:
    model = build_pa_outcome_probabilities(
        batter_profile=offense_profile,
        pitcher_profile=opposing_pitcher_profile,
        environment_profile=environment_profile,
    )
    return {
        "model_version": "shared-pa-outcome-v1",
        "side": side,
        "lineup_average_probabilities": model.get("probabilities") or {},
        "lineup_average_summary": model.get("summary") or {},
        "source_model": model,
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
            return {
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
    model = build_pa_outcome_probabilities(
        batter_profile=offense_profile,
        pitcher_profile=opposing_bullpen_profile,
        environment_profile=environment_profile,
    )
    return {
        "model_version": "shared-bullpen-pa-outcome-v1",
        "side": side,
        "lineup_average_probabilities": model.get("probabilities") or {},
        "lineup_average_summary": model.get("summary") or {},
        "source_model": model,
    }


def _build_bullpen_adjusted_game_sim(
    away_starter_pa_model: Dict[str, Any],
    home_starter_pa_model: Dict[str, Any],
    away_bullpen_pa_model: Dict[str, Any],
    home_bullpen_pa_model: Dict[str, Any],
    simulations: int,
    seed: int,
    starter_innings: int,
) -> Dict[str, Any]:
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
    return {
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
        return {
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

    session_factory = _session_factory()

    with session_factory() as session:
        matchups = generate_matchups_for_date(session, target_date)
        matchup = _pick_game(matchups, int(game_pk))

        if matchup is None:
            # Fallback: search a small date window because game_pk may be from a
            # debug date rather than today's slate.
            today = datetime.date.fromisoformat(target_date)
            for offset in range(-7, 8):
                candidate_date = (today + datetime.timedelta(days=offset)).isoformat()
                matchups = generate_matchups_for_date(session, candidate_date)
                matchup = _pick_game(matchups, int(game_pk))
                if matchup is not None:
                    target_date = candidate_date
                    break

        if matchup is None:
            return {
                "status": "missing_game",
                "game_pk": game_pk,
                "searched_date": target_date,
                "simulation_count": simulations,
                "seed": seed,
                "meta": {
                    "engine": ENGINE_VERSION,
                    "version": "v1",
                    "model_version": "shared-simulation-v1",
                    "source_builder": "mlb_app.simulation.game_simulation_builder",
                    "missing_inputs": ["game_pk_not_found_in_generated_matchups"],
                },
            }

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

        away_offense_profile = _offense_workspace_profile(away_team)
        home_offense_profile = _offense_workspace_profile(home_team)

        away_pitcher_profile = _pitcher_workspace_profile(away_team)
        home_pitcher_profile = _pitcher_workspace_profile(home_team)

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
            team_id=home_team.get("team_id"),
            team_name=home_team.get("team_name"),
        )

        away_bullpen_profile = build_bullpen_profile(
            team_id=away_team.get("team_id"),
            team_name=away_team.get("team_name"),
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

        sources = _source_summary(matchup, environment_profile)

        return {
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
