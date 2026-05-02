from typing import Dict, Any, List

from .simulation.pa_outcome_model import build_pa_outcome_probabilities
from .simulation.inning_simulator import simulate_half_innings
from .simulation.game_simulator import simulate_game, simulate_game_with_bullpen


def _average_probability_dict(models: List[Dict[str, Any]]) -> Dict[str, float]:
    totals = {}
    count = 0
    for model in models or []:
        probs = model.get("probabilities") or {}
        if not probs:
            continue
        count += 1
        for k, v in probs.items():
            if v is None:
                continue
            totals[k] = totals.get(k, 0.0) + float(v)
    if not count:
        return {}
    return {k: round(v / count, 4) for k, v in totals.items()}


def _average_summary_dict(models: List[Dict[str, Any]]) -> Dict[str, float]:
    totals = {}
    count = 0
    for model in models or []:
        summary = model.get("summary") or {}
        if not summary:
            continue
        count += 1
        for k, v in summary.items():
            if v is None:
                continue
            totals[k] = totals.get(k, 0.0) + float(v)
    if not count:
        return {}
    return {k: round(v / count, 4) for k, v in totals.items()}


def build_lineup_pa_outcome_model(lineup, lineup_profile, opposing_pitcher_profile, environment_profile, side_label):
    player_models = []

    for player in lineup or []:
        model = build_pa_outcome_probabilities(
            batter_profile=lineup_profile,
            pitcher_profile=opposing_pitcher_profile,
            environment_profile=environment_profile,
        )
        player_models.append({
            "player_id": player.get("id"),
            "player_name": player.get("name"),
            "probabilities": model.get("probabilities"),
            "summary": model.get("summary"),
        })

    return {
        "model_version": "lineup_pa_outcome_v1",
        "side": side_label,
        "player_count_used": len(player_models),
        "lineup_average_probabilities": _average_probability_dict(player_models),
        "lineup_average_summary": _average_summary_dict(player_models),
        "player_outcomes": player_models,
    }


def build_bullpen_pa_outcome_model(lineup_profile, bullpen_profile, environment_profile, side_label):
    model = build_pa_outcome_probabilities(
        batter_profile=lineup_profile,
        pitcher_profile=bullpen_profile,
        environment_profile=environment_profile,
    )

    return {
        "model_version": "bullpen_pa_outcome_v1",
        "side": side_label,
        "lineup_average_probabilities": model.get("probabilities"),
        "lineup_average_summary": model.get("summary"),
    }


def build_game_simulation(away_pa, home_pa):
    return simulate_game(
        away_probabilities=away_pa.get("lineup_average_probabilities") or {},
        home_probabilities=home_pa.get("lineup_average_probabilities") or {},
        simulations=5000,
        seed=42,
        innings=9,
    )


def build_bullpen_adjusted_game_simulation(away_sp, home_sp, away_bp, home_bp):
    return simulate_game_with_bullpen(
        away_starter_probabilities=away_sp.get("lineup_average_probabilities") or {},
        home_starter_probabilities=home_sp.get("lineup_average_probabilities") or {},
        away_bullpen_probabilities=away_bp.get("lineup_average_probabilities") or {},
        home_bullpen_probabilities=home_bp.get("lineup_average_probabilities") or {},
        simulations=5000,
        seed=42,
        innings=9,
        starter_innings=5,
    )