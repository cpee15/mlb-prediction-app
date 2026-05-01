"""
Full-game simulation engine.

Uses PA outcome probability distributions for each offense to simulate
nine-inning run distributions and game-level probabilities.

V1 is intentionally conservative:
- fixed nine innings for both teams
- no walk-off shortening
- no extras
- no bullpen transition yet
- independent half-innings using one lineup-level PA distribution per team
"""

from __future__ import annotations

import random
from collections import Counter
from typing import Any, Dict, Optional

from .inning_simulator import simulate_half_inning


def _distribution(counter: Counter, simulations: int) -> Dict[str, float]:
    return {
        str(value): round(count / simulations, 4)
        for value, count in sorted(counter.items())
    }


def _prob_greater_than(counter: Counter, threshold: float, simulations: int) -> float:
    return round(
        sum(count for value, count in counter.items() if value > threshold) / simulations,
        4,
    )


def _prob_less_than(counter: Counter, threshold: float, simulations: int) -> float:
    return round(
        sum(count for value, count in counter.items() if value < threshold) / simulations,
        4,
    )


def _prob_at_least(counter: Counter, threshold: int, simulations: int) -> float:
    return round(
        sum(count for value, count in counter.items() if value >= threshold) / simulations,
        4,
    )


GAME_SIM_CALIBRATION = {
    "game_sim_calibration_version": "game_sim_calibration_v1",
    # V1 sim uses starter/lineup-average PA distributions for all 9 innings.
    # This shrinkage tempers that known inflation until bullpen/lineup-order
    # state is modeled explicitly.
    "starter_only_run_shrinkage": 0.92,
    # Pull extreme team totals modestly toward a neutral MLB-ish scoring level.
    "team_run_regression_anchor": 4.45,
    "team_run_regression_weight": 0.12,
}


def _calibrate_expected_runs(raw_runs: float) -> float:
    shrinkage = GAME_SIM_CALIBRATION["starter_only_run_shrinkage"]
    anchor = GAME_SIM_CALIBRATION["team_run_regression_anchor"]
    regression_weight = GAME_SIM_CALIBRATION["team_run_regression_weight"]

    shrunk = raw_runs * shrinkage
    calibrated = (shrunk * (1.0 - regression_weight)) + (anchor * regression_weight)
    return round(calibrated, 4)


def simulate_game(
    away_probabilities: Dict[str, float],
    home_probabilities: Dict[str, float],
    simulations: int = 5000,
    seed: Optional[int] = None,
    innings: int = 9,
) -> Dict[str, Any]:
    rng = random.Random(seed)

    away_runs_counter = Counter()
    home_runs_counter = Counter()
    total_runs_counter = Counter()
    margin_counter = Counter()

    away_wins = 0
    home_wins = 0
    ties_after_regulation = 0

    for _ in range(simulations):
        away_runs = 0
        home_runs = 0

        for _inning in range(innings):
            away_half = simulate_half_inning(away_probabilities, rng=rng)
            home_half = simulate_half_inning(home_probabilities, rng=rng)
            away_runs += away_half["runs"]
            home_runs += home_half["runs"]

        total_runs = away_runs + home_runs
        margin = home_runs - away_runs

        away_runs_counter[away_runs] += 1
        home_runs_counter[home_runs] += 1
        total_runs_counter[total_runs] += 1
        margin_counter[margin] += 1

        if home_runs > away_runs:
            home_wins += 1
        elif away_runs > home_runs:
            away_wins += 1
        else:
            ties_after_regulation += 1

    raw_away_expected_runs = sum(runs * count for runs, count in away_runs_counter.items()) / simulations
    raw_home_expected_runs = sum(runs * count for runs, count in home_runs_counter.items()) / simulations
    raw_total_expected_runs = sum(runs * count for runs, count in total_runs_counter.items()) / simulations

    away_expected_runs = _calibrate_expected_runs(raw_away_expected_runs)
    home_expected_runs = _calibrate_expected_runs(raw_home_expected_runs)
    total_expected_runs = round(away_expected_runs + home_expected_runs, 4)

    # V1 has no extras. Split regulation ties evenly for a rough win-probability estimate.
    home_win_probability = (home_wins + (ties_after_regulation * 0.5)) / simulations
    away_win_probability = (away_wins + (ties_after_regulation * 0.5)) / simulations

    common_totals = [6.5, 7.5, 8.5, 9.5, 10.5]
    total_probabilities = {
        f"over_{line}": _prob_greater_than(total_runs_counter, line, simulations)
        for line in common_totals
    }
    total_probabilities.update({
        f"under_{line}": _prob_less_than(total_runs_counter, line, simulations)
        for line in common_totals
    })

    return {
        "model_version": "full_game_sim_v1",
        "simulations": simulations,
        "innings": innings,
        "away_expected_runs": away_expected_runs,
        "home_expected_runs": home_expected_runs,
        "total_expected_runs": total_expected_runs,
        "raw_away_expected_runs": round(raw_away_expected_runs, 4),
        "raw_home_expected_runs": round(raw_home_expected_runs, 4),
        "raw_total_expected_runs": round(raw_total_expected_runs, 4),
        "away_win_probability": round(away_win_probability, 4),
        "home_win_probability": round(home_win_probability, 4),
        "tie_after_regulation_probability": round(ties_after_regulation / simulations, 4),
        "away_run_distribution": _distribution(away_runs_counter, simulations),
        "home_run_distribution": _distribution(home_runs_counter, simulations),
        "total_run_distribution": _distribution(total_runs_counter, simulations),
        "home_margin_distribution": _distribution(margin_counter, simulations),
        "total_probabilities": total_probabilities,
        "team_total_probabilities": {
            "away_3_plus": _prob_at_least(away_runs_counter, 3, simulations),
            "away_4_plus": _prob_at_least(away_runs_counter, 4, simulations),
            "away_5_plus": _prob_at_least(away_runs_counter, 5, simulations),
            "home_3_plus": _prob_at_least(home_runs_counter, 3, simulations),
            "home_4_plus": _prob_at_least(home_runs_counter, 4, simulations),
            "home_5_plus": _prob_at_least(home_runs_counter, 5, simulations),
        },
        "metadata": {
            "generated_from": "simulation.game_simulator.simulate_game",
            "seed": seed,
            "simulation_count": simulations,
            "notes": [
                "V1 uses fixed nine innings for both teams.",
                "Ties after regulation are split evenly for win probability.",
                "No bullpen, extras, walk-off shortening, or lineup-order state yet.",
            ],
        },
    }

def simulate_game_with_bullpen(
    away_starter_probabilities: Dict[str, float],
    home_starter_probabilities: Dict[str, float],
    away_bullpen_probabilities: Dict[str, float],
    home_bullpen_probabilities: Dict[str, float],
    simulations: int = 5000,
    seed: Optional[int] = None,
    innings: int = 9,
    starter_innings: int = 5,
) -> Dict[str, Any]:
    """
    Simulate a game with separate early-inning starter probabilities and
    late-inning bullpen probabilities.

    Naming convention:
    - away_* probabilities describe the away offense.
    - home_* probabilities describe the home offense.
    """
    rng = random.Random(seed)

    away_runs_counter = Counter()
    home_runs_counter = Counter()
    total_runs_counter = Counter()
    margin_counter = Counter()

    away_wins = 0
    home_wins = 0
    ties_after_regulation = 0

    for _ in range(simulations):
        away_runs = 0
        home_runs = 0

        for inning_index in range(1, innings + 1):
            away_probs = (
                away_starter_probabilities
                if inning_index <= starter_innings
                else away_bullpen_probabilities
            )
            home_probs = (
                home_starter_probabilities
                if inning_index <= starter_innings
                else home_bullpen_probabilities
            )

            away_half = simulate_half_inning(away_probs, rng=rng)
            home_half = simulate_half_inning(home_probs, rng=rng)
            away_runs += away_half["runs"]
            home_runs += home_half["runs"]

        total_runs = away_runs + home_runs
        margin = home_runs - away_runs

        away_runs_counter[away_runs] += 1
        home_runs_counter[home_runs] += 1
        total_runs_counter[total_runs] += 1
        margin_counter[margin] += 1

        if home_runs > away_runs:
            home_wins += 1
        elif away_runs > home_runs:
            away_wins += 1
        else:
            ties_after_regulation += 1

    raw_away_expected_runs = sum(runs * count for runs, count in away_runs_counter.items()) / simulations
    raw_home_expected_runs = sum(runs * count for runs, count in home_runs_counter.items()) / simulations
    raw_total_expected_runs = sum(runs * count for runs, count in total_runs_counter.items()) / simulations

    away_expected_runs = _calibrate_expected_runs(raw_away_expected_runs)
    home_expected_runs = _calibrate_expected_runs(raw_home_expected_runs)
    total_expected_runs = round(away_expected_runs + home_expected_runs, 4)

    home_win_probability = (home_wins + (ties_after_regulation * 0.5)) / simulations
    away_win_probability = (away_wins + (ties_after_regulation * 0.5)) / simulations

    common_totals = [6.5, 7.5, 8.5, 9.5, 10.5]
    total_probabilities = {
        f"over_{line}": _prob_greater_than(total_runs_counter, line, simulations)
        for line in common_totals
    }
    total_probabilities.update({
        f"under_{line}": _prob_less_than(total_runs_counter, line, simulations)
        for line in common_totals
    })

    return {
        "model_version": "full_game_sim_with_bullpen_v1",
        "simulations": simulations,
        "innings": innings,
        "starter_innings": starter_innings,
        "bullpen_innings": max(0, innings - starter_innings),
        "away_expected_runs": away_expected_runs,
        "home_expected_runs": home_expected_runs,
        "total_expected_runs": total_expected_runs,
        "raw_away_expected_runs": round(raw_away_expected_runs, 4),
        "raw_home_expected_runs": round(raw_home_expected_runs, 4),
        "raw_total_expected_runs": round(raw_total_expected_runs, 4),
        "away_win_probability": round(away_win_probability, 4),
        "home_win_probability": round(home_win_probability, 4),
        "tie_after_regulation_probability": round(ties_after_regulation / simulations, 4),
        "away_run_distribution": _distribution(away_runs_counter, simulations),
        "home_run_distribution": _distribution(home_runs_counter, simulations),
        "total_run_distribution": _distribution(total_runs_counter, simulations),
        "home_margin_distribution": _distribution(margin_counter, simulations),
        "total_probabilities": total_probabilities,
        "team_total_probabilities": {
            "away_3_plus": _prob_at_least(away_runs_counter, 3, simulations),
            "away_4_plus": _prob_at_least(away_runs_counter, 4, simulations),
            "away_5_plus": _prob_at_least(away_runs_counter, 5, simulations),
            "home_3_plus": _prob_at_least(home_runs_counter, 3, simulations),
            "home_4_plus": _prob_at_least(home_runs_counter, 4, simulations),
            "home_5_plus": _prob_at_least(home_runs_counter, 5, simulations),
        },
        "metadata": {
            "generated_from": "simulation.game_simulator.simulate_game_with_bullpen",
            "seed": seed,
            "simulation_count": simulations,
            "starter_innings": starter_innings,
            "bullpen_innings": max(0, innings - starter_innings),
            **GAME_SIM_CALIBRATION,
            "calibration_applied_to": ["expected_runs"],
            "notes": [
                "V1 uses starter PA probabilities for early innings and bullpen PA probabilities for late innings.",
                "Ties after regulation are split evenly for win probability.",
                "No extras, walk-off shortening, individual reliever selection, or lineup-order state yet.",
            ],
        },
    }

