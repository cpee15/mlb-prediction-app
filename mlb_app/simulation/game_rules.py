from __future__ import annotations

import random
from collections import Counter
from typing import Any, Dict, Optional

from .inning_simulator import simulate_half_inning
from .game_simulator import (
    _distribution,
    _calibrate_expected_runs,
    _calibrate_win_probability,
)


def simulate_game_with_extras_and_walkoff(
    away_probabilities: Dict[str, float],
    home_probabilities: Dict[str, float],
    simulations: int = 5000,
    seed: Optional[int] = None,
    innings: int = 9,
    max_extra_innings: int = 6,
    ghost_runner_enabled: bool = True,
) -> Dict[str, Any]:

    rng = random.Random(seed)

    away_runs_counter = Counter()
    home_runs_counter = Counter()
    total_runs_counter = Counter()

    away_wins = 0
    home_wins = 0

    extra_innings_games = 0
    walkoff_games = 0
    capped_tie_games = 0

    for _ in range(simulations):

        away_runs = 0
        home_runs = 0

        # regulation innings 1-8
        for _inning in range(1, innings):

            away_half = simulate_half_inning(
                away_probabilities,
                rng=rng,
            )

            home_half = simulate_half_inning(
                home_probabilities,
                rng=rng,
            )

            away_runs += away_half["runs"]
            home_runs += home_half["runs"]

        # top 9
        away_half = simulate_half_inning(
            away_probabilities,
            rng=rng,
        )

        away_runs += away_half["runs"]

        # bottom 9 only if needed
        if home_runs <= away_runs:

            home_half = simulate_half_inning(
                home_probabilities,
                rng=rng,
            )

            home_runs += home_half["runs"]

            if home_runs > away_runs:
                walkoff_games += 1

        # extras
        if away_runs == home_runs:

            extra_innings_games += 1

            resolved = False

            for _extra in range(max_extra_innings):

                away_half = simulate_half_inning(
                    away_probabilities,
                    rng=rng,
                )

                away_runs += away_half["runs"]

                if home_runs <= away_runs:

                    home_half = simulate_half_inning(
                        home_probabilities,
                        rng=rng,
                    )

                    home_runs += home_half["runs"]

                    if home_runs > away_runs:
                        walkoff_games += 1

                if away_runs != home_runs:
                    resolved = True
                    break

            if not resolved:
                capped_tie_games += 1

                if rng.random() < 0.5:
                    away_runs += 1
                else:
                    home_runs += 1

        away_runs_counter[away_runs] += 1
        home_runs_counter[home_runs] += 1
        total_runs_counter[away_runs + home_runs] += 1

        if home_runs > away_runs:
            home_wins += 1
        else:
            away_wins += 1

    raw_away_expected_runs = (
        sum(runs * count for runs, count in away_runs_counter.items())
        / simulations
    )

    raw_home_expected_runs = (
        sum(runs * count for runs, count in home_runs_counter.items())
        / simulations
    )

    away_expected_runs = _calibrate_expected_runs(
        raw_away_expected_runs
    )

    home_expected_runs = _calibrate_expected_runs(
        raw_home_expected_runs
    )

    total_expected_runs = round(
        away_expected_runs + home_expected_runs,
        4,
    )

    raw_home_win_probability = home_wins / simulations
    raw_away_win_probability = away_wins / simulations

    home_win_probability = _calibrate_win_probability(
        raw_home_win_probability
    )

    away_win_probability = _calibrate_win_probability(
        raw_away_win_probability
    )

    return {
        "model_version": "extras_walkoff_prototype_v1",
        "simulations": simulations,
        "innings": innings,
        "max_extra_innings": max_extra_innings,
        "ghost_runner_enabled": ghost_runner_enabled,
        "ghost_runner_model_status": (
            "not_supported_by_current_half_inning_api"
        ),
        "walkoff_shortening_enabled": True,
        "prototype_extra_innings_rate": round(
            extra_innings_games / simulations,
            4,
        ),
        "prototype_walkoff_rate": round(
            walkoff_games / simulations,
            4,
        ),
        "prototype_extra_innings_cap_tie_probability": round(
            capped_tie_games / simulations,
            4,
        ),
        "away_expected_runs": away_expected_runs,
        "home_expected_runs": home_expected_runs,
        "total_expected_runs": total_expected_runs,
        "raw_away_expected_runs": round(
            raw_away_expected_runs,
            4,
        ),
        "raw_home_expected_runs": round(
            raw_home_expected_runs,
            4,
        ),
        "away_win_probability": round(
            away_win_probability,
            4,
        ),
        "home_win_probability": round(
            home_win_probability,
            4,
        ),
        "away_run_distribution": _distribution(
            away_runs_counter,
            simulations,
        ),
        "home_run_distribution": _distribution(
            home_runs_counter,
            simulations,
        ),
        "total_run_distribution": _distribution(
            total_runs_counter,
            simulations,
        ),
    }
