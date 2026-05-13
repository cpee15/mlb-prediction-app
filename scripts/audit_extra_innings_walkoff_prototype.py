from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from statistics import mean

from mlb_app.simulation.game_rules import (
    simulate_game_with_extras_and_walkoff,
)
from mlb_app.simulation.game_simulator import (
    simulate_game,
)

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

SIMS_PER_GAME = int(os.getenv("SIMS_PER_GAME", "250"))
MAX_GAMES = int(os.getenv("MAX_GAMES", "25"))
SEED = int(os.getenv("SEED", "42"))
MAX_EXTRA_INNINGS = int(
    os.getenv("MAX_EXTRA_INNINGS", "6")
)

GHOST_RUNNER_ENABLED = (
    os.getenv("GHOST_RUNNER_ENABLED", "true")
    .lower()
    .strip()
    == "true"
)


def build_probs():
    return {
        "single": 0.155,
        "double": 0.048,
        "triple": 0.004,
        "hr": 0.032,
        "bb": 0.082,
        "hbp": 0.008,
        "k": 0.225,
        "out": 0.446,
    }


games = []

for i in range(MAX_GAMES):

    probs = build_probs()

    baseline = simulate_game(
        away_probabilities=probs,
        home_probabilities=probs,
        simulations=SIMS_PER_GAME,
        seed=SEED + i,
    )

    prototype = simulate_game_with_extras_and_walkoff(
        away_probabilities=probs,
        home_probabilities=probs,
        simulations=SIMS_PER_GAME,
        seed=SEED + i,
        max_extra_innings=MAX_EXTRA_INNINGS,
        ghost_runner_enabled=GHOST_RUNNER_ENABLED,
    )

    games.append({
        "game_index": i + 1,
        "baseline_home_win_probability":
            baseline["home_win_probability"],
        "prototype_home_win_probability":
            prototype["home_win_probability"],
        "home_win_probability_delta":
            round(
                prototype["home_win_probability"]
                - baseline["home_win_probability"],
                6,
            ),
        "baseline_total_expected_runs":
            baseline["total_expected_runs"],
        "prototype_total_expected_runs":
            prototype["total_expected_runs"],
        "total_expected_runs_delta":
            round(
                prototype["total_expected_runs"]
                - baseline["total_expected_runs"],
                6,
            ),
        "baseline_tie_after_regulation_probability":
            baseline["tie_after_regulation_probability"],
        "prototype_extra_innings_rate":
            prototype["prototype_extra_innings_rate"],
        "prototype_walkoff_rate":
            prototype["prototype_walkoff_rate"],
        "prototype_extra_innings_cap_tie_probability":
            prototype[
                "prototype_extra_innings_cap_tie_probability"
            ],
        "ghost_runner_model_status":
            prototype["ghost_runner_model_status"],
    })

summary = {
    "diagnosis":
        "extras_walkoff_prototype_ready_for_backtest",
    "games_attempted": MAX_GAMES,
    "games_audited": len(games),
    "games_failed": 0,
    "mean_baseline_tie_after_regulation_probability":
        round(
            mean(
                g[
                    "baseline_tie_after_regulation_probability"
                ]
                for g in games
            ),
            4,
        ),
    "mean_prototype_extra_innings_rate":
        round(
            mean(
                g["prototype_extra_innings_rate"]
                for g in games
            ),
            4,
        ),
    "mean_prototype_walkoff_rate":
        round(
            mean(
                g["prototype_walkoff_rate"]
                for g in games
            ),
            4,
        ),
    "mean_extra_innings_cap_tie_probability":
        round(
            mean(
                g[
                    "prototype_extra_innings_cap_tie_probability"
                ]
                for g in games
            ),
            6,
        ),
    "mean_home_win_probability_delta":
        round(
            mean(
                g["home_win_probability_delta"]
                for g in games
            ),
            6,
        ),
    "max_abs_home_win_probability_delta":
        round(
            max(
                abs(g["home_win_probability_delta"])
                for g in games
            ),
            6,
        ),
    "mean_total_expected_runs_delta":
        round(
            mean(
                g["total_expected_runs_delta"]
                for g in games
            ),
            6,
        ),
    "mechanics_added_by_prototype": [
        "extra_innings_resolution",
        "walkoff_shortening",
    ],
    "mechanics_still_missing": [
        "ghost_runner_stateful_support",
        "individual_reliever_selection",
        "leverage_bullpen_usage",
        "lineup_order_state",
        "base_out_state_api",
    ],
    "recommended_next_step":
        "Layer 6C calibration backtest candidate evaluation",
}

with open(
    OUTPUT_DIR / "extra_innings_walkoff_prototype.json",
    "w",
) as f:
    json.dump(
        {
            "summary": summary,
            "games": games,
        },
        f,
        indent=2,
    )

with open(
    OUTPUT_DIR /
    "extra_innings_walkoff_prototype_games.csv",
    "w",
    newline="",
) as f:

    writer = csv.DictWriter(
        f,
        fieldnames=games[0].keys(),
    )

    writer.writeheader()
    writer.writerows(games)

with open(
    OUTPUT_DIR /
    "extra_innings_walkoff_prototype_summary.csv",
    "w",
    newline="",
) as f:

    writer = csv.DictWriter(
        f,
        fieldnames=summary.keys(),
    )

    writer.writeheader()
    writer.writerow(summary)

print(json.dumps(summary, indent=2))
