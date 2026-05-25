from __future__ import annotations

import csv
import json
import os
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, List

from mlb_app.simulation.game_simulator import simulate_game_with_bullpen


OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
BACKTEST_END = os.getenv("BACKTEST_END", "2026-05-03")

SIMS_PER_GAME = int(os.getenv("SIMS_PER_GAME", "250"))
SEED = int(os.getenv("SEED", "42"))
MAX_GAMES = int(os.getenv("MAX_GAMES", "25"))


def safe_mean(values):
    values = [v for v in values if isinstance(v, (int, float))]
    return round(mean(values), 4) if values else None


def aggregate_distribution(distributions: List[Dict[str, float]]) -> Dict[str, float]:
    counter = defaultdict(float)

    for dist in distributions:
        for key, value in dist.items():
            try:
                counter[str(key)] += float(value)
            except Exception:
                continue

    total = sum(counter.values())

    if total <= 0:
        return {}

    return {
        key: round(value / total, 4)
        for key, value in sorted(counter.items(), key=lambda x: int(x[0]))
    }


def build_fake_probabilities():
    return {
        "single": 0.155,
        "double": 0.048,
        "triple": 0.004,
        "home_run": 0.032,
        "walk": 0.082,
        "hit_by_pitch": 0.008,
        "strikeout": 0.225,
        "out": 0.446,
    }


def simulate_reference_game(seed_offset: int):
    starter_probs = build_fake_probabilities()

    bullpen_probs = {
        "single": 0.145,
        "double": 0.043,
        "triple": 0.003,
        "home_run": 0.027,
        "walk": 0.078,
        "hit_by_pitch": 0.006,
        "strikeout": 0.248,
        "out": 0.45,
    }

    return simulate_game_with_bullpen(
        away_starter_probabilities=starter_probs,
        home_starter_probabilities=starter_probs,
        away_bullpen_probabilities=bullpen_probs,
        home_bullpen_probabilities=bullpen_probs,
        simulations=SIMS_PER_GAME,
        seed=SEED + seed_offset,
        innings=9,
        starter_innings=5,
        away_starter_quality=0.35,
        home_starter_quality=0.15,
        dynamic_starter_exit=True,
    )


games = []
failures = []

for i in range(MAX_GAMES):
    try:
        result = simulate_reference_game(i)

        metadata = result.get("metadata", {})

        game_row = {
            "game_index": i + 1,
            "model_version": result.get("model_version"),
            "simulations": result.get("simulations"),
            "innings": result.get("innings"),
            "dynamic_starter_exit": result.get("dynamic_starter_exit"),
            "starter_innings": result.get("starter_innings"),
            "away_starter_quality": result.get("away_starter_quality"),
            "home_starter_quality": result.get("home_starter_quality"),
            "away_starter_quality_score": result.get("away_starter_quality_score"),
            "home_starter_quality_score": result.get("home_starter_quality_score"),
            "away_starter_innings_distribution": json.dumps(
                result.get("away_starter_innings_distribution", {})
            ),
            "home_starter_innings_distribution": json.dumps(
                result.get("home_starter_innings_distribution", {})
            ),
            "bullpen_innings": result.get("bullpen_innings"),
            "tie_after_regulation_probability": result.get(
                "tie_after_regulation_probability"
            ),
            "raw_away_expected_runs": result.get("raw_away_expected_runs"),
            "raw_home_expected_runs": result.get("raw_home_expected_runs"),
            "raw_total_expected_runs": result.get("raw_total_expected_runs"),
            "away_expected_runs": result.get("away_expected_runs"),
            "home_expected_runs": result.get("home_expected_runs"),
            "total_expected_runs": result.get("total_expected_runs"),
            "home_win_probability": result.get("home_win_probability"),
            "away_win_probability": result.get("away_win_probability"),
            "has_extras_model": False,
            "has_walkoff_model": False,
            "has_individual_reliever_model": False,
            "has_leverage_bullpen_model": False,
            "has_lineup_order_model": False,
            "has_base_out_state_model": False,
            "metadata_notes": json.dumps(metadata.get("notes", [])),
        }

        games.append(game_row)

    except Exception as exc:
        failures.append(
            {
                "game_index": i + 1,
                "error": str(exc),
            }
        )


tie_probs = [
    g["tie_after_regulation_probability"]
    for g in games
]

home_edges = [
    abs(g["home_win_probability"] - 0.5)
    for g in games
    if isinstance(g["home_win_probability"], (int, float))
]

away_dists = [
    json.loads(g["away_starter_innings_distribution"])
    for g in games
]

home_dists = [
    json.loads(g["home_starter_innings_distribution"])
    for g in games
]

mechanics_missing = [
    "extras",
    "walkoff_shortening",
    "individual_reliever_selection",
    "leverage_bullpen_usage",
    "lineup_order_state",
    "base_out_state",
]

mechanics_active = [
    "starter_vs_bullpen_transition",
    "dynamic_starter_exit",
    "team_run_distribution",
    "win_probability_calibration",
]

recommended_layer_6b_priority = (
    "Layer 6B — extras and walk-off mechanics audit/prototype"
)

summary = {
    "diagnosis": (
        "gameplay_baseline_audit_complete_missing_core_mechanics"
    ),
    "backtest_window": {
        "start": BACKTEST_START,
        "end": BACKTEST_END,
    },
    "games_attempted": MAX_GAMES,
    "games_audited": len(games),
    "games_failed": len(failures),
    "mean_tie_after_regulation_probability": safe_mean(tie_probs),
    "max_tie_after_regulation_probability": (
        round(max(tie_probs), 4) if tie_probs else None
    ),
    "median_tie_after_regulation_probability": (
        round(median(tie_probs), 4) if tie_probs else None
    ),
    "mean_total_expected_runs": safe_mean(
        [g["total_expected_runs"] for g in games]
    ),
    "mean_raw_total_expected_runs": safe_mean(
        [g["raw_total_expected_runs"] for g in games]
    ),
    "mean_home_win_probability": safe_mean(
        [g["home_win_probability"] for g in games]
    ),
    "mean_abs_home_win_edge_from_50": safe_mean(home_edges),
    "away_starter_exit_distribution_aggregate": aggregate_distribution(
        away_dists
    ),
    "home_starter_exit_distribution_aggregate": aggregate_distribution(
        home_dists
    ),
    "mechanics_active": mechanics_active,
    "mechanics_missing": mechanics_missing,
    "recommended_layer_6b_priority": (
        recommended_layer_6b_priority
    ),
    "output_files": [
        "tmp/gameplay_realism_baseline.json",
        "tmp/gameplay_realism_baseline_games.csv",
        "tmp/gameplay_realism_baseline_summary.csv",
    ],
}

with open(
    OUTPUT_DIR / "gameplay_realism_baseline.json",
    "w",
) as f:
    json.dump(
        {
            "summary": summary,
            "games": games,
            "failures": failures,
        },
        f,
        indent=2,
    )

if games:
    with open(
        OUTPUT_DIR / "gameplay_realism_baseline_games.csv",
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
    OUTPUT_DIR / "gameplay_realism_baseline_summary.csv",
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
