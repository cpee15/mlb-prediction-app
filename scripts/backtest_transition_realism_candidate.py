from __future__ import annotations

import csv
import json
import os
import random
from pathlib import Path
from statistics import mean

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.matchup_generator import generate_matchups_for_date
from mlb_app.simulation.inning_simulator import (
    simulate_half_inning,
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///mlb.db",
)

BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
BACKTEST_END = os.getenv("BACKTEST_END", "2026-05-03")

MAX_GAMES = os.getenv("MAX_GAMES")
MAX_GAMES = int(MAX_GAMES) if MAX_GAMES else None

SEED = int(os.getenv("SEED", "42"))

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

BASE_STATES = [
    ("runner_on_second", (False, True, False), 0),
    ("runner_on_third", (False, False, True), 0),
    ("runner_on_second_1_out", (False, True, False), 1),
    ("runner_on_third_1_out", (False, False, True), 1),
]

PROBABILITIES = {
    "k": 0.225,
    "bb": 0.085,
    "hbp": 0.011,
    "single": 0.145,
    "double": 0.045,
    "triple": 0.004,
    "hr": 0.030,
    "reached_on_error": 0.007,
    "out": 0.448,
}


def estimate_runs(
    transition_mode,
    bases,
    outs,
):

    runs = []

    for idx in range(5000):

        result = simulate_half_inning(
            PROBABILITIES,
            rng=random.Random(SEED + idx),
            initial_bases=bases,
            initial_outs=outs,
            transition_mode=transition_mode,
        )

        runs.append(result["runs"])

    return round(mean(runs), 6)


run_expectancy_rows = []

for transition_mode in [
    "current",
    "realism_candidate",
]:

    for state_name, bases, outs in BASE_STATES:

        expected_runs = estimate_runs(
            transition_mode,
            bases,
            outs,
        )

        run_expectancy_rows.append(
            {
                "transition_mode": transition_mode,
                "state_name": state_name,
                "expected_runs": expected_runs,
            }
        )


def lookup(
    transition_mode,
    state_name,
):

    for row in run_expectancy_rows:

        if (
            row["transition_mode"] == transition_mode
            and row["state_name"] == state_name
        ):
            return row["expected_runs"]

    raise KeyError((transition_mode, state_name))


monotonicity_rows = []

for transition_mode in [
    "current",
    "realism_candidate",
]:

    second_0 = lookup(
        transition_mode,
        "runner_on_second",
    )

    third_0 = lookup(
        transition_mode,
        "runner_on_third",
    )

    second_1 = lookup(
        transition_mode,
        "runner_on_second_1_out",
    )

    third_1 = lookup(
        transition_mode,
        "runner_on_third_1_out",
    )

    monotonicity_rows.append(
        {
            "transition_mode": transition_mode,
            "runner_on_third_gt_second_0_outs": (
                third_0 > second_0
            ),
            "runner_on_third_gt_second_1_out": (
                third_1 > second_1
            ),
            "delta_0_outs": round(
                third_0 - second_0,
                6,
            ),
            "delta_1_out": round(
                third_1 - second_1,
                6,
            ),
        }
    )

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

games_attempted = 0

with Session() as session:

    for game_date in [
        BACKTEST_START,
        BACKTEST_END,
    ]:

        matchups = generate_matchups_for_date(
            session,
            game_date,
        )

        for matchup in matchups:

            games_attempted += 1

            if (
                MAX_GAMES is not None
                and games_attempted >= MAX_GAMES
            ):
                break

large_shift_count = 0

candidate_0 = lookup(
    "realism_candidate",
    "runner_on_third",
)

current_0 = lookup(
    "current",
    "runner_on_third",
)

if abs(candidate_0 - current_0) > 0.75:
    large_shift_count += 1

candidate_repair = all(
    row["runner_on_third_gt_second_0_outs"]
    and row["runner_on_third_gt_second_1_out"]
    for row in monotonicity_rows
    if row["transition_mode"] == "realism_candidate"
)

if candidate_repair and large_shift_count == 0:
    diagnosis = (
        "transition_candidate_improves_ordering_and_preserves_calibration"
    )
elif candidate_repair:
    diagnosis = (
        "transition_candidate_overcorrects_run_environment"
    )
else:
    diagnosis = (
        "transition_candidate_no_material_improvement"
    )

summary = {
    "diagnosis": diagnosis,
    "games_attempted": games_attempted,
    "large_shift_count": large_shift_count,
    "candidate_repair": candidate_repair,
    "recommended_next_step": (
        "layer_6k_transition_parameter_sensitivity"
    ),
}

with open(
    OUTPUT_DIR / "transition_realism_candidate.json",
    "w",
) as handle:
    json.dump(
        {
            "summary": summary,
            "run_expectancy": run_expectancy_rows,
            "monotonicity": monotonicity_rows,
        },
        handle,
        indent=2,
    )

with open(
    OUTPUT_DIR / "transition_realism_candidate_run_expectancy.csv",
    "w",
    newline="",
) as handle:

    writer = csv.DictWriter(
        handle,
        fieldnames=run_expectancy_rows[0].keys(),
    )

    writer.writeheader()
    writer.writerows(run_expectancy_rows)

with open(
    OUTPUT_DIR / "transition_realism_candidate_monotonicity.csv",
    "w",
    newline="",
) as handle:

    writer = csv.DictWriter(
        handle,
        fieldnames=monotonicity_rows[0].keys(),
    )

    writer.writeheader()
    writer.writerows(monotonicity_rows)

print(json.dumps(summary, indent=2))
