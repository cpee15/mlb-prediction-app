import csv
import json
import math
import os
import random
from collections import defaultdict
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from mlb_app.simulation.game_engine_v2 import (
    run_full_game_simulation,
)

from mlb_app.matchup_generator import (
    generate_matchups_for_date,
)

from mlb_app.simulation.inning_simulator import (
    simulate_half_inning,
)

from mlb_app.simulation.transition_profiles import (
    resolve_transition_profile,
)

TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = (
    TMP_DIR
    / "subtype_transition_calibration.json"
)

OUTPUT_CONFIGS = (
    TMP_DIR
    / "subtype_transition_calibration_configs.csv"
)

OUTPUT_GAMES = (
    TMP_DIR
    / "subtype_transition_calibration_games.csv"
)

OUTPUT_COMPARISONS = (
    TMP_DIR
    / "subtype_transition_calibration_comparisons.csv"
)

OUTPUT_RANKING = (
    TMP_DIR
    / "subtype_transition_calibration_ranking.csv"
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///mlb.db",
)

BACKTEST_START = os.getenv(
    "BACKTEST_START",
    "2026-04-20",
)

BACKTEST_END = os.getenv(
    "BACKTEST_END",
    "2026-04-20",
)

SIMS_PER_GAME = int(
    os.getenv(
        "SIMS_PER_GAME",
        "50",
    )
)

MAX_GAMES = int(
    os.getenv(
        "MAX_GAMES",
        "999999",
    )
)

SEED = int(
    os.getenv(
        "SEED",
        "42",
    )
)

engine = create_engine(
    DATABASE_URL
)

def write_csv(
    path,
    rows,
):

    if not rows:

        rows = [
            {
                "status":
                    "no_rows"
            }
        ]

    with open(
        path,
        "w",
        newline="",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=list(
                rows[0].keys()
            ),
        )

        writer.writeheader()
        writer.writerows(rows)

def safe_float(x):

    try:

        return float(x)

    except Exception:

        return 0.0

def fetch_actual_results():

    query = text(
        """
        SELECT
            game_pk,
            home_score,
            away_score
        FROM actual_game_results
        """
    )

    actuals = {}

    with engine.connect() as conn:

        rows = conn.execute(query)

        for row in rows:

            actuals[
                int(row.game_pk)
            ] = {
                "home_score":
                    safe_float(
                        row.home_score
                    ),

                "away_score":
                    safe_float(
                        row.away_score
                    ),
            }

    return actuals


def extract_probabilities(
    result,
):

    probabilities = []

    def walk(node):

        if isinstance(node, dict):

            lineup = node.get(
                "lineup_average_probabilities"
            )

            if (
                isinstance(lineup, dict)
                and lineup
            ):

                probabilities.append(
                    lineup
                )

            for value in node.values():

                walk(value)

        elif isinstance(node, list):

            for item in node:

                walk(item)

    walk(result)

    return probabilities

def simulate_local_runs(
    probabilities,
    seed,
    config_name,
):

    rng = random.Random(seed)

    total_runs = 0

    if (
        config_name
        == "low_advancement_profile"
    ):

        profile = (
            resolve_transition_profile(
                "low_advancement"
            )
        )

        transition_mode = (
            profile.get(
                "transition_mode",
                "current",
            )
        )

        transition_rates = (
            profile.get(
                "transition_rates",
                {},
            )
        )

        subtype_mode = "off"

    elif (
        config_name
        == "subtype_prototype"
    ):

        transition_mode = "current"

        transition_rates = None

        subtype_mode = "prototype"

    else:

        transition_mode = "current"

        transition_rates = None

        subtype_mode = "off"

    for inning in range(9):

        if not probabilities:
            continue

        probs = probabilities[
            inning
            % len(probabilities)
        ]

        result = (
            simulate_half_inning(
                probs,
                rng=rng,
                transition_mode=transition_mode,
                transition_rates=transition_rates,
                subtype_transition_mode=subtype_mode,
            )
        )

        total_runs += (
            result.get(
                "runs",
                0,
            )
        )

    return total_runs

actual_results = (
    fetch_actual_results()
)

candidate_games = []

from datetime import datetime
from datetime import timedelta

start_date = datetime.strptime(
    BACKTEST_START,
    "%Y-%m-%d",
)

end_date = datetime.strptime(
    BACKTEST_END,
    "%Y-%m-%d",
)

current_date = start_date

while current_date <= end_date:

    game_date = current_date.strftime(
        "%Y-%m-%d"
    )

    try:

        with Session(engine) as session:

            matchups = (
                generate_matchups_for_date(
                    session,
                    game_date,
                )
            )

    except Exception:

        current_date += timedelta(days=1)
        continue

    for matchup in matchups:

        game_pk = (
            matchup.get("game_pk")
        )

        if game_pk is None:
            continue

        candidate_games.append(
            (
                game_date,
                matchup,
                int(game_pk),
            )
        )

    current_date += timedelta(days=1)

candidate_games = candidate_games[
    :MAX_GAMES
]

games_attempted = 0

config_game_rows = []

config_metrics = defaultdict(
    lambda: {
        "games": 0,
        "total_abs_error": 0.0,
        "total_sq_error": 0.0,
        "bias": 0.0,
        "winner_correct": 0,
        "brier_score": 0.0,
        "log_loss": 0.0,
        "totals": [],
    }
)

parser_missing_count = 0
parser_fallback_count = 0
actual_missing_count = 0
actual_malformed_count = 0

CONFIGS = [
    "production_baseline",
    "current_transition_script",
    "low_advancement_profile",
    "subtype_prototype",
]

for idx, (
    game_date,
    matchup,
    game_pk,
) in enumerate(candidate_games):

    if game_pk not in actual_results:

        actual_missing_count += 1
        continue

    try:

        production_result = (
            run_full_game_simulation(
                int(game_pk),
                config={
                    "simulation_count":
                        SIMS_PER_GAME,

                    "seed":
                        SEED + idx,

                    "date":
                        game_date,

                    "matchup": {
                        "raw":
                            matchup,

                        "game_date":
                            game_date,
                    },
                },
            )
        )

    except Exception:

        parser_missing_count += 1
        continue

    probabilities = (
        extract_probabilities(
            production_result
        )
    )

    if not probabilities:

        parser_fallback_count += 1
        continue

    actual_home = (
        actual_results[
            game_pk
        ]["home_score"]
    )

    actual_away = (
        actual_results[
            game_pk
        ]["away_score"]
    )

    actual_total = (
        actual_home
        + actual_away
    )

    derived = production_result.get(
        "derived_outputs",
        {}
    )

    bullpen = derived.get(
        "bullpen_adjusted_game_simulation",
        {}
    )

    production_total = safe_float(
        bullpen.get(
            "total_expected_runs",
            0.0,
        )
    )

    production_home = safe_float(
        bullpen.get(
            "home_expected_runs",
            0.0,
        )
    )

    production_away = safe_float(
        bullpen.get(
            "away_expected_runs",
            0.0,
        )
    )

    production_home_win = safe_float(
        bullpen.get(
            "home_win_probability",
            0.5,
        )
    )

    actual_home_win = (
        1.0
        if actual_home > actual_away
        else 0.0
    )

    for config_name in CONFIGS:

        if (
            config_name
            == "production_baseline"
        ):

            predicted_total = (
                production_total
            )

            predicted_home = (
                production_home
            )

            predicted_away = (
                production_away
            )

            home_win_probability = (
                production_home_win
            )

        else:

            home_runs = (
                simulate_local_runs(
                    probabilities,
                    SEED + idx,
                    config_name,
                )
            )

            away_runs = (
                simulate_local_runs(
                    probabilities,
                    SEED + idx + 999,
                    config_name,
                )
            )

            predicted_home = (
                home_runs
            )

            predicted_away = (
                away_runs
            )

            predicted_total = (
                home_runs
                + away_runs
            )

            if (
                predicted_home
                == predicted_away
            ):

                home_win_probability = 0.5

            elif (
                predicted_home
                > predicted_away
            ):

                home_win_probability = 0.65

            else:

                home_win_probability = 0.35

        total_error = abs(
            predicted_total
            - actual_total
        )

        total_sq_error = (
            predicted_total
            - actual_total
        ) ** 2

        bias = (
            predicted_total
            - actual_total
        )

        winner_correct = int(
            (
                predicted_home
                > predicted_away
            )
            ==
            (
                actual_home
                > actual_away
            )
        )

        p = min(
            max(
                home_win_probability,
                1e-6,
            ),
            1 - 1e-6,
        )

        brier = (
            p
            - actual_home_win
        ) ** 2

        log_loss = -(
            actual_home_win
            * math.log(p)
            +
            (1 - actual_home_win)
            * math.log(1 - p)
        )

        m = config_metrics[
            config_name
        ]

        m["games"] += 1
        m["total_abs_error"] += total_error
        m["total_sq_error"] += total_sq_error
        m["bias"] += bias
        m["winner_correct"] += winner_correct
        m["brier_score"] += brier
        m["log_loss"] += log_loss
        m["totals"].append(
            predicted_total
        )

        config_game_rows.append(
            {
                "game_pk":
                    game_pk,

                "config":
                    config_name,

                "predicted_total":
                    predicted_total,

                "actual_total":
                    actual_total,

                "predicted_home":
                    predicted_home,

                "predicted_away":
                    predicted_away,

                "actual_home":
                    actual_home,

                "actual_away":
                    actual_away,

                "home_win_probability":
                    home_win_probability,

                "winner_correct":
                    winner_correct,
            }
        )

    games_attempted += 1

config_rows = []

ranking_rows = []

for config_name, m in config_metrics.items():

    games = max(
        m["games"],
        1,
    )

    total_mae = (
        m["total_abs_error"]
        / games
    )

    rmse = math.sqrt(
        m["total_sq_error"]
        / games
    )

    bias = (
        m["bias"]
        / games
    )

    brier = (
        m["brier_score"]
        / games
    )

    log_loss = (
        m["log_loss"]
        / games
    )

    accuracy = (
        m["winner_correct"]
        / games
    )

    totals = m["totals"]

    mean_total = (
        sum(totals)
        / max(len(totals), 1)
    )

    variance = (
        sum(
            (
                x - mean_total
            ) ** 2
            for x in totals
        )
        / max(len(totals), 1)
    )

    std = math.sqrt(
        variance
    )

    combined_score = (
        total_mae
        + brier
        + log_loss
        + abs(bias) * 0.10
    )

    row = {
        "config":
            config_name,

        "games":
            games,

        "total_run_mae":
            round(total_mae, 4),

        "total_run_rmse":
            round(rmse, 4),

        "total_bias":
            round(bias, 4),

        "brier_score":
            round(brier, 4),

        "log_loss":
            round(log_loss, 4),

        "winner_accuracy":
            round(accuracy, 4),

        "expected_total_std":
            round(std, 4),

        "combined_score":
            round(combined_score, 4),
    }

    config_rows.append(row)
    ranking_rows.append(row)

ranking_rows = sorted(
    ranking_rows,
    key=lambda x: x[
        "combined_score"
    ],
)

comparison_rows = []

baseline_lookup = {
    row["config"]: row
    for row in config_rows
}

comparison_pairs = [
    (
        "current_transition_script",
        "production_baseline",
    ),
    (
        "low_advancement_profile",
        "current_transition_script",
    ),
    (
        "subtype_prototype",
        "current_transition_script",
    ),
    (
        "subtype_prototype",
        "low_advancement_profile",
    ),
    (
        "subtype_prototype",
        "production_baseline",
    ),
]

for left, right in comparison_pairs:

    if (
        left not in baseline_lookup
        or right not in baseline_lookup
    ):
        continue

    l = baseline_lookup[left]
    r = baseline_lookup[right]

    comparison_rows.append(
        {
            "left":
                left,

            "right":
                right,

            "total_abs_error_delta":
                round(
                    l["total_run_mae"]
                    - r["total_run_mae"],
                    4,
                ),

            "brier_delta":
                round(
                    l["brier_score"]
                    - r["brier_score"],
                    4,
                ),

            "log_loss_delta":
                round(
                    l["log_loss"]
                    - r["log_loss"],
                    4,
                ),

            "winner_accuracy_delta":
                round(
                    l["winner_accuracy"]
                    - r["winner_accuracy"],
                    4,
                ),

            "expected_total_std_delta":
                round(
                    l["expected_total_std"]
                    - r["expected_total_std"],
                    4,
                ),
        }
    )

best_config = (
    ranking_rows[0]["config"]
    if ranking_rows
    else None
)

if (
    games_attempted
    == 0
):

    diagnosis = (
        "subtype_transition_"
        "calibration_parser_blocked"
    )

elif (
    best_config
    == "production_baseline"
):

    diagnosis = (
        "subtype_transition_"
        "calibration_production_baseline_still_best"
    )

elif (
    best_config
    == "low_advancement_profile"
):

    diagnosis = (
        "subtype_transition_"
        "calibration_low_advancement_still_best"
    )

elif (
    best_config
    == "subtype_prototype"
):

    diagnosis = (
        "subtype_transition_"
        "calibration_subtype_supported"
    )

else:

    diagnosis = (
        "subtype_transition_"
        "calibration_no_local_improvement"
    )

summary = {
    "diagnosis":
        diagnosis,

    "games_attempted":
        games_attempted,

    "parser_missing_count":
        parser_missing_count,

    "parser_fallback_count":
        parser_fallback_count,

    "actual_missing_count":
        actual_missing_count,

    "actual_malformed_count":
        actual_malformed_count,

    "default_activation_changed":
        False,

    "subtype_mode_used":
        True,

    "candidate_configs_tested":
        CONFIGS,

    "best_config":
        best_config,

    "recommended_next_step":
        (
            "layer_6x_"
            "subtype_transition_"
            "promotion_guardrail_audit"
        ),
}

with open(
    OUTPUT_JSON,
    "w",
) as f:

    json.dump(
        summary,
        f,
        indent=2,
    )

write_csv(
    OUTPUT_CONFIGS,
    config_rows,
)

write_csv(
    OUTPUT_GAMES,
    config_game_rows,
)

write_csv(
    OUTPUT_COMPARISONS,
    comparison_rows,
)

write_csv(
    OUTPUT_RANKING,
    ranking_rows,
)

print(
    json.dumps(
        summary,
        indent=2,
    )
)
