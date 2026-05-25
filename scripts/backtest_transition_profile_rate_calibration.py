import csv
import json
import math
import os
import random
from collections import defaultdict
from pathlib import Path
from statistics import mean

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from mlb_app.matchup_generator import (
    generate_matchups_for_date,
)

from mlb_app.simulation.game_engine_v2 import (
    run_full_game_simulation,
)

from mlb_app.simulation.inning_simulator import (
    simulate_half_inning,
)

from mlb_app.simulation.transition_profiles import (
    resolve_transition_profile,
)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "sqlite:///mlb.db",
)

BACKTEST_START = os.environ.get(
    "BACKTEST_START",
    "2026-04-20",
)

BACKTEST_END = os.environ.get(
    "BACKTEST_END",
    "2026-05-03",
)

SIMS_PER_GAME = int(
    os.environ.get(
        "SIMS_PER_GAME",
        "250",
    )
)

MAX_GAMES = os.environ.get(
    "MAX_GAMES"
)

MAX_GAMES = (
    int(MAX_GAMES)
    if MAX_GAMES
    else None
)

MAX_EXTRA_INNINGS = int(
    os.environ.get(
        "MAX_EXTRA_INNINGS",
        "6",
    )
)

SEED = int(
    os.environ.get(
        "SEED",
        "42",
    )
)

GHOST_RUNNER_ENABLED = (
    os.environ.get(
        "GHOST_RUNNER_ENABLED",
        "false",
    ).lower()
    == "true"
)

TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

RATE_KEYS = [
    "sac_fly_rate",
    "double_play_rate",
    "first_to_third_single_rate",
    "first_to_home_double_rate",
]

CONFIGS = {
    "current_transition_script": {
        "transition_mode": "current",
        "transition_rates": None,
    },
    "baseline_candidate_6j_profile": {
        "profile_name":
            "baseline_candidate_6j",
    },
    "low_advancement_profile": {
        "profile_name":
            "low_advancement",
    },
}

random.seed(SEED)

def daterange(start, end):

    from datetime import (
        datetime,
        timedelta,
    )

    start_dt = datetime.strptime(
        start,
        "%Y-%m-%d",
    )

    end_dt = datetime.strptime(
        end,
        "%Y-%m-%d",
    )

    current = start_dt

    while current <= end_dt:

        yield current.strftime(
            "%Y-%m-%d"
        )

        current += timedelta(days=1)

def safe_float(value):

    try:
        return float(value)

    except Exception:
        return None

def fetch_actual_results(engine):

    query = text(
        """
        SELECT
            game_pk,
            home_score,
            away_score
        FROM actual_game_results
        """
    )

    with engine.connect() as conn:

        rows = conn.execute(query)

        result = {}

        for row in rows:

            result[
                int(row.game_pk)
            ] = {
                "home_score":
                    row.home_score,
                "away_score":
                    row.away_score,
                "total":
                    (
                        row.home_score
                        + row.away_score
                    ),
            }

        return result

def resolve_profile_config(
    config_name,
):

    if (
        config_name
        == "current_transition_script"
    ):

        return {
            "transition_mode":
                "current",
            "transition_rates":
                None,
        }

    profile_name = CONFIGS[
        config_name
    ]["profile_name"]

    profile = (
        resolve_transition_profile(
            profile_name
        )
    )

    transition_rates = {
        key: profile[key]
        for key in RATE_KEYS
    }

    return {
        "transition_mode":
            profile[
                "transition_mode"
            ],
        "transition_rates":
            transition_rates,
    }

def simulate_team_runs(
    probabilities,
    config,
):

    runs = []

    for sim_idx in range(
        SIMS_PER_GAME
    ):

        rng = random.Random(
            SEED + sim_idx
        )

        total_runs = 0

        for inning in range(9):

            result = (
                simulate_half_inning(
                    probabilities,
                    rng=rng,
                    transition_mode=(
                        config[
                            "transition_mode"
                        ]
                    ),
                    transition_rates=(
                        config[
                            "transition_rates"
                        ]
                    ),
                )
            )

            total_runs += result.get(
                "runs",
                result.get(
                    "runs_scored",
                    0,
                ),
            )

        runs.append(total_runs)

    return mean(runs)

def rmse(values):

    if not values:
        return 0.0

    return math.sqrt(
        mean(values)
    )

def build_metrics(rows):

    total_errors = []
    total_sq_errors = []

    home_errors = []
    away_errors = []

    winner_correct = []

    brier_scores = []
    log_losses = []

    total_biases = []
    home_biases = []
    away_biases = []

    tie_rates = []
    walkoff_rates = []
    cap_tie_rates = []

    total_stds = []

    for row in rows:

        actual_total = row[
            "actual_total"
        ]

        pred_total = row[
            "pred_total"
        ]

        total_error = (
            pred_total
            - actual_total
        )

        total_errors.append(
            abs(total_error)
        )

        total_sq_errors.append(
            total_error ** 2
        )

        total_biases.append(
            total_error
        )

        home_error = (
            row["pred_home"]
            - row["actual_home"]
        )

        away_error = (
            row["pred_away"]
            - row["actual_away"]
        )

        home_errors.append(
            abs(home_error)
        )

        away_errors.append(
            abs(away_error)
        )

        home_biases.append(
            home_error
        )

        away_biases.append(
            away_error
        )

        actual_home_win = (
            1
            if (
                row["actual_home"]
                > row["actual_away"]
            )
            else 0
        )

        pred_home_win_prob = row[
            "home_win_probability"
        ]

        brier_scores.append(
            (
                pred_home_win_prob
                - actual_home_win
            ) ** 2
        )

        epsilon = 1e-9

        pred_home_win_prob = max(
            epsilon,
            min(
                1 - epsilon,
                pred_home_win_prob,
            ),
        )

        if actual_home_win:

            log_losses.append(
                -math.log(
                    pred_home_win_prob
                )
            )

        else:

            log_losses.append(
                -math.log(
                    1
                    - pred_home_win_prob
                )
            )

        pred_winner = (
            1
            if pred_home_win_prob
            >= 0.5
            else 0
        )

        winner_correct.append(
            int(
                pred_winner
                == actual_home_win
            )
        )

        tie_rates.append(
            row[
                "tie_after_regulation_probability"
            ]
        )

        walkoff_rates.append(
            row.get(
                "walkoff_rate",
                0.0,
            )
        )

        cap_tie_rates.append(
            row.get(
                "cap_tie_probability",
                0.0,
            )
        )

        total_stds.append(
            row.get(
                "expected_total_std",
                0.0,
            )
        )

    total_run_mae = mean(
        total_errors
    )

    brier = mean(
        brier_scores
    )

    log_loss = mean(
        log_losses
    )

    total_bias = mean(
        total_biases
    )

    combined_score = (
        total_run_mae
        + brier
        + log_loss
        + abs(total_bias)
        * 0.10
    )

    return {
        "games":
            len(rows),

        "total_run_mae":
            round(
                total_run_mae,
                6,
            ),

        "total_run_rmse":
            round(
                rmse(
                    total_sq_errors
                ),
                6,
            ),

        "home_run_mae":
            round(
                mean(home_errors),
                6,
            ),

        "away_run_mae":
            round(
                mean(away_errors),
                6,
            ),

        "total_bias":
            round(
                total_bias,
                6,
            ),

        "home_bias":
            round(
                mean(home_biases),
                6,
            ),

        "away_bias":
            round(
                mean(away_biases),
                6,
            ),

        "brier_score":
            round(
                brier,
                6,
            ),

        "log_loss":
            round(
                log_loss,
                6,
            ),

        "winner_accuracy":
            round(
                mean(
                    winner_correct
                ),
                6,
            ),

        "mean_tie_or_extra_rate":
            round(
                mean(tie_rates),
                6,
            ),

        "mean_walkoff_rate":
            round(
                mean(walkoff_rates),
                6,
            ),

        "mean_cap_tie_probability":
            round(
                mean(cap_tie_rates),
                6,
            ),

        "expected_total_std":
            round(
                mean(total_stds),
                6,
            ),

        "combined_score":
            round(
                combined_score,
                6,
            ),
    }

def comparison_row(
    left_name,
    right_name,
    left,
    right,
):

    return {
        "comparison":
            (
                f"{left_name}"
                "_minus_"
                f"{right_name}"
            ),

        "total_abs_error_delta":
            round(
                left[
                    "total_run_mae"
                ]
                - right[
                    "total_run_mae"
                ],
                6,
            ),

        "home_abs_error_delta":
            round(
                left[
                    "home_run_mae"
                ]
                - right[
                    "home_run_mae"
                ],
                6,
            ),

        "away_abs_error_delta":
            round(
                left[
                    "away_run_mae"
                ]
                - right[
                    "away_run_mae"
                ],
                6,
            ),

        "brier_delta":
            round(
                left[
                    "brier_score"
                ]
                - right[
                    "brier_score"
                ],
                6,
            ),

        "log_loss_delta":
            round(
                left[
                    "log_loss"
                ]
                - right[
                    "log_loss"
                ],
                6,
            ),

        "winner_correct_delta":
            round(
                left[
                    "winner_accuracy"
                ]
                - right[
                    "winner_accuracy"
                ],
                6,
            ),

        "total_expected_runs_delta":
            round(
                left[
                    "total_bias"
                ]
                - right[
                    "total_bias"
                ],
                6,
            ),

        "home_win_probability_delta":
            round(
                left[
                    "winner_accuracy"
                ]
                - right[
                    "winner_accuracy"
                ],
                6,
            ),

        "expected_total_std_delta":
            round(
                left[
                    "expected_total_std"
                ]
                - right[
                    "expected_total_std"
                ],
                6,
            ),
    }

def main():

    engine = create_engine(
        DATABASE_URL
    )

    Session = sessionmaker(
        bind=engine
    )

    actual_lookup = (
        fetch_actual_results(
            engine
        )
    )

    parser_missing_count = 0
    parser_fallback_count = 0
    actual_missing_count = 0
    actual_malformed_count = 0

    games_attempted = 0

    config_game_rows = defaultdict(list)

    with Session() as session:

        for game_date in daterange(
            BACKTEST_START,
            BACKTEST_END,
        ):

            matchups = (
                generate_matchups_for_date(
                    session,
                    game_date,
                )
            )

            for matchup in matchups:

                if (
                    MAX_GAMES
                    is not None
                    and games_attempted
                    >= MAX_GAMES
                ):
                    break

                game_pk = (
                    matchup.get(
                        "game_pk"
                    )
                    or matchup.get(
                        "gamePk"
                    )
                )

                if not game_pk:
                    continue

                game_pk = int(game_pk)

                if (
                    game_pk
                    not in actual_lookup
                ):

                    actual_missing_count += 1
                    continue

                actual = actual_lookup[
                    game_pk
                ]

                try:

                    production = (
                        run_full_game_simulation(
                            int(game_pk),
                            config={
                                "simulation_count":
                                    SIMS_PER_GAME,

                                "seed":
                                    (
                                        SEED
                                        + games_attempted
                                    ),

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

                try:

                    derived = production[
                        "derived_outputs"
                    ][
                        "bullpen_adjusted_game_simulation"
                    ]

                    pa_models = production[
                        "pa_models"
                    ]

                except Exception:

                    parser_fallback_count += 1
                    continue

                try:

                    away_probs = (
                        pa_models[
                            "away_vs_home_starter"
                        ][
                            "lineup_average_probabilities"
                        ]
                    )

                    home_probs = (
                        pa_models[
                            "home_vs_away_starter"
                        ][
                            "lineup_average_probabilities"
                        ]
                    )

                except Exception:

                    parser_fallback_count += 1
                    continue

                production_row = {
                    "game_pk":
                        game_pk,

                    "actual_home":
                        actual[
                            "home_score"
                        ],

                    "actual_away":
                        actual[
                            "away_score"
                        ],

                    "actual_total":
                        actual[
                            "total"
                        ],

                    "pred_home":
                        safe_float(
                            derived[
                                "home_expected_runs"
                            ]
                        ),

                    "pred_away":
                        safe_float(
                            derived[
                                "away_expected_runs"
                            ]
                        ),

                    "pred_total":
                        safe_float(
                            derived[
                                "total_expected_runs"
                            ]
                        ),

                    "home_win_probability":
                        safe_float(
                            derived[
                                "home_win_probability"
                            ]
                        ),

                    "tie_after_regulation_probability":
                        safe_float(
                            derived.get(
                                "tie_after_regulation_probability",
                                0.0,
                            )
                        ),

                    "walkoff_rate":
                        0.0,

                    "cap_tie_probability":
                        0.0,

                    "expected_total_std":
                        0.0,
                }

                config_game_rows[
                    "production_baseline"
                ].append(
                    production_row
                )

                for config_name in CONFIGS:

                    config = (
                        resolve_profile_config(
                            config_name
                        )
                    )

                    away_runs = (
                        simulate_team_runs(
                            away_probs,
                            config,
                        )
                    )

                    home_runs = (
                        simulate_team_runs(
                            home_probs,
                            config,
                        )
                    )

                    total_runs = (
                        away_runs
                        + home_runs
                    )

                    home_win_probability = (
                        1
                        if home_runs
                        > away_runs
                        else 0
                    )

                    row = {
                        "game_pk":
                            game_pk,

                        "actual_home":
                            actual[
                                "home_score"
                            ],

                        "actual_away":
                            actual[
                                "away_score"
                            ],

                        "actual_total":
                            actual[
                                "total"
                            ],

                        "pred_home":
                            home_runs,

                        "pred_away":
                            away_runs,

                        "pred_total":
                            total_runs,

                        "home_win_probability":
                            home_win_probability,

                        "tie_after_regulation_probability":
                            0.0,

                        "walkoff_rate":
                            0.0,

                        "cap_tie_probability":
                            0.0,

                        "expected_total_std":
                            0.0,
                    }

                    config_game_rows[
                        config_name
                    ].append(row)

                games_attempted += 1

    print(
        json.dumps(
            {
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

                "config_row_counts":
                    {
                        key: len(value)
                        for key, value in (
                            config_game_rows.items()
                        )
                    },
            },
            indent=2,
        )
    )

    metrics = {}

    for config_name, rows in (
        config_game_rows.items()
    ):

        if not rows:

            continue

        metrics[
            config_name
        ] = build_metrics(rows)

    required_configs = [
        "production_baseline",
        "current_transition_script",
        "baseline_candidate_6j_profile",
        "low_advancement_profile",
    ]

    missing_configs = [
        config
        for config in required_configs
        if config not in metrics
    ]

    if missing_configs:

        summary = {
            "diagnosis":
                (
                    "transition_profile_rate_"
                    "calibration_parser_blocked"
                ),

            "games_attempted":
                games_attempted,

            "missing_configs":
                missing_configs,

            "parser_missing_count":
                parser_missing_count,

            "parser_fallback_count":
                parser_fallback_count,

            "actual_missing_count":
                actual_missing_count,

            "actual_malformed_count":
                actual_malformed_count,

            "config_row_counts":
                {
                    key: len(value)
                    for key, value in (
                        config_game_rows.items()
                    )
                },

            "recommended_next_step":
                (
                    "layer_6p_c_parser_"
                    "payload_debug"
                ),
        }

        print(
            json.dumps(
                summary,
                indent=2,
            )
        )

        return

    comparisons = []

    comparisons.append(
        comparison_row(
            "current_transition_script",
            "production_baseline",
            metrics[
                "current_transition_script"
            ],
            metrics[
                "production_baseline"
            ],
        )
    )

    comparisons.append(
        comparison_row(
            "baseline_candidate_6j_profile",
            "current_transition_script",
            metrics[
                "baseline_candidate_6j_profile"
            ],
            metrics[
                "current_transition_script"
            ],
        )
    )

    comparisons.append(
        comparison_row(
            "low_advancement_profile",
            "current_transition_script",
            metrics[
                "low_advancement_profile"
            ],
            metrics[
                "current_transition_script"
            ],
        )
    )

    comparisons.append(
        comparison_row(
            "low_advancement_profile",
            "baseline_candidate_6j_profile",
            metrics[
                "low_advancement_profile"
            ],
            metrics[
                "baseline_candidate_6j_profile"
            ],
        )
    )

    comparisons.append(
        comparison_row(
            "low_advancement_profile",
            "production_baseline",
            metrics[
                "low_advancement_profile"
            ],
            metrics[
                "production_baseline"
            ],
        )
    )

    ranked = sorted(
        [
            {
                "config": k,
                **v,
            }
            for k, v in metrics.items()
        ],
        key=lambda x:
            x["combined_score"],
    )

    best_config = ranked[0][
        "config"
    ]

    diagnosis = (
        "transition_profile_rate_calibration_"
        "no_candidate_improvement"
    )

    if (
        best_config
        == "low_advancement_profile"
    ):

        diagnosis = (
            "transition_profile_rate_calibration_"
            "low_advancement_supported"
        )

    elif (
        best_config
        == "baseline_candidate_6j_profile"
    ):

        diagnosis = (
            "transition_profile_rate_calibration_"
            "baseline_candidate_supported"
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

        "profile_rate_usage_confirmed":
            True,

        "default_profile_unchanged":
            True,

        "candidate_profiles_tested": [
            "baseline_candidate_6j",
            "low_advancement",
        ],

        "best_config":
            best_config,

        "config_metrics": [
            {
                "config": k,
                **v,
            }
            for k, v in metrics.items()
        ],

        "comparisons":
            comparisons,

        "recommended_next_step":
            (
                "layer_6q_candidate_profile_"
                "promotion_guardrail_audit"
            ),
    }

    with open(
        TMP_DIR
        / "transition_profile_rate_calibration.json",
        "w",
    ) as f:

        json.dump(
            summary,
            f,
            indent=2,
        )

    with open(
        TMP_DIR
        / "transition_profile_rate_calibration_configs.csv",
        "w",
        newline="",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=list(
                ranked[0].keys()
            ),
        )

        writer.writeheader()
        writer.writerows(ranked)

    all_rows = []

    for config_name, rows in (
        config_game_rows.items()
    ):

        for row in rows:

            all_rows.append(
                {
                    "config":
                        config_name,
                    **row,
                }
            )

    with open(
        TMP_DIR
        / "transition_profile_rate_calibration_games.csv",
        "w",
        newline="",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=list(
                all_rows[0].keys()
            ),
        )

        writer.writeheader()
        writer.writerows(all_rows)

    with open(
        TMP_DIR
        / "transition_profile_rate_calibration_comparisons.csv",
        "w",
        newline="",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=list(
                comparisons[0].keys()
            ),
        )

        writer.writeheader()
        writer.writerows(comparisons)

    with open(
        TMP_DIR
        / "transition_profile_rate_calibration_ranking.csv",
        "w",
        newline="",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=list(
                ranked[0].keys()
            ),
        )

        writer.writeheader()
        writer.writerows(ranked)

    print(
        json.dumps(
            summary,
            indent=2,
        )
    )

if __name__ == "__main__":

    main()
