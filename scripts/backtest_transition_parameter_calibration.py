import csv
import json
import math
import os
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from mlb_app.matchup_generator import generate_matchups_for_date
from mlb_app.simulation.game_engine_v2 import (
    run_full_game_simulation,
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
    "2026-05-03",
)

SIMS_PER_GAME = int(
    os.getenv("SIMS_PER_GAME", 250)
)

MAX_GAMES_ENV = os.getenv(
    "MAX_GAMES"
)

MAX_GAMES = (
    int(MAX_GAMES_ENV)
    if MAX_GAMES_ENV
    else None
)

SEED = int(
    os.getenv("SEED", 42)
)

TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

PARAMETER_GRID = [
    {
        "name": "baseline_candidate_6j",
        "sac_fly_rate": 0.30,
        "double_play_rate": 0.11,
        "first_to_third_single_rate": 0.30,
        "first_to_home_double_rate": 0.40,
    },
    {
        "name": "low_advancement",
        "sac_fly_rate": 0.20,
        "double_play_rate": 0.11,
        "first_to_third_single_rate": 0.20,
        "first_to_home_double_rate": 0.30,
    },
    {
        "name": "high_advancement",
        "sac_fly_rate": 0.40,
        "double_play_rate": 0.11,
        "first_to_third_single_rate": 0.40,
        "first_to_home_double_rate": 0.50,
    },
    {
        "name": "low_dp",
        "sac_fly_rate": 0.30,
        "double_play_rate": 0.07,
        "first_to_third_single_rate": 0.30,
        "first_to_home_double_rate": 0.40,
    },
    {
        "name": "high_dp",
        "sac_fly_rate": 0.30,
        "double_play_rate": 0.15,
        "first_to_third_single_rate": 0.30,
        "first_to_home_double_rate": 0.40,
    },
    {
        "name": "balanced_conservative",
        "sac_fly_rate": 0.25,
        "double_play_rate": 0.10,
        "first_to_third_single_rate": 0.25,
        "first_to_home_double_rate": 0.35,
    },
]

def daterange(start_date, end_date):

    from datetime import (
        datetime,
        timedelta,
    )

    start = datetime.strptime(
        start_date,
        "%Y-%m-%d",
    )

    end = datetime.strptime(
        end_date,
        "%Y-%m-%d",
    )

    current = start

    while current <= end:

        yield current.strftime(
            "%Y-%m-%d"
        )

        current += timedelta(
            days=1
        )

def safe_float(
    value,
    fallback=0.0,
):

    try:

        if value is None:
            return fallback

        return float(value)

    except Exception:
        return fallback

def compute_brier_score(
    actual,
    probability,
):

    return (
        probability - actual
    ) ** 2

def compute_log_loss(
    actual,
    probability,
):

    eps = 1e-9

    probability = max(
        eps,
        min(
            1 - eps,
            probability,
        ),
    )

    if actual == 1:
        return -math.log(
            probability
        )

    return -math.log(
        1 - probability
    )

def fetch_actual_results(
    engine,
):

    sql = text(
        """
        SELECT
            game_pk,
            game_date,
            home_score,
            away_score
        FROM actual_game_results
        WHERE game_date >= :start_date
          AND game_date <= :end_date
        """
    )

    rows = []

    with engine.connect() as conn:

        results = conn.execute(
            sql,
            {
                "start_date":
                    BACKTEST_START,
                "end_date":
                    BACKTEST_END,
            },
        )

        for row in results:

            try:

                game_pk = int(
                    row.game_pk
                )

                home_score = int(
                    row.home_score
                )

                away_score = int(
                    row.away_score
                )

            except Exception:
                continue

            rows.append(
                {
                    "game_pk":
                        game_pk,
                    "game_date":
                        str(
                            row.game_date
                        ),
                    "home_score":
                        home_score,
                    "away_score":
                        away_score,
                    "total_score":
                        home_score
                        + away_score,
                    "home_win_actual":
                        (
                            1
                            if home_score
                            > away_score
                            else 0
                        ),
                }
            )

    return {
        row["game_pk"]: row
        for row in rows
    }

def extract_simulation_payload(
    simulation_result,
):

    derived = (
        simulation_result.get(
            "derived_outputs",
            {}
        )
    )

    sim = derived.get(
        "bullpen_adjusted_game_simulation",
        {}
    )

    return {
        "home_expected_runs":
            safe_float(
                sim.get(
                    "home_expected_runs"
                )
            ),
        "away_expected_runs":
            safe_float(
                sim.get(
                    "away_expected_runs"
                )
            ),
        "total_expected_runs":
            safe_float(
                sim.get(
                    "total_expected_runs"
                )
            ),
        "home_win_probability":
            safe_float(
                sim.get(
                    "home_win_probability"
                )
            ),
        "tie_after_regulation_probability":
            safe_float(
                sim.get(
                    "tie_after_regulation_probability"
                )
            ),
    }

def apply_transition_adjustment(
    baseline,
    config,
):

    aggression = (
        config[
            "sac_fly_rate"
        ]
        + config[
            "first_to_third_single_rate"
        ]
        + config[
            "first_to_home_double_rate"
        ]
    ) / 3

    dp_penalty = (
        config[
            "double_play_rate"
        ]
        - 0.11
    )

    run_delta = (
        (
            aggression
            - 0.3333
        )
        * 1.2
    ) - (
        dp_penalty
        * 0.8
    )

    adjusted_total = (
        baseline[
            "total_expected_runs"
        ]
        + run_delta
    )

    home_share = (
        baseline[
            "home_expected_runs"
        ]
        / max(
            0.01,
            baseline[
                "total_expected_runs"
            ],
        )
    )

    adjusted_home = (
        adjusted_total
        * home_share
    )

    adjusted_away = (
        adjusted_total
        - adjusted_home
    )

    adjusted_win_probability = (
        min(
            0.99,
            max(
                0.01,
                baseline[
                    "home_win_probability"
                ]
                + (
                    run_delta
                    * 0.005
                ),
            ),
        )
    )

    return {
        "home_expected_runs":
            adjusted_home,
        "away_expected_runs":
            adjusted_away,
        "total_expected_runs":
            adjusted_total,
        "home_win_probability":
            adjusted_win_probability,
        "tie_after_regulation_probability":
            baseline[
                "tie_after_regulation_probability"
            ],
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
    actual_missing_count = 0

    games_rows = []
    config_metrics = []
    ranking_rows = []

    config_aggregates = (
        defaultdict(list)
    )

    games_attempted = 0

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
                    and games_attempted
                    >= MAX_GAMES
                ):
                    break

                games_attempted += 1

                game_pk = matchup.get(
                    "game_pk"
                )

                if (
                    game_pk is None
                ):
                    continue

                actual = (
                    actual_lookup.get(
                        int(game_pk)
                    )
                )

                if actual is None:

                    actual_missing_count += 1
                    continue

                try:

                    simulation_result = (
                        run_full_game_simulation(
                            int(game_pk),
                            config={
                                "simulation_count": SIMS_PER_GAME,
                                "seed": SEED + len(games_rows),
                                "date": game_date,
                                "matchup": {
                                    "raw": matchup,
                                    "game_date": game_date,
                                },
                            },
                        )
                    )

                except Exception:
                    parser_missing_count += 1
                    continue

                baseline = (
                    extract_simulation_payload(
                        simulation_result
                    )
                )

                if (
                    baseline[
                        "total_expected_runs"
                    ]
                    <= 0
                ):

                    parser_missing_count += 1
                    continue

                actual_total = (
                    actual[
                        "total_score"
                    ]
                )

                actual_home_win = (
                    actual[
                        "home_win_actual"
                    ]
                )

                for config in PARAMETER_GRID:

                    adjusted = (
                        apply_transition_adjustment(
                            baseline,
                            config,
                        )
                    )

                    total_error = abs(
                        adjusted[
                            "total_expected_runs"
                        ]
                        - actual_total
                    )

                    home_error = abs(
                        adjusted[
                            "home_expected_runs"
                        ]
                        - actual[
                            "home_score"
                        ]
                    )

                    away_error = abs(
                        adjusted[
                            "away_expected_runs"
                        ]
                        - actual[
                            "away_score"
                        ]
                    )

                    brier = (
                        compute_brier_score(
                            actual_home_win,
                            adjusted[
                                "home_win_probability"
                            ],
                        )
                    )

                    log_loss = (
                        compute_log_loss(
                            actual_home_win,
                            adjusted[
                                "home_win_probability"
                            ],
                        )
                    )

                    row = {
                        "game_pk":
                            game_pk,
                        "config":
                            config["name"],
                        "actual_total_runs":
                            actual_total,
                        "predicted_total_runs":
                            round(
                                adjusted[
                                    "total_expected_runs"
                                ],
                                4,
                            ),
                        "total_abs_error":
                            round(
                                total_error,
                                4,
                            ),
                        "home_abs_error":
                            round(
                                home_error,
                                4,
                            ),
                        "away_abs_error":
                            round(
                                away_error,
                                4,
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
                    }

                    games_rows.append(
                        row
                    )

                    config_aggregates[
                        config["name"]
                    ].append(row)

    for (
        config_name,
        rows,
    ) in (
        config_aggregates.items()
    ):

        total_mae = mean(
            [
                r[
                    "total_abs_error"
                ]
                for r in rows
            ]
        )

        home_mae = mean(
            [
                r[
                    "home_abs_error"
                ]
                for r in rows
            ]
        )

        away_mae = mean(
            [
                r[
                    "away_abs_error"
                ]
                for r in rows
            ]
        )

        brier = mean(
            [
                r[
                    "brier_score"
                ]
                for r in rows
            ]
        )

        log_loss = mean(
            [
                r[
                    "log_loss"
                ]
                for r in rows
            ]
        )

        total_bias = mean(
            [
                (
                    r[
                        "predicted_total_runs"
                    ]
                    - r[
                        "actual_total_runs"
                    ]
                )
                for r in rows
            ]
        )

        combined_score = (
            total_mae
            + brier
            + log_loss
            + (
                abs(
                    total_bias
                )
                * 0.10
            )
        )

        metric_row = {
            "config":
                config_name,
            "games":
                len(rows),
            "total_run_mae":
                round(
                    total_mae,
                    6,
                ),
            "home_run_mae":
                round(
                    home_mae,
                    6,
                ),
            "away_run_mae":
                round(
                    away_mae,
                    6,
                ),
            "total_bias":
                round(
                    total_bias,
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
            "combined_score":
                round(
                    combined_score,
                    6,
                ),
        }

        config_metrics.append(
            metric_row
        )

        ranking_rows.append(
            {
                "config":
                    config_name,
                "combined_score":
                    combined_score,
            }
        )

    ranking_rows = sorted(
        ranking_rows,
        key=lambda x:
            x[
                "combined_score"
            ],
    )

    best_config = (
        ranking_rows[0][
            "config"
        ]
        if ranking_rows
        else None
    )

    diagnosis = (
        "transition_parameter_calibration_payloads_repaired"
    )

    if (
        parser_missing_count
        > 10
    ):

        diagnosis = (
            "transition_parameter_calibration_parser_blocked"
        )

    summary = {
        "diagnosis":
            diagnosis,
        "games_attempted":
            games_attempted,
        "configs_tested":
            len(
                PARAMETER_GRID
            ),
        "best_config":
            best_config,
        "parser_missing_count":
            parser_missing_count,
        "actual_missing_count":
            actual_missing_count,
        "recommended_next_step":
            (
                "layer_6l_transition_realism_activation_decision_audit"
            ),
    }

    with open(
        TMP_DIR
        / "transition_parameter_calibration.json",
        "w",
    ) as f:

        json.dump(
            summary,
            f,
            indent=2,
        )

    def write_csv(
        path,
        rows,
    ):

        if not rows:
            return

        with open(
            path,
            "w",
            newline="",
        ) as f:

            writer = csv.DictWriter(
                f,
                fieldnames=rows[
                    0
                ].keys(),
            )

            writer.writeheader()
            writer.writerows(
                rows
            )

    write_csv(
        TMP_DIR
        / "transition_parameter_calibration_games.csv",
        games_rows,
    )

    write_csv(
        TMP_DIR
        / "transition_parameter_calibration_configs.csv",
        config_metrics,
    )

    write_csv(
        TMP_DIR
        / "transition_parameter_calibration_ranking.csv",
        ranking_rows,
    )

    print(
        json.dumps(
            summary,
            indent=2,
        )
    )

if __name__ == "__main__":
    main()
