from __future__ import annotations

import copy
import csv
import json
import math
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.matchup_generator import generate_matchups_for_date
from mlb_app.simulation.game_engine_v2 import run_full_game_simulation


BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
BACKTEST_END = os.getenv("BACKTEST_END", "2026-05-03")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///mlb.db",
)

SIMS_PER_GAME = int(
    os.getenv(
        "SIMS_PER_GAME",
        "50",
    )
)

MAX_GAMES_RAW = os.getenv(
    "MAX_GAMES",
    "",
)

MAX_GAMES = (
    int(MAX_GAMES_RAW)
    if MAX_GAMES_RAW.strip()
    else None
)

SEED = int(
    os.getenv(
        "SEED",
        "42",
    )
)


CONFIGS = {
    "baseline_current_production": {},
    "hitter_expected_contact__small": {
        "hitter_contact_boost": 0.015,
    },
    "hitter_recent__medium": {
        "hitter_recent_boost": 0.03,
    },
    "pitcher_stuff__small": {
        "pitcher_stuff_penalty": 0.015,
    },
    "combined_recent_and_stuff__small": {
        "hitter_recent_boost": 0.02,
        "pitcher_stuff_penalty": 0.015,
    },
    "combined_expected_contact_and_stuff__small": {
        "hitter_contact_boost": 0.015,
        "pitcher_stuff_penalty": 0.015,
    },
}


def daterange(start, end):

    import datetime as dt

    current = dt.date.fromisoformat(start)
    stop = dt.date.fromisoformat(end)

    while current <= stop:
        yield current.isoformat()
        current += dt.timedelta(days=1)


def write_csv(path: Path, rows: List[Dict[str, Any]]):

    if not rows:
        path.write_text("")
        return

    fieldnames = sorted({
        key
        for row in rows
        for key in row.keys()
    })

    with path.open("w", newline="") as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )

        writer.writeheader()
        writer.writerows(rows)


def safe_mean(values):

    vals = [
        v for v in values
        if v is not None
    ]

    if not vals:
        return None

    return sum(vals) / len(vals)


def safe_rmse(errors):

    vals = [
        e for e in errors
        if e is not None
    ]

    if not vals:
        return None

    return math.sqrt(
        sum(v * v for v in vals)
        / len(vals)
    )


def clamp_prob(prob):
    return max(
        0.001,
        min(0.999, prob),
    )


def brier(prob, actual):
    return (prob - actual) ** 2


def log_loss(prob, actual):

    prob = clamp_prob(prob)

    if actual == 1:
        return -math.log(prob)

    return -math.log(1 - prob)


def load_actual_results():

    sqlite_path = DATABASE_URL.replace(
        "sqlite:///",
        "",
    )

    conn = sqlite3.connect(sqlite_path)

    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT *
        FROM actual_game_results
        """
    ).fetchall()

    conn.close()

    return {
        int(row["game_pk"]): dict(row)
        for row in rows
    }


def apply_pct_modifier(
    payload,
    key,
    modifier,
):

    if key not in payload:
        return

    value = payload[key]

    if not isinstance(
        value,
        (
            int,
            float,
        ),
    ):
        return

    payload[key] = value * (
        1 + modifier
    )


def apply_config(
    raw_matchup,
    config,
):

    matchup = copy.deepcopy(
        raw_matchup
    )

    for side in [
        "away_offense_inputs",
        "home_offense_inputs",
    ]:

        offense = matchup.get(side)

        if not isinstance(
            offense,
            dict,
        ):
            continue

        if (
            "hitter_contact_boost"
            in config
        ):

            for key in [
                "xba",
                "xwoba",
                "hard_hit_pct",
                "barrel_pct",
            ]:

                apply_pct_modifier(
                    offense,
                    key,
                    config[
                        "hitter_contact_boost"
                    ],
                )

        if (
            "hitter_recent_boost"
            in config
        ):

            for key in [
                "batting_avg",
                "iso",
                "hard_hit_pct",
            ]:

                apply_pct_modifier(
                    offense,
                    key,
                    config[
                        "hitter_recent_boost"
                    ],
                )

    for side in [
        "away_pitcher_features",
        "home_pitcher_features",
    ]:

        pitcher = matchup.get(side)

        if not isinstance(
            pitcher,
            dict,
        ):
            continue

        if (
            "pitcher_stuff_penalty"
            in config
        ):

            for key in [
                "xba",
                "xwoba",
                "hard_hit_pct",
            ]:

                apply_pct_modifier(
                    pitcher,
                    key,
                    config[
                        "pitcher_stuff_penalty"
                    ],
                )

    return matchup


def extract_prediction(
    sim_output,
):

    derived = sim_output.get(
        "derived_outputs",
        {},
    )

    home_prob = derived.get(
        "home_win_probability",
        0.5,
    )

    home_runs = derived.get(
        "expected_home_runs",
        derived.get(
            "home_team_runs",
            4.5,
        ),
    )

    away_runs = derived.get(
        "expected_away_runs",
        derived.get(
            "away_team_runs",
            4.5,
        ),
    )

    return {
        "home_win_prob":
            float(home_prob),
        "away_win_prob":
            float(1 - home_prob),
        "pred_home_runs":
            float(home_runs),
        "pred_away_runs":
            float(away_runs),
        "pred_total_runs":
            float(home_runs)
            + float(away_runs),
        "pred_run_diff":
            float(home_runs)
            - float(away_runs),
    }


def main():

    actual_results = (
        load_actual_results()
    )

    engine = create_engine(
        DATABASE_URL
    )

    SessionLocal = sessionmaker(
        bind=engine
    )

    config_rows = []
    game_rows = []
    bucket_rows = []

    with SessionLocal() as session:

        all_matchups = []

        for game_date in daterange(
            BACKTEST_START,
            BACKTEST_END,
        ):

            print(
                f"[load] "
                f"loading {game_date}"
            )

            try:

                matchups = (
                    generate_matchups_for_date(
                        session,
                        game_date,
                    )
                )

            except Exception as exc:

                print(
                    f"[warn] "
                    f"failed matchup generation "
                    f"{game_date}: {exc}"
                )

                continue

            for matchup in matchups:

                game_pk = matchup.get(
                    "game_pk"
                )

                if (
                    game_pk
                    not in actual_results
                ):
                    continue

                all_matchups.append(
                    (
                        game_date,
                        matchup,
                    )
                )

        if MAX_GAMES:

            all_matchups = (
                all_matchups[
                    :MAX_GAMES
                ]
            )

        print(
            f"[info] "
            f"loaded "
            f"{len(all_matchups)} "
            f"games"
        )

        for (
            config_index,
            (
                config_name,
                config,
            ),
        ) in enumerate(
            CONFIGS.items(),
            start=1,
        ):

            print(
                f"\n"
                f"[config "
                f"{config_index}/"
                f"{len(CONFIGS)}] "
                f"{config_name}"
            )

            total_errors = []
            total_sq_errors = []

            home_errors = []
            away_errors = []
            diff_errors = []

            briers = []
            log_losses = []

            winner_correct = []

            predicted_totals = []
            actual_totals = []

            games_evaluated = 0
            sim_success = 0

            for (
                game_index,
                (
                    game_date,
                    raw_matchup,
                ),
            ) in enumerate(
                all_matchups,
                start=1,
            ):

                game_pk = raw_matchup.get(
                    "game_pk"
                )

                actual = actual_results[
                    game_pk
                ]

                matchup_copy = (
                    apply_config(
                        raw_matchup,
                        config,
                    )
                )

                try:

                    sim_output = (
                        run_full_game_simulation(
                            game_pk,
                            config={
                                "simulation_count":
                                    SIMS_PER_GAME,
                                "seed":
                                    SEED,
                                "matchup": {
                                    "raw":
                                        matchup_copy,
                                    "game_date":
                                        game_date,
                                },
                            },
                        )
                    )

                except Exception as exc:

                    print(
                        f"[warn] "
                        f"simulation failed "
                        f"{game_pk}: "
                        f"{exc}"
                    )

                    continue

                games_evaluated += 1
                sim_success += 1

                pred = (
                    extract_prediction(
                        sim_output
                    )
                )

                actual_home = float(
                    actual[
                        "home_score"
                    ]
                )

                actual_away = float(
                    actual[
                        "away_score"
                    ]
                )

                actual_total = (
                    actual_home
                    + actual_away
                )

                actual_diff = (
                    actual_home
                    - actual_away
                )

                actual_home_win = (
                    1
                    if actual_home
                    > actual_away
                    else 0
                )

                pred_home_win = (
                    1
                    if pred[
                        "home_win_prob"
                    ]
                    >= 0.5
                    else 0
                )

                total_err = abs(
                    pred[
                        "pred_total_runs"
                    ]
                    - actual_total
                )

                total_sq = (
                    pred[
                        "pred_total_runs"
                    ]
                    - actual_total
                ) ** 2

                home_err = abs(
                    pred[
                        "pred_home_runs"
                    ]
                    - actual_home
                )

                away_err = abs(
                    pred[
                        "pred_away_runs"
                    ]
                    - actual_away
                )

                diff_err = abs(
                    pred[
                        "pred_run_diff"
                    ]
                    - actual_diff
                )

                br = brier(
                    pred[
                        "home_win_prob"
                    ],
                    actual_home_win,
                )

                ll = log_loss(
                    pred[
                        "home_win_prob"
                    ],
                    actual_home_win,
                )

                total_errors.append(
                    total_err
                )

                total_sq_errors.append(
                    total_sq
                )

                home_errors.append(
                    home_err
                )

                away_errors.append(
                    away_err
                )

                diff_errors.append(
                    diff_err
                )

                briers.append(br)
                log_losses.append(ll)

                winner_correct.append(
                    1
                    if pred_home_win
                    == actual_home_win
                    else 0
                )

                predicted_totals.append(
                    pred[
                        "pred_total_runs"
                    ]
                )

                actual_totals.append(
                    actual_total
                )

                prob_bucket = round(
                    pred[
                        "home_win_prob"
                    ] * 10
                ) / 10

                bucket_rows.append(
                    {
                        "config":
                            config_name,
                        "bucket":
                            prob_bucket,
                        "actual_home_win":
                            actual_home_win,
                        "pred_home_win_prob":
                            pred[
                                "home_win_prob"
                            ],
                    }
                )

                game_rows.append(
                    {
                        "config":
                            config_name,
                        "game_pk":
                            game_pk,
                        "game_date":
                            game_date,
                        "pred_home_win_prob":
                            pred[
                                "home_win_prob"
                            ],
                        "actual_home_win":
                            actual_home_win,
                        "pred_total":
                            pred[
                                "pred_total_runs"
                            ],
                        "actual_total":
                            actual_total,
                        "total_error":
                            total_err,
                        "brier":
                            br,
                        "log_loss":
                            ll,
                    }
                )

                if (
                    game_index % 10 == 0
                ):

                    print(
                        f"[progress] "
                        f"{config_name} "
                        f"{game_index}/"
                        f"{len(all_matchups)}"
                    )

            avg_brier = safe_mean(
                briers
            )

            avg_log = safe_mean(
                log_losses
            )

            total_mae = safe_mean(
                total_errors
            )

            total_rmse = safe_rmse(
                total_sq_errors
            )

            combined_score = (
                (
                    avg_brier or 0
                )
                + (
                    avg_log or 0
                )
                + (
                    total_mae or 0
                ) / 10
                + (
                    total_rmse or 0
                ) / 10
            )

            config_rows.append(
                {
                    "config":
                        config_name,
                    "games_evaluated":
                        games_evaluated,
                    "games_with_actuals":
                        games_evaluated,
                    "simulation_success_rate":
                        round(
                            sim_success
                            / games_evaluated,
                            4,
                        )
                        if games_evaluated
                        else None,
                    "winner_accuracy":
                        round(
                            safe_mean(
                                winner_correct
                            ),
                            6,
                        )
                        if winner_correct
                        else None,
                    "home_win_brier":
                        round(
                            avg_brier,
                            6,
                        )
                        if avg_brier
                        else None,
                    "home_win_log_loss":
                        round(
                            avg_log,
                            6,
                        )
                        if avg_log
                        else None,
                    "total_run_mae":
                        round(
                            total_mae,
                            6,
                        )
                        if total_mae
                        else None,
                    "total_run_rmse":
                        round(
                            total_rmse,
                            6,
                        )
                        if total_rmse
                        else None,
                    "home_run_mae":
                        round(
                            safe_mean(
                                home_errors
                            ),
                            6,
                        )
                        if home_errors
                        else None,
                    "away_run_mae":
                        round(
                            safe_mean(
                                away_errors
                            ),
                            6,
                        )
                        if away_errors
                        else None,
                    "run_differential_mae":
                        round(
                            safe_mean(
                                diff_errors
                            ),
                            6,
                        )
                        if diff_errors
                        else None,
                    "average_predicted_total":
                        round(
                            safe_mean(
                                predicted_totals
                            ),
                            6,
                        )
                        if predicted_totals
                        else None,
                    "average_actual_total":
                        round(
                            safe_mean(
                                actual_totals
                            ),
                            6,
                        )
                        if actual_totals
                        else None,
                    "predicted_total_bias":
                        round(
                            (
                                safe_mean(
                                    predicted_totals
                                )
                                - safe_mean(
                                    actual_totals
                                )
                            ),
                            6,
                        )
                        if predicted_totals
                        and actual_totals
                        else None,
                    "combined_score":
                        round(
                            combined_score,
                            6,
                        ),
                }
            )

    ranked = sorted(
        config_rows,
        key=lambda row: row[
            "combined_score"
        ],
    )

    best = ranked[0]

    baseline = next(
        row
        for row in ranked
        if row["config"]
        == "baseline_current_production"
    )

    diagnosis = (
        "candidate_improves_calibration"
        if best["config"]
        != "baseline_current_production"
        else "baseline_still_best"
    )

    payload = {
        "diagnosis":
            diagnosis,
        "ranked_configs_by_combined_score":
            ranked,
        "ranked_configs_by_brier":
            sorted(
                ranked,
                key=lambda r: r[
                    "home_win_brier"
                ],
            ),
        "ranked_configs_by_log_loss":
            sorted(
                ranked,
                key=lambda r: r[
                    "home_win_log_loss"
                ],
            ),
        "ranked_configs_by_total_mae":
            sorted(
                ranked,
                key=lambda r: r[
                    "total_run_mae"
                ],
            ),
        "baseline_vs_best_delta": {
            "baseline":
                baseline["config"],
            "best":
                best["config"],
            "combined_score_delta":
                round(
                    baseline[
                        "combined_score"
                    ]
                    - best[
                        "combined_score"
                    ],
                    6,
                ),
        },
        "recommended_profile_config_for_next_layer":
            best["config"],
        "recommended_next_step":
            (
                "Proceed based on "
                "best real calibration "
                "candidate."
            ),
    }

    out_dir = Path("tmp")

    out_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    prefix = (
        f"true_calibration_grid_"
        f"{BACKTEST_START}_to_{BACKTEST_END}"
    )

    json_path = out_dir / f"{prefix}.json"

    json_path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
    )

    write_csv(
        out_dir / f"{prefix}_games.csv",
        game_rows,
    )

    write_csv(
        out_dir / f"{prefix}_configs.csv",
        config_rows,
    )

    write_csv(
        out_dir / f"{prefix}_rankings.csv",
        ranked,
    )

    write_csv(
        out_dir / f"{prefix}_buckets.csv",
        bucket_rows,
    )

    print(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
    )

    print(f"Wrote {json_path}")


if __name__ == "__main__":
    main()
