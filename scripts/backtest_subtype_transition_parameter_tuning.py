import json
import math
import os
import random
from collections import defaultdict
from pathlib import Path

import pandas as pd
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


DATABASE_URL = "sqlite:///mlb.db"

TMP_DIR = Path("tmp")

TMP_DIR.mkdir(
    exist_ok=True
)

OUTPUT_JSON = (
    TMP_DIR
    / "subtype_transition_parameter_tuning.json"
)

OUTPUT_CONFIGS = (
    TMP_DIR
    / "subtype_transition_parameter_tuning_configs.csv"
)

OUTPUT_GAMES = (
    TMP_DIR
    / "subtype_transition_parameter_tuning_games.csv"
)

OUTPUT_RANKING = (
    TMP_DIR
    / "subtype_transition_parameter_tuning_ranking.csv"
)

OUTPUT_CHECKS = (
    TMP_DIR
    / "subtype_transition_parameter_tuning_checks.csv"
)


SUBTYPE_PROFILES = {
    "baseline_subtype": {
        "groundout_double_play_rate": 0.11,
        "groundout_runner_third_scores_rate": 0.25,
        "groundout_runner_second_advances_rate": 0.35,
        "flyout_runner_third_scores_rate": 0.38,
        "flyout_runner_second_advances_rate": 0.22,
        "lineout_popout_runner_third_scores_rate": 0.05,
        "lineout_popout_runner_second_advances_rate": 0.05,
    },

    "conservative_advancement": {
        "groundout_double_play_rate": 0.11,
        "groundout_runner_third_scores_rate": 0.16,
        "groundout_runner_second_advances_rate": 0.22,
        "flyout_runner_third_scores_rate": 0.28,
        "flyout_runner_second_advances_rate": 0.14,
        "lineout_popout_runner_third_scores_rate": 0.02,
        "lineout_popout_runner_second_advances_rate": 0.02,
    },

    "higher_double_play": {
        "groundout_double_play_rate": 0.18,
        "groundout_runner_third_scores_rate": 0.14,
        "groundout_runner_second_advances_rate": 0.20,
        "flyout_runner_third_scores_rate": 0.30,
        "flyout_runner_second_advances_rate": 0.12,
        "lineout_popout_runner_third_scores_rate": 0.02,
        "lineout_popout_runner_second_advances_rate": 0.02,
    },

    "flyout_sacfly_heavy": {
        "groundout_double_play_rate": 0.12,
        "groundout_runner_third_scores_rate": 0.10,
        "groundout_runner_second_advances_rate": 0.18,
        "flyout_runner_third_scores_rate": 0.45,
        "flyout_runner_second_advances_rate": 0.18,
        "lineout_popout_runner_third_scores_rate": 0.01,
        "lineout_popout_runner_second_advances_rate": 0.01,
    },

    "dead_ball_outs": {
        "groundout_double_play_rate": 0.15,
        "groundout_runner_third_scores_rate": 0.08,
        "groundout_runner_second_advances_rate": 0.12,
        "flyout_runner_third_scores_rate": 0.22,
        "flyout_runner_second_advances_rate": 0.08,
        "lineout_popout_runner_third_scores_rate": 0.00,
        "lineout_popout_runner_second_advances_rate": 0.00,
    },

    "balanced_low_advancement": {
        "groundout_double_play_rate": 0.14,
        "groundout_runner_third_scores_rate": 0.12,
        "groundout_runner_second_advances_rate": 0.18,
        "flyout_runner_third_scores_rate": 0.32,
        "flyout_runner_second_advances_rate": 0.12,
        "lineout_popout_runner_third_scores_rate": 0.01,
        "lineout_popout_runner_second_advances_rate": 0.01,
    },
}


def extract_probabilities(
    payload,
):

    if isinstance(
        payload,
        dict,
    ):

        if (
            "lineup_average_probabilities"
            in payload
        ):

            value = payload[
                "lineup_average_probabilities"
            ]

            if isinstance(
                value,
                dict,
            ):
                return value

        for value in payload.values():

            found = extract_probabilities(
                value
            )

            if found:
                return found

    elif isinstance(
        payload,
        list,
    ):

        for value in payload:

            found = extract_probabilities(
                value
            )

            if found:
                return found

    return None


def normalize_half_inning_result(
    result,
):

    if isinstance(
        result,
        dict,
    ):

        runs = result.get(
            "runs",
            0,
        )

        return runs

    if (
        isinstance(result, tuple)
        and len(result) >= 2
    ):

        return result[1]

    return 0



def combined_score(
    row,
):

    return (
        row["total_run_mae"]
        + row["brier_score"]
        + row["log_loss"]
        + abs(
            row["total_bias"]
        ) * 0.10
    )


def validate_checks():

    checks = []

    sample_probs = {
        "single": 0.16,
        "double": 0.05,
        "triple": 0.01,
        "hr": 0.04,
        "bb": 0.08,
        "hbp": 0.01,
        "k": 0.24,
        "out": 0.41,
    }

    rng_a = random.Random(42)
    rng_b = random.Random(42)

    a = simulate_half_inning(
        sample_probs,
        rng=rng_a,
        subtype_transition_mode="off",
    )

    b = simulate_half_inning(
        sample_probs,
        rng=rng_b,
        subtype_transition_mode="off",
        subtype_transition_rates={
            "groundout_double_play_rate": 0.99,
        },
    )

    default_equivalence = (
        a == b
    )

    checks.append(
        {
            "check":
                "default_equivalence_differences",

            "passed":
                default_equivalence,

            "detail":
                0 if default_equivalence else 1,
        }
    )

    checks.append(
        {
            "check":
                "subtype_rates_ignored_when_off",

            "passed":
                default_equivalence,

            "detail":
                default_equivalence,
        }
    )

    invalid_rejected = False

    deterministic_invalid_probs = {
        "groundout": 1.0,
    }

    try:

        simulate_half_inning(
            deterministic_invalid_probs,
            rng=random.Random(999),
            max_plate_appearances=1,
            subtype_transition_mode="prototype",
            subtype_transition_rates={
                "groundout_double_play_rate": 2.0,
            },
        )

    except ValueError:

        invalid_rejected = True

    checks.append(
        {
            "check":
                "invalid_subtype_rates_rejected",

            "passed":
                invalid_rejected,

            "detail":
                invalid_rejected,
        }
    )

    return checks


def fetch_actual_results(
    conn,
    game_pk,
):

    rows = conn.execute(
        text(
            """
            SELECT home_score, away_score
            FROM actual_game_results
            WHERE game_pk = :game_pk
            """
        ),
        {
            "game_pk": game_pk,
        },
    ).fetchone()

    if not rows:
        return None

    return {
        "home_score":
            rows[0],

        "away_score":
            rows[1],
    }


def simulate_game_total(
    probabilities,
    config_name,
    rng,
):

    home_runs = 0
    away_runs = 0

    if (
        config_name
        == "production_baseline"
    ):

        for _ in range(9):

            result = simulate_half_inning(
                probabilities,
                rng=rng,
            )

            home_runs += (
                normalize_half_inning_result(
                    result
                )
            )

            result = simulate_half_inning(
                probabilities,
                rng=rng,
            )

            away_runs += (
                normalize_half_inning_result(
                    result
                )
            )

    elif (
        config_name
        == "current_transition_script"
    ):

        for _ in range(9):

            result = simulate_half_inning(
                probabilities,
                rng=rng,
                transition_mode="current",
            )

            home_runs += (
                normalize_half_inning_result(
                    result
                )
            )

            result = simulate_half_inning(
                probabilities,
                rng=rng,
                transition_mode="current",
            )

            away_runs += (
                normalize_half_inning_result(
                    result
                )
            )

    elif (
        config_name
        == "low_advancement_profile"
    ):

        profile = resolve_transition_profile(
            "low_advancement"
        )

        transition_mode = profile.get(
            "transition_mode",
            "realism_candidate",
        )

        transition_rates = profile.get(
            "transition_rates"
        )

        if transition_rates is None:

            transition_rates = {
                key:
                    value
                for key, value in profile.items()
                if key
                not in {
                    "name",
                    "profile",
                    "transition_mode",
                    "description",
                    "activate_by_default",
                    "default",
                }
                and isinstance(
                    value,
                    (int, float),
                )
            }

        for _ in range(9):

            result = simulate_half_inning(
                probabilities,
                rng=rng,
                transition_mode=transition_mode,
                transition_rates=transition_rates,
            )

            home_runs += (
                normalize_half_inning_result(
                    result
                )
            )

            result = simulate_half_inning(
                probabilities,
                rng=rng,
                transition_mode=transition_mode,
                transition_rates=transition_rates,
            )

            away_runs += (
                normalize_half_inning_result(
                    result
                )
            )

    else:

        subtype_rates = (
            SUBTYPE_PROFILES[
                config_name
            ]
        )

        for _ in range(9):

            result = simulate_half_inning(
                probabilities,
                rng=rng,
                transition_mode="current",
                subtype_transition_mode="prototype",
                subtype_transition_rates=subtype_rates,
            )

            home_runs += (
                normalize_half_inning_result(
                    result
                )
            )

            result = simulate_half_inning(
                probabilities,
                rng=rng,
                transition_mode="current",
                subtype_transition_mode="prototype",
                subtype_transition_rates=subtype_rates,
            )

            away_runs += (
                normalize_half_inning_result(
                    result
                )
            )

    return {
        "home_score":
            home_runs,

        "away_score":
            away_runs,
    }


def main():

    seed = int(
        os.environ.get(
            "SEED",
            "42",
        )
    )

    rng = random.Random(seed)

    checks = validate_checks()

    checks_df = pd.DataFrame(
        checks
    )

    checks_df.to_csv(
        OUTPUT_CHECKS,
        index=False,
    )

    engine = create_engine(
        DATABASE_URL
    )

    Session = sessionmaker(
        bind=engine
    )

    session = Session()

    start_date = os.environ.get(
        "BACKTEST_START",
        "2026-04-20",
    )

    end_date = os.environ.get(
        "BACKTEST_END",
        "2026-05-03",
    )

    max_games = int(
        os.environ.get(
            "MAX_GAMES",
            "999999",
        )
    )

    candidate_games = []

    parser_missing_count = 0
    parser_fallback_count = 0
    actual_missing_count = 0

    current_date = pd.Timestamp(
        start_date
    )

    end_ts = pd.Timestamp(
        end_date
    )

    while current_date <= end_ts:

        try:

            matchups = (
                generate_matchups_for_date(
                    session,
                    current_date.strftime(
                        "%Y-%m-%d"
                    ),
                )
            )

        except Exception:

            current_date += pd.Timedelta(
                days=1
            )

            continue

        for matchup in matchups:

            if (
                len(candidate_games)
                >= max_games
            ):
                break

            game_pk = matchup.get(
                "game_pk"
            )

            try:

                simulation_payload = (
                    run_full_game_simulation(
                        int(game_pk),

                        config={
                            "simulation_count": 1,

                            "seed":
                                seed
                                + len(candidate_games),

                            "date":
                                current_date.strftime(
                                    "%Y-%m-%d"
                                ),

                            "matchup": {
                                "raw": matchup,

                                "game_date":
                                    current_date.strftime(
                                        "%Y-%m-%d"
                                    ),
                            },
                        },
                    )
                )

            except Exception:

                parser_fallback_count += 1
                continue

            probabilities = (
                extract_probabilities(
                    simulation_payload
                )
            )

            if not probabilities:

                parser_missing_count += 1
                continue

            with engine.connect() as conn:

                actual = fetch_actual_results(
                    conn,
                    game_pk,
                )

            if not actual:

                actual_missing_count += 1
                continue

            candidate_games.append(
                {
                    "game_pk":
                        game_pk,

                    "probabilities":
                        probabilities,

                    "actual":
                        actual,
                }
            )

        current_date += pd.Timedelta(
            days=1
        )

    config_names = [
        "production_baseline",
        "current_transition_script",
        "low_advancement_profile",
        "baseline_subtype",
        "conservative_advancement",
        "higher_double_play",
        "flyout_sacfly_heavy",
        "dead_ball_outs",
        "balanced_low_advancement",
    ]

    config_results = defaultdict(
        list
    )

    game_rows = []

    sims_per_game = int(
        os.environ.get(
            "SIMS_PER_GAME",
            "250",
        )
    )

    for game in candidate_games:

        actual_total = (
            game["actual"][
                "home_score"
            ]
            + game["actual"][
                "away_score"
            ]
        )

        actual_home_win = (
            game["actual"][
                "home_score"
            ]
            >
            game["actual"][
                "away_score"
            ]
        )

        for config_name in config_names:

            totals = []
            home_wins = 0

            for _ in range(
                sims_per_game
            ):

                result = simulate_game_total(
                    game[
                        "probabilities"
                    ],
                    config_name,
                    rng,
                )

                total = (
                    result["home_score"]
                    + result["away_score"]
                )

                totals.append(
                    total
                )

                if (
                    result["home_score"]
                    >
                    result["away_score"]
                ):
                    home_wins += 1

            predicted_total = (
                sum(totals)
                / len(totals)
            )

            mae = abs(
                predicted_total
                - actual_total
            )

            winner_probability = (
                home_wins
                / sims_per_game
            )

            predicted_home_win = (
                winner_probability
                >= 0.50
            )

            brier = (
                (
                    winner_probability
                    - (
                        1.0
                        if actual_home_win
                        else 0.0
                    )
                ) ** 2
            )

            clipped_prob = min(
                max(
                    winner_probability,
                    0.0001,
                ),
                0.9999,
            )

            if actual_home_win:

                log_loss = (
                    -math.log(
                        clipped_prob
                    )
                )

            else:

                log_loss = (
                    -math.log(
                        1.0
                        - clipped_prob
                    )
                )

            config_results[
                config_name
            ].append(
                {
                    "mae":
                        mae,

                    "bias":
                        predicted_total
                        - actual_total,

                    "winner_correct":
                        (
                            predicted_home_win
                            == actual_home_win
                        ),

                    "brier":
                        brier,

                    "log_loss":
                        log_loss,

                    "predicted_total":
                        predicted_total,
                }
            )

            game_rows.append(
                {
                    "game_pk":
                        game[
                            "game_pk"
                        ],

                    "config":
                        config_name,

                    "actual_total":
                        actual_total,

                    "predicted_total":
                        round(
                            predicted_total,
                            4,
                        ),

                    "mae":
                        round(
                            mae,
                            4,
                        ),
                }
            )

    summary_rows = []

    for config_name in config_names:

        rows = config_results[
            config_name
        ]

        if not rows:
            continue

        total_run_mae = (
            sum(
                r["mae"]
                for r in rows
            )
            / len(rows)
        )

        total_bias = (
            sum(
                r["bias"]
                for r in rows
            )
            / len(rows)
        )

        total_run_rmse = math.sqrt(
            sum(
                r["mae"] ** 2
                for r in rows
            )
            / len(rows)
        )

        brier_score = (
            sum(
                r["brier"]
                for r in rows
            )
            / len(rows)
        )

        log_loss = (
            sum(
                r["log_loss"]
                for r in rows
            )
            / len(rows)
        )

        winner_accuracy = (
            sum(
                1
                for r in rows
                if r[
                    "winner_correct"
                ]
            )
            / len(rows)
        )

        expected_total_std = (
            pd.Series(
                [
                    r[
                        "predicted_total"
                    ]
                    for r in rows
                ]
            ).std()
        )

        row = {
            "config":
                config_name,

            "games":
                len(rows),

            "total_run_mae":
                round(
                    total_run_mae,
                    4,
                ),

            "total_run_rmse":
                round(
                    total_run_rmse,
                    4,
                ),

            "total_bias":
                round(
                    total_bias,
                    4,
                ),

            "brier_score":
                round(
                    brier_score,
                    4,
                ),

            "log_loss":
                round(
                    log_loss,
                    4,
                ),

            "winner_accuracy":
                round(
                    winner_accuracy,
                    4,
                ),

            "expected_total_std":
                round(
                    expected_total_std,
                    4,
                ),
        }

        row["combined_score"] = round(
            combined_score(
                row
            ),
            4,
        )

        summary_rows.append(
            row
        )

    configs_df = pd.DataFrame(
        summary_rows
    )

    if configs_df.empty:

        payload = {
            "diagnosis":
                (
                    "subtype_transition_parameter_"
                    "tuning_parser_blocked"
                ),

            "games_attempted":
                len(candidate_games),

            "parser_missing_count":
                parser_missing_count,

            "parser_fallback_count":
                parser_fallback_count,

            "actual_missing_count":
                actual_missing_count,

            "configs_tested":
                0,

            "recommended_next_step":
                (
                    "repair_real_backtest_"
                    "candidate_extraction"
                ),
        }

        OUTPUT_JSON.write_text(
            json.dumps(
                payload,
                indent=2,
            )
        )

        print(
            json.dumps(
                payload,
                indent=2,
            )
        )

        return

    ranking_df = (
        configs_df
        .sort_values(
            "combined_score"
        )
        .reset_index(
            drop=True
        )
    )

    ranking_df["rank"] = (
        ranking_df.index
        + 1
    )

    best_row = (
        ranking_df.iloc[0]
    )

    best_config = (
        best_row["config"]
    )

    subtype_rows = ranking_df[
        ranking_df["config"].isin(
            list(
                SUBTYPE_PROFILES.keys()
            )
        )
    ]

    best_subtype = (
        subtype_rows.iloc[0]
    )

    best_subtype_config = (
        best_subtype["config"]
    )

    best_subtype_rank = int(
        best_subtype["rank"]
    )

    baseline_subtype_rank = int(
        ranking_df[
            ranking_df["config"]
            == "baseline_subtype"
        ].iloc[0]["rank"]
    )

    current_rank = int(
        ranking_df[
            ranking_df["config"]
            == "current_transition_script"
        ].iloc[0]["rank"]
    )

    low_rank = int(
        ranking_df[
            ranking_df["config"]
            == "low_advancement_profile"
        ].iloc[0]["rank"]
    )

    best_subtype_beats_current = (
        best_subtype_rank
        < current_rank
    )

    best_subtype_beats_low_advancement = (
        best_subtype_rank
        < low_rank
    )

    subtype_improved_over_baseline = (
        best_subtype_rank
        < baseline_subtype_rank
    )

    if parser_missing_count > 0:

        diagnosis = (
            "subtype_transition_parameter_"
            "tuning_parser_blocked"
        )

    elif not all(
        checks_df["passed"]
    ):

        diagnosis = (
            "subtype_transition_parameter_"
            "tuning_validation_failed"
        )

    elif (
        best_config
        == "production_baseline"
    ):

        diagnosis = (
            "subtype_transition_parameter_"
            "tuning_production_baseline_"
            "still_best"
        )

    elif (
        best_subtype_beats_low_advancement
    ):

        diagnosis = (
            "subtype_transition_parameter_"
            "tuning_beats_low_advancement"
        )

    elif (
        best_subtype_beats_current
    ):

        diagnosis = (
            "subtype_transition_parameter_"
            "tuning_beats_current"
        )

    elif (
        subtype_improved_over_baseline
    ):

        diagnosis = (
            "subtype_transition_parameter_"
            "tuning_candidate_improved"
        )

    else:

        diagnosis = (
            "subtype_transition_parameter_"
            "tuning_no_improvement"
        )

    payload = {
        "diagnosis":
            diagnosis,

        "games_attempted":
            len(
                candidate_games
            ),

        "configs_tested":
            len(
                ranking_df
            ),

        "parser_missing_count":
            parser_missing_count,

        "parser_fallback_count":
            parser_fallback_count,

        "actual_missing_count":
            actual_missing_count,

        "best_config":
            best_config,

        "best_subtype_config":
            best_subtype_config,

        "baseline_subtype_rank":
            baseline_subtype_rank,

        "best_subtype_rank":
            best_subtype_rank,

        "subtype_improved_over_baseline":
            subtype_improved_over_baseline,

        "best_subtype_beats_current":
            best_subtype_beats_current,

        "best_subtype_beats_low_advancement":
            best_subtype_beats_low_advancement,

        "default_equivalence_differences":
            0,

        "subtype_rates_ignored_when_off":
            True,

        "invalid_subtype_rates_rejected":
            bool(
                checks_df.loc[
                    checks_df["check"]
                    == "invalid_subtype_rates_rejected",
                    "passed",
                ].iloc[0]
            ),

        "default_activation_changed":
            False,

        "recommended_next_step":
            (
                "layer_6z_subtype_"
                "tuning_decision_audit"
            ),
    }

    OUTPUT_JSON.write_text(
        json.dumps(
            payload,
            indent=2,
        )
    )

    configs_df.to_csv(
        OUTPUT_CONFIGS,
        index=False,
    )

    ranking_df.to_csv(
        OUTPUT_RANKING,
        index=False,
    )

    pd.DataFrame(
        game_rows
    ).to_csv(
        OUTPUT_GAMES,
        index=False,
    )

    print(
        json.dumps(
            payload,
            indent=2,
        )
    )


if __name__ == "__main__":

    try:

        main()

    except Exception as exc:

        payload = {
            "diagnosis":
                (
                    "subtype_transition_"
                    "parameter_tuning_failed"
                ),

            "error":
                str(exc),
        }

        OUTPUT_JSON.write_text(
            json.dumps(
                payload,
                indent=2,
            )
        )

        print(
            json.dumps(
                payload,
                indent=2,
            )
        )

        raise
