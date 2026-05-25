from __future__ import annotations

import copy
import json
import os
from pathlib import Path
from typing import Any, Dict

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.matchup_generator import generate_matchups_for_date
from mlb_app.simulation.game_engine_v2 import run_full_game_simulation


BACKTEST_START = os.getenv(
    "BACKTEST_START",
    "2026-04-20",
)

BACKTEST_END = os.getenv(
    "BACKTEST_END",
    "2026-05-03",
)

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

MAX_GAMES = int(
    os.getenv(
        "MAX_GAMES",
        "3",
    )
)

SEED = int(
    os.getenv(
        "SEED",
        "42",
    )
)


CONFIGS = {
    "baseline": {},
    "candidate_small": {
        "hitter_contact_boost": 0.05,
        "pitcher_stuff_penalty": 0.05,
    },
    "sentinel_large": {
        "hitter_contact_boost": 0.50,
        "pitcher_stuff_penalty": 0.50,
    },
}


SEARCH_TERMS = [
    "run",
    "runs",
    "score",
    "total",
    "expected",
    "win",
    "prob",
    "mean",
]


def daterange(start, end):

    import datetime as dt

    current = dt.date.fromisoformat(start)
    stop = dt.date.fromisoformat(end)

    while current <= stop:
        yield current.isoformat()
        current += dt.timedelta(days=1)


def flatten(
    obj,
    prefix="",
):

    rows = {}

    if isinstance(obj, dict):

        for key, value in obj.items():

            next_prefix = (
                f"{prefix}.{key}"
                if prefix
                else str(key)
            )

            rows.update(
                flatten(
                    value,
                    next_prefix,
                )
            )

    elif isinstance(obj, list):

        for idx, value in enumerate(obj):

            next_prefix = (
                f"{prefix}[{idx}]"
            )

            rows.update(
                flatten(
                    value,
                    next_prefix,
                )
            )

    else:

        rows[prefix] = obj

    return rows


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

    payload[key] = (
        value
        * (1 + modifier)
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
                "batting_avg",
                "iso",
            ]:

                apply_pct_modifier(
                    offense,
                    key,
                    config[
                        "hitter_contact_boost"
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
                "avg_velocity",
            ]:

                apply_pct_modifier(
                    pitcher,
                    key,
                    config[
                        "pitcher_stuff_penalty"
                    ],
                )

    return matchup


def find_matching_paths(
    flat,
):

    results = {}

    for path, value in flat.items():

        lowered = path.lower()

        if any(
            term in lowered
            for term in SEARCH_TERMS
        ):

            results[path] = value

    return results


def compare_flattened(
    baseline,
    candidate,
):

    deltas = []

    keys = sorted(
        set(baseline.keys())
        | set(candidate.keys())
    )

    for key in keys:

        base = baseline.get(key)
        cand = candidate.get(key)

        if base != cand:

            deltas.append(
                {
                    "path": key,
                    "baseline": base,
                    "candidate": cand,
                }
            )

    return deltas


def main():

    engine = create_engine(
        DATABASE_URL
    )

    SessionLocal = sessionmaker(
        bind=engine
    )

    out = {
        "games": [],
    }

    with SessionLocal() as session:

        loaded = []

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

                loaded.append(
                    (
                        game_date,
                        matchup,
                    )
                )

                if (
                    len(loaded)
                    >= MAX_GAMES
                ):
                    break

            if (
                len(loaded)
                >= MAX_GAMES
            ):
                break

        for (
            game_date,
            raw_matchup,
        ) in loaded:

            game_pk = raw_matchup.get(
                "game_pk"
            )

            print(
                f"\n"
                f"[game] "
                f"{game_pk}"
            )

            game_result = {
                "game_pk": game_pk,
                "configs": {},
            }

            baseline_outputs = None

            for (
                config_name,
                config,
            ) in CONFIGS.items():

                matchup_copy = (
                    apply_config(
                        raw_matchup,
                        config,
                    )
                )

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

                raw_flat = flatten(
                    matchup_copy
                )

                direct_inputs_flat = flatten(
                    sim_output.get(
                        "direct_inputs",
                        {},
                    )
                )

                pa_models_flat = flatten(
                    sim_output.get(
                        "pa_models",
                        {},
                    )
                )

                derived_flat = flatten(
                    sim_output.get(
                        "derived_outputs",
                        {},
                    )
                )

                matching_output_paths = (
                    find_matching_paths(
                        flatten(
                            sim_output
                        )
                    )
                )

                parser_values = {
                    "home_win_probability":
                        derived_flat.get(
                            "home_win_probability",
                            "__fallback__",
                        ),
                    "expected_home_runs":
                        derived_flat.get(
                            "expected_home_runs",
                            "__fallback__",
                        ),
                    "expected_away_runs":
                        derived_flat.get(
                            "expected_away_runs",
                            "__fallback__",
                        ),
                }

                game_result[
                    "configs"
                ][config_name] = {
                    "parser_values":
                        parser_values,
                    "matching_output_paths":
                        matching_output_paths,
                }

                if config_name == "baseline":

                    baseline_outputs = {
                        "raw":
                            raw_flat,
                        "direct_inputs":
                            direct_inputs_flat,
                        "pa_models":
                            pa_models_flat,
                        "derived":
                            derived_flat,
                    }

                else:

                    raw_deltas = (
                        compare_flattened(
                            baseline_outputs[
                                "raw"
                            ],
                            raw_flat,
                        )
                    )

                    direct_deltas = (
                        compare_flattened(
                            baseline_outputs[
                                "direct_inputs"
                            ],
                            direct_inputs_flat,
                        )
                    )

                    pa_deltas = (
                        compare_flattened(
                            baseline_outputs[
                                "pa_models"
                            ],
                            pa_models_flat,
                        )
                    )

                    derived_deltas = (
                        compare_flattened(
                            baseline_outputs[
                                "derived"
                            ],
                            derived_flat,
                        )
                    )

                    game_result[
                        "configs"
                    ][config_name][
                        "raw_delta_count"
                    ] = len(
                        raw_deltas
                    )

                    game_result[
                        "configs"
                    ][config_name][
                        "direct_input_delta_count"
                    ] = len(
                        direct_deltas
                    )

                    game_result[
                        "configs"
                    ][config_name][
                        "pa_model_delta_count"
                    ] = len(
                        pa_deltas
                    )

                    game_result[
                        "configs"
                    ][config_name][
                        "derived_delta_count"
                    ] = len(
                        derived_deltas
                    )

                    game_result[
                        "configs"
                    ][config_name][
                        "sample_raw_deltas"
                    ] = raw_deltas[
                        :15
                    ]

                    game_result[
                        "configs"
                    ][config_name][
                        "sample_direct_input_deltas"
                    ] = direct_deltas[
                        :15
                    ]

                    game_result[
                        "configs"
                    ][config_name][
                        "sample_pa_model_deltas"
                    ] = pa_deltas[
                        :15
                    ]

                    game_result[
                        "configs"
                    ][config_name][
                        "sample_derived_deltas"
                    ] = derived_deltas[
                        :15
                    ]

                    print(
                        f"{config_name}: "
                        f"raw={len(raw_deltas)} "
                        f"direct={len(direct_deltas)} "
                        f"pa={len(pa_deltas)} "
                        f"derived={len(derived_deltas)}"
                    )

            out["games"].append(
                game_result
            )

    diagnosis = (
        "parser_default_fallback_detected"
    )

    propagation_found = False

    for game in out["games"]:

        for (
            config_name,
            config_data,
        ) in game[
            "configs"
        ].items():

            if (
                config_name
                == "baseline"
            ):
                continue

            if (
                config_data.get(
                    "pa_model_delta_count",
                    0,
                )
                > 0
            ):

                propagation_found = True

    if propagation_found:

        diagnosis = (
            "candidate_propagation_confirmed_parser_needs_patch"
        )

    out["diagnosis"] = diagnosis

    out_dir = Path("tmp")

    out_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    out_path = (
        out_dir
        / (
            "true_calibration_parser_propagation.json"
        )
    )

    out_path.write_text(
        json.dumps(
            out,
            indent=2,
            sort_keys=True,
        )
    )

    print(
        json.dumps(
            {
                "diagnosis":
                    diagnosis,
                "games":
                    len(
                        out["games"]
                    ),
            },
            indent=2,
        )
    )

    print(
        f"Wrote {out_path}"
    )


if __name__ == "__main__":
    main()
