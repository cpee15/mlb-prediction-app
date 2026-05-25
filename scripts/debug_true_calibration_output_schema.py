from __future__ import annotations

import copy
import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List

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

DENY_TERMS = [
    "direct_inputs",
    "pa_models",
    "raw",
    "lineup",
    "distribution",
    "histogram",
    "bucket",
]

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
            ]:

                apply_pct_modifier(
                    pitcher,
                    key,
                    config[
                        "pitcher_stuff_penalty"
                    ],
                )

    return matchup


def plausible_class(
    path,
    value,
):

    lowered = path.lower()

    if any(
        deny in lowered
        for deny in DENY_TERMS
    ):
        return (
            "bad_count_or_distribution"
        )

    if (
        "win"
        in lowered
        or "prob"
        in lowered
    ):

        if (
            isinstance(
                value,
                (
                    int,
                    float,
                ),
            )
            and 0 <= value <= 1
        ):

            return "win_probability"

    if (
        "total"
        in lowered
        and isinstance(
            value,
            (
                int,
                float,
            ),
        )
        and 0 <= value <= 24
    ):

        return "total_runs"

    if (
        (
            "home"
            in lowered
            or "away"
            in lowered
        )
        and isinstance(
            value,
            (
                int,
                float,
            ),
        )
        and 0 <= value <= 12
    ):

        return "team_runs"

    return "unknown"


def likely_useful(
    path,
    base,
    cand,
    sent,
):

    if not all(
        isinstance(
            x,
            (
                int,
                float,
            ),
        )
        for x in [
            base,
            cand,
            sent,
        ]
    ):
        return False

    lowered = path.lower()

    if any(
        deny in lowered
        for deny in DENY_TERMS
    ):
        return False

    if not any(
        term in lowered
        for term in SEARCH_TERMS
    ):
        return False

    if (
        base != cand
        or base != sent
    ):
        return True

    return False


def write_csv(path, rows):

    if not rows:
        path.write_text("")
        return

    fields = sorted({
        key
        for row in rows
        for key in row.keys()
    })

    with path.open(
        "w",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
        )

        writer.writeheader()
        writer.writerows(rows)


def main():

    engine = create_engine(
        DATABASE_URL
    )

    SessionLocal = sessionmaker(
        bind=engine
    )

    schema_rows = []
    recommendations = []

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

            outputs = {}

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

                outputs[
                    config_name
                ] = flatten(
                    sim_output
                )

            baseline = outputs[
                "baseline"
            ]

            candidate = outputs[
                "candidate_small"
            ]

            sentinel = outputs[
                "sentinel_large"
            ]

            all_paths = sorted(
                set(baseline.keys())
                | set(candidate.keys())
                | set(sentinel.keys())
            )

            for path in all_paths:

                base = baseline.get(path)
                cand = candidate.get(path)
                sent = sentinel.get(path)

                if not any(
                    term
                    in path.lower()
                    for term
                    in SEARCH_TERMS
                ):
                    continue

                row = {
                    "game_pk":
                        game_pk,
                    "path":
                        path,
                    "section":
                        path.split(".")[0],
                    "baseline_value":
                        base,
                    "candidate_value":
                        cand,
                    "sentinel_value":
                        sent,
                    "candidate_delta":
                        (
                            cand - base
                            if isinstance(
                                base,
                                (
                                    int,
                                    float,
                                ),
                            )
                            and isinstance(
                                cand,
                                (
                                    int,
                                    float,
                                ),
                            )
                            else None
                        ),
                    "sentinel_delta":
                        (
                            sent - base
                            if isinstance(
                                base,
                                (
                                    int,
                                    float,
                                ),
                            )
                            and isinstance(
                                sent,
                                (
                                    int,
                                    float,
                                ),
                            )
                            else None
                        ),
                    "plausibility_class":
                        plausible_class(
                            path,
                            base,
                        ),
                    "likely_useful":
                        likely_useful(
                            path,
                            base,
                            cand,
                            sent,
                        ),
                }

                schema_rows.append(
                    row
                )

        useful_rows = [
            row
            for row in schema_rows
            if row[
                "likely_useful"
            ]
        ]

        useful_rows = sorted(
            useful_rows,
            key=lambda row: (
                str(
                    row[
                        "plausibility_class"
                    ]
                ),
                abs(
                    row.get(
                        "sentinel_delta"
                    )
                    or 0
                ),
            ),
            reverse=True,
        )

        for row in useful_rows[:50]:

            recommendations.append(
                {
                    "path":
                        row["path"],
                    "plausibility_class":
                        row[
                            "plausibility_class"
                        ],
                    "candidate_delta":
                        row[
                            "candidate_delta"
                        ],
                    "sentinel_delta":
                        row[
                            "sentinel_delta"
                        ],
                }
            )

        diagnosis = (
            "observed_output_paths_found_patch_grid_parser"
        )

        if not useful_rows:

            diagnosis = (
                "engine_outputs_do_not_expose_calibration_fields"
            )

        payload = {
            "diagnosis":
                diagnosis,
            "games":
                len(loaded),
            "candidate_paths_found":
                len(useful_rows),
            "top_recommendations":
                recommendations[
                    :20
                ],
        }

        out_dir = Path("tmp")

        out_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        json_path = (
            out_dir
            / (
                "true_calibration_output_schema.json"
            )
        )

        csv_path = (
            out_dir
            / (
                "true_calibration_output_schema_paths.csv"
            )
        )

        rec_path = (
            out_dir
            / (
                "true_calibration_output_schema_recommendations.csv"
            )
        )

        json_path.write_text(
            json.dumps(
                payload,
                indent=2,
                sort_keys=True,
            )
        )

        write_csv(
            csv_path,
            schema_rows,
        )

        write_csv(
            rec_path,
            recommendations,
        )

        print(
            json.dumps(
                payload,
                indent=2,
            )
        )

        print(
            f"Wrote {json_path}"
        )

        print(
            f"Wrote {csv_path}"
        )

        print(
            f"Wrote {rec_path}"
        )


if __name__ == "__main__":
    main()
