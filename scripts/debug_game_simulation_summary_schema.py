from __future__ import annotations

import csv
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


TARGET_PREFIXES = [
    "derived_outputs.game_simulation",
    "derived_outputs.bullpen_adjusted_game_simulation",
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


def classify_path(
    path,
    value,
):

    lowered = path.lower()

    if (
        "win"
        in lowered
        and "prob"
        in lowered
    ):
        return (
            "win_probability_candidate"
        )

    if (
        "team_total"
        in lowered
        and "prob"
        in lowered
    ):
        return (
            "team_total_probability_ladder"
        )

    if (
        "total"
        in lowered
        and "prob"
        in lowered
    ):
        return (
            "total_probability_ladder"
        )

    if (
        "distribution"
        in lowered
        or "histogram"
        in lowered
    ):
        return (
            "score_distribution"
        )

    if any(
        token in lowered
        for token in [
            "mean",
            "average",
            "expected",
            "exp_",
        ]
    ):

        if isinstance(
            value,
            (
                int,
                float,
            ),
        ):

            return (
                "mean_or_expected_runs_candidate"
            )

    if any(
        token in lowered
        for token in [
            "summary",
            "metadata",
            "diagnostic",
        ]
    ):
        return (
            "summary_or_metadata"
        )

    return "unknown"


def plausible_scalar(
    value,
):

    return (
        isinstance(
            value,
            (
                int,
                float,
            ),
        )
        and 0 <= value <= 24
    )


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

    rows = []

    direct_scalar_candidates = []

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
            matchup,
        ) in loaded:

            game_pk = matchup.get(
                "game_pk"
            )

            print(
                f"[game] {game_pk}"
            )

            output = (
                run_full_game_simulation(
                    game_pk,
                    config={
                        "simulation_count":
                            SIMS_PER_GAME,
                        "seed":
                            SEED,
                        "matchup": {
                            "raw":
                                matchup,
                            "game_date":
                                game_date,
                        },
                    },
                )
            )

            flat = flatten(output)

            for (
                path,
                value,
            ) in flat.items():

                if not any(
                    path.startswith(
                        prefix
                    )
                    for prefix
                    in TARGET_PREFIXES
                ):
                    continue

                path_class = (
                    classify_path(
                        path,
                        value,
                    )
                )

                row = {
                    "game_pk":
                        game_pk,
                    "path":
                        path,
                    "value":
                        value,
                    "path_class":
                        path_class,
                    "is_numeric":
                        isinstance(
                            value,
                            (
                                int,
                                float,
                            ),
                        ),
                    "plausible_scalar":
                        plausible_scalar(
                            value
                        ),
                }

                rows.append(
                    row
                )

                if (
                    path_class
                    == "mean_or_expected_runs_candidate"
                    and plausible_scalar(
                        value
                    )
                ):

                    direct_scalar_candidates.append(
                        row
                    )

        recommendations = []

        for row in direct_scalar_candidates[:50]:

            recommendations.append(
                {
                    "path":
                        row["path"],
                    "value":
                        row["value"],
                    "path_class":
                        row[
                            "path_class"
                        ],
                }
            )

        strategy = (
            "engine_summary_outputs_missing"
        )

        if direct_scalar_candidates:

            strategy = (
                "direct_scalar_parser"
            )

        else:

            probability_paths = [
                row
                for row in rows
                if row[
                    "path_class"
                ]
                in [
                    "total_probability_ladder",
                    "team_total_probability_ladder",
                ]
            ]

            if probability_paths:

                strategy = (
                    "derive_from_probability_ladders"
                )

        payload = {
            "games":
                len(loaded),
            "strategy":
                strategy,
            "direct_scalar_candidates":
                len(
                    direct_scalar_candidates
                ),
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
                "game_simulation_summary_schema.json"
            )
        )

        csv_path = (
            out_dir
            / (
                "game_simulation_summary_schema_paths.csv"
            )
        )

        rec_path = (
            out_dir
            / (
                "game_simulation_summary_schema_recommendations.csv"
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
            rows,
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
