from __future__ import annotations

import csv
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.matchup_generator import generate_matchups_for_date
from mlb_app.simulation.game_engine_v2 import run_full_game_simulation


BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
MAX_GAMES = int(os.getenv("MAX_GAMES", "3"))
SIMS_PER_GAME = int(os.getenv("SIMS_PER_GAME", "25"))
SEED = int(os.getenv("SEED", "42"))

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///mlb.db",
)

PITCHER_TERMS = [
    "pitch",
    "starter",
    "probable",
    "throw",
    "arm",
    "hand",
    "platoon",
]

STUFF_TERMS = [
    "stuff",
    "arsenal",
    "whiff",
    "chase",
    "strikeout",
    "k_rate",
    "k_pct",
    "csw",
    "swinging_strike",
]

ALLOWED_TERMS = [
    "xera",
    "xwoba_allowed",
    "xba_allowed",
    "slg_allowed",
    "hard_hit_allowed",
    "barrel_allowed",
]

PA_ENGINE_TERMS = [
    "pa_model",
    "direct_inputs",
    "derived_outputs",
    "simulation_inputs",
    "probability",
    "expected_runs",
]


def flatten(obj: Any, prefix: str = "") -> Dict[str, Any]:
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
            rows.update(
                flatten(
                    value,
                    f"{prefix}[{idx}]",
                )
            )

    else:
        rows[prefix] = obj

    return rows


def keyword_score(
    path: str,
    terms: List[str],
):
    path_lower = path.lower()

    return sum(
        1
        for term in terms
        if term in path_lower
    )


def likely_consumed_score(
    pitcher_score,
    stuff_score,
    pa_score,
):
    score = 0

    score += pitcher_score * 2
    score += stuff_score * 3
    score += pa_score * 4

    if pitcher_score > 0 and pa_score > 0:
        score += 5

    if stuff_score > 0 and pa_score > 0:
        score += 7

    return score


def write_csv(path: Path, rows):
    if not rows:
        path.write_text("")
        return

    fields = sorted(
        {
            key
            for row in rows
            for key in row.keys()
        }
    )

    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
        )

        writer.writeheader()
        writer.writerows(rows)


def inventory_paths(
    source_name,
    flat,
    inventory,
):
    for path, value in flat.items():
        entry = inventory[(source_name, path)]

        entry["source"] = source_name
        entry["path"] = path
        entry["value_type"] = type(value).__name__

        if "example_value" not in entry:
            entry["example_value"] = repr(value)[:120]

        entry["game_count_seen"] += 1

        if isinstance(value, (int, float)):
            entry["numeric_count"] += 1

        pitcher_score = keyword_score(
            path,
            PITCHER_TERMS,
        )

        stuff_score = (
            keyword_score(
                path,
                STUFF_TERMS,
            )
            + keyword_score(
                path,
                ALLOWED_TERMS,
            )
        )

        pa_score = keyword_score(
            path,
            PA_ENGINE_TERMS,
        )

        entry["pitcher_keyword_score"] = max(
            entry.get(
                "pitcher_keyword_score",
                0,
            ),
            pitcher_score,
        )

        entry["stuff_keyword_score"] = max(
            entry.get(
                "stuff_keyword_score",
                0,
            ),
            stuff_score,
        )

        entry["pa_engine_keyword_score"] = max(
            entry.get(
                "pa_engine_keyword_score",
                0,
            ),
            pa_score,
        )

        entry["likely_consumed_score"] = max(
            entry.get(
                "likely_consumed_score",
                0,
            ),
            likely_consumed_score(
                pitcher_score,
                stuff_score,
                pa_score,
            ),
        )


def main():
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(bind=engine)

    inventory = defaultdict(
        lambda: defaultdict(int)
    )

    with SessionLocal() as session:
        matchups = generate_matchups_for_date(
            session,
            BACKTEST_START,
        )[:MAX_GAMES]

        for idx, matchup in enumerate(
            matchups,
            start=1,
        ):
            print(
                f"[game {idx}/{len(matchups)}] "
                f"{matchup.get('game_pk')}"
            )

            flat_payload = flatten(matchup)

            inventory_paths(
                "raw_payload",
                flat_payload,
                inventory,
            )

            sim_output = run_full_game_simulation(
                matchup["game_pk"],
                config={
                    "simulation_count":
                        SIMS_PER_GAME,
                    "seed": SEED,
                    "matchup": {
                        "raw": matchup,
                        "game_date":
                            BACKTEST_START,
                    },
                },
            )

            flat_output = flatten(
                sim_output
            )

            inventory_paths(
                "sim_output",
                flat_output,
                inventory,
            )

    rows = list(inventory.values())

    rows = sorted(
        rows,
        key=lambda r: (
            r["likely_consumed_score"],
            r["stuff_keyword_score"],
            r["pa_engine_keyword_score"],
            r["pitcher_keyword_score"],
            r["numeric_count"],
        ),
        reverse=True,
    )

    recommendations = [
        row
        for row in rows
        if (
            row["likely_consumed_score"] >= 8
            and row["numeric_count"] > 0
        )
    ][:50]

    payload = {
        "games": MAX_GAMES,
        "paths_discovered": len(rows),
        "top_recommendations": recommendations,
    }

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    json_path = (
        out_dir
        / "pitcher_stuff_payload_schema.json"
    )

    paths_csv = (
        out_dir
        / "pitcher_stuff_payload_schema_paths.csv"
    )

    recs_csv = (
        out_dir
        / "pitcher_stuff_payload_schema_recommendations.csv"
    )

    json_path.write_text(
        json.dumps(
            payload,
            indent=2,
        )
    )

    write_csv(
        paths_csv,
        rows,
    )

    write_csv(
        recs_csv,
        recommendations,
    )

    print(
        json.dumps(
            payload,
            indent=2,
        )
    )

    print(f"Wrote {json_path}")
    print(f"Wrote {paths_csv}")
    print(f"Wrote {recs_csv}")


if __name__ == "__main__":
    main()
