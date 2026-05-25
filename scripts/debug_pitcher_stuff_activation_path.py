from __future__ import annotations

import copy
import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.matchup_generator import generate_matchups_for_date
from mlb_app.simulation.game_engine_v2 import run_full_game_simulation


BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
MAX_GAMES = int(os.getenv("MAX_GAMES", "3"))
SIMS_PER_GAME = int(os.getenv("SIMS_PER_GAME", "50"))
SEED = int(os.getenv("SEED", "42"))

SENTINEL_BOOST = float(
    os.getenv(
        "PITCHER_STUFF_SENTINEL_BOOST",
        "0.50",
    )
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///mlb.db",
)

SCALAR_PATHS = [
    "derived_outputs.bullpen_adjusted_game_simulation.home_expected_runs",
    "derived_outputs.bullpen_adjusted_game_simulation.away_expected_runs",
    "derived_outputs.bullpen_adjusted_game_simulation.total_expected_runs",
    "derived_outputs.bullpen_adjusted_game_simulation.home_win_probability",
]

GOOD_TERMS = [
    "stuff",
    "pitching_plus",
    "arsenal",
    "whiff",
    "chase",
    "k_rate",
    "strikeout",
]

BAD_TERMS = [
    "xera",
    "xwoba_allowed",
    "hard_hit_allowed",
    "barrel_allowed",
]

PATH_FAMILIES = {
    "starter_profiles": [
        "starter",
        "probable_pitcher",
    ],
    "home_away_pitchers": [
        "home_pitcher",
        "away_pitcher",
        "home_starter",
        "away_starter",
    ],
    "pitcher_matchup_inputs": [
        "pitcher",
        "matchup",
    ],
    "direct_inputs_pitcher_fields": [
        "direct_inputs",
        "pitcher",
    ],
    "pa_model_pitcher_fields": [
        "pa_model",
        "pitcher",
    ],
}


def flatten(obj: Any, prefix: str = "") -> Dict[str, Any]:
    rows = {}

    if isinstance(obj, dict):
        for key, value in obj.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            rows.update(flatten(value, next_prefix))

    elif isinstance(obj, list):
        for idx, value in enumerate(obj):
            rows.update(flatten(value, f"{prefix}[{idx}]"))

    else:
        rows[prefix] = obj

    return rows


def scalar_snapshot(flat: Dict[str, Any]) -> Dict[str, Any]:
    return {
        path: flat.get(path)
        for path in SCALAR_PATHS
    }


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return

    fields = sorted({k for row in rows for k in row.keys()})

    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def path_matches_family(
    path: str,
    family_terms: List[str],
) -> bool:
    path_lower = path.lower()

    return all(
        term in path_lower
        for term in family_terms
    )


def mutate_value(
    key: str,
    value: float,
    boost: float,
):
    key_lower = key.lower()

    if any(term in key_lower for term in GOOD_TERMS):
        return value * (1.0 + boost)

    if any(term in key_lower for term in BAD_TERMS):
        return value * (1.0 - boost)

    return value


def mutate_family(
    obj: Any,
    current_path: str,
    family_terms: List[str],
    boost: float,
) -> int:
    touched = 0

    if isinstance(obj, dict):
        for key, value in obj.items():
            next_path = (
                f"{current_path}.{key}"
                if current_path
                else str(key)
            )

            if isinstance(value, (dict, list)):
                touched += mutate_family(
                    value,
                    next_path,
                    family_terms,
                    boost,
                )

            elif isinstance(value, (int, float)):
                if path_matches_family(
                    next_path,
                    family_terms,
                ):
                    mutated = mutate_value(
                        key,
                        float(value),
                        boost,
                    )

                    if mutated != value:
                        obj[key] = mutated
                        touched += 1

    elif isinstance(obj, list):
        for idx, item in enumerate(obj):
            touched += mutate_family(
                item,
                f"{current_path}[{idx}]",
                family_terms,
                boost,
            )

    return touched


def diff_count(
    baseline: Dict[str, Any],
    candidate: Dict[str, Any],
    contains: str | None = None,
):
    count = 0

    for key, value in baseline.items():
        if contains and contains not in key:
            continue

        other = candidate.get(key)

        if value != other:
            count += 1

    return count


def classify_family(
    touched: int,
    direct_delta: int,
    pa_delta: int,
    derived_delta: int,
    scalar_delta: int,
):
    if touched == 0:
        return "path_not_found"

    if direct_delta == 0:
        return "path_mutated_but_not_consumed"

    if pa_delta > 0 and scalar_delta == 0:
        return "path_reaches_pa_model_only"

    if derived_delta > 0 and scalar_delta == 0:
        return "path_reaches_derived_outputs_only"

    if scalar_delta > 0:
        return "path_reaches_scalar_outputs"

    return "path_mutated_but_not_consumed"


def overall_diagnosis(
    family_rows: List[Dict[str, Any]],
):
    scalar_hits = [
        r for r in family_rows
        if r["family_classification"]
        == "path_reaches_scalar_outputs"
    ]

    pa_only = [
        r for r in family_rows
        if r["family_classification"]
        == "path_reaches_pa_model_only"
    ]

    touched = [
        r for r in family_rows
        if r["touched_field_count"] > 0
    ]

    if scalar_hits:
        return "pitcher_stuff_reaches_scalar_outputs"

    if pa_only:
        return "pitcher_stuff_reaches_pa_model_but_not_scalars"

    if touched:
        return "pitcher_stuff_mutated_metadata_only"

    return "pitcher_stuff_path_not_found"


def run_game(
    matchup_payload,
):
    return run_full_game_simulation(
        matchup_payload["game_pk"],
        config={
            "simulation_count": SIMS_PER_GAME,
            "seed": SEED,
            "matchup": {
                "raw": matchup_payload,
                "game_date": BACKTEST_START,
            },
        },
    )


def main():
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(bind=engine)

    family_rows = []
    scalar_rows = []

    with SessionLocal() as session:
        matchups = generate_matchups_for_date(
            session,
            BACKTEST_START,
        )[:MAX_GAMES]

        for idx, matchup in enumerate(matchups, start=1):
            print(
                f"[game {idx}/{len(matchups)}] "
                f"{matchup.get('game_pk')}"
            )

            baseline_payload = copy.deepcopy(matchup)

            baseline_output = run_game(
                baseline_payload
            )

            baseline_flat = flatten(
                baseline_output
            )

            baseline_scalars = scalar_snapshot(
                baseline_flat
            )

            for family_name, family_terms in PATH_FAMILIES.items():
                candidate_payload = copy.deepcopy(matchup)

                touched = mutate_family(
                    candidate_payload,
                    "",
                    family_terms,
                    SENTINEL_BOOST,
                )

                candidate_output = run_game(
                    candidate_payload
                )

                candidate_flat = flatten(
                    candidate_output
                )

                candidate_scalars = scalar_snapshot(
                    candidate_flat
                )

                direct_delta_count = diff_count(
                    flatten(matchup),
                    flatten(candidate_payload),
                )

                pa_delta_count = diff_count(
                    baseline_flat,
                    candidate_flat,
                    contains="pa_model",
                )

                derived_delta_count = diff_count(
                    baseline_flat,
                    candidate_flat,
                    contains="derived_outputs",
                )

                scalar_nonzero_count = sum(
                    1
                    for path in SCALAR_PATHS
                    if baseline_scalars.get(path)
                    != candidate_scalars.get(path)
                )

                family_classification = classify_family(
                    touched,
                    direct_delta_count,
                    pa_delta_count,
                    derived_delta_count,
                    scalar_nonzero_count,
                )

                family_rows.append(
                    {
                        "game_pk": matchup.get("game_pk"),
                        "family": family_name,
                        "family_terms": "|".join(
                            family_terms
                        ),
                        "touched_field_count": touched,
                        "direct_delta_count": direct_delta_count,
                        "pa_model_delta_count": pa_delta_count,
                        "derived_output_delta_count":
                            derived_delta_count,
                        "scalar_nonzero_count":
                            scalar_nonzero_count,
                        "family_classification":
                            family_classification,
                    }
                )

                for path in SCALAR_PATHS:
                    scalar_rows.append(
                        {
                            "game_pk": matchup.get("game_pk"),
                            "family": family_name,
                            "scalar_path": path,
                            "baseline_value":
                                baseline_scalars.get(path),
                            "candidate_value":
                                candidate_scalars.get(path),
                            "changed":
                                baseline_scalars.get(path)
                                != candidate_scalars.get(path),
                        }
                    )

    diagnosis = overall_diagnosis(
        family_rows
    )

    payload = {
        "diagnosis": diagnosis,
        "games": len(
            set(r["game_pk"] for r in family_rows)
        ),
        "sentinel_boost": SENTINEL_BOOST,
        "families": family_rows,
    }

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    json_path = (
        out_dir
        / "pitcher_stuff_activation_path.json"
    )

    family_csv = (
        out_dir
        / "pitcher_stuff_activation_path_families.csv"
    )

    scalar_csv = (
        out_dir
        / "pitcher_stuff_activation_path_scalar_deltas.csv"
    )

    json_path.write_text(
        json.dumps(payload, indent=2)
    )

    write_csv(family_csv, family_rows)
    write_csv(scalar_csv, scalar_rows)

    print(
        json.dumps(
            {
                "diagnosis": diagnosis,
                "families": family_rows,
            },
            indent=2,
        )
    )

    print(f"Wrote {json_path}")
    print(f"Wrote {family_csv}")
    print(f"Wrote {scalar_csv}")


if __name__ == "__main__":
    main()
