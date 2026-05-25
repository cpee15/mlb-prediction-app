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

MUTATION_FAMILIES = {
    "pitcher_profile_k_rate": [
        "pitcher_profile",
        "k_rate",
    ],
    "pitcher_profile_xwoba_allowed": [
        "pitcher_profile",
        "xwoba_allowed",
    ],
    "pitcher_profile_xba_allowed": [
        "pitcher_profile",
        "xba_allowed",
    ],
    "arsenal_whiff_pct": [
        "arsenal",
        "whiff_pct",
    ],
    "arsenal_strikeout_pct": [
        "arsenal",
        "strikeout_pct",
    ],
    "arsenal_xwoba": [
        "arsenal",
        "xwoba",
    ],
    "arsenal_hard_hit_pct": [
        "arsenal",
        "hard_hit_pct",
    ],
}


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


def path_matches(
    path: str,
    terms: List[str],
):
    path_lower = path.lower()

    return all(
        term in path_lower
        for term in terms
    )


def mutate_numeric(
    path: str,
    key: str,
    value: float,
):
    key_lower = key.lower()

    positive_terms = [
        "k_rate",
        "whiff_pct",
        "strikeout_pct",
    ]

    negative_terms = [
        "xba_allowed",
        "xwoba_allowed",
        "xwoba",
        "hard_hit_pct",
    ]

    if any(
        term in key_lower
        for term in positive_terms
    ):
        return value * (1.0 + SENTINEL_BOOST)

    if any(
        term in key_lower
        for term in negative_terms
    ):
        return value * (1.0 - SENTINEL_BOOST)

    return value


def mutate_family(
    obj: Any,
    current_path: str,
    family_terms: List[str],
):
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
                )

            elif isinstance(value, (int, float)):
                if path_matches(
                    next_path,
                    family_terms,
                ):
                    mutated = mutate_numeric(
                        next_path,
                        key,
                        float(value),
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
            )

    return touched


def run_game(
    matchup_payload,
):
    return run_full_game_simulation(
        matchup_payload["game_pk"],
        config={
            "simulation_count":
                SIMS_PER_GAME,
            "seed": SEED,
            "matchup": {
                "raw": matchup_payload,
                "game_date":
                    BACKTEST_START,
            },
        },
    )


def scalar_snapshot(flat):
    return {
        path: flat.get(path)
        for path in SCALAR_PATHS
    }


def diff_count(
    baseline,
    candidate,
    contains=None,
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
    touched,
    pa_input_deltas,
    pa_prob_deltas,
    derived_deltas,
    scalar_deltas,
):
    if touched == 0:
        return "raw_path_not_found"

    if (
        touched > 0
        and pa_input_deltas == 0
        and pa_prob_deltas == 0
        and derived_deltas == 0
        and scalar_deltas == 0
    ):
        return "raw_path_mutated_but_not_consumed"

    if (
        pa_input_deltas > 0
        and scalar_deltas == 0
    ):
        return "reaches_pa_model_inputs"

    if (
        pa_prob_deltas > 0
        and scalar_deltas == 0
    ):
        return "reaches_pa_model_probabilities"

    if scalar_deltas > 0:
        return "reaches_scalar_outputs"

    return "engine_generated_output_only"


def overall_diagnosis(rows):
    if any(
        r["classification"]
        == "reaches_scalar_outputs"
        for r in rows
    ):
        return "pitcher_consumed_seam_confirmed"

    if any(
        r["classification"]
        == "reaches_pa_model_inputs"
        for r in rows
    ):
        return (
            "pitcher_mutation_reaches_pa_model_but_not_scalars"
        )

    if all(
        r["classification"]
        == "raw_path_not_found"
        for r in rows
    ):
        return "pitcher_raw_paths_not_found"

    if all(
        r["classification"]
        in [
            "engine_generated_output_only",
            "raw_path_mutated_but_not_consumed",
        ]
        for r in rows
    ):
        return (
            "pitcher_paths_output_only_engine_generated"
        )

    return "pitcher_injection_requires_engine_patch"


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

        for idx, matchup in enumerate(
            matchups,
            start=1,
        ):
            print(
                f"[game {idx}/{len(matchups)}] "
                f"{matchup.get('game_pk')}"
            )

            baseline_payload = copy.deepcopy(
                matchup
            )

            baseline_output = run_game(
                baseline_payload
            )

            baseline_flat = flatten(
                baseline_output
            )

            baseline_scalars = scalar_snapshot(
                baseline_flat
            )

            for (
                family_name,
                family_terms,
            ) in MUTATION_FAMILIES.items():

                candidate_payload = copy.deepcopy(
                    matchup
                )

                touched = mutate_family(
                    candidate_payload,
                    "",
                    family_terms,
                )

                candidate_output = run_game(
                    candidate_payload
                )

                candidate_flat = flatten(
                    candidate_output
                )

                candidate_scalars = (
                    scalar_snapshot(
                        candidate_flat
                    )
                )

                pa_input_deltas = diff_count(
                    baseline_flat,
                    candidate_flat,
                    contains="direct_inputs",
                )

                pa_prob_deltas = diff_count(
                    baseline_flat,
                    candidate_flat,
                    contains="probability",
                )

                derived_deltas = diff_count(
                    baseline_flat,
                    candidate_flat,
                    contains="derived_outputs",
                )

                scalar_deltas = sum(
                    1
                    for path in SCALAR_PATHS
                    if (
                        baseline_scalars.get(path)
                        != candidate_scalars.get(path)
                    )
                )

                classification = classify_family(
                    touched,
                    pa_input_deltas,
                    pa_prob_deltas,
                    derived_deltas,
                    scalar_deltas,
                )

                family_rows.append(
                    {
                        "game_pk":
                            matchup.get("game_pk"),
                        "family":
                            family_name,
                        "terms":
                            "|".join(
                                family_terms
                            ),
                        "touched_fields":
                            touched,
                        "pa_input_deltas":
                            pa_input_deltas,
                        "pa_probability_deltas":
                            pa_prob_deltas,
                        "derived_output_deltas":
                            derived_deltas,
                        "scalar_deltas":
                            scalar_deltas,
                        "classification":
                            classification,
                    }
                )

                for path in SCALAR_PATHS:
                    scalar_rows.append(
                        {
                            "game_pk":
                                matchup.get("game_pk"),
                            "family":
                                family_name,
                            "scalar_path":
                                path,
                            "baseline":
                                baseline_scalars.get(path),
                            "candidate":
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
            set(
                r["game_pk"]
                for r in family_rows
            )
        ),
        "sentinel_boost":
            SENTINEL_BOOST,
        "families":
            family_rows,
    }

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    json_path = (
        out_dir
        / "pitcher_consumed_seam_mutation.json"
    )

    families_csv = (
        out_dir
        / "pitcher_consumed_seam_mutation_families.csv"
    )

    scalar_csv = (
        out_dir
        / "pitcher_consumed_seam_mutation_scalar_deltas.csv"
    )

    json_path.write_text(
        json.dumps(
            payload,
            indent=2,
        )
    )

    write_csv(
        families_csv,
        family_rows,
    )

    write_csv(
        scalar_csv,
        scalar_rows,
    )

    print(
        json.dumps(
            payload,
            indent=2,
        )
    )

    print(f"Wrote {json_path}")
    print(f"Wrote {families_csv}")
    print(f"Wrote {scalar_csv}")


if __name__ == "__main__":
    main()
