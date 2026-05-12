from __future__ import annotations

import copy
import csv
import json
import math
import os
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from mlb_app.matchup_generator import generate_matchups_for_date
from mlb_app.simulation.game_engine_v2 import run_full_game_simulation


BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
BACKTEST_END = os.getenv("BACKTEST_END", "2026-05-03")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mlb.db")

SIMS_PER_GAME = int(os.getenv("SIMS_PER_GAME", "250"))
SEED = int(os.getenv("SEED", "42"))

GRID = [
    ("baseline", 0.000),
    ("ultra_tiny", 0.005),
    ("tiny", 0.010),
    ("small", 0.020),
    ("medium", 0.050),
    ("large", 0.100),
    ("aggressive", 0.200),
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

BAD_ALLOWED_TERMS = [
    "xera",
    "xwoba_allowed",
    "hard_hit_allowed",
    "barrel_allowed",
]

SCALAR_PATHS = {
    "home_expected_runs":
        "derived_outputs.bullpen_adjusted_game_simulation.home_expected_runs",
    "away_expected_runs":
        "derived_outputs.bullpen_adjusted_game_simulation.away_expected_runs",
    "total_expected_runs":
        "derived_outputs.bullpen_adjusted_game_simulation.total_expected_runs",
    "home_win_probability":
        "derived_outputs.bullpen_adjusted_game_simulation.home_win_probability",
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


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return

    fields = sorted({k for row in rows for k in row.keys()})

    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def daterange(start: str, end: str):
    import datetime as dt

    current = dt.date.fromisoformat(start)
    stop = dt.date.fromisoformat(end)

    while current <= stop:
        yield current.isoformat()
        current += dt.timedelta(days=1)


def is_good_metric(key: str) -> bool:
    key = key.lower()

    return any(term in key for term in GOOD_TERMS)


def is_bad_allowed_metric(key: str) -> bool:
    key = key.lower()

    return any(term in key for term in BAD_ALLOWED_TERMS)


def mutate_pitcher_payload(payload, boost):
    touched = 0

    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(value, dict):
                touched += mutate_pitcher_payload(
                    value,
                    boost,
                )

            elif isinstance(value, list):
                for item in value:
                    touched += mutate_pitcher_payload(
                        item,
                        boost,
                    )

            elif isinstance(value, (int, float)):
                if is_good_metric(key):
                    payload[key] = float(value) * (1.0 + boost)
                    touched += 1

                elif is_bad_allowed_metric(key):
                    payload[key] = float(value) * (1.0 - boost)
                    touched += 1

    elif isinstance(payload, list):
        for item in payload:
            touched += mutate_pitcher_payload(
                item,
                boost,
            )

    return touched


def fetch_actual_results(engine):
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
                "start_date": BACKTEST_START,
                "end_date": BACKTEST_END,
            },
        )

        for row in results:
            rows.append(
                {
                    "game_pk": row.game_pk,
                    "game_date": str(row.game_date),
                    "home_score": row.home_score,
                    "away_score": row.away_score,
                    "total_score": row.home_score + row.away_score,
                    "home_win_actual":
                        1 if row.home_score > row.away_score else 0,
                }
            )

    return {
        row["game_pk"]: row
        for row in rows
    }


def get_scalars(sim_output):
    flat = flatten(sim_output)

    values = {}
    parser_missing_count = 0

    for metric, path in SCALAR_PATHS.items():
        value = flat.get(path)

        if value is None:
            parser_missing_count += 1

        values[metric] = value

    return values, parser_missing_count


def mae(values):
    return mean(abs(v) for v in values)


def rmse(values):
    return math.sqrt(mean(v * v for v in values))


def brier(preds, actuals):
    return mean(
        [
            (p - a) ** 2
            for p, a in zip(preds, actuals)
        ]
    )


def log_loss(preds, actuals):
    vals = []

    for p, a in zip(preds, actuals):
        p = max(min(p, 0.999999), 0.000001)

        vals.append(
            -(
                a * math.log(p)
                + (1 - a) * math.log(1 - p)
            )
        )

    return mean(vals)


def run_config(
    session,
    actual_lookup,
    config_name,
    boost,
):
    rows = []

    parser_missing_total = 0
    parser_fallback_total = 0
    total_touched_fields = 0

    for game_date in daterange(
        BACKTEST_START,
        BACKTEST_END,
    ):
        matchups = generate_matchups_for_date(
            session,
            game_date,
        )

        for matchup in matchups:
            game_pk = matchup.get("game_pk")

            actual = actual_lookup.get(game_pk)

            if not actual:
                continue

            copied = copy.deepcopy(matchup)

            touched = mutate_pitcher_payload(
                copied,
                boost,
            )

            total_touched_fields += touched

            sim_output = run_full_game_simulation(
                game_pk,
                config={
                    "simulation_count": SIMS_PER_GAME,
                    "seed": SEED,
                    "matchup": {
                        "raw": copied,
                        "game_date": game_date,
                    },
                },
            )

            scalars, parser_missing = get_scalars(
                sim_output
            )

            parser_missing_total += parser_missing

            home_exp = scalars.get("home_expected_runs")
            away_exp = scalars.get("away_expected_runs")
            total_exp = scalars.get("total_expected_runs")
            home_prob = scalars.get("home_win_probability")

            if any(
                v is None
                for v in [
                    home_exp,
                    away_exp,
                    total_exp,
                    home_prob,
                ]
            ):
                parser_fallback_total += 1
                continue

            rows.append(
                {
                    "config": config_name,
                    "boost": boost,
                    "game_pk": game_pk,
                    "game_date": game_date,
                    "pred_home_runs": home_exp,
                    "pred_away_runs": away_exp,
                    "pred_total_runs": total_exp,
                    "home_win_probability": home_prob,
                    "actual_home_runs": actual["home_score"],
                    "actual_away_runs": actual["away_score"],
                    "actual_total_runs": actual["total_score"],
                    "total_error":
                        total_exp - actual["total_score"],
                    "home_error":
                        home_exp - actual["home_score"],
                    "away_error":
                        away_exp - actual["away_score"],
                    "brier_component":
                        (home_prob - actual["home_win_actual"]) ** 2,
                    "logloss_component":
                        -(
                            actual["home_win_actual"]
                            * math.log(
                                max(min(home_prob, 0.999999), 0.000001)
                            )
                            + (
                                1 - actual["home_win_actual"]
                            )
                            * math.log(
                                max(
                                    min(1 - home_prob, 0.999999),
                                    0.000001,
                                )
                            )
                        ),
                    "touched_fields": touched,
                }
            )

    total_errors = [r["total_error"] for r in rows]
    home_errors = [r["home_error"] for r in rows]
    away_errors = [r["away_error"] for r in rows]

    probs = [r["home_win_probability"] for r in rows]
    actuals = [
        1 if r["actual_home_runs"] > r["actual_away_runs"] else 0
        for r in rows
    ]

    result = {
        "config": config_name,
        "boost": boost,
        "games": len(rows),
        "parser_missing_count": parser_missing_total,
        "parser_fallback_count": parser_fallback_total,
        "touched_fields": total_touched_fields,
        "total_run_mae": mae(total_errors),
        "total_run_rmse": rmse(total_errors),
        "home_run_mae": mae(home_errors),
        "away_run_mae": mae(away_errors),
        "total_bias": mean(total_errors),
        "brier_score": brier(probs, actuals),
        "log_loss": log_loss(probs, actuals),
        "win_rate_accuracy": mean(
            [
                int(
                    (r["home_win_probability"] >= 0.5)
                    == bool(actual)
                )
                for r, actual in zip(rows, actuals)
            ]
        ),
    }

    result["combined_score"] = (
        result["total_run_rmse"]
        + result["brier_score"]
        + abs(result["total_bias"])
    )

    return result, rows


def classify(results):
    baseline = next(
        r for r in results
        if r["config"] == "baseline"
    )

    nonbaseline = [
        r for r in results
        if r["config"] != "baseline"
    ]

    best = min(
        nonbaseline,
        key=lambda r: r["combined_score"],
    )

    improves_run = (
        best["total_run_rmse"]
        < baseline["total_run_rmse"]
    )

    improves_win = (
        best["brier_score"]
        < baseline["brier_score"]
        and best["log_loss"]
        < baseline["log_loss"]
    )

    if improves_run and improves_win:
        return "pitcher_stuff_signal_active_and_promising"

    if improves_win and not improves_run:
        return "pitcher_stuff_directional_only"

    ordered = sorted(
        nonbaseline,
        key=lambda r: r["boost"],
    )

    changes = 0

    prev = None

    for row in ordered:
        current = row["combined_score"]

        if prev is not None:
            direction = current - prev

            if abs(direction) > 0.0001:
                sign = 1 if direction > 0 else -1

                if "last_sign" in locals():
                    if sign != last_sign:
                        changes += 1

                last_sign = sign

        prev = current

    if changes >= 2:
        return "pitcher_stuff_unstable_or_nonmonotonic"

    if abs(best["total_bias"]) > 1.0:
        return "pitcher_stuff_overfit_or_inflationary"

    return "pitcher_stuff_below_noise_floor"


def main():
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(bind=engine)

    actual_lookup = fetch_actual_results(engine)

    config_rows = []
    game_rows = []

    with SessionLocal() as session:
        for config_name, boost in GRID:
            print(
                f"[config] {config_name} boost={boost}"
            )

            summary, rows = run_config(
                session,
                actual_lookup,
                config_name,
                boost,
            )

            config_rows.append(summary)
            game_rows.extend(rows)

    baseline = next(
        r for r in config_rows
        if r["config"] == "baseline"
    )

    for row in config_rows:
        row["combined_score_delta_vs_baseline"] = (
            row["combined_score"]
            - baseline["combined_score"]
        )

        row["rmse_delta_vs_baseline"] = (
            row["total_run_rmse"]
            - baseline["total_run_rmse"]
        )

        row["brier_delta_vs_baseline"] = (
            row["brier_score"]
            - baseline["brier_score"]
        )

        row["log_loss_delta_vs_baseline"] = (
            row["log_loss"]
            - baseline["log_loss"]
        )

        row["bias_delta_vs_baseline"] = (
            row["total_bias"]
            - baseline["total_bias"]
        )

    leaderboard = sorted(
        config_rows,
        key=lambda r: r["combined_score"],
    )

    diagnosis = classify(config_rows)

    payload = {
        "diagnosis": diagnosis,
        "backtest_start": BACKTEST_START,
        "backtest_end": BACKTEST_END,
        "sims_per_game": SIMS_PER_GAME,
        "seed": SEED,
        "leaderboard": leaderboard,
    }

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    json_path = (
        out_dir
        / "pitcher_stuff_magnitude_grid.json"
    )

    configs_csv = (
        out_dir
        / "pitcher_stuff_magnitude_grid_configs.csv"
    )

    games_csv = (
        out_dir
        / "pitcher_stuff_magnitude_grid_games.csv"
    )

    json_path.write_text(
        json.dumps(payload, indent=2)
    )

    write_csv(configs_csv, leaderboard)
    write_csv(games_csv, game_rows)

    print(
        json.dumps(
            {
                "diagnosis": diagnosis,
                "leaderboard": leaderboard,
            },
            indent=2,
        )
    )

    print(f"Wrote {json_path}")
    print(f"Wrote {configs_csv}")
    print(f"Wrote {games_csv}")


if __name__ == "__main__":
    main()
