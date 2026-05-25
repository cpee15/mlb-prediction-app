from __future__ import annotations

import copy
import csv
import json
import math
import os
import random
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
BOOTSTRAP_SAMPLES = int(os.getenv("BOOTSTRAP_SAMPLES", "1000"))
SEED = int(os.getenv("SEED", "42"))

MICRO_GRID = [
    ("baseline", 0.000),
    ("ultra_tiny", 0.005),
    ("tiny", 0.010),
    ("tiny_plus", 0.015),
    ("small", 0.020),
    ("small_plus", 0.025),
]

EXPECTED_CONTACT_KEYS = [
    "xba",
    "xwoba",
    "expected_batting_average",
    "expected_slugging",
    "expected_woba",
    "avg_exit_velocity",
    "hard_hit_pct",
    "hard_hit_rate",
    "barrel_pct",
    "barrel_rate",
    "iso",
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


def apply_expected_contact_boost_to_dict(payload, boost):
    touched = 0

    for key in EXPECTED_CONTACT_KEYS:
        if key not in payload:
            continue

        value = payload.get(key)

        if not isinstance(value, (int, float)):
            continue

        payload[key] = float(value) * (1.0 + boost)
        touched += 1

    return touched


def apply_boost(matchup, boost):
    copied = copy.deepcopy(matchup)

    touched = 0

    for side in ["away_offense_inputs", "home_offense_inputs"]:
        offense = copied.get(side)

        if isinstance(offense, dict):
            touched += apply_expected_contact_boost_to_dict(
                offense,
                boost,
            )

            lineup = offense.get("lineup")

            if isinstance(lineup, list):
                for hitter in lineup:
                    if isinstance(hitter, dict):
                        touched += apply_expected_contact_boost_to_dict(
                            hitter,
                            boost,
                        )

                        sim_inputs = hitter.get("simulation_inputs")

                        if isinstance(sim_inputs, dict):
                            touched += apply_expected_contact_boost_to_dict(
                                sim_inputs,
                                boost,
                            )

    return copied, touched


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


def percentile(values, pct):
    if not values:
        return None

    values = sorted(values)

    idx = int((len(values) - 1) * pct)

    return values[idx]


def bootstrap_metric_delta(
    baseline_rows,
    candidate_rows,
    metric_key,
):
    paired = []

    baseline_lookup = {
        row["game_pk"]: row
        for row in baseline_rows
    }

    candidate_lookup = {
        row["game_pk"]: row
        for row in candidate_rows
    }

    common_games = sorted(
        set(baseline_lookup.keys())
        & set(candidate_lookup.keys())
    )

    for game_pk in common_games:
        paired.append(
            (
                baseline_lookup[game_pk][metric_key],
                candidate_lookup[game_pk][metric_key],
            )
        )

    samples = []

    rng = random.Random(SEED)

    for _ in range(BOOTSTRAP_SAMPLES):
        sample = [
            rng.choice(paired)
            for _ in range(len(paired))
        ]

        baseline_mean = mean([x[0] for x in sample])
        candidate_mean = mean([x[1] for x in sample])

        samples.append(
            candidate_mean - baseline_mean
        )

    return {
        "metric": metric_key,
        "bootstrap_mean_delta": mean(samples),
        "ci_lower": percentile(samples, 0.025),
        "ci_upper": percentile(samples, 0.975),
    }


def run_config(
    session,
    actual_lookup,
    config_name,
    boost,
):
    rows = []

    parser_missing_total = 0
    parser_fallback_total = 0

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

            boosted_matchup, touched = apply_boost(
                matchup,
                boost,
            )

            sim_output = run_full_game_simulation(
                game_pk,
                config={
                    "simulation_count": SIMS_PER_GAME,
                    "seed": SEED,
                    "matchup": {
                        "raw": boosted_matchup,
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
                    "touched_fields": touched,
                    "home_expected_runs": home_exp,
                    "away_expected_runs": away_exp,
                    "total_expected_runs": total_exp,
                    "home_win_probability": home_prob,
                    "home_actual_runs": actual["home_score"],
                    "away_actual_runs": actual["away_score"],
                    "total_actual_runs": actual["total_score"],
                    "home_win_actual": actual["home_win_actual"],
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
                }
            )

    total_errors = [r["total_error"] for r in rows]
    home_errors = [r["home_error"] for r in rows]
    away_errors = [r["away_error"] for r in rows]

    probs = [r["home_win_probability"] for r in rows]
    actuals = [r["home_win_actual"] for r in rows]

    config_summary = {
        "config": config_name,
        "boost": boost,
        "games": len(rows),
        "parser_missing_count": parser_missing_total,
        "parser_fallback_count": parser_fallback_total,
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
                    == bool(r["home_win_actual"])
                )
                for r in rows
            ]
        ),
    }

    config_summary["combined_score"] = (
        config_summary["total_run_rmse"]
        + config_summary["brier_score"]
        + abs(config_summary["total_bias"])
    )

    return config_summary, rows


def classify(configs, bootstrap_rows):
    baseline = next(
        r for r in configs
        if r["config"] == "baseline"
    )

    nonbaseline = [
        r for r in configs
        if r["config"] != "baseline"
    ]

    best = min(
        nonbaseline,
        key=lambda r: r["combined_score"],
    )

    if best["combined_score"] >= baseline["combined_score"]:
        return "expected_contact_below_noise_floor"

    unstable = False

    sorted_grid = sorted(
        nonbaseline,
        key=lambda r: r["boost"],
    )

    prev = None

    for row in sorted_grid:
        if prev and row["combined_score"] < prev:
            unstable = True
        prev = row["combined_score"]

    if unstable:
        return "expected_contact_unstable_or_nonmonotonic"

    for row in configs:
        if abs(row["total_bias"]) > 1.0:
            return "expected_contact_overfit_or_inflationary"

    strong = False

    for row in bootstrap_rows:
        if (
            row["metric"] == "total_error"
            and row["ci_upper"] < 0
        ):
            strong = True

    if strong:
        return "expected_contact_micro_signal_confirmed"

    return "expected_contact_below_noise_floor"


def main():
    random.seed(SEED)

    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(bind=engine)

    actual_lookup = fetch_actual_results(engine)

    config_rows = []
    game_rows = []
    bootstrap_rows = []

    with SessionLocal() as session:
        for config_name, boost in MICRO_GRID:
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

    baseline_rows = [
        r for r in game_rows
        if r["config"] == "baseline"
    ]

    for config_name, _ in MICRO_GRID:
        if config_name == "baseline":
            continue

        candidate_rows = [
            r for r in game_rows
            if r["config"] == config_name
        ]

        for metric_key in [
            "total_error",
            "brier_component",
            "logloss_component",
        ]:
            row = bootstrap_metric_delta(
                baseline_rows,
                candidate_rows,
                metric_key,
            )

            row["config"] = config_name

            bootstrap_rows.append(row)

    diagnosis = classify(
        config_rows,
        bootstrap_rows,
    )

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

        row["bias_delta_vs_baseline"] = (
            row["total_bias"]
            - baseline["total_bias"]
        )

        row["brier_delta_vs_baseline"] = (
            row["brier_score"]
            - baseline["brier_score"]
        )

        row["log_loss_delta_vs_baseline"] = (
            row["log_loss"]
            - baseline["log_loss"]
        )

    leaderboard = sorted(
        config_rows,
        key=lambda r: r["combined_score"],
    )

    payload = {
        "diagnosis": diagnosis,
        "backtest_start": BACKTEST_START,
        "backtest_end": BACKTEST_END,
        "sims_per_game": SIMS_PER_GAME,
        "bootstrap_samples": BOOTSTRAP_SAMPLES,
        "leaderboard": leaderboard,
        "bootstrap": bootstrap_rows,
    }

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    json_path = (
        out_dir
        / "expected_contact_micro_grid.json"
    )

    configs_csv = (
        out_dir
        / "expected_contact_micro_grid_configs.csv"
    )

    games_csv = (
        out_dir
        / "expected_contact_micro_grid_games.csv"
    )

    bootstrap_csv = (
        out_dir
        / "expected_contact_micro_grid_bootstrap.csv"
    )

    json_path.write_text(
        json.dumps(payload, indent=2)
    )

    write_csv(configs_csv, leaderboard)
    write_csv(games_csv, game_rows)
    write_csv(bootstrap_csv, bootstrap_rows)

    print(
        json.dumps(
            {
                "diagnosis": diagnosis,
                "leaderboard": leaderboard[:6],
            },
            indent=2,
        )
    )

    print(f"Wrote {json_path}")
    print(f"Wrote {configs_csv}")
    print(f"Wrote {games_csv}")
    print(f"Wrote {bootstrap_csv}")


if __name__ == "__main__":
    main()
