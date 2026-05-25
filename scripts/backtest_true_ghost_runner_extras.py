from __future__ import annotations

import csv
import json
import math
import os
import random
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from mlb_app.matchup_generator import generate_matchups_for_date
from mlb_app.simulation.game_engine_v2 import run_full_game_simulation
from mlb_app.simulation.inning_simulator import simulate_half_inning


BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
BACKTEST_END = os.getenv("BACKTEST_END", "2026-05-03")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mlb.db")

SIMS_PER_GAME = int(os.getenv("SIMS_PER_GAME", "250"))
BOOTSTRAP_SAMPLES = int(os.getenv("BOOTSTRAP_SAMPLES", "1000"))
MAX_EXTRA_INNINGS = int(os.getenv("MAX_EXTRA_INNINGS", "6"))
SEED = int(os.getenv("SEED", "42"))

MAX_GAMES_RAW = os.getenv("MAX_GAMES")
MAX_GAMES = int(MAX_GAMES_RAW) if MAX_GAMES_RAW else None

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

SCALAR_PATHS = {
    "home_expected_runs":
        "derived_outputs.bullpen_adjusted_game_simulation.home_expected_runs",
    "away_expected_runs":
        "derived_outputs.bullpen_adjusted_game_simulation.away_expected_runs",
    "total_expected_runs":
        "derived_outputs.bullpen_adjusted_game_simulation.total_expected_runs",
    "home_win_probability":
        "derived_outputs.bullpen_adjusted_game_simulation.home_win_probability",
    "away_win_probability":
        "derived_outputs.bullpen_adjusted_game_simulation.away_win_probability",
    "tie_after_regulation_probability":
        "derived_outputs.bullpen_adjusted_game_simulation.tie_after_regulation_probability",
}

PA_PATHS = {
    "away_bullpen":
        "pa_models.away_vs_home_bullpen.lineup_average_probabilities",
    "home_bullpen":
        "pa_models.home_vs_away_bullpen.lineup_average_probabilities",
    "away_starter":
        "pa_models.away_vs_home_starter.lineup_average_probabilities",
    "home_starter":
        "pa_models.home_vs_away_starter.lineup_average_probabilities",
}


def daterange(start: str, end: str):
    import datetime as dt

    current = dt.date.fromisoformat(start)
    stop = dt.date.fromisoformat(end)

    while current <= stop:
        yield current.isoformat()
        current += dt.timedelta(days=1)


def get_nested(obj: Any, path: str) -> Any:
    current = obj

    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)

    return current


def safe_round(value: Any, digits: int = 6) -> Any:
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except Exception:
        return value


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return

    fields = sorted({key for row in rows for key in row.keys()})

    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def fetch_actual_results(engine) -> Dict[int, Dict[str, Any]]:
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
            try:
                game_pk = int(row.game_pk)
                home_score = int(row.home_score)
                away_score = int(row.away_score)
            except Exception:
                continue

            rows.append(
                {
                    "game_pk": game_pk,
                    "game_date": str(row.game_date),
                    "home_score": home_score,
                    "away_score": away_score,
                    "actual_total_runs": home_score + away_score,
                    "actual_home_win": 1 if home_score > away_score else 0,
                }
            )

    return {row["game_pk"]: row for row in rows}


def normalize_pa_probabilities(
    probabilities: Optional[Dict[str, float]],
) -> Optional[Dict[str, float]]:
    if not isinstance(probabilities, dict):
        return None

    mapping = {
        "strikeout": "k",
        "walk": "bb",
        "hit_by_pitch": "hbp",
        "home_run": "hr",
        "reached_on_error": "reached_on_error",
        "single": "single",
        "double": "double",
        "triple": "triple",
        "out": "out",
        "k": "k",
        "bb": "bb",
        "hbp": "hbp",
        "hr": "hr",
    }

    normalized: Dict[str, float] = {}

    for key, value in probabilities.items():
        mapped = mapping.get(str(key))
        if not mapped:
            continue
        try:
            normalized[mapped] = normalized.get(mapped, 0.0) + float(value or 0.0)
        except Exception:
            continue

    required = {"k", "bb", "hbp", "single", "double", "triple", "hr", "out"}

    if not required.issubset(set(normalized.keys())):
        return None

    total = sum(max(v, 0.0) for v in normalized.values())

    if total <= 0:
        return None

    return {
        key: max(value, 0.0) / total
        for key, value in normalized.items()
    }


def extract_baseline_scalars(sim_output: Dict[str, Any]) -> tuple[Dict[str, Any], int]:
    values = {}
    missing = 0

    for metric, path in SCALAR_PATHS.items():
        value = get_nested(sim_output, path)
        if value is None:
            missing += 1
        values[metric] = value

    return values, missing


def extract_probability_sets(
    sim_output: Dict[str, Any],
) -> tuple[
    Optional[Dict[str, float]],
    Optional[Dict[str, float]],
    Optional[Dict[str, float]],
    Optional[Dict[str, float]],
    int,
]:
    missing = 0

    away_starter = normalize_pa_probabilities(
        get_nested(sim_output, PA_PATHS["away_starter"])
    )
    home_starter = normalize_pa_probabilities(
        get_nested(sim_output, PA_PATHS["home_starter"])
    )
    away_bullpen = normalize_pa_probabilities(
        get_nested(sim_output, PA_PATHS["away_bullpen"])
    )
    home_bullpen = normalize_pa_probabilities(
        get_nested(sim_output, PA_PATHS["home_bullpen"])
    )

    for item in [away_starter, home_starter, away_bullpen, home_bullpen]:
        if item is None:
            missing += 1

    return away_starter, home_starter, away_bullpen, home_bullpen, missing


def simulate_config_game(
    away_starter_probs: Dict[str, float],
    home_starter_probs: Dict[str, float],
    away_bullpen_probs: Dict[str, float],
    home_bullpen_probs: Dict[str, float],
    ghost_runner: bool,
    rng: random.Random,
) -> Dict[str, Any]:
    away_runs = 0
    home_runs = 0
    walkoff = False

    for inning in range(1, 10):
        away_probs = away_starter_probs if inning <= 5 else away_bullpen_probs
        home_probs = home_starter_probs if inning <= 5 else home_bullpen_probs

        away_half = simulate_half_inning(
            away_probs,
            rng=rng,
        )
        away_runs += int(away_half["runs"])

        if inning == 9 and home_runs > away_runs:
            break

        home_half = simulate_half_inning(
            home_probs,
            rng=rng,
        )
        home_runs += int(home_half["runs"])

        if inning == 9 and home_runs > away_runs:
            walkoff = True
            break

    extras_played = False
    cap_tie = False

    if away_runs == home_runs:
        extras_played = True

        for _ in range(MAX_EXTRA_INNINGS):
            extra_bases = (
                (False, True, False)
                if ghost_runner
                else (False, False, False)
            )

            away_half = simulate_half_inning(
                away_bullpen_probs,
                rng=rng,
                initial_bases=extra_bases,
                initial_outs=0,
            )
            away_runs += int(away_half["runs"])

            if home_runs > away_runs:
                break

            home_half = simulate_half_inning(
                home_bullpen_probs,
                rng=rng,
                initial_bases=extra_bases,
                initial_outs=0,
            )
            home_runs += int(home_half["runs"])

            if away_runs != home_runs:
                if home_runs > away_runs:
                    walkoff = True
                break

        if away_runs == home_runs:
            cap_tie = True
            if rng.random() < 0.5:
                home_runs += 1
            else:
                away_runs += 1

    return {
        "home_runs": home_runs,
        "away_runs": away_runs,
        "total_runs": home_runs + away_runs,
        "home_win": int(home_runs > away_runs),
        "extras_played": extras_played,
        "walkoff": walkoff,
        "cap_tie": cap_tie,
    }


def simulate_many(
    away_starter_probs: Dict[str, float],
    home_starter_probs: Dict[str, float],
    away_bullpen_probs: Dict[str, float],
    home_bullpen_probs: Dict[str, float],
    ghost_runner: bool,
    seed: int,
) -> Dict[str, Any]:
    home_runs = []
    away_runs = []
    total_runs = []
    home_wins = []
    extras = []
    walkoffs = []
    cap_ties = []

    for idx in range(SIMS_PER_GAME):
        rng = random.Random(seed + idx)
        result = simulate_config_game(
            away_starter_probs,
            home_starter_probs,
            away_bullpen_probs,
            home_bullpen_probs,
            ghost_runner,
            rng,
        )

        home_runs.append(result["home_runs"])
        away_runs.append(result["away_runs"])
        total_runs.append(result["total_runs"])
        home_wins.append(result["home_win"])
        extras.append(result["extras_played"])
        walkoffs.append(result["walkoff"])
        cap_ties.append(result["cap_tie"])

    return {
        "home_expected_runs": mean(home_runs),
        "away_expected_runs": mean(away_runs),
        "total_expected_runs": mean(total_runs),
        "home_win_probability": mean(home_wins),
        "extra_rate": mean(extras),
        "walkoff_rate": mean(walkoffs),
        "cap_tie_probability": mean(cap_ties),
        "expected_total_std": pstdev(total_runs),
    }


def pstdev(values: List[float]) -> float:
    if not values:
        return 0.0

    mu = mean(values)
    return math.sqrt(mean((value - mu) ** 2 for value in values))


def log_loss_value(probability: float, actual: int) -> float:
    p = max(min(float(probability), 0.999999), 0.000001)
    return -(actual * math.log(p) + (1 - actual) * math.log(1 - p))


def metric_row(config_name: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"config": config_name, "games": 0}

    total_errors = [
        row[f"{config_name}_total_expected_runs"] - row["actual_total_runs"]
        for row in rows
    ]
    home_errors = [
        row[f"{config_name}_home_expected_runs"] - row["actual_home_runs"]
        for row in rows
    ]
    away_errors = [
        row[f"{config_name}_away_expected_runs"] - row["actual_away_runs"]
        for row in rows
    ]

    briers = []
    logs = []
    winners = []

    for row in rows:
        p = float(row[f"{config_name}_home_win_probability"])
        actual = int(row["actual_home_win"])
        briers.append((p - actual) ** 2)
        logs.append(log_loss_value(p, actual))
        winners.append(1 if (p >= 0.5) == bool(actual) else 0)

    combined_score = (
        mean(abs(v) for v in total_errors)
        + mean(briers)
        + mean(logs)
    )

    return {
        "config": config_name,
        "games": len(rows),
        "total_run_mae": safe_round(mean(abs(v) for v in total_errors)),
        "total_run_rmse": safe_round(math.sqrt(mean(v * v for v in total_errors))),
        "home_run_mae": safe_round(mean(abs(v) for v in home_errors)),
        "away_run_mae": safe_round(mean(abs(v) for v in away_errors)),
        "total_bias": safe_round(mean(total_errors)),
        "home_bias": safe_round(mean(home_errors)),
        "away_bias": safe_round(mean(away_errors)),
        "brier_score": safe_round(mean(briers)),
        "log_loss": safe_round(mean(logs)),
        "winner_accuracy": safe_round(mean(winners)),
        "mean_tie_or_extra_rate": safe_round(
            mean(row[f"{config_name}_extra_rate"] for row in rows)
        ),
        "mean_walkoff_rate": safe_round(
            mean(row[f"{config_name}_walkoff_rate"] for row in rows)
        ),
        "mean_cap_tie_probability": safe_round(
            mean(row[f"{config_name}_cap_tie_probability"] for row in rows)
        ),
        "expected_total_std": safe_round(
            mean(row[f"{config_name}_expected_total_std"] for row in rows)
        ),
        "combined_score": safe_round(combined_score),
    }


def comparison_row(
    name: str,
    left: str,
    right: str,
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if not rows:
        return {"comparison": name, "games": 0}

    return {
        "comparison": name,
        "games": len(rows),
        "total_abs_error_delta": safe_round(mean(
            abs(row[f"{left}_total_expected_runs"] - row["actual_total_runs"])
            - abs(row[f"{right}_total_expected_runs"] - row["actual_total_runs"])
            for row in rows
        )),
        "home_abs_error_delta": safe_round(mean(
            abs(row[f"{left}_home_expected_runs"] - row["actual_home_runs"])
            - abs(row[f"{right}_home_expected_runs"] - row["actual_home_runs"])
            for row in rows
        )),
        "away_abs_error_delta": safe_round(mean(
            abs(row[f"{left}_away_expected_runs"] - row["actual_away_runs"])
            - abs(row[f"{right}_away_expected_runs"] - row["actual_away_runs"])
            for row in rows
        )),
        "brier_delta": safe_round(mean(
            (row[f"{left}_home_win_probability"] - row["actual_home_win"]) ** 2
            - (row[f"{right}_home_win_probability"] - row["actual_home_win"]) ** 2
            for row in rows
        )),
        "log_loss_delta": safe_round(mean(
            log_loss_value(row[f"{left}_home_win_probability"], row["actual_home_win"])
            - log_loss_value(row[f"{right}_home_win_probability"], row["actual_home_win"])
            for row in rows
        )),
        "winner_correct_delta": safe_round(mean(
            (
                1 if (row[f"{left}_home_win_probability"] >= 0.5) == bool(row["actual_home_win"]) else 0
            )
            -
            (
                1 if (row[f"{right}_home_win_probability"] >= 0.5) == bool(row["actual_home_win"]) else 0
            )
            for row in rows
        )),
        "total_expected_runs_delta": safe_round(mean(
            row[f"{left}_total_expected_runs"] - row[f"{right}_total_expected_runs"]
            for row in rows
        )),
        "home_win_probability_delta": safe_round(mean(
            row[f"{left}_home_win_probability"] - row[f"{right}_home_win_probability"]
            for row in rows
        )),
        "expected_total_std_delta": safe_round(mean(
            row[f"{left}_expected_total_std"] - row[f"{right}_expected_total_std"]
            for row in rows
        )),
    }


def bootstrap_rows_for(
    comparison_name: str,
    left: str,
    right: str,
    rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    metrics = {
        "total_abs_error_delta": [
            abs(row[f"{left}_total_expected_runs"] - row["actual_total_runs"])
            - abs(row[f"{right}_total_expected_runs"] - row["actual_total_runs"])
            for row in rows
        ],
        "brier_delta": [
            (row[f"{left}_home_win_probability"] - row["actual_home_win"]) ** 2
            - (row[f"{right}_home_win_probability"] - row["actual_home_win"]) ** 2
            for row in rows
        ],
        "log_loss_delta": [
            log_loss_value(row[f"{left}_home_win_probability"], row["actual_home_win"])
            - log_loss_value(row[f"{right}_home_win_probability"], row["actual_home_win"])
            for row in rows
        ],
        "winner_correct_delta": [
            (
                1 if (row[f"{left}_home_win_probability"] >= 0.5) == bool(row["actual_home_win"]) else 0
            )
            -
            (
                1 if (row[f"{right}_home_win_probability"] >= 0.5) == bool(row["actual_home_win"]) else 0
            )
            for row in rows
        ],
        "total_expected_runs_delta": [
            row[f"{left}_total_expected_runs"] - row[f"{right}_total_expected_runs"]
            for row in rows
        ],
    }

    rng = random.Random(SEED)
    out = []

    for metric, values in metrics.items():
        if not values:
            out.append(
                {
                    "comparison": comparison_name,
                    "metric": metric,
                    "mean_delta": None,
                    "ci_lower_2_5": None,
                    "ci_upper_97_5": None,
                    "p_improvement": None,
                }
            )
            continue

        draws = []

        for _ in range(BOOTSTRAP_SAMPLES):
            sample = [rng.choice(values) for _ in range(len(values))]
            draws.append(mean(sample))

        draws = sorted(draws)

        negative_improvement = metric != "winner_correct_delta"

        if negative_improvement:
            p_improvement = sum(draw < 0 for draw in draws) / len(draws)
        else:
            p_improvement = sum(draw > 0 for draw in draws) / len(draws)

        out.append(
            {
                "comparison": comparison_name,
                "metric": metric,
                "mean_delta": safe_round(mean(values)),
                "ci_lower_2_5": safe_round(draws[int(0.025 * len(draws))]),
                "ci_upper_97_5": safe_round(draws[int(0.975 * len(draws))]),
                "p_improvement": safe_round(p_improvement),
            }
        )

    return out


def regime_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets = {
        "low_scoring": lambda row: row["actual_total_runs"] <= 7,
        "medium_scoring": lambda row: 8 <= row["actual_total_runs"] <= 10,
        "high_scoring": lambda row: row["actual_total_runs"] >= 11,
        "close_games": lambda row: abs(row["actual_home_runs"] - row["actual_away_runs"]) <= 1,
        "blowout_games": lambda row: abs(row["actual_home_runs"] - row["actual_away_runs"]) >= 5,
    }

    out = []

    for name, predicate in buckets.items():
        subset = [row for row in rows if predicate(row)]
        no_ghost = metric_row("no_ghost", subset)
        ghost = metric_row("ghost", subset)

        out.append(
            {
                "regime": name,
                "games": len(subset),
                "no_ghost_total_run_mae": no_ghost.get("total_run_mae"),
                "ghost_total_run_mae": ghost.get("total_run_mae"),
                "ghost_minus_no_ghost_total_run_mae": (
                    None
                    if not subset
                    else safe_round(
                        ghost["total_run_mae"] - no_ghost["total_run_mae"]
                    )
                ),
                "no_ghost_total_bias": no_ghost.get("total_bias"),
                "ghost_total_bias": ghost.get("total_bias"),
                "no_ghost_cap_tie_probability": no_ghost.get("mean_cap_tie_probability"),
                "ghost_cap_tie_probability": ghost.get("mean_cap_tie_probability"),
            }
        )

    return out


def main() -> None:
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)

    actual_lookup = fetch_actual_results(engine)

    rows = []
    skipped = []

    actual_missing_count = 0
    parser_missing_count = 0
    attempted_games = 0

    with Session() as session:
        for game_date in daterange(BACKTEST_START, BACKTEST_END):
            matchups = generate_matchups_for_date(session, game_date)

            for matchup in matchups:
                if MAX_GAMES is not None and attempted_games >= MAX_GAMES:
                    break

                game_pk_raw = matchup.get("game_pk") or matchup.get("gamePk")

                if game_pk_raw is None:
                    skipped.append(
                        {
                            "game_date": game_date,
                            "reason": "missing_game_pk",
                        }
                    )
                    continue

                attempted_games += 1
                game_pk = int(game_pk_raw)

                actual = actual_lookup.get(game_pk)

                if actual is None:
                    actual_missing_count += 1
                    skipped.append(
                        {
                            "game_pk": game_pk,
                            "game_date": game_date,
                            "reason": "missing_actual_result",
                        }
                    )
                    continue

                sim_output = run_full_game_simulation(
                    game_pk,
                    config={
                        "simulation_count": SIMS_PER_GAME,
                        "seed": SEED + len(rows),
                        "date": game_date,
                        "matchup": {
                            "raw": matchup,
                            "game_date": game_date,
                        },
                    },
                )

                baseline, scalar_missing = extract_baseline_scalars(sim_output)

                (
                    away_starter,
                    home_starter,
                    away_bullpen,
                    home_bullpen,
                    pa_missing,
                ) = extract_probability_sets(sim_output)

                if scalar_missing or pa_missing or not all(
                    [away_starter, home_starter, away_bullpen, home_bullpen]
                ):
                    parser_missing_count += scalar_missing + pa_missing
                    skipped.append(
                        {
                            "game_pk": game_pk,
                            "game_date": game_date,
                            "reason": "parser_missing",
                            "scalar_missing": scalar_missing,
                            "pa_missing": pa_missing,
                        }
                    )
                    continue

                no_ghost = simulate_many(
                    away_starter,
                    home_starter,
                    away_bullpen,
                    home_bullpen,
                    ghost_runner=False,
                    seed=SEED + len(rows),
                )

                ghost = simulate_many(
                    away_starter,
                    home_starter,
                    away_bullpen,
                    home_bullpen,
                    ghost_runner=True,
                    seed=SEED + len(rows),
                )

                row = {
                    "game_pk": game_pk,
                    "game_date": game_date,
                    "actual_home_runs": int(actual["home_score"]),
                    "actual_away_runs": int(actual["away_score"]),
                    "actual_total_runs": int(actual["actual_total_runs"]),
                    "actual_home_win": int(actual["actual_home_win"]),
                }

                row.update(
                    {
                        "baseline_home_expected_runs": float(baseline["home_expected_runs"]),
                        "baseline_away_expected_runs": float(baseline["away_expected_runs"]),
                        "baseline_total_expected_runs": float(baseline["total_expected_runs"]),
                        "baseline_home_win_probability": float(baseline["home_win_probability"]),
                        "baseline_extra_rate": float(baseline["tie_after_regulation_probability"] or 0.0),
                        "baseline_walkoff_rate": 0.0,
                        "baseline_cap_tie_probability": 0.0,
                        "baseline_expected_total_std": 0.0,
                    }
                )

                for prefix, values in [
                    ("no_ghost", no_ghost),
                    ("ghost", ghost),
                ]:
                    row.update(
                        {
                            f"{prefix}_home_expected_runs": float(values["home_expected_runs"]),
                            f"{prefix}_away_expected_runs": float(values["away_expected_runs"]),
                            f"{prefix}_total_expected_runs": float(values["total_expected_runs"]),
                            f"{prefix}_home_win_probability": float(values["home_win_probability"]),
                            f"{prefix}_extra_rate": float(values["extra_rate"]),
                            f"{prefix}_walkoff_rate": float(values["walkoff_rate"]),
                            f"{prefix}_cap_tie_probability": float(values["cap_tie_probability"]),
                            f"{prefix}_expected_total_std": float(values["expected_total_std"]),
                        }
                    )

                rows.append(row)

            if MAX_GAMES is not None and attempted_games >= MAX_GAMES:
                break

    config_metrics = [
        metric_row("baseline", rows),
        metric_row("no_ghost", rows),
        metric_row("ghost", rows),
    ]

    comparisons = [
        comparison_row("no_ghost_minus_baseline", "no_ghost", "baseline", rows),
        comparison_row("ghost_minus_baseline", "ghost", "baseline", rows),
        comparison_row("ghost_minus_no_ghost", "ghost", "no_ghost", rows),
    ]

    bootstrap = []
    bootstrap.extend(
        bootstrap_rows_for("ghost_minus_no_ghost", "ghost", "no_ghost", rows)
    )
    bootstrap.extend(
        bootstrap_rows_for("ghost_minus_baseline", "ghost", "baseline", rows)
    )

    regimes = regime_rows(rows)

    metrics = {row["config"]: row for row in config_metrics}

    if not rows:
        diagnosis = (
            "true_ghost_runner_candidate_actual_results_blocked"
            if actual_missing_count
            else "true_ghost_runner_candidate_parser_blocked"
        )
    elif metrics["ghost"]["total_run_mae"] > metrics["no_ghost"]["total_run_mae"]:
        diagnosis = "true_ghost_runner_candidate_improves_extras_but_worsens_calibration"
    elif metrics["ghost"]["total_bias"] > metrics["no_ghost"]["total_bias"] + 1.0:
        diagnosis = "true_ghost_runner_candidate_improves_wp_but_inflates_totals"
    elif metrics["ghost"]["mean_cap_tie_probability"] <= metrics["no_ghost"]["mean_cap_tie_probability"]:
        diagnosis = "true_ghost_runner_candidate_improves_resolution_and_preserves_distribution"
    else:
        diagnosis = "true_ghost_runner_candidate_below_noise_floor"

    summary = {
        "diagnosis": diagnosis,
        "games": len(rows),
        "attempted_games": attempted_games,
        "parser_missing_count": parser_missing_count,
        "actual_missing_count": actual_missing_count,
        "config_metrics": config_metrics,
        "comparisons": comparisons,
        "recommended_next_step": (
            "layer_6h_stateful_extras_calibration_refinement"
            if diagnosis
            == "true_ghost_runner_candidate_improves_resolution_and_preserves_distribution"
            else "layer_6h_base_out_transition_realism_audit"
        ),
    }

    output = {
        "summary": summary,
        "config_metrics": config_metrics,
        "comparisons": comparisons,
        "bootstrap": bootstrap,
        "regimes": regimes,
        "games": rows,
        "skipped": skipped,
    }

    with (OUTPUT_DIR / "true_ghost_runner_extras.json").open("w") as handle:
        json.dump(output, handle, indent=2)

    write_csv(OUTPUT_DIR / "true_ghost_runner_extras_configs.csv", config_metrics)
    write_csv(OUTPUT_DIR / "true_ghost_runner_extras_games.csv", rows)
    write_csv(OUTPUT_DIR / "true_ghost_runner_extras_comparisons.csv", comparisons)
    write_csv(OUTPUT_DIR / "true_ghost_runner_extras_bootstrap.csv", bootstrap)
    write_csv(OUTPUT_DIR / "true_ghost_runner_extras_regimes.csv", regimes)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
