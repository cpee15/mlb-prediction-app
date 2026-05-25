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
from mlb_app.simulation.game_rules import simulate_game_with_extras_and_walkoff


BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
BACKTEST_END = os.getenv("BACKTEST_END", "2026-05-03")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mlb.db")

SIMS_PER_GAME = int(os.getenv("SIMS_PER_GAME", "250"))
BOOTSTRAP_SAMPLES = int(os.getenv("BOOTSTRAP_SAMPLES", "1000"))
MAX_GAMES_RAW = os.getenv("MAX_GAMES")
MAX_GAMES = int(MAX_GAMES_RAW) if MAX_GAMES_RAW else None

SEED = int(os.getenv("SEED", "42"))
MAX_EXTRA_INNINGS = int(os.getenv("MAX_EXTRA_INNINGS", "6"))
GHOST_RUNNER_ENABLED = (
    os.getenv("GHOST_RUNNER_ENABLED", "true").lower().strip() == "true"
)

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


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return

    fields = sorted({k for row in rows for k in row.keys()})

    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def safe_round(value: Any, digits: int = 6) -> Any:
    if value is None:
        return None

    try:
        return round(float(value), digits)
    except Exception:
        return value


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
                home_score = int(row.home_score)
                away_score = int(row.away_score)
                game_pk = int(row.game_pk)
            except Exception:
                continue

            rows.append(
                {
                    "game_pk": game_pk,
                    "game_date": str(row.game_date),
                    "home_score": home_score,
                    "away_score": away_score,
                    "total_score": home_score + away_score,
                    "home_win_actual":
                        1 if home_score > away_score else 0,
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
            normalized[mapped] = (
                normalized.get(mapped, 0.0)
                + float(value or 0.0)
            )
        except Exception:
            continue

    required = {
        "k",
        "bb",
        "hbp",
        "single",
        "double",
        "triple",
        "hr",
        "out",
    }

    if not required.issubset(set(normalized.keys())):
        return None

    total = sum(max(v, 0.0) for v in normalized.values())

    if total <= 0:
        return None

    return {
        key: max(value, 0.0) / total
        for key, value in normalized.items()
    }


def extract_baseline_scalars(
    sim_output: Dict[str, Any],
) -> tuple[Dict[str, Any], int]:

    values = {}
    missing = 0

    for metric, path in SCALAR_PATHS.items():
        value = get_nested(sim_output, path)

        if value is None:
            missing += 1

        values[metric] = value

    return values, missing


def extract_candidate_probabilities(
    sim_output: Dict[str, Any],
) -> tuple[
    Optional[Dict[str, float]],
    Optional[Dict[str, float]],
    str,
    int,
]:

    missing = 0

    away_bullpen = normalize_pa_probabilities(
        get_nested(sim_output, PA_PATHS["away_bullpen"])
    )
    home_bullpen = normalize_pa_probabilities(
        get_nested(sim_output, PA_PATHS["home_bullpen"])
    )

    if away_bullpen and home_bullpen:
        return (
            away_bullpen,
            home_bullpen,
            "bullpen_late_inning_distribution",
            missing,
        )

    missing += 2

    away_starter = normalize_pa_probabilities(
        get_nested(sim_output, PA_PATHS["away_starter"])
    )
    home_starter = normalize_pa_probabilities(
        get_nested(sim_output, PA_PATHS["home_starter"])
    )

    if away_starter and home_starter:
        return (
            away_starter,
            home_starter,
            "starter_distribution_fallback",
            missing,
        )

    missing += 2

    return (
        None,
        None,
        "parser_blocked_no_pa_probabilities",
        missing,
    )


def mae(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return mean(abs(v) for v in values)


def rmse(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return math.sqrt(mean(v * v for v in values))


def brier(preds: List[float], actuals: List[int]) -> Optional[float]:
    if not preds:
        return None

    return mean(
        (p - a) ** 2
        for p, a in zip(preds, actuals)
    )


def log_loss(preds: List[float], actuals: List[int]) -> Optional[float]:
    if not preds:
        return None

    values = []

    for p, a in zip(preds, actuals):
        p = max(min(float(p), 0.999999), 0.000001)

        values.append(
            -(
                a * math.log(p)
                + (1 - a) * math.log(1 - p)
            )
        )

    return mean(values)


def metric_row(
    config_name: str,
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:

    if not rows:
        return {
            "config": config_name,
            "games": 0,
        }

    prefix = (
        "baseline"
        if config_name == "baseline"
        else "candidate"
    )

    total_errors = [
        row[f"{prefix}_total_expected_runs"]
        - row["actual_total_runs"]
        for row in rows
    ]

    home_errors = [
        row[f"{prefix}_home_expected_runs"]
        - row["actual_home_runs"]
        for row in rows
    ]

    away_errors = [
        row[f"{prefix}_away_expected_runs"]
        - row["actual_away_runs"]
        for row in rows
    ]

    preds = [
        row[f"{prefix}_home_win_probability"]
        for row in rows
    ]

    actuals = [
        row["actual_home_win"]
        for row in rows
    ]

    winner_correct = [
        1 if (pred >= 0.5) == bool(actual) else 0
        for pred, actual in zip(preds, actuals)
    ]

    combined_score = (
        (mae(total_errors) or 0.0)
        + (brier(preds, actuals) or 0.0)
        + (log_loss(preds, actuals) or 0.0)
    )

    return {
        "config": config_name,
        "games": len(rows),
        "total_run_mae": safe_round(mae(total_errors)),
        "total_run_rmse": safe_round(rmse(total_errors)),
        "home_run_mae": safe_round(mae(home_errors)),
        "away_run_mae": safe_round(mae(away_errors)),
        "total_bias": safe_round(mean(total_errors)),
        "brier_score": safe_round(brier(preds, actuals)),
        "log_loss": safe_round(log_loss(preds, actuals)),
        "winner_accuracy": safe_round(mean(winner_correct)),
        "mean_tie_or_extra_rate": safe_round(
            mean(
                row["baseline_tie_after_regulation_probability"]
                if config_name == "baseline"
                else row["candidate_extra_innings_rate"]
                for row in rows
            )
        ),
        "mean_walkoff_rate": (
            None
            if config_name == "baseline"
            else safe_round(
                mean(row["candidate_walkoff_rate"] for row in rows)
            )
        ),
        "mean_cap_tie_probability": (
            None
            if config_name == "baseline"
            else safe_round(
                mean(row["candidate_cap_tie_probability"] for row in rows)
            )
        ),
        "parser_missing_count": sum(
            row["parser_missing_count"]
            for row in rows
        ),
        "parser_fallback_count": sum(
            row["parser_fallback_count"]
            for row in rows
        ),
        "combined_score": safe_round(combined_score),
    }


def percentile(
    values: List[float],
    pct: float,
) -> Optional[float]:

    if not values:
        return None

    values = sorted(values)
    idx = int((len(values) - 1) * pct)

    return values[idx]


def bootstrap_metric(
    values: List[float],
    samples: int,
    improvement_direction: str,
) -> Dict[str, Any]:

    if not values:
        return {
            "mean_delta": None,
            "ci_lower_2_5": None,
            "ci_upper_97_5": None,
            "p_improvement": None,
        }

    rng = random.Random(SEED)
    draws = []

    for _ in range(samples):
        sample = [
            rng.choice(values)
            for _ in range(len(values))
        ]
        draws.append(mean(sample))

    if improvement_direction == "negative":
        p_improvement = (
            sum(draw < 0 for draw in draws)
            / len(draws)
        )
    else:
        p_improvement = (
            sum(draw > 0 for draw in draws)
            / len(draws)
        )

    return {
        "mean_delta": safe_round(mean(values)),
        "ci_lower_2_5": safe_round(percentile(draws, 0.025)),
        "ci_upper_97_5": safe_round(percentile(draws, 0.975)),
        "p_improvement": safe_round(p_improvement),
    }


def build_bootstrap(
    rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:

    specs = [
        ("total_abs_error_delta", "negative"),
        ("home_abs_error_delta", "negative"),
        ("away_abs_error_delta", "negative"),
        ("brier_delta", "negative"),
        ("log_loss_delta", "negative"),
        ("winner_correct_delta", "positive"),
        ("total_expected_runs_delta", "negative"),
        ("home_win_probability_delta", "negative"),
    ]

    output = []

    for metric, direction in specs:
        values = [
            row[metric]
            for row in rows
        ]

        result = bootstrap_metric(
            values,
            BOOTSTRAP_SAMPLES,
            direction,
        )

        result["metric"] = metric
        result["improvement_direction"] = direction
        output.append(result)

    return output


def delta_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {}

    return {
        "total_abs_error_delta":
            safe_round(mean(row["total_abs_error_delta"] for row in rows)),
        "brier_delta":
            safe_round(mean(row["brier_delta"] for row in rows)),
        "log_loss_delta":
            safe_round(mean(row["log_loss_delta"] for row in rows)),
        "winner_correct_delta":
            safe_round(mean(row["winner_correct_delta"] for row in rows)),
        "total_expected_runs_delta":
            safe_round(mean(row["total_expected_runs_delta"] for row in rows)),
        "home_win_probability_delta":
            safe_round(mean(row["home_win_probability_delta"] for row in rows)),
    }


def split_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ordered = sorted(
        rows,
        key=lambda row: (
            row["game_date"],
            row["game_pk"],
        ),
    )

    midpoint = len(ordered) // 2

    groups = {
        "early_window": ordered[:midpoint],
        "late_window": ordered[midpoint:],
    }

    output = []

    for name, group in groups.items():
        base = metric_row("baseline", group)
        cand = metric_row("candidate", group)

        output.append(
            {
                "split_name": name,
                "games": len(group),
                "baseline_total_run_mae": base.get("total_run_mae"),
                "candidate_total_run_mae": cand.get("total_run_mae"),
                "total_run_mae_delta": (
                    None
                    if not group
                    else safe_round(
                        cand["total_run_mae"]
                        - base["total_run_mae"]
                    )
                ),
                "baseline_brier": base.get("brier_score"),
                "candidate_brier": cand.get("brier_score"),
                "brier_delta": (
                    None
                    if not group
                    else safe_round(
                        cand["brier_score"]
                        - base["brier_score"]
                    )
                ),
                "baseline_log_loss": base.get("log_loss"),
                "candidate_log_loss": cand.get("log_loss"),
                "log_loss_delta": (
                    None
                    if not group
                    else safe_round(
                        cand["log_loss"]
                        - base["log_loss"]
                    )
                ),
            }
        )

    return output


def regime_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets = {
        "low_scoring":
            lambda row: row["actual_total_runs"] <= 7,
        "medium_scoring":
            lambda row: 8 <= row["actual_total_runs"] <= 10,
        "high_scoring":
            lambda row: row["actual_total_runs"] >= 11,
        "close_games":
            lambda row: abs(
                row["actual_home_runs"]
                - row["actual_away_runs"]
            ) <= 1,
        "non_close_games":
            lambda row: abs(
                row["actual_home_runs"]
                - row["actual_away_runs"]
            ) >= 2,
    }

    output = []

    for name, predicate in buckets.items():
        group = [
            row
            for row in rows
            if predicate(row)
        ]

        base = metric_row("baseline", group)
        cand = metric_row("candidate", group)

        output.append(
            {
                "regime": name,
                "games": len(group),
                "baseline_total_run_mae": base.get("total_run_mae"),
                "candidate_total_run_mae": cand.get("total_run_mae"),
                "total_run_mae_delta": (
                    None
                    if not group
                    else safe_round(
                        cand["total_run_mae"]
                        - base["total_run_mae"]
                    )
                ),
                "baseline_brier": base.get("brier_score"),
                "candidate_brier": cand.get("brier_score"),
                "brier_delta": (
                    None
                    if not group
                    else safe_round(
                        cand["brier_score"]
                        - base["brier_score"]
                    )
                ),
                "baseline_log_loss": base.get("log_loss"),
                "candidate_log_loss": cand.get("log_loss"),
                "log_loss_delta": (
                    None
                    if not group
                    else safe_round(
                        cand["log_loss"]
                        - base["log_loss"]
                    )
                ),
            }
        )

    return output


def classify_diagnosis(
    rows: List[Dict[str, Any]],
    config_metrics: List[Dict[str, Any]],
    bootstrap_rows: List[Dict[str, Any]],
    actual_missing_count: int,
    parser_missing_count: int,
) -> str:

    if not rows:
        if actual_missing_count:
            return "extras_walkoff_candidate_actual_results_blocked"
        return "extras_walkoff_candidate_parser_blocked"

    if parser_missing_count >= len(rows) * 4:
        return "extras_walkoff_candidate_parser_blocked"

    baseline = next(
        row
        for row in config_metrics
        if row["config"] == "baseline"
    )

    candidate = next(
        row
        for row in config_metrics
        if row["config"] == "candidate"
    )

    brier_boot = next(
        row
        for row in bootstrap_rows
        if row["metric"] == "brier_delta"
    )

    log_boot = next(
        row
        for row in bootstrap_rows
        if row["metric"] == "log_loss_delta"
    )

    brier_improved = (
        candidate.get("brier_score") is not None
        and baseline.get("brier_score") is not None
        and candidate["brier_score"] < baseline["brier_score"]
    )

    log_improved = (
        candidate.get("log_loss") is not None
        and baseline.get("log_loss") is not None
        and candidate["log_loss"] < baseline["log_loss"]
    )

    total_preserved = (
        candidate.get("total_run_mae") is not None
        and baseline.get("total_run_mae") is not None
        and candidate["total_run_mae"]
        <= baseline["total_run_mae"] + 0.05
    )

    brier_ci_excludes_zero = (
        brier_boot.get("ci_upper_97_5") is not None
        and brier_boot["ci_upper_97_5"] < 0
    )

    log_ci_excludes_zero = (
        log_boot.get("ci_upper_97_5") is not None
        and log_boot["ci_upper_97_5"] < 0
    )

    if (
        len(rows) >= 25
        and (brier_improved or log_improved)
        and (brier_ci_excludes_zero or log_ci_excludes_zero)
        and total_preserved
        and parser_missing_count == 0
    ):
        return "extras_walkoff_candidate_empirically_supported"

    if (
        brier_improved
        or log_improved
        or candidate["total_run_mae"]
        <= baseline["total_run_mae"]
    ):
        return "extras_walkoff_candidate_promising_but_inconclusive"

    return "extras_walkoff_candidate_below_noise_floor"


def main() -> None:
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)

    actual_lookup = fetch_actual_results(engine)

    rows: List[Dict[str, Any]] = []
    skipped_games: List[Dict[str, Any]] = []

    actual_missing_count = 0
    actual_malformed_count = 0
    parser_missing_total = 0
    parser_fallback_total = 0

    attempted_games = 0

    with Session() as session:
        for game_date in daterange(BACKTEST_START, BACKTEST_END):
            matchups = generate_matchups_for_date(session, game_date)

            for matchup in matchups:
                if MAX_GAMES is not None and attempted_games >= MAX_GAMES:
                    break

                game_pk_raw = matchup.get("game_pk") or matchup.get("gamePk")
                if game_pk_raw is None:
                    skipped_games.append(
                        {
                            "game_date": game_date,
                            "reason": "missing_game_pk",
                        }
                    )
                    continue

                attempted_games += 1
                game_pk = int(game_pk_raw)

                actual = actual_lookup.get(game_pk)

                if not actual:
                    actual_missing_count += 1
                    skipped_games.append(
                        {
                            "game_pk": game_pk,
                            "game_date": game_date,
                            "reason": "missing_actual_result",
                        }
                    )
                    continue

                try:
                    actual_home_runs = int(actual["home_score"])
                    actual_away_runs = int(actual["away_score"])
                except Exception as exc:
                    actual_malformed_count += 1
                    skipped_games.append(
                        {
                            "game_pk": game_pk,
                            "game_date": game_date,
                            "reason": "malformed_actual_result",
                            "error": str(exc),
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

                required_scalar_keys = [
                    "home_expected_runs",
                    "away_expected_runs",
                    "total_expected_runs",
                    "home_win_probability",
                    "away_win_probability",
                    "tie_after_regulation_probability",
                ]

                if any(
                    baseline.get(key) is None
                    for key in required_scalar_keys
                ):
                    parser_missing_total += scalar_missing
                    skipped_games.append(
                        {
                            "game_pk": game_pk,
                            "game_date": game_date,
                            "reason": "missing_baseline_scalars",
                            "scalar_missing": scalar_missing,
                        }
                    )
                    continue

                (
                    away_probs,
                    home_probs,
                    pairing_mode,
                    pa_missing,
                ) = extract_candidate_probabilities(sim_output)

                if away_probs is None or home_probs is None:
                    parser_missing_total += scalar_missing + pa_missing
                    skipped_games.append(
                        {
                            "game_pk": game_pk,
                            "game_date": game_date,
                            "reason": "candidate_probability_parser_blocked",
                            "candidate_pairing_mode": pairing_mode,
                            "pa_missing": pa_missing,
                        }
                    )
                    continue

                if pairing_mode != "bullpen_late_inning_distribution":
                    parser_fallback_total += 1

                candidate = simulate_game_with_extras_and_walkoff(
                    away_probabilities=away_probs,
                    home_probabilities=home_probs,
                    simulations=SIMS_PER_GAME,
                    seed=SEED + len(rows),
                    innings=9,
                    max_extra_innings=MAX_EXTRA_INNINGS,
                    ghost_runner_enabled=GHOST_RUNNER_ENABLED,
                )

                actual_total_runs = actual_home_runs + actual_away_runs
                actual_home_win = 1 if actual_home_runs > actual_away_runs else 0

                baseline_home_exp = float(baseline["home_expected_runs"])
                baseline_away_exp = float(baseline["away_expected_runs"])
                baseline_total_exp = float(baseline["total_expected_runs"])
                baseline_home_wp = float(baseline["home_win_probability"])
                baseline_away_wp = float(baseline["away_win_probability"])
                baseline_tie_prob = float(baseline["tie_after_regulation_probability"])

                candidate_home_exp = float(candidate["home_expected_runs"])
                candidate_away_exp = float(candidate["away_expected_runs"])
                candidate_total_exp = float(candidate["total_expected_runs"])
                candidate_home_wp = float(candidate["home_win_probability"])
                candidate_away_wp = float(candidate["away_win_probability"])

                def _log_loss(prob: float, actual_win: int) -> float:
                    prob = max(min(prob, 0.999999), 0.000001)
                    return -(
                        actual_win * math.log(prob)
                        + (1 - actual_win) * math.log(1 - prob)
                    )

                baseline_total_abs_error = abs(
                    baseline_total_exp - actual_total_runs
                )
                candidate_total_abs_error = abs(
                    candidate_total_exp - actual_total_runs
                )

                baseline_home_abs_error = abs(
                    baseline_home_exp - actual_home_runs
                )
                candidate_home_abs_error = abs(
                    candidate_home_exp - actual_home_runs
                )

                baseline_away_abs_error = abs(
                    baseline_away_exp - actual_away_runs
                )
                candidate_away_abs_error = abs(
                    candidate_away_exp - actual_away_runs
                )

                baseline_brier = (
                    baseline_home_wp - actual_home_win
                ) ** 2
                candidate_brier = (
                    candidate_home_wp - actual_home_win
                ) ** 2

                baseline_log = _log_loss(
                    baseline_home_wp,
                    actual_home_win,
                )
                candidate_log = _log_loss(
                    candidate_home_wp,
                    actual_home_win,
                )

                baseline_winner_correct = int(
                    (baseline_home_wp >= 0.5)
                    == bool(actual_home_win)
                )
                candidate_winner_correct = int(
                    (candidate_home_wp >= 0.5)
                    == bool(actual_home_win)
                )

                row = {
                    "game_pk": game_pk,
                    "game_date": game_date,
                    "away_team": matchup.get("away_team_name") or matchup.get("awayTeamName") or matchup.get("away_team"),
                    "home_team": matchup.get("home_team_name") or matchup.get("homeTeamName") or matchup.get("home_team"),

                    "actual_home_runs": actual_home_runs,
                    "actual_away_runs": actual_away_runs,
                    "actual_total_runs": actual_total_runs,
                    "actual_home_win": actual_home_win,

                    "baseline_home_expected_runs": safe_round(baseline_home_exp),
                    "baseline_away_expected_runs": safe_round(baseline_away_exp),
                    "baseline_total_expected_runs": safe_round(baseline_total_exp),
                    "baseline_home_win_probability": safe_round(baseline_home_wp),
                    "baseline_away_win_probability": safe_round(baseline_away_wp),
                    "baseline_tie_after_regulation_probability": safe_round(baseline_tie_prob),

                    "candidate_home_expected_runs": safe_round(candidate_home_exp),
                    "candidate_away_expected_runs": safe_round(candidate_away_exp),
                    "candidate_total_expected_runs": safe_round(candidate_total_exp),
                    "candidate_home_win_probability": safe_round(candidate_home_wp),
                    "candidate_away_win_probability": safe_round(candidate_away_wp),
                    "candidate_extra_innings_rate": candidate["prototype_extra_innings_rate"],
                    "candidate_walkoff_rate": candidate["prototype_walkoff_rate"],
                    "candidate_cap_tie_probability": candidate["prototype_extra_innings_cap_tie_probability"],
                    "candidate_pairing_mode": pairing_mode,
                    "ghost_runner_model_status": candidate["ghost_runner_model_status"],

                    "baseline_total_abs_error": safe_round(baseline_total_abs_error),
                    "candidate_total_abs_error": safe_round(candidate_total_abs_error),
                    "total_abs_error_delta": safe_round(candidate_total_abs_error - baseline_total_abs_error),

                    "baseline_home_abs_error": safe_round(baseline_home_abs_error),
                    "candidate_home_abs_error": safe_round(candidate_home_abs_error),
                    "home_abs_error_delta": safe_round(candidate_home_abs_error - baseline_home_abs_error),

                    "baseline_away_abs_error": safe_round(baseline_away_abs_error),
                    "candidate_away_abs_error": safe_round(candidate_away_abs_error),
                    "away_abs_error_delta": safe_round(candidate_away_abs_error - baseline_away_abs_error),

                    "baseline_brier": safe_round(baseline_brier),
                    "candidate_brier": safe_round(candidate_brier),
                    "brier_delta": safe_round(candidate_brier - baseline_brier),

                    "baseline_log_loss": safe_round(baseline_log),
                    "candidate_log_loss": safe_round(candidate_log),
                    "log_loss_delta": safe_round(candidate_log - baseline_log),

                    "baseline_winner_correct": baseline_winner_correct,
                    "candidate_winner_correct": candidate_winner_correct,
                    "winner_correct_delta": candidate_winner_correct - baseline_winner_correct,

                    "total_bias_delta": safe_round(
                        (candidate_total_exp - actual_total_runs)
                        - (baseline_total_exp - actual_total_runs)
                    ),
                    "home_win_probability_delta": safe_round(candidate_home_wp - baseline_home_wp),
                    "total_expected_runs_delta": safe_round(candidate_total_exp - baseline_total_exp),

                    "parser_missing_count": scalar_missing + pa_missing,
                    "parser_fallback_count": 1 if pairing_mode != "bullpen_late_inning_distribution" else 0,
                }

                rows.append(row)

            if MAX_GAMES is not None and attempted_games >= MAX_GAMES:
                break

    config_metrics = [
        metric_row("baseline", rows),
        metric_row("candidate", rows),
    ]

    for item in config_metrics:
        item["actual_missing_count"] = actual_missing_count
        item["actual_malformed_count"] = actual_malformed_count

    bootstrap_rows = build_bootstrap(rows)
    splits = split_rows(rows)
    regimes = regime_rows(rows)

    parser_missing_total = sum(row["parser_missing_count"] for row in rows)
    parser_fallback_total = sum(row["parser_fallback_count"] for row in rows)

    diagnosis = classify_diagnosis(
        rows,
        config_metrics,
        bootstrap_rows,
        actual_missing_count,
        parser_missing_total,
    )

    summary = {
        "diagnosis": diagnosis,
        "games": len(rows),
        "attempted_games": attempted_games,
        "config_metrics": config_metrics,
        "delta_summary": delta_summary(rows),
        "mean_candidate_extra_innings_rate": (
            None
            if not rows
            else safe_round(
                mean(row["candidate_extra_innings_rate"] for row in rows)
            )
        ),
        "mean_candidate_walkoff_rate": (
            None
            if not rows
            else safe_round(
                mean(row["candidate_walkoff_rate"] for row in rows)
            )
        ),
        "mean_candidate_cap_tie_probability": (
            None
            if not rows
            else safe_round(
                mean(row["candidate_cap_tie_probability"] for row in rows)
            )
        ),
        "parser_missing_count": parser_missing_total,
        "parser_fallback_count": parser_fallback_total,
        "actual_missing_count": actual_missing_count,
        "actual_malformed_count": actual_malformed_count,
        "candidate_pairing_modes": sorted(
            set(row["candidate_pairing_mode"] for row in rows)
        ),
        "recommended_next_step": (
            "retain_candidate_for_higher_power_calibration"
            if diagnosis in {
                "extras_walkoff_candidate_empirically_supported",
                "extras_walkoff_candidate_promising_but_inconclusive",
            }
            else "do_not_promote_candidate"
        ),
    }

    diagnostics = {
        "backtest_start": BACKTEST_START,
        "backtest_end": BACKTEST_END,
        "database_url": DATABASE_URL,
        "actual_results_loaded": len(actual_lookup),
        "attempted_games": attempted_games,
        "evaluated_games": len(rows),
        "skipped_games": len(skipped_games),
        "actual_missing_count": actual_missing_count,
        "actual_malformed_count": actual_malformed_count,
        "parser_missing_total": parser_missing_total,
        "parser_fallback_total": parser_fallback_total,
        "confirmed_scalar_paths": SCALAR_PATHS,
        "confirmed_pa_paths": PA_PATHS,
    }

    output = {
        "summary": summary,
        "config_metrics": config_metrics,
        "bootstrap": bootstrap_rows,
        "splits": splits,
        "regimes": regimes,
        "games": rows,
        "skipped_games": skipped_games,
        "diagnostics": diagnostics,
    }

    with (OUTPUT_DIR / "extras_walkoff_calibration.json").open("w") as handle:
        json.dump(output, handle, indent=2)

    write_csv(OUTPUT_DIR / "extras_walkoff_calibration_configs.csv", config_metrics)
    write_csv(OUTPUT_DIR / "extras_walkoff_calibration_games.csv", rows)
    write_csv(OUTPUT_DIR / "extras_walkoff_calibration_bootstrap.csv", bootstrap_rows)
    write_csv(OUTPUT_DIR / "extras_walkoff_calibration_splits.csv", splits)
    write_csv(OUTPUT_DIR / "extras_walkoff_calibration_regimes.csv", regimes)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
