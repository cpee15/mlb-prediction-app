from __future__ import annotations

import copy
import csv
import json
import math
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.matchup_generator import generate_matchups_for_date
from mlb_app.simulation.game_engine_v2 import run_full_game_simulation


BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
BACKTEST_END = os.getenv("BACKTEST_END", "2026-05-03")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
SIMS_PER_GAME = int(os.getenv("SIMS_PER_GAME", "50"))
SEED = int(os.getenv("SEED", "42"))

MAX_GAMES_RAW = os.getenv("MAX_GAMES", "")
MAX_GAMES = int(MAX_GAMES_RAW) if MAX_GAMES_RAW.strip() else None


CONFIGS = {
    "baseline_current_production": {},
    "hitter_expected_contact__small": {
        "hitter_contact_boost": 0.015,
    },
    "hitter_recent__medium": {
        "hitter_recent_boost": 0.03,
    },
    "pitcher_stuff__small": {
        "pitcher_stuff_penalty": 0.015,
    },
    "combined_recent_and_stuff__small": {
        "hitter_recent_boost": 0.02,
        "pitcher_stuff_penalty": 0.015,
    },
    "combined_expected_contact_and_stuff__small": {
        "hitter_contact_boost": 0.015,
        "pitcher_stuff_penalty": 0.015,
    },
}


PRIMARY_RUN_PATHS = {
    "home": "derived_outputs.bullpen_adjusted_game_simulation.home_expected_runs",
    "away": "derived_outputs.bullpen_adjusted_game_simulation.away_expected_runs",
    "total": "derived_outputs.bullpen_adjusted_game_simulation.total_expected_runs",
}

FALLBACK_RUN_PATHS = {
    "home": "derived_outputs.game_simulation.home_expected_runs",
    "away": "derived_outputs.game_simulation.away_expected_runs",
    "total": "derived_outputs.game_simulation.total_expected_runs",
}

# Conservative only. Do not use over/under or total-probability paths as winner probability.
WIN_PROB_PATHS = [
    "derived_outputs.game_simulation.home_win_probability",
    "derived_outputs.game_simulation.home_win_prob",
    "derived_outputs.bullpen_adjusted_game_simulation.home_win_probability",
    "derived_outputs.bullpen_adjusted_game_simulation.home_win_prob",
]


def daterange(start: str, end: str):
    import datetime as dt

    current = dt.date.fromisoformat(start)
    stop = dt.date.fromisoformat(end)

    while current <= stop:
        yield current.isoformat()
        current += dt.timedelta(days=1)


def flatten(obj: Any, prefix: str = "") -> Dict[str, Any]:
    rows: Dict[str, Any] = {}

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

    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def safe_mean(values: List[Optional[float]]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def safe_rmse(errors: List[Optional[float]]) -> Optional[float]:
    vals = [e for e in errors if e is not None]
    if not vals:
        return None

    return math.sqrt(
        sum(v * v for v in vals) / len(vals)
    )


def safe_mse(errors: List[Optional[float]]) -> Optional[float]:
    vals = [e for e in errors if e is not None]

    if not vals:
        return None

    return sum(v * v for v in vals) / len(vals)


def clamp_prob(prob: float) -> float:
    return max(0.001, min(0.999, prob))


def brier(prob: float, actual: int) -> float:
    return (prob - actual) ** 2


def log_loss(prob: float, actual: int) -> float:
    prob = clamp_prob(prob)
    if actual == 1:
        return -math.log(prob)
    return -math.log(1 - prob)


def load_actual_results() -> Dict[int, Dict[str, Any]]:
    sqlite_path = DATABASE_URL.replace("sqlite:///", "")
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT *
        FROM actual_game_results
        """
    ).fetchall()

    conn.close()

    return {int(row["game_pk"]): dict(row) for row in rows}


def apply_pct_modifier(payload: Dict[str, Any], key: str, modifier: float) -> None:
    if key not in payload:
        return

    value = payload[key]
    if not isinstance(value, (int, float)):
        return

    payload[key] = value * (1 + modifier)


def apply_config(raw_matchup: Dict[str, Any], config: Dict[str, float]) -> Dict[str, Any]:
    matchup = copy.deepcopy(raw_matchup)

    for side in ["away_offense_inputs", "home_offense_inputs"]:
        offense = matchup.get(side)

        if not isinstance(offense, dict):
            continue

        if "hitter_contact_boost" in config:
            for key in ["xba", "xwoba", "hard_hit_pct", "barrel_pct"]:
                apply_pct_modifier(offense, key, config["hitter_contact_boost"])

        if "hitter_recent_boost" in config:
            for key in ["batting_avg", "iso", "hard_hit_pct"]:
                apply_pct_modifier(offense, key, config["hitter_recent_boost"])

    for side in ["away_pitcher_features", "home_pitcher_features"]:
        pitcher = matchup.get(side)

        if not isinstance(pitcher, dict):
            continue

        if "pitcher_stuff_penalty" in config:
            for key in ["xba", "xwoba", "hard_hit_pct"]:
                apply_pct_modifier(pitcher, key, config["pitcher_stuff_penalty"])

    return matchup


def get_float_path(flat: Dict[str, Any], path: str, min_value: float, max_value: float) -> Tuple[Optional[str], Optional[float]]:
    value = flat.get(path)

    if not isinstance(value, (int, float)):
        return None, None

    value = float(value)

    if value < min_value or value > max_value:
        return None, None

    return path, value


def parse_expected_runs(flat: Dict[str, Any]) -> Dict[str, Any]:
    home_path, home = get_float_path(flat, PRIMARY_RUN_PATHS["home"], 0.0, 12.0)
    away_path, away = get_float_path(flat, PRIMARY_RUN_PATHS["away"], 0.0, 12.0)
    total_path, total = get_float_path(flat, PRIMARY_RUN_PATHS["total"], 0.0, 24.0)

    fallback_used = False
    fallback_reasons = []

    if home is None:
        fallback_used = True
        fallback_reasons.append("primary_home_missing")
        home_path, home = get_float_path(flat, FALLBACK_RUN_PATHS["home"], 0.0, 12.0)

    if away is None:
        fallback_used = True
        fallback_reasons.append("primary_away_missing")
        away_path, away = get_float_path(flat, FALLBACK_RUN_PATHS["away"], 0.0, 12.0)

    if total is None:
        fallback_used = True
        fallback_reasons.append("primary_total_missing")
        total_path, total = get_float_path(flat, FALLBACK_RUN_PATHS["total"], 0.0, 24.0)

    derived_total_used = False
    if total is None and home is not None and away is not None:
        total = home + away
        total_path = "__derived_home_plus_away__"
        derived_total_used = True
        fallback_used = True
        fallback_reasons.append("total_derived_from_home_away")

    valid = home is not None and away is not None and total is not None

    return {
        "home": home,
        "away": away,
        "total": total,
        "home_path": home_path,
        "away_path": away_path,
        "total_path": total_path,
        "fallback_used": fallback_used,
        "fallback_reasons": ",".join(fallback_reasons),
        "derived_total_used": derived_total_used,
        "valid": valid,
    }


def parse_home_win_probability(flat: Dict[str, Any]) -> Dict[str, Any]:
    for path in WIN_PROB_PATHS:
        parsed_path, value = get_float_path(flat, path, 0.0, 1.0)
        if value is not None:
            return {
                "value": value,
                "path": parsed_path,
                "available": True,
            }

    return {
        "value": None,
        "path": None,
        "available": False,
    }


def extract_prediction(sim_output: Dict[str, Any]) -> Dict[str, Any]:
    flat = flatten(sim_output)

    runs = parse_expected_runs(flat)
    win = parse_home_win_probability(flat)

    parser_quality_status = "valid_paths_no_fallbacks"
    if not runs["valid"]:
        parser_quality_status = "expected_run_parser_paths_missing"
    elif runs["fallback_used"]:
        parser_quality_status = "run_paths_valid_with_fallbacks"

    if not win["available"]:
        win_status = "win_parser_missing"
    else:
        win_status = "win_parser_valid"

    return {
        "home_win_prob": win["value"],
        "home_win_prob_path": win["path"],
        "win_probability_available": win["available"],
        "pred_home_runs": runs["home"],
        "pred_away_runs": runs["away"],
        "pred_total_runs": runs["total"],
        "pred_run_diff": (
            runs["home"] - runs["away"]
            if runs["home"] is not None and runs["away"] is not None
            else None
        ),
        "home_runs_path": runs["home_path"],
        "away_runs_path": runs["away_path"],
        "total_runs_path": runs["total_path"],
        "run_parser_fallback_used": runs["fallback_used"],
        "run_parser_fallback_reasons": runs["fallback_reasons"],
        "derived_total_used": runs["derived_total_used"],
        "run_parser_valid": runs["valid"],
        "parser_quality_status": parser_quality_status,
        "win_parser_status": win_status,
    }


def main() -> None:
    actual_results = load_actual_results()

    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(bind=engine)

    config_rows: List[Dict[str, Any]] = []
    game_rows: List[Dict[str, Any]] = []
    parser_rows: List[Dict[str, Any]] = []

    run_parser_fallback_count = 0
    run_parser_missing_count = 0
    win_parser_missing_count = 0

    with SessionLocal() as session:
        all_matchups = []

        for game_date in daterange(BACKTEST_START, BACKTEST_END):
            matchups = generate_matchups_for_date(session, game_date)

            for matchup in matchups:
                game_pk = matchup.get("game_pk")

                if game_pk not in actual_results:
                    continue

                all_matchups.append((game_date, matchup))

        if MAX_GAMES:
            all_matchups = all_matchups[:MAX_GAMES]

        print(f"[info] loaded {len(all_matchups)} games")

        for config_index, (config_name, config) in enumerate(CONFIGS.items(), start=1):
            print(f"\n[config {config_index}/{len(CONFIGS)}] {config_name}")

            total_errors: List[float] = []
            total_signed_errors: List[float] = []

            home_errors: List[float] = []
            home_signed_errors: List[float] = []

            away_errors: List[float] = []
            away_signed_errors: List[float] = []

            diff_errors: List[float] = []
            diff_signed_errors: List[float] = []

            briers: List[float] = []
            log_losses: List[float] = []
            winner_correct: List[float] = []

            predicted_totals: List[float] = []
            predicted_probs: List[float] = []
            actual_totals: List[float] = []

            simulation_success = 0
            games_evaluated = 0

            for game_index, (game_date, raw_matchup) in enumerate(all_matchups, start=1):
                game_pk = raw_matchup.get("game_pk")
                actual = actual_results[game_pk]

                matchup_copy = apply_config(raw_matchup, config)

                try:
                    sim_output = run_full_game_simulation(
                        game_pk,
                        config={
                            "simulation_count": SIMS_PER_GAME,
                            "seed": SEED,
                            "matchup": {
                                "raw": matchup_copy,
                                "game_date": game_date,
                            },
                        },
                    )
                    simulation_success += 1
                except Exception as exc:
                    print(f"[warn] simulation failed {game_pk}: {exc}")
                    continue

                games_evaluated += 1
                pred = extract_prediction(sim_output)

                if pred["run_parser_fallback_used"]:
                    run_parser_fallback_count += 1

                if not pred["run_parser_valid"]:
                    run_parser_missing_count += 1

                if not pred["win_probability_available"]:
                    win_parser_missing_count += 1

                actual_home = float(actual["home_score"])
                actual_away = float(actual["away_score"])
                actual_total = actual_home + actual_away
                actual_diff = actual_home - actual_away
                actual_home_win = 1 if actual_home > actual_away else 0

                total_err = None
                total_signed = None

                home_err = None
                home_signed = None

                away_err = None
                away_signed = None

                diff_err = None
                diff_signed = None

                if pred["run_parser_valid"]:
                    total_signed = pred["pred_total_runs"] - actual_total
                    total_err = abs(total_signed)

                    home_signed = pred["pred_home_runs"] - actual_home
                    home_err = abs(home_signed)

                    away_signed = pred["pred_away_runs"] - actual_away
                    away_err = abs(away_signed)

                    diff_signed = pred["pred_run_diff"] - actual_diff
                    diff_err = abs(diff_signed)

                    total_errors.append(total_err)
                    total_signed_errors.append(total_signed)

                    home_errors.append(home_err)
                    home_signed_errors.append(home_signed)

                    away_errors.append(away_err)
                    away_signed_errors.append(away_signed)

                    diff_errors.append(diff_err)
                    diff_signed_errors.append(diff_signed)

                    predicted_totals.append(pred["pred_total_runs"])
                    actual_totals.append(actual_total)

                if pred["win_probability_available"]:
                    br = brier(pred["home_win_prob"], actual_home_win)
                    ll = log_loss(pred["home_win_prob"], actual_home_win)
                    pred_home_win = 1 if pred["home_win_prob"] >= 0.5 else 0

                    briers.append(br)
                    log_losses.append(ll)
                    winner_correct.append(1 if pred_home_win == actual_home_win else 0)
                    predicted_probs.append(pred["home_win_prob"])
                else:
                    br = None
                    ll = None

                game_rows.append(
                    {
                        "config": config_name,
                        "game_pk": game_pk,
                        "game_date": game_date,
                        "pred_home_runs": pred["pred_home_runs"],
                        "pred_away_runs": pred["pred_away_runs"],
                        "pred_total": pred["pred_total_runs"],
                        "pred_home_win_prob": pred["home_win_prob"],
                        "actual_home": actual_home,
                        "actual_away": actual_away,
                        "actual_total": actual_total,
                        "total_error": total_err,
                        "home_error": home_err,
                        "away_error": away_err,
                        "run_diff_error": diff_err,
                        "brier": br,
                        "log_loss": ll,
                        "parser_quality_status": pred["parser_quality_status"],
                        "win_parser_status": pred["win_parser_status"],
                    }
                )

                for metric_name, value, path, fallback in [
                    ("home_expected_runs", pred["pred_home_runs"], pred["home_runs_path"], pred["run_parser_fallback_used"]),
                    ("away_expected_runs", pred["pred_away_runs"], pred["away_runs_path"], pred["run_parser_fallback_used"]),
                    ("total_expected_runs", pred["pred_total_runs"], pred["total_runs_path"], pred["run_parser_fallback_used"]),
                    ("home_win_probability", pred["home_win_prob"], pred["home_win_prob_path"], not pred["win_probability_available"]),
                ]:
                    parser_rows.append(
                        {
                            "config": config_name,
                            "game_pk": game_pk,
                            "metric": metric_name,
                            "path": path,
                            "value": value,
                            "fallback_used": fallback,
                            "parser_quality_status": pred["parser_quality_status"],
                            "win_parser_status": pred["win_parser_status"],
                        }
                    )

                if game_index % 10 == 0:
                    print(f"[progress] {config_name} {game_index}/{len(all_matchups)}")

            avg_brier = safe_mean(briers)
            avg_log = safe_mean(log_losses)
            total_mae = safe_mean(total_errors)
            total_rmse = safe_rmse(total_signed_errors)

            total_mse = safe_mse(total_signed_errors)
            home_mse = safe_mse(home_signed_errors)
            away_mse = safe_mse(away_signed_errors)
            diff_mse = safe_mse(diff_signed_errors)

            home_rmse = safe_rmse(home_signed_errors)
            away_rmse = safe_rmse(away_signed_errors)
            diff_rmse = safe_rmse(diff_signed_errors)

            combined_score = None
            if total_mae is not None and total_rmse is not None:
                combined_score = (total_mae / 10) + (total_rmse / 10)
                if avg_brier is not None:
                    combined_score += avg_brier
                if avg_log is not None:
                    combined_score += avg_log

            config_rows.append(
                {
                    "config": config_name,
                    "games_evaluated": games_evaluated,
                    "simulation_success_rate": round(simulation_success / games_evaluated, 6) if games_evaluated else None,
                    "run_games_scored": len(total_errors),
                    "win_games_scored": len(briers),
                    "winner_accuracy": round(safe_mean(winner_correct), 6) if winner_correct else None,
                    "home_win_brier": round(avg_brier, 6) if avg_brier is not None else None,
                    "home_win_log_loss": round(avg_log, 6) if avg_log is not None else None,
                    "total_run_mae": round(total_mae, 6) if total_mae is not None else None,
                    "total_run_rmse": round(total_rmse, 6) if total_rmse is not None else None,
                    "home_run_mae": round(safe_mean(home_errors), 6) if home_errors else None,
                    "away_run_mae": round(safe_mean(away_errors), 6) if away_errors else None,
                    "run_differential_mae": round(safe_mean(diff_errors), 6) if diff_errors else None,

                    "home_run_rmse": round(home_rmse, 6) if home_rmse is not None else None,
                    "away_run_rmse": round(away_rmse, 6) if away_rmse is not None else None,
                    "run_differential_rmse": round(diff_rmse, 6) if diff_rmse is not None else None,

                    "total_run_mse": round(total_mse, 6) if total_mse is not None else None,
                    "home_run_mse": round(home_mse, 6) if home_mse is not None else None,
                    "away_run_mse": round(away_mse, 6) if away_mse is not None else None,
                    "run_differential_mse": round(diff_mse, 6) if diff_mse is not None else None,
                    "average_predicted_total": round(safe_mean(predicted_totals), 6) if predicted_totals else None,
                    "average_actual_total": round(safe_mean(actual_totals), 6) if actual_totals else None,
                    "predicted_total_bias": round(safe_mean(predicted_totals) - safe_mean(actual_totals), 6)
                    if predicted_totals and actual_totals
                    else None,
                    "prediction_variability_total": round(max(predicted_totals) - min(predicted_totals), 6) if predicted_totals else None,
                    "prediction_variability_prob": round(max(predicted_probs) - min(predicted_probs), 6) if predicted_probs else None,
                    "combined_score": round(combined_score, 6) if combined_score is not None else None,
                }
            )

    ranked_by_runs = sorted(
        [row for row in config_rows if row["total_run_mae"] is not None],
        key=lambda row: (row["total_run_mae"], row["total_run_rmse"]),
    )

    ranked_by_combined = sorted(
        [row for row in config_rows if row["combined_score"] is not None],
        key=lambda row: row["combined_score"],
    )

    suspicious_rmse = False

    for row in config_rows:

        mae = row.get("total_run_mae")
        rmse = row.get("total_run_rmse")

        if (
            mae is not None
            and rmse is not None
            and rmse > (mae * 3.0)
        ):
            suspicious_rmse = True

    if suspicious_rmse:
        diagnosis = "rmse_inflation_detected"
    elif run_parser_missing_count > 0:
        diagnosis = "expected_run_parser_paths_missing"
    elif win_parser_missing_count > 0:
        diagnosis = "run_calibration_ready_win_parser_missing"
    else:
        diagnosis = "rmse_verified"

    payload = {
        "diagnosis": diagnosis,
        "backtest_start": BACKTEST_START,
        "backtest_end": BACKTEST_END,
        "sims_per_game": SIMS_PER_GAME,
        "max_games": MAX_GAMES,
        "run_parser_fallback_count": run_parser_fallback_count,
        "run_parser_missing_count": run_parser_missing_count,
        "win_parser_missing_count": win_parser_missing_count,
        "ranked_configs_by_run_mae": ranked_by_runs,
        "ranked_configs_by_combined_score": ranked_by_combined,
        "config_summaries": config_rows,
        "recommended_next_step": (
            "Run full calibration with SIMS_PER_GAME=500."
            if diagnosis == "true_calibration_grid_ready_for_full_run"
            else "Patch/confirm winner probability parser or proceed with run-only calibration."
        ),
    }

    out_dir = Path("tmp")
    out_dir.mkdir(parents=True, exist_ok=True)

    prefix = f"true_calibration_grid_{BACKTEST_START}_to_{BACKTEST_END}"

    json_path = out_dir / f"{prefix}.json"
    games_csv = out_dir / f"{prefix}_games.csv"
    configs_csv = out_dir / f"{prefix}_configs.csv"
    parser_csv = out_dir / "true_calibration_grid_parser_paths.csv"

    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    write_csv(games_csv, game_rows)
    write_csv(configs_csv, config_rows)
    write_csv(parser_csv, parser_rows)

    print(json.dumps(payload, indent=2, sort_keys=True))
    print(f"Wrote {json_path}")
    print(f"Wrote {games_csv}")
    print(f"Wrote {configs_csv}")
    print(f"Wrote {parser_csv}")


if __name__ == "__main__":
    main()
