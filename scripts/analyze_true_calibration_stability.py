from __future__ import annotations

import csv
import json
import math
import os
import random
from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, List

BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
BACKTEST_END = os.getenv("BACKTEST_END", "2026-05-03")
BOOTSTRAP_SAMPLES = int(os.getenv("BOOTSTRAP_SAMPLES", "2000"))
SEED = int(os.getenv("SEED", "42"))

BASELINE = "baseline_current_production"

INPUT_GAMES = Path(
    f"tmp/true_calibration_grid_{BACKTEST_START}_to_{BACKTEST_END}_games.csv"
)

INPUT_CONFIGS = Path(
    f"tmp/true_calibration_grid_{BACKTEST_START}_to_{BACKTEST_END}_configs.csv"
)

OUT_JSON = Path(
    f"tmp/true_calibration_stability_{BACKTEST_START}_to_{BACKTEST_END}.json"
)

OUT_DELTAS = Path(
    "tmp/true_calibration_stability_candidate_deltas.csv"
)

OUT_BOOTSTRAP = Path(
    "tmp/true_calibration_stability_bootstrap.csv"
)

OUT_BUCKETS = Path(
    "tmp/true_calibration_stability_buckets.csv"
)


METRICS = [
    "total_error",
    "home_error",
    "away_error",
    "run_diff_error",
    "brier",
    "log_loss",
]


def read_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open() as f:
        return list(csv.DictReader(f))


def to_float(x):
    try:
        return float(x)
    except:
        return None


def percentile(vals, pct):
    if not vals:
        return None

    vals = sorted(vals)
    idx = int((len(vals) - 1) * pct)
    return vals[idx]


def bootstrap_mean(vals, n=2000, seed=42):
    rng = random.Random(seed)

    if not vals:
        return None

    means = []

    for _ in range(n):
        sample = [rng.choice(vals) for _ in range(len(vals))]
        means.append(mean(sample))

    return {
        "mean": mean(vals),
        "median": median(vals),
        "ci_lower": percentile(means, 0.025),
        "ci_upper": percentile(means, 0.975),
    }


def bucket_actual_total(actual_total):
    if actual_total <= 7:
        return "low_actual_total"

    if actual_total <= 10:
        return "medium_actual_total"

    return "high_actual_total"


def bucket_pred_total(pred_total):
    if pred_total <= 7:
        return "low_pred_total"

    if pred_total <= 10:
        return "medium_pred_total"

    return "high_pred_total"


def bucket_probability(prob):
    if prob is None:
        return "missing_prob"

    if 0.45 <= prob <= 0.55:
        return "tossup"

    if (
        (0.55 < prob <= 0.65)
        or
        (0.35 <= prob < 0.45)
    ):
        return "moderate_favorite"

    return "strong_favorite"


def main():

    random.seed(SEED)

    games = read_csv(INPUT_GAMES)
    configs = read_csv(INPUT_CONFIGS)

    by_config_game = defaultdict(dict)

    for row in games:
        config = row["config"]
        game_pk = row["game_pk"]

        by_config_game[config][game_pk] = row

    baseline_games = by_config_game[BASELINE]

    all_configs = sorted(by_config_game.keys())

    candidate_rows = []
    bootstrap_rows = []
    bucket_rows = []

    stability_summary = {}

    for config in all_configs:

        if config == BASELINE:
            continue

        paired_rows = []

        for game_pk, base_row in baseline_games.items():

            cand_row = by_config_game[config].get(game_pk)

            if not cand_row:
                continue

            merged = {
                "game_pk": game_pk,
                "game_date": cand_row.get("game_date"),
            }

            valid = True

            for metric in METRICS:

                base_val = to_float(base_row.get(metric))
                cand_val = to_float(cand_row.get(metric))

                if base_val is None or cand_val is None:
                    valid = False
                    break

                delta = cand_val - base_val

                merged[f"{metric}_baseline"] = base_val
                merged[f"{metric}_candidate"] = cand_val
                merged[f"{metric}_delta"] = delta

            if not valid:
                continue

            pred_total = to_float(cand_row.get("pred_total"))
            actual_total = to_float(cand_row.get("actual_total"))
            pred_prob = to_float(cand_row.get("pred_home_win_prob"))

            merged["actual_total_bucket"] = bucket_actual_total(actual_total)
            merged["pred_total_bucket"] = bucket_pred_total(pred_total)
            merged["favorite_bucket"] = bucket_probability(pred_prob)

            paired_rows.append(merged)

        metric_summary = {}

        for metric in METRICS:

            deltas = [
                row[f"{metric}_delta"]
                for row in paired_rows
            ]

            stats = bootstrap_mean(
                deltas,
                n=BOOTSTRAP_SAMPLES,
                seed=SEED,
            )

            metric_summary[metric] = stats

            bootstrap_rows.append({
                "config": config,
                "metric": metric,
                **stats,
            })

        bucket_groups = defaultdict(list)

        for row in paired_rows:

            for bucket_type in [
                "actual_total_bucket",
                "pred_total_bucket",
                "favorite_bucket",
            ]:

                bucket_key = row[bucket_type]

                bucket_groups[(bucket_type, bucket_key)].append(
                    row["total_error_delta"]
                )

        for (bucket_type, bucket_key), vals in bucket_groups.items():

            bucket_rows.append({
                "config": config,
                "bucket_type": bucket_type,
                "bucket_key": bucket_key,
                "games": len(vals),
                "mean_total_error_delta": mean(vals),
                "median_total_error_delta": median(vals),
            })

        total_delta = metric_summary["total_error"]["mean"]
        brier_delta = metric_summary["brier"]["mean"]
        log_delta = metric_summary["log_loss"]["mean"]

        verdict = "needs_more_sample"

        if (
            total_delta < 0
            and
            brier_delta < 0
            and
            log_delta < 0
        ):

            ci_upper = metric_summary["total_error"]["ci_upper"]

            if ci_upper is not None and ci_upper < 0:
                verdict = "candidate_stable_improvement"
            else:
                verdict = "candidate_small_unstable_edge"

        elif (
            total_delta > 0
            and
            log_delta > 0
        ):
            verdict = "baseline_preferred"

        if "recent" in config:
            bias_delta = mean([
                row["total_error_candidate"] - row["total_error_baseline"]
                for row in paired_rows
            ])

            if bias_delta > 0:
                verdict = "recent_modifier_overfit_or_bias"

        stability_summary[config] = {
            "verdict": verdict,
            "games": len(paired_rows),
            "metrics": metric_summary,
        }

        for row in paired_rows:
            out = {"config": config}
            out.update(row)
            candidate_rows.append(out)

    overall_verdict = "needs_more_sample"

    pitcher_summary = stability_summary.get("pitcher_stuff__small")

    if pitcher_summary:

        verdict = pitcher_summary["verdict"]

        if verdict == "candidate_stable_improvement":
            overall_verdict = verdict

        elif verdict == "candidate_small_unstable_edge":
            overall_verdict = verdict

        else:
            overall_verdict = "baseline_preferred"

    payload = {
        "diagnosis": overall_verdict,
        "bootstrap_samples": BOOTSTRAP_SAMPLES,
        "seed": SEED,
        "configs_evaluated": len(stability_summary),
        "stability_summary": stability_summary,
        "recommended_next_step": (
            "Proceed to production integration design."
            if overall_verdict == "candidate_stable_improvement"
            else "Keep baseline and continue research."
        ),
    }

    with OUT_JSON.open("w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)

    def write_csv(path, rows):

        if not rows:
            return

        keys = sorted({
            k
            for row in rows
            for k in row.keys()
        })

        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(rows)

    write_csv(OUT_DELTAS, candidate_rows)
    write_csv(OUT_BOOTSTRAP, bootstrap_rows)
    write_csv(OUT_BUCKETS, bucket_rows)

    print(json.dumps(payload, indent=2, sort_keys=True))

    print(f"Wrote {OUT_JSON}")
    print(f"Wrote {OUT_DELTAS}")
    print(f"Wrote {OUT_BOOTSTRAP}")
    print(f"Wrote {OUT_BUCKETS}")


if __name__ == "__main__":
    main()
