from __future__ import annotations

import csv
import json
import math
import os
import random
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, List


BOOTSTRAP_SAMPLES = int(os.getenv("BOOTSTRAP_SAMPLES", "5000"))
SEED = int(os.getenv("SEED", "42"))

INPUT_GAMES = Path("tmp/expected_contact_micro_grid_games.csv")
INPUT_CONFIGS = Path("tmp/expected_contact_micro_grid_configs.csv")
INPUT_BOOTSTRAP = Path("tmp/expected_contact_micro_grid_bootstrap.csv")

OUT_JSON = Path("tmp/expected_contact_micro_stability.json")
OUT_BOOTSTRAP = Path("tmp/expected_contact_micro_stability_bootstrap.csv")
OUT_SUMMARY = Path("tmp/expected_contact_micro_stability_candidate_summary.csv")

BASELINE = "baseline"

METRICS = {
    "total_abs_error": {
        "source": "total_error",
        "transform": "abs",
        "direction": "lower_better",
    },
    "total_squared_error": {
        "source": "total_error",
        "transform": "square",
        "direction": "lower_better",
    },
    "home_abs_error": {
        "source": "home_error",
        "transform": "abs",
        "direction": "lower_better",
    },
    "away_abs_error": {
        "source": "away_error",
        "transform": "abs",
        "direction": "lower_better",
    },
    "brier_component": {
        "source": "brier_component",
        "transform": "identity",
        "direction": "lower_better",
    },
    "logloss_component": {
        "source": "logloss_component",
        "transform": "identity",
        "direction": "lower_better",
    },
    "signed_total_error": {
        "source": "total_error",
        "transform": "identity",
        "direction": "zero_better",
    },
}


def read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(path)

    with path.open() as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return

    fields = sorted({k for row in rows for k in row.keys()})

    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def to_float(value: Any) -> float:
    if value is None or value == "":
        return float("nan")

    return float(value)


def transformed_value(row: Dict[str, Any], metric_name: str) -> float:
    spec = METRICS[metric_name]
    raw = to_float(row.get(spec["source"]))

    if spec["transform"] == "abs":
        return abs(raw)

    if spec["transform"] == "square":
        return raw * raw

    return raw


def percentile(values: List[float], pct: float) -> float:
    values = sorted(values)

    if not values:
        return float("nan")

    idx = int((len(values) - 1) * pct)

    return values[idx]


def paired_metric_deltas(
    baseline_rows: List[Dict[str, Any]],
    candidate_rows: List[Dict[str, Any]],
    metric_name: str,
) -> List[float]:
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

    deltas = []

    for game_pk in common_games:
        base_value = transformed_value(
            baseline_lookup[game_pk],
            metric_name,
        )

        candidate_value = transformed_value(
            candidate_lookup[game_pk],
            metric_name,
        )

        if math.isnan(base_value) or math.isnan(candidate_value):
            continue

        deltas.append(candidate_value - base_value)

    return deltas


def bootstrap_deltas(deltas: List[float]) -> Dict[str, Any]:
    if not deltas:
        return {
            "mean_delta": None,
            "median_delta": None,
            "ci_lower": None,
            "ci_upper": None,
            "pct_better_than_baseline": None,
        }

    rng = random.Random(SEED)
    boot_means = []

    for _ in range(BOOTSTRAP_SAMPLES):
        sample = [
            rng.choice(deltas)
            for _ in range(len(deltas))
        ]

        boot_means.append(mean(sample))

    return {
        "mean_delta": mean(deltas),
        "median_delta": median(deltas),
        "ci_lower": percentile(boot_means, 0.025),
        "ci_upper": percentile(boot_means, 0.975),
        "pct_better_than_baseline": sum(1 for x in deltas if x < 0) / len(deltas),
    }


def detect_nonmonotonicity(config_summaries: List[Dict[str, Any]]) -> Dict[str, Any]:
    nonbaseline = [
        row
        for row in config_summaries
        if row["config"] != BASELINE
    ]

    ordered = sorted(
        nonbaseline,
        key=lambda row: float(row["boost"]),
    )

    combined_values = [
        float(row["combined_score"])
        for row in ordered
    ]

    # For a clean monotonic improvement then degradation curve, values should not
    # bounce repeatedly. Count direction changes.
    signs = []

    for left, right in zip(combined_values, combined_values[1:]):
        diff = right - left

        if diff > 0:
            signs.append(1)
        elif diff < 0:
            signs.append(-1)
        else:
            signs.append(0)

    cleaned = [
        s for s in signs
        if s != 0
    ]

    direction_changes = 0

    for a, b in zip(cleaned, cleaned[1:]):
        if a != b:
            direction_changes += 1

    best = min(
        ordered,
        key=lambda row: float(row["combined_score"]),
    )

    return {
        "direction_changes": direction_changes,
        "best_config": best["config"],
        "best_boost": float(best["boost"]),
        "is_materially_nonmonotonic": direction_changes >= 2,
    }


def classify(
    candidate_summary_rows: List[Dict[str, Any]],
    bootstrap_rows: List[Dict[str, Any]],
    monotonicity: Dict[str, Any],
) -> str:
    best = min(
        candidate_summary_rows,
        key=lambda row: float(row["combined_mean_delta"]),
    )

    best_config = best["config"]

    best_bootstrap = [
        row
        for row in bootstrap_rows
        if row["config"] == best_config
    ]

    by_metric = {
        row["metric"]: row
        for row in best_bootstrap
    }

    total_abs = by_metric.get("total_abs_error")
    total_sq = by_metric.get("total_squared_error")
    brier = by_metric.get("brier_component")
    logloss = by_metric.get("logloss_component")

    if not total_abs or not brier or not logloss:
        return "expected_contact_below_noise_floor"

    improves_run = (
        total_abs["mean_delta"] < 0
        and total_sq["mean_delta"] < 0
    )

    improves_win = (
        brier["mean_delta"] < 0
        and logloss["mean_delta"] < 0
    )

    confirmed = (
        improves_run
        and improves_win
        and total_abs["ci_upper"] < 0
        and brier["ci_upper"] < 0
        and logloss["ci_upper"] < 0
    )

    if confirmed:
        return "expected_contact_micro_signal_confirmed"

    if improves_win and not improves_run:
        return "expected_contact_directional_only"

    if monotonicity["is_materially_nonmonotonic"]:
        return "expected_contact_unstable_or_nonmonotonic"

    if improves_run or improves_win:
        return "expected_contact_small_but_inconclusive"

    return "expected_contact_below_noise_floor"


def main() -> None:
    game_rows = read_csv(INPUT_GAMES)
    config_rows = read_csv(INPUT_CONFIGS)

    baseline_rows = [
        row for row in game_rows
        if row["config"] == BASELINE
    ]

    configs = sorted({
        row["config"]
        for row in game_rows
        if row["config"] != BASELINE
    })

    bootstrap_rows = []
    candidate_summary_rows = []

    for config in configs:
        candidate_rows = [
            row for row in game_rows
            if row["config"] == config
        ]

        combined_components = []

        for metric_name in METRICS:
            deltas = paired_metric_deltas(
                baseline_rows,
                candidate_rows,
                metric_name,
            )

            stats = bootstrap_deltas(deltas)

            out = {
                "config": config,
                "metric": metric_name,
                "games": len(deltas),
                **stats,
            }

            bootstrap_rows.append(out)

            if metric_name in [
                "total_abs_error",
                "total_squared_error",
                "brier_component",
                "logloss_component",
            ]:
                if stats["mean_delta"] is not None:
                    combined_components.append(stats["mean_delta"])

        summary = {
            "config": config,
            "combined_mean_delta": sum(combined_components),
            "metrics_improved_count": sum(
                1
                for row in bootstrap_rows
                if row["config"] == config
                and row["mean_delta"] is not None
                and row["mean_delta"] < 0
            ),
        }

        for row in bootstrap_rows:
            if row["config"] != config:
                continue

            summary[f"{row['metric']}_mean_delta"] = row["mean_delta"]
            summary[f"{row['metric']}_ci_lower"] = row["ci_lower"]
            summary[f"{row['metric']}_ci_upper"] = row["ci_upper"]
            summary[f"{row['metric']}_pct_better"] = row["pct_better_than_baseline"]

        candidate_summary_rows.append(summary)

    monotonicity = detect_nonmonotonicity(config_rows)

    diagnosis = classify(
        candidate_summary_rows,
        bootstrap_rows,
        monotonicity,
    )

    ranked_candidates = sorted(
        candidate_summary_rows,
        key=lambda row: row["combined_mean_delta"],
    )

    payload = {
        "diagnosis": diagnosis,
        "bootstrap_samples": BOOTSTRAP_SAMPLES,
        "seed": SEED,
        "baseline_games": len(baseline_rows),
        "monotonicity": monotonicity,
        "ranked_candidates": ranked_candidates,
        "recommended_next_step": (
            "Keep expected-contact as a research candidate, but do not productionize."
            if diagnosis in [
                "expected_contact_small_but_inconclusive",
                "expected_contact_unstable_or_nonmonotonic",
            ]
            else "Expected-contact may advance to broader validation."
            if diagnosis == "expected_contact_micro_signal_confirmed"
            else "Do not prioritize expected-contact until new feature design."
        ),
    }

    OUT_JSON.write_text(
        json.dumps(payload, indent=2, sort_keys=True)
    )

    write_csv(OUT_BOOTSTRAP, bootstrap_rows)
    write_csv(OUT_SUMMARY, ranked_candidates)

    print(
        json.dumps(
            {
                "diagnosis": diagnosis,
                "monotonicity": monotonicity,
                "ranked_candidates": ranked_candidates,
                "recommended_next_step": payload["recommended_next_step"],
            },
            indent=2,
            sort_keys=True,
        )
    )

    print(f"Wrote {OUT_JSON}")
    print(f"Wrote {OUT_BOOTSTRAP}")
    print(f"Wrote {OUT_SUMMARY}")


if __name__ == "__main__":
    main()
