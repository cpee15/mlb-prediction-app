"""Cross-season validation for shadow hitter walk-skill evidence."""

from __future__ import annotations

import math
import random
from collections.abc import Mapping, Sequence
from typing import Any


MODEL_FEATURES = {
    "intercept_only": (),
    "actual_bb_rate": ("pre_actual_bb_rate",),
    "called_ball_rate": ("pre_called_ball_rate",),
    "actual_called_ball": ("pre_actual_bb_rate", "pre_called_ball_rate"),
    "actual_called_ball_take": (
        "pre_actual_bb_rate",
        "pre_called_ball_rate",
        "pre_take_rate",
    ),
    "actual_called_ball_called_strike": (
        "pre_actual_bb_rate",
        "pre_called_ball_rate",
        "pre_called_strike_rate",
    ),
    "full": (
        "pre_actual_bb_rate",
        "pre_called_ball_rate",
        "pre_take_rate",
        "pre_called_strike_rate",
    ),
}


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _clean(samples: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    required = {
        "season",
        "holdout_pa",
        "holdout_bb_rate",
        *{item for values in MODEL_FEATURES.values() for item in values},
    }
    cleaned = []
    for source in samples:
        values = {key: _number(source.get(key)) for key in required}
        if any(value is None for value in values.values()):
            continue
        if values["holdout_pa"] <= 0:
            continue
        cleaned.append({**dict(source), **values, "season": int(values["season"])})
    return cleaned


def _solve(matrix: list[list[float]], vector: list[float]) -> list[float]:
    size = len(vector)
    augmented = [list(matrix[row]) + [vector[row]] for row in range(size)]
    for pivot in range(size):
        best = max(range(pivot, size), key=lambda row: abs(augmented[row][pivot]))
        augmented[pivot], augmented[best] = augmented[best], augmented[pivot]
        divisor = augmented[pivot][pivot]
        if abs(divisor) < 1e-12:
            raise ValueError("singular weighted regression matrix")
        augmented[pivot] = [value / divisor for value in augmented[pivot]]
        for row in range(size):
            if row == pivot:
                continue
            factor = augmented[row][pivot]
            augmented[row] = [
                value - factor * pivot_value
                for value, pivot_value in zip(augmented[row], augmented[pivot])
            ]
    return [augmented[row][-1] for row in range(size)]


def _fit(samples: Sequence[Mapping[str, Any]], features: Sequence[str]) -> list[float]:
    width = len(features) + 1
    matrix = [[0.0] * width for _ in range(width)]
    vector = [0.0] * width
    for sample in samples:
        weight = float(sample["holdout_pa"])
        row = [1.0] + [float(sample[key]) for key in features]
        target = float(sample["holdout_bb_rate"])
        for left in range(width):
            vector[left] += weight * row[left] * target
            for right in range(width):
                matrix[left][right] += weight * row[left] * row[right]
    for index in range(1, width):
        matrix[index][index] += 1e-9
    return _solve(matrix, vector)


def _score(
    samples: Sequence[Mapping[str, Any]],
    features: Sequence[str],
    coefficients: Sequence[float],
) -> dict[str, Any]:
    weight_total = sum(float(row["holdout_pa"]) for row in samples)
    squared = absolute = 0.0
    for sample in samples:
        prediction = coefficients[0] + sum(
            coefficient * float(sample[feature])
            for coefficient, feature in zip(coefficients[1:], features)
        )
        prediction = max(0.0, min(1.0, prediction))
        error = prediction - float(sample["holdout_bb_rate"])
        weight = float(sample["holdout_pa"])
        squared += weight * error * error
        absolute += weight * abs(error)
    return {
        "sample_count": len(samples),
        "holdout_pa": int(weight_total),
        "weighted_mean_squared_error": squared / weight_total,
        "weighted_mean_absolute_error": absolute / weight_total,
    }


def evaluate_hitter_walk_skill_models(
    samples: Sequence[Mapping[str, Any]],
    *,
    minimum_fold_samples: int = 50,
) -> dict[str, Any]:
    """Compare cutoff-safe walk feature sets on held-out seasons."""

    cleaned = _clean(samples)
    seasons = sorted({row["season"] for row in cleaned})
    blockers = []
    if len(seasons) < 2:
        blockers.append("insufficient_validation_seasons")
    aggregate = {
        name: {"sse": 0.0, "sae": 0.0, "weight": 0.0, "folds": 0}
        for name in MODEL_FEATURES
    }
    folds = []
    for validation_season in seasons:
        training = [row for row in cleaned if row["season"] != validation_season]
        validation = [row for row in cleaned if row["season"] == validation_season]
        if min(len(training), len(validation)) < minimum_fold_samples:
            blockers.append(f"insufficient_fold_samples:{validation_season}")
            continue
        models = {}
        for name, features in MODEL_FEATURES.items():
            coefficients = _fit(training, features)
            scores = _score(validation, features, coefficients)
            models[name] = {
                "features": list(features),
                "coefficients": coefficients,
                "validation_scores": scores,
            }
            weight = scores["holdout_pa"]
            aggregate[name]["sse"] += scores["weighted_mean_squared_error"] * weight
            aggregate[name]["sae"] += scores["weighted_mean_absolute_error"] * weight
            aggregate[name]["weight"] += weight
            aggregate[name]["folds"] += 1
        folds.append({
            "validation_season": validation_season,
            "training_seasons": [item for item in seasons if item != validation_season],
            "training_sample_count": len(training),
            "validation_sample_count": len(validation),
            "best_model": min(
                models,
                key=lambda name: (
                    models[name]["validation_scores"]["weighted_mean_squared_error"],
                    len(MODEL_FEATURES[name]),
                    name,
                ),
            ),
            "models": models,
        })
    summary = {
        name: {
            "fold_count": values["folds"],
            "holdout_pa": int(values["weight"]),
            "weighted_mean_squared_error": values["sse"] / values["weight"],
            "weighted_mean_absolute_error": values["sae"] / values["weight"],
        }
        for name, values in aggregate.items()
        if values["weight"]
    }
    ranked = sorted(
        summary,
        key=lambda name: (
            summary[name]["weighted_mean_squared_error"],
            len(MODEL_FEATURES[name]),
            name,
        ),
    )
    comparisons = {}
    if summary:
        mse = {
            name: value["weighted_mean_squared_error"]
            for name, value in summary.items()
        }
        best_single = min(mse["actual_bb_rate"], mse["called_ball_rate"])
        comparisons = {
            "called_ball_vs_actual_relative_mse_improvement":
                (mse["actual_bb_rate"] - mse["called_ball_rate"]) / mse["actual_bb_rate"],
            "blend_vs_best_univariate_relative_mse_improvement":
                (best_single - mse["actual_called_ball"]) / best_single,
            "take_increment_over_blend_relative_mse_improvement":
                (mse["actual_called_ball"] - mse["actual_called_ball_take"])
                / mse["actual_called_ball"],
            "called_strike_increment_over_blend_relative_mse_improvement":
                (mse["actual_called_ball"] - mse["actual_called_ball_called_strike"])
                / mse["actual_called_ball"],
            "full_increment_over_blend_relative_mse_improvement":
                (mse["actual_called_ball"] - mse["full"]) / mse["actual_called_ball"],
        }
    return {
        "schema_version": "shadow_hitter_walk_skill_validation_v1",
        "status": "blocked" if blockers else "ready",
        "shadow_only": True,
        "parameter_selected": False,
        "production_authority_changed": False,
        "sample_count": len(cleaned),
        "seasons": seasons,
        "model_features": {key: list(value) for key, value in MODEL_FEATURES.items()},
        "cross_season_folds": folds,
        "cross_season_summary": summary,
        "ranked_models": ranked,
        "comparisons": comparisons,
        "blockers": blockers,
    }


def _percentile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def bootstrap_hitter_walk_skill_differences(
    samples: Sequence[Mapping[str, Any]],
    *,
    iterations: int = 400,
    seed: int = 20260730,
    minimum_fold_samples: int = 50,
) -> dict[str, Any]:
    """Bootstrap cross-season MSE improvements by whole-player clusters."""

    cleaned = _clean(samples)
    grouped: dict[Any, list[dict[str, Any]]] = {}
    for index, sample in enumerate(cleaned):
        grouped.setdefault(sample.get("player_id", ("row", index)), []).append(sample)
    keys = sorted(grouped, key=str)
    blockers = []
    if len(keys) < 30:
        blockers.append("insufficient_player_clusters")
    if iterations < 100:
        blockers.append("insufficient_bootstrap_iterations")
    pairs = {
        "called_ball_minus_actual_bb": ("actual_bb_rate", "called_ball_rate"),
        "blend_increment_over_best_univariate": (None, "actual_called_ball"),
        "take_increment_over_blend": ("actual_called_ball", "actual_called_ball_take"),
        "called_strike_increment_over_blend": (
            "actual_called_ball",
            "actual_called_ball_called_strike",
        ),
        "full_increment_over_blend": ("actual_called_ball", "full"),
    }
    draws = {name: [] for name in pairs}
    successful = 0
    rng = random.Random(seed)
    if not blockers:
        for _ in range(iterations):
            selected = [keys[rng.randrange(len(keys))] for _ in keys]
            resampled = [row for key in selected for row in grouped[key]]
            result = evaluate_hitter_walk_skill_models(
                resampled,
                minimum_fold_samples=minimum_fold_samples,
            )
            if result["status"] != "ready":
                continue
            summary = result["cross_season_summary"]
            for name, (reference, candidate) in pairs.items():
                if reference is None:
                    reference_mse = min(
                        summary["actual_bb_rate"]["weighted_mean_squared_error"],
                        summary["called_ball_rate"]["weighted_mean_squared_error"],
                    )
                else:
                    reference_mse = summary[reference]["weighted_mean_squared_error"]
                candidate_mse = summary[candidate]["weighted_mean_squared_error"]
                draws[name].append(reference_mse - candidate_mse)
            successful += 1
    if successful < max(100, int(iterations * 0.9)):
        blockers.append("insufficient_successful_bootstrap_draws")
    comparisons = {}
    if successful:
        for name, values in draws.items():
            comparisons[name] = {
                "mse_improvement_median": _percentile(values, 0.5),
                "mse_improvement_ci_95": {
                    "lower": _percentile(values, 0.025),
                    "upper": _percentile(values, 0.975),
                },
                "probability_of_improvement":
                    sum(value > 0 for value in values) / len(values),
            }
    return {
        "schema_version": "shadow_hitter_walk_skill_bootstrap_v1",
        "status": "blocked" if blockers else "ready",
        "shadow_only": True,
        "parameter_selected": False,
        "production_authority_changed": False,
        "cluster_key": "player_id",
        "cluster_count": len(keys),
        "requested_iterations": iterations,
        "successful_iterations": successful,
        "difference_definition": "reference cross-season MSE minus candidate cross-season MSE",
        "comparisons": comparisons,
        "blockers": blockers,
    }
