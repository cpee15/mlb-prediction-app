"""Cross-season validation for shadow hitter expected-power evidence."""

from __future__ import annotations

import math
import random
from collections.abc import Mapping, Sequence
from typing import Any, Optional


MODEL_FEATURES = {
    "intercept_only": (),
    "actual_iso": ("pre_actual_iso",),
    "expected_damage": ("pre_expected_damage_per_ab",),
    "expected_damage_hard_hit": (
        "pre_expected_damage_per_ab",
        "pre_hard_hit_rate",
    ),
    "expected_damage_barrel": (
        "pre_expected_damage_per_ab",
        "pre_barrel_proxy_rate",
    ),
    "expected_damage_hard_hit_barrel": (
        "pre_expected_damage_per_ab",
        "pre_hard_hit_rate",
        "pre_barrel_proxy_rate",
    ),
    "actual_expected": (
        "pre_actual_iso",
        "pre_expected_damage_per_ab",
    ),
    "actual_expected_hard_hit": (
        "pre_actual_iso",
        "pre_expected_damage_per_ab",
        "pre_hard_hit_rate",
    ),
    "actual_expected_barrel": (
        "pre_actual_iso",
        "pre_expected_damage_per_ab",
        "pre_barrel_proxy_rate",
    ),
    "actual_expected_hard_hit_barrel": (
        "pre_actual_iso",
        "pre_expected_damage_per_ab",
        "pre_hard_hit_rate",
        "pre_barrel_proxy_rate",
    ),
}


def _number(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _clean_samples(
    samples: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    required = {
        "season",
        "holdout_ab",
        "holdout_iso",
        *{
            feature
            for features in MODEL_FEATURES.values()
            for feature in features
        },
    }
    cleaned = []
    for sample in samples:
        values = {key: _number(sample.get(key)) for key in required}
        if any(value is None for value in values.values()):
            continue
        if values["holdout_ab"] <= 0:
            continue
        cleaned.append({
            **dict(sample),
            **values,
            "season": int(values["season"]),
        })
    return cleaned


def _solve(matrix: list[list[float]], vector: list[float]) -> list[float]:
    size = len(vector)
    augmented = [
        list(matrix[row]) + [vector[row]]
        for row in range(size)
    ]
    for pivot in range(size):
        best = max(
            range(pivot, size),
            key=lambda row: abs(augmented[row][pivot]),
        )
        augmented[pivot], augmented[best] = (
            augmented[best],
            augmented[pivot],
        )
        divisor = augmented[pivot][pivot]
        if abs(divisor) < 1e-12:
            raise ValueError("singular weighted regression matrix")
        augmented[pivot] = [
            value / divisor for value in augmented[pivot]
        ]
        for row in range(size):
            if row == pivot:
                continue
            factor = augmented[row][pivot]
            augmented[row] = [
                value - factor * pivot_value
                for value, pivot_value in zip(
                    augmented[row],
                    augmented[pivot],
                )
            ]
    return [augmented[row][-1] for row in range(size)]


def _fit(
    samples: Sequence[Mapping[str, Any]],
    features: Sequence[str],
) -> list[float]:
    width = len(features) + 1
    matrix = [[0.0] * width for _ in range(width)]
    vector = [0.0] * width
    for sample in samples:
        weight = float(sample["holdout_ab"])
        row = [1.0] + [float(sample[key]) for key in features]
        target = float(sample["holdout_iso"])
        for left in range(width):
            vector[left] += weight * row[left] * target
            for right in range(width):
                matrix[left][right] += (
                    weight * row[left] * row[right]
                )
    for index in range(1, width):
        matrix[index][index] += 1e-9
    return _solve(matrix, vector)


def _score(
    samples: Sequence[Mapping[str, Any]],
    features: Sequence[str],
    coefficients: Sequence[float],
) -> dict[str, Any]:
    total_weight = sum(float(row["holdout_ab"]) for row in samples)
    squared_error = 0.0
    absolute_error = 0.0
    for sample in samples:
        prediction = coefficients[0] + sum(
            coefficient * float(sample[feature])
            for coefficient, feature in zip(
                coefficients[1:],
                features,
            )
        )
        prediction = max(0.0, min(1.0, prediction))
        error = prediction - float(sample["holdout_iso"])
        weight = float(sample["holdout_ab"])
        squared_error += weight * error * error
        absolute_error += weight * abs(error)
    return {
        "sample_count": len(samples),
        "holdout_ab": int(total_weight),
        "weighted_mean_squared_error": (
            squared_error / total_weight if total_weight else None
        ),
        "weighted_mean_absolute_error": (
            absolute_error / total_weight if total_weight else None
        ),
    }


def _relative_improvement(candidate: float, reference: float) -> float:
    return (reference - candidate) / reference if reference else 0.0


def evaluate_hitter_power_incremental_models(
    samples: Sequence[Mapping[str, Any]],
    *,
    minimum_fold_samples: int = 50,
) -> dict[str, Any]:
    """Compare expected-power feature sets on held-out seasons."""

    cleaned = _clean_samples(samples)
    seasons = sorted({row["season"] for row in cleaned})
    blockers = []
    if len(seasons) < 2:
        blockers.append("insufficient_validation_seasons")
    folds = []
    aggregate: dict[str, dict[str, float]] = {
        name: {
            "weighted_squared_error": 0.0,
            "weighted_absolute_error": 0.0,
            "holdout_ab": 0.0,
            "fold_count": 0.0,
        }
        for name in MODEL_FEATURES
    }
    for validation_season in seasons:
        training = [
            row for row in cleaned
            if row["season"] != validation_season
        ]
        validation = [
            row for row in cleaned
            if row["season"] == validation_season
        ]
        if (
            len(training) < minimum_fold_samples
            or len(validation) < minimum_fold_samples
        ):
            blockers.append(
                f"insufficient_fold_samples:{validation_season}"
            )
            continue
        results = {}
        for name, features in MODEL_FEATURES.items():
            coefficients = _fit(training, features)
            scores = _score(validation, features, coefficients)
            results[name] = {
                "features": list(features),
                "coefficients": list(coefficients),
                "validation_scores": scores,
            }
            accumulator = aggregate[name]
            holdout_ab = float(scores["holdout_ab"])
            accumulator["weighted_squared_error"] += (
                scores["weighted_mean_squared_error"] * holdout_ab
            )
            accumulator["weighted_absolute_error"] += (
                scores["weighted_mean_absolute_error"] * holdout_ab
            )
            accumulator["holdout_ab"] += holdout_ab
            accumulator["fold_count"] += 1
        ranked = sorted(
            results,
            key=lambda name: (
                results[name]["validation_scores"][
                    "weighted_mean_squared_error"
                ],
                len(MODEL_FEATURES[name]),
                name,
            ),
        )
        folds.append({
            "validation_season": validation_season,
            "training_seasons": [
                season for season in seasons
                if season != validation_season
            ],
            "training_sample_count": len(training),
            "validation_sample_count": len(validation),
            "best_model": ranked[0],
            "models": results,
        })

    summary = {}
    for name, accumulator in aggregate.items():
        holdout_ab = accumulator["holdout_ab"]
        if not holdout_ab:
            continue
        summary[name] = {
            "fold_count": int(accumulator["fold_count"]),
            "holdout_ab": int(holdout_ab),
            "weighted_mean_squared_error": (
                accumulator["weighted_squared_error"] / holdout_ab
            ),
            "weighted_mean_absolute_error": (
                accumulator["weighted_absolute_error"] / holdout_ab
            ),
        }
    ranked_models = sorted(
        summary,
        key=lambda name: (
            summary[name]["weighted_mean_squared_error"],
            len(MODEL_FEATURES[name]),
            name,
        ),
    )
    if summary:
        actual_mse = summary["actual_iso"]["weighted_mean_squared_error"]
        expected_mse = summary["expected_damage"][
            "weighted_mean_squared_error"
        ]
        blend_mse = summary["actual_expected"][
            "weighted_mean_squared_error"
        ]
        full_mse = summary["actual_expected_hard_hit_barrel"][
            "weighted_mean_squared_error"
        ]
        comparisons = {
            "expected_vs_actual_relative_mse_improvement":
                _relative_improvement(expected_mse, actual_mse),
            "blend_vs_best_univariate_relative_mse_improvement":
                _relative_improvement(
                    blend_mse,
                    min(actual_mse, expected_mse),
                ),
            "full_vs_actual_expected_relative_mse_improvement":
                _relative_improvement(full_mse, blend_mse),
        }
    else:
        comparisons = {}
    return {
        "schema_version":
            "shadow_hitter_power_incremental_validation_v1",
        "status": "blocked" if blockers else "ready",
        "shadow_only": True,
        "parameter_selected": False,
        "production_authority_changed": False,
        "sample_count": len(cleaned),
        "seasons": seasons,
        "model_features": {
            name: list(features)
            for name, features in MODEL_FEATURES.items()
        },
        "cross_season_folds": folds,
        "cross_season_summary": summary,
        "ranked_models": ranked_models,
        "comparisons": comparisons,
        "blockers": blockers,
    }


def _percentile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("percentile requires at least one value")
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return (
        ordered[lower] * (1.0 - fraction)
        + ordered[upper] * fraction
    )


def bootstrap_hitter_power_model_differences(
    samples: Sequence[Mapping[str, Any]],
    *,
    iterations: int = 400,
    seed: int = 20260729,
    minimum_fold_samples: int = 50,
) -> dict[str, Any]:
    """Bootstrap cross-season MSE differences by whole player clusters."""

    cleaned = _clean_samples(samples)
    grouped: dict[Any, list[dict[str, Any]]] = {}
    for index, sample in enumerate(cleaned):
        cluster = sample.get("player_id")
        if cluster is None:
            cluster = ("row", index)
        grouped.setdefault(cluster, []).append(sample)
    cluster_keys = sorted(grouped, key=str)
    blockers = []
    if len(cluster_keys) < 30:
        blockers.append("insufficient_player_clusters")
    if iterations < 100:
        blockers.append("insufficient_bootstrap_iterations")
    comparisons = {
        "expected_damage_minus_actual_iso": (
            "actual_iso",
            "expected_damage",
        ),
        "hard_hit_increment_over_expected_damage": (
            "expected_damage",
            "expected_damage_hard_hit",
        ),
        "barrel_increment_over_expected_damage": (
            "expected_damage",
            "expected_damage_barrel",
        ),
        "full_increment_over_expected_damage": (
            "expected_damage",
            "expected_damage_hard_hit_barrel",
        ),
    }
    draws = {name: [] for name in comparisons}
    rng = random.Random(seed)
    successful = 0
    if not blockers:
        for _ in range(iterations):
            selected = [
                cluster_keys[rng.randrange(len(cluster_keys))]
                for _ in cluster_keys
            ]
            resampled = [
                row
                for cluster in selected
                for row in grouped[cluster]
            ]
            result = evaluate_hitter_power_incremental_models(
                resampled,
                minimum_fold_samples=minimum_fold_samples,
            )
            if result["status"] != "ready":
                continue
            summary = result["cross_season_summary"]
            for name, (reference, candidate) in comparisons.items():
                reference_mse = summary[reference][
                    "weighted_mean_squared_error"
                ]
                candidate_mse = summary[candidate][
                    "weighted_mean_squared_error"
                ]
                draws[name].append(reference_mse - candidate_mse)
            successful += 1
    if successful < max(100, int(iterations * 0.90)):
        blockers.append("insufficient_successful_bootstrap_draws")
    intervals = {}
    for name, values in draws.items():
        if not values:
            continue
        intervals[name] = {
            "mse_improvement_median": _percentile(values, 0.50),
            "mse_improvement_ci_95": {
                "lower": _percentile(values, 0.025),
                "upper": _percentile(values, 0.975),
            },
            "probability_of_improvement": (
                sum(value > 0 for value in values) / len(values)
            ),
        }
    return {
        "schema_version":
            "shadow_hitter_power_clustered_bootstrap_v1",
        "status": "blocked" if blockers else "ready",
        "shadow_only": True,
        "parameter_selected": False,
        "production_authority_changed": False,
        "cluster_key": "player_id",
        "cluster_count": len(cluster_keys),
        "requested_iterations": iterations,
        "successful_iterations": successful,
        "seed": seed,
        "difference_definition":
            "reference cross-season MSE minus candidate cross-season MSE",
        "comparisons": intervals,
        "blockers": blockers,
    }
