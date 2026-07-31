"""Cross-season validation for shadow hitter hit-type allocation evidence."""

from __future__ import annotations

import math
import random
from collections.abc import Mapping, Sequence
from typing import Any, Optional


HIT_TYPES = ("single", "double", "triple", "home_run")
MODEL_FEATURES = {
    "league_prior": (),
    "actual_allocation": (
        "pre_double_share",
        "pre_triple_share",
        "pre_home_run_share",
    ),
    "expected_damage": (
        "pre_expected_damage_per_bbe",
    ),
    "actual_expected": (
        "pre_double_share",
        "pre_triple_share",
        "pre_home_run_share",
        "pre_expected_damage_per_bbe",
    ),
    "actual_expected_geometry": (
        "pre_double_share",
        "pre_triple_share",
        "pre_home_run_share",
        "pre_expected_damage_per_bbe",
        "pre_avg_exit_velocity",
        "pre_avg_launch_angle",
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
        "holdout_hits",
        *{
            f"holdout_{hit_type}_count"
            for hit_type in HIT_TYPES
        },
        *{
            feature
            for features in MODEL_FEATURES.values()
            for feature in features
        },
    }
    cleaned = []
    for sample in samples:
        values = {
            key: _number(sample.get(key))
            for key in required
        }
        if any(value is None for value in values.values()):
            continue
        if values["holdout_hits"] <= 0:
            continue
        counts = [
            values[f"holdout_{hit_type}_count"]
            for hit_type in HIT_TYPES
        ]
        if any(count < 0 for count in counts):
            continue
        if abs(sum(counts) - values["holdout_hits"]) > 1e-6:
            continue
        cleaned.append({
            **dict(sample),
            **values,
            "season": int(values["season"]),
        })
    return cleaned


def _solve(
    matrix: list[list[float]],
    vector: list[float],
) -> list[float]:
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
            raise ValueError(
                "singular weighted regression matrix"
            )
        augmented[pivot] = [
            value / divisor
            for value in augmented[pivot]
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
    return [
        augmented[row][-1]
        for row in range(size)
    ]


def _fit_outcome(
    samples: Sequence[Mapping[str, Any]],
    features: Sequence[str],
    hit_type: str,
) -> list[float]:
    width = len(features) + 1
    matrix = [
        [0.0] * width
        for _ in range(width)
    ]
    vector = [0.0] * width
    count_key = f"holdout_{hit_type}_count"

    for sample in samples:
        weight = float(sample["holdout_hits"])
        row = [1.0] + [
            float(sample[feature])
            for feature in features
        ]
        target = float(sample[count_key]) / weight
        for left in range(width):
            vector[left] += (
                weight * row[left] * target
            )
            for right in range(width):
                matrix[left][right] += (
                    weight
                    * row[left]
                    * row[right]
                )

    for index in range(1, width):
        matrix[index][index] += 1e-7
    return _solve(matrix, vector)


def _fit_model(
    samples: Sequence[Mapping[str, Any]],
    features: Sequence[str],
) -> dict[str, list[float]]:
    return {
        hit_type: _fit_outcome(
            samples,
            features,
            hit_type,
        )
        for hit_type in HIT_TYPES
    }


def _predict(
    sample: Mapping[str, Any],
    features: Sequence[str],
    coefficients: Mapping[str, Sequence[float]],
) -> dict[str, float]:
    raw = {}
    for hit_type in HIT_TYPES:
        values = coefficients[hit_type]
        prediction = values[0] + sum(
            coefficient * float(sample[feature])
            for coefficient, feature in zip(
                values[1:],
                features,
            )
        )
        raw[hit_type] = max(prediction, 1e-9)

    total = sum(raw.values())
    return {
        hit_type: raw[hit_type] / total
        for hit_type in HIT_TYPES
    }


def _score(
    samples: Sequence[Mapping[str, Any]],
    features: Sequence[str],
    coefficients: Mapping[str, Sequence[float]],
) -> dict[str, Any]:
    total_hits = 0.0
    log_loss = 0.0
    brier = 0.0
    outcome_absolute_error = {
        hit_type: 0.0
        for hit_type in HIT_TYPES
    }

    for sample in samples:
        weight = float(sample["holdout_hits"])
        total_hits += weight
        prediction = _predict(
            sample,
            features,
            coefficients,
        )

        for hit_type in HIT_TYPES:
            count = float(
                sample[f"holdout_{hit_type}_count"]
            )
            observed_share = count / weight
            predicted_share = prediction[hit_type]
            log_loss -= count * math.log(
                max(predicted_share, 1e-12)
            )
            brier += (
                weight
                * (
                    predicted_share
                    - observed_share
                )
                ** 2
            )
            outcome_absolute_error[hit_type] += (
                weight
                * abs(
                    predicted_share
                    - observed_share
                )
            )

    return {
        "sample_count": len(samples),
        "holdout_hits": int(total_hits),
        "weighted_multinomial_log_loss": (
            log_loss / total_hits
            if total_hits
            else None
        ),
        "weighted_compositional_brier": (
            brier / total_hits
            if total_hits
            else None
        ),
        "weighted_absolute_error_by_hit_type": {
            hit_type: (
                outcome_absolute_error[hit_type]
                / total_hits
                if total_hits
                else None
            )
            for hit_type in HIT_TYPES
        },
    }


def _relative_improvement(
    candidate: float,
    reference: float,
) -> float:
    if not reference:
        return 0.0
    return (reference - candidate) / reference


def evaluate_hitter_hit_type_allocation_models(
    samples: Sequence[Mapping[str, Any]],
    *,
    minimum_fold_samples: int = 50,
) -> dict[str, Any]:
    """Compare conditional hit-type allocation models."""

    cleaned = _clean_samples(samples)
    seasons = sorted({
        row["season"]
        for row in cleaned
    })
    blockers = []

    if len(seasons) < 2:
        blockers.append(
            "insufficient_validation_seasons"
        )

    folds = []
    aggregate = {
        name: {
            "weighted_log_loss": 0.0,
            "weighted_brier": 0.0,
            "holdout_hits": 0.0,
            "fold_count": 0.0,
        }
        for name in MODEL_FEATURES
    }

    for validation_season in seasons:
        training = [
            row
            for row in cleaned
            if row["season"] != validation_season
        ]
        validation = [
            row
            for row in cleaned
            if row["season"] == validation_season
        ]

        if (
            len(training) < minimum_fold_samples
            or len(validation) < minimum_fold_samples
        ):
            blockers.append(
                "insufficient_fold_samples:"
                f"{validation_season}"
            )
            continue

        models = {}
        for name, features in MODEL_FEATURES.items():
            coefficients = _fit_model(
                training,
                features,
            )
            scores = _score(
                validation,
                features,
                coefficients,
            )
            models[name] = {
                "features": list(features),
                "coefficients": coefficients,
                "validation_scores": scores,
            }

            weight = float(scores["holdout_hits"])
            accumulator = aggregate[name]
            accumulator["weighted_log_loss"] += (
                scores[
                    "weighted_multinomial_log_loss"
                ]
                * weight
            )
            accumulator["weighted_brier"] += (
                scores[
                    "weighted_compositional_brier"
                ]
                * weight
            )
            accumulator["holdout_hits"] += weight
            accumulator["fold_count"] += 1

        ranked = sorted(
            models,
            key=lambda name: (
                models[name]["validation_scores"][
                    "weighted_multinomial_log_loss"
                ],
                len(MODEL_FEATURES[name]),
                name,
            ),
        )
        folds.append({
            "validation_season": validation_season,
            "training_seasons": [
                season
                for season in seasons
                if season != validation_season
            ],
            "training_sample_count": len(training),
            "validation_sample_count": len(validation),
            "best_model": ranked[0],
            "models": models,
        })

    summary = {}
    for name, accumulator in aggregate.items():
        weight = accumulator["holdout_hits"]
        if not weight:
            continue
        summary[name] = {
            "fold_count": int(
                accumulator["fold_count"]
            ),
            "holdout_hits": int(weight),
            "weighted_multinomial_log_loss": (
                accumulator["weighted_log_loss"]
                / weight
            ),
            "weighted_compositional_brier": (
                accumulator["weighted_brier"]
                / weight
            ),
        }

    ranked_models = sorted(
        summary,
        key=lambda name: (
            summary[name][
                "weighted_multinomial_log_loss"
            ],
            len(MODEL_FEATURES[name]),
            name,
        ),
    )

    comparisons = {}
    if summary:
        metric = "weighted_multinomial_log_loss"
        prior = summary["league_prior"][metric]
        actual = summary["actual_allocation"][metric]
        expected = summary["expected_damage"][metric]
        combined = summary["actual_expected"][metric]
        geometry = summary[
            "actual_expected_geometry"
        ][metric]

        comparisons = {
            "actual_vs_league_relative_log_loss_improvement":
                _relative_improvement(actual, prior),
            "expected_vs_league_relative_log_loss_improvement":
                _relative_improvement(expected, prior),
            "actual_expected_vs_actual_relative_log_loss_improvement":
                _relative_improvement(combined, actual),
            "geometry_increment_relative_log_loss_improvement":
                _relative_improvement(
                    geometry,
                    combined,
                ),
        }

    return {
        "schema_version":
            "shadow_hitter_hit_type_allocation_validation_v1",
        "status": "blocked" if blockers else "ready",
        "shadow_only": True,
        "parameter_selected": False,
        "production_authority_changed": False,
        "allocation_condition": "conditional_on_hit",
        "hit_types": list(HIT_TYPES),
        "primary_metric":
            "weighted_multinomial_log_loss",
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
        "known_limitations": [
            "sprint_speed_not_stored",
            "triple_allocation_lacks_direct_speed_evidence",
        ],
    }


def _percentile(
    values: Sequence[float],
    probability: float,
) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError(
            "percentile requires at least one value"
        )
    position = (
        (len(ordered) - 1)
        * probability
    )
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return (
        ordered[lower] * (1.0 - fraction)
        + ordered[upper] * fraction
    )


def bootstrap_hitter_hit_type_allocation_differences(
    samples: Sequence[Mapping[str, Any]],
    *,
    iterations: int = 400,
    seed: int = 20260731,
    minimum_fold_samples: int = 50,
) -> dict[str, Any]:
    """Bootstrap allocation log-loss differences by player."""

    cleaned = _clean_samples(samples)
    grouped = {}
    for index, sample in enumerate(cleaned):
        cluster = sample.get("player_id")
        if cluster is None:
            cluster = ("row", index)
        grouped.setdefault(cluster, []).append(sample)

    cluster_keys = sorted(grouped, key=str)
    blockers = []

    if len(cluster_keys) < 30:
        blockers.append(
            "insufficient_player_clusters"
        )
    if iterations < 100:
        blockers.append(
            "insufficient_bootstrap_iterations"
        )

    comparisons = {
        "actual_minus_league_prior": (
            "league_prior",
            "actual_allocation",
        ),
        "expected_minus_league_prior": (
            "league_prior",
            "expected_damage",
        ),
        "expected_increment_over_actual": (
            "actual_allocation",
            "actual_expected",
        ),
        "geometry_increment_over_actual_expected": (
            "actual_expected",
            "actual_expected_geometry",
        ),
    }
    draws = {
        name: []
        for name in comparisons
    }
    successful = 0
    rng = random.Random(seed)

    if not blockers:
        for _ in range(iterations):
            selected = [
                cluster_keys[
                    rng.randrange(len(cluster_keys))
                ]
                for _ in cluster_keys
            ]
            resampled = [
                row
                for cluster in selected
                for row in grouped[cluster]
            ]
            result = (
                evaluate_hitter_hit_type_allocation_models(
                    resampled,
                    minimum_fold_samples=(
                        minimum_fold_samples
                    ),
                )
            )
            if result["status"] != "ready":
                continue

            summary = result[
                "cross_season_summary"
            ]
            metric = (
                "weighted_multinomial_log_loss"
            )
            for (
                name,
                (reference, candidate),
            ) in comparisons.items():
                draws[name].append(
                    summary[reference][metric]
                    - summary[candidate][metric]
                )
            successful += 1

    if successful < max(
        100,
        int(iterations * 0.90),
    ):
        blockers.append(
            "insufficient_successful_bootstrap_draws"
        )

    intervals = {}
    for name, values in draws.items():
        if not values:
            continue
        intervals[name] = {
            "log_loss_improvement_median":
                _percentile(values, 0.50),
            "log_loss_improvement_ci_95": {
                "lower": _percentile(
                    values,
                    0.025,
                ),
                "upper": _percentile(
                    values,
                    0.975,
                ),
            },
            "probability_of_improvement": (
                sum(
                    value > 0
                    for value in values
                )
                / len(values)
            ),
        }

    return {
        "schema_version":
            "shadow_hitter_hit_type_allocation_bootstrap_v1",
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
            "reference cross-season log loss minus "
            "candidate cross-season log loss",
        "comparisons": intervals,
        "blockers": blockers,
    }
