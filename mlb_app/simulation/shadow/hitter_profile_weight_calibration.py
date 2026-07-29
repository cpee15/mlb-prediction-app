"""Leakage-safe calibration of actual batting average and xBA weights."""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Iterable, Mapping, Optional, Sequence

SCHEMA_VERSION = "shadow_hitter_profile_weight_calibration_v1"
DEFAULT_EXPECTED_WEIGHTS = tuple(index / 20 for index in range(21))
MIN_HOLDOUT_AB = 20
EPSILON = 1e-9


def _number(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _prepare_samples(samples: Iterable[Mapping[str, Any]]) -> tuple[list[dict], list[dict]]:
    ready = []
    rejected = []
    for index, raw in enumerate(samples):
        actual = _number(raw.get("actual_batting_avg"))
        expected = _number(raw.get("expected_xba"))
        hits = _number(raw.get("holdout_hits"))
        at_bats = _number(raw.get("holdout_ab"))
        season = raw.get("season")
        split = raw.get("split")
        reasons = []
        if actual is None or not 0.0 <= actual <= 1.0:
            reasons.append("invalid_actual_batting_avg")
        if expected is None or not 0.0 <= expected <= 1.0:
            reasons.append("invalid_expected_xba")
        if at_bats is None or at_bats < MIN_HOLDOUT_AB:
            reasons.append("insufficient_holdout_ab")
        if hits is None or hits < 0 or (at_bats is not None and hits > at_bats):
            reasons.append("invalid_holdout_hits")
        try:
            season = int(season)
        except (TypeError, ValueError):
            reasons.append("invalid_season")
        if split not in {"vsR", "vsL"}:
            reasons.append("invalid_split")
        if reasons:
            rejected.append({"index": index, "reasons": reasons})
            continue
        ready.append(
            {
                "player_id": raw.get("player_id"),
                "season": season,
                "split": split,
                "actual_batting_avg": actual,
                "expected_xba": expected,
                "holdout_hits": int(hits),
                "holdout_ab": int(at_bats),
            }
        )
    return ready, rejected


def _score(samples: Sequence[Mapping[str, Any]], expected_weight: float) -> dict[str, Any]:
    log_loss_sum = 0.0
    brier_sum = 0.0
    absolute_error_sum = 0.0
    total_ab = 0
    for sample in samples:
        probability = (
            sample["actual_batting_avg"] * (1.0 - expected_weight)
            + sample["expected_xba"] * expected_weight
        )
        probability = min(max(probability, EPSILON), 1.0 - EPSILON)
        hits = sample["holdout_hits"]
        outs = sample["holdout_ab"] - hits
        total_ab += sample["holdout_ab"]
        log_loss_sum += -(
            hits * math.log(probability)
            + outs * math.log(1.0 - probability)
        )
        brier_sum += hits * ((1.0 - probability) ** 2)
        brier_sum += outs * (probability**2)
        absolute_error_sum += sample["holdout_ab"] * abs(
            probability - (hits / sample["holdout_ab"])
        )
    return {
        "sample_count": len(samples),
        "holdout_ab": total_ab,
        "log_loss": log_loss_sum / total_ab if total_ab else None,
        "brier_score": brier_sum / total_ab if total_ab else None,
        "weighted_absolute_error": (
            absolute_error_sum / total_ab if total_ab else None
        ),
    }


def _rank(grid: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    return min(
        grid,
        key=lambda row: (
            row["scores"]["log_loss"],
            row["scores"]["brier_score"],
            abs(row["expected_weight"] - 0.50),
        ),
    )


def _grid(samples: Sequence[Mapping[str, Any]], weights: Sequence[float]) -> list[dict]:
    return [
        {
            "expected_weight": weight,
            "actual_weight": 1.0 - weight,
            "scores": _score(samples, weight),
        }
        for weight in weights
    ]


def calibrate_shadow_hitter_profile_weights(
    samples: Iterable[Mapping[str, Any]],
    *,
    candidate_expected_weights: Sequence[float] = DEFAULT_EXPECTED_WEIGHTS,
) -> dict[str, Any]:
    """Evaluate candidate weights with season-disjoint cross-validation."""

    weights = tuple(float(weight) for weight in candidate_expected_weights)
    if not weights or any(
        not math.isfinite(weight) or not 0.0 <= weight <= 1.0
        for weight in weights
    ):
        raise ValueError("candidate weights must be finite values in [0, 1]")
    if len(set(weights)) != len(weights):
        raise ValueError("candidate weights must be unique")

    ready, rejected = _prepare_samples(samples)
    seasons = sorted({sample["season"] for sample in ready})
    blockers = []
    if not ready:
        blockers.append("no_eligible_calibration_samples")
    if len(seasons) < 2:
        blockers.append("insufficient_cross_season_coverage")
    if blockers:
        return {
            "schema_version": SCHEMA_VERSION,
            "status": "blocked",
            "shadow_only": True,
            "production_authority_changed": False,
            "parameter_selected": False,
            "blockers": blockers,
            "eligible_sample_count": len(ready),
            "rejected_samples": rejected,
        }

    pooled_grid = _grid(ready, weights)
    pooled_best = _rank(pooled_grid)
    folds = []
    selected_weights = []
    for validation_season in seasons:
        training = [
            sample for sample in ready
            if sample["season"] != validation_season
        ]
        validation = [
            sample for sample in ready
            if sample["season"] == validation_season
        ]
        training_grid = _grid(training, weights)
        training_best = _rank(training_grid)
        selected_weight = training_best["expected_weight"]
        selected_weights.append(selected_weight)
        folds.append(
            {
                "validation_season": validation_season,
                "training_seasons": sorted({
                    sample["season"] for sample in training
                }),
                "selected_expected_weight": selected_weight,
                "selected_actual_weight": 1.0 - selected_weight,
                "training_scores": training_best["scores"],
                "validation_scores": _score(validation, selected_weight),
                "candidate_reselected_on_validation": False,
            }
        )

    by_split = {}
    for split in ("vsR", "vsL"):
        subset = [sample for sample in ready if sample["split"] == split]
        split_grid = _grid(subset, weights)
        by_split[split] = {
            "sample_count": len(subset),
            "best_candidate": _rank(split_grid),
        }

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ready",
        "shadow_only": True,
        "production_authority_changed": False,
        "parameter_selected": False,
        "selection_role": "candidate_evidence_only",
        "eligible_sample_count": len(ready),
        "rejected_sample_count": len(rejected),
        "rejected_samples": rejected,
        "seasons": seasons,
        "candidate_expected_weights": list(weights),
        "pooled_candidate": pooled_best,
        "pooled_grid": pooled_grid,
        "cross_season_folds": folds,
        "cross_season_weight_range": {
            "minimum": min(selected_weights),
            "maximum": max(selected_weights),
            "spread": max(selected_weights) - min(selected_weights),
        },
        "split_diagnostics": by_split,
        "objective": {
            "primary": "plate_appearance_weighted_binomial_log_loss",
            "supporting": [
                "plate_appearance_weighted_brier_score",
                "plate_appearance_weighted_absolute_error",
            ],
            "target": "future_holdout_hit_per_at_bat",
        },
    }
