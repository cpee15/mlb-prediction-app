"""Shadow-only stability gate for hitter profile weight calibration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Optional


DEFAULT_STABILITY_POLICY = {
    "minimum_windows": 4,
    "minimum_seasons": 2,
    "minimum_samples_per_window": 200,
    "minimum_total_holdout_ab": 50_000,
    "maximum_global_weight_spread": 0.15,
    "maximum_split_weight_spread": 0.20,
}


def _number(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _candidate_weight(candidate: Any) -> Optional[float]:
    if not isinstance(candidate, Mapping):
        return None
    return _number(candidate.get("expected_weight"))


def _range(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {
            "weights": [],
            "minimum": None,
            "maximum": None,
            "spread": None,
        }
    minimum = min(values)
    maximum = max(values)
    return {
        "weights": list(values),
        "minimum": minimum,
        "maximum": maximum,
        "spread": maximum - minimum,
    }


def validate_shadow_hitter_weight_stability(
    audit: Mapping[str, Any],
    *,
    policy: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    """Evaluate temporal and handedness stability without selecting a parameter."""

    applied_policy = dict(DEFAULT_STABILITY_POLICY)
    applied_policy.update(dict(policy or {}))
    windows = [
        row for row in (audit.get("windows") or ())
        if isinstance(row, Mapping)
    ]
    seasons = sorted({
        int(row["season"])
        for row in windows
        if row.get("season") is not None
    })
    global_weights = [
        weight
        for row in windows
        if (weight := _candidate_weight(row.get("best_candidate"))) is not None
    ]
    split_weights: dict[str, list[float]] = {"vsR": [], "vsL": []}
    for row in windows:
        split_results = row.get("split_results") or {}
        for split in split_weights:
            split_row = split_results.get(split) or {}
            weight = _candidate_weight(split_row.get("best_candidate"))
            if weight is not None:
                split_weights[split].append(weight)

    global_range = _range(global_weights)
    split_ranges = {
        split: _range(weights)
        for split, weights in split_weights.items()
    }
    blockers: list[str] = []
    warnings: list[str] = []

    if len(windows) < int(applied_policy["minimum_windows"]):
        blockers.append("insufficient_stability_windows")
    if len(seasons) < int(applied_policy["minimum_seasons"]):
        blockers.append("insufficient_stability_seasons")
    if any(
        int(row.get("sample_count") or 0)
        < int(applied_policy["minimum_samples_per_window"])
        for row in windows
    ):
        blockers.append("insufficient_window_samples")
    if int(audit.get("holdout_ab") or 0) < int(
        applied_policy["minimum_total_holdout_ab"]
    ):
        blockers.append("insufficient_total_holdout_ab")
    if len(global_weights) != len(windows):
        blockers.append("missing_window_weight_candidates")
    elif (
        global_range["spread"]
        > float(applied_policy["maximum_global_weight_spread"])
    ):
        blockers.append("unstable_global_expected_weight")

    for split, split_range in split_ranges.items():
        if len(split_range["weights"]) != len(windows):
            blockers.append(f"missing_{split}_weight_candidates")
        elif (
            split_range["spread"]
            > float(applied_policy["maximum_split_weight_spread"])
        ):
            blockers.append(f"unstable_{split}_expected_weight")

    pooled_candidate = audit.get("pooled_candidate")
    pooled_weight = _candidate_weight(pooled_candidate)
    if pooled_weight is None:
        blockers.append("missing_pooled_candidate")
    elif pooled_weight != 0.50:
        warnings.append("pooled_candidate_differs_from_current_policy")

    return {
        "schema_version": "shadow_hitter_weight_stability_gate_v1",
        "status": "blocked" if blockers else "ready_for_selection_review",
        "shadow_only": True,
        "parameter_selected": False,
        "production_authority_changed": False,
        "candidate_expected_weight": pooled_weight,
        "candidate_actual_weight": (
            None if pooled_weight is None else 1.0 - pooled_weight
        ),
        "window_count": len(windows),
        "seasons": seasons,
        "sample_count": int(audit.get("sample_count") or 0),
        "holdout_ab": int(audit.get("holdout_ab") or 0),
        "global_weight_range": global_range,
        "split_weight_ranges": split_ranges,
        "policy": applied_policy,
        "blockers": blockers,
        "warnings": warnings,
        "decision": (
            "retain_current_policy"
            if blockers
            else "eligible_for_separate_selection_pr"
        ),
    }
