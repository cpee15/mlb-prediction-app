"""Selected shadow hitter hit-type allocation parameterization."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any


HIT_TYPES = (
    "single",
    "double",
    "triple",
    "home_run",
)
SELECTED_NON_TRIPLE_TYPES = (
    "single",
    "double",
    "home_run",
)

EXPECTED_DAMAGE_COEFFICIENTS = {
    "single": (
        0.7528494269872082,
        -1.417128517190727,
    ),
    "double": (
        0.18342632599531214,
        0.16515326588651238,
    ),
    "home_run": (
        0.045412442752847434,
        1.2559422367292983,
    ),
}

MINIMUM_EXPECTED_DAMAGE_PER_BBE = (
    0.010037499999999991
)
MAXIMUM_EXPECTED_DAMAGE_PER_BBE = (
    0.2687966791454845
)

EXPECTED_BOOTSTRAP_PROBABILITY = 1.0
EXPECTED_BOOTSTRAP_CI_95 = (
    0.006337619580733706,
    0.013231422817683957,
)


def _rate(value: Any) -> float | None:
    if isinstance(value, bool):
        return None

    try:
        result = float(value)
    except (TypeError, ValueError):
        return None

    if (
        not math.isfinite(result)
        or result < 0.0
        or result > 1.0
    ):
        return None

    return result


def _allocation(
    value: Any,
) -> dict[str, float] | None:
    if not isinstance(value, Mapping):
        return None

    parsed = {
        hit_type: _rate(
            value.get(hit_type)
        )
        for hit_type in HIT_TYPES
    }

    if any(
        item is None
        for item in parsed.values()
    ):
        return None

    total = sum(parsed.values())
    if total <= 0.0:
        return None

    return {
        hit_type:
            parsed[hit_type] / total
        for hit_type in HIT_TYPES
    }


def selected_hitter_hit_type_allocation_parameterization(
) -> dict[str, Any]:
    """Return the frozen conditional allocation contract."""

    return {
        "schema_version":
            "shadow_hitter_hit_type_allocation_parameterization_v1",
        "status": "selected",
        "shadow_only": True,
        "parameter_selected": True,
        "production_authority_changed": False,
        "allocation_condition":
            "conditional_on_hit",
        "selected_model":
            "expected_damage",
        "selected_signal":
            "expected_damage_per_bbe",
        "selected_outcomes": [
            "single",
            "double",
            "home_run",
        ],
        "mapping": {
            hit_type: {
                "intercept":
                    coefficients[0],
                "expected_damage_coefficient":
                    coefficients[1],
            }
            for hit_type, coefficients
            in EXPECTED_DAMAGE_COEFFICIENTS.items()
        },
        "normalization_policy":
            "normalize_selected_non_triples_to_remaining_mass",
        "triple_policy": {
            "selected_model_controls_triples":
                False,
            "policy":
                "retain_current_conservative_triple_probability",
            "reason":
                "direct_speed_evidence_unavailable",
        },
        "supported_input_range": {
            "minimum_expected_damage_per_bbe":
                MINIMUM_EXPECTED_DAMAGE_PER_BBE,
            "maximum_expected_damage_per_bbe":
                MAXIMUM_EXPECTED_DAMAGE_PER_BBE,
            "outside_range_policy":
                "fallback_to_actual_allocation",
        },
        "fallback": {
            "signal": "actual_allocation",
            "used_when": [
                "expected_damage_missing",
                "expected_damage_invalid",
                "expected_damage_outside_evidence_range",
                "conservative_triple_probability_missing",
                "conservative_triple_probability_invalid",
            ],
        },
        "excluded_models": [
            "actual_expected_blend",
            "actual_expected_geometry",
            "speed_based_triple_adjustment",
        ],
        "selection_evidence": {
            "sample_count": 1329,
            "holdout_hits": 19828,
            "seasons": [
                2024,
                2025,
            ],
            "expected_relative_log_loss_improvement_over_actual":
                0.0013222043312561623,
            "expected_vs_league_bootstrap_probability":
                EXPECTED_BOOTSTRAP_PROBABILITY,
            "expected_vs_league_bootstrap_ci_95": {
                "lower":
                    EXPECTED_BOOTSTRAP_CI_95[0],
                "upper":
                    EXPECTED_BOOTSTRAP_CI_95[1],
            },
            "blend_increment_probability":
                0.905,
            "blend_increment_ci_crosses_zero":
                True,
            "prediction_stability": {
                "metric":
                    "total_variation_distance",
                "median":
                    0.00765036580673064,
                "p95":
                    0.01716114609187617,
                "maximum":
                    0.04109522139859782,
            },
        },
        "activation": {
            "activation_eligible": True,
            "feature_flag_required": True,
            "shadow_canary_required": True,
            "production_enabled": False,
        },
    }


def resolve_hitter_hit_type_allocation(
    *,
    expected_damage_per_bbe: Any,
    conservative_triple_probability: Any,
    actual_allocation: Any,
) -> dict[str, Any]:
    """Resolve selected non-triples or actual fallback."""

    expected_damage = _rate(
        expected_damage_per_bbe
    )
    triple_probability = _rate(
        conservative_triple_probability
    )
    actual = _allocation(
        actual_allocation
    )

    fallback_reason = None

    if expected_damage is None:
        fallback_reason = (
            "expected_damage_missing"
            if expected_damage_per_bbe is None
            else "expected_damage_invalid"
        )
    elif not (
        MINIMUM_EXPECTED_DAMAGE_PER_BBE
        <= expected_damage
        <= MAXIMUM_EXPECTED_DAMAGE_PER_BBE
    ):
        fallback_reason = (
            "expected_damage_outside_evidence_range"
        )
    elif triple_probability is None:
        fallback_reason = (
            "conservative_triple_probability_missing"
            if conservative_triple_probability
            is None
            else
            "conservative_triple_probability_invalid"
        )
    elif triple_probability >= 1.0:
        fallback_reason = (
            "conservative_triple_probability_invalid"
        )

    if fallback_reason is not None:
        if actual is None:
            return {
                "status": "blocked",
                "allocation": None,
                "source": None,
                "fallback_used": False,
                "fallback_reason":
                    fallback_reason,
                "blockers": [
                    "actual_allocation_fallback_unavailable",
                ],
                "parameter_selected": True,
                "production_authority_changed": False,
            }

        return {
            "status": "ready",
            "allocation": actual,
            "source": "actual_allocation",
            "fallback_used": True,
            "fallback_reason":
                fallback_reason,
            "blockers": [],
            "parameter_selected": True,
            "production_authority_changed": False,
        }

    raw = {
        hit_type: max(
            coefficients[0]
            + coefficients[1]
            * expected_damage,
            1e-12,
        )
        for hit_type, coefficients
        in EXPECTED_DAMAGE_COEFFICIENTS.items()
    }
    raw_total = sum(raw.values())
    remaining_mass = (
        1.0 - triple_probability
    )

    allocation = {
        hit_type:
            remaining_mass
            * raw[hit_type]
            / raw_total
        for hit_type
        in SELECTED_NON_TRIPLE_TYPES
    }
    allocation["triple"] = (
        triple_probability
    )
    allocation = {
        hit_type: allocation[hit_type]
        for hit_type in HIT_TYPES
    }

    return {
        "status": "ready",
        "allocation": allocation,
        "source":
            "expected_damage_with_conservative_triple",
        "fallback_used": False,
        "fallback_reason": None,
        "blockers": [],
        "parameter_selected": True,
        "production_authority_changed": False,
    }
