"""Selected shadow hitter power-skill parameterization."""

from __future__ import annotations

import math
from typing import Any


INTERCEPT = 0.08025334564396619
EXPECTED_DAMAGE_COEFFICIENT = (
    1.5145365016897803
)

MINIMUM_EXPECTED_DAMAGE_PER_AB = (
    0.005903692307692304
)
MAXIMUM_EXPECTED_DAMAGE_PER_AB = (
    0.1880161714254247
)

FOLD_COEFFICIENTS = (
    (
        0.08328975015864178,
        1.462696501756593,
    ),
    (
        0.07641492450475228,
        1.5771476716029722,
    ),
)

BOOTSTRAP_PROBABILITY_OF_IMPROVEMENT = 1.0
BOOTSTRAP_CI_95 = (
    0.00030631363360671446,
    0.0007716463476866683,
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


def selected_hitter_power_skill_parameterization(
) -> dict[str, Any]:
    """Return the frozen shadow-only power mapping."""

    return {
        "schema_version":
            "shadow_hitter_power_skill_parameterization_v1",
        "status": "selected",
        "shadow_only": True,
        "parameter_selected": True,
        "production_authority_changed": False,
        "selected_signal":
            "expected_damage_per_ab",
        "source_denominator":
            "at_bats",
        "target":
            "isolated_power",
        "target_denominator":
            "at_bats",
        "mapping": {
            "formula":
                "iso = intercept"
                " + expected_damage_coefficient"
                " * expected_damage_per_ab",
            "intercept": INTERCEPT,
            "expected_damage_coefficient":
                EXPECTED_DAMAGE_COEFFICIENT,
            "output_clamp": {
                "minimum": 0.0,
                "maximum": 1.0,
            },
        },
        "supported_input_range": {
            "minimum_expected_damage_per_ab":
                MINIMUM_EXPECTED_DAMAGE_PER_AB,
            "maximum_expected_damage_per_ab":
                MAXIMUM_EXPECTED_DAMAGE_PER_AB,
            "outside_range_policy":
                "fallback_to_actual_iso",
        },
        "fallback": {
            "signal": "actual_iso",
            "used_when": [
                "expected_damage_missing",
                "expected_damage_invalid",
                "expected_damage_outside_evidence_range",
            ],
        },
        "excluded_features": [
            "actual_iso_blend",
            "hard_hit_rate_increment",
            "barrel_proxy_increment",
            "full_auxiliary_model",
        ],
        "selection_evidence": {
            "sample_count": 1867,
            "holdout_ab": 89625,
            "seasons": [
                2024,
                2025,
            ],
            "relative_mse_improvement_over_actual_iso":
                0.058420323214843806,
            "actual_iso_blend_relative_improvement":
                -0.0036500125063122315,
            "bootstrap_probability_of_improvement":
                BOOTSTRAP_PROBABILITY_OF_IMPROVEMENT,
            "bootstrap_mse_improvement_ci_95": {
                "lower": BOOTSTRAP_CI_95[0],
                "upper": BOOTSTRAP_CI_95[1],
            },
            "fold_coefficients": [
                list(coefficients)
                for coefficients
                in FOLD_COEFFICIENTS
            ],
            "coefficient_stability": {
                "intercept_absolute_spread":
                    0.006874825653889499,
                "relative_slope_spread":
                    0.07530068208719427,
                "all_fold_slopes_positive":
                    True,
            },
            "prediction_stability": {
                "median_spread":
                    0.002043936385427353,
                "p95_spread":
                    0.005938486968585549,
                "maximum_spread":
                    0.014643845115787757,
            },
        },
        "activation": {
            "activation_eligible": True,
            "feature_flag_required": True,
            "shadow_canary_required": True,
            "production_enabled": False,
        },
    }


def resolve_hitter_iso(
    *,
    expected_damage_per_ab: Any,
    actual_iso: Any,
) -> dict[str, Any]:
    """Resolve selected expected damage or actual-ISO fallback."""

    expected_damage = _rate(
        expected_damage_per_ab
    )
    actual = _rate(actual_iso)

    fallback_reason = None

    if expected_damage is None:
        fallback_reason = (
            "expected_damage_missing"
            if expected_damage_per_ab is None
            else "expected_damage_invalid"
        )
    elif not (
        MINIMUM_EXPECTED_DAMAGE_PER_AB
        <= expected_damage
        <= MAXIMUM_EXPECTED_DAMAGE_PER_AB
    ):
        fallback_reason = (
            "expected_damage_outside_evidence_range"
        )

    if fallback_reason is not None:
        if actual is None:
            return {
                "status": "blocked",
                "iso": None,
                "source": None,
                "fallback_used": False,
                "fallback_reason":
                    fallback_reason,
                "blockers": [
                    "actual_iso_fallback_unavailable",
                ],
                "parameter_selected": True,
                "production_authority_changed": False,
            }

        return {
            "status": "ready",
            "iso": actual,
            "source": "actual_iso",
            "fallback_used": True,
            "fallback_reason":
                fallback_reason,
            "blockers": [],
            "parameter_selected": True,
            "production_authority_changed": False,
        }

    mapped = (
        INTERCEPT
        + EXPECTED_DAMAGE_COEFFICIENT
        * expected_damage
    )
    mapped = max(
        0.0,
        min(1.0, mapped),
    )

    return {
        "status": "ready",
        "iso": mapped,
        "source":
            "expected_damage_per_ab",
        "fallback_used": False,
        "fallback_reason": None,
        "blockers": [],
        "parameter_selected": True,
        "production_authority_changed": False,
    }
