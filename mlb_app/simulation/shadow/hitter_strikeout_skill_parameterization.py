"""Selected shadow hitter strikeout-skill parameterization."""

from __future__ import annotations

import math
from typing import Any


INTERCEPT = 0.04501320822630234
ACTUAL_K_COEFFICIENT = 0.4339959775511906
WHIFF_COEFFICIENT = 0.34344736989394414

MINIMUM_ACTUAL_K_RATE = (
    0.013513513513513514
)
MAXIMUM_ACTUAL_K_RATE = (
    0.4888888888888889
)
MINIMUM_WHIFF_RATE = (
    0.02857142857142857
)
MAXIMUM_WHIFF_RATE = (
    0.4946236559139785
)

FOLD_COEFFICIENTS = (
    (
        0.04058515695596576,
        0.3922078243165415,
        0.3973261085148262,
    ),
    (
        0.05029054609184294,
        0.4771567108488448,
        0.284833397731686,
    ),
)

BOOTSTRAP_PROBABILITY_OF_IMPROVEMENT = (
    0.9975
)
BOOTSTRAP_CI_95 = (
    5.427378025309307e-05,
    0.0002162508715375169,
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


def selected_hitter_strikeout_skill_parameterization(
) -> dict[str, Any]:
    """Return the frozen shadow-only strikeout mapping."""

    return {
        "schema_version":
            "shadow_hitter_strikeout_skill_parameterization_v1",
        "status": "selected",
        "shadow_only": True,
        "parameter_selected": True,
        "production_authority_changed": False,
        "selected_signals": [
            "actual_strikeout_rate",
            "whiff_rate",
        ],
        "source_denominators": {
            "actual_strikeout_rate":
                "plate_appearances",
            "whiff_rate": "swings",
        },
        "target_denominator":
            "plate_appearances",
        "mapping": {
            "formula":
                "k_rate = intercept"
                " + actual_k_coefficient * actual_k_rate"
                " + whiff_coefficient * whiff_rate",
            "intercept": INTERCEPT,
            "actual_k_coefficient":
                ACTUAL_K_COEFFICIENT,
            "whiff_coefficient":
                WHIFF_COEFFICIENT,
            "output_clamp": {
                "minimum": 0.0,
                "maximum": 1.0,
            },
        },
        "supported_input_ranges": {
            "actual_k_rate": {
                "minimum":
                    MINIMUM_ACTUAL_K_RATE,
                "maximum":
                    MAXIMUM_ACTUAL_K_RATE,
            },
            "whiff_rate": {
                "minimum":
                    MINIMUM_WHIFF_RATE,
                "maximum":
                    MAXIMUM_WHIFF_RATE,
            },
            "outside_range_policy":
                "fallback_to_actual_strikeout_rate",
        },
        "fallback": {
            "signal":
                "actual_strikeout_rate",
            "used_when": [
                "actual_k_rate_missing",
                "actual_k_rate_invalid",
                "actual_k_rate_outside_evidence_range",
                "whiff_rate_missing",
                "whiff_rate_invalid",
                "whiff_rate_outside_evidence_range",
            ],
        },
        "excluded_features": [
            "whiff_rate_alone",
            "called_strike_rate_increment",
            "swinging_strike_rate_increment",
            "full_auxiliary_model",
            "contact_rate_redundant_with_whiff_rate",
        ],
        "selection_evidence": {
            "sample_count": 2283,
            "holdout_pa": 112835,
            "seasons": [
                2024,
                2025,
            ],
            "blend_relative_mse_improvement":
                0.031632504725093616,
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
                    0.00970538913587718,
                "actual_k_relative_spread":
                    0.19542754068324814,
                "whiff_relative_spread":
                    0.32981351063218545,
                "all_non_intercept_signs_stable":
                    True,
            },
            "prediction_stability": {
                "median_spread":
                    0.0034591149395032383,
                "p95_spread":
                    0.010033095014830904,
                "maximum_spread":
                    0.020210620182578576,
            },
            "selection_basis":
                "stable_cross_season_predictions",
        },
        "activation": {
            "activation_eligible": True,
            "feature_flag_required": True,
            "shadow_canary_required": True,
            "production_enabled": False,
        },
    }


def resolve_hitter_strikeout_rate(
    *,
    actual_k_rate: Any,
    whiff_rate: Any,
) -> dict[str, Any]:
    """Resolve the selected mapping or actual-K fallback."""

    actual_k = _rate(actual_k_rate)
    whiff = _rate(whiff_rate)

    fallback_reason = None

    if actual_k is None:
        fallback_reason = (
            "actual_k_rate_missing"
            if actual_k_rate is None
            else "actual_k_rate_invalid"
        )
    elif not (
        MINIMUM_ACTUAL_K_RATE
        <= actual_k
        <= MAXIMUM_ACTUAL_K_RATE
    ):
        fallback_reason = (
            "actual_k_rate_outside_evidence_range"
        )
    elif whiff is None:
        fallback_reason = (
            "whiff_rate_missing"
            if whiff_rate is None
            else "whiff_rate_invalid"
        )
    elif not (
        MINIMUM_WHIFF_RATE
        <= whiff
        <= MAXIMUM_WHIFF_RATE
    ):
        fallback_reason = (
            "whiff_rate_outside_evidence_range"
        )

    if fallback_reason is not None:
        if actual_k is None:
            return {
                "status": "blocked",
                "strikeout_rate": None,
                "source": None,
                "fallback_used": False,
                "fallback_reason":
                    fallback_reason,
                "blockers": [
                    "actual_strikeout_rate_fallback_unavailable",
                ],
                "parameter_selected": True,
                "production_authority_changed": False,
            }

        return {
            "status": "ready",
            "strikeout_rate": actual_k,
            "source":
                "actual_strikeout_rate",
            "fallback_used": True,
            "fallback_reason":
                fallback_reason,
            "blockers": [],
            "parameter_selected": True,
            "production_authority_changed": False,
        }

    mapped = (
        INTERCEPT
        + ACTUAL_K_COEFFICIENT * actual_k
        + WHIFF_COEFFICIENT * whiff
    )
    mapped = max(
        0.0,
        min(1.0, mapped),
    )

    return {
        "status": "ready",
        "strikeout_rate": mapped,
        "source":
            "actual_strikeout_rate_plus_whiff_rate",
        "fallback_used": False,
        "fallback_reason": None,
        "blockers": [],
        "parameter_selected": True,
        "production_authority_changed": False,
    }
