"""Selected shadow hitter walk-skill parameterization."""

from __future__ import annotations

import math
from typing import Any


INTERCEPT = -0.12043625608007737
SLOPE = 0.5605462949774747

MINIMUM_CALLED_BALL_RATE = (
    0.2383177570093458
)
MAXIMUM_CALLED_BALL_RATE = 0.5

FOLD_COEFFICIENTS = (
    (
        -0.1316003330531189,
        0.5914475805750489,
    ),
    (
        -0.10621455198132441,
        0.5205614482626899,
    ),
)

RELATIVE_SLOPE_SPREAD = (
    0.12749200856120468
)
MAXIMUM_PREDICTION_SPREAD = (
    0.01005728508438497
)

BOOTSTRAP_PROBABILITY_OF_IMPROVEMENT = 0.995
BOOTSTRAP_CI_95 = (
    9.329340466915365e-06,
    9.386277643518045e-05,
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


def selected_hitter_walk_skill_parameterization(
) -> dict[str, Any]:
    """Return the frozen shadow-only walk mapping."""

    return {
        "schema_version":
            "shadow_hitter_walk_skill_parameterization_v1",
        "status": "selected",
        "shadow_only": True,
        "parameter_selected": True,
        "production_authority_changed": False,
        "selected_signal": "called_ball_rate",
        "source_denominator": "pitches",
        "target_denominator": "plate_appearances",
        "mapping": {
            "formula":
                "bb_rate = intercept + slope * called_ball_rate",
            "intercept": INTERCEPT,
            "slope": SLOPE,
            "output_clamp": {
                "minimum": 0.0,
                "maximum": 1.0,
            },
        },
        "supported_input_range": {
            "minimum_called_ball_rate":
                MINIMUM_CALLED_BALL_RATE,
            "maximum_called_ball_rate":
                MAXIMUM_CALLED_BALL_RATE,
            "outside_range_policy":
                "fallback_to_actual_walk_rate",
        },
        "fallback": {
            "signal": "actual_walk_rate",
            "used_when": [
                "called_ball_rate_missing",
                "called_ball_rate_invalid",
                "called_ball_rate_outside_evidence_range",
            ],
        },
        "excluded_features": [
            "actual_walk_rate_blend",
            "take_rate",
            "called_strike_rate",
            "full_auxiliary_model",
            "chase_rate",
        ],
        "selection_evidence": {
            "sample_count": 2289,
            "holdout_pa": 113016,
            "seasons": [
                2024,
                2025,
            ],
            "relative_mse_improvement_over_actual_bb":
                0.027927450312243456,
            "bootstrap_probability_of_improvement":
                BOOTSTRAP_PROBABILITY_OF_IMPROVEMENT,
            "bootstrap_mse_improvement_ci_95": {
                "lower": BOOTSTRAP_CI_95[0],
                "upper": BOOTSTRAP_CI_95[1],
            },
            "fold_coefficients": [
                list(coefficients)
                for coefficients in FOLD_COEFFICIENTS
            ],
            "relative_slope_spread":
                RELATIVE_SLOPE_SPREAD,
            "maximum_prediction_spread":
                MAXIMUM_PREDICTION_SPREAD,
            "selection_gates": {
                "validation_ready": True,
                "bootstrap_ready": True,
                "bootstrap_probability_at_least_0_95":
                    True,
                "bootstrap_interval_fully_positive":
                    True,
                "all_fold_slopes_positive": True,
                "relative_slope_spread_at_most_0_20":
                    True,
                "maximum_prediction_spread_at_most_0_02":
                    True,
            },
        },
        "activation": {
            "activation_eligible": True,
            "feature_flag_required": True,
            "shadow_canary_required": True,
            "production_enabled": False,
        },
    }


def resolve_hitter_walk_rate(
    *,
    called_ball_rate: Any,
    actual_walk_rate: Any,
) -> dict[str, Any]:
    """Resolve the selected mapping or its explicit fallback."""

    called_ball = _rate(called_ball_rate)
    actual_walk = _rate(actual_walk_rate)

    fallback_reason = None

    if called_ball is None:
        fallback_reason = (
            "called_ball_rate_missing"
            if called_ball_rate is None
            else "called_ball_rate_invalid"
        )
    elif not (
        MINIMUM_CALLED_BALL_RATE
        <= called_ball
        <= MAXIMUM_CALLED_BALL_RATE
    ):
        fallback_reason = (
            "called_ball_rate_outside_evidence_range"
        )

    if fallback_reason is not None:
        if actual_walk is None:
            return {
                "status": "blocked",
                "walk_rate": None,
                "source": None,
                "fallback_used": False,
                "fallback_reason":
                    fallback_reason,
                "blockers": [
                    "actual_walk_rate_fallback_unavailable",
                ],
                "parameter_selected": True,
                "production_authority_changed": False,
            }

        return {
            "status": "ready",
            "walk_rate": actual_walk,
            "source": "actual_walk_rate",
            "fallback_used": True,
            "fallback_reason":
                fallback_reason,
            "blockers": [],
            "parameter_selected": True,
            "production_authority_changed": False,
        }

    mapped = (
        INTERCEPT
        + SLOPE * called_ball
    )
    mapped = max(
        0.0,
        min(1.0, mapped),
    )

    return {
        "status": "ready",
        "walk_rate": mapped,
        "source": "called_ball_rate",
        "fallback_used": False,
        "fallback_reason": None,
        "blockers": [],
        "parameter_selected": True,
        "production_authority_changed": False,
    }
