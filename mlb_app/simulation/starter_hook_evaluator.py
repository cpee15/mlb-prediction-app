"""
Pure deterministic starter-hook state evaluator.

This module has no production simulation authority. It evaluates a supplied
starter/game-state snapshot and returns an auditable recommendation only.

It does not:
- alter starter innings;
- select a reliever;
- change bullpen probabilities;
- change plate-appearance probabilities;
- change simulation scores or win probabilities;
- activate pitching-plan classification behavior.
"""

from __future__ import annotations

import math
from copy import deepcopy
from typing import Any, Dict, Iterable


STARTER_HOOK_EVALUATOR_VERSION = (
    "starter-hook-evaluator-v1"
)

REQUIRED_STATE_FIELDS = (
    "inning",
    "outs",
    "base_state",
    "batters_faced",
    "pitch_count_estimate",
    "times_through_order",
    "runs_allowed",
    "recent_traffic_index",
    "score_margin",
    "leverage_proxy",
    "starter_quality_score",
    "expected_starter_innings",
    "fatigue_index",
)

OPTIONAL_STATE_FIELDS = (
    "bullpen_availability",
    "pitching_plan",
)

EVALUATOR_OUTPUT_FIELDS = (
    "decision",
    "pull_probability",
    "trigger_reasons",
    "state_completeness",
    "fallback_used",
    "fallback_reason",
    "behavioral_effect",
    "canonical_probability_authority_changed",
    "production_activation",
)


def _is_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _clamp(
    value: float,
    lower: float,
    upper: float,
) -> float:
    return max(
        lower,
        min(upper, value),
    )


def _missing_fields(
    state: Dict[str, Any],
    required_fields: Iterable[str],
) -> list[str]:
    return [
        field
        for field in required_fields
        if field not in state
        or state.get(field) is None
    ]


def validate_starter_hook_state(
    state: Any,
) -> Dict[str, Any]:
    """
    Validate the starter-hook state contract.

    Validation is deterministic and does not mutate the supplied payload.
    """

    errors: list[str] = []
    warnings: list[str] = []

    if not isinstance(state, dict):
        return {
            "valid": False,
            "errors": [
                "state_must_be_object",
            ],
            "warnings": [],
            "missing_fields": list(
                REQUIRED_STATE_FIELDS
            ),
            "state_completeness": 0.0,
        }

    missing = _missing_fields(
        state,
        REQUIRED_STATE_FIELDS,
    )

    for field in missing:
        errors.append(
            f"missing_required_field:{field}"
        )

    numeric_constraints = {
        "inning": (1.0, 20.0),
        "outs": (0.0, 2.0),
        "batters_faced": (0.0, 100.0),
        "pitch_count_estimate": (
            0.0,
            250.0,
        ),
        "times_through_order": (
            0.0,
            10.0,
        ),
        "runs_allowed": (0.0, 30.0),
        "recent_traffic_index": (
            0.0,
            1.0,
        ),
        "score_margin": (-30.0, 30.0),
        "leverage_proxy": (0.0, 1.0),
        "starter_quality_score": (
            -1.0,
            1.0,
        ),
        "expected_starter_innings": (
            0.1,
            9.0,
        ),
        "fatigue_index": (0.0, 1.0),
    }

    integer_fields = {
        "inning",
        "outs",
        "batters_faced",
        "runs_allowed",
        "score_margin",
    }

    for field, bounds in (
        numeric_constraints.items()
    ):
        if field in missing:
            continue

        value = state.get(field)

        if not _is_number(value):
            errors.append(
                f"field_must_be_numeric:{field}"
            )
            continue

        numeric_value = float(value)
        lower, upper = bounds

        if (
            numeric_value < lower
            or numeric_value > upper
        ):
            errors.append(
                f"field_out_of_range:{field}"
            )

        if (
            field in integer_fields
            and not numeric_value.is_integer()
        ):
            errors.append(
                f"field_must_be_integer:{field}"
            )

    if (
        "base_state" not in missing
        and not isinstance(
            state.get("base_state"),
            dict,
        )
    ):
        errors.append(
            "field_must_be_object:base_state"
        )

    bullpen_availability = state.get(
        "bullpen_availability"
    )

    if (
        bullpen_availability is not None
        and not isinstance(
            bullpen_availability,
            dict,
        )
    ):
        warnings.append(
            "optional_field_ignored:"
            "bullpen_availability"
        )

    pitching_plan = state.get(
        "pitching_plan"
    )

    if (
        pitching_plan is not None
        and not isinstance(
            pitching_plan,
            dict,
        )
    ):
        warnings.append(
            "optional_field_ignored:"
            "pitching_plan"
        )

    present_required = (
        len(REQUIRED_STATE_FIELDS)
        - len(missing)
    )

    completeness = round(
        present_required
        / len(REQUIRED_STATE_FIELDS),
        4,
    )

    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "missing_fields": missing,
        "state_completeness": completeness,
    }


def _add_reason(
    reasons: list[str],
    reason: str,
) -> None:
    if reason not in reasons:
        reasons.append(reason)


def evaluate_starter_hook(
    state: Any,
) -> Dict[str, Any]:
    """
    Return a deterministic, auditable starter-hook recommendation.

    The recommendation is diagnostic only. It is not wired to the production
    game simulator and cannot alter model outputs.
    """

    state_snapshot = deepcopy(state)

    validation = validate_starter_hook_state(
        state_snapshot
    )

    if validation["valid"] is not True:
        return {
            "decision": "insufficient_state",
            "pull_probability": 0.0,
            "trigger_reasons": [
                "insufficient_state",
            ],
            "state_completeness": (
                validation[
                    "state_completeness"
                ]
            ),
            "fallback_used": True,
            "fallback_reason": (
                "invalid_or_incomplete_state"
            ),
            "behavioral_effect": "none",
            (
                "canonical_probability_"
                "authority_changed"
            ): False,
            "production_activation": False,
        }

    inning = int(state_snapshot["inning"])
    batters_faced = int(
        state_snapshot["batters_faced"]
    )
    pitch_count = float(
        state_snapshot[
            "pitch_count_estimate"
        ]
    )
    times_through = float(
        state_snapshot[
            "times_through_order"
        ]
    )
    runs_allowed = int(
        state_snapshot["runs_allowed"]
    )
    traffic = float(
        state_snapshot[
            "recent_traffic_index"
        ]
    )
    score_margin = int(
        state_snapshot["score_margin"]
    )
    leverage = float(
        state_snapshot["leverage_proxy"]
    )
    quality = float(
        state_snapshot[
            "starter_quality_score"
        ]
    )
    expected_innings = float(
        state_snapshot[
            "expected_starter_innings"
        ]
    )
    fatigue = float(
        state_snapshot["fatigue_index"]
    )

    pull_score = 0.05
    reasons: list[str] = []

    if pitch_count >= 105:
        pull_score += 0.55
        _add_reason(
            reasons,
            "critical_pitch_count",
        )
    elif pitch_count >= 95:
        pull_score += 0.38
        _add_reason(
            reasons,
            "high_pitch_count",
        )
    elif pitch_count >= 85:
        pull_score += 0.20
        _add_reason(
            reasons,
            "elevated_pitch_count",
        )

    if batters_faced >= 27:
        pull_score += 0.25
        _add_reason(
            reasons,
            "high_batters_faced",
        )
    elif batters_faced >= 24:
        pull_score += 0.15
        _add_reason(
            reasons,
            "elevated_batters_faced",
        )

    if times_through >= 3.0:
        pull_score += 0.42
        _add_reason(
            reasons,
            "third_time_through_order",
        )
    elif times_through >= 2.5:
        pull_score += 0.20
        _add_reason(
            reasons,
            "approaching_third_time_through",
        )

    if runs_allowed >= 5:
        pull_score += 0.45
        _add_reason(
            reasons,
            "five_plus_runs_allowed",
        )
    elif runs_allowed >= 4:
        pull_score += 0.30
        _add_reason(
            reasons,
            "four_runs_allowed",
        )
    elif runs_allowed >= 3:
        pull_score += 0.15
        _add_reason(
            reasons,
            "three_runs_allowed",
        )

    if traffic >= 0.75:
        pull_score += 0.35
        _add_reason(
            reasons,
            "heavy_recent_traffic",
        )
    elif traffic >= 0.50:
        pull_score += 0.18
        _add_reason(
            reasons,
            "elevated_recent_traffic",
        )

    if fatigue >= 0.80:
        pull_score += 0.38
        _add_reason(
            reasons,
            "critical_fatigue",
        )
    elif fatigue >= 0.65:
        pull_score += 0.22
        _add_reason(
            reasons,
            "high_fatigue",
        )
    elif fatigue >= 0.50:
        pull_score += 0.10
        _add_reason(
            reasons,
            "moderate_fatigue",
        )

    if inning >= 7:
        pull_score += 0.18
        _add_reason(
            reasons,
            "late_inning",
        )
    elif inning >= 6:
        pull_score += 0.08
        _add_reason(
            reasons,
            "middle_late_inning",
        )

    if inning >= math.ceil(
        expected_innings
    ):
        pull_score += 0.12
        _add_reason(
            reasons,
            "expected_innings_reached",
        )

    if (
        inning >= 6
        and abs(score_margin) <= 2
        and leverage >= 0.75
    ):
        pull_score += 0.25
        _add_reason(
            reasons,
            "late_high_leverage_close_game",
        )

    if (
        inning >= 6
        and abs(score_margin) >= 6
        and leverage <= 0.30
    ):
        pull_score -= 0.28
        _add_reason(
            reasons,
            "low_leverage_blowout_extension",
        )

    if quality >= 0.55:
        pull_score -= 0.18
        _add_reason(
            reasons,
            "strong_starter_retention",
        )
    elif quality <= -0.55:
        pull_score += 0.20
        _add_reason(
            reasons,
            "weak_starter_short_leash",
        )

    pull_probability = round(
        _clamp(
            pull_score,
            0.0,
            1.0,
        ),
        4,
    )

    decision = (
        "pull"
        if pull_probability >= 0.50
        else "keep"
    )

    if not reasons:
        reasons = [
            "no_pull_threshold_reached",
        ]

    return {
        "decision": decision,
        "pull_probability": (
            pull_probability
        ),
        "trigger_reasons": reasons,
        "state_completeness": (
            validation[
                "state_completeness"
            ]
        ),
        "fallback_used": False,
        "fallback_reason": None,
        "behavioral_effect": "none",
        (
            "canonical_probability_"
            "authority_changed"
        ): False,
        "production_activation": False,
    }


def validate_starter_hook_evaluation(
    payload: Any,
) -> Dict[str, Any]:
    """
    Validate the evaluator output contract.
    """

    errors: list[str] = []

    if not isinstance(payload, dict):
        return {
            "valid": False,
            "errors": [
                "evaluation_must_be_object",
            ],
        }

    if set(payload) != set(
        EVALUATOR_OUTPUT_FIELDS
    ):
        errors.append(
            "evaluation_field_contract_mismatch"
        )

    if payload.get("decision") not in {
        "keep",
        "pull",
        "insufficient_state",
    }:
        errors.append(
            "invalid_decision"
        )

    probability = payload.get(
        "pull_probability"
    )

    if (
        not _is_number(probability)
        or float(probability) < 0.0
        or float(probability) > 1.0
    ):
        errors.append(
            "invalid_pull_probability"
        )

    if not isinstance(
        payload.get("trigger_reasons"),
        list,
    ):
        errors.append(
            "trigger_reasons_must_be_array"
        )

    completeness = payload.get(
        "state_completeness"
    )

    if (
        not _is_number(completeness)
        or float(completeness) < 0.0
        or float(completeness) > 1.0
    ):
        errors.append(
            "invalid_state_completeness"
        )

    if not isinstance(
        payload.get("fallback_used"),
        bool,
    ):
        errors.append(
            "fallback_used_must_be_boolean"
        )

    if (
        payload.get("behavioral_effect")
        != "none"
    ):
        errors.append(
            "behavioral_effect_must_be_none"
        )

    if (
        payload.get(
            (
                "canonical_probability_"
                "authority_changed"
            )
        )
        is not False
    ):
        errors.append(
            "canonical_probability_authority_changed"
        )

    if (
        payload.get(
            "production_activation"
        )
        is not False
    ):
        errors.append(
            "production_activation_must_be_false"
        )

    return {
        "valid": not errors,
        "errors": errors,
    }
