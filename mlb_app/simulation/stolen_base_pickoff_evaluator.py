"""
Pure deterministic stolen-base and pickoff-state evaluator.

This module evaluates candidate baserunning decisions only. It does not:

- sample random outcomes;
- mutate base or out state;
- create steal attempts;
- create pickoff attempts;
- change simulation behavior;
- change canonical probability authority;
- activate production behavior.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable


REQUIRED_STATE_FIELDS = {
    "inning",
    "half",
    "outs",
    "base_state",
    "score_margin",
    "runner",
    "origin_base",
    "target_base",
    "pitcher",
    "catcher",
}

EXPECTED_OUTPUT_FIELDS = {
    "steal_eligible",
    "attempt_recommendation",
    "attempt_probability",
    "success_probability",
    "pickoff_pressure",
    "pickoff_out_probability",
    "selection_reason",
    "fallback_used",
    "fallback_reason",
    "state_completeness",
    "behavioral_effect",
    "canonical_probability_authority_changed",
    "production_activation",
}

VALID_BASES = {
    "first",
    "second",
    "third",
}

VALID_TRANSITIONS = {
    ("first", "second"),
    ("second", "third"),
}

VALID_HALVES = {
    "top",
    "bottom",
}

VALID_ATTEMPT_RECOMMENDATIONS = {
    "attempt",
    "hold",
    "unknown_fallback",
}

VALID_PICKOFF_PRESSURES = {
    "none",
    "low",
    "medium",
    "high",
    "unknown",
}

VALID_COMPLETENESS = {
    "complete",
    "partial",
    "invalid",
}


def _clamp(
    value: Any,
    lower: float = 0.0,
    upper: float = 1.0,
    default: float = 0.0,
) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = default

    return max(
        lower,
        min(
            upper,
            number,
        ),
    )


def _safe_int(
    value: Any,
    default: int = 0,
) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _base_state(
    value: Any,
) -> Dict[str, bool]:
    if not isinstance(value, dict):
        return {
            "first": False,
            "second": False,
            "third": False,
        }

    return {
        "first": bool(
            value.get("first", False)
        ),
        "second": bool(
            value.get("second", False)
        ),
        "third": bool(
            value.get("third", False)
        ),
    }


def _missing_fields(
    payload: Dict[str, Any],
    required: Iterable[str],
) -> list[str]:
    return sorted(
        field
        for field in required
        if field not in payload
    )


def validate_stolen_base_and_pickoff_state(
    state: Any,
) -> Dict[str, Any]:
    """
    Validate and classify input completeness.

    Returns a deterministic validation payload with:
    - valid
    - state_completeness
    - errors
    - warnings
    """

    errors: list[str] = []
    warnings: list[str] = []

    if not isinstance(state, dict):
        return {
            "valid": False,
            "state_completeness": "invalid",
            "errors": [
                "state_must_be_mapping",
            ],
            "warnings": [],
        }

    missing = _missing_fields(
        state,
        REQUIRED_STATE_FIELDS,
    )

    if missing:
        errors.append(
            "missing_required_fields:"
            + ",".join(missing)
        )

    inning = _safe_int(
        state.get("inning"),
        default=-1,
    )

    outs = _safe_int(
        state.get("outs"),
        default=-1,
    )

    half = str(
        state.get("half", "")
    ).lower()

    origin_base = str(
        state.get("origin_base", "")
    ).lower()

    target_base = str(
        state.get("target_base", "")
    ).lower()

    if inning < 1:
        errors.append(
            "inning_must_be_positive"
        )

    if outs not in {
        0,
        1,
        2,
    }:
        errors.append(
            "outs_must_be_zero_one_or_two"
        )

    if half not in VALID_HALVES:
        errors.append(
            "half_must_be_top_or_bottom"
        )

    if origin_base not in VALID_BASES:
        errors.append(
            "origin_base_invalid"
        )

    if target_base not in VALID_BASES:
        errors.append(
            "target_base_invalid"
        )

    if (
        origin_base in VALID_BASES
        and target_base in VALID_BASES
        and (
            origin_base,
            target_base,
        )
        not in VALID_TRANSITIONS
    ):
        errors.append(
            "unsupported_base_transition"
        )

    raw_base_state = state.get(
        "base_state"
    )

    if not isinstance(
        raw_base_state,
        dict,
    ):
        errors.append(
            "base_state_must_be_mapping"
        )
    else:
        for base in VALID_BASES:
            if base not in raw_base_state:
                errors.append(
                    f"base_state_missing_{base}"
                )
            elif not isinstance(
                raw_base_state[base],
                bool,
            ):
                errors.append(
                    f"base_state_{base}_must_be_boolean"
                )

    runner = state.get("runner")
    pitcher = state.get("pitcher")
    catcher = state.get("catcher")

    for name, participant, id_field in [
        (
            "runner",
            runner,
            "runner_id",
        ),
        (
            "pitcher",
            pitcher,
            "pitcher_id",
        ),
        (
            "catcher",
            catcher,
            "catcher_id",
        ),
    ]:
        if not isinstance(
            participant,
            dict,
        ):
            errors.append(
                f"{name}_must_be_mapping"
            )
            continue

        if not participant.get(
            id_field
        ):
            errors.append(
                f"{name}_{id_field}_required"
            )

        if (
            "evidence_complete"
            not in participant
        ):
            warnings.append(
                f"{name}_evidence_complete_missing"
            )
        elif participant.get(
            "evidence_complete"
        ) is not True:
            warnings.append(
                f"{name}_evidence_partial"
            )

    if errors:
        completeness = "invalid"
    elif warnings:
        completeness = "partial"
    else:
        completeness = "complete"

    return {
        "valid": not errors,
        "state_completeness": (
            completeness
        ),
        "errors": errors,
        "warnings": warnings,
    }


def _fallback_output(
    *,
    completeness: str,
    reason: str,
    eligible: bool = False,
) -> Dict[str, Any]:
    return {
        "steal_eligible": eligible,
        "attempt_recommendation": (
            "unknown_fallback"
        ),
        "attempt_probability": 0.0,
        "success_probability": 0.0,
        "pickoff_pressure": "unknown",
        "pickoff_out_probability": 0.0,
        "selection_reason": (
            "conservative_fallback"
        ),
        "fallback_used": True,
        "fallback_reason": reason,
        "state_completeness": (
            completeness
        ),
        "behavioral_effect": "none",
        (
            "canonical_probability_"
            "authority_changed"
        ): False,
        "production_activation": False,
    }


def _eligibility(
    state: Dict[str, Any],
) -> tuple[bool, str]:
    bases = _base_state(
        state.get("base_state")
    )

    origin = str(
        state.get("origin_base", "")
    ).lower()

    target = str(
        state.get("target_base", "")
    ).lower()

    runner = state.get(
        "runner"
    ) or {}

    if (
        origin,
        target,
    ) not in VALID_TRANSITIONS:
        return (
            False,
            "unsupported_transition",
        )

    if not bases.get(
        origin,
        False,
    ):
        return (
            False,
            "origin_base_unoccupied",
        )

    if bases.get(
        target,
        False,
    ):
        return (
            False,
            "target_base_occupied",
        )

    if runner.get(
        "injury_limit_flag"
    ) is True:
        return (
            False,
            "runner_injury_limit",
        )

    return (
        True,
        "eligible_base_state",
    )


def _attempt_probability(
    state: Dict[str, Any],
) -> float:
    runner = state["runner"]
    pitcher = state["pitcher"]
    catcher = state["catcher"]

    speed = _clamp(
        runner.get(
            "speed_score",
            0.50,
        ),
        default=0.50,
    )

    attempt_rate = _clamp(
        runner.get(
            "attempt_rate",
            0.10,
        ),
        default=0.10,
    )

    lead_quality = _clamp(
        runner.get(
            "lead_quality",
            0.50,
        ),
        default=0.50,
    )

    fatigue = _clamp(
        runner.get(
            "fatigue_index",
            0.20,
        ),
        default=0.20,
    )

    pitcher_hold = _clamp(
        pitcher.get(
            "hold_score",
            0.50,
        ),
        default=0.50,
    )

    catcher_throwing = _clamp(
        catcher.get(
            "throwing_score",
            0.50,
        ),
        default=0.50,
    )

    outs = _safe_int(
        state.get("outs"),
    )

    inning = _safe_int(
        state.get("inning"),
    )

    score_margin = _safe_int(
        state.get(
            "score_margin"
        ),
    )

    target = str(
        state.get("target_base")
    ).lower()

    probability = (
        0.04
        + 0.24 * speed
        + 0.22 * attempt_rate
        + 0.10 * lead_quality
        - 0.12 * fatigue
        - 0.14 * pitcher_hold
        - 0.10 * catcher_throwing
    )

    if outs == 2:
        probability -= 0.03

    if (
        inning >= 7
        and abs(score_margin) <= 1
    ):
        probability += 0.04

    if target == "third":
        probability -= 0.03

    return round(
        _clamp(
            probability,
            lower=0.0,
            upper=0.80,
        ),
        6,
    )


def _success_probability(
    state: Dict[str, Any],
) -> float:
    runner = state["runner"]
    pitcher = state["pitcher"]
    catcher = state["catcher"]

    speed = _clamp(
        runner.get(
            "speed_score",
            0.50,
        ),
        default=0.50,
    )

    success_rate = _clamp(
        runner.get(
            "success_rate",
            0.70,
        ),
        default=0.70,
    )

    lead_quality = _clamp(
        runner.get(
            "lead_quality",
            0.50,
        ),
        default=0.50,
    )

    delivery_time = _clamp(
        pitcher.get(
            "delivery_time_score",
            0.50,
        ),
        default=0.50,
    )

    catcher_throwing = _clamp(
        catcher.get(
            "throwing_score",
            0.50,
        ),
        default=0.50,
    )

    pop_time = _clamp(
        catcher.get(
            "pop_time_score",
            0.50,
        ),
        default=0.50,
    )

    probability = (
        0.40
        + 0.22 * speed
        + 0.14 * success_rate
        + 0.08 * lead_quality
        - 0.12 * delivery_time
        - 0.10 * catcher_throwing
        - 0.06 * pop_time
    )

    return round(
        _clamp(
            probability,
            lower=0.10,
            upper=0.95,
        ),
        6,
    )


def _pickoff_metrics(
    state: Dict[str, Any],
) -> tuple[str, float]:
    runner = state["runner"]
    pitcher = state["pitcher"]

    attempt_rate = _clamp(
        pitcher.get(
            "pickoff_attempt_rate",
            0.08,
        ),
        default=0.08,
    )

    success_rate = _clamp(
        pitcher.get(
            "pickoff_success_rate",
            0.02,
        ),
        default=0.02,
    )

    hold_score = _clamp(
        pitcher.get(
            "hold_score",
            0.50,
        ),
        default=0.50,
    )

    runner_speed = _clamp(
        runner.get(
            "speed_score",
            0.50,
        ),
        default=0.50,
    )

    lead_quality = _clamp(
        runner.get(
            "lead_quality",
            0.50,
        ),
        default=0.50,
    )

    disengagements = max(
        0,
        _safe_int(
            state.get(
                "disengagements_used",
                0,
            )
        ),
    )

    pressure_score = (
        0.55 * attempt_rate
        + 0.25 * hold_score
        + 0.20 * lead_quality
    )

    if disengagements >= 2:
        pressure_score -= 0.15

    pressure_score = _clamp(
        pressure_score
    )

    if pressure_score >= 0.55:
        pressure = "high"
    elif pressure_score >= 0.35:
        pressure = "medium"
    elif pressure_score > 0.05:
        pressure = "low"
    else:
        pressure = "none"

    out_probability = (
        0.005
        + 0.08 * success_rate
        + 0.04 * hold_score
        + 0.03 * lead_quality
        - 0.03 * runner_speed
    )

    if disengagements >= 2:
        out_probability *= 0.60

    return (
        pressure,
        round(
            _clamp(
                out_probability,
                lower=0.0,
                upper=0.25,
            ),
            6,
        ),
    )


def evaluate_stolen_base_and_pickoff_state(
    state: Any,
) -> Dict[str, Any]:
    """
    Evaluate a candidate stolen-base and pickoff state.

    The returned probabilities are candidate diagnostic values only.
    No random event is sampled and no state transition is applied.
    """

    state_snapshot = deepcopy(
        state
    )

    validation = (
        validate_stolen_base_and_pickoff_state(
            state_snapshot
        )
    )

    completeness = validation[
        "state_completeness"
    ]

    if completeness == "invalid":
        return _fallback_output(
            completeness="invalid",
            reason="invalid_state:"
            + "|".join(
                validation["errors"]
            ),
        )

    eligible, eligibility_reason = (
        _eligibility(
            state_snapshot
        )
    )

    if completeness == "partial":
        return _fallback_output(
            completeness="partial",
            reason="partial_evidence:"
            + "|".join(
                validation["warnings"]
            ),
            eligible=eligible,
        )

    if not eligible:
        return {
            "steal_eligible": False,
            "attempt_recommendation": (
                "hold"
            ),
            "attempt_probability": 0.0,
            "success_probability": 0.0,
            "pickoff_pressure": "none",
            "pickoff_out_probability": 0.0,
            "selection_reason": (
                eligibility_reason
            ),
            "fallback_used": False,
            "fallback_reason": None,
            "state_completeness": (
                "complete"
            ),
            "behavioral_effect": "none",
            (
                "canonical_probability_"
                "authority_changed"
            ): False,
            "production_activation": False,
        }

    attempt_probability = (
        _attempt_probability(
            state_snapshot
        )
    )

    success_probability = (
        _success_probability(
            state_snapshot
        )
    )

    (
        pickoff_pressure,
        pickoff_out_probability,
    ) = _pickoff_metrics(
        state_snapshot
    )

    recommendation = (
        "attempt"
        if (
            attempt_probability >= 0.18
            and success_probability >= 0.65
        )
        else "hold"
    )

    selection_reason = (
        "attempt_threshold_met"
        if recommendation == "attempt"
        else "attempt_threshold_not_met"
    )

    return {
        "steal_eligible": True,
        "attempt_recommendation": (
            recommendation
        ),
        "attempt_probability": (
            attempt_probability
        ),
        "success_probability": (
            success_probability
        ),
        "pickoff_pressure": (
            pickoff_pressure
        ),
        "pickoff_out_probability": (
            pickoff_out_probability
        ),
        "selection_reason": (
            selection_reason
        ),
        "fallback_used": False,
        "fallback_reason": None,
        "state_completeness": (
            "complete"
        ),
        "behavioral_effect": "none",
        (
            "canonical_probability_"
            "authority_changed"
        ): False,
        "production_activation": False,
    }


def validate_stolen_base_and_pickoff_evaluation(
    evaluation: Any,
) -> Dict[str, Any]:
    """
    Validate the exact evaluator output contract.
    """

    errors: list[str] = []

    if not isinstance(
        evaluation,
        dict,
    ):
        return {
            "valid": False,
            "errors": [
                "evaluation_must_be_mapping",
            ],
        }

    actual_fields = set(
        evaluation
    )

    if actual_fields != EXPECTED_OUTPUT_FIELDS:
        missing = sorted(
            EXPECTED_OUTPUT_FIELDS
            - actual_fields
        )

        extra = sorted(
            actual_fields
            - EXPECTED_OUTPUT_FIELDS
        )

        if missing:
            errors.append(
                "missing_output_fields:"
                + ",".join(missing)
            )

        if extra:
            errors.append(
                "extra_output_fields:"
                + ",".join(extra)
            )

    if not isinstance(
        evaluation.get(
            "steal_eligible"
        ),
        bool,
    ):
        errors.append(
            "steal_eligible_must_be_boolean"
        )

    if evaluation.get(
        "attempt_recommendation"
    ) not in VALID_ATTEMPT_RECOMMENDATIONS:
        errors.append(
            "attempt_recommendation_invalid"
        )

    for field in [
        "attempt_probability",
        "success_probability",
        "pickoff_out_probability",
    ]:
        value = evaluation.get(
            field
        )

        if not isinstance(
            value,
            (
                int,
                float,
            ),
        ):
            errors.append(
                f"{field}_must_be_numeric"
            )
        elif not (
            0.0
            <= float(value)
            <= 1.0
        ):
            errors.append(
                f"{field}_out_of_range"
            )

    if evaluation.get(
        "pickoff_pressure"
    ) not in VALID_PICKOFF_PRESSURES:
        errors.append(
            "pickoff_pressure_invalid"
        )

    if not isinstance(
        evaluation.get(
            "selection_reason"
        ),
        str,
    ):
        errors.append(
            "selection_reason_must_be_string"
        )

    if not isinstance(
        evaluation.get(
            "fallback_used"
        ),
        bool,
    ):
        errors.append(
            "fallback_used_must_be_boolean"
        )

    if evaluation.get(
        "state_completeness"
    ) not in VALID_COMPLETENESS:
        errors.append(
            "state_completeness_invalid"
        )

    if evaluation.get(
        "behavioral_effect"
    ) != "none":
        errors.append(
            "behavioral_effect_must_be_none"
        )

    if evaluation.get(
        "canonical_probability_"
        "authority_changed"
    ) is not False:
        errors.append(
            "canonical_probability_authority_changed_must_be_false"
        )

    if evaluation.get(
        "production_activation"
    ) is not False:
        errors.append(
            "production_activation_must_be_false"
        )

    return {
        "valid": not errors,
        "errors": errors,
    }
