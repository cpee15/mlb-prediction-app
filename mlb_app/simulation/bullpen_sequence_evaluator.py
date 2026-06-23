"""
Pure deterministic bullpen sequence evaluator.

This module has no production simulation authority. It evaluates a supplied
bullpen/game-state snapshot and returns an auditable reliever recommendation.

It does not:
- replace the current pitcher;
- alter starter innings;
- alter bullpen transitions;
- change plate-appearance probabilities;
- change simulation scores or win probabilities;
- activate any production bullpen behavior.
"""

from __future__ import annotations

import math
from copy import deepcopy
from typing import Any, Dict, Iterable


BULLPEN_SEQUENCE_EVALUATOR_VERSION = (
    "bullpen-sequence-evaluator-v1"
)

REQUIRED_STATE_FIELDS = (
    "team_id",
    "inning",
    "outs",
    "base_state",
    "score_margin",
    "leverage_proxy",
    "available_relievers",
)

OPTIONAL_STATE_FIELDS = (
    "current_pitcher_id",
    "used_pitcher_ids",
    "usage_log",
    "bullpen_depletion_index",
    "extra_inning_flag",
)

REQUIRED_RELIEVER_FIELDS = (
    "pitcher_id",
    "role",
    "availability_status",
    "evidence_complete",
)

OPTIONAL_RELIEVER_FIELDS = (
    "throws",
    "quality_score",
    "fatigue_index",
    "recent_usage_count",
    "back_to_back_flag",
    "innings_capacity",
)

ALLOWED_ROLES = {
    "closer",
    "setup",
    "high_leverage",
    "middle_relief",
    "long_relief",
    "low_leverage",
    "unknown",
}

ALLOWED_AVAILABILITY = {
    "available",
    "limited",
    "unavailable",
    "unknown",
}

ALLOWED_LEVERAGE_BANDS = {
    "low",
    "medium",
    "high",
    "critical",
    "unknown",
}

EVALUATOR_OUTPUT_FIELDS = (
    "recommended_pitcher_id",
    "ranked_candidates",
    "leverage_band",
    "selection_reason",
    "fallback_used",
    "fallback_reason",
    "state_completeness",
    "behavioral_effect",
    "canonical_probability_authority_changed",
    "production_activation",
)


ROLE_SCORES = {
    "critical": {
        "closer": 100.0,
        "high_leverage": 94.0,
        "setup": 90.0,
        "middle_relief": 62.0,
        "long_relief": 44.0,
        "low_leverage": 28.0,
        "unknown": 20.0,
    },
    "high": {
        "high_leverage": 96.0,
        "setup": 92.0,
        "closer": 89.0,
        "middle_relief": 68.0,
        "long_relief": 48.0,
        "low_leverage": 35.0,
        "unknown": 22.0,
    },
    "medium": {
        "middle_relief": 91.0,
        "setup": 76.0,
        "high_leverage": 72.0,
        "long_relief": 68.0,
        "closer": 56.0,
        "low_leverage": 48.0,
        "unknown": 32.0,
    },
    "low": {
        "low_leverage": 92.0,
        "long_relief": 87.0,
        "middle_relief": 78.0,
        "unknown": 52.0,
        "setup": 42.0,
        "high_leverage": 36.0,
        "closer": 26.0,
    },
    "unknown": {
        "middle_relief": 70.0,
        "long_relief": 68.0,
        "low_leverage": 62.0,
        "setup": 58.0,
        "high_leverage": 55.0,
        "closer": 50.0,
        "unknown": 45.0,
    },
}


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


def _clean_text(value: Any) -> str:
    if value is None:
        return ""

    return str(value).strip().lower()


def _clean_id(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None

    if isinstance(value, (int, float)):
        if (
            isinstance(value, float)
            and not value.is_integer()
        ):
            return value

        return int(value)

    return str(value).strip() or None


def _missing_fields(
    payload: Dict[str, Any],
    required_fields: Iterable[str],
) -> list[str]:
    return [
        field
        for field in required_fields
        if field not in payload
        or payload.get(field) is None
    ]


def validate_reliever_candidate(
    candidate: Any,
) -> Dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []

    if not isinstance(candidate, dict):
        return {
            "valid": False,
            "errors": [
                "reliever_must_be_object",
            ],
            "warnings": [],
            "missing_fields": list(
                REQUIRED_RELIEVER_FIELDS
            ),
        }

    missing = _missing_fields(
        candidate,
        REQUIRED_RELIEVER_FIELDS,
    )

    for field in missing:
        errors.append(
            f"missing_required_field:{field}"
        )

    pitcher_id = _clean_id(
        candidate.get("pitcher_id")
    )

    if (
        "pitcher_id" not in missing
        and pitcher_id is None
    ):
        errors.append(
            "invalid_pitcher_id"
        )

    role = _clean_text(
        candidate.get("role")
    )

    if (
        "role" not in missing
        and role not in ALLOWED_ROLES
    ):
        errors.append(
            "invalid_role"
        )

    availability = _clean_text(
        candidate.get(
            "availability_status"
        )
    )

    if (
        "availability_status"
        not in missing
        and availability
        not in ALLOWED_AVAILABILITY
    ):
        errors.append(
            "invalid_availability_status"
        )

    if (
        "evidence_complete" not in missing
        and not isinstance(
            candidate.get(
                "evidence_complete"
            ),
            bool,
        )
    ):
        errors.append(
            "evidence_complete_must_be_boolean"
        )

    bounded_numeric_fields = {
        "quality_score": (-1.0, 1.0),
        "fatigue_index": (0.0, 1.0),
        "recent_usage_count": (0.0, 20.0),
        "innings_capacity": (0.0, 9.0),
    }

    for field, bounds in (
        bounded_numeric_fields.items()
    ):
        value = candidate.get(field)

        if value is None:
            warnings.append(
                f"optional_field_missing:{field}"
            )
            continue

        if not _is_number(value):
            errors.append(
                f"field_must_be_numeric:{field}"
            )
            continue

        numeric_value = float(value)

        if not (
            bounds[0]
            <= numeric_value
            <= bounds[1]
        ):
            errors.append(
                f"field_out_of_range:{field}"
            )

    back_to_back = candidate.get(
        "back_to_back_flag"
    )

    if (
        back_to_back is not None
        and not isinstance(
            back_to_back,
            bool,
        )
    ):
        errors.append(
            "back_to_back_flag_must_be_boolean"
        )

    throws = candidate.get("throws")

    if (
        throws is not None
        and str(throws).strip().upper()
        not in {"L", "R", "UNKNOWN"}
    ):
        warnings.append(
            "unrecognized_throws_value"
        )

    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "missing_fields": missing,
    }


def validate_bullpen_sequence_state(
    state: Any,
) -> Dict[str, Any]:
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
            "invalid_reliever_indexes": [],
            "state_completeness": (
                "invalid"
            ),
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
        "score_margin": (-30.0, 30.0),
        "leverage_proxy": (0.0, 1.0),
    }

    integer_fields = {
        "inning",
        "outs",
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

        if not (
            bounds[0]
            <= numeric_value
            <= bounds[1]
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

    relievers = state.get(
        "available_relievers"
    )

    invalid_reliever_indexes: list[int] = []

    if (
        "available_relievers" not in missing
        and not isinstance(
            relievers,
            list,
        )
    ):
        errors.append(
            "available_relievers_must_be_array"
        )
    elif isinstance(relievers, list):
        for index, candidate in enumerate(
            relievers
        ):
            candidate_validation = (
                validate_reliever_candidate(
                    candidate
                )
            )

            if (
                candidate_validation["valid"]
                is not True
            ):
                invalid_reliever_indexes.append(
                    index
                )

        if invalid_reliever_indexes:
            errors.append(
                "invalid_reliever_candidates"
            )

    used_pitcher_ids = state.get(
        "used_pitcher_ids"
    )

    if (
        used_pitcher_ids is not None
        and not isinstance(
            used_pitcher_ids,
            list,
        )
    ):
        warnings.append(
            "optional_field_ignored:"
            "used_pitcher_ids"
        )

    usage_log = state.get(
        "usage_log"
    )

    if (
        usage_log is not None
        and not isinstance(
            usage_log,
            list,
        )
    ):
        warnings.append(
            "optional_field_ignored:usage_log"
        )

    depletion = state.get(
        "bullpen_depletion_index"
    )

    if depletion is not None:
        if not _is_number(depletion):
            warnings.append(
                "optional_field_ignored:"
                "bullpen_depletion_index"
            )
        elif not (
            0.0
            <= float(depletion)
            <= 1.0
        ):
            warnings.append(
                "optional_field_out_of_range:"
                "bullpen_depletion_index"
            )

    extra_inning = state.get(
        "extra_inning_flag"
    )

    if (
        extra_inning is not None
        and not isinstance(
            extra_inning,
            bool,
        )
    ):
        warnings.append(
            "optional_field_ignored:"
            "extra_inning_flag"
        )

    completeness = (
        "invalid"
        if errors
        else (
            "partial"
            if warnings
            or any(
                not candidate.get(
                    "evidence_complete",
                    False,
                )
                for candidate in (
                    relievers or []
                )
                if isinstance(
                    candidate,
                    dict,
                )
            )
            else "complete"
        )
    )

    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "missing_fields": missing,
        "invalid_reliever_indexes": (
            invalid_reliever_indexes
        ),
        "state_completeness": (
            completeness
        ),
    }


def _leverage_band(
    state: Dict[str, Any],
) -> str:
    leverage = float(
        state["leverage_proxy"]
    )

    score_margin = abs(
        float(state["score_margin"])
    )

    inning = int(state["inning"])

    extra_inning = bool(
        state.get(
            "extra_inning_flag",
            False,
        )
    )

    if (
        leverage >= 0.85
        or (
            extra_inning
            and score_margin <= 1.0
        )
    ):
        return "critical"

    if (
        leverage >= 0.65
        or (
            inning >= 8
            and score_margin <= 2.0
        )
    ):
        return "high"

    if leverage >= 0.35:
        return "medium"

    return "low"


def _candidate_score(
    candidate: Dict[str, Any],
    leverage_band: str,
) -> tuple[float, list[str]]:
    role = _clean_text(
        candidate.get("role")
    )

    availability = _clean_text(
        candidate.get(
            "availability_status"
        )
    )

    score = ROLE_SCORES[
        leverage_band
    ].get(
        role,
        ROLE_SCORES[
            leverage_band
        ]["unknown"],
    )

    reasons = [
        f"role_match:{role or 'unknown'}",
        (
            "leverage_band:"
            f"{leverage_band}"
        ),
    ]

    if availability == "limited":
        score -= 35.0
        reasons.append(
            "limited_availability_penalty"
        )
    elif availability == "unknown":
        score -= 50.0
        reasons.append(
            "unknown_availability_penalty"
        )

    quality = candidate.get(
        "quality_score"
    )

    if _is_number(quality):
        score += (
            _clamp(
                float(quality),
                -1.0,
                1.0,
            )
            * 12.0
        )
        reasons.append(
            "quality_adjustment"
        )

    fatigue = candidate.get(
        "fatigue_index"
    )

    if _is_number(fatigue):
        score -= (
            _clamp(
                float(fatigue),
                0.0,
                1.0,
            )
            * 28.0
        )
        reasons.append(
            "fatigue_adjustment"
        )

    recent_usage = candidate.get(
        "recent_usage_count"
    )

    if _is_number(recent_usage):
        score -= (
            _clamp(
                float(recent_usage),
                0.0,
                20.0,
            )
            * 2.5
        )
        reasons.append(
            "recent_usage_adjustment"
        )

    if (
        candidate.get(
            "back_to_back_flag"
        )
        is True
    ):
        score -= 9.0
        reasons.append(
            "back_to_back_penalty"
        )

    capacity = candidate.get(
        "innings_capacity"
    )

    if _is_number(capacity):
        score += (
            min(
                float(capacity),
                3.0,
            )
            * 2.0
        )
        reasons.append(
            "innings_capacity_adjustment"
        )

    if (
        candidate.get(
            "evidence_complete"
        )
        is not True
    ):
        score -= 10.0
        reasons.append(
            "incomplete_evidence_penalty"
        )

    return (
        round(score, 4),
        reasons,
    )


def evaluate_bullpen_sequence(
    state: Any,
) -> Dict[str, Any]:
    """
    Return a deterministic, non-authoritative bullpen recommendation.
    """

    state_snapshot = deepcopy(state)

    validation = (
        validate_bullpen_sequence_state(
            state_snapshot
        )
    )

    if validation["valid"] is not True:
        return {
            "recommended_pitcher_id": None,
            "ranked_candidates": [],
            "leverage_band": "unknown",
            "selection_reason": (
                "insufficient_state"
            ),
            "fallback_used": True,
            "fallback_reason": (
                "invalid_or_incomplete_state"
            ),
            "state_completeness": (
                "invalid"
            ),
            "behavioral_effect": "none",
            (
                "canonical_probability_"
                "authority_changed"
            ): False,
            "production_activation": False,
        }

    leverage_band = _leverage_band(
        state_snapshot
    )

    current_pitcher_id = _clean_id(
        state_snapshot.get(
            "current_pitcher_id"
        )
    )

    used_pitcher_ids = {
        _clean_id(value)
        for value in (
            state_snapshot.get(
                "used_pitcher_ids"
            )
            or []
        )
    }

    ranked_candidates: list[
        Dict[str, Any]
    ] = []

    for candidate in state_snapshot[
        "available_relievers"
    ]:
        pitcher_id = _clean_id(
            candidate.get("pitcher_id")
        )

        availability = _clean_text(
            candidate.get(
                "availability_status"
            )
        )

        if availability == "unavailable":
            continue

        if pitcher_id == current_pitcher_id:
            continue

        if pitcher_id in used_pitcher_ids:
            continue

        score, reasons = (
            _candidate_score(
                candidate,
                leverage_band,
            )
        )

        ranked_candidates.append(
            {
                "pitcher_id": pitcher_id,
                "role": _clean_text(
                    candidate.get("role")
                ),
                "availability_status": (
                    availability
                ),
                "score": score,
                "reasons": reasons,
            }
        )

    ranked_candidates.sort(
        key=lambda row: (
            -float(row["score"]),
            str(row["pitcher_id"]),
        )
    )

    for index, candidate in enumerate(
        ranked_candidates,
        start=1,
    ):
        candidate["rank"] = index

    if not ranked_candidates:
        return {
            "recommended_pitcher_id": None,
            "ranked_candidates": [],
            "leverage_band": leverage_band,
            "selection_reason": (
                "no_eligible_reliever"
            ),
            "fallback_used": True,
            "fallback_reason": (
                "bullpen_depleted_or_unavailable"
            ),
            "state_completeness": (
                validation[
                    "state_completeness"
                ]
            ),
            "behavioral_effect": "none",
            (
                "canonical_probability_"
                "authority_changed"
            ): False,
            "production_activation": False,
        }

    recommended = ranked_candidates[0]

    return {
        "recommended_pitcher_id": (
            recommended["pitcher_id"]
        ),
        "ranked_candidates": (
            ranked_candidates
        ),
        "leverage_band": leverage_band,
        "selection_reason": (
            "highest_deterministic_candidate_score"
        ),
        "fallback_used": False,
        "fallback_reason": None,
        "state_completeness": (
            validation[
                "state_completeness"
            ]
        ),
        "behavioral_effect": "none",
        (
            "canonical_probability_"
            "authority_changed"
        ): False,
        "production_activation": False,
    }


def validate_bullpen_sequence_evaluation(
    payload: Any,
) -> Dict[str, Any]:
    errors: list[str] = []

    if not isinstance(payload, dict):
        return {
            "valid": False,
            "errors": [
                "evaluation_must_be_object",
            ],
        }

    payload_fields = set(payload)

    expected_fields = set(
        EVALUATOR_OUTPUT_FIELDS
    )

    if payload_fields != expected_fields:
        errors.append(
            "evaluation_fields_do_not_match_contract"
        )

    if (
        payload.get("leverage_band")
        not in ALLOWED_LEVERAGE_BANDS
    ):
        errors.append(
            "invalid_leverage_band"
        )

    if not isinstance(
        payload.get("ranked_candidates"),
        list,
    ):
        errors.append(
            "ranked_candidates_must_be_array"
        )

    for field in [
        "fallback_used",
        (
            "canonical_probability_"
            "authority_changed"
        ),
        "production_activation",
    ]:
        if not isinstance(
            payload.get(field),
            bool,
        ):
            errors.append(
                f"field_must_be_boolean:{field}"
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
            "canonical_probability_"
            "authority_changed"
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

    if (
        payload.get("state_completeness")
        not in {
            "complete",
            "partial",
            "invalid",
        }
    ):
        errors.append(
            "invalid_state_completeness"
        )

    if not isinstance(
        payload.get("selection_reason"),
        str,
    ):
        errors.append(
            "selection_reason_must_be_string"
        )

    return {
        "valid": not errors,
        "errors": errors,
    }
