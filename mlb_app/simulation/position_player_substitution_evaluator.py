"""
Pure deterministic position-player substitution evaluator.

This module has no production simulation authority. It does not alter lineups,
batting order, defensive alignment, baserunners, base/out state, probabilities,
scores, or win probabilities.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any


SUPPORTED_SUBSTITUTION_TYPES = {
    "pinch_hitter",
    "pinch_runner",
    "defensive_replacement",
    "injury_replacement",
    "double_switch_or_lineup_reassignment",
}

REQUIRED_STATE_FIELDS = {
    "inning",
    "half",
    "outs",
    "score_margin",
    "base_state",
    "substitution_type",
    "current_player",
    "candidate_players",
    "batting_order",
    "current_lineup_slot",
}

REQUIRED_PLAYER_FIELDS = {
    "player_id",
    "active",
    "already_used",
    "evidence_complete",
}

OUTPUT_FIELDS = {
    "substitution_eligible",
    "recommended_action",
    "recommended_player_id",
    "substitution_type",
    "candidate_score",
    "current_player_score",
    "selection_reason",
    "lineup_constraint_valid",
    "fallback_used",
    "fallback_reason",
    "state_completeness",
    "behavioral_effect",
    "production_activation",
}


def _clamp(value: Any, default: float = 0.50) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default

    return max(0.0, min(1.0, number))


def _player_contract_valid(player: Any) -> bool:
    return (
        isinstance(player, dict)
        and REQUIRED_PLAYER_FIELDS.issubset(player)
        and isinstance(player.get("player_id"), str)
        and bool(player.get("player_id"))
    )


def validate_position_player_substitution_state(
    state: Any,
) -> dict[str, Any]:
    if not isinstance(state, dict):
        return {
            "valid": False,
            "state_completeness": "invalid",
            "errors": ["state_must_be_object"],
            "warnings": [],
        }

    errors: list[str] = []
    warnings: list[str] = []

    missing = sorted(REQUIRED_STATE_FIELDS - set(state))
    if missing:
        errors.append(
            "missing_required_fields:" + "|".join(missing)
        )

    if state.get("substitution_type") not in SUPPORTED_SUBSTITUTION_TYPES:
        errors.append("unsupported_substitution_type")

    current_player = state.get("current_player")
    candidates = state.get("candidate_players")
    batting_order = state.get("batting_order")

    if not _player_contract_valid(current_player):
        errors.append("current_player_contract_invalid")

    if not isinstance(candidates, list):
        errors.append("candidate_players_must_be_array")
    else:
        for index, candidate in enumerate(candidates):
            if not _player_contract_valid(candidate):
                errors.append(
                    f"candidate_{index}_contract_invalid"
                )

    if not isinstance(batting_order, list):
        errors.append("batting_order_must_be_array")
    elif len(batting_order) != 9:
        errors.append("batting_order_must_have_nine_slots")

    try:
        inning = int(state.get("inning"))
        if inning < 1:
            errors.append("inning_out_of_range")
    except (TypeError, ValueError):
        errors.append("inning_invalid")

    try:
        outs = int(state.get("outs"))
        if outs not in {0, 1, 2}:
            errors.append("outs_out_of_range")
    except (TypeError, ValueError):
        errors.append("outs_invalid")

    try:
        slot = int(state.get("current_lineup_slot"))
        if slot < 1 or slot > 9:
            errors.append("lineup_slot_out_of_range")
    except (TypeError, ValueError):
        errors.append("lineup_slot_invalid")

    evidence_partial = (
        isinstance(current_player, dict)
        and current_player.get("evidence_complete") is not True
    )

    if isinstance(candidates, list):
        evidence_partial = evidence_partial or any(
            isinstance(candidate, dict)
            and candidate.get("evidence_complete") is not True
            for candidate in candidates
        )

    if evidence_partial:
        warnings.append("participant_evidence_partial")

    completeness = (
        "invalid"
        if errors
        else "partial"
        if warnings
        else "complete"
    )

    return {
        "valid": not errors,
        "state_completeness": completeness,
        "errors": errors,
        "warnings": warnings,
    }


def _position_compatible(
    state: dict[str, Any],
    candidate: dict[str, Any],
) -> bool:
    substitution_type = state.get("substitution_type")

    if substitution_type in {
        "pinch_hitter",
        "pinch_runner",
        "injury_replacement",
    }:
        return True

    current_player = state.get("current_player") or {}
    current_position = current_player.get("primary_position")

    if not current_position:
        return True

    eligible_positions = candidate.get("eligible_positions")
    if not isinstance(eligible_positions, list):
        eligible_positions = []

    return (
        candidate.get("primary_position") == current_position
        or current_position in eligible_positions
    )


def _candidate_eligible(
    state: dict[str, Any],
    candidate: dict[str, Any],
) -> bool:
    used_player_ids = set(state.get("used_player_ids") or [])

    return all(
        [
            candidate.get("active") is True,
            candidate.get("already_used") is not True,
            candidate.get("player_id") not in used_player_ids,
            _position_compatible(state, candidate),
        ]
    )


def _player_score(
    player: dict[str, Any],
    substitution_type: str,
) -> float:
    offense = _clamp(player.get("offense_score"))
    running = _clamp(player.get("running_score"))
    defense = _clamp(player.get("defense_score"))

    if substitution_type == "pinch_hitter":
        score = offense
    elif substitution_type == "pinch_runner":
        score = running
    elif substitution_type == "defensive_replacement":
        score = defense
    elif substitution_type == "injury_replacement":
        score = 0.40 * offense + 0.20 * running + 0.40 * defense
    else:
        score = 0.50 * offense + 0.50 * defense

    return round(score, 6)


def _lineup_constraint_valid(
    state: dict[str, Any],
    candidate: dict[str, Any] | None,
) -> bool:
    if candidate is None:
        return False

    batting_order = state.get("batting_order")
    if not isinstance(batting_order, list) or len(batting_order) != 9:
        return False

    try:
        slot = int(state.get("current_lineup_slot"))
    except (TypeError, ValueError):
        return False

    if slot < 1 or slot > 9:
        return False

    if (
        state.get("substitution_type")
        == "double_switch_or_lineup_reassignment"
        and state.get("designated_hitter_active") is True
    ):
        return False

    return True


def _fallback_output(
    *,
    substitution_type: Any,
    completeness: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "substitution_eligible": False,
        "recommended_action": "fallback",
        "recommended_player_id": None,
        "substitution_type": (
            substitution_type
            if isinstance(substitution_type, str)
            else "unknown"
        ),
        "candidate_score": None,
        "current_player_score": None,
        "selection_reason": reason,
        "lineup_constraint_valid": False,
        "fallback_used": True,
        "fallback_reason": reason,
        "state_completeness": completeness,
        "behavioral_effect": "none",
        "production_activation": False,
    }


def evaluate_position_player_substitution(
    state: Any,
) -> dict[str, Any]:
    state_snapshot = deepcopy(state)

    validation = validate_position_player_substitution_state(
        state_snapshot
    )

    substitution_type = (
        state_snapshot.get("substitution_type")
        if isinstance(state_snapshot, dict)
        else "unknown"
    )

    completeness = validation.get("state_completeness")

    if completeness == "invalid":
        return _fallback_output(
            substitution_type=substitution_type,
            completeness="invalid",
            reason="invalid_state",
        )

    if completeness == "partial":
        return _fallback_output(
            substitution_type=substitution_type,
            completeness="partial",
            reason="partial_evidence",
        )

    current_player = deepcopy(
        state_snapshot.get("current_player") or {}
    )

    eligible_candidates = [
        deepcopy(candidate)
        for candidate in state_snapshot.get("candidate_players") or []
        if isinstance(candidate, dict)
        and _candidate_eligible(state_snapshot, candidate)
    ]

    current_score = _player_score(
        current_player,
        substitution_type,
    )

    if not eligible_candidates:
        return {
            "substitution_eligible": False,
            "recommended_action": (
                "required_replacement"
                if state_snapshot.get("injury_required") is True
                else "retain"
            ),
            "recommended_player_id": None,
            "substitution_type": substitution_type,
            "candidate_score": None,
            "current_player_score": current_score,
            "selection_reason": "no_eligible_candidate",
            "lineup_constraint_valid": False,
            "fallback_used": False,
            "fallback_reason": None,
            "state_completeness": "complete",
            "behavioral_effect": "none",
            "production_activation": False,
        }

    ranked = sorted(
        [
            (
                _player_score(candidate, substitution_type),
                str(candidate.get("player_id")),
                candidate,
            )
            for candidate in eligible_candidates
        ],
        key=lambda item: (-item[0], item[1]),
    )

    candidate_score, candidate_id, selected = ranked[0]

    lineup_valid = _lineup_constraint_valid(
        state_snapshot,
        selected,
    )

    injury_required = (
        state_snapshot.get("injury_required") is True
        or substitution_type == "injury_replacement"
    )

    if injury_required and lineup_valid:
        action = "required_replacement"
    elif injury_required:
        action = "fallback"
    elif lineup_valid and candidate_score > current_score:
        action = "substitute"
    else:
        action = "retain"

    fallback_used = injury_required and not lineup_valid

    return {
        "substitution_eligible": lineup_valid,
        "recommended_action": action,
        "recommended_player_id": (
            candidate_id
            if action in {"substitute", "required_replacement"}
            else None
        ),
        "substitution_type": substitution_type,
        "candidate_score": candidate_score,
        "current_player_score": current_score,
        "selection_reason": (
            "mandatory_replacement_best_candidate"
            if action == "required_replacement"
            else "candidate_improves_role_score"
            if action == "substitute"
            else "lineup_constraint_invalid"
            if not lineup_valid
            else "current_player_retained"
        ),
        "lineup_constraint_valid": lineup_valid,
        "fallback_used": fallback_used,
        "fallback_reason": (
            "lineup_constraint_invalid"
            if fallback_used
            else None
        ),
        "state_completeness": "complete",
        "behavioral_effect": "none",
        "production_activation": False,
    }


def validate_position_player_substitution_evaluation(
    payload: Any,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "valid": False,
            "errors": ["evaluation_must_be_object"],
        }

    errors: list[str] = []

    missing = sorted(OUTPUT_FIELDS - set(payload))
    unexpected = sorted(set(payload) - OUTPUT_FIELDS)

    if missing:
        errors.append(
            "missing_output_fields:" + "|".join(missing)
        )

    if unexpected:
        errors.append(
            "unexpected_output_fields:" + "|".join(unexpected)
        )

    if payload.get("behavioral_effect") != "none":
        errors.append("behavioral_effect_must_be_none")

    if payload.get("production_activation") is not False:
        errors.append("production_activation_must_be_false")

    if payload.get("state_completeness") not in {
        "complete",
        "partial",
        "invalid",
    }:
        errors.append("state_completeness_invalid")

    if payload.get("recommended_action") not in {
        "substitute",
        "retain",
        "required_replacement",
        "fallback",
    }:
        errors.append("recommended_action_invalid")

    return {
        "valid": not errors,
        "errors": errors,
    }
