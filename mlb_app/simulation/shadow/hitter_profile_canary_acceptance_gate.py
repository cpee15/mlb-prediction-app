"""Acceptance gates for hitter-profile shadow canary evidence."""

from __future__ import annotations

import math
from typing import Any, Mapping


SCHEMA_VERSION = (
    "hitter_profile_canary_acceptance_gate_v1"
)

MINIMUM_AUDITED_PLAYER_SPLITS = 250
MINIMUM_EXECUTED_PLAYER_SPLITS = 50
MINIMUM_EXECUTION_RATE = 0.20
MAXIMUM_FALLBACK_RATE = 0.05

MAXIMUM_MEDIAN_PROBABILITY_DELTA = 0.05
MAXIMUM_P95_PROBABILITY_DELTA = 0.08
MAXIMUM_OBSERVED_PROBABILITY_DELTA = 0.10

OUTCOME_P95_LIMITS = {
    "bb": 0.05,
    "hr": 0.03,
    "k": 0.05,
    "out": 0.08,
}


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return (
        result
        if math.isfinite(result)
        else None
    )


def _at_least(
    value: Any,
    minimum: float,
) -> bool:
    parsed = _number(value)
    return (
        parsed is not None
        and parsed >= minimum
    )


def _at_most(
    value: Any,
    maximum: float,
) -> bool:
    parsed = _number(value)
    return (
        parsed is not None
        and parsed <= maximum
    )


def evaluate_hitter_profile_canary_acceptance(
    audit: Mapping[str, Any],
) -> dict[str, Any]:
    """Evaluate observed canary evidence without enabling production."""

    payload = dict(audit)
    safety = dict(
        payload.get("safety_checks") or {}
    )
    fallback = dict(
        payload.get("fallback_telemetry") or {}
    )
    maximum_delta = dict(
        payload.get(
            "maximum_absolute_probability_delta"
        )
        or {}
    )
    outcome_deltas = dict(
        payload.get(
            "absolute_probability_delta_by_outcome"
        )
        or {}
    )
    state_counts = dict(
        payload.get("state_counts") or {}
    )

    outcome_checks = {
        outcome: _at_most(
            (
                outcome_deltas.get(outcome)
                or {}
            ).get("p95"),
            limit,
        )
        for outcome, limit in (
            OUTCOME_P95_LIMITS.items()
        )
    }

    checks = {
        "audit_status_observed": (
            payload.get("status")
            == "observed"
        ),
        "minimum_audited_player_splits": (
            _at_least(
                payload.get(
                    "audited_player_split_count"
                ),
                MINIMUM_AUDITED_PLAYER_SPLITS,
            )
        ),
        "minimum_executed_player_splits": (
            _at_least(
                payload.get(
                    "executed_player_split_count"
                ),
                MINIMUM_EXECUTED_PLAYER_SPLITS,
            )
        ),
        "minimum_execution_rate": (
            _at_least(
                payload.get("execution_rate"),
                MINIMUM_EXECUTION_RATE,
            )
        ),
        "maximum_fallback_rate": (
            _at_most(
                fallback.get("fallback_rate"),
                MAXIMUM_FALLBACK_RATE,
            )
        ),
        "zero_audit_errors": (
            int(
                state_counts.get(
                    "audit_error",
                    0,
                )
                or 0
            )
            == 0
        ),
        "production_inputs_unchanged": (
            safety.get(
                "all_production_inputs_unchanged"
            )
            is True
        ),
        "production_authority_unchanged": (
            safety.get(
                "all_production_authority_unchanged"
            )
            is True
            and payload.get(
                "production_authority_changed"
            )
            is False
        ),
        "candidate_probabilities_normalized": (
            safety.get(
                "all_candidate_probabilities_normalized"
            )
            is True
        ),
        "database_writes_absent": (
            safety.get(
                "database_writes_performed"
            )
            is False
        ),
        "maximum_median_probability_delta": (
            _at_most(
                maximum_delta.get("median"),
                MAXIMUM_MEDIAN_PROBABILITY_DELTA,
            )
        ),
        "maximum_p95_probability_delta": (
            _at_most(
                maximum_delta.get("p95"),
                MAXIMUM_P95_PROBABILITY_DELTA,
            )
        ),
        "maximum_observed_probability_delta": (
            _at_most(
                maximum_delta.get("maximum"),
                MAXIMUM_OBSERVED_PROBABILITY_DELTA,
            )
        ),
        **{
            f"{outcome}_p95_probability_delta":
                passed
            for outcome, passed in (
                outcome_checks.items()
            )
        },
    }

    blockers = sorted(
        name
        for name, passed in checks.items()
        if passed is not True
    )
    gate_passed = not blockers

    return {
        "schema_version": SCHEMA_VERSION,
        "status": (
            "accepted_for_feature_flag_integration"
            if gate_passed
            else "blocked"
        ),
        "gate_passed": gate_passed,
        "checks": checks,
        "blockers": blockers,
        "thresholds": {
            "minimum_audited_player_splits":
                MINIMUM_AUDITED_PLAYER_SPLITS,
            "minimum_executed_player_splits":
                MINIMUM_EXECUTED_PLAYER_SPLITS,
            "minimum_execution_rate":
                MINIMUM_EXECUTION_RATE,
            "maximum_fallback_rate":
                MAXIMUM_FALLBACK_RATE,
            "maximum_median_probability_delta":
                MAXIMUM_MEDIAN_PROBABILITY_DELTA,
            "maximum_p95_probability_delta":
                MAXIMUM_P95_PROBABILITY_DELTA,
            "maximum_observed_probability_delta":
                MAXIMUM_OBSERVED_PROBABILITY_DELTA,
            "outcome_p95_limits":
                dict(OUTCOME_P95_LIMITS),
        },
        "observed": {
            "audited_player_split_count":
                payload.get(
                    "audited_player_split_count"
                ),
            "executed_player_split_count":
                payload.get(
                    "executed_player_split_count"
                ),
            "execution_rate":
                payload.get("execution_rate"),
            "fallback_rate":
                fallback.get("fallback_rate"),
            "maximum_absolute_probability_delta":
                maximum_delta,
            "outcome_p95_deltas": {
                outcome: (
                    outcome_deltas.get(outcome)
                    or {}
                ).get("p95")
                for outcome in OUTCOME_P95_LIMITS
            },
        },
        "activation_scope": {
            "eligible_player_splits_only": True,
            "production_fallback_required": True,
            "feature_flag_required": True,
            "simulation_shadow_required": True,
            "production_enabled": False,
        },
        "decision": {
            "feature_flag_integration_allowed":
                gate_passed,
            "production_activation_allowed":
                False,
            "recommended_next_slice": (
                "integrate_hitter_profiles_into_simulation_shadow"
                if gate_passed
                else "collect_additional_hitter_profile_canary_evidence"
            ),
        },
        "parameter_selected": False,
        "production_authority_changed": False,
    }
