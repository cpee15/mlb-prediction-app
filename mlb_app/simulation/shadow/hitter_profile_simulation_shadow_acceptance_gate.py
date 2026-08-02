"""Evaluate live hitter-profile simulation-shadow evidence."""

from __future__ import annotations

import math
from typing import Any, Mapping


SCHEMA_VERSION = (
    "hitter_profile_simulation_shadow_acceptance_gate_v1"
)

MINIMUM_AUDITED_GAMES = 15
MINIMUM_OBSERVED_GAMES = 10
MINIMUM_OBSERVATION_RATE = 0.50
MINIMUM_COMPARISON_COUNT = 2500
MINIMUM_SIMULATION_COUNT = 1000
MINIMUM_ARTIFACT_READY_RATE = 0.50
MAXIMUM_SHARED_EXECUTION_ERROR_RATE = 0.10

SCOPE_LIMITS = {
    "game_probability": {
        "p95": 0.03,
        "maximum": 0.05,
    },
    "game": {
        "p95": 0.25,
        "maximum": 0.35,
    },
    "team": {
        "p95": 0.30,
        "maximum": 0.40,
    },
    "batter": {
        "p95": 0.15,
        "maximum": 1.25,
    },
    "pitcher": {
        "p95": 0.15,
        "maximum": 0.50,
    },
}

METRIC_LIMITS = {
    "home_win_probability": {
        "p95": 0.03,
        "maximum": 0.05,
    },
    "away_win_probability": {
        "p95": 0.03,
        "maximum": 0.05,
    },
    "total_run_distribution_mean": {
        "p95": 0.30,
        "maximum": 0.35,
    },
    "dfs_points": {
        "p95": 0.75,
        "maximum": 1.25,
    },
}


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None

    try:
        result = float(value)
    except (TypeError, ValueError):
        return None

    return result if math.isfinite(result) else None


def _at_least(
    value: Any,
    minimum: float,
) -> bool:
    number = _number(value)
    return (
        number is not None
        and number >= minimum
    )


def _at_most(
    value: Any,
    maximum: float,
) -> bool:
    number = _number(value)
    return (
        number is not None
        and number <= maximum
    )


def _records(
    payload: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    value = payload.get("records")
    if not isinstance(value, list):
        return []

    return [
        record
        for record in value
        if isinstance(record, Mapping)
    ]


def evaluate_hitter_profile_simulation_shadow_acceptance(
    audit: Mapping[str, Any],
) -> dict[str, Any]:
    """Evaluate bounded live shadow evidence without activating production."""

    payload = dict(audit or {})
    records = _records(payload)
    audited_game_count = _number(
        payload.get("audited_game_count")
    )
    observed_game_count = _number(
        payload.get("observed_game_count")
    )

    observed_record_count = sum(
        1
        for record in records
        if record.get("status") == "observed"
    )
    artifact_ready_game_count = sum(
        1
        for record in records
        if record.get("materialization_status")
        == "ready"
    )
    shared_execution_error_game_count = sum(
        1
        for record in records
        if (
            (
                record.get("baseline_execution")
                or {}
            ).get("status")
            == "error"
            and (
                record.get("candidate_execution")
                or {}
            ).get("status")
            == "error"
        )
    )

    artifact_ready_rate = (
        artifact_ready_game_count
        / audited_game_count
        if (
            audited_game_count is not None
            and audited_game_count > 0
        )
        else None
    )
    shared_execution_error_rate = (
        shared_execution_error_game_count
        / audited_game_count
        if (
            audited_game_count is not None
            and audited_game_count > 0
        )
        else None
    )

    scope_evidence = (
        payload.get("absolute_delta_by_scope")
        or {}
    )
    metric_evidence = (
        payload.get("absolute_delta_by_metric")
        or {}
    )
    safety = payload.get("safety_checks") or {}

    scope_checks = {
        f"{scope}_{stat}_delta": _at_most(
            (
                scope_evidence.get(scope)
                or {}
            ).get(stat),
            limit,
        )
        for scope, limits in SCOPE_LIMITS.items()
        for stat, limit in limits.items()
    }
    metric_checks = {
        f"{metric}_{stat}_delta": _at_most(
            (
                metric_evidence.get(metric)
                or {}
            ).get(stat),
            limit,
        )
        for metric, limits in METRIC_LIMITS.items()
        for stat, limit in limits.items()
    }

    checks = {
        "audit_status_observed": (
            payload.get("status") == "observed"
        ),
        "complete_game_records": (
            audited_game_count is not None
            and len(records)
            == audited_game_count
        ),
        "observed_game_count_reconciles": (
            observed_game_count is not None
            and observed_record_count
            == observed_game_count
        ),
        "minimum_audited_games": _at_least(
            audited_game_count,
            MINIMUM_AUDITED_GAMES,
        ),
        "minimum_observed_games": _at_least(
            observed_game_count,
            MINIMUM_OBSERVED_GAMES,
        ),
        "minimum_observation_rate": _at_least(
            payload.get("observation_rate"),
            MINIMUM_OBSERVATION_RATE,
        ),
        "minimum_comparison_count": _at_least(
            payload.get("comparison_count"),
            MINIMUM_COMPARISON_COUNT,
        ),
        "minimum_simulation_count": _at_least(
            payload.get("simulation_count"),
            MINIMUM_SIMULATION_COUNT,
        ),
        "minimum_artifact_ready_rate": _at_least(
            artifact_ready_rate,
            MINIMUM_ARTIFACT_READY_RATE,
        ),
        "maximum_shared_execution_error_rate":
            _at_most(
                shared_execution_error_rate,
                MAXIMUM_SHARED_EXECUTION_ERROR_RATE,
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
        "simulation_counts_match": (
            safety.get(
                "all_simulation_counts_match"
            )
            is True
        ),
        "database_writes_absent": (
            safety.get(
                "database_writes_performed"
            )
            is False
            and payload.get(
                "database_writes_performed"
            )
            is False
        ),
        **scope_checks,
        **metric_checks,
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
            "accepted_for_extended_shadow_evaluation"
            if gate_passed
            else "blocked"
        ),
        "gate_passed": gate_passed,
        "checks": checks,
        "blockers": blockers,
        "thresholds": {
            "minimum_audited_games":
                MINIMUM_AUDITED_GAMES,
            "minimum_observed_games":
                MINIMUM_OBSERVED_GAMES,
            "minimum_observation_rate":
                MINIMUM_OBSERVATION_RATE,
            "minimum_comparison_count":
                MINIMUM_COMPARISON_COUNT,
            "minimum_simulation_count":
                MINIMUM_SIMULATION_COUNT,
            "minimum_artifact_ready_rate":
                MINIMUM_ARTIFACT_READY_RATE,
            "maximum_shared_execution_error_rate":
                MAXIMUM_SHARED_EXECUTION_ERROR_RATE,
            "scope_limits": {
                scope: dict(limits)
                for scope, limits in (
                    SCOPE_LIMITS.items()
                )
            },
            "metric_limits": {
                metric: dict(limits)
                for metric, limits in (
                    METRIC_LIMITS.items()
                )
            },
        },
        "observed": {
            "audited_game_count":
                payload.get("audited_game_count"),
            "observed_game_count":
                payload.get("observed_game_count"),
            "observed_record_count":
                observed_record_count,
            "observation_rate":
                payload.get("observation_rate"),
            "comparison_count":
                payload.get("comparison_count"),
            "simulation_count":
                payload.get("simulation_count"),
            "artifact_ready_game_count":
                artifact_ready_game_count,
            "artifact_ready_rate":
                artifact_ready_rate,
            "shared_execution_error_game_count":
                shared_execution_error_game_count,
            "shared_execution_error_rate":
                shared_execution_error_rate,
            "absolute_delta_by_scope":
                scope_evidence,
            "absolute_delta_by_metric":
                metric_evidence,
        },
        "evaluation_scope": {
            "scope_specific_thresholds": True,
            "feature_flag_required": True,
            "simulation_shadow_required": True,
            "production_fallback_required": True,
            "production_enabled": False,
        },
        "decision": {
            "extended_shadow_evaluation_allowed":
                gate_passed,
            "production_activation_allowed": False,
            "recommended_next_slice": (
                "run_extended_hitter_profile_simulation_shadow_evaluation"
                if gate_passed
                else "collect_additional_hitter_profile_simulation_shadow_evidence"
            ),
        },
        "parameter_selected": False,
        "production_authority_changed": False,
    }
