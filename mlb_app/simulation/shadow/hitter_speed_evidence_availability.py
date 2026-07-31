"""Readiness audit for cutoff-safe hitter speed evidence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


SCHEMA_VERSION = (
    "shadow_hitter_speed_evidence_availability_v1"
)
DIRECT_SPEED_FIELDS = frozenset({
    "sprint_speed",
    "competitive_runs",
    "bolt_count",
    "home_to_first",
})
REQUIRED_SNAPSHOT_FIELDS = frozenset({
    "player_id",
    "season",
    "as_of_date",
    "sprint_speed",
    "competitive_runs",
    "source_updated_at",
    "source_version",
})
AUTHORITATIVE_SOURCE = {
    "provider": "MLB Baseball Savant",
    "dataset": "Statcast Sprint Speed Leaderboard",
    "url":
        "https://baseballsavant.mlb.com/"
        "leaderboard/sprint_speed",
    "metric_unit": "feet_per_second",
    "qualification":
        "minimum 10 competitive runs",
}
FORBIDDEN_PROXIES = (
    "launch_speed",
    "launch_angle",
    "stolen_base_rate",
    "triple_rate",
)


def evaluate_hitter_speed_evidence_availability(
    *,
    persisted_fields: Sequence[str],
    source_capabilities: Mapping[str, Any],
    baserunning_capabilities: Mapping[str, Any],
) -> dict[str, Any]:
    """Evaluate whether speed evidence is ready for modeling."""

    stored = {
        str(field)
        for field in persisted_fields
    }
    direct_stored = sorted(
        stored & DIRECT_SPEED_FIELDS
    )
    missing_snapshot_fields = sorted(
        REQUIRED_SNAPSHOT_FIELDS - stored
    )

    authoritative_source_confirmed = bool(
        source_capabilities.get(
            "authoritative_source_confirmed"
        )
    )
    adapter_available = bool(
        source_capabilities.get(
            "adapter_available"
        )
    )
    historical_snapshots_available = bool(
        source_capabilities.get(
            "historical_snapshots_available"
        )
    )
    cutoff_query_supported = bool(
        source_capabilities.get(
            "cutoff_query_supported"
        )
    )
    stable_player_identifier = bool(
        source_capabilities.get(
            "stable_player_identifier"
        )
    )
    freshness_metadata_available = bool(
        source_capabilities.get(
            "freshness_metadata_available"
        )
    )

    outcome_evidence_available = bool(
        baserunning_capabilities.get(
            "play_by_play_outcomes_available"
        )
    )
    opportunity_denominators_available = bool(
        baserunning_capabilities.get(
            "opportunity_denominators_available"
        )
    )
    direct_run_measurements_available = bool(
        baserunning_capabilities.get(
            "direct_run_measurements_available"
        )
    )

    blockers = []
    if not direct_stored:
        blockers.append(
            "direct_speed_fields_not_persisted"
        )
    if not authoritative_source_confirmed:
        blockers.append(
            "authoritative_source_not_confirmed"
        )
    if not adapter_available:
        blockers.append(
            "cutoff_safe_source_adapter_unavailable"
        )
    if not historical_snapshots_available:
        blockers.append(
            "historical_speed_snapshots_unavailable"
        )
    if not cutoff_query_supported:
        blockers.append(
            "historical_cutoff_query_unsupported"
        )
    if not stable_player_identifier:
        blockers.append(
            "stable_player_identifier_unverified"
        )
    if not freshness_metadata_available:
        blockers.append(
            "source_freshness_metadata_unavailable"
        )

    speed_signal_ready = not blockers
    predictive_evaluation_allowed = (
        speed_signal_ready
        and outcome_evidence_available
        and opportunity_denominators_available
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "status": (
            "ready"
            if speed_signal_ready
            else "blocked"
        ),
        "shadow_only": True,
        "audit_only": True,
        "parameter_selected": False,
        "production_authority_changed": False,
        "speed_signal_ready": speed_signal_ready,
        "predictive_evaluation_allowed":
            predictive_evaluation_allowed,
        "persisted_field_count": len(stored),
        "direct_speed_fields_present":
            direct_stored,
        "missing_snapshot_fields":
            missing_snapshot_fields,
        "authoritative_source":
            dict(AUTHORITATIVE_SOURCE),
        "source_capabilities": {
            "authoritative_source_confirmed":
                authoritative_source_confirmed,
            "adapter_available":
                adapter_available,
            "historical_snapshots_available":
                historical_snapshots_available,
            "cutoff_query_supported":
                cutoff_query_supported,
            "stable_player_identifier":
                stable_player_identifier,
            "freshness_metadata_available":
                freshness_metadata_available,
        },
        "baserunning_capabilities": {
            "play_by_play_outcomes_available":
                outcome_evidence_available,
            "opportunity_denominators_available":
                opportunity_denominators_available,
            "direct_run_measurements_available":
                direct_run_measurements_available,
        },
        "proxy_policy": {
            "proxy_substitution_allowed": False,
            "forbidden_speed_proxies":
                list(FORBIDDEN_PROXIES),
            "reason":
                "outcomes and contact quality are not "
                "direct measurements of runner speed",
        },
        "blockers": blockers,
        "recommended_next_slice": (
            None
            if speed_signal_ready
            else
            "build_cutoff_safe_hitter_speed_source_adapter"
        ),
        "production_impact": {
            "database_schema_changed": False,
            "external_fetch_performed": False,
            "production_model_modified": False,
            "simulation_authority_changed": False,
        },
    }
