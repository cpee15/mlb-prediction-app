from mlb_app.simulation.shadow.hitter_speed_evidence_availability import (
    REQUIRED_SNAPSHOT_FIELDS,
    evaluate_hitter_speed_evidence_availability,
)
from scripts.audit_shadow_hitter_speed_evidence_availability import (
    build_audit,
)


def evaluate(
    *,
    persisted_fields=(),
    source=None,
    baserunning=None,
):
    return evaluate_hitter_speed_evidence_availability(
        persisted_fields=persisted_fields,
        source_capabilities=source or {},
        baserunning_capabilities=baserunning or {},
    )


def ready_source():
    return {
        "authoritative_source_confirmed": True,
        "adapter_available": True,
        "historical_snapshots_available": True,
        "cutoff_query_supported": True,
        "stable_player_identifier": True,
        "freshness_metadata_available": True,
    }


def ready_baserunning():
    return {
        "play_by_play_outcomes_available": True,
        "opportunity_denominators_available": True,
        "direct_run_measurements_available": True,
    }


def test_current_repository_contract_is_blocked():
    result = evaluate(
        persisted_fields={
            "player_id",
            "season",
            "launch_speed",
            "launch_angle",
        },
        source={
            "authoritative_source_confirmed": True,
        },
        baserunning={
            "play_by_play_outcomes_available": True,
        },
    )

    assert result["status"] == "blocked"
    assert result["speed_signal_ready"] is False
    assert (
        result["predictive_evaluation_allowed"]
        is False
    )
    assert "direct_speed_fields_not_persisted" in (
        result["blockers"]
    )
    assert (
        "cutoff_safe_source_adapter_unavailable"
        in result["blockers"]
    )
    assert (
        "historical_speed_snapshots_unavailable"
        in result["blockers"]
    )


def test_contact_and_outcome_metrics_are_not_speed_proxies():
    result = evaluate()

    assert (
        result["proxy_policy"][
            "proxy_substitution_allowed"
        ]
        is False
    )
    assert {
        "launch_speed",
        "launch_angle",
        "stolen_base_rate",
        "triple_rate",
    } == set(
        result["proxy_policy"][
            "forbidden_speed_proxies"
        ]
    )


def test_authoritative_source_contract_is_explicit():
    result = evaluate()

    assert (
        result["authoritative_source"]["provider"]
        == "MLB Baseball Savant"
    )
    assert (
        result["authoritative_source"][
            "metric_unit"
        ]
        == "feet_per_second"
    )
    assert "10 competitive runs" in (
        result["authoritative_source"][
            "qualification"
        ]
    )


def test_complete_cutoff_safe_contract_can_be_ready():
    result = evaluate(
        persisted_fields=REQUIRED_SNAPSHOT_FIELDS,
        source=ready_source(),
        baserunning=ready_baserunning(),
    )

    assert result["status"] == "ready"
    assert result["speed_signal_ready"] is True
    assert (
        result["predictive_evaluation_allowed"]
        is True
    )
    assert result["blockers"] == []
    assert result["recommended_next_slice"] is None


def test_outcomes_without_opportunities_do_not_allow_evaluation():
    baserunning = ready_baserunning()
    baserunning[
        "opportunity_denominators_available"
    ] = False

    result = evaluate(
        persisted_fields=REQUIRED_SNAPSHOT_FIELDS,
        source=ready_source(),
        baserunning=baserunning,
    )

    assert result["speed_signal_ready"] is True
    assert (
        result["predictive_evaluation_allowed"]
        is False
    )


def test_audit_remains_shadow_only():
    result = evaluate()

    assert result["shadow_only"] is True
    assert result["audit_only"] is True
    assert result["parameter_selected"] is False
    assert (
        result["production_authority_changed"]
        is False
    )
    assert (
        result["production_impact"][
            "external_fetch_performed"
        ]
        is False
    )
    assert (
        result["production_impact"][
            "database_schema_changed"
        ]
        is False
    )

def test_repository_audit_proves_current_source_gap():
    result = build_audit()

    assert result["status"] == "blocked"
    assert (
        result["decision"][
            "current_speed_signal_usable"
        ]
        is False
    )
    assert (
        result["decision"][
            "source_acquisition_required"
        ]
        is True
    )
    assert result["repository_evidence"][
        "statcast_speed_fields_present"
    ] == []
    assert result["repository_evidence"][
        "speed_adapter_paths"
    ] == []
    assert result["repository_evidence"][
        "speed_snapshot_paths"
    ] == []


def test_repository_audit_does_not_authorize_proxy_or_production():
    result = build_audit()

    assert (
        result["decision"][
            "proxy_substitution_allowed"
        ]
        is False
    )
    assert (
        result["predictive_evaluation_allowed"]
        is False
    )
    assert result["parameter_selected"] is False
    assert (
        result["production_authority_changed"]
        is False
    )
    assert (
        result["recommended_next_slice"]
        == "build_cutoff_safe_hitter_speed_source_adapter"
    )
