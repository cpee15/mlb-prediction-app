from __future__ import annotations

from mlb_app.simulation.shadow import (
    DEFAULT_PRODUCTION_SHADOW_SIMULATION_COUNT,
)


def test_initial_production_shadow_batch_is_small():
    assert (
        DEFAULT_PRODUCTION_SHADOW_SIMULATION_COUNT
        == 25
    )


def test_initial_execution_does_not_replace_legacy():
    expected_contract = {
        "activation_permitted": False,
        "production_authority_changed": False,
        "authoritative_source": "legacy",
    }

    assert expected_contract[
        "activation_permitted"
    ] is False
    assert expected_contract[
        "production_authority_changed"
    ] is False
    assert expected_contract[
        "authoritative_source"
    ] == "legacy"


def test_baserunning_fail_open_constructor_matches_contract():
    import inspect

    from mlb_app.simulation.shadow import (
        CanonicalShadowBaserunningEvidenceDiscovery,
    )

    parameters = inspect.signature(
        CanonicalShadowBaserunningEvidenceDiscovery
    ).parameters

    assert "error_message" in parameters
    assert "error_type" not in parameters

    failure = (
        CanonicalShadowBaserunningEvidenceDiscovery(
            status="error",
            error_message="production prior unavailable",
        )
    )

    assert failure.status == "error"
    assert (
        failure.error_message
        == "production prior unavailable"
    )
    assert failure.ready is False
